// Arkilian Hydration Engine v2 — implementation
//
// Logical replay: downloads snapshot binary + incremental SQL chunks
// via Pre-Signed URLs, plays them back with sqlite3_exec() inside
// explicit transactions.  No binary WAL frame manipulation needed.
//
// Auth model: SigV4 only. The client holds a per-database S3 access/
// secret key pair, presigns every GET locally, and never sends a bearer
// token anywhere. The manifest at {prefix}/manifest.json — maintained by
// the shipping side (src/class.c) — is the registry of the baseline
// snapshot and every WAL chunk with its LSN range and SHA-256 digest.

// Feature-test macros MUST precede every system include: this file is
// compiled standalone (N-API addon, test TUs) with CMake's strict
// -std=c99 (extensions OFF), where glibc hides gmtime_r/strcasecmp/
// O_DIRECTORY behind _DEFAULT_SOURCE/_POSIX_C_SOURCE unless they are
// defined here. class.c does the same for the same reason. macOS needs
// _DARWIN_C_SOURCE for BSD symbols (flock in <sys/file.h>) that
// _POSIX_C_SOURCE alone hides — same as class.c's statfs guard.
#ifndef _POSIX_C_SOURCE
#define _POSIX_C_SOURCE 200809L
#endif
#ifndef _DEFAULT_SOURCE
#define _DEFAULT_SOURCE
#endif
#ifdef __APPLE__
#ifndef _DARWIN_C_SOURCE
#define _DARWIN_C_SOURCE
#endif
#endif

#include "hydration.h"
#include "sha256.h"
#include <curl/curl.h>
#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>

// ── Platform shims ──────────────────────────────────────────────────
// Windows (MSVC and MinGW) has no POSIX layer: the single-flight mutex
// becomes a CRITICAL_SECTION, gmtime_r becomes gmtime_s (REVERSED
// argument order), and fsync becomes _commit. The POSIX open/close/
// fileno spellings used by fsync_parent_dir resolve through <io.h>
// because both builds (CMake and node-gyp) define
// _CRT_NONSTDC_NO_WARNINGS. Keep this list in lockstep with class.c's
// shims.
#ifdef _WIN32
  #include <windows.h>
  #include <io.h>
  #include <process.h>
  #include <sys/locking.h>
  #define strcasecmp _stricmp
  #define ARK_FILENO(f) _fileno(f)
  #define ARK_FSYNC(fd) _commit(fd)
  #ifndef __MINGW32__
  /* MSVC: strtok_s has the same (str, delim, *ctx) signature as strtok_r.
     Kept in lockstep with class.c's shim — this file is compiled by the
     Windows N-API/prebuild leg, which is MSVC. */
  #define strtok_r strtok_s
  #endif
#else
  #include <pthread.h>
  #include <strings.h>
  #include <sys/file.h>
  #include <unistd.h>
  #define ARK_FILENO(f) fileno(f)
  #define ARK_FSYNC(fd) fsync(fd)
#endif

// ── Restore-exclusion + staging-file shims ──────────────────────────
// Hydration REPLACES the database file on disk. The BEGIN IMMEDIATE
// probe is point-in-time only; the OS lock below is the actual exclusion
// contract: an exclusive, non-blocking lock on <db_path>.arklock held for
// the entire restore. Any other hydrator (process, runtime, or library
// instance) that also takes the lock is refused immediately — the
// process-global single-flight mutex only ever serialized hydrates within
// one process. Application writers that do not take the lock are still
// caught by the probe if actively writing; the operator contract
// ("hydrate from a cold process") remains, now enforced as far as the
// environment permits.
#ifdef _WIN32
  #define ARK_LOCKFILE_OPEN(path) \
    _open((path), _O_CREAT | _O_RDWR, _S_IREAD | _S_IWRITE)
  #define ARK_LOCKFILE_LOCK(fd)   (_locking((fd), _LK_NBLCK, 1) == 0)
  #define ARK_LOCKFILE_UNLOCK(fd) ((void)_locking((fd), _LK_UNLCK, 1))
  #define ARK_LOCKFILE_CLOSE(fd)  _close(fd)
  #define ARK_OPEN_EXCL(path) \
    _open((path), _O_CREAT | _O_EXCL | _O_WRONLY | _O_BINARY, _S_IREAD | _S_IWRITE)
  #define ARK_FDOPEN(fd, mode) _fdopen((fd), (mode))
  #define ARK_CLOSE_FD(fd)     _close(fd)
  #define ARK_GETPID           _getpid
#else
  #define ARK_LOCKFILE_OPEN(path) open((path), O_CREAT | O_RDWR, 0600)
  #define ARK_LOCKFILE_LOCK(fd)   (flock((fd), LOCK_EX | LOCK_NB) == 0)
  #define ARK_LOCKFILE_UNLOCK(fd) ((void)flock((fd), LOCK_UN))
  #define ARK_LOCKFILE_CLOSE(fd)  close(fd)
  #define ARK_OPEN_EXCL(path)     open((path), O_CREAT | O_EXCL | O_WRONLY, 0600)
  #define ARK_FDOPEN(fd, mode)    fdopen((fd), (mode))
  #define ARK_CLOSE_FD(fd)        close(fd)
  #define ARK_GETPID              getpid
#endif

// Unique per-instance staging file (same contract as class.c's
// arkilian_unique_tmp — kept in lockstep; hydration.c links standalone in
// the test_minio_hydrate target, so it cannot depend on class.c symbols).
// The old fixed "<path>.arkdl" name was shared across processes hydrating
// the same db path.
static FILE *hydrate_unique_tmp(const char *base, const char *suffix,
                                char *out, size_t out_cap) {
  static volatile int g_tmp_counter = 0;
  if (!base || !suffix || !out || out_cap == 0) return NULL;
  for (int attempt = 0; attempt < 8; attempt++) {
    int seq;
#ifdef __GNUC__
    seq = __atomic_add_fetch(&g_tmp_counter, 1, __ATOMIC_ACQ_REL);
#else
    seq = ++g_tmp_counter;
#endif
    int n = snprintf(out, out_cap, "%s.arktmp.%ld.%d.%s",
                     base, (long)ARK_GETPID(), seq, suffix);
    if (n <= 0 || (size_t)n >= out_cap) return NULL;
    int fd = ARK_OPEN_EXCL(out);
    if (fd < 0) {
      if (errno == EEXIST) continue;
      return NULL;
    }
    FILE *f = ARK_FDOPEN(fd, "wb");
    if (!f) { ARK_CLOSE_FD(fd); remove(out); return NULL; }
    return f;
  }
  return NULL;
}

// Pre-signed object-storage URLs carry their own authz in the query
// string. The client never attaches any bearer token: storage auth is
// SigV4-only, so there is no secondary credential to leak.
static size_t url_host(const char *url, char *out, size_t out_cap) {
  if (!url || !out || out_cap == 0) return 0;
  const char *p = strstr(url, "://");
  if (!p) return 0;
  p += 3;
  const char *end = p;
  while (*end && *end != '/' && *end != ':' && *end != '@' &&
         *end != '?' && *end != '#') end++;
  const char *at = NULL;
  for (const char *q = p; q < end; q++) if (*q == '@') at = q;
  const char *hstart = at ? at + 1 : p;
  size_t hlen = (size_t)(end - hstart);
  if (hlen >= out_cap) hlen = out_cap - 1;
  memcpy(out, hstart, hlen);
  out[hlen] = '\0';
  return hlen;
}

// Storage-safe host: excludes link-local (169.254.0.0/16 and IPv6
// fe80::) AND ULA fc00::/7 (fd00::/8) because that range hosts the cloud
// instance-metadata service (IMDS at 169.254.169.254 on AWS/GCP/Azure and
// fd00:ec2::254 for AWS IMDSv2). A tampered manifest returning a
// snapshot/chunk URL pointing at IMDS would otherwise have the client
// download from the metadata service. Kept in lockstep with class.c's
// host_is_storage_safe.
static int host_is_storage_safe(const char *host) {
  if (!host || !*host) return 0;
  if (host[0] == '[') {
    if (strncmp(host, "[::1]", 5) == 0) return 1;
    // NO [fc / [fd ULA — AWS IMDSv2 is reachable at fd00:ec2::254 and
    // must never be treated as a safe storage destination. ULA prefixes
    // are NOT metadata-endpoint boundaries.
    return 0;
  }
  if (strcmp(host, "localhost") == 0) return 1;
  if (strncmp(host, "127.", 4) == 0) return 1;
  if (strncmp(host, "10.", 3) == 0) return 1;
  if (strncmp(host, "192.168.", 8) == 0) return 1;
  // NO 169.254. — IMDS (IPv4) excluded
  // NO fe80 — IPv6 link-local excluded
  // NO fc / fd ULA — AWS IMDSv2 (fd00:ec2::254) excluded
  if (strcmp(host, "::1") == 0) return 1;
  if (strncmp(host, "172.", 4) == 0) {
    unsigned second = 0;
    if (sscanf(host, "172.%u.", &second) == 1 && second >= 16 && second <= 31)
      return 1;
  }
  return 0;
}

static int host_has_suffix(const char *host, const char *suffix) {
  if (!host || !suffix) return 0;
  size_t hlen = strlen(host);
  size_t slen = strlen(suffix);
  if (hlen < slen) return 0;
  return strcmp(host + hlen - slen, suffix) == 0;
}
static int host_is_known_storage(const char *host) {
  if (!host || !*host) return 0;
  // Suffix-matched (not substring) so evil.amazonaws.com.evil.com is NOT accepted.
  if (host_has_suffix(host, ".amazonaws.com")) return 1;
  if (strcmp(host, "amazonaws.com") == 0) return 1;
  if (strcmp(host, "storage.googleapis.com") == 0) return 1;
  if (host_has_suffix(host, ".storage.googleapis.com")) return 1;
  if (host_has_suffix(host, ".blob.core.windows.net")) return 1;
  if (host_has_suffix(host, ".backblazeb2.com")) return 1;
  if (host_has_suffix(host, ".r2.cloudflarestorage.com")) return 1;
  if (host_has_suffix(host, ".wasabisys.com")) return 1;
  if (host_has_suffix(host, ".digitaloceanspaces.com")) return 1;
  return 0;
}

// SSRF guard: a tampered manifest or bucket listing returning a snapshot
// or chunk URL pointing at cloud metadata or an internal service would
// otherwise have the client download the wrong content over a valid
// pre-signed URL. Even though SHA-256 catches tampered CONTENT, refusing
// the host is the right posture — never let the client fetch from an
// untrusted storage host in the first place. Local addresses (MinIO on
// 127.0.0.1 / RFC1918) are allowed for dev/test.
static int url_is_allowed_storage(const char *url) {
  if (!url) return 0;
  char host[256];
  if (url_host(url, host, sizeof(host)) == 0) return 0;
  if (host_is_known_storage(host)) return 1;
  // host_is_storage_safe EXCLUDES link-local (169.254.x / fe80::) so
  // 169.254.169.254 (IMDS) is never an accepted storage destination.
  if (host_is_storage_safe(host)) return 1;
  const char *extra = getenv("ARKILIAN_STORAGE_HOSTS");
  if (extra && *extra) {
    char buf[1024];
    strncpy(buf, extra, sizeof(buf) - 1);
    buf[sizeof(buf) - 1] = '\0';
    char *save = NULL;
    char *tok = strtok_r(buf, ",", &save);
    while (tok) {
      while (*tok == ' ') tok++;
      size_t tlen = strlen(tok);
      if (tlen && tok[tlen - 1] == ' ') tok[--tlen] = '\0';
      if (tlen == 0) { tok = strtok_r(NULL, ",", &save); continue; }
      size_t hlen = strlen(host);
      if (strcmp(host, tok) == 0) return 1;
      if (hlen > tlen + 1 && host[hlen - tlen - 1] == '.' &&
          strcmp(host + hlen - tlen, tok) == 0) return 1;
      tok = strtok_r(NULL, ",", &save);
    }
  }
  return 0;
}

void hydration_remove_db_files(const char *db_path) {
  if (!db_path) return;
  char side[4096];
  remove(db_path);
  static const char *const suffixes[] = {"-wal", "-shm", "-journal", NULL};
  for (int i = 0; suffixes[i]; i++) {
    int n = snprintf(side, sizeof(side), "%s%s", db_path, suffixes[i]);
    if (n > 0 && (size_t)n < sizeof(side)) remove(side);
  }
}

// Remove ONLY the -wal/-shm/-journal sidecars of a database, leaving the
// main db file intact. Stale sidecars from the previous database must be
// cleared before installing a downloaded snapshot: replaying them into the
// new snapshot (which has a different page structure) silently corrupts
// it. Unlike hydration_remove_db_files, this preserves the main file so an
// atomic rename() can replace it — if the rename fails, the old database
// is still there and the local data is not destroyed.
static void hydration_remove_sidecars(const char *db_path) {
  if (!db_path) return;
  char side[4096];
  static const char *const suffixes[] = {"-wal", "-shm", "-journal", NULL};
  for (int i = 0; suffixes[i]; i++) {
    int n = snprintf(side, sizeof(side), "%s%s", db_path, suffixes[i]);
    if (n > 0 && (size_t)n < sizeof(side)) remove(side);
  }
}

// fsync the parent directory of `path` so the rename() that installed a
// snapshot is durable across a power loss. No-op on platforms without
// directory fsync. Best-effort: a failure is logged but does not abort,
// since the data is already written; only the rename's directory entry
// metadata could be at risk on an immediately-following crash.
// O_DIRECTORY is Linux-specific: absent on some POSIX builds, where
// opening the directory O_RDONLY alone is sufficient for fsync().
#ifndef O_DIRECTORY
#define O_DIRECTORY 0
#endif
static void fsync_parent_dir(const char *path) {
  if (!path) return;
  char dir[4096];
  strncpy(dir, path, sizeof(dir) - 1);
  dir[sizeof(dir) - 1] = '\0';
  char *slash = strrchr(dir, '/');
  if (!slash) return; // relative path in cwd — no directory to fsync
  if (slash == dir) slash[1] = '\0';
  else *slash = '\0';
#ifndef _WIN32
  int fd = open(dir, O_RDONLY | O_DIRECTORY);
  if (fd >= 0) {
    if (fsync(fd) != 0)
      fprintf(stderr, "arkilian: hydration directory fsync warning (%s): %s\n",
              dir, strerror(errno));
    close(fd);
  }
#else
  (void)0; // Windows fsync semantics differ; rename is atomic + durable
#endif
}

// ── libcurl response buffer ─────────────────────────────────────────

// Single-flight guard: arkilian_hydrate replaces the database file on
// disk (remove + rename). Two concurrent hydrates on the same db_path
// would truncate each other's temp file and race on rename(). A process-
// global mutex serializes all hydration calls — acceptable for a cold-
// restore path (the application must not have the DB open during hydrate).
// POSIX uses a statically-initialized pthread mutex; Windows has no
// static initializer for CRITICAL_SECTION, so it is created once via
// INIT_ONCE (the same one-time-init pattern class.c uses for libcurl).
#ifdef _WIN32
static INIT_ONCE g_hydrate_once = INIT_ONCE_STATIC_INIT;
static CRITICAL_SECTION g_hydrate_mutex;
static BOOL CALLBACK hydrate_mutex_init(PINIT_ONCE once, PVOID param,
                                        PVOID *ctx) {
  (void)once; (void)param; (void)ctx;
  InitializeCriticalSection(&g_hydrate_mutex);
  return TRUE;
}
#define HYDRATE_LOCK()                                                     \
  do {                                                                     \
    InitOnceExecuteOnce(&g_hydrate_once, hydrate_mutex_init, NULL, NULL);  \
    EnterCriticalSection(&g_hydrate_mutex);                                \
  } while (0)
#define HYDRATE_UNLOCK() LeaveCriticalSection(&g_hydrate_mutex)
#else
static pthread_mutex_t g_hydrate_mutex = PTHREAD_MUTEX_INITIALIZER;
#define HYDRATE_LOCK()   pthread_mutex_lock(&g_hydrate_mutex)
#define HYDRATE_UNLOCK() pthread_mutex_unlock(&g_hydrate_mutex)
#endif

struct curl_buf {
  uint8_t *data;
  size_t   len;
  size_t   cap;
};

static size_t curl_write_cb(void *ptr, size_t sz, size_t nmemb, void *user) {
  struct curl_buf *buf = (struct curl_buf *)user;
  size_t total = sz * nmemb;
  size_t needed = buf->len + total;
  if (needed > buf->cap) {
    size_t new_cap = buf->cap ? buf->cap * 2 : (total > 65536 ? total : 65536);
    if (new_cap < needed) new_cap = needed;
    uint8_t *p = realloc(buf->data, new_cap);
    if (!p) return 0;
    buf->data = p;
    buf->cap  = new_cap;
  }
  memcpy(buf->data + buf->len, ptr, total);
  buf->len = needed;
  return total;
}

// ── HTTP helpers ────────────────────────────────────────────────────

typedef struct {
  CURL           *handle;
  struct curl_slist *headers;
} HttpReq;

static int http_init(HttpReq *r, const char *url) {
  r->handle = curl_easy_init();
  if (!r->handle) return -1;
  r->headers = NULL;
  curl_easy_setopt(r->handle, CURLOPT_URL, url);
  curl_easy_setopt(r->handle, CURLOPT_WRITEFUNCTION, curl_write_cb);
  curl_easy_setopt(r->handle, CURLOPT_TIMEOUT, 120L);
  curl_easy_setopt(r->handle, CURLOPT_CONNECTTIMEOUT, 15L);
  // Do NOT follow redirects. The SSRF guard only inspects the INITIAL URL
  // host; a 302 to http://169.25.169.254/ would otherwise bypass it and
  // feed attacker-controlled bytes into hydration's SQL executor
  // (remote-code-execution chain). Presigned S3/GCS/R2 GET URLs never
  // 302 in practice — the object IS at the signed URL — so disabling
  // redirects closes the bypass with zero benign impact.
  curl_easy_setopt(r->handle, CURLOPT_FOLLOWLOCATION, 0L);
#if CURL_AT_LEAST_VERSION(7, 85, 0)
  curl_easy_setopt(r->handle, CURLOPT_REDIR_PROTOCOLS_STR, "http,https");
  curl_easy_setopt(r->handle, CURLOPT_PROTOCOLS_STR, "http,https");
#else
  curl_easy_setopt(r->handle, CURLOPT_REDIR_PROTOCOLS,
                   (long)(CURLPROTO_HTTP | CURLPROTO_HTTPS));
  curl_easy_setopt(r->handle, CURLOPT_PROTOCOLS,
                   (long)(CURLPROTO_HTTP | CURLPROTO_HTTPS));
#endif
  // Explicit TLS posture: system defaults are 1/2; setting them explicitly
  // documents intent and protects against a regression patch disabling
  // verification. Storage object bodies are additionally
  // content-authenticated via SHA-256.
  curl_easy_setopt(r->handle, CURLOPT_SSL_VERIFYPEER, 1L);
  curl_easy_setopt(r->handle, CURLOPT_SSL_VERIFYHOST, 2L);
  return 0;
}

static void http_free(HttpReq *r) {
  if (r->headers) curl_slist_free_all(r->headers);
  if (r->handle)  curl_easy_cleanup(r->handle);
}

// GET a URL into a malloc'd string.  Returns NULL on failure.
static char *http_get_string(const char *url, int *err_out) {
  HttpReq r;
  if (http_init(&r, url) != 0) { *err_out = HYDRATION_ERR_NET; return NULL; }

  struct curl_buf buf = {NULL, 0, 0};
  curl_easy_setopt(r.handle, CURLOPT_WRITEDATA, &buf);
  // Cap the response size: a chunk is replayable SQL text bounded by the
  // chunker (4 MB). A misbehaving/bucket-compromised object streaming
  // gigabytes would otherwise OOM this process before the SQL is parsed.
  curl_easy_setopt(r.handle, CURLOPT_MAXFILESIZE_LARGE, (curl_off_t)(256LL * 1024 * 1024));

  CURLcode rc = curl_easy_perform(r.handle);
  long http_code = 0;
  if (rc == CURLE_OK) curl_easy_getinfo(r.handle, CURLINFO_RESPONSE_CODE, &http_code);
  http_free(&r);

  if (rc != CURLE_OK || http_code != 200) {
    free(buf.data);
    if (http_code == 401 || http_code == 403) *err_out = HYDRATION_ERR_PROTO;
    else if (http_code == 404) *err_out = HYDRATION_ERR_NOTFOUND;
    else *err_out = HYDRATION_ERR_NET;
    return NULL;
  }

  // Null-terminate
  uint8_t *term = realloc(buf.data, buf.len + 1);
  if (!term) { free(buf.data); *err_out = HYDRATION_ERR_MEM; return NULL; }
  term[buf.len] = '\0';
  *err_out = 0;
  return (char *)term;
}

// libcurl write callback that streams straight to a FILE*
static size_t file_write_cb(void *ptr, size_t sz, size_t nmemb, void *user) {
  return fwrite(ptr, sz, nmemb, (FILE *)user);
}

// Download a binary file from a URL and atomically install it at
// local_path.  Streams to a unique staging file first; only on a complete
// 200 response + SHA-256 verification + SQLite quick_check are the old
// database's stale -wal/-shm sidecars replaced and the snapshot atomically
// installed.  A 404 maps to HYDRATION_ERR_NOTFOUND (cold start); any other
// failure leaves the existing local database untouched.
// `expected_sha256` is REQUIRED: a missing/empty digest is a hard refusal
// (HYDRATION_ERR_PROTO) — silently installing unauthenticated content is
// a downgrade-attack surface, not a back-compat feature. (An earlier
// revision of this docblock claimed a NULL digest "skips verification
// with a warning" — the code has refused for a long time; the docblock
// now matches the behavior.)
static int http_download_file(const char *url,
                               const char *local_path, const char *expected_sha256,
                               int *err_out) {
  // SSRF guard: never fetch a snapshot from a host that isn't an allowed
  // storage destination. A tampered manifest pointing at a
  // metadata-service URL has no path to the local DB even before content
  // auth runs.
  if (!url_is_allowed_storage(url)) {
    const char *q = strchr(url, '?');
    size_t host_len = q ? (size_t)(q - url) : strlen(url);
    if (host_len > 200) host_len = 200;
    fprintf(stderr,
            "arkilian: snapshot download refused — host is not an allowed "
            "storage destination (SSRF guard): %.*s\n", (int)host_len, url);
    *err_out = HYDRATION_ERR_PROTO;
    return -1;
  }
  HttpReq r;
  if (http_init(&r, url) != 0) { *err_out = HYDRATION_ERR_NET; return -1; }

  char tmp_path[4096];
  // Unique per-instance staging file: the fixed "<path>.arkdl" name was
  // shared across processes hydrating the same db path (two processes
  // would truncate each other's download and race the install).
  FILE *f = hydrate_unique_tmp(local_path, "arkdl", tmp_path, sizeof(tmp_path));
  if (!f) { http_free(&r); *err_out = HYDRATION_ERR_DISK; return -1; }

  curl_easy_setopt(r.handle, CURLOPT_WRITEFUNCTION, file_write_cb);
  curl_easy_setopt(r.handle, CURLOPT_WRITEDATA, f);

  CURLcode rc = curl_easy_perform(r.handle);
  long http_code = 0;
  if (rc == CURLE_OK) curl_easy_getinfo(r.handle, CURLINFO_RESPONSE_CODE, &http_code);
  http_free(&r);

  int io_failed = ferror(f);
  if (fflush(f) != 0) io_failed = 1;
  if (ARK_FSYNC(ARK_FILENO(f)) != 0) io_failed = 1;
  if (fclose(f) != 0) io_failed = 1;

  if (rc != CURLE_OK || http_code != 200 || io_failed) {
    remove(tmp_path);
    if (io_failed && rc == CURLE_OK && http_code == 200) *err_out = HYDRATION_ERR_DISK;
    else if (http_code == 404) *err_out = HYDRATION_ERR_NOTFOUND;
    else if (http_code == 401 || http_code == 403) *err_out = HYDRATION_ERR_PROTO;
    else *err_out = HYDRATION_ERR_NET;
    return -1;
  }

  // ── Content authentication (SHA-256) — run FIRST ─────────────────
  // A pre-signed GET URL authorizes WHO can read but not WHAT was stored
  // there: a leaked bucket-write credential lets an attacker swap the
  // object body. quick_check catches a malformed SQLite file, but a
  // valid-looking SQLite file with a different schema/contents would
  // still pass it. Verifying the downloaded bytes against the
  // manifest's recorded digest is the strongest available content guarantee,
  // so it runs BEFORE the structural quick_check — a tampered-but-valid
  // SQLite file is refused here. A missing digest is a HARD refusal: for
  // a cloud product, silently installing unauthenticated content is a
  // downgradeattack surface, not a back-compat feature.
  if (expected_sha256 && expected_sha256[0]) {
    char digest[65];
    if (ark_sha256_hex_file(tmp_path, digest) != 0) {
      fprintf(stderr, "arkilian: snapshot SHA-256 read failed — aborting\n");
      remove(tmp_path);
      *err_out = HYDRATION_ERR_DISK;
      return -1;
    }
    if (strcasecmp(digest, expected_sha256) != 0) {
      fprintf(stderr,
              "arkilian: snapshot SHA-256 MISMATCH — refusing to install "
              "(expected %.16s…, got %.16s…). Storage tampering or wrong "
              "snapshot served; local database untouched\n",
              expected_sha256, digest);
      remove(tmp_path);
      *err_out = HYDRATION_ERR_PROTO;
      return -1;
    }
  } else {
    fprintf(stderr,
            "arkilian: snapshot SHA-256 digest NOT provided by the "
            "manifest — refusing to install unauthenticated content "
            "(HYDRATION_ERR_PROTO)\n");
    remove(tmp_path);
    *err_out = HYDRATION_ERR_PROTO;
    return -1;
  }

  // ── Structural validation ────────────────────────────────────────
  // A 200 body that matches its declared digest can still be a non-SQLite
  // or corrupt file (a buggy uploader, a truncated write). The temp file
  // must open as a real SQLite database AND pass PRAGMA quick_check
  // (page-level corruption scan) before rename() is allowed.
  {
    sqlite3 *chk = NULL;
    int ok = 0;
    if (sqlite3_open_v2(tmp_path, &chk, SQLITE_OPEN_READONLY, NULL) == SQLITE_OK) {
      sqlite3_stmt *q = NULL;
      if (sqlite3_prepare_v2(chk, "PRAGMA quick_check", -1, &q, NULL) == SQLITE_OK &&
          sqlite3_step(q) == SQLITE_ROW) {
        const char *cell = (const char *)sqlite3_column_text(q, 0);
        ok = cell && strcmp(cell, "ok") == 0;
      }
      sqlite3_finalize(q);
      sqlite3_close(chk);
    }
    if (!ok) {
      fprintf(stderr,
              "arkilian: snapshot failed validation (not a clean SQLite "
              "database) — restore aborted, local database untouched\n");
      remove(tmp_path);
      *err_out = HYDRATION_ERR_DISK;
      return -1;
    }
  }

  // Success: install the validated snapshot atomically. POSIX rename()
  // atomically overwrites the existing db file, so the local database is
  // never in a half-installed state — either the old DB or the new one
  // exists at local_path at every instant. Stale -wal/-shm/-journal
  // sidecars from the PREVIOUS database are removed first (replaying them
  // into the new snapshot, which has a different page structure, would
  // corrupt it silently); the main file is left for the atomic replace.
  // If rename fails, the previous database is still intact (unlike the
  // previous remove-db-then-rename which destroyed it before the rename).
  hydration_remove_sidecars(local_path);
  if (rename(tmp_path, local_path) != 0) {
    fprintf(stderr,
            "arkilian: snapshot install rename failed (%s -> %s): %s; "
            "local database untouched\n",
            tmp_path, local_path, strerror(errno));
    remove(tmp_path);
    *err_out = HYDRATION_ERR_DISK;
    return -1;
  }
  fsync_parent_dir(local_path);

  *err_out = 0;
  return 0;
}

// ── Minimal JSON helpers (no external library) ──────────────────────
//
// These are deliberately small but CORRECT for the manifest
// contract: string-aware scanning (braces/brackets inside string
// values never confuse structure), full escape handling, and exact
// key matching at the top level of the object.

// Advance past a JSON string starting at p (which points at the
// opening quote), honoring backslash escapes.
static const char *json_skip_string(const char *p) {
  p++; // opening quote
  while (*p) {
    if (*p == '\\') { p += 2; continue; }
    if (*p == '"') return p + 1;
    p++;
  }
  return p;
}

// Locate a top-level key in a JSON object; returns a pointer to the
// start of its value (whitespace already skipped), or NULL.
static const char *json_find_key(const char *json, const char *key) {
  const char *p = json;
  while (*p && *p != '{') p++;
  if (!*p) return NULL;
  p++;
  int depth = 1;
  size_t klen = strlen(key);
  while (*p && depth > 0) {
    if (*p == '"') {
      const char *end = json_skip_string(p);
      if (depth == 1 && (size_t)(end - p - 2) == klen &&
          strncmp(p + 1, key, klen) == 0) {
        const char *q = end;
        while (*q == ' ' || *q == '\t' || *q == '\n' || *q == '\r') q++;
        if (*q == ':') {
          q++;
          while (*q == ' ' || *q == '\t' || *q == '\n' || *q == '\r') q++;
          return q;
        }
      }
      p = end;
      continue;
    }
    if (*p == '{' || *p == '[') depth++;
    else if (*p == '}' || *p == ']') depth--;
    p++;
  }
  return NULL;
}

// Encode a Unicode code point as UTF-8 (BMP only; surrogate halves
// that arrive unpaired become U+FFFD).
static size_t utf8_encode(unsigned cp, char *out) {
  if (cp < 0x80) { out[0] = (char)cp; return 1; }
  if (cp < 0x800) {
    out[0] = (char)(0xC0 | (cp >> 6));
    out[1] = (char)(0x80 | (cp & 0x3F));
    return 2;
  }
  if (cp >= 0xD800 && cp <= 0xDFFF) cp = 0xFFFD;
  out[0] = (char)(0xE0 | (cp >> 12));
  out[1] = (char)(0x80 | ((cp >> 6) & 0x3F));
  out[2] = (char)(0x80 | (cp & 0x3F));
  return 3;
}

static unsigned hex4(const char *p) {
  unsigned v = 0;
  for (int i = 0; i < 4; i++) {
    char c = p[i];
    v <<= 4;
    if (c >= '0' && c <= '9') v |= (unsigned)(c - '0');
    else if (c >= 'a' && c <= 'f') v |= (unsigned)(c - 'a' + 10);
    else if (c >= 'A' && c <= 'F') v |= (unsigned)(c - 'A' + 10);
    else return 0xFFFFFFFF; // invalid
  }
  return v;
}

// Presence check for the strict int parser: distinguishes "field absent
// (legacy manifest, default applies)" from "field present but malformed
// (protocol refusal)".
static int json_has_key(const char *json, const char *key) {
  return json_find_key(json, key) != NULL;
}

// Find a string value for a key in a flat JSON object: {"key":"value",...}
// Handles all JSON string escapes.  Returns malloc'd string or NULL.
char *json_get_string(const char *json, const char *key) {
  const char *p = json_find_key(json, key);
  if (!p || *p != '"') return NULL;
  p++;

  char *val = malloc(strlen(p) + 1); // output can only be shorter
  if (!val) return NULL;

  size_t out = 0;
  while (*p && *p != '"') {
    if (*p != '\\') { val[out++] = *p++; continue; }
    p++;
    switch (*p) {
      case 'n': val[out++] = '\n'; p++; break;
      case 't': val[out++] = '\t'; p++; break;
      case 'r': val[out++] = '\r'; p++; break;
      case 'b': val[out++] = '\b'; p++; break;
      case 'f': val[out++] = '\f'; p++; break;
      case '/': val[out++] = '/';  p++; break;
      case '\\': val[out++] = '\\'; p++; break;
      case '"': val[out++] = '"';  p++; break;
      case 'u': {
        unsigned cp = hex4(p + 1);
        if (cp == 0xFFFFFFFF) { free(val); return NULL; }
        p += 5; // past \uXXXX
        // Combine surrogate pairs when present
        if (cp >= 0xD800 && cp <= 0xDBFF && p[0] == '\\' && p[1] == 'u') {
          unsigned lo = hex4(p + 2);
          if (lo >= 0xDC00 && lo <= 0xDFFF) {
            cp = 0x10000 + ((cp - 0xD800) << 10) + (lo - 0xDC00);
            p += 6;
            // Encode 4-byte UTF-8
            val[out++] = (char)(0xF0 | (cp >> 18));
            val[out++] = (char)(0x80 | ((cp >> 12) & 0x3F));
            val[out++] = (char)(0x80 | ((cp >> 6) & 0x3F));
            val[out++] = (char)(0x80 | (cp & 0x3F));
            break;
          }
        }
        out += utf8_encode(cp, &val[out]);
        break;
      }
      default: free(val); return NULL; // strict: unknown escape is protocol error
    }
  }
  val[out] = '\0';
  return val;
}

int64_t json_get_int64(const char *json, const char *key) {
  int64_t v = 0;
  (void)json_get_int64_checked(json, key, &v);
  return v;
}

// Strict integer field parse for the manifest wire protocol. The old
// accessor was strtoll(..., NULL, 10) with no end-pointer validation, so
// "lsn_end":"500" (string), "lsn_end":5ab, or a truncated number parsed
// partially or as 0 instead of being rejected — and a missing field
// silently became 0, which the replay loop then SKIPS as already-applied.
// A manifest is a wire protocol, not trusted configuration: reject
// anything that is not a complete, delimiter-terminated integer token.
// Returns 0 and stores the value on success, -1 on absence/malformation.
int json_get_int64_checked(const char *json, const char *key, int64_t *out) {
  if (!json || !key || !out) return -1;
  const char *p = json_find_key(json, key);
  if (!p) return -1;
  errno = 0;
  char *end = NULL;
  long long v = strtoll(p, &end, 10);
  if (end == p || errno == ERANGE) return -1;
  while (*end == ' ' || *end == '\t' || *end == '\n' || *end == '\r') end++;
  if (*end != '\0' && *end != ',' && *end != '}' && *end != ']') return -1;
  *out = (int64_t)v;
  return 0;
}

// Return the end of the array element starting at p: the first comma
// or closing bracket at the element's own nesting level, skipping
// string contents entirely.
static const char *json_array_elem_end(const char *p) {
  int depth = 0;
  while (*p) {
    if (*p == '"') { p = json_skip_string(p); continue; }
    if (*p == '{' || *p == '[') depth++;
    else if (*p == '}' || *p == ']') {
      if (depth == 0) return p;
      depth--;
    } else if (*p == ',' && depth == 0) {
      return p;
    }
    p++;
  }
  return p;
}

// Count elements in a JSON array at the given key: {"key":[{...},{...}]}
int json_array_count(const char *json, const char *key) {
  const char *p = json_find_key(json, key);
  if (!p || *p != '[') return 0;
  p++;
  int count = 0;
  for (;;) {
    while (*p == ' ' || *p == '\t' || *p == '\n' || *p == '\r') p++;
    if (!*p || *p == ']') break;
    count++;
    p = json_array_elem_end(p);
    if (*p == ',') p++;
    else break;
  }
  return count;
}

// Return a malloc'd copy of the raw {...} object value for `key` at the
// top level of `json`, or NULL if the key is absent or is not an object.
// json_find_key only matches at depth 1, so nested fields (the v3
// manifest's "snapshot":{"s3_key",...}) are invisible to the flat
// accessors — the caller extracts the object first, then re-scans it.
char *json_object_get(const char *json, const char *key) {
  const char *p = json_find_key(json, key);
  if (!p || *p != '{') return NULL;
  int depth = 0;
  const char *q = p;
  while (*q) {
    if (*q == '"') { q = json_skip_string(q); continue; }
    if (*q == '{' || *q == '[') depth++;
    else if (*q == '}' || *q == ']') {
      depth--;
      if (depth == 0) { q++; break; }
    }
    q++;
  }
  if (depth != 0) return NULL;  // truncated / malformed object
  size_t len = (size_t)(q - p);
  char *copy = malloc(len + 1);
  if (!copy) return NULL;
  memcpy(copy, p, len);
  copy[len] = '\0';
  return copy;
}

// Parse the i-th element from a JSON array at key: {"key":[{...},{...}]}
// Returns malloc'd copy of the i-th element, or NULL if out of range.
char *json_array_get(const char *json, const char *key, int index) {
  const char *p = json_find_key(json, key);
  if (!p || *p != '[' || index < 0) return NULL;
  p++;
  for (int i = 0;; i++) {
    while (*p == ' ' || *p == '\t' || *p == '\n' || *p == '\r') p++;
    if (!*p || *p == ']') return NULL;
    const char *end = json_array_elem_end(p);
    if (i == index) {
      while (end > p && isspace((unsigned char)end[-1])) end--;
      size_t len = (size_t)(end - p);
      char *copy = malloc(len + 1);
      if (!copy) return NULL;
      memcpy(copy, p, len);
      copy[len] = '\0';
      return copy;
    }
    if (*end == ',') { p = end + 1; continue; }
    return NULL;
  }
}

// Forward decl: ark_manifest_fetch presigns the manifest GET; the SigV4
// signer is defined further down in this file.
static char *s3_presign_get(const char *endpoint, const char *bucket,
                             const char *region, const char *access_key,
                             const char *secret_key, const char *key,
                             long expires_sec);

// ── Manifest registry reader ────────────────────────────────────────
// The manifest is maintained by the shipping side (src/class.c); this
// reader turns it into a replay plan. Exported so class.c can seed its
// in-memory registry from the last known manifest at init.

int ark_manifest_fetch(const char *endpoint, const char *bucket,
                        const char *region, const char *access_key,
                        const char *secret_key, const char *prefix,
                        HydratePlan *plan) {
  if (!endpoint || !bucket || !access_key || !secret_key || !prefix || !plan)
    return -1;

  char manifest_key[512];
  snprintf(manifest_key, sizeof(manifest_key), "%s/manifest.json", prefix);

  char *manifest_url = s3_presign_get(endpoint, bucket, region,
                                       access_key, secret_key,
                                       manifest_key, 3600L);
  if (!manifest_url) return -1;

  int err = 0;
  char *json = http_get_string(manifest_url, &err);
  if (!json) {
    // Propagate the specific failure so callers can tell "no manifest yet"
    // (a genuine cold start) from a transient error that must be retried —
    // publishing a registry over an unreadable manifest would orphan the
    // predecessor's chunks.
    free(manifest_url);
    if (err == HYDRATION_ERR_NOTFOUND) return HYDRATION_ERR_NOTFOUND;
    return err ? err : HYDRATION_ERR_NET;
  }

  // ── Version contract ────────────────────────────────────────────────
  // The v3 manifest is the only layout this reader writes. A present
  // version field that is not 3 is a protocol refusal, not a parse
  // mystery; an ABSENT version means a legacy (v1/v2 flat-form) writer —
  // accepted as before for back-compat.
  {
    int64_t version = 0;
    if (json_get_int64_checked(json, "version", &version) == 0 &&
        version != 3) {
      fprintf(stderr,
              "arkilian: unsupported manifest version %lld (expected 3)\n",
              (long long)version);
      free(json);
      free(manifest_url);
      return HYDRATION_ERR_PROTO;
    }
  }

  // ── Manifest authenticity (HMAC over the exact manifest bytes) ──────
  // The manifest is the ROOT OF TRUST: digests cannot authenticate it
  // (circular) and the restore path EXECUTES its SQL. Object SHA-256 only
  // proves "object matches manifest" — bucket compromise controls both.
  // ARKILIAN_MANIFEST_HMAC_KEY is REQUIRED (fail closed, no legacy). The
  // key MUST live outside the storage it protects (env/secret manager,
  // never bucket). Missing/invalid manifest.sig is HYDRATION_ERR_PROTO.
  {
    const char *hmac_key = getenv("ARKILIAN_MANIFEST_HMAC_KEY");
    if (!hmac_key || !hmac_key[0]) {
      fprintf(stderr,
              "arkilian: ARKILIAN_MANIFEST_HMAC_KEY not configured — "
              "refusing unauthenticated manifest (no legacy)\n");
      free(json);
      free(manifest_url);
      return HYDRATION_ERR_PROTO;
    }
    char sig_key[512];
    snprintf(sig_key, sizeof(sig_key), "%s/manifest.sig", prefix);
    int verified = 0;
    for (int attempt = 0; attempt < 3 && !verified; attempt++) {
        if (attempt > 0) {
          // Publishing is manifest-PUT then sig-PUT: a signature mismatch
          // can be a torn read of that two-object commit. Re-fetch BOTH
          // before refusing so a normal publish race is not reported as
          // tampering.
          sqlite3_sleep(300);
          free(json);
          json = http_get_string(manifest_url, &err);
          if (!json) {
            fprintf(stderr,
                    "arkilian: manifest re-fetch failed during signature "
                    "verification (err=%d)\n", err);
            free(manifest_url);
            return err ? err : HYDRATION_ERR_NET;
          }
        }
        char *sig_url = s3_presign_get(endpoint, bucket, region,
                                       access_key, secret_key,
                                       sig_key, 3600L);
        if (!sig_url) {
          free(json);
          free(manifest_url);
          return HYDRATION_ERR_MEM;
        }
        int sig_err = 0;
        char *sig = http_get_string(sig_url, &sig_err);
        free(sig_url);
        if (!sig) {
          if (sig_err == HYDRATION_ERR_NOTFOUND) {
            fprintf(stderr,
                    "arkilian: manifest.sig MISSING but "
                    "ARKILIAN_MANIFEST_HMAC_KEY is configured — refusing to "
                    "trust an unauthenticated manifest\n");
          } else {
            fprintf(stderr, "arkilian: manifest.sig fetch failed (err=%d)\n",
                    sig_err);
          }
          free(json);
          free(manifest_url);
          return sig_err == HYDRATION_ERR_NOTFOUND ? HYDRATION_ERR_PROTO
                 : (sig_err ? sig_err : HYDRATION_ERR_NET);
        }
        // Trim surrounding whitespace, then compare (64 hex chars).
        char *b = sig;
        char *e = sig + strlen(sig);
        while (b < e && isspace((unsigned char)*b)) b++;
        while (e > b && isspace((unsigned char)e[-1])) e--;
        *e = '\0';
        char expect[65];
        ark_hmac_sha256_hex((const uint8_t *)hmac_key, strlen(hmac_key),
                            json, strlen(json), expect);
        verified = (strlen(b) == 64 && strcasecmp(b, expect) == 0);
        free(sig);
        if (!verified && attempt == 2) {
          fprintf(stderr,
                  "arkilian: manifest HMAC MISMATCH — refusing a manifest "
                  "that does not verify under ARKILIAN_MANIFEST_HMAC_KEY "
                  "(tampered storage, wrong key, or torn publish)\n");
        }
      }
      if (!verified) {
        free(json);
        free(manifest_url);
        return HYDRATION_ERR_PROTO;
      }
    }
  free(manifest_url);

  memset(plan, 0, sizeof(*plan));
  plan->baseline_lsn = 0;

  // The v3 manifest (written by manifest_registry_upload in class.c) nests
  // the baseline snapshot in an OBJECT:
  //   "snapshot":{"s3_key":"...","sha256":"...","baseline_lsn":N}
  // json_find_key only matches at depth 1, so the object is extracted
  // first and then re-scanned with the same accessors. An empty s3_key
  // means no snapshot has been published yet (chunks-only registry) —
  // hydration cold-starts and replays from LSN 1.
  char *snap = json_object_get(json, "snapshot");
  if (!snap) snap = json_array_get(json, "snapshot", 0); // legacy array form
  if (snap) {
    char *s3_key = json_get_string(snap, "s3_key");
    char *sha    = json_get_string(snap, "sha256");
    if (!sha)    sha = json_get_string(snap, "snapshot_sha256"); // legacy
    // Strict baseline parse: malformed (quoted/truncated) is a protocol
    // refusal; ABSENT is tolerated as 0 (legacy manifests predate it).
    {
      int64_t bl = 0;
      if (json_get_int64_checked(snap, "baseline_lsn", &bl) != 0) {
        if (json_has_key(snap, "baseline_lsn")) {
          fprintf(stderr,
                  "arkilian: manifest snapshot.baseline_lsn is malformed — "
                  "refusing manifest\n");
          free(s3_key); free(sha); free(snap); free(json);
          hydrate_plan_free(plan);
          return HYDRATION_ERR_PROTO;
        }
        bl = 0;
      }
      plan->baseline_lsn = bl;
    }
    if (s3_key && s3_key[0]) {
      plan->snapshot_s3_key = s3_key;
      // The snapshot is fetched over a locally presigned GET: the manifest
      // never carries credentials or a remote-issued URL.
      plan->snapshot_url = s3_presign_get(endpoint, bucket, region,
                                          access_key, secret_key,
                                          s3_key, 3600L);
      if (!plan->snapshot_url) {
        fprintf(stderr, "arkilian: failed to presign snapshot GET\n");
        free(s3_key); free(sha); free(snap); free(json);
        hydrate_plan_free(plan);
        return HYDRATION_ERR_MEM;
      }
    } else {
      free(s3_key);
    }
    if (sha && sha[0]) plan->snapshot_sha256 = sha;
    else free(sha);
    free(snap);
  } else {
    // Legacy flat form: fields at the top level of the document.
    plan->snapshot_url    = json_get_string(json, "snapshot_url");
    plan->snapshot_s3_key = json_get_string(json, "s3_key");
    plan->snapshot_sha256 = json_get_string(json, "snapshot_sha256");
    plan->baseline_lsn    = json_get_int64(json, "baseline_lsn");
  }

  plan->chunk_count = json_array_count(json, "chunks");
  if (plan->chunk_count > 0) {
    plan->chunks = calloc((size_t)plan->chunk_count, sizeof(HydrateChunk));
    if (plan->chunks) {
      for (int i = 0; i < plan->chunk_count; i++) {
        char *elem = json_array_get(json, "chunks", i);
        if (!elem) continue;
        char *ckey = json_get_string(elem, "s3_key");
        if (ckey) {
          plan->chunks[i].s3_key = ckey;
          plan->chunks[i].url = s3_presign_get(endpoint, bucket, region,
                                                access_key, secret_key,
                                                ckey, 3600L);
        }
        plan->chunks[i].sha256 = json_get_string(elem, "sha256");
        // LSN values are JSON numbers in the v3 manifest (see
        // manifest_registry_upload) — parse them STRICTLY: a missing or
        // malformed field used to silently parse as 0, which the replay
        // loop then skipped as "already applied" (silent data omission).
        // It is now a protocol refusal.
        {
          int64_t ls = 0, le = 0;
          if (json_get_int64_checked(elem, "lsn_start", &ls) != 0 ||
              json_get_int64_checked(elem, "lsn_end", &le) != 0) {
            fprintf(stderr,
                    "arkilian: chunk %d has missing/malformed LSN fields — "
                    "refusing manifest\n", i);
            free(elem);
            hydrate_plan_free(plan);
            free(json);
            return HYDRATION_ERR_PROTO;
          }
          plan->chunks[i].lsn_start = ls;
          plan->chunks[i].lsn_end   = le;
        }
        free(elem);
      }
    }
  }

  plan->expires_at = (int64_t)time(NULL) + 3600;

  free(json);
  // A manifest without a snapshot entry (published by the chunk flusher
  // before the first hourly snapshot) is still a valid registry: the
  // chunk list alone can restore a fresh database.
  if (!plan->snapshot_url && plan->chunk_count == 0) {
    // The object EXISTS (we got a 200) but carries nothing usable. That is
    // never a state this client writes — the flusher only publishes when it
    // has a chunk record and the snapshot thread always names a snapshot —
    // so it means a corrupt or foreign manifest. Report PROTO, NOT
    // NOTFOUND: the shipping side must freeze its registry rather than
    // publish over records it could not read (which would orphan objects
    // whose outbox rows are already deleted).
    fprintf(stderr,
            "arkilian: manifest is present but carries no snapshot and no "
            "chunks — refusing to treat it as a cold start\n");
    hydrate_plan_free(plan);
    return HYDRATION_ERR_PROTO;
  }
  return 0;
}

void hydrate_plan_free(HydratePlan *plan) {
  if (!plan) return;
  free(plan->snapshot_url);
  free(plan->snapshot_s3_key);
  free(plan->snapshot_sha256);
  for (int i = 0; i < plan->chunk_count; i++) {
    free(plan->chunks[i].url);
    free(plan->chunks[i].s3_key);
    free(plan->chunks[i].sha256);
  }
  free(plan->chunks);
  memset(plan, 0, sizeof(*plan));
}

// ── Wire-protocol validation of a parsed plan ───────────────────────
// The manifest decides what database content gets restored, so it is
// validated like a wire protocol, not like trusted configuration:
// required fields, sane ranges, strict ordering (no overlaps, no
// duplicates, no unsorted entries — the previous gap-only check accepted
// overlapping ranges), digest shape, and baseline sanity. Any violation
// is HYDRATION_ERR_PROTO before a byte is downloaded.
static int hydrate_validate_plan(const HydratePlan *plan) {
  if (!plan) return HYDRATION_ERR_PROTO;
  if (plan->baseline_lsn < 0) return HYDRATION_ERR_PROTO;
  // A recorded snapshot must always carry a digest (the installer hard-
  // refuses without one — make the refusal happen before any download).
  if (plan->snapshot_s3_key && plan->snapshot_s3_key[0] &&
      !(plan->snapshot_sha256 && plan->snapshot_sha256[0]))
    return HYDRATION_ERR_PROTO;
  int64_t prev_end = plan->baseline_lsn;
  for (int i = 0; i < plan->chunk_count; i++) {
    const HydrateChunk *ch = &plan->chunks[i];
    if (!ch->s3_key || !ch->s3_key[0]) return HYDRATION_ERR_PROTO;
    if (!ch->url) return HYDRATION_ERR_PROTO;
    if (ch->lsn_start < 1 || ch->lsn_end < ch->lsn_start)
      return HYDRATION_ERR_PROTO;
    // Strict sequence: each chunk starts strictly after the previous one
    // ends — no overlaps, no duplicates, no unsorted entries.
    if (i > 0 && ch->lsn_start <= prev_end) return HYDRATION_ERR_PROTO;
    if (!ch->sha256 || strlen(ch->sha256) != 64)
      return HYDRATION_ERR_PROTO;
    for (const char *h = ch->sha256; *h; h++)
      if (!isxdigit((unsigned char)*h)) return HYDRATION_ERR_PROTO;
    prev_end = ch->lsn_end;
  }
  return 0;
}

// ── Step 1: Download & decompress snapshot ──────────────────────────

// Cold-start initialization shared by "snapshot 404" and "manifest has
// no snapshot yet": create a clean local database with the hydration
// meta table so chunk replay can proceed from LSN 0.
static int hydration_cold_start(const char *db_path) {
  struct stat st;
  if (stat(db_path, &st) != 0) {
    // No local DB: clear any orphaned sidecars from a previously
    // deleted database, then create a fresh one.
    hydration_remove_db_files(db_path);
  }
  sqlite3 *db = NULL;
  if (sqlite3_open_v2(db_path, &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_FULLMUTEX, NULL) == SQLITE_OK) {
    char *perr = NULL;
    int prc = sqlite3_exec(db,
        "CREATE TABLE IF NOT EXISTS _arkilian_meta (k TEXT PRIMARY KEY, v TEXT);",
        NULL, NULL, &perr);
    if (prc != SQLITE_OK) {
      fprintf(stderr, "arkilian: cold-start meta init failed: %s\n",
              perr ? perr : sqlite3_errmsg(db));
      sqlite3_free(perr);
      sqlite3_close(db);
      return HYDRATION_ERR_SQL;
    }
    sqlite3_close(db);
    return 0;
  }
  return HYDRATION_ERR_DISK;
}

static int download_snapshot(const char *snapshot_url,
                              const char *db_path, const char *expected_sha256,
                              hydration_progress_cb progress, void *user) {
  if (progress) progress(1, 0, 1, user);

  int err = 0;
  int rc = http_download_file(snapshot_url, db_path, expected_sha256, &err);
  if (rc != 0) {
    // Only a genuine 404 means "no baseline snapshot uploaded yet"
    // (cold start).  Every other failure — network blip, expired URL,
    // disk full — is a real error and MUST NOT be masked as success,
    // otherwise hydration would replay chunks onto an empty database
    // and report a silently incomplete restore as OK.
    if (err != HYDRATION_ERR_NOTFOUND) return err;

    // Cold start: initialize a clean target so chunks replay from
    // LSN 0.  Never destroy an existing local database in the process.
    int cs = hydration_cold_start(db_path);
    if (cs != 0) return cs;
    if (progress) progress(1, 1, 1, user);
    return 0;
  }

  if (progress) progress(1, 1, 1, user);
  return 0;
}

// ── Step 2: Read local last_applied_lsn ─────────────────────────────

static int64_t read_last_applied_lsn(sqlite3 *db) {
  sqlite3_stmt *stmt = NULL;
  int rc = sqlite3_prepare_v2(db,
    "SELECT v FROM _arkilian_meta WHERE k = 'last_applied_lsn'",
    -1, &stmt, NULL);
  if (rc != SQLITE_OK) return 0;

  int64_t lsn = 0;
  if (sqlite3_step(stmt) == SQLITE_ROW)
    lsn = sqlite3_column_int64(stmt, 0);
  sqlite3_finalize(stmt);
  return lsn;
}

// ── Step 3: Replay a single chunk ───────────────────────────────────

// Hydration authorizer: the remote SQL is untrusted (it came from S3 via a
// manifest that may have been forged). Even after SHA-256 content auth, the
// SQL text itself must be restricted to the replication format's allowlist.
// This prevents an attacker who somehow bypasses content auth (or a bug)
// from executing arbitrary ATTACH, PRAGMA, virtual-table, or extension
// operations on the hydration connection.
static int hydration_authorizer(void *pUser, int action,
                                const char *arg1, const char *arg2,
                                const char *dbName, const char *inner) {
  (void)pUser; (void)arg2; (void)dbName; (void)inner;
  switch (action) {
    case SQLITE_INSERT: // REPLACE is INSERT with conflict handling
    case SQLITE_DELETE:
    case SQLITE_UPDATE:
    case SQLITE_CREATE_TABLE:
    case SQLITE_DROP_TABLE:
    case SQLITE_ALTER_TABLE:
    case SQLITE_CREATE_INDEX:
    case SQLITE_DROP_INDEX:
    case SQLITE_CREATE_TRIGGER:
    case SQLITE_DROP_TRIGGER:
    case SQLITE_TRANSACTION:
    case SQLITE_SAVEPOINT:
    case SQLITE_READ:
    case SQLITE_SELECT:
      return SQLITE_OK;
    // Explicitly denied: PRAGMA changes, virtual tables, ATTACH/DETACH,
    // reindex, analyze, etc. Hydration's own PRAGMAs (journal_mode,
    // etc.) are executed outside the authorizer-protected replay. The
    // replication payload uses quote() for deterministic expansion, so
    // allow it (and a few other safe scalar functions that may appear in
    // DEFAULT values).
    case SQLITE_PRAGMA:
    case SQLITE_ATTACH:
    case SQLITE_DETACH:
    case SQLITE_CREATE_VTABLE:
    case SQLITE_DROP_VTABLE:
    case SQLITE_REINDEX:
    case SQLITE_ANALYZE:
      fprintf(stderr, "arkilian: hydration authorizer DENY action=%d arg1=%.80s\n",
              action, arg1 ? arg1 : "(null)");
      return SQLITE_DENY;
    case SQLITE_FUNCTION:
      // For SQLITE_FUNCTION, arg2 is the function name (arg1 is NULL per
      // sqlite3.c:3824). Allow the safe scalar functions the replication
      // payload may use (quote for deterministic expansion, plus a few
      // others that appear in DEFAULT values and aggregate queries that
      // may be part of the payload).
      if (arg2 && (strcasecmp(arg2, "quote") == 0 ||
                   strcasecmp(arg2, "hex") == 0 ||
                   strcasecmp(arg2, "random") == 0 ||
                   strcasecmp(arg2, "datetime") == 0 ||
                   strcasecmp(arg2, "strftime") == 0 ||
                   strcasecmp(arg2, "abs") == 0 ||
                   strcasecmp(arg2, "replace") == 0 ||
                   strcasecmp(arg2, "substr") == 0 ||
                   strcasecmp(arg2, "count") == 0 ||
                   strcasecmp(arg2, "sum") == 0 ||
                   strcasecmp(arg2, "avg") == 0 ||
                   strcasecmp(arg2, "min") == 0 ||
                   strcasecmp(arg2, "max") == 0)) {
        return SQLITE_OK;
      }
      fprintf(stderr, "arkilian: hydration authorizer DENY FUNCTION %.80s\n",
              arg2 ? arg2 : "(null)");
      return SQLITE_DENY;
    default:
      // arg1 often carries the SQL or object name – useful for forensics.
      fprintf(stderr, "arkilian: hydration authorizer DENY action=%d arg1=%.80s\n",
              action, arg1 ? arg1 : "(null)");
      return SQLITE_DENY;
  }
}

int hydrate_replay_chunk(sqlite3 *db, const char *raw_sql, int64_t chunk_lsn) {
  char *err_msg = NULL;

  // ── Trigger isolation (defense in depth) ──────────────────────────
  // Even if the outer hydration loop already disabled triggers via
  // SQLITE_DBCONFIG_ENABLE_TRIGGER, a standalone call to this helper
  // (tests, tooling) must also not fire customer or capture triggers.
  // Disabling here is idempotent and restores the prior state on exit.
  int replay_old_trigger = 1;
#ifdef SQLITE_DBCONFIG_ENABLE_TRIGGER
  sqlite3_db_config(db, SQLITE_DBCONFIG_ENABLE_TRIGGER, 0, &replay_old_trigger);
#endif

  // Ensure the metadata table exists (defensive, in case the snapshot
  // did not contain it and the caller did not create it).
  sqlite3_exec(db, "CREATE TABLE IF NOT EXISTS _arkilian_meta (k TEXT PRIMARY KEY, v TEXT);", NULL, NULL, NULL);

  // Install the authorizer before touching untrusted SQL. It stays
  // installed for the whole replay transaction and is cleared on exit.
  sqlite3_set_authorizer(db, hydration_authorizer, NULL);

  // 1. Begin explicit transaction for throughput
  int rc = sqlite3_exec(db, "BEGIN TRANSACTION;", NULL, NULL, &err_msg);
  if (rc != SQLITE_OK) {
    if (err_msg) sqlite3_free(err_msg);
    sqlite3_set_authorizer(db, NULL, NULL);
#ifdef SQLITE_DBCONFIG_ENABLE_TRIGGER
    sqlite3_db_config(db, SQLITE_DBCONFIG_ENABLE_TRIGGER, replay_old_trigger, NULL);
#endif
    return HYDRATION_ERR_SQL;
  }

  // 2. Replay the raw multi-statement SQL text
  rc = sqlite3_exec(db, raw_sql, NULL, NULL, &err_msg);
  if (rc != SQLITE_OK) {
    if (err_msg) sqlite3_free(err_msg);
    sqlite3_exec(db, "ROLLBACK;", NULL, NULL, NULL);
    sqlite3_set_authorizer(db, NULL, NULL);
#ifdef SQLITE_DBCONFIG_ENABLE_TRIGGER
    sqlite3_db_config(db, SQLITE_DBCONFIG_ENABLE_TRIGGER, replay_old_trigger, NULL);
#endif
    return HYDRATION_ERR_SQL;
  }

  // 3. Update metadata tracking
  char meta_sql[256];
  snprintf(meta_sql, sizeof(meta_sql),
    "INSERT OR REPLACE INTO _arkilian_meta (k, v) VALUES ('last_applied_lsn', '%lld');",
    (long long)chunk_lsn);
  rc = sqlite3_exec(db, meta_sql, NULL, NULL, NULL);
  if (rc != SQLITE_OK) {
    sqlite3_exec(db, "ROLLBACK;", NULL, NULL, NULL);
    sqlite3_set_authorizer(db, NULL, NULL);
#ifdef SQLITE_DBCONFIG_ENABLE_TRIGGER
    sqlite3_db_config(db, SQLITE_DBCONFIG_ENABLE_TRIGGER, replay_old_trigger, NULL);
#endif
    return HYDRATION_ERR_SQL;
  }

  rc = sqlite3_exec(db, "COMMIT;", NULL, NULL, NULL);
  sqlite3_set_authorizer(db, NULL, NULL);
#ifdef SQLITE_DBCONFIG_ENABLE_TRIGGER
  sqlite3_db_config(db, SQLITE_DBCONFIG_ENABLE_TRIGGER, replay_old_trigger, NULL);
#endif
  if (rc != SQLITE_OK) {
    // Best effort: don't leave an open transaction behind.
    sqlite3_exec(db, "ROLLBACK;", NULL, NULL, NULL);
    return HYDRATION_ERR_SQL;
  }
  return 0;
}


static int is_unreserved_s3(char c) {
  return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') ||
         (c >= '0' && c <= '9') || c == '-' || c == '_' ||
         c == '.' || c == '~';
}

static void s3_url_encode(const char *src, char *dst, size_t cap) {
  if (!src || !dst || cap == 0) return;
  size_t di = 0;
  for (const unsigned char *p = (const unsigned char *)src; *p && di + 4 < cap; p++) {
    if (is_unreserved_s3((char)*p)) {
      dst[di++] = (char)*p;
    } else {
      snprintf(dst + di, cap - di, "%%%02X", *p);
      di += 3;
    }
  }
  dst[di] = '\0';
}

// Portable UTC broken-down time: gmtime_r (POSIX) vs gmtime_s (Windows —
// REVERSED argument order, errno_t return). Writes to *out and returns
// out on success, NULL on failure.
static struct tm *ark_gmtime_utc(const time_t *tp, struct tm *out) {
#ifdef _WIN32
  return (gmtime_s(out, tp) == 0) ? out : NULL;
#else
  return gmtime_r(tp, out);
#endif
}

static char *s3_presign_get(const char *endpoint, const char *bucket,
                             const char *region, const char *access_key,
                             const char *secret_key, const char *key,
                             long expires_sec) {
  if (!endpoint || !bucket || !access_key || !secret_key || !key) return NULL;
  if (!region) region = "us-east-1";

  time_t now = time(NULL);
  struct tm g;
  if (!ark_gmtime_utc(&now, &g)) return NULL;
  char date_stamp[9], amz_date[17];
  strftime(date_stamp, sizeof(date_stamp), "%Y%m%d", &g);
  strftime(amz_date, sizeof(amz_date), "%Y%m%dT%H%M%SZ", &g);

  char cred_plain[512];
  snprintf(cred_plain, sizeof(cred_plain), "%s/%s/%s/s3/aws4_request",
           access_key, date_stamp, region);
  char cred_enc[2048];
  s3_url_encode(cred_plain, cred_enc, sizeof(cred_enc));

  const char *scheme = "https";
  const char *host = endpoint;
  if (strncmp(host, "https://", 8) == 0) host += 8;
  else if (strncmp(host, "http://", 7) == 0) { scheme = "http"; host += 7; }
  char host_clean[256];
  {
    size_t hl = strlen(host);
    if (hl >= sizeof(host_clean)) hl = sizeof(host_clean) - 1;
    memcpy(host_clean, host, hl);
    while (hl > 0 && host_clean[hl - 1] == '/') hl--;
    host_clean[hl] = '\0';
  }

  char key_enc[1024];
  s3_url_encode(key, key_enc, sizeof(key_enc));
  for (char *p = key_enc; *p; p++) {
    if (p[0] == '%' && p[1] == '2' && (p[2] == 'F' || p[2] == 'f')) {
      *p = '/'; memmove(p + 1, p + 3, strlen(p + 3) + 1);
    }
  }

  char canonical[4096];
  snprintf(canonical, sizeof(canonical),
    "GET\n/%s/%s\n"
    "X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=%s&"
    "X-Amz-Date=%s&X-Amz-Expires=%ld&X-Amz-SignedHeaders=host\n"
    "host:%s\n\nhost\nUNSIGNED-PAYLOAD",
    bucket, key_enc, cred_enc, amz_date, expires_sec, host_clean);

  char scope[256];
  snprintf(scope, sizeof(scope), "%s/%s/s3/aws4_request", date_stamp, region);
  char canon_hash[65];
  ark_sha256_hex(canonical, strlen(canonical), canon_hash);

  char sts[1024];
  snprintf(sts, sizeof(sts), "AWS4-HMAC-SHA256\n%s\n%s\n%s",
           amz_date, scope, canon_hash);

  uint8_t k_date[32], k_region[32], k_service[32], k_signing[32];
  char kseed[512];
  snprintf(kseed, sizeof(kseed), "AWS4%s", secret_key);
  ark_hmac_sha256((const uint8_t *)kseed, strlen(kseed),
                  date_stamp, strlen(date_stamp), k_date);
  ark_hmac_sha256(k_date, 32, region, strlen(region), k_region);
  ark_hmac_sha256(k_region, 32, "s3", 2, k_service);
  ark_hmac_sha256(k_service, 32, "aws4_request", 12, k_signing);

  char sig_hex[65];
  ark_hmac_sha256_hex(k_signing, 32, sts, strlen(sts), sig_hex);
  char sig_enc[256];
  s3_url_encode(sig_hex, sig_enc, sizeof(sig_enc));

  size_t url_len = strlen(scheme) + 3 + strlen(host_clean) + 1 +
                   strlen(bucket) + 1 + strlen(key_enc) + 2048;
  char *url = malloc(url_len);
  if (!url) return NULL;
  snprintf(url, url_len,
    "%s://%s/%s/%s?X-Amz-Algorithm=AWS4-HMAC-SHA256"
    "&X-Amz-Credential=%s&X-Amz-Date=%s&X-Amz-Expires=%ld"
    "&X-Amz-SignedHeaders=host&X-Amz-Signature=%s",
    scheme, host_clean, bucket, key_enc,
    cred_enc, amz_date, expires_sec, sig_enc);

  return url;
}

int arkilian_hydrate_s3(const char *db_path,
                         const char *s3_endpoint,
                         const char *s3_bucket,
                         const char *s3_region,
                         const char *s3_access_key,
                         const char *s3_secret_key,
                         const char *s3_prefix,
                         hydration_progress_cb progress,
                         void *user_data) {
  if (!db_path) return HYDRATION_ERR_PROTO;

  HYDRATE_LOCK();
  int hydrate_result = 0;

  // ── Restore exclusivity (OS lock, P0) ──────────────────────────────
  // Hydration replaces the database file and removes its sidecars; the
  // BEGIN IMMEDIATE probe below is a point-in-time diagnostic, not an
  // exclusion. An exclusive non-blocking lock on <db_path>.arklock held
  // for the whole restore is the actual contract: any other hydrator —
  // other process, runtime, or library instance — that also takes the
  // lock is refused immediately (the process-global mutex below only ever
  // serialized hydrates within ONE process). Application writers that do
  // not take the lock are still caught by the probe while actively
  // writing; the cold-process operator contract remains, now enforced as
  // far as the environment permits.
  char lock_path[4352];
  int lock_fd = -1;
  {
    int n = snprintf(lock_path, sizeof(lock_path), "%s.arklock", db_path);
    if (n <= 0 || (size_t)n >= sizeof(lock_path)) {
      HYDRATE_UNLOCK();
      return HYDRATION_ERR_PROTO;
    }
    lock_fd = ARK_LOCKFILE_OPEN(lock_path);
    if (lock_fd < 0) {
      // A durability product does not restore on a maybe: failure to
      // create the exclusion artifact is a refusal, not a downgrade.
      fprintf(stderr,
              "arkilian: hydration refused — cannot create restore lock "
              "file %s\n", lock_path);
      HYDRATE_UNLOCK();
      return HYDRATION_ERR_DISK;
    }
    if (!ARK_LOCKFILE_LOCK(lock_fd)) {
      fprintf(stderr,
              "arkilian: hydration refused — another restore owns %s "
              "(HYDRATION_ERR_BUSY)\n", lock_path);
      ARK_LOCKFILE_CLOSE(lock_fd);
      lock_fd = -1;
      HYDRATE_UNLOCK();
      return HYDRATION_ERR_BUSY;
    }
  }

  HydratePlan plan = {0};
  int plan_ok = 0;

  if (s3_endpoint && s3_endpoint[0] && s3_bucket && s3_bucket[0] &&
      s3_access_key && s3_access_key[0] && s3_secret_key && s3_secret_key[0] &&
      s3_prefix && s3_prefix[0]) {
    int rc = ark_manifest_fetch(s3_endpoint, s3_bucket, s3_region,
                                 s3_access_key, s3_secret_key, s3_prefix,
                                 &plan);
    if (rc == 0) plan_ok = 1;
  }

  if (!plan_ok) {
    hydrate_result = HYDRATION_ERR_PROTO;
    goto hydrate_done;
  }

  // ── Wire-protocol validation BEFORE any download/replay ─────────────
  {
    int vr = hydrate_validate_plan(&plan);
    if (vr != 0) {
      fprintf(stderr,
              "arkilian: manifest failed wire-protocol validation "
              "(refusing restore)\n");
      hydrate_plan_free(&plan);
      hydrate_result = vr;
      goto hydrate_done;
    }
  }

  // ── Reuse the existing arkilian_hydrate core logic (Phase 0.5 through Phase 2) ──
  // The plan is populated; we run the same download + replay pipeline.

  if (plan.expires_at > 0 && (int64_t)time(NULL) > plan.expires_at) {
    hydrate_plan_free(&plan);
    hydrate_result = HYDRATION_ERR_EXPIRED;
    goto hydrate_done;
  }

  // Phase 0.5: LSN guard
  {
    int64_t pre_local_lsn = 0;
    sqlite3 *ldb = NULL;
    if (sqlite3_open_v2(db_path, &ldb, SQLITE_OPEN_READONLY, NULL) == SQLITE_OK)
      pre_local_lsn = read_last_applied_lsn(ldb);
    sqlite3_close(ldb);
    if (pre_local_lsn > plan.baseline_lsn) {
      fprintf(stderr,
              "arkilian: hydration refused — local DB is at LSN %lld but the "
              "snapshot baseline is %lld\n",
              (long long)pre_local_lsn, (long long)plan.baseline_lsn);
      hydrate_plan_free(&plan);
      hydrate_result = HYDRATION_ERR_NEWER;
      goto hydrate_done;
    }
  }

  // Phase 0.6: Live-writer probe
  {
    sqlite3 *ldb = NULL;
    if (sqlite3_open_v2(db_path, &ldb,
                        SQLITE_OPEN_READWRITE | SQLITE_OPEN_FULLMUTEX, NULL) == SQLITE_OK) {
      char *perr = NULL;
      int prc = sqlite3_exec(ldb, "BEGIN IMMEDIATE;", NULL, NULL, &perr);
      if (prc != SQLITE_OK) {
        sqlite3_free(perr);
        sqlite3_close(ldb);
        hydrate_plan_free(&plan);
        hydrate_result = HYDRATION_ERR_BUSY;
        goto hydrate_done;
      }
      sqlite3_exec(ldb, "ROLLBACK;", NULL, NULL, NULL);
    }
    sqlite3_close(ldb);
  }

  // Phase 1: Download snapshot — or cold-start when the manifest has
  // chunks but no snapshot object yet (the chunk flusher publishes the
  // manifest before the first hourly snapshot). A non-zero baseline
  // without a snapshot object is an inconsistent storage state.
  {
    if (plan.snapshot_url) {
      int rc = download_snapshot(plan.snapshot_url, db_path,
                                 plan.snapshot_sha256, progress, user_data);
      if (rc != 0) { hydrate_plan_free(&plan); hydrate_result = rc; goto hydrate_done; }
    } else {
      if (plan.baseline_lsn > 0) {
        fprintf(stderr,
                "arkilian: hydration refused — manifest records baseline "
                "LSN %lld but no snapshot object exists\n",
                (long long)plan.baseline_lsn);
        hydrate_plan_free(&plan);
        hydrate_result = HYDRATION_ERR_PROTO;
        goto hydrate_done;
      }
      int cs = hydration_cold_start(db_path);
      if (cs != 0) { hydrate_plan_free(&plan); hydrate_result = cs; goto hydrate_done; }
      if (progress) progress(1, 1, 1, user_data);
    }
  }

  // Phase 2: Open database and replay chunks
  {
    sqlite3 *db = NULL;
    int rc = sqlite3_open_v2(db_path, &db,
      SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_FULLMUTEX, NULL);
    if (rc != SQLITE_OK) {
      if (db) sqlite3_close(db);
      hydrate_plan_free(&plan); hydrate_result = HYDRATION_ERR_SQL; goto hydrate_done;
    }

    {
      char *perr = NULL;
      sqlite3_exec(db, "PRAGMA journal_mode=WAL;", NULL, NULL, &perr);
      sqlite3_exec(db, "PRAGMA synchronous=NORMAL;", NULL, NULL, &perr);
      sqlite3_exec(db, "PRAGMA foreign_keys=OFF;", NULL, NULL, &perr);
      sqlite3_free(perr);
    }
    // Ensure the hydration metadata table exists (snapshots created outside
    // Arkilian, e.g. test fixtures, may not have it).
    sqlite3_exec(db, "CREATE TABLE IF NOT EXISTS _arkilian_meta (k TEXT PRIMARY KEY, v TEXT);", NULL, NULL, NULL);

    // ── P0 #1 & P1: trigger isolation + internal state sanitization ──
    // Hydration replays SQL against a snapshot that already contains both
    // customer triggers and Arkilian's own capture triggers. Replaying DML
    // with those triggers active would duplicate side effects (customer
    // trigger fires + captured audit row replayed) and contaminate the
    // restored DB's _pending_backup with new rows generated solely because
    // we restored it. We therefore enter a strict replay mode where NO
    // triggers fire: the authoritative state is snapshot + the ordered
    // captured stream, not the stream re-triggered.
    //
    // SQLITE_DBCONFIG_ENABLE_TRIGGER is the supported way to disable trigger
    // firing on a connection (creation still works). If the SQLite version
    // lacks it (pre-3.13), fall back to dropping Arkilian triggers and
    // warn — customer triggers would still be a risk there, but the vendored
    // 3.51 always has the config.
    int hydrate_old_trigger = 1;
#ifdef SQLITE_DBCONFIG_ENABLE_TRIGGER
    sqlite3_db_config(db, SQLITE_DBCONFIG_ENABLE_TRIGGER, 0, &hydrate_old_trigger);
#else
    // No trigger disable support: fail closed — we cannot guarantee
    // replay equivalence without it. The vendored 3.51 always has the
    // flag, so this path is for arbitrary system SQLite builds.
    fprintf(stderr, "arkilian: hydration requires SQLITE_DBCONFIG_ENABLE_TRIGGER — restore refused\n");
    sqlite3_close(db);
    hydrate_plan_free(&plan);
    hydrate_result = HYDRATION_ERR_SQL;
    goto hydrate_done;
#endif
    // Snapshot is a whole-file copy and therefore contains the source's
    // internal outbox state (_pending_backup, _dead_backup) that is NOT part
    // of the logical data. A restored DB must start clean: any rows in those
    // tables are either already shipped (baseline) or would be duplicate
    // outbox work after restore. Clean them while triggers are disabled so
    // the DELETEs themselves do not generate new capture rows.
    sqlite3_exec(db, "DELETE FROM _pending_backup; DELETE FROM _dead_backup; DELETE FROM sqlite_sequence WHERE name IN ('_pending_backup','_dead_backup');", NULL, NULL, NULL);

    int64_t local_lsn = read_last_applied_lsn(db);
    if (local_lsn == 0) local_lsn = plan.baseline_lsn;
    else if (local_lsn < plan.baseline_lsn) {
      fprintf(stderr,
              "arkilian: hydration refused — snapshot LSN %lld < plan baseline %lld\n",
              (long long)local_lsn, (long long)plan.baseline_lsn);
      sqlite3_close(db);
      hydrate_plan_free(&plan);
      hydrate_result = HYDRATION_ERR_PROTO;
      goto hydrate_done;
    }

    int chunk_total = 0;
    for (int i = 0; i < plan.chunk_count; i++) {
      HydrateChunk *ch = &plan.chunks[i];
      if (ch->lsn_end <= local_lsn) continue;
      if (ch->lsn_start > local_lsn + 1) {
        fprintf(stderr, "arkilian: hydration LSN gap at chunk %d\n", i);
        sqlite3_close(db);
        hydrate_plan_free(&plan);
        hydrate_result = HYDRATION_ERR_PROTO;
        goto hydrate_done;
      }
      if (ch->expires_at > 0 && (int64_t)time(NULL) > ch->expires_at) {
        sqlite3_close(db);
        hydrate_plan_free(&plan);
        hydrate_result = HYDRATION_ERR_EXPIRED;
        goto hydrate_done;
      }

      if (!url_is_allowed_storage(ch->url)) {
        fprintf(stderr, "arkilian: chunk %d SSRF guard refused\n", i);
        sqlite3_close(db);
        hydrate_plan_free(&plan);
        hydrate_result = HYDRATION_ERR_PROTO;
        goto hydrate_done;
      }

      int err = 0;
      char *sql_text = http_get_string(ch->url, &err);
      if (!sql_text) { sqlite3_close(db); hydrate_plan_free(&plan); hydrate_result = err; goto hydrate_done; }

      // Content authentication — the missing-digest case is a HARD
      // refusal, matching the snapshot path and hydration.h's documented
      // contract. Chunks are replayed as SQL: unauthenticated SQL in the
      // restore path is the exact attack the digest exists to stop. The
      // shipper never publishes digest-less chunks (a hashing failure
      // fails the flush), so this can only be a foreign/tampered manifest.
      if (!ch->sha256 || !ch->sha256[0]) {
        fprintf(stderr,
                "arkilian: chunk %d has NO SHA-256 digest in the manifest — "
                "refusing to replay unauthenticated SQL\n", i);
        free(sql_text);
        sqlite3_close(db);
        hydrate_plan_free(&plan);
        hydrate_result = HYDRATION_ERR_PROTO;
        goto hydrate_done;
      }
      {
        char digest[65];
        ark_sha256_hex(sql_text, strlen(sql_text), digest);
        if (strcasecmp(digest, ch->sha256) != 0) {
          fprintf(stderr, "arkilian: chunk %d SHA-256 mismatch\n", i);
          free(sql_text);
          sqlite3_close(db);
          hydrate_plan_free(&plan);
          hydrate_result = HYDRATION_ERR_PROTO;
          goto hydrate_done;
        }
      }

      rc = hydrate_replay_chunk(db, sql_text, ch->lsn_end);
      free(sql_text);
      if (rc != 0) {
        fprintf(stderr, "arkilian: hydrate_replay_chunk failed rc=%d\n", rc);
        sqlite3_close(db); hydrate_plan_free(&plan); hydrate_result = rc; goto hydrate_done;
      }

      local_lsn = ch->lsn_end;
      chunk_total++;
      if (progress) progress(2, chunk_total, plan.chunk_count, user_data);
    }

    // Leave replay mode: re-enable trigger firing so the restored DB is
    // immediately usable by a normal Arkilian runtime. The next db_init()
    // will call sync_backup_triggers() to create any capture triggers that
    // are missing for tables created by replayed DDL while triggers were
    // off (those DDLs created the tables but not their capture triggers).
    // That post-hydration repair is required because CREATE TABLE replayed
    // with triggers disabled does not magically get a capture trigger.
#ifdef SQLITE_DBCONFIG_ENABLE_TRIGGER
    sqlite3_db_config(db, SQLITE_DBCONFIG_ENABLE_TRIGGER, hydrate_old_trigger, NULL);
#else
    // Fallback path already warned above.
#endif

    sqlite3_exec(db, "PRAGMA synchronous=NORMAL;", NULL, NULL, NULL);
    sqlite3_exec(db, "PRAGMA foreign_keys=ON;", NULL, NULL, NULL);
    sqlite3_close(db);
  }

  hydrate_plan_free(&plan);

hydrate_done:
  if (lock_fd >= 0) {
    ARK_LOCKFILE_UNLOCK(lock_fd);
    ARK_LOCKFILE_CLOSE(lock_fd);
  }
  HYDRATE_UNLOCK();
  return hydrate_result;
}
