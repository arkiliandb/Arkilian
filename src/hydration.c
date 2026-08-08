// Arkilian Hydration Engine v2 — implementation
//
// Logical replay: downloads snapshot binary + incremental SQL chunks
// via Pre-Signed URLs, plays them back with sqlite3_exec() inside
// explicit transactions.  No binary WAL frame manipulation needed.
//
// Auth model: the client uses ONLY the API key (ARKILIAN_API_KEY) as
// "Authorization: Bearer <api_key>" to the control plane. No other
// credential is used. The control plane issues pre-signed S3 GET URLs
// for snapshot/chunk downloads — the API key is never sent to S3.

#include "hydration.h"
#include "sha256.h"
#include <curl/curl.h>
#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

// Pre-signed object-storage URLs carry their own credentials in the
// query string — attaching our bearer token both leaks the credential
// to the storage host and can break signature validation.
static int url_is_presigned(const char *url) {
  if (!url) return 0;
  return strstr(url, "X-Amz-Signature=") != NULL ||
         strstr(url, "X-Amz-Credential=") != NULL ||
         strstr(url, "X-Goog-Signature=") != NULL ||
         strstr(url, "X-Goog-Credential=") != NULL ||
         strstr(url, "sig=") != NULL; /* Azure SAS */
}

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
// fe80::) because that range hosts the cloud instance-metadata service
// (IMDS at 169.254.169.254 on AWS/GCP/Azure). A compromised control
// plane returning a snapshot/chunk URL pointing at IMDS would otherwise
// have the client download from (or, for non-presigned URLs, send the
// API key to) the metadata service.
static int host_is_storage_safe(const char *host) {
  if (!host || !*host) return 0;
  if (host[0] == '[') {
    if (strncmp(host, "[::1]", 5) == 0) return 1;
    if (strncmp(host, "[fc", 3) == 0 || strncmp(host, "[fd", 3) == 0) return 1;
    // NO [fe80 — link-local / IMDS excluded
    return 0;
  }
  if (strcmp(host, "localhost") == 0) return 1;
  if (strncmp(host, "127.", 4) == 0) return 1;
  if (strncmp(host, "10.", 3) == 0) return 1;
  if (strncmp(host, "192.168.", 8) == 0) return 1;
  // NO 169.254. — IMDS excluded
  // NO fe80 — IPv6 link-local excluded
  if (strncmp(host, "::1", 3) == 0) return 1;
  if (strncmp(host, "fc", 2) == 0 || strncmp(host, "fd", 2) == 0) return 1;
  if (strncmp(host, "172.", 4) == 0) {
    unsigned second = 0;
    if (sscanf(host, "172.%u.", &second) == 1 && second >= 16 && second <= 31)
      return 1;
  }
  return 0;
}

static int host_is_known_storage(const char *host) {
  if (!host || !*host) return 0;
  if (strstr(host, ".amazonaws.com")) return 1;
  if (strcmp(host, "storage.googleapis.com") == 0) return 1;
  if (strstr(host, ".storage.googleapis.com")) return 1;
  if (strstr(host, ".blob.core.windows.net")) return 1;
  if (strstr(host, ".backblazeb2.com")) return 1;
  if (strstr(host, ".r2.cloudflarestorage.com")) return 1;
  if (strstr(host, ".wasabisys.com")) return 1;
  if (strstr(host, ".digitaloceanspaces.com")) return 1;
  return 0;
}

// SSRF guard: a control plane (compromised or buggy) returning a snapshot
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
static pthread_mutex_t g_hydrate_mutex = PTHREAD_MUTEX_INITIALIZER;

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

static int http_init(HttpReq *r, const char *url, const char *token) {
  r->handle = curl_easy_init();
  if (!r->handle) return -1;
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
  // verification. Hydration URLs come from the (trusted) control plane,
  // but the storage backend body is content-authenticated via SHA-256.
  curl_easy_setopt(r->handle, CURLOPT_SSL_VERIFYPEER, 1L);
  curl_easy_setopt(r->handle, CURLOPT_SSL_VERIFYHOST, 2L);
  r->headers = NULL;
  if (token && strlen(token) > 0 && !url_is_presigned(url)) {
    char auth[512];
    snprintf(auth, sizeof(auth), "Authorization: Bearer %s", token);
    r->headers = curl_slist_append(r->headers, auth);
    curl_easy_setopt(r->handle, CURLOPT_HTTPHEADER, r->headers);
  }
  return 0;
}

static void http_free(HttpReq *r) {
  if (r->headers) curl_slist_free_all(r->headers);
  if (r->handle)  curl_easy_cleanup(r->handle);
}

// GET a URL into a malloc'd string.  Returns NULL on failure.
static char *http_get_string(const char *url, const char *token, int *err_out) {
  HttpReq r;
  if (http_init(&r, url, token) != 0) { *err_out = HYDRATION_ERR_NET; return NULL; }

  struct curl_buf buf = {NULL, 0, 0};
  curl_easy_setopt(r.handle, CURLOPT_WRITEDATA, &buf);
  // Cap the response size: a chunk is replayable SQL text bounded by the
  // control plane's chunking. A compromised/buggy control plane streaming
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
// local_path.  Streams to a temp file first; only on a complete 200
// response + (optional) SHA-256 verification + SQLite quick_check are the
// old database's stale -wal/-shm sidecars replaced and the snapshot
// atomically installed.  A 404 maps to HYDRATION_ERR_NOTFOUND (cold
// start); any other failure leaves the existing local database untouched.
// `expected_sha256` is an optional lowercase hex digest (64 chars, no
// dashes) authored by the uploader + control plane; NULL/empty skips
// content verification with a warning (back-compat with older planes).
static int http_download_file(const char *url, const char *token,
                               const char *local_path, const char *expected_sha256,
                               int *err_out) {
  // SSRF guard: never fetch a snapshot from a host that isn't an allowed
  // storage destination. A compromised control plane returning a
  // metadata-service URL has no path to the local DB even before content
  // auth runs.
  if (!url_is_allowed_storage(url)) {
    fprintf(stderr,
            "arkilian: snapshot download refused — host is not an allowed "
            "storage destination (SSRF guard): %.200s\n", url);
    *err_out = HYDRATION_ERR_PROTO;
    return -1;
  }
  HttpReq r;
  if (http_init(&r, url, token) != 0) { *err_out = HYDRATION_ERR_NET; return -1; }

  char tmp_path[4096];
  int n = snprintf(tmp_path, sizeof(tmp_path), "%s.arkdl", local_path);
  if (n <= 0 || (size_t)n >= sizeof(tmp_path)) {
    http_free(&r); *err_out = HYDRATION_ERR_DISK; return -1;
  }

  FILE *f = fopen(tmp_path, "wb");
  if (!f) { http_free(&r); *err_out = HYDRATION_ERR_DISK; return -1; }

  curl_easy_setopt(r.handle, CURLOPT_WRITEFUNCTION, file_write_cb);
  curl_easy_setopt(r.handle, CURLOPT_WRITEDATA, f);

  CURLcode rc = curl_easy_perform(r.handle);
  long http_code = 0;
  if (rc == CURLE_OK) curl_easy_getinfo(r.handle, CURLINFO_RESPONSE_CODE, &http_code);
  http_free(&r);

  int io_failed = ferror(f);
  if (fflush(f) != 0) io_failed = 1;
  if (fsync(fileno(f)) != 0) io_failed = 1;
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
  // still pass it. Verifying the downloaded bytes against the control
  // plane's recorded digest is the strongest available content guarantee,
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
            "arkilian: snapshot SHA-256 digest NOT provided by control "
            "plane — refusing to install unauthenticated content "
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
        const char *r = (const char *)sqlite3_column_text(q, 0);
        ok = r && strcmp(r, "ok") == 0;
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
// These are deliberately small but CORRECT for the control-plane
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
        if (cp == 0xFFFFFFFF) { val[out++] = '?'; p++; break; }
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
      default: if (*p) { val[out++] = *p++; } break; // unknown escape → literal
    }
  }
  val[out] = '\0';
  return val;
}

int64_t json_get_int64(const char *json, const char *key) {
  const char *p = json_find_key(json, key);
  if (!p) return 0;
  return (int64_t)strtoll(p, NULL, 10);
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

// ── Control Plane: request hydration plan ───────────────────────────

static int request_hydrate_plan(const char *server_url, const char *api_key,
                                 HydratePlan *plan) {
  // The control URL is the base (e.g. https://api.arkilian.com); the
  // hydrate plan endpoint is /v1/hydrate/plan under that base.
  size_t url_len = strlen(server_url) + strlen("/v1/hydrate/plan") + 1;
  char *url = malloc(url_len);
  if (!url) return HYDRATION_ERR_MEM;
  // Strip trailing slash from base to avoid doubles.
  size_t blen = strlen(server_url);
  int strip = (blen > 0 && server_url[blen - 1] == '/');
  if (strip) snprintf(url, url_len, "%.*s/v1/hydrate/plan", (int)(blen - 1), server_url);
  else snprintf(url, url_len, "%s/v1/hydrate/plan", server_url);

  int err = 0;
  char *json = http_get_string(url, api_key, &err);
  free(url);
  if (!json) return err;

  // Parse plan
  memset(plan, 0, sizeof(*plan));
  plan->snapshot_url    = json_get_string(json, "snapshot_url");
  plan->snapshot_sha256 = json_get_string(json, "snapshot_sha256");
  plan->baseline_lsn  = json_get_int64(json, "baseline_lsn");
  plan->expires_at    = json_get_int64(json, "expires_at");
  plan->chunk_count   = json_array_count(json, "chunks");

  if (plan->chunk_count > 0) {
    // calloc so a partial parse failure leaves no uninitialized
    // pointers for hydrate_plan_free() to choke on.
    plan->chunks = calloc((size_t)plan->chunk_count, sizeof(HydrateChunk));
    if (!plan->chunks) { free(json); hydrate_plan_free(plan); return HYDRATION_ERR_MEM; }

    for (int i = 0; i < plan->chunk_count; i++) {
      char *elem = json_array_get(json, "chunks", i);
      if (!elem) { free(json); hydrate_plan_free(plan); return HYDRATION_ERR_PROTO; }
      plan->chunks[i].url        = json_get_string(elem, "url");
      plan->chunks[i].sha256     = json_get_string(elem, "sha256");
      plan->chunks[i].lsn_start  = json_get_int64(elem, "lsn_start");
      plan->chunks[i].lsn_end    = json_get_int64(elem, "lsn_end");
      plan->chunks[i].expires_at = json_get_int64(elem, "expires_at");
      free(elem);
      if (!plan->chunks[i].url) { free(json); hydrate_plan_free(plan); return HYDRATION_ERR_PROTO; }
    }
  }

  free(json);

  if (!plan->snapshot_url) { hydrate_plan_free(plan); return HYDRATION_ERR_PROTO; }
  return 0;
}

void hydrate_plan_free(HydratePlan *plan) {
  if (!plan) return;
  free(plan->snapshot_url);
  free(plan->snapshot_sha256);
  for (int i = 0; i < plan->chunk_count; i++) {
    free(plan->chunks[i].url);
    free(plan->chunks[i].sha256);
  }
  free(plan->chunks);
  memset(plan, 0, sizeof(*plan));
}

// ── Step 1: Download & decompress snapshot ──────────────────────────

static int download_snapshot(const char *snapshot_url, const char *token,
                              const char *db_path, const char *expected_sha256,
                              hydration_progress_cb progress, void *user) {
  if (progress) progress(1, 0, 1, user);

  int err = 0;
  int rc = http_download_file(snapshot_url, token, db_path, expected_sha256, &err);
  if (rc != 0) {
    // Only a genuine 404 means "no baseline snapshot uploaded yet"
    // (cold start).  Every other failure — network blip, expired URL,
    // disk full — is a real error and MUST NOT be masked as success,
    // otherwise hydration would replay chunks onto an empty database
    // and report a silently incomplete restore as OK.
    if (err != HYDRATION_ERR_NOTFOUND) return err;

    // Cold start: initialize a clean target so chunks replay from
    // LSN 0.  Never destroy an existing local database in the process.
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
      if (progress) progress(1, 1, 1, user);
      return 0;
    }
    return HYDRATION_ERR_DISK;
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

int hydrate_replay_chunk(sqlite3 *db, const char *raw_sql, int64_t chunk_lsn) {
  char *err_msg = NULL;

  // 1. Begin explicit transaction for throughput
  int rc = sqlite3_exec(db, "BEGIN TRANSACTION;", NULL, NULL, &err_msg);
  if (rc != SQLITE_OK) {
    if (err_msg) { fprintf(stderr, "Hydration BEGIN error: %s\n", err_msg); sqlite3_free(err_msg); }
    return HYDRATION_ERR_SQL;
  }

  // 2. Replay the raw multi-statement SQL text
  rc = sqlite3_exec(db, raw_sql, NULL, NULL, &err_msg);
  if (rc != SQLITE_OK) {
    fprintf(stderr, "Hydration replay error: %s\n", err_msg);
    sqlite3_free(err_msg);
    sqlite3_exec(db, "ROLLBACK;", NULL, NULL, NULL);
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
    return HYDRATION_ERR_SQL;
  }

  rc = sqlite3_exec(db, "COMMIT;", NULL, NULL, NULL);
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

static char *s3_presign_get(const char *endpoint, const char *bucket,
                             const char *region, const char *access_key,
                             const char *secret_key, const char *key,
                             long expires_sec) {
  if (!endpoint || !bucket || !access_key || !secret_key || !key) return NULL;
  if (!region) region = "us-east-1";

  time_t now = time(NULL);
  struct tm g;
  gmtime_r(&now, &g);
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

static int manifest_read_from_s3(const char *endpoint, const char *bucket,
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
  char *json = http_get_string(manifest_url, NULL, &err);
  free(manifest_url);

  if (!json) return -1;

  memset(plan, 0, sizeof(*plan));
  plan->snapshot_url    = json_get_string(json, "s3_key");
  plan->snapshot_sha256 = json_get_string(json, "sha256");
  plan->baseline_lsn    = 0;

  // Parse snapshot object
  char *snap = json_array_get(json, "snapshot", 0);
  if (!snap) {
    // Direct top-level fields from simplified manifest
    plan->snapshot_url    = json_get_string(json, "snapshot_url");
    plan->snapshot_sha256 = json_get_string(json, "snapshot_sha256");
    plan->baseline_lsn    = json_get_int64(json, "baseline_lsn");
  } else {
    // Nested snapshot object from full manifest
    char *s3_key = json_get_string(snap, "s3_key");
    if (s3_key) {
      plan->snapshot_url = s3_presign_get(endpoint, bucket, region,
                                           access_key, secret_key,
                                           s3_key, 3600L);
      free(s3_key);
    }
    plan->snapshot_sha256 = json_get_string(snap, "sha256");
    char *bl = json_get_string(snap, "baseline_lsn");
    if (bl) { plan->baseline_lsn = (int64_t)strtoll(bl, NULL, 10); free(bl); }
    free(snap);
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
          plan->chunks[i].url = s3_presign_get(endpoint, bucket, region,
                                                access_key, secret_key,
                                                ckey, 3600L);
          free(ckey);
        }
        plan->chunks[i].sha256 = json_get_string(elem, "sha256");
        char *ls = json_get_string(elem, "lsn_start");
        if (ls) { plan->chunks[i].lsn_start = (int64_t)strtoll(ls, NULL, 10); free(ls); }
        char *le = json_get_string(elem, "lsn_end");
        if (le) { plan->chunks[i].lsn_end = (int64_t)strtoll(le, NULL, 10); free(le); }
        free(elem);
      }
    }
  }

  plan->expires_at = (int64_t)time(NULL) + 3600;

  free(json);
  if (!plan->snapshot_url) { hydrate_plan_free(plan); return -1; }
  return 0;
}

int arkilian_hydrate_s3(const char *db_path,
                         const char *server_url,
                         const char *api_key,
                         const char *s3_endpoint,
                         const char *s3_bucket,
                         const char *s3_region,
                         const char *s3_access_key,
                         const char *s3_secret_key,
                         const char *s3_prefix,
                         hydration_progress_cb progress,
                         void *user_data) {
  if (!db_path) return HYDRATION_ERR_PROTO;

  pthread_mutex_lock(&g_hydrate_mutex);
  int hydrate_result = 0;

  HydratePlan plan;
  int plan_ok = 0;

  if (s3_endpoint && s3_endpoint[0] && s3_bucket && s3_bucket[0] &&
      s3_access_key && s3_access_key[0] && s3_secret_key && s3_secret_key[0] &&
      s3_prefix && s3_prefix[0]) {
    int rc = manifest_read_from_s3(s3_endpoint, s3_bucket, s3_region,
                                    s3_access_key, s3_secret_key, s3_prefix,
                                    &plan);
    if (rc == 0) plan_ok = 1;
  }

  if (!plan_ok) {
    hydrate_result = HYDRATION_ERR_PROTO;
    goto hydrate_done;
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

  // Phase 1: Download snapshot
  {
    int rc = download_snapshot(plan.snapshot_url, api_key, db_path,
                                plan.snapshot_sha256, progress, user_data);
    if (rc != 0) { hydrate_plan_free(&plan); hydrate_result = rc; goto hydrate_done; }
  }

  // Phase 2: Open database and replay chunks
  {
    sqlite3 *db = NULL;
    int rc = sqlite3_open_v2(db_path, &db,
      SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_FULLMUTEX, NULL);
    if (rc != SQLITE_OK) { hydrate_plan_free(&plan); hydrate_result = HYDRATION_ERR_SQL; goto hydrate_done; }

    {
      char *perr = NULL;
      sqlite3_exec(db, "PRAGMA journal_mode=WAL;", NULL, NULL, &perr);
      sqlite3_exec(db, "PRAGMA synchronous=NORMAL;", NULL, NULL, &perr);
      sqlite3_exec(db, "PRAGMA foreign_keys=OFF;", NULL, NULL, &perr);
      sqlite3_free(perr);
    }

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
      char *sql_text = http_get_string(ch->url, api_key, &err);
      if (!sql_text) { sqlite3_close(db); hydrate_plan_free(&plan); hydrate_result = err; goto hydrate_done; }

      if (ch->sha256 && ch->sha256[0]) {
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
      if (rc != 0) { sqlite3_close(db); hydrate_plan_free(&plan); hydrate_result = rc; goto hydrate_done; }

      local_lsn = ch->lsn_end;
      chunk_total++;
      if (progress) progress(2, chunk_total, plan.chunk_count, user_data);
    }

    sqlite3_exec(db, "PRAGMA synchronous=NORMAL;", NULL, NULL, NULL);
    sqlite3_exec(db, "PRAGMA foreign_keys=ON;", NULL, NULL, NULL);
    sqlite3_close(db);
  }

  hydrate_plan_free(&plan);

hydrate_done:
  pthread_mutex_unlock(&g_hydrate_mutex);
  return hydrate_result;
}

// ── Public API ──────────────────────────────────────────────────────

int arkilian_hydrate(const char *db_path,
                     const char *server_url,
                     const char *api_key,
                     hydration_progress_cb progress,
                     void *user_data) {
  if (!db_path || !server_url) return HYDRATION_ERR_PROTO;

  // Single-flight: serialize concurrent hydration calls so two
  // hydrates on the same db_path can't race on the temp file or
  // rename(). The application must not have the DB open during
  // hydrate (documented in hydration.h) — this guard protects against
  // two cold-start processes racing, not against a live application.
  pthread_mutex_lock(&g_hydrate_mutex);
  int hydrate_result = 0;

  // ── Phase 0: Request hydration plan ──
  HydratePlan plan;
  int rc = request_hydrate_plan(server_url, api_key, &plan);
  if (rc != 0) { hydrate_result = rc; goto hydrate_done; }

  // A snapshot URL that is already expired can only fail — say so.
  if (plan.expires_at > 0 && (int64_t)time(NULL) > plan.expires_at) {
    hydrate_plan_free(&plan);
    hydrate_result = HYDRATION_ERR_EXPIRED;
    goto hydrate_done;
  }

  // ── Phase 0.5: Local-vs-snapshot LSN guard (data-loss protection) ──
  // The snapshot download REPLACES the local database file, so the
  // comparison MUST happen before any file is touched. If the local
  // database was hydrated further than the snapshot's baseline
  // (incremental hydration to LSN 5000 while the control plane serves a
  // snapshot at LSN 3000), installing the snapshot would silently roll
  // back 2000 LSNs of data. Refuse instead. Reads the local meta
  // read-only; a missing file or missing meta reads as LSN 0 (cold
  // start / unhydrated local DB — the explicit hydrate() call opts
  // those in).
  {
    int64_t pre_local_lsn = 0;
    sqlite3 *ldb = NULL;
    if (sqlite3_open_v2(db_path, &ldb, SQLITE_OPEN_READONLY, NULL) == SQLITE_OK) {
      pre_local_lsn = read_last_applied_lsn(ldb);
    }
    sqlite3_close(ldb);
    if (pre_local_lsn > plan.baseline_lsn) {
      fprintf(stderr,
              "arkilian: hydration refused — local DB is at LSN %lld but the "
              "snapshot baseline is %lld; installing it would destroy %lld "
              "LSN(s) of local data (HYDRATION_ERR_NEWER)\n",
              (long long)pre_local_lsn, (long long)plan.baseline_lsn,
              (long long)(pre_local_lsn - plan.baseline_lsn));
      hydrate_plan_free(&plan);
      hydrate_result = HYDRATION_ERR_NEWER;
      goto hydrate_done;
    }
  }

  // ── Phase 0.6: Live-writer probe (footgun guard) ───────────────────
  // Installing the snapshot remove()+rename()s the file; if the
  // application is actively writing through another connection, its
  // writes keep landing on the orphaned inode and diverge from the
  // restored file. A BEGIN IMMEDIATE with no busy wait detects an
  // actively-writing connection. Best-effort: an idle-but-open
  // connection can start writing right after the probe — callers must
  // not hydrate a live database (documented in hydration.h).
  {
    sqlite3 *ldb = NULL;
    if (sqlite3_open_v2(db_path, &ldb,
                        SQLITE_OPEN_READWRITE | SQLITE_OPEN_FULLMUTEX, NULL) == SQLITE_OK) {
      char *perr = NULL;
      int prc = sqlite3_exec(ldb, "BEGIN IMMEDIATE;", NULL, NULL, &perr);
      if (prc != SQLITE_OK) {
        fprintf(stderr,
                "arkilian: hydration refused — the local database is locked by "
                "another connection (application running?); refusing to clobber "
                "a live database (HYDRATION_ERR_BUSY)\n");
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

  // ── Phase 1: Download baseline snapshot ──
  rc = download_snapshot(plan.snapshot_url, api_key, db_path,
                          plan.snapshot_sha256, progress, user_data);
  if (rc != 0) { hydrate_plan_free(&plan); hydrate_result = rc; goto hydrate_done; }

  // ── Phase 2: Open database, check LSN, replay chunks ──
  sqlite3 *db = NULL;
  rc = sqlite3_open_v2(db_path, &db,
    SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_FULLMUTEX, NULL);
  if (rc != SQLITE_OK) { hydrate_plan_free(&plan); hydrate_result = HYDRATION_ERR_SQL; goto hydrate_done; }

  // Apply PRAGMAs for the replay. synchronous=NORMAL (NOT OFF) keeps the
  // WAL durable on commit even during replay — a power loss mid-replay
  // with synchronous=OFF can corrupt the local DB, unacceptable for a
  // data-durability product. Best-effort: a failure slows the bulk load
  // but must never abort the restore.
  {
    char *perr = NULL;
    if (sqlite3_exec(db, "PRAGMA journal_mode=WAL;", NULL, NULL, &perr) != SQLITE_OK ||
        sqlite3_exec(db, "PRAGMA synchronous=NORMAL;", NULL, NULL, &perr) != SQLITE_OK ||
        sqlite3_exec(db, "PRAGMA foreign_keys=OFF;", NULL, NULL, &perr) != SQLITE_OK) {
      fprintf(stderr, "arkilian: hydration speed pragma warning: %s\n",
              perr ? perr : "unknown error");
      sqlite3_free(perr);
    }
  }

  // Read the snapshot's ACTUAL recorded LSN (it was just installed as the
  // local db). The control plane's baseline_lsn is the authority for the
  // EXPECTED LSN; a mismatch means the served snapshot is not the one the
  // plan was built against.
  int64_t local_lsn = read_last_applied_lsn(db);
  if (local_lsn == 0) {
    // Fresh snapshot with no recorded LSN (cold-start baseline): trust the
    // control plane's declared baseline.
    local_lsn = plan.baseline_lsn;
  } else if (local_lsn < plan.baseline_lsn) {
    // The downloaded snapshot is STALER than the control plane claims.
    // Previously the code clamped local_lsn UP to baseline and silently
    // skipped chunks [local_lsn+1 .. baseline] → silent data loss reported
    // as OK. Refuse instead: the control plane or storage is inconsistent.
    fprintf(stderr,
            "arkilian: hydration refused — downloaded snapshot's recorded "
            "LSN %lld is less than the plan's baseline %lld (served a stale "
            "snapshot; control-plane or storage inconsistency)\n",
            (long long)local_lsn, (long long)plan.baseline_lsn);
    sqlite3_close(db);
    hydrate_plan_free(&plan);
    hydrate_result = HYDRATION_ERR_PROTO;
    goto hydrate_done;
  }

  int chunk_total = 0;
  for (int i = 0; i < plan.chunk_count; i++) {
    HydrateChunk *ch = &plan.chunks[i];

    // Skip chunks already applied
    if (ch->lsn_end <= local_lsn) continue;

    // A hole between what we have applied and what this chunk covers
    // means permanently missing data — fail loudly, never skip silently.
    if (ch->lsn_start > local_lsn + 1) {
      fprintf(stderr,
              "arkilian: hydration LSN gap — have up to %lld but chunk "
              "starts at %lld (missing %lld LSN(s))\n",
              (long long)local_lsn, (long long)ch->lsn_start,
              (long long)(ch->lsn_start - local_lsn - 1));
      sqlite3_close(db);
      hydrate_plan_free(&plan);
      hydrate_result = HYDRATION_ERR_PROTO;
      goto hydrate_done;
    }

    // Check URL expiry
    if (ch->expires_at > 0 && (int64_t)time(NULL) > ch->expires_at) {
      sqlite3_close(db);
      hydrate_plan_free(&plan);
      hydrate_result = HYDRATION_ERR_EXPIRED;
      goto hydrate_done;
    }

    // Download chunk. SSRF-guarded: never fetch a chunk from a host that
    // isn't an allowed storage destination (a compromised control plane
    // could otherwise point the client at cloud metadata or an internal
    // service to read the auth header or exfiltrate state).
    if (!url_is_allowed_storage(ch->url)) {
      fprintf(stderr,
              "arkilian: chunk %d download refused — host is not an "
              "allowed storage destination (SSRF guard): %.200s\n",
              i, ch->url);
      sqlite3_close(db);
      hydrate_plan_free(&plan);
      hydrate_result = HYDRATION_ERR_PROTO;
      goto hydrate_done;
    }
    int err = 0;
    char *sql_text = http_get_string(ch->url, api_key, &err);
    if (!sql_text) { sqlite3_close(db); hydrate_plan_free(&plan); hydrate_result = err; goto hydrate_done; }

    // Content authentication: verify the chunk's SHA-256 against the
    // control plane's recorded digest BEFORE replaying it as raw SQL
    // against the local database. A mismatch is a tampered/broken chunk
    // and must never reach sqlite3_exec. A missing digest is a HARD
    // refusal — silently replaying unauthenticated content is a
    // downgrade-attack surface, not a back-compat feature.
    if (ch->sha256 && ch->sha256[0]) {
      char digest[65];
      ark_sha256_hex(sql_text, strlen(sql_text), digest);
      if (strcasecmp(digest, ch->sha256) != 0) {
        fprintf(stderr,
                "arkilian: chunk %d SHA-256 MISMATCH — refusing to replay "
                "(expected %.16s…, got %.16s…). Storage tampering\n",
                i, ch->sha256, digest);
        free(sql_text);
        sqlite3_close(db);
        hydrate_plan_free(&plan);
        hydrate_result = HYDRATION_ERR_PROTO;
        goto hydrate_done;
      }
    } else {
      fprintf(stderr,
              "arkilian: chunk %d sha256 NOT provided by control plane — "
              "refusing to replay unauthenticated content (HYDRATION_ERR_PROTO)\n",
              i);
      free(sql_text);
      sqlite3_close(db);
      hydrate_plan_free(&plan);
      hydrate_result = HYDRATION_ERR_PROTO;
      goto hydrate_done;
    }

    // Replay
    rc = hydrate_replay_chunk(db, sql_text, ch->lsn_end);
    free(sql_text);

    if (rc != 0) { sqlite3_close(db); hydrate_plan_free(&plan); hydrate_result = rc; goto hydrate_done; }

    local_lsn = ch->lsn_end;
    chunk_total++;
    if (progress) progress(2, chunk_total, plan.chunk_count, user_data);
  }

  // Restore safe PRAGMAs
  {
    char *perr = NULL;
    if (sqlite3_exec(db, "PRAGMA synchronous=NORMAL;", NULL, NULL, &perr) != SQLITE_OK ||
        sqlite3_exec(db, "PRAGMA foreign_keys=ON;", NULL, NULL, &perr) != SQLITE_OK) {
      fprintf(stderr, "arkilian: hydration restore pragma warning: %s\n",
              perr ? perr : "unknown error");
      sqlite3_free(perr);
    }
  }

  sqlite3_close(db);
  hydrate_plan_free(&plan);

hydrate_done:
  pthread_mutex_unlock(&g_hydrate_mutex);
  return hydrate_result;
}
