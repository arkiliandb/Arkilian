// Arkilian SQLite Wrapper — Production Realtime Backup Engine
// Implements realtime SQL trigger capture, non-blocking wake signals,
// and dedicated background WAL shipping.

#ifndef _POSIX_C_SOURCE
#define _POSIX_C_SOURCE 200809L
#endif
#ifndef _DEFAULT_SOURCE
#define _DEFAULT_SOURCE
#endif
// macOS <sys/mount.h> (for statfs, used by the network-FS guard in
// db_init) transitively needs BSD types (u_int/u_short/...) that
// _POSIX_C_SOURCE hides. _DARWIN_C_SOURCE re-exposes them; it only
// ADDS Darwin/BSD symbols and must be set before any system include.
#ifdef __APPLE__
#ifndef _DARWIN_C_SOURCE
#define _DARWIN_C_SOURCE
#endif
#endif

#include "class.h"
#include "hydration.h"
#include "sha256.h"
#include <curl/curl.h>

// Atomic helpers for cross-thread flags.  These fields are read and
// written concurrently by multiple threads.  Plain volatile access is
// not a portable memory model (and is not recognized by TSAN), so we
// use compiler builtins on GNU-compatible compilers (GCC, Clang, MinGW)
// and MSVC interlocked intrinsics on Windows.  All accesses to these
// flags go through these macros so the access model is consistent
// everywhere.
#ifdef __GNUC__
#define ARK_LOAD(ptr)        __atomic_load_n((ptr), __ATOMIC_ACQUIRE)
#define ARK_STORE(ptr, val)  __atomic_store_n((ptr), (val), __ATOMIC_RELEASE)
#elif defined(_WIN32)
#define ARK_LOAD(ptr)        ((int)_InterlockedExchangeAdd((volatile long*)(ptr), 0L))
#define ARK_STORE(ptr, val)  ((void)_InterlockedExchange((volatile long*)(ptr), (long)(val)))
#else
#error "Unsupported compiler: need atomic load/store primitives"
#endif

#ifdef _WIN32
#include <windows.h>
#include <share.h>
#ifndef __MINGW32__
#define strcasecmp _stricmp
#define strncasecmp _strnicmp
/* MSVC: strtok_s has the same (str, delim, *ctx) signature as strtok_r. */
#define strtok_r strtok_s
#else
#include <strings.h>
#endif
#define strdup _strdup
#else
#include <pthread.h>
#include <strings.h>
#include <unistd.h>
#include <sys/file.h>
#endif
#include <fcntl.h>
#include <sys/stat.h>

// ── Unique temp-file creation (O_EXCL, 0600) ────────────────────────
// The chunk/manifest staging files used to be FIXED names derived from
// backup_path ("backup.sqlite.chunk"). Two Arkilian instances sharing a
// backup path (including two instances in the SAME process — the default
// backup path is CWD-relative) would truncate each other's staging file,
// and since the digest is computed from the same shared file, the
// cross-contaminated bytes passed the integrity check and shipped under
// the wrong prefix. Every staging file now gets a per-instance name
// (pid + atomic counter) created with O_CREAT|O_EXCL and owner-only mode.
// Windows spellings kept in lockstep with hydration.c's shims.
#ifdef _WIN32
#define ARK_OPEN_EXCL(path) _open((path), _O_CREAT | _O_EXCL | _O_WRONLY | _O_BINARY, _S_IREAD | _S_IWRITE)
#define ARK_FDOPEN(fd, mode) _fdopen((fd), (mode))
#define ARK_CLOSE_FD(fd) _close(fd)
#define ARK_GETPID _getpid
#else
#define ARK_OPEN_EXCL(path) open((path), O_CREAT | O_EXCL | O_WRONLY, 0600)
#define ARK_FDOPEN(fd, mode) fdopen((fd), (mode))
#define ARK_CLOSE_FD(fd) close(fd)
#define ARK_GETPID getpid
#endif
#ifdef __GNUC__
#define ARK_ATOMIC_ADD(ptr, val) __atomic_add_fetch((ptr), (val), __ATOMIC_ACQ_REL)
#elif defined(_WIN32)
#define ARK_ATOMIC_ADD(ptr, val) (_InterlockedExchangeAdd((volatile long*)(ptr), (long)(val)) + (val))
#endif

// statfs-based network filesystem detection (NFS/SMB/AFP) for the
// WAL-incompatibility guard in db_init (Risk #3). BSD/Mac expose
// f_fstypename; Linux exposes f_type magic numbers. No portable
// detection on Windows (the network-share hazard is surfaced by the
// caller if it ever appears).
#if !defined(_WIN32) && (defined(__APPLE__) || defined(__linux__) || \
    defined(__FreeBSD__) || defined(__OpenBSD__) || defined(__NetBSD__) || \
    defined(__DragonFly__))
#  define ARK_HAVE_STATFS 1
#  if defined(__APPLE__) || defined(__FreeBSD__) || defined(__OpenBSD__) || \
      defined(__NetBSD__) || defined(__DragonFly__)
#    include <sys/mount.h>
#  elif defined(__linux__)
#    include <sys/vfs.h>
#  endif
#endif

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdarg.h>
#include <ctype.h>
#include <time.h>
#include <errno.h>
#include <limits.h>

#include "deps/sqlite/sqlite3.h"

// ── Config Defaults ─────────────────────────────────────────────────
//
// Configuration via environment (or a ./.env file in the working
// directory; real environment variables always win over .env values).
// No endpoint defaults to a vendor URL — nothing phones home unless
// explicitly configured:
//
//   ARKILIAN_S3_ENDPOINT    Base URL of any S3-compatible endpoint
//                           (path-style addressing, e.g. a MinIO/R2/
//                           self-hosted server). No endpoint defaults to
//                           a vendor URL — nothing ships anywhere unless
//                           explicitly configured.
//   ARKILIAN_S3_BUCKET      Destination bucket
//   ARKILIAN_S3_REGION      Signature region (default us-east-1)
//   ARKILIAN_S3_ACCESS_KEY / ARKILIAN_S3_SECRET_KEY
//                           Per-database SigV4 credentials. The ONLY
//                           credential the client holds; requests are
//                           signed locally and no bearer token exists.
//   ARKILIAN_S3_PREFIX      Key prefix for this database's snapshot,
//                           chunks, and manifest (e.g. "user-42-appdb").

#define DEFAULT_DB_PATH "app.sqlite"
#define DEFAULT_BACKUP_PATH "backup.sqlite"
#define DEFAULT_BACKUP_INTERVAL 3600

#define BATCH_SIZE 100
#define POLL_INTERVAL_MS 2000

#define CHUNK_FLUSH_INTERVAL_SEC   1
#define CHUNK_MAX_SIZE_BYTES       (4 * 1024 * 1024)   // 4 MB uncompressed
#define CHUNK_MAX_ENTRIES          100000               // ~100K rows (belt-and-suspenders)
#define CHUNK_PREFIX               "chunks"

// Internal outbox schema version (see _arkilian_meta.schema_version).
#define ARKILIAN_SCHEMA_VERSION 1

// Default soft ceiling on _pending_backup rows. Capture pauses (skips the
// INSERT) once the queue reaches this depth so the outbox can never grow
// without bound and exhaust the disk the primary database lives on —
// keeping spec §0 ("backup must never break the application") intact even
// during a prolonged push-endpoint outage. Shipping drains the queue and
// capture resumes automatically when the depth drops back below the cap.
// db_backup_is_healthy() flips to 0 at this depth so the loss of capture
// is visible via monitoring, not silent. Override with
// ARKILIAN_MAX_QUEUE_DEPTH. The cap is baked into the capture triggers at
// sync time; changing the env requires a restart (or db_resync_triggers)
// to take effect.
#define ARKILIAN_DEFAULT_MAX_QUEUE_DEPTH 100000

// ── Struct Definitions ──────────────────────────────────────────────

// WAL chunk accumulator.
// One uploaded WAL chunk, as recorded in {prefix}/manifest.json.
// The manifest is the client's only registry of shipped chunks — without
// it, incremental hydration would have no index of what was uploaded.
typedef struct {
  char     *s3_key;    // object key relative to the bucket (malloc'd)
  char     *sha256;    // lowercase hex digest of the object body (malloc'd)
  uint64_t  lsn_start; // first outbox row id in the chunk
  uint64_t  lsn_end;   // last outbox row id in the chunk
} ark_manifest_chunk;

// One chunk per flush thread; reset after each successful S3 PUT.
// Bounded by time (CHUNK_FLUSH_INTERVAL_SEC) and size (CHUNK_MAX_SIZE_BYTES).
typedef struct {
  char     *buffer;         // growing SQL text accumulator (length-prefixed)
  size_t    len;            // bytes written
  size_t    cap;            // allocated capacity
  uint64_t  lsn_start;      // first outbox row id in this chunk
  uint64_t  lsn_end;        // last outbox row id in this chunk
  uint32_t  entry_count;    // number of rows batched
  uint64_t  byte_count;     // uncompressed bytes in this chunk
  time_t    opened_at;      // when the chunk started accumulating
  time_t    last_s3_flush;  // last successful S3 PUT (0 = never)
} wal_chunk;

struct arkilian {
  sqlite3 *handle;            // Primary connection (game / application thread)
  sqlite3 *backup_db;         // Dedicated connection (flush/shipping thread)
  sqlite3 *snapshot_db;       // Dedicated connection (hourly snapshot thread)
  char *db_path;
  int is_open;
  int sync_initialized;        // 1 once wake/payload/manifest/log mutexes are initialized
  int last_error_code;
  char last_error_msg[256];

  // Statement pool for caller
  sqlite3_stmt **stmts;
  unsigned char *stmt_is_ddl;   // parallel to stmts[]: 1 = DDL statement
  int stmt_count;
  int stmt_capacity;
  int stmt_current;

  // Configuration — the ONLY destination is S3-compatible object storage.
  // SigV4 requests are signed locally; no other credential exists.
  char *backup_path;
  char *s3_endpoint;
  char *s3_bucket;
  char *s3_region;
  char *s3_access_key;
  char *s3_secret_key;
  char *s3_prefix;              // S3 key prefix (ARKILIAN_S3_PREFIX)
  volatile int s3_creds_loaded; // 1 when the S3 destination is fully configured
  int backup_interval;
  int outbox_durable;          // 1 = capture outbox is synchronous=FULL (default)
  volatile int backup_enabled; // runtime kill-switch (written under wake_mutex)

  wal_chunk chunk;               // current chunk accumulator
  int chunk_interval;
  // Drain watermark: the highest outbox id whose chunk OBJECT is durably
  // PUT. Rows at or below it are never re-chunked (that would put two
  // overlapping LSN ranges in the manifest, which hydration replays
  // twice). Deleting those outbox rows on flush ack is safe even though
  // the manifest naming the chunk may lag: a crash in that window loses
  // only the incremental *records* — the next hourly snapshot baseline
  // re-covers every row the chunk objects could have carried.
  uint64_t chunk_flushed_upto;

  // Manifest registry: the client-side record of every uploaded WAL chunk,
  // mirrored to {prefix}/manifest.json. Guarded by manifest_mutex; the
  // flush thread appends, the snapshot thread prunes, and db_init seeds
  // it from the last known manifest (restart safety).
  ark_manifest_chunk *manifest_chunks;
  int manifest_chunk_count;
  int manifest_chunk_cap;
  char *manifest_snapshot_key;   // current baseline snapshot's object key
  char *manifest_snapshot_sha;   // its sha256
  uint64_t manifest_baseline_lsn;
  int manifest_pending;          // chunk records added since last manifest PUT
  time_t manifest_last_upload;   // last successful manifest PUT (0 = never)
  // Startup manifest read state. Publishing is gated on it: until the
  // read resolves, no manifest PUT may overwrite a predecessor registry
  // this process could not read (its chunk records are the only pointers
  // to objects whose outbox rows are already deleted). 0 = unresolved
  // (flush loop retries), 1 = resolved (adopted a registry, or confirmed
  // none exists), -1 = resolved-unreadable (frozen: shipping continues
  // but nothing is published — loudly logged by the seed path).
  volatile int manifest_seed_resolved;

  // Optional manifest authenticity key (ARKILIAN_MANIFEST_HMAC_KEY). When
  // set, every published manifest.json is accompanied by
  // {prefix}/manifest.sig — an HMAC-SHA-256 over the exact manifest bytes.
  // Hydration (and the startup seed path, which shares ark_manifest_fetch)
  // verify it before trusting ANY manifest content: the object store is no
  // longer the root of trust for the restore protocol. The key must live
  // OUTSIDE the storage it protects; hydration reads it from the process
  // environment (set it as a real env var, not only .env).
  char *manifest_hmac_key;

  // Cumulative count of WAL chunks that were durably uploaded AND recorded
  // in the manifest registry. Backs db_backup_chunk_count() — which
  // previously returned a 1/0 "has ever flushed" value under a counter's
  // name, so dashboards read a boolean as a total.
  uint64_t chunks_flushed_total;

  // Background thread tracking & synchronization
  volatile int shutdown_requested;
#ifdef _WIN32
  HANDLE backup_thread_handle;
  HANDLE flush_thread_handle;
  CRITICAL_SECTION wake_mutex;
  CONDITION_VARIABLE wake_cond;
#else
  pthread_t backup_thread_id;
  int backup_thread_running;
  pthread_t flush_thread_id;
  int flush_thread_running;
  pthread_mutex_t wake_mutex;
  pthread_cond_t wake_cond;
#endif

  volatile int wake_flag;
  char last_shipped_payload[1024];
  char wal_last_buf[1024];
#ifdef _WIN32
  CRITICAL_SECTION payload_mutex;
  CRITICAL_SECTION manifest_mutex;
  CRITICAL_SECTION log_mutex;
#else
  pthread_mutex_t payload_mutex;
  pthread_mutex_t manifest_mutex; // guards the manifest chunk registry
  pthread_mutex_t log_mutex;     // guards log_fn/log_ctx pair
#endif

  // Transaction state tracking
  int in_batch_txn;
  sqlite3_stmt *begin_stmt;
  sqlite3_stmt *commit_stmt;
  sqlite3_stmt *rollback_stmt;

  // Monitoring (spec §9). Seconds-based (not ms): a 32-bit int is never
  // torn on any platform — a 64-bit heartbeat could be read half-written
  // on 32-bit ARM and cause spurious unhealthy alerts. All cross-thread
  // accesses to these fields go through ARK_LOAD/ARK_STORE.
  volatile int last_heartbeat_sec;      // flush thread liveness (monotonic)
  volatile int last_snapshot_heartbeat_sec; // hourly snapshot thread liveness
  ark_log_fn_t log_fn;                  // optional structured log sink
  void *log_ctx;

  // Schema-change authorizer (Risk #1 / spec §1): set when DDL bypasses
  // our wrapper via the raw handle returned by db_get_handle(). The next
  // wrapped dispatch (or db_resync_triggers) clears it after re-syncing
  // the capture triggers. Accessed concurrently by multiple db_exec threads
  // and the authorizer callback, so all accesses use ARK_LOAD/STORE.
  volatile int triggers_dirty;
  volatile int trigger_sync_in_progress; // guards authorizer vs our own sync
  volatile int in_wrapped_dispatch;     // db_exec/db_step in progress: suppress
                                       // the raw-DDL warning for DDL the wrapper
                                       // already auto-re-syncs (see on_schema_authorizer)
  volatile int auto_resync_triggers;   // opt-in: resync on next wrapped dispatch
                                       // when triggers_dirty is set (raw-handle DDL users)
volatile int capture_paused;         // sticky: set when outbox hits cap (CDC rows
                                        // are being dropped); cleared on successful
                                        // snapshot upload (the gap is recovered)
};

// ── Helper Prototypes ───────────────────────────────────────────────

static void load_env(void);
static const char *get_env_default(const char *env_var, const char *default_val);
static int get_env_int_default(const char *env_var, int default_val);
static long outbox_cap(void);
#ifdef _WIN32
DWORD WINAPI run_hourly_backup(LPVOID arg);
DWORD WINAPI run_wal_flush(LPVOID arg);
#else
void *run_hourly_backup(void *arg);
void *run_wal_flush(void *arg);
#endif

// v2 forward declarations (used before definition)
static char *s3_presign_put(arkilian *db, const char *key, long expires_sec);
static int upload_to_s3(arkilian *db, const char *signed_url,
                          const char *file_path);
static int has_direct_s3(arkilian *db);
static void manifest_registry_lock(arkilian *db);
static void manifest_registry_unlock(arkilian *db);
static int manifest_registry_append(arkilian *db, char *s3_key, char *sha256,
                                    uint64_t lsn_start, uint64_t lsn_end);
static void manifest_registry_prune_upto(arkilian *db, uint64_t baseline_lsn);
static int manifest_registry_upload(arkilian *db, const char *snapshot_key,
                                    const char *snapshot_sha,
                                    uint64_t baseline_lsn);
static void manifest_registry_maybe_upload(arkilian *db);
static int manifest_registry_seed(arkilian *db);
char *db_s3_presign_get(arkilian *db, const char *key, long expires_sec);

// ── Environment Loader ──────────────────────────────────────────────

static const char *get_env_default(const char *env_var, const char *default_val) {
  const char *val = getenv(env_var);
  return (val && strlen(val) > 0) ? val : default_val;
}

static int get_env_int_default(const char *env_var, int default_val) {
  const char *val = getenv(env_var);
  if (!val || strlen(val) == 0) return default_val;
  // atoi() silently returns 0 for malformed input, which would be
  // indistinguishable from a legitimate "0" and bypass the caller's
  // default (e.g. ARKILIAN_BACKUP_INTERVAL=oops quietly becoming 0).
  // strtol reports parse failure via endptr so we fall back to the
  // documented default instead of producing a silent misconfiguration.
  char *end = NULL;
  errno = 0;
  long parsed = strtol(val, &end, 10);
  if (end == val || *end != '\0' || errno != 0 ||
      parsed < INT_MIN || parsed > INT_MAX) {
    return default_val;
  }
  return (int)parsed;
}


// Boolean env var accepting 1/0/true/false/yes/no.
static int get_env_bool_default(const char *env_var, int default_val) {
  const char *val = getenv(env_var);
  if (!val || strlen(val) == 0) return default_val;
  if (strcasecmp(val, "true") == 0 || strcasecmp(val, "yes") == 0 ||
      strcmp(val, "1") == 0) return 1;
  if (strcasecmp(val, "false") == 0 || strcasecmp(val, "no") == 0 ||
      strcmp(val, "0") == 0) return 0;
  // Permissive truthiness fallback for unrecognized boolean env values.
  // strtol (not atoi) avoids bugprone-unchecked-string-to-number-conversion
  // and yields 0 (falsy) for malformed input — the documented behavior for
  // an unparseable env var that isn't an explicit "true/yes/1".
  char *end = NULL;
  errno = 0;
  long parsed = strtol(val, &end, 10);
  if (end == val || *end != '\0' || errno != 0) return 0;
  return parsed != 0;
}

// Soft ceiling on _pending_backup rows — see ARKILIAN_DEFAULT_MAX_QUEUE_DEPTH.
// Read fresh so db_resync_triggers (which re-runs sync_backup_triggers)
// and db_backup_is_healthy observe env changes without a restart for the
// health gate; the trigger-baked literal takes effect on the next sync.
static long outbox_cap(void) {
  long cap = (long)get_env_int_default("ARKILIAN_MAX_QUEUE_DEPTH",
                                       ARKILIAN_DEFAULT_MAX_QUEUE_DEPTH);
  if (cap < 1) cap = 1;
  return cap;
}

// ── Unique staging files (O_EXCL, owner-only) ───────────────────────
// Creates <base>.arktmp.<pid>.<counter>.<suffix> exclusively and returns a
// writable FILE* (caller fcloses, then unlinks by name). Never collides
// across instances sharing a backup path; never world-readable. Exported
// (class.h) so the snapshot-cycle determinism tests can assert the
// uniqueness contract directly.
FILE *arkilian_unique_tmp(const char *base, const char *suffix,
                          char *out, size_t out_cap) {
  static volatile int g_tmp_counter = 0;
  if (!base || !suffix || !out || out_cap == 0) return NULL;
  for (int attempt = 0; attempt < 8; attempt++) {
    long seq = ARK_ATOMIC_ADD(&g_tmp_counter, 1);
    int n = snprintf(out, out_cap, "%s.arktmp.%ld.%ld.%s",
                     base, (long)ARK_GETPID(), seq, suffix);
    if (n <= 0 || (size_t)n >= out_cap) return NULL;
    int fd = ARK_OPEN_EXCL(out);
    if (fd < 0) {
      if (errno == EEXIST) continue;  // astronomically unlikely; re-sequence
      return NULL;
    }
    FILE *f = ARK_FDOPEN(fd, "wb");
    if (!f) { ARK_CLOSE_FD(fd); unlink(out); return NULL; }
    return f;
  }
  return NULL;
}

// ── Structured logging ──────────────────────────────────────────────
// Every diagnostic goes through ark_log: applications can install a
// callback (db_set_log_callback) to route messages into their own logger;
// the default sink is stderr, preserving the historical behavior.

static void default_log_sink(ark_log_level_t level, const char *msg, void *ctx) {
  (void)ctx;
  const char *lvl = (level == ARK_LOG_ERROR) ? "error"
                   : (level == ARK_LOG_WARN)  ? "warn"
                   : (level == ARK_LOG_INFO)  ? "info" : "debug";
  fprintf(stderr, "arkilian: [%s] %s\n", lvl, msg);
}

// Global sink for messages emitted before a handle exists (init-time
// warnings) and as the fallback when a per-handle callback is not set.
static ark_log_fn_t g_default_log_fn = NULL;
static void *g_default_log_ctx = NULL;
#ifndef _WIN32
static pthread_mutex_t g_log_mutex = PTHREAD_MUTEX_INITIALIZER;
#define ARK_GLOG_LOCK()   pthread_mutex_lock(&g_log_mutex)
#define ARK_GLOG_UNLOCK() pthread_mutex_unlock(&g_log_mutex)
#else
static CRITICAL_SECTION g_log_mutex;
static BOOL CALLBACK log_mutex_init_w(PINIT_ONCE once, PVOID param, PVOID *ctx) {
  (void)once; (void)param; (void)ctx;
  InitializeCriticalSection(&g_log_mutex);
  return TRUE;
}
static void ensure_log_mutex(void) {
  static INIT_ONCE once = INIT_ONCE_STATIC_INIT;
  InitOnceExecuteOnce(&once, log_mutex_init_w, NULL, NULL);
}
#define ARK_GLOG_LOCK()   do { ensure_log_mutex(); EnterCriticalSection(&g_log_mutex); } while(0)
#define ARK_GLOG_UNLOCK() LeaveCriticalSection(&g_log_mutex)
#endif

#ifdef _WIN32
#define ARK_LOG_LOCK(db)   EnterCriticalSection(&(db)->log_mutex)
#define ARK_LOG_UNLOCK(db) LeaveCriticalSection(&(db)->log_mutex)
#else
#define ARK_LOG_LOCK(db)   pthread_mutex_lock(&(db)->log_mutex)
#define ARK_LOG_UNLOCK(db) pthread_mutex_unlock(&(db)->log_mutex)
#endif

void db_set_default_log_callback(ark_log_fn_t fn, void *ctx) {
  ARK_GLOG_LOCK();
  g_default_log_fn = fn;
  g_default_log_ctx = ctx;
  ARK_GLOG_UNLOCK();
}

void db_set_log_callback(arkilian *db, ark_log_fn_t fn, void *ctx) {
  if (!db) return;
  ARK_LOG_LOCK(db);
  db->log_fn = fn;
  db->log_ctx = ctx;
  ARK_LOG_UNLOCK(db);
}

void ark_log(arkilian *db, ark_log_level_t level, const char *fmt, ...) {
  char buf[1024];
  va_list ap;
  va_start(ap, fmt);
  vsnprintf(buf, sizeof(buf), fmt, ap);
  va_end(ap);
  buf[sizeof(buf) - 1] = '\0';

  ark_log_fn_t fn = NULL;
  void *ctx = NULL;
  if (db) {
    ARK_LOG_LOCK(db);
    fn = db->log_fn;
    ctx = db->log_ctx;
    ARK_LOG_UNLOCK(db);
  }
  if (!fn) {
    ARK_GLOG_LOCK();
    fn = g_default_log_fn;
    ctx = g_default_log_ctx;
    ARK_GLOG_UNLOCK();
  }

  if (fn) {
    fn(level, buf, ctx);
  } else {
    default_log_sink(level, buf, NULL);
  }
}

// Monotonic milliseconds, for heartbeats and latency instrumentation.
// POSIX uses clock_gettime(CLOCK_MONOTONIC); Windows uses
// QueryPerformanceCounter (high-resolution, monotonic). Both return a
// millisecond value suitable for heartbeat age computation.
static long long now_ms_mono(void) {
#ifndef _WIN32
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return (long long)ts.tv_sec * 1000LL + ts.tv_nsec / 1000000LL;
#else
  static LARGE_INTEGER freq = {0};
  if (freq.QuadPart == 0) QueryPerformanceFrequency(&freq);
  LARGE_INTEGER count;
  QueryPerformanceCounter(&count);
  return (long long)(count.QuadPart * 1000LL / freq.QuadPart);
#endif
}

// ── libcurl one-time global init ────────────────────────────────────
// libcurl requires curl_global_init() before any thread calls
// curl_easy_init(); concurrent first use from multiple threads is
// undefined. The once-guard makes db_init idempotent-safe; cleanup is
// deliberately never called (see the comment at the call site).

#ifndef _WIN32
static void curl_global_init_once(void) {
  curl_global_init(CURL_GLOBAL_DEFAULT);
}
#else
static BOOL CALLBACK curl_global_init_once_w(PINIT_ONCE once, PVOID param, PVOID *ctx) {
  (void)once; (void)param; (void)ctx;
  curl_global_init(CURL_GLOBAL_DEFAULT);
  return TRUE;
}
#endif

static void ensure_curl_global_init(void) {
#ifndef _WIN32
  static pthread_once_t once = PTHREAD_ONCE_INIT;
  pthread_once(&once, curl_global_init_once);
#else
  static INIT_ONCE once = INIT_ONCE_STATIC_INIT;
  InitOnceExecuteOnce(&once, curl_global_init_once_w, NULL, NULL);
#endif
}

// Apply ./.env at most ONCE per process. load_env is called from every
// db_init; without a guard it re-injects .env values AFTER the application
// has unsetenv()'d them (e.g. between two handles in the same process),
// silently overriding the operator's runtime intent — the process-global
// env pollution that broke multi-instance isolation (Risk #5). Real
// environment variables always win on the FIRST load (setenv overwrite=0);
// after that, the application's runtime setenv / unsetenv IS the live
// configuration and is respected for every subsequent handle. Runtime
// ./.env edits mid-process are not re-applied to new handles — use
// db_backup_set_enabled or setenv instead.
static void load_env_impl(void) {
  FILE *fp = fopen(".env", "r");
  if (!fp) return;
  char line[256];
  while (fgets(line, sizeof(line), fp)) {
    // Discard the remainder of overlong lines so a truncated
    // fragment is never parsed as a separate KEY=VALUE pair.
    size_t len = strlen(line);
    if (len > 0 && line[len - 1] != '\n' && !feof(fp)) {
      int c;
      while ((c = fgetc(fp)) != EOF && c != '\n') { /* drain */ }
    }
    char *save = NULL;
    char *key = strtok_r(line, "=", &save);
    char *val = strtok_r(NULL, "\n\r", &save);
    // A real environment variable always wins over a ./.env value —
    // a stray .env in the working directory must never override the
    // deployment's explicit configuration.
    if (key && val && !getenv(key)) {
#ifdef _WIN32
      _putenv_s(key, val);
#else
      setenv(key, val, 0);
#endif
    }
  }
  fclose(fp);
}

#ifdef _WIN32
static BOOL CALLBACK load_env_once_w(PINIT_ONCE once, PVOID param, PVOID *ctx) {
  (void)once; (void)param; (void)ctx;
  load_env_impl();
  return TRUE;
}
#endif

static void load_env(void) {
#ifndef _WIN32
  static pthread_once_t once = PTHREAD_ONCE_INIT;
  pthread_once(&once, load_env_impl);
#else
  static INIT_ONCE once = INIT_ONCE_STATIC_INIT;
  InitOnceExecuteOnce(&once, load_env_once_w, NULL, NULL);
#endif
}

// ── Small Shared Helpers ────────────────────────────────────────────

// Portable UTC broken-down time: gmtime_r (POSIX) vs gmtime_s (Windows —
// REVERSED argument order, errno_t return). Writes to *out and returns
// out on success, NULL on failure. Kept in lockstep with hydration.c's
// shim of the same name: both SigV4 signers need it and MSVC has no
// gmtime_r, so the shipping path would not compile on the Windows leg.
static struct tm *ark_gmtime_utc(const time_t *tp, struct tm *out) {
#ifdef _WIN32
  return (gmtime_s(out, tp) == 0) ? out : NULL;
#else
  return gmtime_r(tp, out);
#endif
}

// Escape a string for embedding inside a SQL single-quoted literal
// (doubles every single quote).  Caller frees.
static char *sql_literal_escape(const char *s) {
  size_t len = 0;
  for (const char *p = s; *p; p++) len += (*p == '\'') ? 2 : 1;
  char *out = malloc(len + 1);
  if (!out) return NULL;
  char *w = out;
  for (const char *p = s; *p; p++) {
    if (*p == '\'') *w++ = '\'';
    *w++ = *p;
  }
  *w = '\0';
  return out;
}

// Skip leading whitespace and SQL comments so DDL verbs are detected
// even when the statement doesn't start at column 0.
static const char *skip_sql_prefix(const char *sql) {
  for (;;) {
    while (*sql && isspace((unsigned char)*sql)) sql++;
    if (sql[0] == '-' && sql[1] == '-') {
      while (*sql && *sql != '\n') sql++;
      continue;
    }
    if (sql[0] == '/' && sql[1] == '*') {
      const char *e = strstr(sql + 2, "*/");
      sql = e ? e + 2 : sql + strlen(sql);
      continue;
    }
    return sql;
  }
}

// Extract the host component of a URL into a caller-provided buffer.
// Strips any user@info and :port. Returns the host length, or 0 on
// failure / parse error. e.g. "http://user@127.0.0.1:9000/x" -> "127.0.0.1".


// Storage-safe host check: EXCLUDES link-local (169.254.0.0/16 and IPv6
// fe80::) because that range hosts the cloud
// instance-metadata service (IMDS at 169.254.169.254 on AWS/GCP/Azure).
// A tampered manifest yielding a presigned URL pointing at IMDS
// would otherwise have the client upload the full database to the
// metadata service.
static int host_is_storage_safe(const char *host) {
  if (!host || !*host) return 0;
  if (host[0] == '[') {
    if (strncmp(host, "[::1]", 5) == 0) return 1;
    // NO [fc / [fd ULA — AWS IMDSv2 is reachable at fd00:ec2::254 and
    // must never be treated as a safe storage destination. ULA prefixes
    // are NOT metadata-endpoint boundaries; rejecting them closes the
    // exfiltration path where a tampered manifest or misconfigured
    // storage configuration supplies a presigned URL pointing at
    // fd00:ec2::254 and the client uploads the full database to the
    // cloud instance-metadata service.
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
    // sscanf's %u overflow on malformed input yields ULONG_MAX, which the
    // >= 16 && <= 31 range guard rejects — the original value never reaches
    // a wrong decision branch, so the unchecked-conversion warning is moot.
    // NOLINTNEXTLINE(bugprone-unchecked-string-to-number-conversion,cert-err34-c)
    if (sscanf(host, "172.%u.", &second) == 1 && second >= 16 && second <= 31)
      return 1;
  }
  return 0;
}

// A URL is "transport-safe" iff it is HTTPS, OR it points at a local
// address (loopback / RFC1918 / link-local) for dev. Operators with an
// internal-but-non-RFC1918 cleartext storage endpoint may opt in with
// ARKILIAN_ALLOW_INSECURE=1 — the loud-failure default keeps a
// misconfiguration from leaking request signatures on the wire.

// Extract the host component of a URL into a caller-provided buffer.
// Strips any user@info and :port. Returns the host length, or 0 on
// failure / parse error. e.g. "http://user@127.0.0.1:9000/x" -> "127.0.0.1".
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

// Well-known object-storage providers, matched on host SUFFIX so regional
// variants (bucket.s3.us-east-2.amazonaws.com) are covered. A pre-signed
// URL whose host matches one of these is treated as a legitimate storage
// destination. Operators with a self-hosted MinIO or other custom storage
// add its host via ARKILIAN_STORAGE_HOSTS="host1,host2" (comma-separated,
// suffix-matched).
static int host_is_known_storage(const char *host) {
  if (!host || !*host) return 0;
  // AWS S3 (and s3-website, s3-accelerate, dualstack)
  if (strstr(host, ".amazonaws.com")) return 1;
  // Google Cloud Storage
  if (strcmp(host, "storage.googleapis.com") == 0) return 1;
  if (strstr(host, ".storage.googleapis.com")) return 1;
  // Azure Blob
  if (strstr(host, ".blob.core.windows.net")) return 1;
  // Backblaze B2
  if (strstr(host, ".backblazeb2.com")) return 1;
  // Cloudflare R2
  if (strstr(host, ".r2.cloudflarestorage.com")) return 1;
  // Wasabi
  if (strstr(host, ".wasabisys.com")) return 1;
  // DigitalOcean Spaces
  if (strstr(host, ".digitaloceanspaces.com")) return 1;
  return 0;
}

// Returns 1 (and parses `extra_hosts`) when a URL's host is an allowed
// storage destination: a well-known provider, a storage-safe local address
// (loopback / RFC1918 — but NOT link-local 169.254.x which hosts IMDS),
// or a host in the operator-provided allowlist.
// Guards against SSRF: a tampered manifest or a compromised storage host
// list that yields an upload_url pointing at cloud metadata (169.254.169.254)
// or an internal service is refused here, so the customer's snapshot is
// never exfiltrated.
static int url_is_allowed_storage(const char *url) {
  if (!url) return 0;
  char host[256];
  if (url_host(url, host, sizeof(host)) == 0) return 0;
  if (host_is_known_storage(host)) return 1;
  if (host_is_storage_safe(host)) return 1;
  // Operator allowlist (comma-separated, exact-or-suffix match)
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
      // exact match, or host ends with ".<allowed>" (subdomain), or equals
      if (strcmp(host, tok) == 0) return 1;
      if (hlen > tlen + 1 && host[hlen - tlen - 1] == '.' &&
          strcmp(host + hlen - tlen, tok) == 0) return 1;
      tok = strtok_r(NULL, ",", &save);
    }
  }
  return 0;
}

// (Risk #3) Detect a network filesystem (NFS/SMB/AFP/CIFS) hosting the
// database. SQLite WAL mode uses a mmap'd <db>-shm file for shared
// memory; on network mounts the mmap is either rejected (SQLITE_IOERR_LOCK)
// or, worse, silently produces torn locks and database corruption. A
// clear local filesystem reads as "not network" (no false positives on
// the dev/CI platforms); when statfs is unavailable, returns 0 so the
// existing WAL path is taken unchanged.
//
// stat the parent directory of the database (the file may not exist yet
// at db_init time, but its directory does). Fall back to the current
// directory if no directory component is present or the parent is the
// root.
#ifdef ARK_HAVE_STATFS
static int fs_is_network(const char *path) {
  if (!path || !*path) return 0;
  struct statfs s;
  if (statfs(path, &s) != 0) {
    char dir[4096];
    size_t n = strlen(path);
    if (n >= sizeof(dir)) n = sizeof(dir) - 1;
    memcpy(dir, path, n);
    dir[n] = '\0';
    char *slash = strrchr(dir, '/');
    const char *probe = ".";
    if (slash == dir) probe = "/";
    else if (slash) { *slash = '\0'; probe = dir; }
    if (statfs(probe, &s) != 0) return 0; // unknown — do not false-positive
  }
#  if defined(__APPLE__) || defined(__FreeBSD__) || defined(__OpenBSD__) || \
      defined(__NetBSD__) || defined(__DragonFly__)
  const char *t = s.f_fstypename;
  return (strcmp(t, "nfs") == 0 ||
          strcmp(t, "smbfs") == 0 ||
          strcmp(t, "smb") == 0 ||
          strcmp(t, "cifs") == 0 ||
          strcmp(t, "afpfs") == 0 ||
          strcmp(t, "webdav") == 0) ? 1 : 0;
#  elif defined(__linux__)
  // Magic numbers from linux/magic.h. Cast through unsigned long so the
  // comparison is sign-safe on 32- and 64-bit targets (the largest value,
  // 0xFF534D42, does not fit in signed int). Modern SMB2/SMB3 mounts report
  // CIFS_MAGIC_NUMBER, so the one value covers them.
  switch ((unsigned long)s.f_type) {
    case 0x00006969UL: return 1; // NFS_SUPER_MAGIC
    case 0xFF534D42UL: return 1; // CIFS_MAGIC_NUMBER (cifs / smb2 / smb3)
    case 0x0000517BUL: return 1; // SMB_SUPER_MAGIC (legacy)
    default:           return 0;
  }
#  else
  return 0;
#  endif
}
#else
static int fs_is_network(const char *path) {
  (void)path;
  return 0;
}
#endif

// ── Trigger Auto-Generator ──────────────────────────────────────────

static const char *RESERVED_TABLES[] = {
    "_pending_backup", "_dead_backup", "_arkilian_meta", "sqlite_sequence", NULL
};

static int is_reserved_table(const char *name) {
  if (!name) return 1;
  if (strncmp(name, "sqlite_", 7) == 0) return 1;
  for (int i = 0; RESERVED_TABLES[i]; i++) {
    if (strcmp(name, RESERVED_TABLES[i]) == 0) return 1;
  }
  return 0;
}

int sync_backup_triggers(sqlite3 *db, char **err_out) {
  if (!db) return SQLITE_ERROR;
  int rc;
  char *errmsg = NULL;
  int began = 0;

  // Only open our own transaction when the connection is in autocommit
  // mode.  When the caller already holds a transaction (e.g. db_begin
  // batch), join it and leave commit/rollback to the caller.
  if (sqlite3_get_autocommit(db)) {
    rc = sqlite3_exec(db, "BEGIN IMMEDIATE;", NULL, NULL, &errmsg);
    if (rc != SQLITE_OK) {
      if (err_out) *err_out = errmsg;
      else sqlite3_free(errmsg);
      return rc;
    }
    began = 1;
  }

  // Ensure internal outbox & metadata tables exist
  static const char *const kInternalDDL[] = {
    "CREATE TABLE IF NOT EXISTS _pending_backup ("
    "  id INTEGER PRIMARY KEY AUTOINCREMENT,"
    "  payload TEXT NOT NULL,"
    "  attempts INTEGER NOT NULL DEFAULT 0,"
    "  created_at INTEGER NOT NULL DEFAULT (strftime('%s','now')),"
    "  last_attempt_at INTEGER"
    ");",
    "CREATE TABLE IF NOT EXISTS _dead_backup ("
    "  id INTEGER PRIMARY KEY,"
    "  payload TEXT NOT NULL,"
    "  attempts INTEGER NOT NULL,"
    "  failed_reason TEXT,"
    "  created_at INTEGER NOT NULL DEFAULT (strftime('%s','now')),"
    "  dead_lettered_at INTEGER NOT NULL DEFAULT (strftime('%s','now'))"
    ");",
    "CREATE TABLE IF NOT EXISTS _arkilian_meta ("
    "  k TEXT PRIMARY KEY,"
    "  v TEXT"
    ");",
    // Oldest-pending-age monitoring does MIN(created_at) on the app
    // thread; without an index that's a full scan of a (possibly large)
    // backlog.
    "CREATE INDEX IF NOT EXISTS idx_pending_backup_created "
    "  ON _pending_backup(created_at);",
    // Internal-schema version marker: future releases bump
    // ARKILIAN_SCHEMA_VERSION and migrate; a node whose schema version
    // EXCEEDS the running binary's is detected at init instead of
    // corrupting a newer outbox format.
    "INSERT OR IGNORE INTO _arkilian_meta (k, v) VALUES ('schema_version', '1');",
    NULL
  };
  for (int i = 0; kInternalDDL[i]; i++) {
    rc = sqlite3_exec(db, kInternalDDL[i], NULL, NULL, &errmsg);
    if (rc != SQLITE_OK) {
      if (began) sqlite3_exec(db, "ROLLBACK;", NULL, NULL, NULL);
      if (err_out) *err_out = errmsg;
      else sqlite3_free(errmsg);
      return rc;
    }
  }

  // Scan only real tables. pragma_table_list's type column distinguishes
  // 'table' from 'virtual' (FTS5, rtree) and 'shadow' (FTS shadow
  // tables) — CREATE TRIGGER ON a virtual table is rejected by SQLite,
  // so those MUST be excluded or the whole scan fails and (per spec
  // §0/§1) the game would be prevented from starting. Fall back to
  // sqlite_master on SQLite versions without pragma_table_list.
  sqlite3_stmt *table_stmt = NULL;
  rc = sqlite3_prepare_v2(db,
      "SELECT name FROM pragma_table_list "
      "WHERE schema = 'main' AND type = 'table'", -1, &table_stmt, NULL);
  if (rc != SQLITE_OK) {
    rc = sqlite3_prepare_v2(db,
        "SELECT name FROM sqlite_master WHERE type = 'table'",
        -1, &table_stmt, NULL);
  }
  if (rc != SQLITE_OK) {
    if (err_out) *err_out = sqlite3_mprintf("prepare table list: %s", sqlite3_errmsg(db));
    if (began) sqlite3_exec(db, "ROLLBACK;", NULL, NULL, NULL);
    return rc;
  }

  while ((rc = sqlite3_step(table_stmt)) == SQLITE_ROW) {
    const char *table = (const char *)sqlite3_column_text(table_stmt, 0);
    if (!table || is_reserved_table(table)) continue;

    // Per-table allocations declared up-front and NULL-initialized so that
    // EVERY OOM/error path (including ones reached before they are
    // assigned) can free them safely (NULL-free is a no-op) — instead of
    // either leaking them or using indeterminate values after a forward
    // jump over their declaration (which is UB in C).
    char **cols = NULL;
    int *pk_ranks = NULL;
    int ncols = 0, cap = 0;
    char *raw_cols = NULL;
    char *new_vals = NULL;
    char *replace_lit = NULL;
    char *delete_prefix = NULL;
    char *delete_expr = NULL;
    char *pragma_sql = NULL;
    sqlite3_stmt *col_stmt = NULL;

    pragma_sql = sqlite3_mprintf("PRAGMA table_xinfo(\"%w\");", table);
    if (!pragma_sql) { rc = SQLITE_NOMEM; goto oom; }
    rc = sqlite3_prepare_v2(db, pragma_sql, -1, &col_stmt, NULL);
    sqlite3_free(pragma_sql);
    pragma_sql = NULL;

    if (rc != SQLITE_OK) {
      // Build the message BEFORE finalizing table_stmt — `table` points
      // into table_stmt's row buffer and is invalid after finalize.
      if (err_out) *err_out = sqlite3_mprintf("prepare table_info(%s): %s", table, sqlite3_errmsg(db));
      sqlite3_finalize(col_stmt);
      sqlite3_finalize(table_stmt);
      if (began) sqlite3_exec(db, "ROLLBACK;", NULL, NULL, NULL);
      return rc;
    }

    // Column collection (names + primary-key ranks)

    while ((rc = sqlite3_step(col_stmt)) == SQLITE_ROW) {
      const char *col = (const char *)sqlite3_column_text(col_stmt, 1);
      if (!col) continue;
      // table_xinfo's 7th column flags hidden (1) and generated (2/3)
      // columns — those can never appear in INSERT/REPLACE column lists,
      // so capturing them would produce payloads that fail on replay.
      if (sqlite3_column_count(col_stmt) > 6 &&
          sqlite3_column_int(col_stmt, 6) != 0) continue;
      if (ncols == cap) {
        int ncap = cap ? cap * 2 : 8;
        char **nc = realloc(cols, (size_t)ncap * sizeof(char *));
        int *nr = realloc(pk_ranks, (size_t)ncap * sizeof(int));
        // A failed realloc leaves the original block intact; a successful
        // one moved it — adopt each result independently to stay consistent.
        if (nc) cols = nc;
        if (nr) pk_ranks = nr;
        if (!nc || !nr) {
          sqlite3_finalize(col_stmt);
          goto oom;
        }
        cap = ncap;
      }
      cols[ncols] = strdup(col);
      pk_ranks[ncols] = sqlite3_column_int(col_stmt, 5);
      if (!cols[ncols]) { sqlite3_finalize(col_stmt); goto oom; }
      ncols++;
    }
    sqlite3_finalize(col_stmt);
    if (rc != SQLITE_DONE) goto fail;

    if (ncols == 0) goto next_table;

    // Raw identifier list: "c1", "c2"  and  NEW-value expression:
    // quote(NEW."c1") || ', ' || quote(NEW."c2")
    for (int i = 0; i < ncols; i++) {
      char *next_cols, *next_vals;
      if (i == 0) {
        next_cols = sqlite3_mprintf("\"%w\"", cols[i]);
        next_vals = sqlite3_mprintf("quote(NEW.\"%w\")", cols[i]);
      } else {
        next_cols = sqlite3_mprintf("%s, \"%w\"", raw_cols, cols[i]);
        next_vals = sqlite3_mprintf("%s || ', ' || quote(NEW.\"%w\")", new_vals, cols[i]);
      }
      if (!next_cols || !next_vals) {
        sqlite3_free(next_cols); sqlite3_free(next_vals);
        goto oom;
      }
      sqlite3_free(raw_cols); sqlite3_free(new_vals);
      raw_cols = next_cols; new_vals = next_vals;
    }

    // Payload SQL texts.  Literals get a second escape pass so single
    // quotes inside identifiers survive embedding in the trigger's
    // string literal; expressions stay raw.
    char *replace_lit_raw = sqlite3_mprintf("REPLACE INTO \"%w\" (%s) VALUES (", table, raw_cols);
    char *delete_prefix_raw = sqlite3_mprintf("DELETE FROM \"%w\" WHERE ", table);
    replace_lit = sql_literal_escape(replace_lit_raw ? replace_lit_raw : "");
    delete_prefix = sql_literal_escape(delete_prefix_raw ? delete_prefix_raw : "");
    sqlite3_free(replace_lit_raw); sqlite3_free(delete_prefix_raw);
    if (!replace_lit || !delete_prefix) goto oom;

    // DELETE payloads are keyed on the PRIMARY KEY columns for every
    // table that has one. Keying on OLD.rowid is NOT replay-faithful:
    // REPLACE INTO deletes + reinserts, so rowids shift on the
    // destination after any UPDATE — rowid tables without INTEGER
    // PRIMARY KEY desynchronize and every later DELETE hits the wrong
    // row (proven divergence). PK values survive REPLACE, so PK-keyed
    // deletes stay correct for INTEGER, TEXT, and composite keys alike.
    // Tables with NO key at all (plain rowid tables) are unreplayable —
    // REPLACE appends and rowids drift — so they are skipped with a loud
    // warning (spec §1: capture must not be silently bypassed).
    int pk_seen = 0;
    for (int i = 0; i < ncols; i++) {
      if (pk_ranks[i] > 0) pk_seen++;
    }
    if (pk_seen == 0) {
      ark_log(NULL, ARK_LOG_WARN,
              "trigger sync: skipping table %s — it has no PRIMARY KEY, so "
              "row-level replication would diverge on the destination "
              "(REPLACE appends, rowids drift). It will not be captured",
              table);
      free(replace_lit); free(delete_prefix);
      sqlite3_free(raw_cols); sqlite3_free(new_vals);
      replace_lit = delete_prefix = NULL;
      raw_cols = new_vals = NULL;
      goto next_table;
    }
    {
      char *lit_accum = strdup(delete_prefix);
      if (!lit_accum) goto oom;
      int pk_done = 0;
      for (int i = 0; i < ncols; i++) {
        if (pk_ranks[i] == 0) continue;
        char *piece = sqlite3_mprintf(pk_done == 0 ? "\"%w\" = " : " AND \"%w\" = ", cols[i]);
        if (!piece) { free(lit_accum); goto oom; }
        char *new_accum = malloc(strlen(lit_accum) + strlen(piece) + 1);
        if (!new_accum) { sqlite3_free(piece); free(lit_accum); goto oom; }
        strcpy(new_accum, lit_accum);
        strcat(new_accum, piece);
        free(lit_accum); sqlite3_free(piece);
        lit_accum = new_accum;

        char *esc = sql_literal_escape(lit_accum);
        char *expr = sqlite3_mprintf("quote(OLD.\"%w\")", cols[i]);
        if (!esc || !expr) { free(esc); sqlite3_free(expr); free(lit_accum); goto oom; }
        char *next_expr = delete_expr
          ? sqlite3_mprintf("%s || '%s' || %s", delete_expr, esc, expr)
          : sqlite3_mprintf("'%s' || %s", esc, expr);
        free(esc); sqlite3_free(expr);
        sqlite3_free(delete_expr);
        delete_expr = next_expr;
        if (!delete_expr) { free(lit_accum); goto oom; }
        lit_accum[0] = '\0';
        pk_done++;
      }
      free(lit_accum);
    }
    if (!delete_expr) goto oom;

    {
      const char *ops[3][2] = {
          {"ai", "INSERT"}, {"au", "UPDATE"}, {"ad", "DELETE"}
      };

      for (int i = 0; i < 3; i++) {
        char *drop_sql = sqlite3_mprintf("DROP TRIGGER IF EXISTS \"trg_%w_%s\";", table, ops[i][0]);
        if (!drop_sql) goto trigger_oom;
        rc = sqlite3_exec(db, drop_sql, NULL, NULL, &errmsg);
        sqlite3_free(drop_sql);
        if (rc != SQLITE_OK) goto trigger_fail;

        char *create_sql = NULL;
        if (i == 2) {
          create_sql = sqlite3_mprintf(
              "CREATE TRIGGER \"trg_%w_ad\" AFTER DELETE ON \"%w\" BEGIN "
              "INSERT INTO _pending_backup (payload) SELECT (%s) "
              "WHERE (SELECT COUNT(*) FROM _pending_backup) < %ld; END;",
              table, table, delete_expr, outbox_cap());
        } else {
          create_sql = sqlite3_mprintf(
              "CREATE TRIGGER \"trg_%w_%s\" AFTER %s ON \"%w\" BEGIN "
              "INSERT INTO _pending_backup (payload) SELECT ("
              "'%s' || %s || ')') WHERE (SELECT COUNT(*) FROM _pending_backup) < %ld; END;",
              table, ops[i][0], ops[i][1], table, replace_lit, new_vals, outbox_cap());
        }
        if (!create_sql) goto trigger_oom;

        rc = sqlite3_exec(db, create_sql, NULL, NULL, &errmsg);
        sqlite3_free(create_sql);
        if (rc != SQLITE_OK) goto trigger_fail;
      }
      goto triggers_done;

trigger_oom:
      rc = SQLITE_NOMEM;
    trigger_fail:
      // All per-table allocations are released by fail_with_errmsg below
      // (NULL-safe frees), so no inline frees here — a single owner
      // prevents the double-free that the previous inline-free-then-goto
      // pattern incurred.
      goto fail;
    }

triggers_done:
    free(replace_lit); free(delete_prefix); sqlite3_free(delete_expr);
    sqlite3_free(raw_cols); sqlite3_free(new_vals);

next_table:
    for (int i = 0; i < ncols; i++) free(cols[i]);
    free(cols); free(pk_ranks);
    continue;

oom:
    rc = SQLITE_NOMEM;
    if (!errmsg) errmsg = sqlite3_mprintf("out of memory");
    goto fail_with_errmsg;

fail:
    if (!errmsg) errmsg = sqlite3_mprintf("trigger sync failed (rc=%d)", rc);
fail_with_errmsg:
    // Single owner of every per-table allocation: NULL-safe frees so that
    // any entry path (oom before partial assignment, oom mid-build,
    // trigger_fail after assignment, or normal fail) releases exactly the
    // live set once. Fixes the OOM leak where raw_cols/new_vals/
    // replace_lit/delete_prefix/delete_expr were leaked on the OOM paths.
    free(replace_lit);
    free(delete_prefix);
    sqlite3_free(delete_expr);
    sqlite3_free(raw_cols);
    sqlite3_free(new_vals);
    for (int i = 0; i < ncols; i++) free(cols[i]);
    free(cols); free(pk_ranks);
    sqlite3_finalize(col_stmt);
    sqlite3_finalize(table_stmt);
    if (began) sqlite3_exec(db, "ROLLBACK;", NULL, NULL, NULL);
    if (err_out) *err_out = errmsg;
    else sqlite3_free(errmsg);
    return rc;
  }
  sqlite3_finalize(table_stmt);

  if (rc != SQLITE_DONE) {
    if (began) sqlite3_exec(db, "ROLLBACK;", NULL, NULL, NULL);
    if (err_out) *err_out = sqlite3_mprintf("table scan: %s", sqlite3_errmsg(db));
    return rc;
  }

  if (began) {
    rc = sqlite3_exec(db, "COMMIT;", NULL, NULL, &errmsg);
    if (rc != SQLITE_OK) {
      if (err_out) *err_out = errmsg;
      else sqlite3_free(errmsg);
      return rc;
    }
  }

  return SQLITE_OK;
}

// ── Wake Signal Update Hook ─────────────────────────────────────────

static void on_db_update(void *user_data, int op_type, char const *db_name,
                          char const *table_name, sqlite3_int64 row_id) {
  arkilian *db = (arkilian *)user_data;
  (void)op_type; (void)db_name; (void)row_id;
  if (!db || is_reserved_table(table_name)) return;

  // The flag and the signal must be issued under the mutex — otherwise
  // the flush thread can miss the wakeup between its predicate check
  // and pthread_cond_timedwait().
#ifndef _WIN32
  pthread_mutex_lock(&db->wake_mutex);
  db->wake_flag = 1;
  pthread_cond_signal(&db->wake_cond);
  pthread_mutex_unlock(&db->wake_mutex);
#else
  EnterCriticalSection(&db->wake_mutex);
  db->wake_flag = 1;
  WakeConditionVariable(&db->wake_cond);
  LeaveCriticalSection(&db->wake_mutex);
#endif
}

// Schema-change authorizer (Risk #1 / spec §1). DDL executed through
// db_exec / db_prepare/db_step is already intercepted and re-synced by
// apply_ddl_capture; DDL run on the raw handle from db_get_handle() (e.g.
// Prisma/Drizzle/TypeORM/raw sqlite3_exec) bypasses the wrapper and would
// silently desynchronize the capture triggers — across 5,000 businesses,
// routine migrations would quietly miss replication. Installed on the
// primary connection, this observer flags `triggers_dirty` the instant DDL
// happens so monitoring (db_backup_triggers_dirty) can surface the gap
// and the operator can repair with db_resync_triggers(). It NEVER blocks
// (returns SQLITE_OK) and performs no I/O — only an atomic flag store — so
// it is safe from inside SQLite's prepare/step path. CREATE/DROP TRIGGER
// actions are deliberately not in the switch (they are our own
// bookkeeping), and sync_backup_triggers sets trigger_sync_in_progress so
// the authorizer ignores the internal CREATE TABLE IF NOT EXISTS for the
// outbox/meta tables.
static int on_schema_authorizer(void *user_data, int action,
                                const char *detail1, const char *detail2,
                                const char *db_name, const char *trigger_name) {
  (void)detail2; (void)db_name; (void)trigger_name;
  arkilian *db = (arkilian *)user_data;
  if (!db) return SQLITE_OK;
  switch (action) {
    case SQLITE_CREATE_TABLE:
    case SQLITE_ALTER_TABLE:
    case SQLITE_DROP_TABLE:
      // Only base-table DDL desyncs the row-capture triggers (column
      // lists / table existence change). Views, indexes, and virtual
      // tables carry no capture triggers, so their DDL is ignored —
      // flagging it would only trigger a wasteful no-op resync on the
      // game thread.
      if (ARK_LOAD(&db->trigger_sync_in_progress)) return SQLITE_OK;
      if (detail1 && is_reserved_table(detail1)) return SQLITE_OK;
      ARK_STORE(&db->triggers_dirty, 1);
      // DDL routed through db_exec / db_prepare+db_step is re-synced
      // automatically by apply_ddl_capture; only DDL that bypassed the
      // wrapper (raw handle from db_get_handle) leaves the capture stale,
      // so warn only there. The wrapped path is silent here.
      if (!ARK_LOAD(&db->in_wrapped_dispatch)) {
        ark_log(db, ARK_LOG_WARN,
                "schema change bypassed the backup wrapper (action=%d, "
                "object=%s) — likely DDL on the raw handle from "
                "db_get_handle() (Prisma/Drizzle/TypeORM/raw sqlite3_exec). "
                "Capture triggers are stale; call db_resync_triggers() "
                "after the migration to restore realtime backup coverage",
                action, detail1 ? detail1 : "(null)");
      }
      break;
    default:
      break;
  }
  return SQLITE_OK;
}

// ── Backup Shipping & Delivery Thread ───────────────────────────────


// Abort callback for in-flight transfers: returns non-zero when shutdown
// is requested so db_close() never waits out a full curl timeout (10s /
// 30s) joining a thread stuck in a slow request.
static int curl_abort_cb(void *clientp, curl_off_t dltotal, curl_off_t dlnow,
                         curl_off_t ultotal, curl_off_t ulnow) {
  (void)dltotal; (void)dlnow; (void)ultotal; (void)ulnow;
  volatile int *shutdown_flag = (volatile int *)clientp;
  return (shutdown_flag && ARK_LOAD(shutdown_flag)) ? 1 : 0;
}

typedef enum { SHIP_OK = 0, SHIP_RETRY = 1 } ship_result_t;

// Exponential backoff: seconds to wait before retrying a row that has
// failed `attempts` times. Caps at 5 minutes so a prolonged outage
// retries for ~1 hour (20 attempts) instead of dead-lettering after 20s.
// Adaptive request timeout: base seconds plus ~10s per MB of payload, so
// large rows/snapshots aren't dead-lettered by a fixed short window.
static long curl_timeout_sec(size_t bytes, long base) {
  long extra = (long)(bytes / 100000);
  long t = base + extra;
  if (t > 600) t = 600; // hard cap: 10 minutes
  return t;
}

static void wal_chunk_reset(wal_chunk *c) {
  free(c->buffer);
  memset(c, 0, sizeof(*c));
}

// Attempt budget before a chunk's rows are dead-lettered (moved to
// _dead_backup for operator inspection and recovery via arkilian-dlq —
// nothing is ever silently dropped). Configurable via ARKILIAN_MAX_ATTEMPTS.
// Default 100 attempts: with the exponential backoff below that spans a
// very long outage before rows leave the live outbox; operators who want
// faster give-up set the env var lower.
static int max_attempts(void) {
  int m = get_env_int_default("ARKILIAN_MAX_ATTEMPTS", 100);
  if (m < 1) m = 1;
  if (m > 1000000) m = 1000000;
  return m;
}

// Exponential backoff for a chunk that failed `attempts` times: 2^attempts
// seconds, capped at 2^20 (~12 days) so a prolonged outage never hot-loops.
// The sleep is interruptible (db_wal_flush / db_close wake it), so the
// backoff never blocks a clean shutdown.
static long backoff_seconds(int attempts) {
  if (attempts <= 0) return 0;
  if (attempts > 20) attempts = 20;
  return 1L << attempts;
}

// Record a failed flush attempt for every row in the chunk's LSN range and,
// once the attempt budget is exhausted, dead-letter the WHOLE chunk (move
// its rows to _dead_backup with a failure reason and remove them from the
// live outbox) — a permanently-rejected chunk must not pin the queue head
// behind it forever, blocking every younger row from shipping under the
// outbox cap. Returns the chunk's new attempt count on success (the caller
// backs off and will retry), or -1 when the chunk was dead-lettered (the
// caller must reset the buffer to advance the queue head).
static int chunk_failure_logic(arkilian *db, wal_chunk *c,
                               sqlite3_stmt *attempts_stmt,
                               sqlite3_stmt *attempts_max_stmt,
                               sqlite3_stmt *dead_letter_stmt,
                               sqlite3_stmt *dlq_delete_stmt) {
  sqlite3_reset(attempts_stmt);
  sqlite3_clear_bindings(attempts_stmt);
  sqlite3_bind_int64(attempts_stmt, 1, (sqlite3_int64)c->lsn_start);
  sqlite3_bind_int64(attempts_stmt, 2, (sqlite3_int64)c->lsn_end);
  if (sqlite3_step(attempts_stmt) != SQLITE_DONE) return 0;

  sqlite3_reset(attempts_max_stmt);
  sqlite3_clear_bindings(attempts_max_stmt);
  sqlite3_bind_int64(attempts_max_stmt, 1, (sqlite3_int64)c->lsn_start);
  sqlite3_bind_int64(attempts_max_stmt, 2, (sqlite3_int64)c->lsn_end);
  int attempts = 0;
  if (sqlite3_step(attempts_max_stmt) == SQLITE_ROW) {
    attempts = sqlite3_column_int(attempts_max_stmt, 0);
  }
  if (attempts < max_attempts()) return attempts;

  ark_log(db, ARK_LOG_ERROR,
          "chunk lsn %llu..%llu dead-lettered after %d attempts — rows moved "
          "to _dead_backup for operator recovery (arkilian-dlq --replay)",
          (unsigned long long)c->lsn_start, (unsigned long long)c->lsn_end,
          attempts);
  sqlite3_reset(dead_letter_stmt);
  sqlite3_clear_bindings(dead_letter_stmt);
  sqlite3_bind_text(dead_letter_stmt, 1, "max attempts exceeded", -1,
                    SQLITE_STATIC);
  sqlite3_bind_int64(dead_letter_stmt, 2, (sqlite3_int64)c->lsn_start);
  sqlite3_bind_int64(dead_letter_stmt, 3, (sqlite3_int64)c->lsn_end);
  if (sqlite3_step(dead_letter_stmt) != SQLITE_DONE) return 0;
  // INSERT OR IGNORE above absorbed any pre-existing dead copy (the
  // "dead-letter succeeded, delete failed" zombie residue); this DELETE
  // resolves that double state by removing the live copy.
  sqlite3_reset(dlq_delete_stmt);
  sqlite3_clear_bindings(dlq_delete_stmt);
  sqlite3_bind_int64(dlq_delete_stmt, 1, (sqlite3_int64)c->lsn_start);
  sqlite3_bind_int64(dlq_delete_stmt, 2, (sqlite3_int64)c->lsn_end);
  sqlite3_step(dlq_delete_stmt);
  return -1;
}

static int wal_chunk_append(wal_chunk *c, const char *sql, int sql_len,
                             uint64_t outbox_id) {
  if (c->entry_count == 0) {
    c->lsn_start = outbox_id;
    c->opened_at = time(NULL);
  }

  size_t needed = c->len + (size_t)sql_len + 2;
  if (c->cap < needed) {
    c->cap = needed < 65536 ? needed * 2 : needed + 65536;
    char *p = realloc(c->buffer, c->cap);
    if (!p) return -1;
    c->buffer = p;
  }

  // Store plain replayable SQL terminated with ";\n": the flushed chunk
  // object is fed straight into sqlite3_exec() by the hydration engine,
  // so no binary framing may remain in the object body.
  memcpy(c->buffer + c->len, sql, (size_t)sql_len);
  c->buffer[c->len + (size_t)sql_len] = ';';
  c->buffer[c->len + (size_t)sql_len + 1] = '\n';
  c->len += (size_t)sql_len + 2;
  c->lsn_end = outbox_id;
  c->entry_count++;
  c->byte_count += (uint64_t)sql_len;

  return (c->len >= CHUNK_MAX_SIZE_BYTES || c->entry_count >= CHUNK_MAX_ENTRIES) ? 1 : 0;
}

// Flush the accumulated WAL chunk to object storage as a plain replayable
// SQL object, then record it in the manifest registry. This is the ONLY
// realtime shipping path — there is no fallback.
// On success the covered outbox rows are deleted (delete-on-flush-ack):
// until the PUT returns 2xx, every captured row stays in _pending_backup,
// so a crash can never lose an acknowledged-capture write.
static int wal_chunk_flush_to_s3(arkilian *db, wal_chunk *c,
                                 sqlite3_stmt *delete_stmt) {
  if (c->entry_count == 0) return SHIP_OK;
  if (!has_direct_s3(db)) return SHIP_RETRY;
  if (!db->s3_prefix || !db->s3_prefix[0]) return SHIP_RETRY;
  if (strstr(db->s3_prefix, "..") || db->s3_prefix[0] == '/')
    return SHIP_RETRY;

  // 1. Write the chunk body to a unique per-instance staging file
  // (upload_to_s3 streams from disk). The body is plain SQL: one "stmt;\n"
  // per captured row. Fixed names ("%s.chunk") collided across Arkilian
  // instances sharing a backup path — including two instances in the same
  // process — and the digest was computed from the same shared file, so
  // cross-contaminated bytes shipped with a valid self-referential digest.
  char tmp_path[1024];
  FILE *f = arkilian_unique_tmp(db->backup_path, "chunk",
                                tmp_path, sizeof(tmp_path));
  if (!f) return SHIP_RETRY;
  if (fwrite(c->buffer, 1, c->len, f) != c->len) {
    fclose(f); unlink(tmp_path); return SHIP_RETRY;
  }
  fclose(f);

  // 2. SHA-256 for content authentication on restore. A hashing failure
  // must NOT publish a digest-less chunk: hydration hard-refuses chunks
  // without a digest (matching the snapshot path and hydration.h's
  // documented contract), so a digest-less object could never be restored
  // anyway — the flush retries instead of shipping one.
  char sha256_hex[65] = {0};
  if (ark_sha256_hex_file(tmp_path, sha256_hex) != 0) {
    unlink(tmp_path);
    return SHIP_RETRY;
  }

  // 3. Build the S3 key and presign the PUT URL locally (SigV4).
  char s3_key[512];
  snprintf(s3_key, sizeof(s3_key),
           "%s/chunks/lsn_%010llu_%010llu.sql",
           db->s3_prefix,
           (unsigned long long)c->lsn_start,
           (unsigned long long)c->lsn_end);

  char *put_url = s3_presign_put(db, s3_key, 600L);
  if (!put_url) { unlink(tmp_path); return SHIP_RETRY; }

  // 4. Upload.
  int rc = upload_to_s3(db, put_url, tmp_path);
  free(put_url);
  unlink(tmp_path);

  if (rc != SHIP_OK) return rc;

  // 5. Record the chunk in the manifest registry BEFORE the durability ack
  // (the outbox delete). If registry bookkeeping cannot record the shipped
  // object, the covered rows MUST stay queued so a later cycle re-ships
  // them under a fresh key. The previous order (delete, then best-effort
  // append) silently orphaned the object on an append/strdup failure — a
  // comment claimed "retried later" but nothing retried it, leaving rows
  // unreachable by hydration until the next hourly snapshot. SHIP_RETRY
  // feeds the normal failure logic (attempt counters → loud dead-lettering
  // into _dead_backup, recoverable via arkilian-dlq) instead of a silent
  // registry gap. The retried flush re-uploads the same rows to the same
  // deterministic key — idempotent by REPLACE/DELETE replay semantics.
  char *key_copy = strdup(s3_key);
  char *sha_copy = strdup(sha256_hex);
  if (!key_copy || !sha_copy) {
    free(key_copy); free(sha_copy);   // ownership never handed off
    ark_log(db, ARK_LOG_ERROR,
            "chunk lsn %llu..%llu uploaded but registry record allocation "
            "failed — rows stay queued for re-ship",
            (unsigned long long)c->lsn_start, (unsigned long long)c->lsn_end);
    return SHIP_RETRY;
  }
  // On failure manifest_registry_append consumes key_copy/sha_copy itself.
  if (manifest_registry_append(db, key_copy, sha_copy,
                               c->lsn_start, c->lsn_end) != 0) {
    ark_log(db, ARK_LOG_ERROR,
            "chunk lsn %llu..%llu uploaded but registry record failed — "
            "rows stay queued for re-ship",
            (unsigned long long)c->lsn_start, (unsigned long long)c->lsn_end);
    return SHIP_RETRY;
  }
  manifest_registry_maybe_upload(db);
  db->chunks_flushed_total++;

  // 6. Durability ack: the chunk object is durable AND registered — delete
  // the covered outbox rows and advance the flush watermark.
  if (delete_stmt) {
    sqlite3_reset(delete_stmt);
    sqlite3_clear_bindings(delete_stmt);
    sqlite3_bind_int64(delete_stmt, 1, (sqlite3_int64)c->lsn_end);
    if (sqlite3_step(delete_stmt) != SQLITE_DONE) {
      ark_log(db, ARK_LOG_ERROR,
              "outbox delete after chunk flush failed: %s",
              sqlite3_errmsg(db->backup_db));
      // The object IS durable and registered; the stale rows are
      // replay-safe duplicates (REPLACE/DELETE semantics) that the next
      // flush cycle re-deletes.
    }
  }
  db->chunk_flushed_upto = c->lsn_end;
  c->last_s3_flush = time(NULL);
  ARK_STORE(&db->capture_paused, 0);
  return SHIP_OK;
}

// Batch rows are copied off the SELECT into heap memory before any
// network I/O or write, and the SELECT's read transaction is ended
// (reset) before the first DELETE runs. Holding a read snapshot across a
// write on the same connection is a WAL hazard: if another connection
// checkpoints and truncates the WAL in between, the write fails with
// SQLITE_BUSY_SNAPSHOT (extended rc 517) and the busy handler does not
// retry it.
typedef struct {
  sqlite3_int64 id;
  char *payload; // heap copy, valid for the whole pass
  int attempts;
  sqlite3_int64 last_attempt_at; // unix seconds, 0 = never attempted
} outbox_row;


// Reads rows from
// _pending_backup and appends them to the current WAL chunk.
// Rows are deleted from the outbox immediately.
// Returns the number
// of rows drained.
static int drain_chunk(arkilian *db, wal_chunk *c, sqlite3_stmt *select_stmt) {
  if (!db || !db->backup_db || !c) return 0;

  outbox_row rows[BATCH_SIZE];
  int nrows = 0;

  sqlite3_reset(select_stmt);
  sqlite3_clear_bindings(select_stmt);
  // Only rows NOT yet durably shipped AND not already sitting in the
  // chunk buffer: the watermark is the higher of the last flushed id
  // (delete-on-flush-ack) and the buffer's own last appended id.
  uint64_t watermark = db->chunk_flushed_upto;
  if (c->lsn_end > watermark) watermark = c->lsn_end;
  sqlite3_bind_int64(select_stmt, 1, (sqlite3_int64)watermark);
  sqlite3_bind_int(select_stmt, 2, BATCH_SIZE);

  for (;;) {
    int rc = sqlite3_step(select_stmt);
    if (rc == SQLITE_DONE) break;
    if (rc != SQLITE_ROW) break;
    if (nrows >= BATCH_SIZE) break;
    const unsigned char *payload = sqlite3_column_text(select_stmt, 1);
    if (!payload) continue;
    char *copy = strdup((const char *)payload);
    if (!copy) break;
    rows[nrows].id = sqlite3_column_int64(select_stmt, 0);
    rows[nrows].attempts = sqlite3_column_int(select_stmt, 2);
    rows[nrows].last_attempt_at = sqlite3_column_int64(select_stmt, 3);
    rows[nrows].payload = copy;
    nrows++;
  }
  sqlite3_reset(select_stmt);

  int processed = 0;
  for (int i = 0; i < nrows; i++) {
    int full = wal_chunk_append(c, rows[i].payload,
                                 (int)strlen(rows[i].payload),
                                 (uint64_t)rows[i].id);
    if (full < 0) break;  // allocation failure — stop, rows stay in outbox

    // NOTE: the outbox row is NOT deleted here. It is deleted only after
    // the chunk containing it is durably PUT (see wal_chunk_flush_to_s3)
    // — a crash between capture and upload can then never lose the row.

    processed++;

    if (full > 0) break;
  }

  for (int i = 0; i < nrows; i++) free(rows[i].payload);
  return processed;
}

// Prepare the six outbox statements on the backup connection. Returns 1
// when all six prepared, 0 on any failure — finalizing whatever did
// prepare so a retry starts clean. The caller logs and retries with
// backoff: a transient failure here (schema lock, missing outbox table)
// must not silently disable shipping, and a silently-dead flush thread is
// worse than a loudly retrying one.
static int prepare_outbox_statements(sqlite3 *db, sqlite3_stmt **select_stmt,
                                     sqlite3_stmt **delete_stmt,
                                     sqlite3_stmt **attempts_stmt,
                                     sqlite3_stmt **attempts_max_stmt,
                                     sqlite3_stmt **dead_letter_stmt,
                                     sqlite3_stmt **dlq_delete_stmt) {
  *select_stmt = NULL;
  *delete_stmt = NULL;
  *attempts_stmt = NULL;
  *attempts_max_stmt = NULL;
  *dead_letter_stmt = NULL;
  *dlq_delete_stmt = NULL;

  if (sqlite3_prepare_v2(db,
        "SELECT id, payload, attempts, COALESCE(last_attempt_at, 0) FROM _pending_backup "
        "WHERE id > ?1 ORDER BY id LIMIT ?2",
        -1, select_stmt, NULL) != SQLITE_OK) goto fail;
  if (sqlite3_prepare_v2(db,
        "DELETE FROM _pending_backup WHERE id <= ?1",
        -1, delete_stmt, NULL) != SQLITE_OK) goto fail;
  // Failed-flush bookkeeping: bump attempts for the chunk's whole LSN
  // range, read the range's max, dead-letter the range (OR IGNORE absorbs
  // a pre-existing copy — the zombie residue), then drop the live copy.
  if (sqlite3_prepare_v2(db,
        "UPDATE _pending_backup SET attempts = attempts + 1, "
        "last_attempt_at = strftime('%s','now') WHERE id >= ?1 AND id <= ?2",
        -1, attempts_stmt, NULL) != SQLITE_OK) goto fail;
  if (sqlite3_prepare_v2(db,
        "SELECT COALESCE(MAX(attempts), 0) FROM _pending_backup "
        "WHERE id >= ?1 AND id <= ?2",
        -1, attempts_max_stmt, NULL) != SQLITE_OK) goto fail;
  if (sqlite3_prepare_v2(db,
        "INSERT OR IGNORE INTO _dead_backup (id, payload, attempts, failed_reason, created_at) "
        "SELECT id, payload, attempts, ?1, strftime('%s','now') "
        "FROM _pending_backup WHERE id >= ?2 AND id <= ?3",
        -1, dead_letter_stmt, NULL) != SQLITE_OK) goto fail;
  if (sqlite3_prepare_v2(db,
        "DELETE FROM _pending_backup WHERE id >= ?1 AND id <= ?2",
        -1, dlq_delete_stmt, NULL) != SQLITE_OK) goto fail;
  return 1;

fail:
  if (*select_stmt) { sqlite3_finalize(*select_stmt); *select_stmt = NULL; }
  if (*delete_stmt) { sqlite3_finalize(*delete_stmt); *delete_stmt = NULL; }
  if (*attempts_stmt) { sqlite3_finalize(*attempts_stmt); *attempts_stmt = NULL; }
  if (*attempts_max_stmt) { sqlite3_finalize(*attempts_max_stmt); *attempts_max_stmt = NULL; }
  if (*dead_letter_stmt) { sqlite3_finalize(*dead_letter_stmt); *dead_letter_stmt = NULL; }
  if (*dlq_delete_stmt) { sqlite3_finalize(*dlq_delete_stmt); *dlq_delete_stmt = NULL; }
  return 0;
}

// Sleep for `seconds`, interruptible by shutdown (via the shared wake
// condition variable). Returns 1 if shutdown was requested.
static int sleep_interruptible(arkilian *db, int seconds) {
  if (seconds < 1) seconds = 1;
#ifndef _WIN32
  pthread_mutex_lock(&db->wake_mutex);
  struct timespec ts;
  clock_gettime(CLOCK_REALTIME, &ts);
  ts.tv_sec += seconds;
  while (!ARK_LOAD(&db->shutdown_requested)) {
    pthread_cond_timedwait(&db->wake_cond, &db->wake_mutex, &ts);
    time_t now = time(NULL);
    if (now >= ts.tv_sec) break;
  }
  int shutdown = ARK_LOAD(&db->shutdown_requested);
  pthread_mutex_unlock(&db->wake_mutex);
  return shutdown;
#else
  EnterCriticalSection(&db->wake_mutex);
  DWORD remaining_ms = (DWORD)seconds * 1000;
  while (!ARK_LOAD(&db->shutdown_requested) && remaining_ms > 0) {
    DWORD start = GetTickCount();
    SleepConditionVariableCS(&db->wake_cond, &db->wake_mutex, remaining_ms);
    DWORD elapsed = GetTickCount() - start;
    remaining_ms = (elapsed >= remaining_ms) ? 0 : remaining_ms - elapsed;
  }
  int shutdown = ARK_LOAD(&db->shutdown_requested);
  LeaveCriticalSection(&db->wake_mutex);
  return shutdown;
#endif
}

#ifdef _WIN32
DWORD WINAPI run_wal_flush(LPVOID arg) {
#else
void *run_wal_flush(void *arg) {
#endif
  arkilian *db = (arkilian *)arg;
  if (!db || !db->backup_db) {
#ifdef _WIN32
    return 0;
#else
    return NULL;
#endif
  }
  int manifest_seeded = 0;
  time_t next_seed_retry = 0;

  sqlite3_stmt *select_stmt = NULL;
  sqlite3_stmt *delete_stmt = NULL;
  sqlite3_stmt *attempts_stmt = NULL;
  sqlite3_stmt *attempts_max_stmt = NULL;
  sqlite3_stmt *dead_letter_stmt = NULL;
  sqlite3_stmt *dlq_delete_stmt = NULL;

  // Prepare once, reuse via sqlite3_reset — avoids re-parsing SQL every
  // loop. Every prepare below is checked. On failure (e.g. the outbox
  // tables don't exist yet because trigger sync hasn't run, or a schema
  // lock is held), log, back off, and retry instead of exiting — a
  // silently-dead flush thread means writes never leave _pending_backup,
  // discovered only from a growing queue days later.
  int backoff_s = 1;
  while (!ARK_LOAD(&db->shutdown_requested)) {
    if (prepare_outbox_statements(db->backup_db, &select_stmt, &delete_stmt,
                                  &attempts_stmt, &attempts_max_stmt,
                                  &dead_letter_stmt, &dlq_delete_stmt)) {
      break;
    }
    ark_log(db, ARK_LOG_WARN,
            "flush thread: failed to prepare outbox statements: %s "
            "(shipping paused; retrying in %ds)",
            sqlite3_errmsg(db->backup_db), backoff_s);
    if (sleep_interruptible(db, backoff_s)) break;
    if (backoff_s < 60) backoff_s *= 2;
  }


  while (!ARK_LOAD(&db->shutdown_requested) && select_stmt) {
    // Liveness heartbeat (spec §9): the watchdog reads this from another
    // thread; a stale age means the thread died silently.
    long long now_ms = now_ms_mono();
    ARK_STORE(&db->last_heartbeat_sec, (int)(now_ms / 1000));

    // Seed the manifest registry from the last uploaded manifest (restart
    // safety) — async on this thread so db_init never blocks on a slow
    // storage endpoint. Resolves to a terminal state (adopted registry,
    // cold start, or frozen) in one call; only a transient net/mem failure
    // stays unresolved and is retried here on a bounded cadence, so
    // shipping is never blocked awaiting the seed (chunk PUTs proceed).
    if (!manifest_seeded) {
      time_t now = time(NULL);
      if (now >= next_seed_retry) {
        manifest_seeded = manifest_registry_seed(db);
        if (!manifest_seeded) next_seed_retry = now + 5;
      }
    }

    int drained = 0;
    if (ARK_LOAD(&db->backup_enabled) && ARK_LOAD(&db->s3_creds_loaded)) {
      drained = drain_chunk(db, &db->chunk, select_stmt);
    }

    // Both the drain and the flush are gated on the kill-switch: a
    // disabled backup must never ship rows already sitting in the buffer.
    if (ARK_LOAD(&db->backup_enabled) && ARK_LOAD(&db->s3_creds_loaded) &&
        db->chunk.entry_count > 0) {
      time_t age = time(NULL) - db->chunk.opened_at;
      if (age >= db->chunk_interval || db->chunk.len >= CHUNK_MAX_SIZE_BYTES) {
        int flush_rc = wal_chunk_flush_to_s3(db, &db->chunk, delete_stmt);
        if (flush_rc == SHIP_OK) {
          wal_chunk_reset(&db->chunk);
          drained = 1;  // indicate work was done (prevents unnecessary sleep)
        } else {
          // The chunk failed as a unit (any non-2xx, or a local failure).
          // Record the attempt for every row it covers; once the attempt
          // budget is exhausted, dead-letter the whole chunk so a
          // permanently-rejected chunk (revoked credentials, deleted
          // bucket, malformed key) never pins the queue head behind it.
          int chunk_attempts = chunk_failure_logic(
              db, &db->chunk, attempts_stmt, attempts_max_stmt,
              dead_letter_stmt, dlq_delete_stmt);
          if (chunk_attempts < 0) {
            // Dead-lettered: the queue head advances past the poison rows.
            wal_chunk_reset(&db->chunk);
            drained = 1;
          } else {
            // Retry with interruptible exponential backoff (db_close /
            // db_wal_flush wake it early on shutdown).
            long b = backoff_seconds(chunk_attempts);
            if (sleep_interruptible(db, (int)b)) break;
          }
        }
      }
    }

    // (Risk #1) Sticky capture-paused signal: when the outbox is at cap,
    // the capture trigger's `WHERE count < cap` gate stops inserting —
    // CDC rows are being dropped and only the hourly snapshot will recover
    // them. Set the sticky flag so monitoring surfaces the gap even after
    // the queue drains (the snapshot thread clears it on successful upload).
    // One COUNT(*) per poll cycle (~2s) is negligible vs the drain workload.
    if (db_backup_queue_depth(db) >= outbox_cap()) {
      ARK_STORE(&db->capture_paused, 1);
    }

    if (!drained) {
#ifndef _WIN32
      pthread_mutex_lock(&db->wake_mutex);
      if (!db->wake_flag && !ARK_LOAD(&db->shutdown_requested)) {
        struct timespec ts;
        clock_gettime(CLOCK_REALTIME, &ts);
        ts.tv_sec += POLL_INTERVAL_MS / 1000;
        pthread_cond_timedwait(&db->wake_cond, &db->wake_mutex, &ts);
      }
      db->wake_flag = 0;
      pthread_mutex_unlock(&db->wake_mutex);
#else
      EnterCriticalSection(&db->wake_mutex);
      if (!db->wake_flag && !ARK_LOAD(&db->shutdown_requested)) {
        SleepConditionVariableCS(&db->wake_cond, &db->wake_mutex, POLL_INTERVAL_MS);
      }
      db->wake_flag = 0;
      LeaveCriticalSection(&db->wake_mutex);
#endif
    }
  }

  if (db->chunk.entry_count > 0) {
    wal_chunk_flush_to_s3(db, &db->chunk, delete_stmt);
    wal_chunk_reset(&db->chunk);
  }

  if (select_stmt) sqlite3_finalize(select_stmt);
  if (delete_stmt) sqlite3_finalize(delete_stmt);
  if (attempts_stmt) sqlite3_finalize(attempts_stmt);
  if (attempts_max_stmt) sqlite3_finalize(attempts_max_stmt);
  if (dead_letter_stmt) sqlite3_finalize(dead_letter_stmt);
  if (dlq_delete_stmt) sqlite3_finalize(dlq_delete_stmt);

#ifdef _WIN32
  return 0;
#else
  return NULL;
#endif
}

// ── Database Lifecycle: db_init / db_close ──────────────────────────

int db_init(arkilian **db_ptr, const char *filename) {
  if (!db_ptr) return 1;

  arkilian *db = malloc(sizeof(arkilian));
  if (!db) return 1;
  memset(db, 0, sizeof(arkilian));

  // Initialize all sync primitives before any logging or configuration
  // step that could call ark_log() or touch shared state.
#ifndef _WIN32
  int init_ok = (pthread_mutex_init(&db->wake_mutex, NULL) == 0) &&
                (pthread_cond_init(&db->wake_cond, NULL) == 0) &&
                (pthread_mutex_init(&db->payload_mutex, NULL) == 0) &&
                (pthread_mutex_init(&db->manifest_mutex, NULL) == 0) &&
                (pthread_mutex_init(&db->log_mutex, NULL) == 0);
#else
  InitializeCriticalSection(&db->wake_mutex);
  InitializeConditionVariable(&db->wake_cond);
  InitializeCriticalSection(&db->payload_mutex);
  InitializeCriticalSection(&db->manifest_mutex);
  InitializeCriticalSection(&db->log_mutex);
  int init_ok = 1;
#endif
  if (!init_ok) {
    *db_ptr = NULL;
    free(db);
    return 1;
  }
  db->sync_initialized = 1;

  load_env();

  const char *path = (filename != NULL) ? filename :
    get_env_default("ARKILIAN_DB_PATH", DEFAULT_DB_PATH);

  db->db_path = malloc(strlen(path) + 1);
  if (db->db_path) strcpy(db->db_path, path);

  const char *backup_path_tmp = get_env_default("ARKILIAN_BACKUP_PATH", DEFAULT_BACKUP_PATH);
  db->backup_path = malloc(strlen(backup_path_tmp) + 1);
  if (db->backup_path) strcpy(db->backup_path, backup_path_tmp);

  // S3 destination — the ONLY backup target. SigV4 requests are signed
  // locally with the per-database access/secret keys; no other credential
  // exists in the client.
  {
    const char *ep = get_env_default("ARKILIAN_S3_ENDPOINT", "");
    const char *bk = get_env_default("ARKILIAN_S3_BUCKET", "");
    const char *rg = get_env_default("ARKILIAN_S3_REGION", "us-east-1");
    const char *ak = get_env_default("ARKILIAN_S3_ACCESS_KEY", "");
    const char *sk = get_env_default("ARKILIAN_S3_SECRET_KEY", "");
    const char *pf = get_env_default("ARKILIAN_S3_PREFIX", "db_default");
    db->s3_endpoint   = malloc(strlen(ep) + 1);
    db->s3_bucket     = malloc(strlen(bk) + 1);
    db->s3_region     = malloc(strlen(rg) + 1);
    db->s3_access_key = malloc(strlen(ak) + 1);
    db->s3_secret_key = malloc(strlen(sk) + 1);
    db->s3_prefix     = malloc(strlen(pf) + 1);
    if (db->s3_endpoint)   strcpy(db->s3_endpoint, ep);
    if (db->s3_bucket)     strcpy(db->s3_bucket, bk);
    if (db->s3_region)     strcpy(db->s3_region, rg);
    if (db->s3_access_key) strcpy(db->s3_access_key, ak);
    if (db->s3_secret_key) strcpy(db->s3_secret_key, sk);
    if (db->s3_prefix)     strcpy(db->s3_prefix, pf);
  }

  // Optional manifest authenticity key. NOT part of has_direct_s3(): a
  // destination without a signing key still ships (legacy buckets keep
  // working); with a key, publishes carry manifest.sig and hydrators with
  // the same key refuse unverified manifests.
  {
    const char *hk = get_env_default("ARKILIAN_MANIFEST_HMAC_KEY", "");
    db->manifest_hmac_key = malloc(strlen(hk) + 1);
    if (db->manifest_hmac_key) strcpy(db->manifest_hmac_key, hk);
  }
  db->chunks_flushed_total = 0;

  db->backup_interval = get_env_int_default("ARKILIAN_BACKUP_INTERVAL", DEFAULT_BACKUP_INTERVAL);
  // A 0 or negative interval would make the hourly thread hot-loop
  // (backup + signed-URL request with no sleep in between). Clamp it.
  if (db->backup_interval < 1) db->backup_interval = 1;
  ARK_STORE(&db->backup_enabled, get_env_bool_default("ARKILIAN_ENABLE_BACKUP", 1));
  db->chunk_interval = get_env_int_default("ARKILIAN_CHUNK_INTERVAL_SEC",
                                           CHUNK_FLUSH_INTERVAL_SEC);
  if (db->chunk_interval < 1) db->chunk_interval = 1;
  // S3 readiness: shipping and snapshots require a fully configured
  // destination (endpoint + bucket + both keys). Partial configuration is
  // surfaced loudly — never a hard failure (spec §0), but shipping only
  // runs against a complete destination.
  ARK_STORE(&db->s3_creds_loaded, has_direct_s3(db) ? 1 : 0);

  // (Hardening) Cleartext S3 endpoint guard: a presigned SigV4 request
  // carries the request signature in the query string, so shipping over
  // http:// to a NON-LOCAL host would expose replayable signatures on the
  // wire. Refuse loudly at init
  // (backup disabled; the game is unaffected, spec §0) unless the operator
  // opts in with ARKILIAN_ALLOW_INSECURE=1 for local development only.
  // Loopback / RFC1918 http:// endpoints (local MinIO-style test servers)
  // remain permitted.
  if (db->s3_endpoint && db->s3_endpoint[0] &&
      strncmp(db->s3_endpoint, "http://", 7) == 0) {
    char ep_host[256];
    if (url_host(db->s3_endpoint, ep_host, sizeof(ep_host)) == 0 ||
        !host_is_storage_safe(ep_host)) {
      const char *allow = getenv("ARKILIAN_ALLOW_INSECURE");
      if (!(allow && strcmp(allow, "1") == 0)) {
        ark_log(db, ARK_LOG_ERROR,
                "S3 endpoint '%s' is cleartext http:// to a non-local host "
                "— presigned request signatures would travel unencrypted "
                "and are replayable. Backup DISABLED; use https://, or set "
                "ARKILIAN_ALLOW_INSECURE=1 for local development only",
                db->s3_endpoint);
        ARK_STORE(&db->backup_enabled, 0);
      }
    }
  }

  // Config validation (spec §9's "fail loudly, never silently"): a
  // kill-switch-ON install with no destination will capture rows forever
  // without shipping them. Loud warning at startup — never a hard
  // failure, per the §0 rule that the backup subsystem must not break
  // the application.
  if (db->backup_enabled && !ARK_LOAD(&db->s3_creds_loaded)) {
    ark_log(db, ARK_LOG_WARN,
            "backup is enabled (ARKILIAN_ENABLE_BACKUP) but the S3 destination "
            "is incomplete — set ARKILIAN_S3_ENDPOINT, ARKILIAN_S3_BUCKET, "
            "ARKILIAN_S3_ACCESS_KEY and ARKILIAN_S3_SECRET_KEY; rows will "
            "accumulate in _pending_backup and never ship until then");
  }

  // libcurl global init must happen before ANY thread calls
  // curl_easy_init — concurrent first use is not thread-safe. The
  // once-guard makes repeated db_init/db_close cycles safe; cleanup is
  // intentionally never called (curl_global_cleanup while any other
  // instance still uses libcurl is a use-after-free; a one-time leak at
  // process exit is the accepted tradeoff).
  ensure_curl_global_init();

  // Open primary connection
  int rc = sqlite3_open_v2(
      path, &db->handle,
      SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_FULLMUTEX, NULL);

  if (rc != SQLITE_OK) {
    const char *err = sqlite3_errstr(rc);
    strncpy(db->last_error_msg, err, sizeof(db->last_error_msg) - 1);
    db->last_error_msg[sizeof(db->last_error_msg) - 1] = '\0';
    if (db->handle) sqlite3_close(db->handle);
    db->handle = NULL;
    *db_ptr = db;
    return 1;
  }

  // Open secondary backup connection
  rc = sqlite3_open_v2(
      path, &db->backup_db,
      SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_FULLMUTEX, NULL);
  if (rc != SQLITE_OK) {
    snprintf(db->last_error_msg, sizeof(db->last_error_msg),
             "backup connection: %s", sqlite3_errstr(rc));
    if (db->backup_db) sqlite3_close(db->backup_db);
    sqlite3_close(db->handle);
    db->handle = NULL;
    db->backup_db = NULL;
    *db_ptr = db;
    return 1;
  }

  // Third connection: owned exclusively by the hourly snapshot thread.
  // Spec §3.1 is "one connection per thread" — sharing backup_db between
  // the flush and snapshot threads would make shipping contend with the
  // file copy and stall the realtime path during large snapshots.
  rc = sqlite3_open_v2(
      path, &db->snapshot_db,
      SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_FULLMUTEX, NULL);
  if (rc != SQLITE_OK) {
    snprintf(db->last_error_msg, sizeof(db->last_error_msg),
             "snapshot connection: %s", sqlite3_errstr(rc));
    if (db->snapshot_db) sqlite3_close(db->snapshot_db);
    if (db->backup_db) sqlite3_close(db->backup_db);
    sqlite3_close(db->handle);
    db->handle = NULL;
    db->backup_db = NULL;
    db->snapshot_db = NULL;
    *db_ptr = db;
    return 1;
  }

  sqlite3_busy_timeout(db->handle, 5000);
  sqlite3_busy_timeout(db->backup_db, 5000);
  sqlite3_busy_timeout(db->snapshot_db, 5000);

  // ── Checked PRAGMA application (spec §0: no unchecked SQLite calls) ──
  // WAL is load-bearing for the entire design: if it silently failed
  // (read-only FS, wrong lock holder) the subsystem would run with the
  // wrong durability/contention profile and nothing would notice. If
  // WAL is not active, shipping is disabled outright. Other PRAGMA
  // failures are logged, not fatal.
  int capture_ok = 1;
  char *perr = NULL;

  // (Risk #3) Network-filesystem guard. SQLite WAL mode uses a mmap'd
  // <db>-shm file that cannot be shared across network mounts (NFS/EFS/
  // AFP/SMB): locks break with SQLITE_IOERR_LOCK or, worse, silently
  // corrupt the database. On a network FS, disable capture (backup
  // visibly off via db_backup_is_healthy) and run in rollback-journal
  // mode — the application keeps running (spec §0). Local filesystems
  // always proceed normally (no false positives on dev/CI platforms).
  int fs_network = fs_is_network(path);
  if (fs_network) {
    ark_log(db, ARK_LOG_ERROR,
            "database path '%s' is on a network filesystem "
            "(NFS/SMB/AFP/CIFS). SQLite WAL mode uses a mmap'd -shm file "
            "that does not work across network mounts "
            "(SQLITE_IOERR_LOCK / silent corruption). Backup capture is "
            "DISABLED and the database will run in rollback-journal mode; "
            "the application keeps running. Move the database to a local "
            "filesystem to enable realtime backup", path);
    capture_ok = 0;
  }

  if (!fs_network) {
    if (sqlite3_exec(db->handle, "PRAGMA journal_mode=WAL;", NULL, NULL, &perr) != SQLITE_OK) {
      ark_log(db, ARK_LOG_ERROR, "PRAGMA journal_mode=WAL failed: %s",
              perr ? perr : "unknown error");
      sqlite3_free(perr);
      perr = NULL;
      capture_ok = 0;
    }
    if (capture_ok) {
      // Verify the mode actually took — exec can report OK while the
      // journal stays rollback-mode on some paths.
      sqlite3_stmt *jm = NULL;
      if (sqlite3_prepare_v2(db->handle, "PRAGMA journal_mode", -1, &jm, NULL) == SQLITE_OK &&
          sqlite3_step(jm) == SQLITE_ROW) {
        const char *mode = (const char *)sqlite3_column_text(jm, 0);
        capture_ok = mode && strcmp(mode, "wal") == 0;
      }
      sqlite3_finalize(jm);
      if (!capture_ok) {
        ark_log(db, ARK_LOG_ERROR,
                "journal mode is not WAL — backup capture disabled");
      }
    }
  }
  {
    // The game connection hosts the _pending_backup outbox (capture
    // triggers write to it on every row change). synchronous=NORMAL
    // (the SQLite default for WAL) fsyncs WAL pages at checkpoint, not
    // at commit — committed outbox rows can be lost on power loss
    // before the next checkpoint. For a backup product, losing the
    // very rows the subsystem exists to ship is unacceptable.
    // ARKILIAN_OUTBOX_DURABLE=1 (default) sets synchronous=FULL on
    // the game connection so the outbox is durable at commit. The
    // ~15% write-latency cost is the price of not silently losing
    // captured data on power loss. Operators who prioritize throughput
    // over power-loss durability can set ARKILIAN_OUTBOX_DURABLE=0.
    // These PRAGMAs apply in rollback-journal mode too and are run
    // unconditionally so the application's durability profile is right
    // even when capture was disabled by the network-FS guard.
    int outbox_durable = get_env_bool_default("ARKILIAN_OUTBOX_DURABLE", 1);
    db->outbox_durable = outbox_durable ? 1 : 0;
    const char *sync_pragma = outbox_durable ? "PRAGMA synchronous=FULL;"
                                              : "PRAGMA synchronous=NORMAL;";
    const struct { const char *sql; const char *what; } pragmas[] = {
      { sync_pragma,                  "synchronous" },
      { "PRAGMA foreign_keys=ON;",    "foreign_keys=ON" },
      { "PRAGMA cache_size=-64000;",  "cache_size" },
      { NULL, NULL }
    };
    for (int i = 0; pragmas[i].sql; i++) {
      if (sqlite3_exec(db->handle, pragmas[i].sql, NULL, NULL, &perr) != SQLITE_OK) {
        ark_log(db, ARK_LOG_WARN, "PRAGMA %s failed: %s",
                pragmas[i].what, perr ? perr : "unknown error");
        sqlite3_free(perr);
        perr = NULL;
      }
    }
    // WAL on the backup/snapshot connections has the same network-mount
    // hazard as on the primary; skip it (and let them inherit rollback
    // journal) when a network filesystem was detected.
    if (!fs_network) {
      if (sqlite3_exec(db->backup_db, "PRAGMA journal_mode=WAL;", NULL, NULL, &perr) != SQLITE_OK ||
          sqlite3_exec(db->backup_db, "PRAGMA synchronous=NORMAL;", NULL, NULL, &perr) != SQLITE_OK) {
        ark_log(db, ARK_LOG_WARN, "backup connection PRAGMA failed: %s",
                perr ? perr : "unknown error");
        sqlite3_free(perr);
        perr = NULL;
      }
      // Snapshot connection: WAL mode needed for concurrent readers; the
      // synchronous setting matters little (read-only workload).
      if (sqlite3_exec(db->snapshot_db, "PRAGMA journal_mode=WAL;", NULL, NULL, &perr) != SQLITE_OK) {
        ark_log(db, ARK_LOG_WARN, "snapshot connection PRAGMA failed: %s",
                perr ? perr : "unknown error");
        sqlite3_free(perr);
        perr = NULL;
      }
    }
  }

  // Register non-blocking update hook
  sqlite3_update_hook(db->handle, on_db_update, db);
  // Schema-change observer: flag raw-handle DDL so capture desync is
  // visible (Risk #1 / spec §1). Never blocks, performs no I/O.
  sqlite3_set_authorizer(db->handle, on_schema_authorizer, db);

  // Sync backup triggers. Per spec §0/§1 a capture failure must NEVER
  // prevent the game from starting: log loudly and fall back to the
  // kill-switch's disabled state — the game runs normally, nothing
  // ships, and db_backup_is_enabled() + monitoring make the outage
  // visible instead of silent.
  if (capture_ok) {
    char *trigger_err = NULL;
    ARK_STORE(&db->trigger_sync_in_progress, 1);
    int sync_rc = sync_backup_triggers(db->handle, &trigger_err);
    ARK_STORE(&db->trigger_sync_in_progress, 0);
    if (sync_rc != SQLITE_OK) {
      ark_log(db, ARK_LOG_ERROR,
              "backup trigger sync FAILED — capture disabled: %s",
              trigger_err ? trigger_err : "unknown error");
      capture_ok = 0;
    }
    if (trigger_err) sqlite3_free(trigger_err);
    // Init sync establishes full coverage: clear any transient dirty flag
    // the authorizer may have raised before the guard took effect.
    ARK_STORE(&db->triggers_dirty, 0);
  }

  // Internal-schema version check: an outbox written by a NEWER release
  // must never be touched by this (older) binary — refuse capture rather
  // than corrupt a format we don't understand. Never fatal to the game.
  if (capture_ok) {
    sqlite3_stmt *sv = NULL;
    int db_version = 0;
    if (sqlite3_prepare_v2(db->handle,
          "SELECT v FROM _arkilian_meta WHERE k = 'schema_version'",
          -1, &sv, NULL) == SQLITE_OK) {
      if (sqlite3_step(sv) == SQLITE_ROW) db_version = sqlite3_column_int(sv, 0);
      sqlite3_finalize(sv);
    }
    if (db_version > ARKILIAN_SCHEMA_VERSION) {
      ark_log(db, ARK_LOG_ERROR,
              "outbox schema version %d is NEWER than this binary supports (%d) — "
              "capture disabled; upgrade the client",
              db_version, ARKILIAN_SCHEMA_VERSION);
      capture_ok = 0;
    }
  }

  if (!capture_ok) {
    ARK_STORE(&db->backup_enabled, 0); // kill-switch state: game runs, nothing ships
    snprintf(db->last_error_msg, sizeof(db->last_error_msg),
             "backup disabled: WAL or trigger setup failed");
  }

  // Transaction statements
  sqlite3_prepare_v2(db->handle, "BEGIN;", -1, &db->begin_stmt, NULL);
  sqlite3_prepare_v2(db->handle, "COMMIT;", -1, &db->commit_stmt, NULL);
  sqlite3_prepare_v2(db->handle, "ROLLBACK;", -1, &db->rollback_stmt, NULL);
  if (!db->begin_stmt || !db->commit_stmt || !db->rollback_stmt) {
    snprintf(db->last_error_msg, sizeof(db->last_error_msg),
             "failed to prepare transaction statements: %s", sqlite3_errmsg(db->handle));
    *db_ptr = db;
    return 1;
  }

  db->is_open = 1;
  ARK_STORE(&db->shutdown_requested, 0);
  *db_ptr = db;

  // Start WAL flusher thread. A creation failure must not take the game
  // down (spec §0/§1): log loudly and drop into the kill-switch's
  // disabled state.
#ifndef _WIN32
  db->flush_thread_running = 0;
  if (pthread_create(&db->flush_thread_id, NULL, run_wal_flush, db) != 0) {
    ark_log(db, ARK_LOG_ERROR,
            "failed to start WAL flush thread — backup disabled");
    ARK_STORE(&db->backup_enabled, 0);
  } else {
    db->flush_thread_running = 1;
  }
#else
  db->flush_thread_handle = CreateThread(NULL, 0, run_wal_flush, db, 0, NULL);
  if (!db->flush_thread_handle) {
    ark_log(db, ARK_LOG_ERROR,
            "failed to start WAL flush thread — backup disabled");
    ARK_STORE(&db->backup_enabled, 0);
  }
#endif

  // Start backup thread. Failure here only loses the hourly snapshots —
  // realtime shipping keeps running; the failure is logged loudly.
  if (ARK_LOAD(&db->backup_enabled)) {
#ifdef _WIN32
    db->backup_thread_handle = CreateThread(NULL, 0, run_hourly_backup, db, 0, NULL);
    if (!db->backup_thread_handle) {
      ark_log(db, ARK_LOG_ERROR, "failed to start hourly backup thread");
    }
#else
    db->backup_thread_running = 0;
    if (pthread_create(&db->backup_thread_id, NULL, run_hourly_backup, db) != 0) {
      ark_log(db, ARK_LOG_ERROR, "failed to start hourly backup thread");
    } else {
      db->backup_thread_running = 1;
    }
#endif
  }

  return 0;
}

void db_close(arkilian *db) {
  if (!db) return;

  // Wake BOTH sleeper threads (flush + hourly backup) under the mutex
  // so neither can miss the shutdown signal.
#ifndef _WIN32
  pthread_mutex_lock(&db->wake_mutex);
  ARK_STORE(&db->shutdown_requested, 1);
  db->wake_flag = 1;
  pthread_cond_broadcast(&db->wake_cond);
  pthread_mutex_unlock(&db->wake_mutex);

  if (db->flush_thread_running) {
    pthread_join(db->flush_thread_id, NULL);
    db->flush_thread_running = 0;
  }
  if (db->backup_thread_running) {
    pthread_join(db->backup_thread_id, NULL);
    db->backup_thread_running = 0;
  }
#else
  EnterCriticalSection(&db->wake_mutex);
  ARK_STORE(&db->shutdown_requested, 1);
  db->wake_flag = 1;
  WakeAllConditionVariable(&db->wake_cond);
  LeaveCriticalSection(&db->wake_mutex);

  if (db->flush_thread_handle) {
    WaitForSingleObject(db->flush_thread_handle, INFINITE);
    CloseHandle(db->flush_thread_handle);
    db->flush_thread_handle = NULL;
  }
  if (db->backup_thread_handle) {
    WaitForSingleObject(db->backup_thread_handle, INFINITE);
    CloseHandle(db->backup_thread_handle);
    db->backup_thread_handle = NULL;
  }
#endif

  for (int i = 0; i < db->stmt_count; i++) {
    if (db->stmts && db->stmts[i]) sqlite3_finalize(db->stmts[i]);
  }
  if (db->stmts) free(db->stmts);
  if (db->stmt_is_ddl) free(db->stmt_is_ddl);

  if (db->begin_stmt) sqlite3_finalize(db->begin_stmt);
  if (db->commit_stmt) sqlite3_finalize(db->commit_stmt);
  if (db->rollback_stmt) sqlite3_finalize(db->rollback_stmt);

  if (db->backup_db) {
    sqlite3_close(db->backup_db);
    db->backup_db = NULL;
  }

  if (db->snapshot_db) {
    sqlite3_close(db->snapshot_db);
    db->snapshot_db = NULL;
  }

  if (db->handle) {
    sqlite3_close(db->handle); // deregisters the update hook
    db->handle = NULL;
  }

  // Destroy the sync primitives only now: threads are joined and the
  // update hook is deregistered, so nothing can acquire them again. The
  // caller must not race db_close with in-flight DB statements on the
  // game thread — that is UB by contract (see class.h).
#ifndef _WIN32
  if (db->sync_initialized) {
    pthread_mutex_destroy(&db->wake_mutex);
    pthread_cond_destroy(&db->wake_cond);
    pthread_mutex_destroy(&db->payload_mutex);
    pthread_mutex_destroy(&db->manifest_mutex);
    pthread_mutex_destroy(&db->log_mutex);
  }
#else
  if (db->sync_initialized) {
    DeleteCriticalSection(&db->wake_mutex);
    DeleteCriticalSection(&db->payload_mutex);
    DeleteCriticalSection(&db->manifest_mutex);
    DeleteCriticalSection(&db->log_mutex);
  }
#endif

  if (db->db_path) free(db->db_path);
  if (db->backup_path) free(db->backup_path);
  if (db->manifest_hmac_key) free(db->manifest_hmac_key);
  if (db->s3_endpoint) free(db->s3_endpoint);
  if (db->s3_bucket) free(db->s3_bucket);
  if (db->s3_region) free(db->s3_region);
  if (db->s3_access_key) free(db->s3_access_key);
  if (db->s3_secret_key) free(db->s3_secret_key);
  if (db->s3_prefix) free(db->s3_prefix);
  for (int i = 0; i < db->manifest_chunk_count; i++) {
    free(db->manifest_chunks[i].s3_key);
    free(db->manifest_chunks[i].sha256);
  }
  free(db->manifest_chunks);
  free(db->manifest_snapshot_key);
  free(db->manifest_snapshot_sha);
  if (db->chunk.buffer) free(db->chunk.buffer);

  free(db);
}

// ── Query Execution ─────────────────────────────────────────────────

const char *db_errmsg(arkilian *db) {
  if (!db) return "Invalid database handle";
  if (db->last_error_msg[0] != '\0') return db->last_error_msg;
  if (db->handle) return sqlite3_errmsg(db->handle);
  return "Unknown error";
}

sqlite3 *db_get_handle(arkilian *db) { return db ? db->handle : NULL; }

// Shared post-DDL path for every execution route (db_exec AND DDL run
// through db_prepare/db_step): re-sync the capture triggers for the
// (possibly new) schema, then record the DDL itself in the outbox so
// the destination mirror applies it before the rows it creates. Without
// this, a table created outside db_exec is never captured (spec §1:
// no write path may silently bypass capture). Never fatal — failures
// are logged loudly and capture of other tables keeps working.
static void apply_ddl_capture(arkilian *db, const char *sql) {
  char *terr = NULL;
  ARK_STORE(&db->trigger_sync_in_progress, 1);
  int sync_rc = sync_backup_triggers(db->handle, &terr);
  ARK_STORE(&db->trigger_sync_in_progress, 0);
  if (sync_rc != SQLITE_OK) {
    snprintf(db->last_error_msg, sizeof(db->last_error_msg),
             "backup trigger sync failed after DDL: %s", terr ? terr : "unknown error");
    ark_log(db, ARK_LOG_ERROR, "%s", db->last_error_msg);
  } else {
    // Wrapped DDL re-established full trigger coverage: a previously-
    // flagged raw-handle desync is now repaired too.
    ARK_STORE(&db->triggers_dirty, 0);
  }
  if (terr) sqlite3_free(terr);

  sqlite3_stmt *ddl_stmt = NULL;
  char ddl_insert_sql[160];
  snprintf(ddl_insert_sql, sizeof(ddl_insert_sql),
    "INSERT INTO _pending_backup (payload) SELECT ? "
    "WHERE (SELECT COUNT(*) FROM _pending_backup) < %ld",
    outbox_cap());
  if (sqlite3_prepare_v2(db->handle, ddl_insert_sql, -1, &ddl_stmt, NULL) == SQLITE_OK) {
    int bind_rc = sqlite3_bind_text(ddl_stmt, 1, sql, -1, SQLITE_TRANSIENT);
    int step_rc = (bind_rc == SQLITE_OK) ? sqlite3_step(ddl_stmt) : bind_rc;
    if (step_rc != SQLITE_DONE) {
      ark_log(db, ARK_LOG_ERROR,
              "DDL capture to _pending_backup failed (rc=%d): %s",
              step_rc, sqlite3_errmsg(db->handle));
    }
    sqlite3_finalize(ddl_stmt);
  }
}

int db_exec(arkilian *db, const char *sql) {
  if (!db || !db->handle || !sql) return SQLITE_ERROR;

  // (Risk #1) Opt-in auto-resync: if raw-handle DDL set triggers_dirty
  // and the operator enabled this, repair NOW — before the user's SQL
  // runs — so the statement is captured. Post-commit (this runs after
  // the raw DDL's transaction committed), on the game thread, never
  // inside a SQLite hook. A resync failure logs + leaves dirty set;
  // it never rolls back app work (spec §0).
  if (ARK_LOAD(&db->auto_resync_triggers) && ARK_LOAD(&db->triggers_dirty)) {
    db_resync_triggers(db);
  }

  char *errmsg = NULL;
  // Mark this as a wrapped dispatch so the schema authorizer knows the
  // DDL (if any) will be auto-re-synced by apply_ddl_capture below — it
  // stays silent and lets the wrapper handle it, instead of warning about
  // a bypass that did not happen.
  ARK_STORE(&db->in_wrapped_dispatch, 1);
  int rc = sqlite3_exec(db->handle, sql, NULL, NULL, &errmsg);
  ARK_STORE(&db->in_wrapped_dispatch, 0);
  if (rc != SQLITE_OK) {
    if (errmsg) {
      strncpy(db->last_error_msg, errmsg, sizeof(db->last_error_msg) - 1);
      db->last_error_msg[sizeof(db->last_error_msg) - 1] = '\0';
      sqlite3_free(errmsg);
    } else {
      snprintf(db->last_error_msg, sizeof(db->last_error_msg), "%s", sqlite3_errmsg(db->handle));
    }
    return rc;
  }

  const char *sql_verb = skip_sql_prefix(sql);
  if (strncasecmp(sql_verb, "CREATE", 6) == 0 ||
      strncasecmp(sql_verb, "ALTER", 5) == 0 ||
      strncasecmp(sql_verb, "DROP", 4) == 0) {
    apply_ddl_capture(db, sql);
  }

  // Public contract: SQLITE_OK (0) on success. sqlite3_exec returns
  // SQLITE_OK; surfacing SQLITE_DONE here would break every C caller
  // that compares against the conventional success code.
  return SQLITE_OK;
}

int db_prepare(arkilian *db, const char *sql) {
  if (!db || !db->handle || !sql) return SQLITE_ERROR;

  if (db->stmt_count >= db->stmt_capacity) {
    int new_cap = (db->stmt_capacity == 0) ? 8 : db->stmt_capacity * 2;
    // Grow the DDL-flag array first; if the statement array then fails
    // to grow, the (larger) flag block is harmless — it is only ever
    // indexed below stmt_capacity.
    unsigned char *new_flags = realloc(db->stmt_is_ddl, (size_t)new_cap);
    if (!new_flags) return SQLITE_NOMEM;
    sqlite3_stmt **new_arr = realloc(db->stmts, (size_t)new_cap * sizeof(sqlite3_stmt *));
    if (!new_arr) {
      db->stmt_is_ddl = new_flags;
      return SQLITE_NOMEM;
    }
    db->stmts = new_arr;
    db->stmt_is_ddl = new_flags;
    db->stmt_capacity = new_cap;
  }

  sqlite3_stmt *stmt = NULL;
  int rc = sqlite3_prepare_v2(db->handle, sql, -1, &stmt, NULL);
  if (rc != SQLITE_OK) {
    snprintf(db->last_error_msg, sizeof(db->last_error_msg), "%s", sqlite3_errmsg(db->handle));
    return rc;
  }
  if (!stmt) {
    // Empty/whitespace-only SQL prepares OK but yields a NULL statement —
    // storing it would create a ghost slot in the pool.
    snprintf(db->last_error_msg, sizeof(db->last_error_msg), "empty SQL statement");
    return SQLITE_ERROR;
  }

  db->stmts[db->stmt_count] = stmt;
  // Flag DDL statements at prepare time so db_step can resync capture
  // triggers after they execute — DDL through this API must be as
  // invisible-proof as db_exec (spec §1).
  {
    const char *raw = sqlite3_sql(stmt);
    const char *verb = skip_sql_prefix(raw ? raw : "");
    db->stmt_is_ddl[db->stmt_count] =
        (strncasecmp(verb, "CREATE", 6) == 0 ||
         strncasecmp(verb, "ALTER", 5) == 0 ||
         strncasecmp(verb, "DROP", 4) == 0) ? 1 : 0;
  }
  db->stmt_current = db->stmt_count;
  db->stmt_count++;
  return rc;
}

int db_use_stmt(arkilian *db, int index) {
  if (!db || index < 0 || index >= db->stmt_count) return SQLITE_ERROR;
  if (!db->stmts[index]) return SQLITE_ERROR;
  db->stmt_current = index;
  return SQLITE_OK;
}

int db_stmt_count(arkilian *db) {
  return db ? db->stmt_count : 0;
}

static sqlite3_stmt *get_current_stmt(arkilian *db) {
  if (!db || db->stmt_current < 0 || db->stmt_current >= db->stmt_count) return NULL;
  return db->stmts[db->stmt_current];
}

int db_step(arkilian *db) {
  sqlite3_stmt *stmt = get_current_stmt(db);
  if (!stmt) return SQLITE_ERROR;

  // (Risk #1) Opt-in auto-resync: same as db_exec — if raw-handle DDL
  // set triggers_dirty, repair before the step runs. Post-commit, game
  // thread, never inside a hook.
  if (ARK_LOAD(&db->auto_resync_triggers) && ARK_LOAD(&db->triggers_dirty)) {
    db_resync_triggers(db);
  }

  // Mask DDL-through-step as wrapped so the authorizer suppresses its
  // bypass warning (apply_ddl_capture below re-syncs it). Non-DDL steps
  // never raise a DDL action, so the flag is harmless for them.
  int is_ddl = (db->stmt_is_ddl &&
                db->stmt_current >= 0 && db->stmt_current < db->stmt_count &&
                db->stmt_is_ddl[db->stmt_current]);
  if (is_ddl) ARK_STORE(&db->in_wrapped_dispatch, 1);
  int rc = sqlite3_step(stmt);
  if (is_ddl) ARK_STORE(&db->in_wrapped_dispatch, 0);
  // DDL executed through prepare/step used to bypass trigger resync —
  // a table created this way was never captured (spec §1). Resync once
  // the statement completes successfully. The flag check is one
  // load-free branch on the non-DDL hot path.
  if (rc == SQLITE_DONE && is_ddl) {
    const char *raw = sqlite3_sql(stmt);
    apply_ddl_capture(db, raw ? raw : "");
  }
  return rc;
}

int db_finalize(arkilian *db) {
  if (!db) return SQLITE_ERROR;
  sqlite3_stmt *stmt = get_current_stmt(db);
  if (stmt) {
    sqlite3_finalize(stmt);
    db->stmts[db->stmt_current] = NULL;
    if (db->stmt_is_ddl) db->stmt_is_ddl[db->stmt_current] = 0;
  }
  return SQLITE_OK;
}

int db_reset(arkilian *db) {
  sqlite3_stmt *stmt = get_current_stmt(db);
  if (!stmt) return SQLITE_ERROR;
  return sqlite3_reset(stmt);
}

// ── Column & Binding Accessors ──────────────────────────────────────

int db_column_count(arkilian *db) {
  sqlite3_stmt *stmt = get_current_stmt(db);
  return stmt ? sqlite3_column_count(stmt) : 0;
}

const char *db_column_name(arkilian *db, int col) {
  sqlite3_stmt *stmt = get_current_stmt(db);
  return stmt ? (const char *)sqlite3_column_name(stmt, col) : NULL;
}

const char *db_column_text(arkilian *db, int col) {
  sqlite3_stmt *stmt = get_current_stmt(db);
  return stmt ? (const char *)sqlite3_column_text(stmt, col) : NULL;
}

int db_column_int(arkilian *db, int col) {
  sqlite3_stmt *stmt = get_current_stmt(db);
  return stmt ? sqlite3_column_int(stmt, col) : 0;
}

double db_column_double(arkilian *db, int col) {
  sqlite3_stmt *stmt = get_current_stmt(db);
  return stmt ? sqlite3_column_double(stmt, col) : 0.0;
}

sqlite3_int64 db_column_int64(arkilian *db, int col) {
  sqlite3_stmt *stmt = get_current_stmt(db);
  return stmt ? sqlite3_column_int64(stmt, col) : 0;
}

int db_column_type(arkilian *db, int col) {
  sqlite3_stmt *stmt = get_current_stmt(db);
  return stmt ? sqlite3_column_type(stmt, col) : SQLITE_NULL;
}

const void *db_column_blob(arkilian *db, int col) {
  sqlite3_stmt *stmt = get_current_stmt(db);
  return stmt ? sqlite3_column_blob(stmt, col) : NULL;
}

int db_column_bytes(arkilian *db, int col) {
  sqlite3_stmt *stmt = get_current_stmt(db);
  return stmt ? sqlite3_column_bytes(stmt, col) : 0;
}

int db_bind_text(arkilian *db, int idx, const char *val) {
  sqlite3_stmt *stmt = get_current_stmt(db);
  if (!stmt || !val) return SQLITE_ERROR;
  return sqlite3_bind_text(stmt, idx, val, -1, SQLITE_TRANSIENT);
}

int db_bind_int(arkilian *db, int idx, int val) {
  sqlite3_stmt *stmt = get_current_stmt(db);
  return stmt ? sqlite3_bind_int(stmt, idx, val) : SQLITE_ERROR;
}

int db_bind_int64(arkilian *db, int idx, sqlite3_int64 val) {
  sqlite3_stmt *stmt = get_current_stmt(db);
  return stmt ? sqlite3_bind_int64(stmt, idx, val) : SQLITE_ERROR;
}

int db_bind_double(arkilian *db, int idx, double val) {
  sqlite3_stmt *stmt = get_current_stmt(db);
  return stmt ? sqlite3_bind_double(stmt, idx, val) : SQLITE_ERROR;
}

int db_bind_null(arkilian *db, int idx) {
  sqlite3_stmt *stmt = get_current_stmt(db);
  return stmt ? sqlite3_bind_null(stmt, idx) : SQLITE_ERROR;
}

int db_changes(arkilian *db) {
  return (db && db->handle) ? sqlite3_changes(db->handle) : 0;
}

sqlite3_int64 db_last_insert_rowid(arkilian *db) {
  return (db && db->handle) ? sqlite3_last_insert_rowid(db->handle) : 0;
}

// ── Transaction Control ─────────────────────────────────────────────

int db_begin(arkilian *db) {
  if (!db || !db->handle) return SQLITE_ERROR;
  if (db->in_batch_txn) return SQLITE_BUSY;
  int rc = sqlite3_step(db->begin_stmt);
  sqlite3_reset(db->begin_stmt);
  if (rc == SQLITE_DONE) {
    db->in_batch_txn = 1;
    return SQLITE_OK;
  }
  return rc;
}

int db_commit(arkilian *db) {
  if (!db || !db->handle) return SQLITE_ERROR;
  if (!db->in_batch_txn) return SQLITE_ERROR;
  int rc = sqlite3_step(db->commit_stmt);
  sqlite3_reset(db->commit_stmt);
  db->in_batch_txn = 0;
  return (rc == SQLITE_DONE) ? SQLITE_OK : rc;
}

int db_rollback(arkilian *db) {
  if (!db || !db->handle) return SQLITE_ERROR;
  if (!db->in_batch_txn) return SQLITE_ERROR;
  int rc = sqlite3_step(db->rollback_stmt);
  sqlite3_reset(db->rollback_stmt);
  db->in_batch_txn = 0;
  if (rc != SQLITE_DONE) {
    ark_log(db, ARK_LOG_ERROR, "rollback failed (rc=%d): %s",
            rc, sqlite3_errmsg(db->handle));
    return rc;
  }
  return SQLITE_OK;
}

// ── Introspection & Diagnostics ─────────────────────────────────────

int db_wal_pending(arkilian *db) {
  if (!db || !db->handle) return 0;
  sqlite3_stmt *stmt = NULL;
  int count = 0;
  if (sqlite3_prepare_v2(db->handle, "SELECT COUNT(*) FROM _pending_backup", -1, &stmt, NULL) == SQLITE_OK) {
    if (sqlite3_step(stmt) == SQLITE_ROW) {
      count = sqlite3_column_int(stmt, 0);
    }
    sqlite3_finalize(stmt);
  }
  return count;
}

const char *db_wal_last_sql(arkilian *db) {
  if (!db || !db->handle) return NULL;
  // Per-instance buffer — a static buffer would race and leak data
  // across database instances.
  db->wal_last_buf[0] = '\0';
  sqlite3_stmt *stmt = NULL;
  if (sqlite3_prepare_v2(db->handle, "SELECT payload FROM _pending_backup ORDER BY id DESC LIMIT 1", -1, &stmt, NULL) == SQLITE_OK) {
    if (sqlite3_step(stmt) == SQLITE_ROW) {
      const char *p = (const char *)sqlite3_column_text(stmt, 0);
      if (p) {
        strncpy(db->wal_last_buf, p, sizeof(db->wal_last_buf) - 1);
        db->wal_last_buf[sizeof(db->wal_last_buf) - 1] = '\0';
      }
    }
    sqlite3_finalize(stmt);
  }
  if (db->wal_last_buf[0] != '\0') return db->wal_last_buf;
#ifndef _WIN32
  pthread_mutex_lock(&db->payload_mutex);
#else
  EnterCriticalSection(&db->payload_mutex);
#endif
  if (db->last_shipped_payload[0] != '\0') {
    strncpy(db->wal_last_buf, db->last_shipped_payload, sizeof(db->wal_last_buf) - 1);
    db->wal_last_buf[sizeof(db->wal_last_buf) - 1] = '\0';
  }
#ifndef _WIN32
  pthread_mutex_unlock(&db->payload_mutex);
#else
  LeaveCriticalSection(&db->payload_mutex);
#endif
  return db->wal_last_buf[0] != '\0' ? db->wal_last_buf : NULL;
}

void db_wal_flush(arkilian *db) {
  if (!db) return;
#ifndef _WIN32
  pthread_mutex_lock(&db->wake_mutex);
  db->wake_flag = 1;
  pthread_cond_signal(&db->wake_cond);
  pthread_mutex_unlock(&db->wake_mutex);
#else
  EnterCriticalSection(&db->wake_mutex);
  db->wake_flag = 1;
  WakeConditionVariable(&db->wake_cond);
  LeaveCriticalSection(&db->wake_mutex);
#endif
}

// ── Runtime Kill-Switch ─────────────────────────────────────────────

// db_backup_set_enabled is the incident-response kill-switch (spec §1).
// Disabling stops ALL outbound backup activity — WAL shipping to the
// destination and hourly snapshot uploads — without touching game logic
// or requiring a restart. Capture keeps running: rows still accumulate
// in _pending_backup (attempts stay 0, nothing is deleted), so
// re-enabling resumes exactly where the queue left off.
//
// An in-flight ship/upload completes before the threads observe the new
// state; the switch gates new work, not already-running requests.
void db_backup_set_enabled(arkilian *db, int enabled) {
  if (!db) return;
#ifndef _WIN32
  pthread_mutex_lock(&db->wake_mutex);
  ARK_STORE(&db->backup_enabled, enabled ? 1 : 0);
  // Wake both threads so the new state is observed immediately (the
  // flush thread drains right away on re-enable instead of waiting out
  // the poll interval).
  db->wake_flag = 1;
  pthread_cond_broadcast(&db->wake_cond);
  pthread_mutex_unlock(&db->wake_mutex);
#else
  EnterCriticalSection(&db->wake_mutex);
  ARK_STORE(&db->backup_enabled, enabled ? 1 : 0);
  db->wake_flag = 1;
  WakeAllConditionVariable(&db->wake_cond);
  LeaveCriticalSection(&db->wake_mutex);
#endif
}

int db_backup_is_enabled(arkilian *db) {
  return (db && ARK_LOAD(&db->backup_enabled)) ? 1 : 0;
}

// ── Monitoring & health (spec §9) ───────────────────────────────────

int db_backup_queue_depth(arkilian *db) {
  return db_wal_pending(db);
}

long long db_backup_oldest_pending_age_sec(arkilian *db) {
  if (!db || !db->handle) return 0;
  sqlite3_stmt *stmt = NULL;
  long long age = 0;
  if (sqlite3_prepare_v2(db->handle,
        "SELECT strftime('%s','now') - MIN(created_at) FROM _pending_backup",
        -1, &stmt, NULL) == SQLITE_OK) {
    if (sqlite3_step(stmt) == SQLITE_ROW && sqlite3_column_type(stmt, 0) != SQLITE_NULL) {
      age = sqlite3_column_int64(stmt, 0);
      if (age < 0) age = 0;
    }
    sqlite3_finalize(stmt);
  }
  return age;
}

int db_backup_dead_letter_count(arkilian *db) {
  if (!db || !db->handle) return 0;
  sqlite3_stmt *stmt = NULL;
  int count = 0;
  if (sqlite3_prepare_v2(db->handle, "SELECT COUNT(*) FROM _dead_backup",
                         -1, &stmt, NULL) == SQLITE_OK) {
    if (sqlite3_step(stmt) == SQLITE_ROW) count = sqlite3_column_int(stmt, 0);
    sqlite3_finalize(stmt);
  }
  return count;
}

long long db_backup_thread_heartbeat_age_ms(arkilian *db) {
  if (!db) return -1;
  int hb = ARK_LOAD(&db->last_heartbeat_sec);
  if (hb == 0) return -1; // never beat — thread not (yet) running
  long long now = now_ms_mono();
  long long hb_ms = (long long)hb * 1000LL;
  return (now >= hb_ms) ? (now - hb_ms) : 0;
}

long long db_backup_snapshot_heartbeat_age_ms(arkilian *db) {
  if (!db) return -1;
  int hb = ARK_LOAD(&db->last_snapshot_heartbeat_sec);
  if (hb == 0) return -1; // never beat — thread not (yet) running
  long long now = now_ms_mono();
  long long hb_ms = (long long)hb * 1000LL;
  return (now >= hb_ms) ? (now - hb_ms) : 0;
}

int db_backup_trigger_coverage(arkilian *db) {
  if (!db || !db->handle) return -1;
  sqlite3_stmt *stmt = NULL;
  int expect = 0, have = 0;
  // Expected: 3 triggers per captured table. Must mirror the trigger
  // scan exactly: real (non-virtual, non-shadow) tables WITH a PRIMARY
  // KEY — keyless rowid tables are skipped (unreplayable) and get no
  // triggers.
  if (sqlite3_prepare_v2(db->handle,
        "SELECT COUNT(*) FROM pragma_table_list t "
        "WHERE t.schema = 'main' AND t.type = 'table' "
        "AND t.name NOT LIKE 'sqlite\\_%' ESCAPE '\\' "
        "AND t.name NOT IN ('_pending_backup', '_dead_backup', '_arkilian_meta') "
        "AND EXISTS (SELECT 1 FROM pragma_table_xinfo(t.name) WHERE pk > 0)",
        -1, &stmt, NULL) == SQLITE_OK) {
    if (sqlite3_step(stmt) == SQLITE_ROW) expect = 3 * sqlite3_column_int(stmt, 0);
    sqlite3_finalize(stmt);
  }
  if (sqlite3_prepare_v2(db->handle,
        "SELECT COUNT(*) FROM sqlite_master WHERE type = 'trigger' AND name LIKE 'trg\\_%' ESCAPE '\\'",
        -1, &stmt, NULL) == SQLITE_OK) {
    if (sqlite3_step(stmt) == SQLITE_ROW) have = sqlite3_column_int(stmt, 0);
    sqlite3_finalize(stmt);
  }
  int deficit = expect - have;
  return deficit < 0 ? 0 : deficit;
}

// ── Health state machine ────────────────────────────────────────────
// A durability product needs more than one boolean. db_backup_health_flags()
// isolates each failure class as a bit so operators can alarm on the
// specific degraded state; db_backup_is_healthy() is now defined ON TOP of
// the flags (single source of truth): healthy ⇔ every core flag set.
unsigned db_backup_health_flags(arkilian *db) {
  if (!db) return 0;
  unsigned f = 0;
  if (ARK_LOAD(&db->backup_enabled)) f |= ARK_HF_BACKUP_ENABLED;
  if (has_direct_s3(db)) f |= ARK_HF_DEST_CONFIGURED;
  // Flush thread liveness: a 30s threshold covers a 10s ship + margin.
  long long hb_age = db_backup_thread_heartbeat_age_ms(db);
  if (hb_age >= 0 && hb_age <= 30000) f |= ARK_HF_FLUSH_ALIVE;
  // Snapshot thread liveness: the hourly thread beats once per backup
  // interval (default 3600s); threshold is 2× interval + margin. A stale
  // snapshot heartbeat means the thread died.
  if (db->backup_interval > 0) {
    long long snap_age = db_backup_snapshot_heartbeat_age_ms(db);
    long long snap_threshold = (long long)db->backup_interval * 1000LL * 2 + 60000LL;
    if (snap_age >= 0 && snap_age <= snap_threshold) f |= ARK_HF_SNAPSHOT_ALIVE;
  } else {
    f |= ARK_HF_SNAPSHOT_ALIVE;  // no snapshot schedule — vacuously alive
  }
  if (db_backup_queue_depth(db) < outbox_cap()) f |= ARK_HF_QUEUE_BELOW_CAP;
  // (Risk #1) A raw-handle DDL gap means tables created/changed since it
  // are NOT being captured — a real, silent CDC hole, not a cosmetic
  // warning. The old health boolean ignored it.
  if (!ARK_LOAD(&db->triggers_dirty)) f |= ARK_HF_SCHEMA_IN_SYNC;
  // Dead-lettered rows are CDC rows that never shipped — a non-empty DLQ
  // is missing remote protection, visible here and via
  // db_backup_dead_letter_count().
  if (db_backup_dead_letter_count(db) == 0) f |= ARK_HF_NO_DEAD_LETTER;
  // A frozen startup manifest read means nothing is being PUBLISHED
  // (manifest PUTs are gated on it): restores silently use the last good
  // manifest while shipping continues. That is a restore gap, not a green
  // light.
  if (ARK_LOAD(&db->manifest_seed_resolved) == 1) f |= ARK_HF_MANIFEST_RESOLVED;
  // Sticky "CDC rows were dropped at the cap" signal — set until a
  // successful snapshot re-baselines. A gap occurred; healthy must wait
  // for the snapshot that closes it.
  if (!ARK_LOAD(&db->capture_paused)) f |= ARK_HF_NO_CAPTURE_GAP;
  // Informational: capture outbox durability mode (FULL vs NORMAL). Does
  // NOT gate the boolean — NORMAL is an operator choice, not a fault.
  if (db->outbox_durable) f |= ARK_HF_DURABLE_CAPTURE;
  return f;
}

// Default queue-depth ceiling for db_backup_is_healthy; override with
// ARKILIAN_MAX_QUEUE_DEPTH. Kept in sync with the cap baked into the
// capture triggers via outbox_cap().
int db_backup_is_healthy(arkilian *db) {
  if (!db) return 0;
  // A disabled subsystem is NOT healthy — whether kill-switched, forced
  // off by an init failure (WAL/trigger setup), or configured without a
  // destination. A green light while nothing is shipping is exactly the
  // silent failure monitoring exists to catch. Beyond the original five
  // checks (enabled/dest/heartbeats/queue), the boolean now also requires:
  // schema capture in sync, an empty dead-letter queue, a resolved (not
  // frozen) manifest registry, and no unclosed capture gap — each is a
  // state under which "backup enabled" silently stops meaning "every
  // committed mutation is remotely protected".
  return (db_backup_health_flags(db) & ARK_HF_ALL_CORE) == ARK_HF_ALL_CORE
             ? 1 : 0;
}

// ── WAL Chunk Monitoring ─────────────────────────────────────────

int db_backup_chunk_count(arkilian *db) {
  if (!db) return -1;
  // Cumulative count of WAL chunks durably uploaded AND recorded in the
  // manifest registry. (This used to return a 1/0 "has ever flushed"
  // value under a counter's name — telemetry built on it read a boolean
  // as a total.)
  return (db->chunks_flushed_total >= (uint64_t)INT_MAX)
             ? INT_MAX : (int)db->chunks_flushed_total;
}

long long db_backup_last_chunk_flush_age_ms(arkilian *db) {
  if (!db || db->chunk.last_s3_flush <= 0) return -1;
  long long now_s = (long long)time(NULL);
  long long age_s = now_s - (long long)db->chunk.last_s3_flush;
  return age_s * 1000LL;
}


// Count of real (non-virtual, non-shadow) tables that are NOT captured:
// rowid tables with no PRIMARY KEY are unreplayable and skipped by
// sync_backup_triggers. Every skipped table is data that never leaves
// the box — operators must see this, not just the one-time WARN.
int db_backup_skipped_table_count(arkilian *db) {
  if (!db || !db->handle) return -1;
  sqlite3_stmt *stmt = NULL;
  int count = -1;
  if (sqlite3_prepare_v2(db->handle,
        "SELECT COUNT(*) FROM pragma_table_list t "
        "WHERE t.schema = 'main' AND t.type = 'table' "
        "AND t.name NOT LIKE 'sqlite\\_%' ESCAPE '\\' "
        "AND t.name NOT IN ('_pending_backup', '_dead_backup', '_arkilian_meta') "
        "AND NOT EXISTS (SELECT 1 FROM pragma_table_xinfo(t.name) WHERE pk > 0)",
        -1, &stmt, NULL) == SQLITE_OK) {
    if (sqlite3_step(stmt) == SQLITE_ROW) count = sqlite3_column_int(stmt, 0);
    sqlite3_finalize(stmt);
  }
  return count;
}

int db_resync_triggers(arkilian *db) {
  if (!db || !db->handle) return SQLITE_ERROR;
  char *err = NULL;
  ARK_STORE(&db->trigger_sync_in_progress, 1);
  int rc = sync_backup_triggers(db->handle, &err);
  ARK_STORE(&db->trigger_sync_in_progress, 0);
  if (rc != SQLITE_OK) {
    ark_log(db, ARK_LOG_ERROR, "trigger resync failed: %s",
            err ? err : "unknown error");
    // resync failed — the schema may still be stale; leave the dirty flag
    // as-is so monitoring keeps surfacing the gap.
  } else {
    ARK_STORE(&db->triggers_dirty, 0);
  }
  if (err) sqlite3_free(err);
  return rc;
}

// (Risk #1) Whether a raw-handle schema change has desynchronized the
// capture triggers and is awaiting db_resync_triggers(). Surfaced so
// monitoring can detect the gap between DDL happening and the operator
// repairing it (or the next wrapped dispatch auto-repairing).
int db_backup_triggers_dirty(arkilian *db) {
  return (db && ARK_LOAD(&db->triggers_dirty)) ? 1 : 0;
}

// (Risk #1) Opt-in post-commit auto-resync. When enabled, the next wrapped
// dispatch (db_exec / db_step) checks triggers_dirty and resyncs before
// executing — so raw-handle DDL (Prisma/Drizzle/TypeORM) is auto-repaired.
// Defaults off. The resync runs on the game thread, post-commit, never
// inside SQLite's commit hook — so a resync failure can never roll back
// legitimate app work (spec §0).
void db_set_auto_resync_triggers(arkilian *db, int enabled) {
  if (!db) return;
  ARK_STORE(&db->auto_resync_triggers, enabled ? 1 : 0);
}

int db_get_auto_resync_triggers(arkilian *db) {
  return (db && ARK_LOAD(&db->auto_resync_triggers)) ? 1 : 0;
}

// Sticky capture-paused signal. Set by the flush thread when outbox depth
// hits ARKILIAN_MAX_QUEUE_DEPTH (CDC rows are being dropped — the trigger's
// `WHERE count < cap` gate stops inserting). Stays 1 after the queue
// drains so the operator knows "a gap occurred, the hourly snapshot is
// the fallback — verify it succeeded." Cleared by the snapshot thread
// after a successful upload. db_backup_is_healthy()==0 is too broad (fires
// for disabled/dest-down/thread-dead/cap); this is the specific "CDC rows
// are being dropped" signal.
int db_backup_capture_paused(arkilian *db) {
  return (db && ARK_LOAD(&db->capture_paused)) ? 1 : 0;
}

// ── Hourly Backup Implementation ────────────────────────────────────

int backup_database(sqlite3 *pSource, const char *zFilename,
                    volatile int *shutdown_flag) {
  if (!pSource) return SQLITE_ERROR;
  sqlite3 *pDest = NULL;
  const char *actualPath = (zFilename != NULL) ? zFilename : DEFAULT_BACKUP_PATH;
  int rc = sqlite3_open_v2(actualPath, &pDest, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, NULL);
  if (rc != SQLITE_OK) {
    if (pDest) sqlite3_close(pDest);
    return rc;
  }

  // Hardened posture: the snapshot is a full plaintext copy of the
  // customer database at a predictable path. Create it owner-only
  // (best-effort — an existing file keeps its mode; document that the
  // backup path should live in a protected directory).
#ifndef _WIN32
  (void)chmod(actualPath, 0600);
#else
  (void)_chmod(actualPath, _S_IREAD | _S_IWRITE);
#endif

  sqlite3_backup *pBackup = sqlite3_backup_init(pDest, "main", pSource, "main");
  if (!pBackup) {
    rc = sqlite3_errcode(pDest);
    sqlite3_close(pDest);
    return rc;
  }

  int busy_retries = 0;
  do {
    // Abort promptly on shutdown: without this check, a persistent
    // SQLITE_BUSY could hold db_close() for up to 6000x100ms (10
    // minutes) waiting to join this thread.
    if (shutdown_flag && ARK_LOAD(shutdown_flag)) {
      rc = SQLITE_ABORT;
      break;
    }
    rc = sqlite3_backup_step(pBackup, 5);
    if (rc == SQLITE_BUSY || rc == SQLITE_LOCKED) {
      if (++busy_retries >= 6000) {
        rc = SQLITE_BUSY;
        break;
      }
      sqlite3_sleep(100);
    }
  } while (rc == SQLITE_OK || rc == SQLITE_BUSY || rc == SQLITE_LOCKED);

  // Defensive check: a successful backup must report DONE with no pages left.
  if (rc == SQLITE_DONE && sqlite3_backup_remaining(pBackup) != 0) {
    rc = SQLITE_ERROR;
  }

  int finish_rc = sqlite3_backup_finish(pBackup);
  if (finish_rc != SQLITE_OK && rc == SQLITE_DONE) rc = finish_rc;
  else if (rc == SQLITE_DONE) rc = SQLITE_OK;
  sqlite3_close(pDest);
  return rc;
}

// PUT a file to object storage via a locally presigned URL. Streams from
// disk so multi-hundred-MB snapshots never sit in memory.
static int upload_to_s3(arkilian *db, const char *signed_url,
                       const char *file_path) {
  // Defense-in-depth SSRF guard: never upload the database to a host that
  // is not an allowed storage destination.
  if (!url_is_allowed_storage(signed_url)) {
    ark_log(db, ARK_LOG_ERROR,
            "upload_to_s3 refused: signed_url host is not an allowed "
            "storage destination (SSRF guard): %.200s", signed_url);
    return 1;
  }
  CURL *curl = curl_easy_init();
  if (!curl) return 1;
  FILE *fd = fopen(file_path, "rb");
  if (!fd) {
    curl_easy_cleanup(curl);
    return 1;
  }

  if (fseek(fd, 0L, SEEK_END) != 0) {
    fclose(fd);
    curl_easy_cleanup(curl);
    return 1;
  }
  long file_size = ftell(fd);
  if (file_size < 0) {
    fclose(fd);
    curl_easy_cleanup(curl);
    return 1;
  }
  // rewind() discards errno — a failed seek would silently upload from
  // the wrong position, producing a torn snapshot. Use fseek() and treat
  // any failure as an upload failure (consistent with the SEEK_END probe
  // above) so the snapshot re-attempts on the next hourly cycle rather
  // than shipping a corrupted backup.
  if (fseek(fd, 0L, SEEK_SET) != 0) {
    fclose(fd);
    curl_easy_cleanup(curl);
    return 1;
  }

  // Every curl_easy_setopt / curl_slist_append return code is checked —
  // a misconfigured upload must be reported, not silently swallowed.
  CURLcode rc = CURLE_OK;
  rc = curl_easy_setopt(curl, CURLOPT_URL, signed_url);
  if (rc == CURLE_OK) rc = curl_easy_setopt(curl, CURLOPT_UPLOAD, 1L);
  if (rc == CURLE_OK) rc = curl_easy_setopt(curl, CURLOPT_READDATA, fd);
  if (rc == CURLE_OK) rc = curl_easy_setopt(curl, CURLOPT_INFILESIZE_LARGE, (curl_off_t)file_size);
  // Timeout scales with file size (~10s per MB past the 30s base) so
  // multi-hundred-MB snapshots aren't guaranteed failures on a WAN.
  if (rc == CURLE_OK) rc = curl_easy_setopt(curl, CURLOPT_TIMEOUT, curl_timeout_sec((size_t)file_size, 30));
  if (rc == CURLE_OK) rc = curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT, 10L);
  // Explicit TLS verification posture — system defaults are 1/2; setting
  // them explicitly documents intent and protects against a future patch
  // accidentally disabling verification.
  if (rc == CURLE_OK) rc = curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 1L);
  if (rc == CURLE_OK) rc = curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, 2L);
  if (rc == CURLE_OK) rc = curl_easy_setopt(curl, CURLOPT_NOPROGRESS, 0L);
  if (rc == CURLE_OK) rc = curl_easy_setopt(curl, CURLOPT_XFERINFOFUNCTION, curl_abort_cb);
  if (rc == CURLE_OK) rc = curl_easy_setopt(curl, CURLOPT_XFERINFODATA, (void *)&db->shutdown_requested);

  struct curl_slist *headers = NULL;
  if (rc == CURLE_OK) {
    headers = curl_slist_append(headers, "Content-Type: application/x-sqlite3");
    if (!headers) rc = CURLE_OUT_OF_MEMORY;
  }
  if (rc == CURLE_OK) rc = curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

  int ok = 0;
  if (rc != CURLE_OK) {
    ark_log(db, ARK_LOG_ERROR, "backup upload: request setup failed: %s",
             curl_easy_strerror(rc));
  } else {
    CURLcode res = curl_easy_perform(curl);
    long http_code = 0;
    if (res == CURLE_OK) curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);

    // A completed transfer is not success — check the HTTP status so
    // rejected uploads (4xx/5xx) are reported rather than swallowed.
    ok = (res == CURLE_OK && http_code >= 200 && http_code < 300);
    if (!ok) {
      ark_log(db, ARK_LOG_ERROR, "backup upload failed (curl_rc=%d http=%ld)",
               (int)res, http_code);
    }
  }

  fclose(fd);
  curl_slist_free_all(headers);
  curl_easy_cleanup(curl);

  return ok ? 0 : 1;
}

// ── Direct S3 upload helpers ───────────────────────────────────────
// When ARKILIAN_S3_ENDPOINT / _BUCKET / _ACCESS_KEY / _SECRET_KEY are
// configured, the snapshot thread signs presigned PUT URLs locally using
// AWS Signature V4 and uploads directly to S3 — no intermediate service
// in the data write path.

static int has_direct_s3(arkilian *db) {
  return db &&
    db->s3_endpoint && db->s3_endpoint[0] &&
    db->s3_bucket && db->s3_bucket[0] &&
    db->s3_access_key && db->s3_access_key[0] &&
    db->s3_secret_key && db->s3_secret_key[0];
}

static void s3_url_encode_inline(const char *src, char *dst, size_t cap) {
  static const char *okchars =
      "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_.~";
  size_t w = 0;
  for (const char *p = src; *p && w + 4 < cap; p++) {
    if (strchr(okchars, *p))
      dst[w++] = *p;
    else
      w += (size_t)snprintf(dst + w, cap - w, "%%%02X", (unsigned char)*p);
  }
  dst[w] = '\0';
}

static char *s3_presign_put(arkilian *db, const char *key, long expires_sec) {
  if (!db || !key || !has_direct_s3(db)) return NULL;

  time_t now = time(NULL);
  struct tm g;
  memset(&g, 0, sizeof(g));
  // A failed conversion must never sign with a garbage timestamp: the
  // resulting URL would be rejected by the endpoint and (worse) could
  // carry a date_stamp outside the credential's validity window.
  if (!ark_gmtime_utc(&now, &g)) return NULL;
  char date_stamp[9], amz_date[17];
  strftime(date_stamp, sizeof(date_stamp), "%Y%m%d", &g);
  strftime(amz_date, sizeof(amz_date), "%Y%m%dT%H%M%SZ", &g);

  char cred_plain[512];
  snprintf(cred_plain, sizeof(cred_plain), "%s/%s/%s/s3/aws4_request",
           db->s3_access_key, date_stamp, db->s3_region);
  char cred_enc[2048];
  s3_url_encode_inline(cred_plain, cred_enc, sizeof(cred_enc));

  const char *scheme = "https";
  const char *host = db->s3_endpoint;
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

  // URL-encode the S3 object key for the URL path: per AWS SigV4 for the
  // S3 service the canonical URI is NOT normalized and the '/' separators
  // between key components are NOT encoded (they're structural). S3 keys
  // built here are always "db_<hex>/backup.sqlite" or
  // "db_<hex>/chunks/lsn_..._...sql.zst" — all chars are already in the
  // RFC 3986 unreserved set except '/'. The shared s3_url_encode_inline
  // helper preserves '/', so passing the key through it both (a) ensures
  // any future exotic char in a key is handled and (b) keeps the canonical
  // request's path identical to the path in the final URL (S3 recomputes
  // the signature from the URL it receives and they must match byte for
  // byte). For keys containing only the unreserved set the encoding is a
  // no-op — the safe, defensive default.
  char key_enc[1024];
  s3_url_encode_inline(key, key_enc, sizeof(key_enc));
  // The helper above encodes ALL non-unreserved chars, but S3 SigV4
  // requires '/' to remain literal in the canonical URI for the S3
  // service. Walk key_enc and convert %2F → / so path separators are
  // preserved exactly as S3 expects.
  for (char *p = key_enc; *p; p++) {
    if (p[0] == '%' && p[1] == '2' && (p[2] == 'F' || p[2] == 'f')) {
      *p = '/'; memmove(p + 1, p + 3, strlen(p + 3) + 1);
    }
  }

  char canonical[4096];
  snprintf(canonical, sizeof(canonical),
    "PUT\n/%s/%s\n"
    "X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=%s&"
    "X-Amz-Date=%s&X-Amz-Expires=%ld&X-Amz-SignedHeaders=host\n"
    "host:%s\n\nhost\nUNSIGNED-PAYLOAD",
    db->s3_bucket, key_enc, cred_enc, amz_date, expires_sec, host_clean);

  char scope[256];
  snprintf(scope, sizeof(scope), "%s/%s/s3/aws4_request",
           date_stamp, db->s3_region);
  char canon_hash[65];
  ark_sha256_hex(canonical, strlen(canonical), canon_hash);

  char sts[1024];
  snprintf(sts, sizeof(sts), "AWS4-HMAC-SHA256\n%s\n%s\n%s",
           amz_date, scope, canon_hash);

  uint8_t k_date[32], k_region[32], k_service[32], k_signing[32];
  char kseed[512];
  snprintf(kseed, sizeof(kseed), "AWS4%s", db->s3_secret_key);
  ark_hmac_sha256((const uint8_t *)kseed, strlen(kseed),
                  date_stamp, strlen(date_stamp), k_date);
  ark_hmac_sha256(k_date, 32, db->s3_region, strlen(db->s3_region), k_region);
  ark_hmac_sha256(k_region, 32, "s3", 2, k_service);
  ark_hmac_sha256(k_service, 32, "aws4_request", 12, k_signing);

  char sig_hex[65];
  ark_hmac_sha256_hex(k_signing, 32, sts, strlen(sts), sig_hex);

  char sig_enc[256];
  s3_url_encode_inline(sig_hex, sig_enc, sizeof(sig_enc));

  // Final URL path uses the URL-encoded key (matches the canonical request,
  // so S3's signature recomputation matches; '/' in the key is %2F-encoded
  // and decoded by S3 as part of the object key, not as a path separator).
  size_t url_len = strlen(scheme) + 3 + strlen(host_clean) + 1 +
                   strlen(db->s3_bucket) + 1 + strlen(key_enc) + 2048;
  char *url = malloc(url_len);
  if (!url) return NULL;
  snprintf(url, url_len,
    "%s://%s/%s/%s"
    "?X-Amz-Algorithm=AWS4-HMAC-SHA256"
    "&X-Amz-Credential=%s"
    "&X-Amz-Date=%s"
    "&X-Amz-Expires=%ld"
    "&X-Amz-SignedHeaders=host"
    "&X-Amz-Signature=%s",
    scheme, host_clean, db->s3_bucket, key_enc,
    cred_enc, amz_date, expires_sec, sig_enc);
  return url;
}

// GET variant of s3_presign_put.
char *db_s3_presign_get(arkilian *db, const char *key, long expires_sec) {
  if (!db || !key || !has_direct_s3(db)) return NULL;

  time_t now = time(NULL);
  struct tm g;
  memset(&g, 0, sizeof(g));
  if (!ark_gmtime_utc(&now, &g)) return NULL;
  char date_stamp[9], amz_date[17];
  strftime(date_stamp, sizeof(date_stamp), "%Y%m%d", &g);
  strftime(amz_date, sizeof(amz_date), "%Y%m%dT%H%M%SZ", &g);

  char cred_plain[512];
  snprintf(cred_plain, sizeof(cred_plain), "%s/%s/%s/s3/aws4_request",
           db->s3_access_key, date_stamp, db->s3_region);
  char cred_enc[2048];
  s3_url_encode_inline(cred_plain, cred_enc, sizeof(cred_enc));

  const char *scheme = "https";
  const char *host = db->s3_endpoint;
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
  s3_url_encode_inline(key, key_enc, sizeof(key_enc));
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
    db->s3_bucket, key_enc, cred_enc, amz_date, expires_sec, host_clean);

  char scope[256];
  snprintf(scope, sizeof(scope), "%s/%s/s3/aws4_request",
           date_stamp, db->s3_region);
  char canon_hash[65];
  ark_sha256_hex(canonical, strlen(canonical), canon_hash);

  char sts[1024];
  snprintf(sts, sizeof(sts), "AWS4-HMAC-SHA256\n%s\n%s\n%s",
           amz_date, scope, canon_hash);

  uint8_t k_date[32], k_region[32], k_service[32], k_signing[32];
  char kseed[512];
  snprintf(kseed, sizeof(kseed), "AWS4%s", db->s3_secret_key);
  ark_hmac_sha256((const uint8_t *)kseed, strlen(kseed),
                  date_stamp, strlen(date_stamp), k_date);
  ark_hmac_sha256(k_date, 32, db->s3_region, strlen(db->s3_region), k_region);
  ark_hmac_sha256(k_region, 32, "s3", 2, k_service);
  ark_hmac_sha256(k_service, 32, "aws4_request", 12, k_signing);

  char sig_hex[65];
  ark_hmac_sha256_hex(k_signing, 32, sts, strlen(sts), sig_hex);

  char sig_enc[256];
  s3_url_encode_inline(sig_hex, sig_enc, sizeof(sig_enc));

  size_t url_len = strlen(scheme) + 3 + strlen(host_clean) + 1 +
                   strlen(db->s3_bucket) + 1 + strlen(key_enc) + 2048;
  char *url = malloc(url_len);
  if (!url) return NULL;
  snprintf(url, url_len,
    "%s://%s/%s/%s"
    "?X-Amz-Algorithm=AWS4-HMAC-SHA256"
    "&X-Amz-Credential=%s"
    "&X-Amz-Date=%s"
    "&X-Amz-Expires=%ld"
    "&X-Amz-SignedHeaders=host"
    "&X-Amz-Signature=%s",
    scheme, host_clean, db->s3_bucket, key_enc,
    cred_enc, amz_date, expires_sec, sig_enc);

  return url;
}

// Escape a string for embedding inside a JSON string literal: quotes and
// backslashes are backslash-escaped, control characters use \u00XX.
// Returns a malloc'd copy of the escaped text, or NULL on OOM. NULL input
// is treated as the empty string. Needed because the prefix and snapshot
// keys are operator/authored strings — embedding them raw would let a `"`
// character produce a malformed manifest.
static char *json_escape_str(const char *s) {
  const char *src = s ? s : "";
  size_t len = 0;
  for (const char *p = src; *p; p++) {
    unsigned char c = (unsigned char)*p;
    len += (c == '"' || c == '\\') ? 2 : (c < 0x20) ? 6 : 1;
  }
  char *out = malloc(len + 1);
  if (!out) return NULL;
  static const char hex[] = "0123456789abcdef";
  char *w = out;
  for (const char *p = src; *p; p++) {
    unsigned char c = (unsigned char)*p;
    if (c == '"' || c == '\\') { *w++ = '\\'; *w++ = (char)c; }
    else if (c < 0x20) {
      *w++ = '\\'; *w++ = 'u'; *w++ = '0'; *w++ = '0';
      *w++ = hex[(c >> 4) & 0xf]; *w++ = hex[c & 0xf];
    } else {
      *w++ = (char)c;
    }
  }
  *w = '\0';
  return out;
}

// ── Manifest registry ───────────────────────────────────────────────
// {prefix}/manifest.json is the durable registry that hydration replays:
//   {"version":3,"prefix":"...","snapshot":{"s3_key","sha256","baseline_lsn"},
//    "chunks":[{"s3_key","sha256","lsn_start","lsn_end"}, ...]}
// The flush thread appends chunk records, the snapshot thread sets the
// baseline and prunes covered chunks, and db_init seeds the registry from
// the last uploaded copy so a restart never orphans shipped chunks.

// Append formatted text to a growable buffer. Returns 0 on success.
static int jbuf_append(char **buf, size_t *len, size_t *cap,
                       const char *fmt, ...) {
  va_list ap, ap2;
  va_start(ap, fmt);
  va_copy(ap2, ap);
  int need = vsnprintf(NULL, 0, fmt, ap);
  va_end(ap);
  if (need < 0) { va_end(ap2); return -1; }
  if (*len + (size_t)need + 1 > *cap) {
    size_t ncap = *cap ? *cap * 2 : 1024;
    while (ncap < *len + (size_t)need + 1) ncap *= 2;
    char *nb = realloc(*buf, ncap);
    if (!nb) { va_end(ap2); return -1; }
    *buf = nb;
    *cap = ncap;
  }
  vsnprintf(*buf + *len, (size_t)need + 1, fmt, ap2);
  va_end(ap2);
  *len += (size_t)need;
  return 0;
}

static void manifest_registry_lock(arkilian *db) {
#ifndef _WIN32
  pthread_mutex_lock(&db->manifest_mutex);
#else
  EnterCriticalSection(&db->manifest_mutex);
#endif
}

static void manifest_registry_unlock(arkilian *db) {
#ifndef _WIN32
  pthread_mutex_unlock(&db->manifest_mutex);
#else
  LeaveCriticalSection(&db->manifest_mutex);
#endif
}

// Append a chunk record. Caller MUST hold manifest_mutex. Takes ownership
// of s3_key/sha256 on success; frees them on failure. The locking wrapper
// below is what the flush thread uses; the seed path calls this directly
// because it already holds the lock (re-locking a non-recursive pthread
// mutex from the same thread deadlocks).
static int manifest_registry_append_locked(arkilian *db, char *s3_key,
                                           char *sha256,
                                           uint64_t lsn_start,
                                           uint64_t lsn_end) {
  if (!db || !s3_key) { free(s3_key); free(sha256); return -1; }
  if (db->manifest_chunk_count == db->manifest_chunk_cap) {
    int ncap = db->manifest_chunk_cap ? db->manifest_chunk_cap * 2 : 16;
    ark_manifest_chunk *nc =
        realloc(db->manifest_chunks, (size_t)ncap * sizeof(ark_manifest_chunk));
    if (!nc) {
      free(s3_key); free(sha256);
      return -1;
    }
    db->manifest_chunks = nc;
    db->manifest_chunk_cap = ncap;
  }
  ark_manifest_chunk *rec = &db->manifest_chunks[db->manifest_chunk_count++];
  rec->s3_key = s3_key;
  rec->sha256 = sha256;
  rec->lsn_start = lsn_start;
  rec->lsn_end = lsn_end;
  db->manifest_pending++;
  return 0;
}

// Takes ownership of s3_key/sha256 on success; frees them on failure.
static int manifest_registry_append(arkilian *db, char *s3_key, char *sha256,
                                    uint64_t lsn_start, uint64_t lsn_end) {
  if (!db || !s3_key) { free(s3_key); free(sha256); return -1; }
  manifest_registry_lock(db);
  int rc = manifest_registry_append_locked(db, s3_key, sha256,
                                           lsn_start, lsn_end);
  manifest_registry_unlock(db);
  return rc;
}

// Drop chunk records fully covered by the new baseline snapshot: their
// rows are already IN the snapshot, so replaying them is redundant.
static void manifest_registry_prune_upto(arkilian *db, uint64_t baseline_lsn) {
  manifest_registry_lock(db);
  int w = 0;
  for (int i = 0; i < db->manifest_chunk_count; i++) {
    ark_manifest_chunk *rec = &db->manifest_chunks[i];
    if (rec->lsn_end <= baseline_lsn) {
      free(rec->s3_key);
      free(rec->sha256);
    } else {
      db->manifest_chunks[w++] = *rec;
    }
  }
  db->manifest_chunk_count = w;
  manifest_registry_unlock(db);
}

// Rebuild and PUT {prefix}/manifest.json from the in-memory registry.
// snapshot_key/sha may be NULL to keep the previously recorded baseline.
// Returns 0 on success. The registry itself is untouched on failure —
// a later flush or snapshot retries the upload.
static int manifest_registry_upload(arkilian *db, const char *snapshot_key,
                                    const char *snapshot_sha,
                                    uint64_t baseline_lsn) {
  if (!db || !has_direct_s3(db)) return -1;
  if (!db->s3_prefix || !db->s3_prefix[0]) return -1;
  if (strstr(db->s3_prefix, "..") || db->s3_prefix[0] == '/') return -1;
  // Never publish over a predecessor registry this process could not read.
  // Its chunk records are the only pointers to objects whose outbox rows
  // were already deleted, so overwriting them is unrecoverable data loss.
  // The registry stays frozen (and loudly logged by the seed path) until
  // the startup read resolves.
  if (ARK_LOAD(&db->manifest_seed_resolved) != 1) return -1;

  manifest_registry_lock(db);
  if (snapshot_key) {
    char *nk = strdup(snapshot_key);
    char *ns = snapshot_sha ? strdup(snapshot_sha) : NULL;
    if (!nk || (snapshot_sha && !ns)) {
      free(nk); free(ns);
      manifest_registry_unlock(db);
      return -1;
    }
    free(db->manifest_snapshot_key);
    free(db->manifest_snapshot_sha);
    db->manifest_snapshot_key = nk;
    db->manifest_snapshot_sha = ns;
    db->manifest_baseline_lsn = baseline_lsn;
  }

  char *json = NULL;
  size_t jlen = 0, jcap = 0;
  char *prefix_json = json_escape_str(db->s3_prefix);
  char *skey_json = json_escape_str(db->manifest_snapshot_key
                                        ? db->manifest_snapshot_key : "");
  char *ssha_json = json_escape_str(db->manifest_snapshot_sha
                                        ? db->manifest_snapshot_sha : "");
  int bad = !prefix_json || !skey_json || !ssha_json ||
            jbuf_append(&json, &jlen, &jcap,
                        "{\"version\":3,\"prefix\":\"%s\","
                        "\"snapshot\":{\"s3_key\":\"%s\",\"sha256\":\"%s\","
                        "\"baseline_lsn\":%lld},\"chunks\":[",
                        prefix_json, skey_json, ssha_json,
                        (long long)db->manifest_baseline_lsn) != 0;
  for (int i = 0; !bad && i < db->manifest_chunk_count; i++) {
    ark_manifest_chunk *rec = &db->manifest_chunks[i];
    char *k = json_escape_str(rec->s3_key);
    char *s = json_escape_str(rec->sha256 ? rec->sha256 : "");
    bad = !k || !s ||
          jbuf_append(&json, &jlen, &jcap,
                      "%s{\"s3_key\":\"%s\",\"sha256\":\"%s\","
                      "\"lsn_start\":%llu,\"lsn_end\":%llu}",
                      i ? "," : "", k, s,
                      (unsigned long long)rec->lsn_start,
                      (unsigned long long)rec->lsn_end) != 0;
    free(k);
    free(s);
  }
  free(prefix_json);
  free(skey_json);
  free(ssha_json);
  if (!bad) bad = jbuf_append(&json, &jlen, &jcap, "]}") != 0;
  if (bad) {
    free(json);
    manifest_registry_unlock(db);
    return -1;
  }

  // Stage the manifest body in a unique per-instance file (fixed
  // "%s.manifest" names collided across instances sharing a backup path).
  char tmp_path[1200];
  FILE *f = arkilian_unique_tmp(db->backup_path, "manifest",
                                tmp_path, sizeof(tmp_path));
  if (!f) {
    free(json);
    manifest_registry_unlock(db);
    return -1;
  }
  size_t wrote = fwrite(json, 1, jlen, f);
  fclose(f);
  if (wrote != jlen) {
    free(json);
    unlink(tmp_path);
    manifest_registry_unlock(db);
    return -1;
  }

  // ── Manifest authenticity: sign the exact bytes we are about to PUT ──
  // The manifest is the root of trust for the restore protocol (its
  // digest fields cannot authenticate it — that would be circular). With
  // ARKILIAN_MANIFEST_HMAC_KEY set, {prefix}/manifest.sig carries an
  // HMAC-SHA-256 over the exact manifest bytes; hydration and the seed
  // path (shared ark_manifest_fetch) fail closed on a missing/mismatched
  // signature. If signing is configured but cannot be published, we
  // refuse to publish an unsigned manifest at all: the registry keeps its
  // records and the next cadence retries.
  char sig_path[1200];
  int have_sig = 0;
  if (db->manifest_hmac_key && db->manifest_hmac_key[0]) {
    char sig_hex[65];
    ark_hmac_sha256_hex((const uint8_t *)db->manifest_hmac_key,
                        strlen(db->manifest_hmac_key),
                        json, jlen, sig_hex);
    FILE *sf = arkilian_unique_tmp(db->backup_path, "manifestsig",
                                   sig_path, sizeof(sig_path));
    if (!sf || fwrite(sig_hex, 1, 64, sf) != 64) {
      if (sf) fclose(sf);
      unlink(sig_path);
      free(json);
      manifest_registry_unlock(db);
      ark_log(db, ARK_LOG_ERROR,
              "manifest.sig staging failed — refusing to publish an "
              "unsigned manifest (HMAC key configured)");
      return -1;
    }
    fclose(sf);
    have_sig = 1;
  }

  char manifest_key[512];
  snprintf(manifest_key, sizeof(manifest_key), "%s/manifest.json",
           db->s3_prefix);
  char *put_url = s3_presign_put(db, manifest_key, 600L);
  if (!put_url) {
    free(json);
    unlink(tmp_path);
    if (have_sig) unlink(sig_path);
    manifest_registry_unlock(db);
    return -1;
  }
  // Publish order: manifest.json first, then manifest.sig (hydrators
  // re-fetch both once on a signature mismatch to absorb the torn
  // two-object commit window).
  int rc = upload_to_s3(db, put_url, tmp_path);
  free(put_url);
  unlink(tmp_path);

  if (rc == 0 && have_sig) {
    char sig_key[512];
    snprintf(sig_key, sizeof(sig_key), "%s/manifest.sig", db->s3_prefix);
    char *sig_url = s3_presign_put(db, sig_key, 600L);
    if (!sig_url || upload_to_s3(db, sig_url, sig_path) != 0) {
      ark_log(db, ARK_LOG_ERROR,
              "manifest.sig upload failed — HMAC-configured hydrators will "
              "refuse manifest.json until the signature publish succeeds");
      free(sig_url);
      free(json);
      unlink(sig_path);
      manifest_registry_unlock(db);
      return -1;
    }
    free(sig_url);
  }
  if (have_sig) unlink(sig_path);
  free(json);

  if (rc == 0) {
    db->manifest_pending = 0;
    db->manifest_last_upload = time(NULL);
    // The manifest (and its signature) are durable: chunk records it
    // names are now reachable by hydration. Outbox rows for those chunks
    // were already deleted on flush ack (delete-on-flush-ack); any chunk
    // PUT in the batching window but not yet named here is re-covered by
    // the next hourly snapshot baseline, so nothing accumulates.
  }
  manifest_registry_unlock(db);
  return rc;
}

#define MANIFEST_UPLOAD_MIN_INTERVAL_SEC 30
// Manifest PUTs are batched: at most one per
// MANIFEST_UPLOAD_MIN_INTERVAL_SEC, so the 1s chunk cadence doesn't double
// the request count against the storage endpoint. Overridable for tests
// via ARKILIAN_MANIFEST_INTERVAL_SEC.
static int manifest_interval_sec(void) {
  int v = get_env_int_default("ARKILIAN_MANIFEST_INTERVAL_SEC",
                              MANIFEST_UPLOAD_MIN_INTERVAL_SEC);
  return v < 1 ? 1 : v;
}
static void manifest_registry_maybe_upload(arkilian *db) {
  manifest_registry_lock(db);
  int pending = db->manifest_pending;
  time_t last = db->manifest_last_upload;
  manifest_registry_unlock(db);
  if (pending <= 0) return;
  time_t now = time(NULL);
  int interval = manifest_interval_sec();
  if (last != 0 && now - last < interval) return;
  manifest_registry_upload(db, NULL, NULL, 0);
}

// Seed the registry from the last uploaded manifest (restart safety): a
// process that restarts between snapshots must adopt its predecessor's
// chunk records, or hydration would never replay them. Also resolves the
// startup manifest read that gates ALL manifest publishes:
//   - success or HYDRATION_ERR_NOTFOUND (genuine cold start) → resolved:
//     registry adopted, or confirmed empty — publishing may proceed
//   - HYDRATION_ERR_PROTO (present but corrupt/foreign) → frozen: never
//     overwrite a registry this process could not read (its chunk records
//     are the only pointers to objects whose outbox rows are already
//     deleted)
//   - transient net/mem failure → stays unresolved; the flush loop retries
//     on a bounded cadence, and NO manifest is published until then
// Returns 1 once resolved (terminal), 0 while still pending.
static int manifest_registry_seed(arkilian *db) {
  if (!db || !has_direct_s3(db)) {
    // No destination: there is no remote registry to adopt and nothing to
    // gate — record the resolved state so db_backup_health_flags() reports
    // the truth instead of a permanently "unresolved" read.
    ARK_STORE(&db->manifest_seed_resolved, 1);
    return 1;
  }
  if (!db->s3_prefix || !db->s3_prefix[0]) return 1;
  HydratePlan plan;
  int rc = ark_manifest_fetch(db->s3_endpoint, db->s3_bucket, db->s3_region,
                              db->s3_access_key, db->s3_secret_key,
                              db->s3_prefix, &plan);
  if (rc != 0) {
    if (rc == HYDRATION_ERR_NOTFOUND) {
      ARK_STORE(&db->manifest_seed_resolved, 1);  // cold start, nothing to adopt
    } else if (rc == HYDRATION_ERR_PROTO) {
      ARK_STORE(&db->manifest_seed_resolved, -1); // frozen; see comment above
    } else {
      return 0; // transient (net/mem) — retried by the flush loop
    }
    return 1;
  }
  manifest_registry_lock(db);
  for (int i = 0; i < plan.chunk_count; i++) {
    HydrateChunk *ch = &plan.chunks[i];
    if (!ch->s3_key || !ch->s3_key[0]) continue;
    // _locked variant: this path already holds manifest_mutex (calling the
    // plain wrapper here would deadlock the non-recursive mutex).
    manifest_registry_append_locked(db, strdup(ch->s3_key),
                                    ch->sha256 ? strdup(ch->sha256) : NULL,
                                    (uint64_t)ch->lsn_start,
                                    (uint64_t)ch->lsn_end);
  }
  if (plan.snapshot_s3_key) {
    free(db->manifest_snapshot_key);
    free(db->manifest_snapshot_sha);
    db->manifest_snapshot_key = strdup(plan.snapshot_s3_key);
    db->manifest_snapshot_sha =
        plan.snapshot_sha256 ? strdup(plan.snapshot_sha256) : NULL;
    db->manifest_baseline_lsn = (uint64_t)plan.baseline_lsn;
  }
  db->manifest_pending = 0;
  db->manifest_last_upload = time(NULL);
  manifest_registry_unlock(db);
  ARK_STORE(&db->manifest_seed_resolved, 1);
  hydrate_plan_free(&plan);
  return 1;
}

// Overridable copy step for deterministic snapshot-cycle tests. NULL (the
// production default) uses backup_database(). The hook lets a test run the
// REAL cycle while injecting work into the copy window — the exact window
// in which the flush thread ships chunks concurrently.
int (*arkilian_snapshot_copy_hook)(sqlite3 *src, const char *dest_path,
                                   volatile int *shutdown_flag) = NULL;

// Record a chunk in the manifest registry. Production path is
// wal_chunk_flush_to_s3 (which also batches manifest publishes); this
// wrapper exists for operational tooling and the snapshot-cycle
// determinism tests, which simulate "the flusher shipped a chunk while
// the snapshot copy was running".
int arkilian_registry_record(arkilian *db, const char *s3_key,
                             const char *sha256, uint64_t lsn_start,
                             uint64_t lsn_end) {
  if (!db || !s3_key) return -1;
  char *k = strdup(s3_key);
  char *s = sha256 ? strdup(sha256) : NULL;
  if (!k || (sha256 && !s)) { free(k); free(s); return -1; }
  if (manifest_registry_append(db, k, s, lsn_start, lsn_end) != 0) return -1;
  return 0;
}

// One snapshot attempt: heartbeat → capture pruning watermark → copy →
// upload → prune + publish. Extracted from the hourly loop so the
// watermark/copy ordering is directly testable.
//
// ── P0 correctness note (snapshot-baseline invariant) ────────────────
// The baseline LSN published with a snapshot MUST be captured BEFORE the
// copy starts. The previous code read it from the (mutable) manifest
// registry AFTER backup_database() returned — while claiming in its own
// comment that it was "the highest chunk LSN flushed before the copy
// began". A chunk shipped during the copy (the normal case under
// continuous writes) would be declared "contained in the snapshot",
// pruned from the registry, and then skipped by hydration — a silent
// restore gap. The copy's image is guaranteed to contain every change
// committed BEFORE it started (sqlite3_backup_step restarts on source
// modification, so the image is some state in [copy-start, copy-end]);
// only a pre-copy watermark is sound.
int arkilian_run_snapshot_cycle(arkilian *db) {
  if (!db || !db->is_open || !db->handle) return -1;
  // Kill-switch: skip the snapshot + upload entirely while disabled.
  if (!ARK_LOAD(&db->backup_enabled)) return 0;

  // Snapshot-thread heartbeat (spec §9): so a silent death of this
  // thread is visible via db_backup_snapshot_heartbeat_age_ms().
  ARK_STORE(&db->last_snapshot_heartbeat_sec, (int)(now_ms_mono() / 1000));

  // ── Watermark FIRST (the fix): highest registered chunk LSN, floored by
  // the previously published baseline so a later snapshot never
  // REPUBLISHES a LOWER baseline over a durable higher one.
  uint64_t snapshot_upto = 0;
  manifest_registry_lock(db);
  for (int i = 0; i < db->manifest_chunk_count; i++) {
    if (db->manifest_chunks[i].lsn_end > snapshot_upto)
      snapshot_upto = db->manifest_chunks[i].lsn_end;
  }
  if (db->manifest_baseline_lsn > snapshot_upto)
    snapshot_upto = db->manifest_baseline_lsn;
  manifest_registry_unlock(db);

  // Snapshot from the SNAPSHOT connection (this thread's own, spec
  // §3.1) — never the game connection: sqlite3_backup_step page I/O
  // would otherwise hold the game connection's mutex for the whole
  // copy, making game-thread writes wait on the backup thread (the
  // exact §3.3 failure mode the spec forbids). Sharing the flush
  // thread's connection would stall shipping during large snapshots.
  int status = arkilian_snapshot_copy_hook
      ? arkilian_snapshot_copy_hook(db->snapshot_db, db->backup_path,
                                    &db->shutdown_requested)
      : backup_database(db->snapshot_db, db->backup_path,
                        &db->shutdown_requested);

  // Upload path: the ONLY destination is S3-compatible object storage,
  // signed locally with the configured credentials.
  if (status == SQLITE_OK && has_direct_s3(db)) {
    // Compute sha256 of the backup file — the manifest records it so
    // hydration authenticates the object before installing it.
    char snap_sha256[65] = {0};
    (void)ark_sha256_hex_file(db->backup_path, snap_sha256);

    char s3_key[512];
    snprintf(s3_key, sizeof(s3_key), "%s/backup.sqlite", db->s3_prefix);

    char *upload_url = s3_presign_put(db, s3_key, 3600L);
    if (!upload_url) {
      ark_log(db, ARK_LOG_ERROR,
              "snapshot upload skipped: local SigV4 signing failed");
    } else if (upload_to_s3(db, upload_url, db->backup_path) != 0) {
      ark_log(db, ARK_LOG_ERROR, "scheduled backup upload failed");
      free(upload_url);
    } else {
      free(upload_url);
      ARK_STORE(&db->capture_paused, 0);
      // Baseline + prune + publish: everything up to the PRE-COPY
      // watermark is inside this snapshot, so its chunk records leave
      // the registry. Chunks registered during the copy stay.
      if (snap_sha256[0]) {
        manifest_registry_prune_upto(db, snapshot_upto);
        if (manifest_registry_upload(db, s3_key, snap_sha256,
                                     snapshot_upto) != 0) {
          // Loud, every occurrence: a failed publish means remote
          // restores silently stay on the previous manifest while
          // shipping continues. Not a reason to stop shipping — but
          // never a quiet one.
          ark_log(db, ARK_LOG_ERROR,
                  "snapshot manifest publish failed (baseline %llu) — "
                  "remote restores remain on the previous manifest until "
                  "a publish succeeds",
                  (unsigned long long)snapshot_upto);
        }
      }
    }
  }
  return status;
}

#ifdef _WIN32
DWORD WINAPI run_hourly_backup(LPVOID arg) {
#else
void *run_hourly_backup(void *arg) {
#endif
  arkilian *db = (arkilian *)arg;
  // First backup runs immediately, then every backup_interval seconds.
  // The wait is interruptible so db_close() never blocks on a sleeping
  // backup interval (previously close could hang for up to an hour).
  time_t next_backup = time(NULL);

  while (1) {
#ifndef _WIN32
    pthread_mutex_lock(&db->wake_mutex);
    while (!ARK_LOAD(&db->shutdown_requested)) {
      time_t now = time(NULL);
      if (now >= next_backup) break;
      struct timespec ts;
      ts.tv_sec = next_backup;
      ts.tv_nsec = 0;
      pthread_cond_timedwait(&db->wake_cond, &db->wake_mutex, &ts);
    }
    int shutdown = ARK_LOAD(&db->shutdown_requested);
    pthread_mutex_unlock(&db->wake_mutex);
#else
    EnterCriticalSection(&db->wake_mutex);
    while (!ARK_LOAD(&db->shutdown_requested)) {
      time_t now = time(NULL);
      if (now >= next_backup) break;
      DWORD remaining_ms = (DWORD)((next_backup - now) * 1000);
      SleepConditionVariableCS(&db->wake_cond, &db->wake_mutex, remaining_ms);
    }
    int shutdown = ARK_LOAD(&db->shutdown_requested);
    LeaveCriticalSection(&db->wake_mutex);
#endif

    if (shutdown || !db->is_open || !db->handle) break;
    next_backup = time(NULL) + db->backup_interval;

    // One snapshot attempt. The kill-switch check, heartbeat, PRE-COPY
    // watermark capture, copy, upload, and prune/publish live inside
    // arkilian_run_snapshot_cycle() so the baseline invariant is directly
    // testable (tests/test_snapshot_watermark.c drives the real cycle
    // with a copy hook that ships a chunk mid-copy).
    (void)arkilian_run_snapshot_cycle(db);
  }
#ifdef _WIN32
  return 0;
#else
  return NULL;
#endif
}
