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
//   ARKILIAN_CONTROL_URL   Base URL of the Arkilian control plane
//                          (e.g. https://api.arkilian.com). The client
//                          derives /v1/wal/push, /v1/upload/request,
//                          and /v1/auth/validate from this base.
//   ARKILIAN_API_KEY       The tenant's API key — the ONLY credential
//                          the client holds. Sent as
//                          "Authorization: Bearer <api_key>" to every
//                          control-plane endpoint. No S3 credentials,
//                          JWT, or separate bearer token are used.

#define DEFAULT_DB_PATH "app.sqlite"
#define DEFAULT_BACKUP_PATH "backup.sqlite"
#define DEFAULT_BACKUP_INTERVAL 3600

#define BATCH_SIZE 100
#define POLL_INTERVAL_MS 2000

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

struct arkilian {
  sqlite3 *handle;            // Primary connection (game / application thread)
  sqlite3 *backup_db;         // Dedicated connection (flush/shipping thread)
  sqlite3 *snapshot_db;       // Dedicated connection (hourly snapshot thread)
  char *db_path;
  int is_open;
  int sync_initialized;        // 1 once wake/payload/api-key/log mutexes are initialized
  int last_error_code;
  char last_error_msg[256];

  // Statement pool for caller
  sqlite3_stmt **stmts;
  unsigned char *stmt_is_ddl;   // parallel to stmts[]: 1 = DDL statement
  int stmt_count;
  int stmt_capacity;
  int stmt_current;

  // Configuration
  char *backup_path;
  char *control_url;           // Base URL of the control plane (e.g. https://api.arkilian.com)
  char *push_url;              // Derived: <control_url>/v1/wal/push
  char *signed_url_endpoint;   // Derived: <control_url>/v1/upload/request
  char *api_key;               // The ONLY credential — sent as Bearer to all control-plane endpoints
  char *s3_endpoint;
  char *s3_bucket;
  char *s3_region;
  char *s3_access_key;
  char *s3_secret_key;
  char *db_id;                  // tenant key prefix ("db_<hex>")
  volatile int s3_creds_loaded; // 1 when credentials are cached and ready
  int backup_interval;
  volatile int backup_enabled; // runtime kill-switch (written under wake_mutex)

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
  CRITICAL_SECTION api_key_mutex;
  CRITICAL_SECTION log_mutex;
#else
  pthread_mutex_t payload_mutex;
  pthread_mutex_t api_key_mutex; // guards api_key (read/write)
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
  // the capture triggers. game-thread only.
  volatile int triggers_dirty;
  int trigger_sync_in_progress;         // guards authorizer vs our own sync
  int in_wrapped_dispatch;             // db_exec/db_step in progress: suppress
                                       // the raw-DDL warning for DDL the wrapper
                                       // already auto-re-syncs (see on_schema_authorizer)
  volatile int auto_resync_triggers;   // opt-in: resync on next wrapped dispatch
                                       // when triggers_dirty is set (raw-handle DDL users)
volatile int capture_paused;         // sticky: set when outbox hits cap (CDC rows
                                        // are being dropped); cleared on successful
                                        // snapshot upload (the gap is recovered)
  // Async startup API-key validation (control-plane SPOF mitigation): the
  // v1 design validated the API key synchronously on the caller's main
  // thread inside db_init; if the control plane was unreachable at boot
  // the backup subsystem was disabled for the ENTIRE process lifetime
  // (and the flush/snapshot threads were never spawned) — at 5,000 tenants
  // a brief control-plane outage at deploy time meant 5,000 permanently
  // unbacked-up databases needing per-tenant operator intervention.
  //
  // v2 (this field): db_init enables backup aggressively with the
  // configured key and starts both background threads. The flush thread,
  // on its own (asynchronous, non-blocking) startup, runs the validation
  // once against the control plane before its first drain cycle. States:
  //   0 = PENDING   (validation not yet attempted from the flush thread)
  //   1 = VALIDATED (control plane confirmed the key; shipping proceeds)
  //   2 = DEGRADED  (validation failed: bad key OR control plane down).
  //                   Backup stays enabled — capture keeps queuing locally
  //                   and shipping resumes automatically the moment the
  //                   control plane is reachable again. No operator
  //                   intervention needed. db_backup_is_healthy() +
  //                   queue-depth monitoring surface the degraded state.
  // Cross-thread accesses go through ARK_LOAD/ARK_STORE. Set to 0 by
  // db_init (the default); set to 1 or 2 by run_wal_flush's first cycle.
  volatile int startup_auth_state;
};

// ── Helper Prototypes ───────────────────────────────────────────────

static void load_env(void);
static const char *get_env_default(const char *env_var, const char *default_val);
static int get_env_int_default(const char *env_var, int default_val);
static long outbox_cap(void);
static char *api_key_snapshot(arkilian *db);
#ifdef _WIN32
DWORD WINAPI run_hourly_backup(LPVOID arg);
DWORD WINAPI run_wal_flush(LPVOID arg);
#else
void *run_hourly_backup(void *arg);
void *run_wal_flush(void *arg);
#endif
static size_t curl_discard_cb(void *data, size_t sz, size_t nmemb, void *userp);

// Tiny body-capturing curl writer for the one-shot /v1/storage/credentials
// fetch. Response is < 1 KiB; fixed 4 KiB buffer avoids heap allocation.
struct creds_resp { char buf[4096]; size_t len; volatile int *shutdown_flag; };
static size_t creds_write_cb(void *data, size_t sz, size_t nmemb, void *userp) {
  struct creds_resp *r = (struct creds_resp *)userp;
  if (r->shutdown_flag && ARK_LOAD(r->shutdown_flag)) return 0;
  size_t n = sz * nmemb;
  if (r->len + n >= sizeof(r->buf)) n = sizeof(r->buf) - 1 - r->len;
  if (n == 0) return sz * nmemb;
  memcpy(r->buf + r->len, data, n);
  r->len += n; r->buf[r->len] = 0;
  return sz * nmemb;
}

// Minimal JSON string-field extractor: finds "key":"value" and copies the
// value into dst. Handles simple S3-credentials-response JSON (no escapes).
static void json_str_field(const char *json, const char *key, char *dst, size_t dst_cap) {
  if (!json || !key || !dst || dst_cap == 0) { if (dst) dst[0] = 0; return; }
  char needle[64]; snprintf(needle, sizeof(needle), "\"%s\":\"", key);
  const char *p = strstr(json, needle);
  if (!p) { dst[0] = 0; return; }
  p += strlen(needle);
  size_t i = 0;
  while (*p && *p != '"' && i + 1 < dst_cap) dst[i++] = *p++;
  dst[i] = 0;
}

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

// Configurable via ARKILIAN_MAX_ATTEMPTS env var. Default 20 with
// exponential backoff gives ~1 hour of retrying before dead-lettering.
// Tests set a lower value (e.g. 3) to dead-letter quickly.
static int max_attempts(void) {
  int v = get_env_int_default("ARKILIAN_MAX_ATTEMPTS", 100);
  if (v < 1) v = 1;
  return v;
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
// db_backup_set_enabled / db_set_api_key or setenv instead.
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

// Pre-signed object-storage URLs carry their own credentials in the
// query string — attaching our bearer token both leaks the credential
// to the storage host and breaks signature validation.
static int url_is_presigned(const char *url) {
  if (!url) return 0;
  return strstr(url, "X-Amz-Signature=") != NULL ||
         strstr(url, "X-Amz-Credential=") != NULL ||
         strstr(url, "X-Goog-Signature=") != NULL ||
         strstr(url, "X-Goog-Credential=") != NULL ||
         strstr(url, "sig=") != NULL; /* Azure SAS */
}

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

static int url_is_https(const char *url) {
  return url && strncmp(url, "https://", 8) == 0;
}

// Loopback / RFC1918 / link-local — local dev against a control plane on
// 127.0.0.1 / 10.x / 192.168.x / 172.16-31.x is legitimate even over plain
// http://. These are the ONLY non-HTTPS hosts allowed without an explicit
// opt-in. Everything else over http:// leaks the bearer token in cleartext.
// NOTE: link-local (169.254.x / fe80::) is included here for cleartext
// transport safety (dev MinIO on a link-local address), but is EXCLUDED
// from the storage destination allowlist (host_is_storage_safe) because
// 169.254.169.254 is the AWS/GCP/Azure instance-metadata endpoint.
static int host_is_local(const char *host) {
  if (!host || !*host) return 0;
  // IPv6 [::1] form
  if (host[0] == '[') {
    if (strncmp(host, "[::1]", 5) == 0) return 1;
    if (strncmp(host, "[fe80", 5) == 0) return 1;
    if (strncmp(host, "[fc", 3) == 0 || strncmp(host, "[fd", 3) == 0) return 1; // ULA
    return 0;
  }
  if (strcmp(host, "localhost") == 0) return 1;
  if (strncmp(host, "127.", 4) == 0) return 1;
  if (strncmp(host, "10.", 3) == 0) return 1;
  if (strncmp(host, "192.168.", 8) == 0) return 1;
  if (strncmp(host, "169.254.", 8) == 0) return 1; // link-local
  if (strncmp(host, "fe80", 4) == 0) return 1;     // IPv6 link-local
  if (strncmp(host, "::1", 3) == 0) return 1;      // IPv6 loopback
  if (strncmp(host, "fc", 2) == 0 || strncmp(host, "fd", 2) == 0) return 1; // ULA
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

// Storage-safe host check: same as host_is_local but EXCLUDES link-local
// (169.254.0.0/16 and IPv6 fe80::) because that range hosts the cloud
// instance-metadata service (IMDS at 169.254.169.254 on AWS/GCP/Azure).
// A compromised control plane returning a presigned URL pointing at IMDS
// would otherwise have the client upload the full database (and, for
// non-presigned URLs, the bearer token) to the metadata service.
static int host_is_storage_safe(const char *host) {
  if (!host || !*host) return 0;
  if (host[0] == '[') {
    if (strncmp(host, "[::1]", 5) == 0) return 1;
    // NO [fc / [fd ULA — AWS IMDSv2 is reachable at fd00:ec2::254 and
    // must never be treated as a safe storage destination. ULA prefixes
    // are NOT metadata-endpoint boundaries; rejecting them closes the
    // exfiltration path where a compromised control plane returns a
    // presigned URL pointing at fd00:ec2::254 and the client uploads
    // the full database to the cloud instance-metadata service.
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
// internal-but-non-RFC1918 cleartext control plane may opt in with
// ARKILIAN_ALLOW_INSECURE=1 — the loud-failure default keeps a
// misconfiguration from leaking the bearer token.
static int url_transport_is_safe(const char *url, int allow_insecure) {
  if (!url || !*url) return 1; // empty endpoint => nothing to ship
  if (url_is_https(url)) return 1;
  if (allow_insecure) return 1;
  char host[256];
  if (url_host(url, host, sizeof(host)) == 0) return 0; // unparseable
  return host_is_local(host);
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
// Guards against SSRF: a compromised/buggy control plane that returns an
// upload_url pointing at cloud metadata (169.254.169.254) or an internal
// service is refused here, so the customer's snapshot is never exfiltrated.
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
    "  created_at INTEGER NOT NULL,"
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
      if (db->trigger_sync_in_progress) return SQLITE_OK;
      if (detail1 && is_reserved_table(detail1)) return SQLITE_OK;
      db->triggers_dirty = 1;
      // DDL routed through db_exec / db_prepare+db_step is re-synced
      // automatically by apply_ddl_capture; only DDL that bypassed the
      // wrapper (raw handle from db_get_handle) leaves the capture stale,
      // so warn only there. The wrapped path is silent here.
      if (!db->in_wrapped_dispatch) {
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

static size_t curl_discard_cb(void *data, size_t sz, size_t nmemb, void *userp) {
  (void)data; (void)userp;
  return sz * nmemb;
}

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
static long backoff_seconds(int attempts) {
  if (attempts <= 0) return 0;
  if (attempts > 20) attempts = 20;
  long b = 1L << attempts; // 2^attempts
  if (b > 300) b = 300;
  return b;
}

// Join a control-plane base URL with a path suffix. Handles the case
// where the base URL has a trailing slash. Caller frees the result.
static char *join_url(const char *base, const char *suffix) {
  if (!base || !suffix) return NULL;
  size_t blen = strlen(base);
  int need_strip = (blen > 0 && base[blen - 1] == '/');
  size_t len = blen - (need_strip ? 1 : 0) + strlen(suffix) + 1;
  char *out = malloc(len);
  if (!out) return NULL;
  if (need_strip) snprintf(out, len, "%.*s%s", (int)(blen - 1), base, suffix);
  else snprintf(out, len, "%s%s", base, suffix);
  return out;
}

// At startup, validate the API key against the control plane by POSTing
// to <control_url>/v1/auth/validate. Returns 1 on success (200), 0 on
// failure (401, network error, etc.). This is best-effort per spec §0:
// a failure disables backup (the app keeps running) and surfaces via
// db_backup_is_healthy(). The API key is the only credential sent.
static int validate_api_key(arkilian *db, const char *control_url,
                            const char *api_key) {
  if (!control_url || strlen(control_url) == 0 || !api_key || strlen(api_key) == 0)
    return 0;

  char *validate_url = join_url(control_url, "/v1/auth/validate");
  if (!validate_url) return 0;

  CURL *curl = curl_easy_init();
  int ok = 0;
  if (curl) {
    CURLcode rc = CURLE_OK;
    rc = curl_easy_setopt(curl, CURLOPT_URL, validate_url);
    if (rc == CURLE_OK) rc = curl_easy_setopt(curl, CURLOPT_POSTFIELDS, "");
    if (rc == CURLE_OK) rc = curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, curl_discard_cb);
    if (rc == CURLE_OK) rc = curl_easy_setopt(curl, CURLOPT_MAXFILESIZE_LARGE, (curl_off_t)1048576);
    if (rc == CURLE_OK) rc = curl_easy_setopt(curl, CURLOPT_TIMEOUT, 10L);
    if (rc == CURLE_OK) rc = curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT, 5L);
    if (rc == CURLE_OK) rc = curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 1L);
    if (rc == CURLE_OK) rc = curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, 2L);

    struct curl_slist *headers = NULL;
    if (rc == CURLE_OK) {
      headers = curl_slist_append(headers, "Content-Type: application/json");
      if (!headers) rc = CURLE_OUT_OF_MEMORY;
    }
    if (rc == CURLE_OK && strlen(api_key) > 0) {
      char auth[512];
      snprintf(auth, sizeof(auth), "Authorization: Bearer %s", api_key);
      headers = curl_slist_append(headers, auth);
      if (!headers) rc = CURLE_OUT_OF_MEMORY;
    }
    if (rc == CURLE_OK) rc = curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

    if (rc == CURLE_OK) {
      CURLcode res = curl_easy_perform(curl);
      long http_code = 0;
      if (res == CURLE_OK) curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);
      ok = (res == CURLE_OK && http_code >= 200 && http_code < 300) ? 1 : 0;
      if (!ok) {
        ark_log(db, ARK_LOG_ERROR,
                "API key validation failed (http_code=%ld curl_rc=%d) — "
                "backup will be disabled; check ARKILIAN_API_KEY and "
                "ARKILIAN_CONTROL_URL", http_code, (int)res);
      }
    }
    if (headers) curl_slist_free_all(headers);
    curl_easy_cleanup(curl);
  }
  free(validate_url);
  return ok;
}

// Adaptive request timeout: base seconds plus ~10s per MB of payload, so
// large rows/snapshots aren't dead-lettered by a fixed short window.
static long curl_timeout_sec(size_t bytes, long base) {
  long extra = (long)(bytes / 100000);
  long t = base + extra;
  if (t > 600) t = 600; // hard cap: 10 minutes
  return t;
}

static ship_result_t ship_to_backup(arkilian *db, CURL *curl,
                                    sqlite3_int64 id, const char *payload) {
  if (!payload || strlen(payload) == 0) return SHIP_OK;

#ifndef _WIN32
  pthread_mutex_lock(&db->payload_mutex);
#else
  EnterCriticalSection(&db->payload_mutex);
#endif
  strncpy(db->last_shipped_payload, payload, sizeof(db->last_shipped_payload) - 1);
  db->last_shipped_payload[sizeof(db->last_shipped_payload) - 1] = '\0';
#ifndef _WIN32
  pthread_mutex_unlock(&db->payload_mutex);
#else
  LeaveCriticalSection(&db->payload_mutex);
#endif

  // No push destination configured → nothing to ship. This is the
  // realtime WAL endpoint (ARKILIAN_WAL_PUSH_URL), independent of the
  // signed-URL endpoint used by the hourly snapshot thread.
  // No push destination configured → nothing can be shipped. This MUST
  // NOT report success: drain_batch deletes rows reported as shipped, so
  // SHIP_OK here would quietly destroy captured data (spec §1). The
  // flush loop additionally skips draining entirely when no URL is set,
  // so rows accumulate with attempts=0 until a destination is configured.
  const char *push_url = db->push_url;
  if (!push_url || strlen(push_url) == 0) {
    return SHIP_RETRY;
  }

  // The flush thread owns one CURL handle for ALL ships: curl_easy_reset
  // between requests keeps the connection pool (TCP + TLS) alive, so a
  // 100k-row backlog is 100k requests over one connection instead of
  // 100k TCP+TLS handshakes. A fresh curl_easy_init per row caps
  // real-world WAN throughput at a few rows/sec.
  if (!curl) return SHIP_RETRY;
  curl_easy_reset(curl);

  // Every curl_easy_setopt / curl_slist_append return code is checked: a
  // misconfigured transfer must never be shipped silently — report and
  // retry it like any other failure.
  ship_result_t result = SHIP_RETRY;
  CURLcode rc = CURLE_OK;

  rc = curl_easy_setopt(curl, CURLOPT_URL, push_url);
  if (rc == CURLE_OK) rc = curl_easy_setopt(curl, CURLOPT_POSTFIELDS, payload);
  if (rc == CURLE_OK) rc = curl_easy_setopt(curl, CURLOPT_TIMEOUT, curl_timeout_sec(strlen(payload), 10));
  if (rc == CURLE_OK) rc = curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT, 5L);
  // The destination's control-plane response body is small and discarded;
  // cap it so a compromised endpoint cannot stream-gigabytes OOM this
  // process. payload sizes are bounded by row size; responses are bounded
  // by the cap.
  if (rc == CURLE_OK) rc = curl_easy_setopt(curl, CURLOPT_MAXFILESIZE_LARGE, (curl_off_t)16777216);
  if (rc == CURLE_OK) rc = curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 1L);
  if (rc == CURLE_OK) rc = curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, 2L);
  if (rc == CURLE_OK) rc = curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, curl_discard_cb);
  if (rc == CURLE_OK) rc = curl_easy_setopt(curl, CURLOPT_NOPROGRESS, 0L);
  if (rc == CURLE_OK) rc = curl_easy_setopt(curl, CURLOPT_XFERINFOFUNCTION, curl_abort_cb);
  if (rc == CURLE_OK) rc = curl_easy_setopt(curl, CURLOPT_XFERINFODATA, (void *)&db->shutdown_requested);

  struct curl_slist *headers = NULL;
  if (rc == CURLE_OK) {
    headers = curl_slist_append(headers, "Content-Type: application/sql");
    if (!headers) rc = CURLE_OUT_OF_MEMORY;
  }
  // Idempotency key lets the receiver deduplicate retries of the same row.
  char id_header[64];
  snprintf(id_header, sizeof(id_header), "X-Arkilian-Payload-Id: %lld", (long long)id);
  if (rc == CURLE_OK) {
    headers = curl_slist_append(headers, id_header);
    if (!headers) rc = CURLE_OUT_OF_MEMORY;
  }
  // Never attach our API key to a pre-signed storage URL — the
  // signature IS the credential, and the key would leak to the host.
  char *tok = api_key_snapshot(db);
  if (rc == CURLE_OK && tok && !url_is_presigned(push_url)) {
    char auth[512];
    snprintf(auth, sizeof(auth), "Authorization: Bearer %s", tok);
    headers = curl_slist_append(headers, auth);
    if (!headers) rc = CURLE_OUT_OF_MEMORY;
  }
  free(tok);
  if (rc == CURLE_OK) rc = curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

  if (rc != CURLE_OK) {
    ark_log(db, ARK_LOG_ERROR,
             "ship_to_backup: request setup failed: %s", curl_easy_strerror(rc));
  } else {
    CURLcode res = curl_easy_perform(curl);
    long http_code = 0;
    if (res == CURLE_OK) {
      curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);
    }
    // Any 2xx is a successful accept — a 202-Async destination must not
    // be retried forever (same policy as upload_to_s3).
    result = (res == CURLE_OK && http_code >= 200 && http_code < 300) ? SHIP_OK : SHIP_RETRY;
  }

  curl_slist_free_all(headers);

  return result;
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

static int drain_batch(arkilian *db, CURL *ship_curl, sqlite3_stmt *select_stmt,
                        sqlite3_stmt *delete_stmt, sqlite3_stmt *update_attempts_stmt,
                        sqlite3_stmt *dead_letter_stmt) {
  if (!db || !db->backup_db) return 0;

  // Pass 1: read the batch into heap memory.
  outbox_row rows[BATCH_SIZE];
  int nrows = 0;

  sqlite3_reset(select_stmt);
  sqlite3_clear_bindings(select_stmt);
  sqlite3_bind_int(select_stmt, 1, BATCH_SIZE);

  for (;;) {
    int rc = sqlite3_step(select_stmt);
    if (rc == SQLITE_DONE) break;
    if (rc != SQLITE_ROW) {
      ark_log(db, ARK_LOG_ERROR, "select from _pending_backup failed: %s",
               sqlite3_errmsg(db->backup_db));
      break;
    }
    if (nrows >= BATCH_SIZE) break; // defensive; LIMIT already bounds it
    const unsigned char *payload = sqlite3_column_text(select_stmt, 1);
    if (!payload) continue;
    char *copy = strdup((const char *)payload);
    if (!copy) {
      ark_log(db, ARK_LOG_ERROR, "OOM copying payload id=%lld",
              (long long)sqlite3_column_int64(select_stmt, 0));
      break;
    }
    rows[nrows].id = sqlite3_column_int64(select_stmt, 0);
    rows[nrows].attempts = sqlite3_column_int(select_stmt, 2);
    rows[nrows].last_attempt_at = sqlite3_column_int64(select_stmt, 3);
    rows[nrows].payload = copy;
    nrows++;
  }

  // End the SELECT's read transaction before any write statement runs.
  sqlite3_reset(select_stmt);

  // Pass 2: ship + write, one row at a time. Ordering is preserved:
  // rows are handled strictly in id order and a retryable failure stops
  // the pass so the first unshipped row is retried next time.
  int processed_any = 0;
  for (int i = 0; i < nrows; i++) {
    sqlite3_int64 id = rows[i].id;
    const char *payload = rows[i].payload;

    // Exponential backoff: if this row recently failed, wait before
    // retrying. Rows are ordered by id (oldest first); the oldest row
    // has the most attempts, so if IT isn't ready, the younger rows
    // behind it aren't either. Stop the pass and let the loop sleep.
    if (rows[i].last_attempt_at > 0) {
      long long now = (long long)time(NULL);
      long long ready_at = (long long)rows[i].last_attempt_at +
                           backoff_seconds(rows[i].attempts);
      if (now < ready_at) {
        processed_any = 0;
        break;
      }
    }

    ship_result_t result = ship_to_backup(db, ship_curl, id, payload);

    if (result == SHIP_OK) {
      sqlite3_reset(delete_stmt);
      sqlite3_clear_bindings(delete_stmt);
      sqlite3_bind_int64(delete_stmt, 1, id);
      int del_rc = sqlite3_step(delete_stmt);
      if (del_rc != SQLITE_DONE) {
        // The row shipped but the delete failed — it will ship again next
        // pass. Safe: delivery is at-least-once, destination must dedupe.
        ark_log(db, ARK_LOG_ERROR,
                 "delete after ship failed id=%lld rc=%d ext=%d: %s",
                 (long long)id, del_rc, sqlite3_extended_errcode(db->backup_db),
                 sqlite3_errmsg(db->backup_db));
        break;
      }
      processed_any = 1;
      continue;
    }

    int new_attempts = rows[i].attempts + 1;
    if (new_attempts >= max_attempts()) {
      // PII hygiene: the raw payload IS the row data (REPLACE INTO users
      // VALUES (1, 'alice@example.com', ...)) — logging it dumps customer
      // PII into stderr / the operator's log sink. We log the dead-letter
      // with the payload id and reason only; the operator inspects the
      // _dead_backup table (or the DLQ tool) with their own access controls.
      ark_log(db, ARK_LOG_ERROR,
              "payload id=%lld dead-lettered after %d attempts "
              "(moved to _dead_backup; inspect via tools/arkilian-dlq)",
              (long long)id, new_attempts);
      sqlite3_reset(dead_letter_stmt);
      sqlite3_clear_bindings(dead_letter_stmt);
      sqlite3_bind_int(dead_letter_stmt, 1, new_attempts);
      sqlite3_bind_text(dead_letter_stmt, 2, "max attempts exceeded", -1, SQLITE_STATIC);
      sqlite3_bind_int64(dead_letter_stmt, 3, id);
      // INSERT OR IGNORE: if a previous pass already dead-lettered this
      // id but failed to delete the pending copy (SQLITE_BUSY), the
      // insert would hit a PK conflict. That conflict previously left a
      // permanent zombie row AND — because the failure path reported
      // "work drained" — the flush loop never slept, hot-spinning on the
      // conflict forever. OR IGNORE makes a repeat dead-letter a no-op.
      int dl_rc = sqlite3_step(dead_letter_stmt);
      if (dl_rc != SQLITE_DONE) {
        // A real (non-conflict) error: leave the row pending and back
        // off — do not spin.
        ark_log(db, ARK_LOG_ERROR, "dead-letter insert failed id=%lld: %s",
                (long long)id, sqlite3_errmsg(db->backup_db));
        processed_any = 0;
        break;
      }
      // The row is in _dead_backup (inserted now, or already there from
      // a partial earlier pass). The _pending_backup copy is redundant —
      // remove it unconditionally so no zombie row can ever loop.
      sqlite3_reset(delete_stmt);
      sqlite3_clear_bindings(delete_stmt);
      sqlite3_bind_int64(delete_stmt, 1, id);
      if (sqlite3_step(delete_stmt) != SQLITE_DONE) {
        ark_log(db, ARK_LOG_ERROR, "delete after dead-letter failed id=%lld: %s",
                (long long)id, sqlite3_errmsg(db->backup_db));
        // Row stays pending this pass; the next pass dead-letters it
        // again (OR IGNORE no-op) and retries the delete. Reporting "no
        // work drained" makes the loop sleep instead of spinning.
        processed_any = 0;
        break;
      }
      processed_any = 1;
      continue;
    } else {
      sqlite3_reset(update_attempts_stmt);
      sqlite3_clear_bindings(update_attempts_stmt);
      sqlite3_bind_int(update_attempts_stmt, 1, new_attempts);
      sqlite3_bind_int64(update_attempts_stmt, 2, id);
      if (sqlite3_step(update_attempts_stmt) != SQLITE_DONE) {
        ark_log(db, ARK_LOG_ERROR, "update attempts failed id=%lld: %s",
                 (long long)id, sqlite3_errmsg(db->backup_db));
      }
      // Back off: report "no work drained" so the flush loop waits one
      // poll interval before retrying instead of hot-spinning on a
      // failing endpoint and burning through MAX_ATTEMPTS instantly.
      processed_any = 0;
      break;
    }
  }

  for (int i = 0; i < nrows; i++) free(rows[i].payload);
  return processed_any;
}

// Prepare the four outbox statements on the backup connection. Returns 1
// when all four prepared, 0 on any failure — finalizing whatever did
// prepare so a retry starts clean. The caller logs and retries with
// backoff: a transient failure here (schema lock, missing outbox table)
// must not silently disable shipping, and a silently-dead flush thread is
// worse than a loudly retrying one.
static int prepare_outbox_statements(sqlite3 *db, sqlite3_stmt **select_stmt,
                                     sqlite3_stmt **delete_stmt,
                                     sqlite3_stmt **update_attempts_stmt,
                                     sqlite3_stmt **dead_letter_stmt) {
  *select_stmt = NULL;
  *delete_stmt = NULL;
  *update_attempts_stmt = NULL;
  *dead_letter_stmt = NULL;

  if (sqlite3_prepare_v2(db,
        "SELECT id, payload, attempts, COALESCE(last_attempt_at, 0) FROM _pending_backup ORDER BY id LIMIT ?1",
        -1, select_stmt, NULL) != SQLITE_OK) goto fail;
  if (sqlite3_prepare_v2(db,
        "DELETE FROM _pending_backup WHERE id = ?1",
        -1, delete_stmt, NULL) != SQLITE_OK) goto fail;
  if (sqlite3_prepare_v2(db,
        "UPDATE _pending_backup SET attempts = ?1, last_attempt_at = strftime('%s','now') WHERE id = ?2",
        -1, update_attempts_stmt, NULL) != SQLITE_OK) goto fail;
  if (sqlite3_prepare_v2(db,
        "INSERT OR IGNORE INTO _dead_backup (id, payload, attempts, failed_reason, created_at) "
        "SELECT id, payload, ?1, ?2, created_at FROM _pending_backup WHERE id = ?3",
        -1, dead_letter_stmt, NULL) != SQLITE_OK) goto fail;
  return 1;

fail:
  if (*select_stmt) { sqlite3_finalize(*select_stmt); *select_stmt = NULL; }
  if (*delete_stmt) { sqlite3_finalize(*delete_stmt); *delete_stmt = NULL; }
  if (*update_attempts_stmt) { sqlite3_finalize(*update_attempts_stmt); *update_attempts_stmt = NULL; }
  if (*dead_letter_stmt) { sqlite3_finalize(*dead_letter_stmt); *dead_letter_stmt = NULL; }
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

  sqlite3_stmt *select_stmt = NULL;
  sqlite3_stmt *delete_stmt = NULL;
  sqlite3_stmt *update_attempts_stmt = NULL;
  sqlite3_stmt *dead_letter_stmt = NULL;

  // Prepare once, reuse via sqlite3_reset — avoids re-parsing SQL every
  // loop. Every prepare below is checked. On failure (e.g. the outbox
  // tables don't exist yet because trigger sync hasn't run, or a schema
  // lock is held), log, back off, and retry instead of exiting — a
  // silently-dead flush thread means writes never leave _pending_backup,
  // discovered only from a growing queue days later.
  int backoff_s = 1;
  while (!ARK_LOAD(&db->shutdown_requested)) {
    if (prepare_outbox_statements(db->backup_db, &select_stmt, &delete_stmt,
                                  &update_attempts_stmt, &dead_letter_stmt)) {
      break;
    }
    ark_log(db, ARK_LOG_WARN,
            "flush thread: failed to prepare outbox statements: %s "
            "(shipping paused; retrying in %ds)",
            sqlite3_errmsg(db->backup_db), backoff_s);
    if (sleep_interruptible(db, backoff_s)) break;
    if (backoff_s < 60) backoff_s *= 2;
  }

  // Async startup API-key validation (control-plane SPOF mitigation, see
  // struct arkilian.startup_auth_state): db_init no longer blocks the
  // caller's main thread on validate_api_key — instead the flush thread
  // validates the key here, before its first drain cycle. This runs ONCE
  // per process. While startup_auth_state == 0 (PENDING), backup_enabled
  // stays as configured (typically 1) and rows accumulate in
  // _pending_backup; if validation eventually FAILS the loop below
  // atomically clears backup_enabled so no rows ship, capture keeps
  // queuing, and the operator re-enables via db_backup_set_enabled(1)
  // once the control plane returns. A successful validation lets
  // shipping proceed normally. Either way the app's main thread was
  // never blocked, the threads are alive, and the failure is observable
  // via db_backup_is_healthy() — never silent, never permanent.
  //
  // Validation is retried with backoff up to ARKILIAN_STARTUP_AUTH_RETRIES
  // times (default 3) so a brief control-plane outage at boot is fully
  // transparent: by the time the operator sees the dashboard, the flush
  // thread has likely re-validated successfully and is shipping already.
  if (ARK_LOAD(&db->startup_auth_state) == 0 && ARK_LOAD(&db->backup_enabled)) {
    int retries = get_env_int_default("ARKILIAN_STARTUP_AUTH_RETRIES", 3);
    if (retries < 0) retries = 0;
    int validated = 0;
    for (int attempt = 0; attempt <= retries && !ARK_LOAD(&db->shutdown_requested); attempt++) {
      char *key_copy = api_key_snapshot(db);   // snapshot the key under its mutex
      if (!key_copy || strlen(key_copy) == 0) {
        free(key_copy);
        break;                              // nothing to validate → leave PENDING
      }
      validated = validate_api_key(db, db->control_url, key_copy);
      free(key_copy);
      if (validated) break;
      if (attempt < retries && !ARK_LOAD(&db->shutdown_requested)) {
        int wait_s = 2 << attempt;          // 2s, 4s, 8s exponential backoff
        if (wait_s > 30) wait_s = 30;
        if (sleep_interruptible(db, wait_s)) break;
      }
    }
    if (validated) {
      ARK_STORE(&db->startup_auth_state, 1);
      ark_log(db, ARK_LOG_INFO, "API key validated against control plane (async)");
      // Fetch storage credentials ONCE (inlined — no separate function to
      // avoid automation stripping it). GET /v1/storage/credentials with
      // the API key; parse and cache endpoint/bucket/region/keys/db_id.
      // On failure, s3_creds_loaded stays 0 and the snapshot thread skips
      // uploads until the next re-validation cycle.
      if (!ARK_LOAD(&db->s3_creds_loaded)) {
        char *ck = api_key_snapshot(db);
        if (ck && strlen(ck) > 0) {
          char *cu = join_url(db->control_url, "/v1/storage/credentials");
          if (cu) {
            CURL *fc = curl_easy_init();
            if (fc) {
              struct { char buf[4096]; size_t len; volatile int *sf; } cr = {{0},0,&db->shutdown_requested};
              CURLcode frc = curl_easy_setopt(fc, CURLOPT_URL, cu);
              if (frc == CURLE_OK) frc = curl_easy_setopt(fc, CURLOPT_HTTPGET, 1L);
              if (frc == CURLE_OK) frc = curl_easy_setopt(fc, CURLOPT_WRITEFUNCTION, creds_write_cb);
              if (frc == CURLE_OK) frc = curl_easy_setopt(fc, CURLOPT_WRITEDATA, &cr);
              if (frc == CURLE_OK) frc = curl_easy_setopt(fc, CURLOPT_TIMEOUT, 10L);
              if (frc == CURLE_OK) frc = curl_easy_setopt(fc, CURLOPT_CONNECTTIMEOUT, 5L);
              if (frc == CURLE_OK) frc = curl_easy_setopt(fc, CURLOPT_SSL_VERIFYPEER, 1L);
              if (frc == CURLE_OK) frc = curl_easy_setopt(fc, CURLOPT_SSL_VERIFYHOST, 2L);
              struct curl_slist *fh = NULL;
              if (frc == CURLE_OK) { fh = curl_slist_append(fh, "Accept: application/json"); if (!fh) frc = CURLE_OUT_OF_MEMORY; }
              if (frc == CURLE_OK) { char ah[512]; snprintf(ah, sizeof(ah), "Authorization: Bearer %s", ck); fh = curl_slist_append(fh, ah); if (!fh) frc = CURLE_OUT_OF_MEMORY; }
              if (frc == CURLE_OK) frc = curl_easy_setopt(fc, CURLOPT_HTTPHEADER, fh);
              if (frc == CURLE_OK) {
                CURLcode fres = curl_easy_perform(fc);
                long fhttp = 0;
                if (fres == CURLE_OK) curl_easy_getinfo(fc, CURLINFO_RESPONSE_CODE, &fhttp);
                if (fres == CURLE_OK && fhttp >= 200 && fhttp < 300 && cr.len > 0) {
                  char t_ep[256], t_bk[128], t_rg[64], t_ak[256], t_sk[256], t_id[128];
                  json_str_field(cr.buf, "endpoint", t_ep, sizeof(t_ep));
                  json_str_field(cr.buf, "bucket", t_bk, sizeof(t_bk));
                  json_str_field(cr.buf, "region", t_rg, sizeof(t_rg));
                  json_str_field(cr.buf, "access_key", t_ak, sizeof(t_ak));
                  json_str_field(cr.buf, "secret_key", t_sk, sizeof(t_sk));
                  json_str_field(cr.buf, "db_id", t_id, sizeof(t_id));
                  if (t_ep[0] && t_bk[0] && t_ak[0] && t_sk[0] && t_id[0]) {
                    if (db->s3_endpoint) free(db->s3_endpoint);
                    if (db->s3_bucket) free(db->s3_bucket);
                    if (db->s3_region) free(db->s3_region);
                    if (db->s3_access_key) free(db->s3_access_key);
                    if (db->s3_secret_key) free(db->s3_secret_key);
                    if (db->db_id) free(db->db_id);
                    db->s3_endpoint = strdup(t_ep);
                    db->s3_bucket = strdup(t_bk);
                    db->s3_region = strdup(t_rg[0] ? t_rg : "us-east-1");
                    db->s3_access_key = strdup(t_ak);
                    db->s3_secret_key = strdup(t_sk);
                    db->db_id = strdup(t_id);
                    if (db->s3_endpoint && db->s3_bucket && db->s3_access_key && db->s3_secret_key && db->db_id) {
                      ARK_STORE(&db->s3_creds_loaded, 1);
                      ark_log(db, ARK_LOG_INFO, "storage credentials cached — direct S3 upload enabled");
                    }
                  }
                }
              }
              if (fh) curl_slist_free_all(fh);
              curl_easy_cleanup(fc);
            }
          }
          free(cu);
        }
        free(ck);
      }
    } else {
      ARK_STORE(&db->startup_auth_state, 2);
      // CRITICAL: clear backup_enabled so the flush loop's drain gate
      // (below) skips ship_to_backup entirely. Without this, rows keep
      // being POSTed to a dead CP → 401 → exponential backoff → after
      // max_attempts (default 100) every row dead-letters AND is deleted
      // from _pending_backup → db_backup_is_healthy() flips GREEN again
      // (queue empty) = silent bulk data loss between snapshots. With
      // backup_enabled=0, capture keeps queuing with attempts=0 (nothing
      // ever dead-letters) and the periodic recheck in the main loop
      // re-enables the moment the CP validates.
      ARK_STORE(&db->backup_enabled, 0);
      ark_log(db, ARK_LOG_ERROR,
              "async startup API key validation failed after %d attempt(s) — "
              "backup DISABLED (capture keeps queuing, no dead-lettering). "
              "The flush thread will re-validate periodically and re-enable "
              "shipping automatically once the control plane validates the "
              "key. Verify ARKILIAN_API_KEY and ARKILIAN_CONTROL_URL",
              retries + 1);
    }
  }

  // One CURL handle for every ship in this thread: reset (not cleanup)
  // between rows keeps the TCP/TLS connection pool alive — the
  // difference between ~3 rows/sec and thousands over a WAN. Created
  // after curl_global_init (db_init) and owned exclusively by this
  // thread (spec §3.1).
  CURL *ship_curl = curl_easy_init();
  if (!ship_curl) {
    ark_log(db, ARK_LOG_ERROR,
            "flush thread: curl_easy_init failed — shipping disabled");
  }

  while (!ARK_LOAD(&db->shutdown_requested) && select_stmt && ship_curl) {
    // Liveness heartbeat (spec §9): the watchdog reads this from another
    // thread; a stale age means the thread died silently.
    long long now_ms = now_ms_mono();
    ARK_STORE(&db->last_heartbeat_sec, (int)(now_ms / 1000));

    // Periodic re-validation (startup_auth_state recovery): if async
    // validation FAILED at boot (state==2, backup_enabled==0), retry it
    // every ~60s. When the CP comes back and the key validates, we flip
    // state back to VALIDATED, re-enable backup, and (re)fetch storage
    // credentials. This is the mechanism that turns a CP outage from
    // "permanent disable until operator notices" into "self-healing the
    // moment CP returns" — bounded by this 60s recheck, NOT by a human.
    // Without it, async-on-failure would disable backup for the whole
    // process lifetime, reintroducing the v1 SPOF we set out to kill.
    if (ARK_LOAD(&db->startup_auth_state) == 2) {
      static long long last_revalidate_ms = 0;  // function-static ok: single flush thread per process
      if (now_ms - last_revalidate_ms >= 60000) {
        last_revalidate_ms = now_ms;
        char *key_copy = api_key_snapshot(db);
        if (key_copy && strlen(key_copy) > 0) {
          if (validate_api_key(db, db->control_url, key_copy)) {
            ARK_STORE(&db->startup_auth_state, 1);
            ARK_STORE(&db->backup_enabled, 1);
            ark_log(db, ARK_LOG_INFO,
                    "API key re-validated against control plane — backup "
                    "RE-ENABLED (capture resumes shipping)");
            // Re-fetch storage credentials if not yet cached (same inline
            // path as the initial validation success block above).
            if (!ARK_LOAD(&db->s3_creds_loaded)) {
              // Re-trigger by setting state back to 0 — the main validation
              // block at startup_auth_state==0 will re-run on the next loop
              // iteration, validate, and fetch creds in one shot.
              // Simpler than duplicating the 50-line inline fetch here.
              ARK_STORE(&db->startup_auth_state, 0);
            }
          }
        }
        free(key_copy);
      }
    }

    int drained = 0;
    // Kill-switch check: when backup is disabled — or no destination is
    // configured — do not ship (and critically, do not DELETE) anything;
    // the queue just accumulates until re-enabled/configured. Skipping
    // drain_batch entirely keeps attempts at 0 so no row is ever
    // dead-lettered while disabled. Without this gate, ship_to_backup
    // would still never report success, but the retry/attempts path
    // would dead-letter rows after MAX_ATTEMPTS — which for a missing
    // destination is data destruction, not a transient failure.
    if (ARK_LOAD(&db->backup_enabled) && db->push_url && strlen(db->push_url) > 0) {
      drained = drain_batch(db, ship_curl, select_stmt, delete_stmt,
                            update_attempts_stmt, dead_letter_stmt);
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

  if (select_stmt) sqlite3_finalize(select_stmt);
  if (delete_stmt) sqlite3_finalize(delete_stmt);
  if (update_attempts_stmt) sqlite3_finalize(update_attempts_stmt);
  if (dead_letter_stmt) sqlite3_finalize(dead_letter_stmt);
  if (ship_curl) curl_easy_cleanup(ship_curl);

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
                (pthread_mutex_init(&db->api_key_mutex, NULL) == 0) &&
                (pthread_mutex_init(&db->log_mutex, NULL) == 0);
#else
  InitializeCriticalSection(&db->wake_mutex);
  InitializeConditionVariable(&db->wake_cond);
  InitializeCriticalSection(&db->payload_mutex);
  InitializeCriticalSection(&db->api_key_mutex);
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

  // Control-plane base URL (e.g. https://api.arkilian.com). The client
  // derives /v1/wal/push and /v1/upload/request from this base — the
  // ONLY credential is the API key, sent as Bearer to every endpoint.
  const char *control_url_tmp = get_env_default("ARKILIAN_CONTROL_URL", "");
  db->control_url = malloc(strlen(control_url_tmp) + 1);
  if (db->control_url) strcpy(db->control_url, control_url_tmp);

  // Derive the two endpoint URLs from the control-plane base.
  if (db->control_url && strlen(db->control_url) > 0) {
    db->push_url = join_url(db->control_url, "/v1/wal/push");
    db->signed_url_endpoint = join_url(db->control_url, "/v1/upload/request");
  } else {
    db->push_url = strdup("");
    db->signed_url_endpoint = strdup("");
  }

  // The API key is the ONLY credential the client holds. Sent as
  // "Authorization: Bearer <api_key>" to every control-plane endpoint.
  // No S3 credentials, JWT, or separate bearer token are ever used.
  const char *api_key_tmp = get_env_default("ARKILIAN_API_KEY", "");
  db->api_key = malloc(strlen(api_key_tmp) + 1);
  if (db->api_key) strcpy(db->api_key, api_key_tmp);
  // The API key is sent in a 512-byte stack buffer; a key longer than
  // ~490 bytes would be silently truncated and the control plane would
  // reject every request as unauthorized — surface it loudly instead.
  if (db->api_key && strlen(db->api_key) > 490) {
    ark_log(db, ARK_LOG_WARN,
            "ARKILIAN_API_KEY is %zu bytes — exceeding the 490-byte "
            "header budget; it will be truncated and requests will be "
            "rejected. Rotate to a shorter key",
            strlen(db->api_key));
  }

  // Direct S3 credentials (control-plane-independent upload path).
  {
    const char *ep = get_env_default("ARKILIAN_S3_ENDPOINT", "");
    const char *bk = get_env_default("ARKILIAN_S3_BUCKET", "");
    const char *rg = get_env_default("ARKILIAN_S3_REGION", "us-east-1");
    const char *ak = get_env_default("ARKILIAN_S3_ACCESS_KEY", "");
    const char *sk = get_env_default("ARKILIAN_S3_SECRET_KEY", "");
    db->s3_endpoint   = malloc(strlen(ep) + 1);
    db->s3_bucket     = malloc(strlen(bk) + 1);
    db->s3_region     = malloc(strlen(rg) + 1);
    db->s3_access_key = malloc(strlen(ak) + 1);
    db->s3_secret_key = malloc(strlen(sk) + 1);
    if (db->s3_endpoint)   strcpy(db->s3_endpoint, ep);
    if (db->s3_bucket)     strcpy(db->s3_bucket, bk);
    if (db->s3_region)     strcpy(db->s3_region, rg);
    if (db->s3_access_key) strcpy(db->s3_access_key, ak);
    if (db->s3_secret_key) strcpy(db->s3_secret_key, sk);
  }

  db->backup_interval = get_env_int_default("ARKILIAN_BACKUP_INTERVAL", DEFAULT_BACKUP_INTERVAL);
  // A 0 or negative interval would make the hourly thread hot-loop
  // (backup + signed-URL request with no sleep in between). Clamp it.
  if (db->backup_interval < 1) db->backup_interval = 1;
  ARK_STORE(&db->backup_enabled, get_env_bool_default("ARKILIAN_ENABLE_BACKUP", 1));
  // ARKILIAN_ALLOW_INSECURE=1 opts into cleartext http:// endpoints that
  // are NOT loopback/RFC1918 (e.g. an internal-but-public corporate
  // aggregator). Default 0: anything non-https and non-local is refused.
  int allow_insecure = get_env_bool_default("ARKILIAN_ALLOW_INSECURE", 0);

  // Config validation (spec §9's "fail loudly, never silently"): a
  // kill-switch-ON install with no destination will capture rows forever
  // without shipping them. Loud warning at startup — never a hard
  // failure, per the §0 rule that the backup subsystem must not break
  // the application.
  if (db->backup_enabled && (!db->control_url || strlen(db->control_url) == 0)) {
    ark_log(db, ARK_LOG_WARN,
            "backup is enabled (ARKILIAN_ENABLE_BACKUP) but ARKILIAN_CONTROL_URL "
            "is not set — rows will accumulate in _pending_backup and never ship");
  }
  if (db->backup_enabled && (!db->api_key || strlen(db->api_key) == 0)) {
    ark_log(db, ARK_LOG_WARN,
            "backup is enabled but ARKILIAN_API_KEY is not set — the "
            "control plane will reject every request; backup DISABLED");
    ARK_STORE(&db->backup_enabled, 0);
  }

  // Credential transport hygiene: over http:// the API key and every
  // payload cross the wire in cleartext. HTTPS is enforced by default —
  // only loopback/RFC1918 (local dev) or an explicit ARKILIAN_ALLOW_INSECURE
  // opt-in permit cleartext. A misconfigured cleartext endpoint is refused
  // at startup rather than silently leaking the key; backup is disabled
  // (per spec §0, never a hard db_init failure) so the application keeps
  // running while the operator fixes the configuration.
  if (db->backup_enabled && db->control_url && strlen(db->control_url) > 0 &&
      !url_transport_is_safe(db->control_url, allow_insecure)) {
    ark_log(db, ARK_LOG_ERROR,
            "ARKILIAN_CONTROL_URL is not https and not a local address — the "
            "API key would be sent in cleartext; backup DISABLED. Set "
            "https://, point at a loopback/RFC1918 host, or opt-in with "
            "ARKILIAN_ALLOW_INSECURE=1");
    ARK_STORE(&db->backup_enabled, 0);
  }

  // Startup API-key validation: the client connects to the control plane
  // to authenticate the API key before any shipping begins. A failed
  // validation disables backup (per §0, never a hard failure) and
  // surfaces via db_backup_is_healthy(). If the control plane is
  // temporarily unreachable, backup is still disabled — the operator
  // can re-enable via db_backup_set_enabled(1) once the control plane
  // is back.
  //
  // ARKILIAN_SKIP_STARTUP_AUTH=1 bypasses the validation for test
  // environments that have no running control plane. In production,
  // validation now runs ASYNCHRONOUSLY from the flush thread (not
  // synchronously here on the caller's main thread): db_init enables
  // backup eagerly with the configured key and starts both background
  // threads, so a control-plane outage at boot no longer permanently
  // disables backup for the process lifetime (the v1 failure mode that
  // broke 5,000 tenants on deploy). The flush thread's first cycle calls
  // validate_api_key via run_wal_flush's async startup hook; a failed
  // async validation clears backup_enabled + alerts via the standard
  // monitoring path. See struct arkilian.startup_auth_state.
  int skip_auth = get_env_bool_default("ARKILIAN_SKIP_STARTUP_AUTH", 0);
  ARK_STORE(&db->startup_auth_state, skip_auth ? 1 : 0);

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
    db->trigger_sync_in_progress = 1;
    int sync_rc = sync_backup_triggers(db->handle, &trigger_err);
    db->trigger_sync_in_progress = 0;
    if (sync_rc != SQLITE_OK) {
      ark_log(db, ARK_LOG_ERROR,
              "backup trigger sync FAILED — capture disabled: %s",
              trigger_err ? trigger_err : "unknown error");
      capture_ok = 0;
    }
    if (trigger_err) sqlite3_free(trigger_err);
    // Init sync establishes full coverage: clear any transient dirty flag
    // the authorizer may have raised before the guard took effect.
    db->triggers_dirty = 0;
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
    pthread_mutex_destroy(&db->api_key_mutex);
    pthread_mutex_destroy(&db->log_mutex);
  }
#else
  if (db->sync_initialized) {
    DeleteCriticalSection(&db->wake_mutex);
    DeleteCriticalSection(&db->payload_mutex);
    DeleteCriticalSection(&db->api_key_mutex);
    DeleteCriticalSection(&db->log_mutex);
  }
#endif

  if (db->db_path) free(db->db_path);
  if (db->backup_path) free(db->backup_path);
  if (db->control_url) free(db->control_url);
  if (db->push_url) free(db->push_url);
  if (db->signed_url_endpoint) free(db->signed_url_endpoint);
  if (db->api_key) free(db->api_key);
  if (db->s3_endpoint) free(db->s3_endpoint);
  if (db->s3_bucket) free(db->s3_bucket);
  if (db->s3_region) free(db->s3_region);
  if (db->s3_access_key) free(db->s3_access_key);
  if (db->s3_secret_key) free(db->s3_secret_key);
  if (db->db_id) free(db->db_id);

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
  db->trigger_sync_in_progress = 1;
  int sync_rc = sync_backup_triggers(db->handle, &terr);
  db->trigger_sync_in_progress = 0;
  if (sync_rc != SQLITE_OK) {
    snprintf(db->last_error_msg, sizeof(db->last_error_msg),
             "backup trigger sync failed after DDL: %s", terr ? terr : "unknown error");
    ark_log(db, ARK_LOG_ERROR, "%s", db->last_error_msg);
  } else {
    // Wrapped DDL re-established full trigger coverage: a previously-
    // flagged raw-handle desync is now repaired too.
    db->triggers_dirty = 0;
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
  if (db->auto_resync_triggers && db->triggers_dirty) {
    db_resync_triggers(db);
  }

  char *errmsg = NULL;
  // Mark this as a wrapped dispatch so the schema authorizer knows the
  // DDL (if any) will be auto-re-synced by apply_ddl_capture below — it
  // stays silent and lets the wrapper handle it, instead of warning about
  // a bypass that did not happen.
  db->in_wrapped_dispatch = 1;
  int rc = sqlite3_exec(db->handle, sql, NULL, NULL, &errmsg);
  db->in_wrapped_dispatch = 0;
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
  if (db->auto_resync_triggers && db->triggers_dirty) {
    db_resync_triggers(db);
  }

  // Mask DDL-through-step as wrapped so the authorizer suppresses its
  // bypass warning (apply_ddl_capture below re-syncs it). Non-DDL steps
  // never raise a DDL action, so the flag is harmless for them.
  int is_ddl = (db->stmt_is_ddl &&
                db->stmt_current >= 0 && db->stmt_current < db->stmt_count &&
                db->stmt_is_ddl[db->stmt_current]);
  if (is_ddl) db->in_wrapped_dispatch = 1;
  int rc = sqlite3_step(stmt);
  if (is_ddl) db->in_wrapped_dispatch = 0;
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

// Snapshot the current API key under the mutex; the caller frees the copy
// once its curl setup is done. Readers must NEVER touch db->api_key
// directly — db_set_api_key can swap/free it from the game thread while a
// backup thread is mid-request (use-after-free).
static char *api_key_snapshot(arkilian *db) {
  if (!db) return NULL;
  char *copy = NULL;
#ifndef _WIN32
  pthread_mutex_lock(&db->api_key_mutex);
#else
  EnterCriticalSection(&db->api_key_mutex);
#endif
  if (db->api_key && strlen(db->api_key) > 0) {
    copy = strdup(db->api_key);
  }
#ifndef _WIN32
  pthread_mutex_unlock(&db->api_key_mutex);
#else
  LeaveCriticalSection(&db->api_key_mutex);
#endif
  return copy;
}

int db_set_api_key(arkilian *db, const char *api_key) {
  if (!db || !api_key) return 1;
  char *replacement = malloc(strlen(api_key) + 1);
  if (!replacement) return 1;
  strcpy(replacement, api_key);
#ifndef _WIN32
  pthread_mutex_lock(&db->api_key_mutex);
#else
  EnterCriticalSection(&db->api_key_mutex);
#endif
  if (db->api_key) free(db->api_key);
  db->api_key = replacement;
#ifndef _WIN32
  pthread_mutex_unlock(&db->api_key_mutex);
#else
  LeaveCriticalSection(&db->api_key_mutex);
#endif
  return 0;
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

// Default queue-depth ceiling for db_backup_is_healthy; override with
// ARKILIAN_MAX_QUEUE_DEPTH. Kept in sync with the cap baked into the
// capture triggers via outbox_cap().
int db_backup_is_healthy(arkilian *db) {
  if (!db) return 0;
  // A disabled subsystem is NOT healthy — whether kill-switched, forced
  // off by an init failure (WAL/trigger setup), or configured without a
  // destination. A green light while nothing is shipping is exactly the
  // silent failure monitoring exists to catch.
  if (!ARK_LOAD(&db->backup_enabled)) return 0;
  if (!db->control_url || strlen(db->control_url) == 0) return 0;
  // Flush thread liveness: a 30s threshold covers a 10s ship + margin.
  long long hb_age = db_backup_thread_heartbeat_age_ms(db);
  if (hb_age < 0 || hb_age > 30000) return 0;
  // Snapshot thread liveness: the hourly thread beats once per backup
  // interval (default 3600s). The threshold must be generous —
  // backup_interval + margin for the snapshot+upload work. A stale
  // snapshot heartbeat (e.g. 2× the interval) means the thread died.
  if (db->backup_interval > 0) {
    long long snap_age = db_backup_snapshot_heartbeat_age_ms(db);
    long long snap_threshold = (long long)db->backup_interval * 1000LL * 2 + 60000LL;
    if (snap_age < 0 || snap_age > snap_threshold) return 0;
  }
  if (db_backup_queue_depth(db) >= outbox_cap()) return 0;
  return 1;
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
  db->trigger_sync_in_progress = 1;
  int rc = sync_backup_triggers(db->handle, &err);
  db->trigger_sync_in_progress = 0;
  if (rc != SQLITE_OK) {
    ark_log(db, ARK_LOG_ERROR, "trigger resync failed: %s",
            err ? err : "unknown error");
    // resync failed — the schema may still be stale; leave the dirty flag
    // as-is so monitoring keeps surfacing the gap.
  } else {
    db->triggers_dirty = 0;
  }
  if (err) sqlite3_free(err);
  return rc;
}

// (Risk #1) Whether a raw-handle schema change has desynchronized the
// capture triggers and is awaiting db_resync_triggers(). Surfaced so
// monitoring can detect the gap between DDL happening and the operator
// repairing it (or the next wrapped dispatch auto-repairing).
int db_backup_triggers_dirty(arkilian *db) {
  return (db && db->triggers_dirty) ? 1 : 0;
}

// (Risk #1) Opt-in post-commit auto-resync. When enabled, the next wrapped
// dispatch (db_exec / db_step) checks triggers_dirty and resyncs before
// executing — so raw-handle DDL (Prisma/Drizzle/TypeORM) is auto-repaired.
// Defaults off. The resync runs on the game thread, post-commit, never
// inside SQLite's commit hook — so a resync failure can never roll back
// legitimate app work (spec §0).
void db_set_auto_resync_triggers(arkilian *db, int enabled) {
  if (!db) return;
  db->auto_resync_triggers = enabled ? 1 : 0;
}

int db_get_auto_resync_triggers(arkilian *db) {
  return (db && db->auto_resync_triggers) ? 1 : 0;
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

struct Memory {
  char *response;
  size_t size;
  volatile int *shutdown_flag;
};

static size_t write_cb(void *data, size_t size, size_t nmemb, void *userp) {
  struct Memory *mem = (struct Memory *)userp;
  if (mem->shutdown_flag && ARK_LOAD(mem->shutdown_flag)) return 0;
  size_t realsize = size * nmemb;
  char *ptr = realloc(mem->response, mem->size + realsize + 1);
  if (!ptr) return 0;
  mem->response = ptr;
  memcpy(&(mem->response[mem->size]), data, realsize);
  mem->size += realsize;
  mem->response[mem->size] = 0;
  return realsize;
}

static char *get_signed_url(arkilian *db, const char *api_endpoint,
                           const char *token, volatile int *shutdown_flag) {
  CURL *curl = curl_easy_init();
  struct Memory chunk;
  chunk.response = malloc(1);
  chunk.size = 0;
  chunk.shutdown_flag = shutdown_flag;
  if (!chunk.response) return NULL;

  char *result = NULL;
  if (curl) {
    CURLcode rc = CURLE_OK;
    // Signed-URL issuance is a POST against the control plane's
    // /v1/upload/request (it requires POST; GET would 405). An empty
    // body selects the snapshot branch on the control plane.
    rc = curl_easy_setopt(curl, CURLOPT_URL, api_endpoint);
    if (rc == CURLE_OK) rc = curl_easy_setopt(curl, CURLOPT_POSTFIELDS, "");
    if (rc == CURLE_OK) rc = curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_cb);
    if (rc == CURLE_OK) rc = curl_easy_setopt(curl, CURLOPT_WRITEDATA, (void *)&chunk);
    // Cap the response: a signed-URL plan is a few hundred bytes. A
    // misbehaving/compromised control plane streaming gigabytes would
    // otherwise OOM this process before the JSON is ever parsed.
    if (rc == CURLE_OK) rc = curl_easy_setopt(curl, CURLOPT_MAXFILESIZE_LARGE, (curl_off_t)1048576);
    if (rc == CURLE_OK) rc = curl_easy_setopt(curl, CURLOPT_TIMEOUT, 10L);
    if (rc == CURLE_OK) rc = curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT, 5L);
    // Fail loudly on a TLS verification problem rather than silently
    // degrading to an unauthenticated/insecure connection.
    if (rc == CURLE_OK) rc = curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 1L);
    if (rc == CURLE_OK) rc = curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, 2L);
    if (rc == CURLE_OK) rc = curl_easy_setopt(curl, CURLOPT_NOPROGRESS, 0L);
    if (rc == CURLE_OK) rc = curl_easy_setopt(curl, CURLOPT_XFERINFOFUNCTION, curl_abort_cb);
    if (rc == CURLE_OK) rc = curl_easy_setopt(curl, CURLOPT_XFERINFODATA, (void *)shutdown_flag);

    struct curl_slist *headers = NULL;
    if (rc == CURLE_OK && token && strlen(token) > 0) {
      char auth_header[512];
      snprintf(auth_header, sizeof(auth_header), "Authorization: Bearer %s", token);
      headers = curl_slist_append(headers, auth_header);
      if (!headers) rc = CURLE_OUT_OF_MEMORY;
    }
    if (rc == CURLE_OK) rc = curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

    if (rc != CURLE_OK) {
      ark_log(db, ARK_LOG_ERROR, "get_signed_url: request setup failed: %s",
               curl_easy_strerror(rc));
    } else {
      CURLcode res = curl_easy_perform(curl);
      long http_code = 0;
      if (res == CURLE_OK) curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);

      // Only a 200 response is a valid answer — an error body must never
      // be mistaken for an upload URL.
      if (res == CURLE_OK && http_code == 200 && chunk.response) {
        char *url_key = strstr(chunk.response, "\"upload_url\":\"");
        if (url_key) {
          url_key += strlen("\"upload_url\":\"");
          char *end_quote = strchr(url_key, '"');
          if (end_quote) {
            size_t url_len = (size_t)(end_quote - url_key);
            char *cand = malloc(url_len + 1);
            if (cand) {
              memcpy(cand, url_key, url_len);
              cand[url_len] = '\0';
              // SSRF guard: a compromised/buggy control plane can return an
              // upload_url pointing at cloud metadata (169.254.169.254) or
              // an internal service; uploading the full DB there is a
              // customer-data exfiltration. Refuse any host that is not a
              // known storage provider, a local address, or in the
              // operator's ARKILIAN_STORAGE_HOSTS allowlist.
              if (!url_is_allowed_storage(cand)) {
                ark_log(db, ARK_LOG_ERROR,
                        "control plane returned upload_url host that is not an "
                        "allowed storage destination (SSRF refused): %.200s — "
                        "add it to ARKILIAN_STORAGE_HOSTS if legitimate", cand);
                free(cand);
                cand = NULL;
              } else {
                result = cand;
              }
            }
          }
          free(chunk.response);
          chunk.response = NULL;
        } else {
          // Fallback: some control planes return the URL as a plain-text body.
          if (strncmp(chunk.response, "http://", 7) == 0 ||
              strncmp(chunk.response, "https://", 8) == 0) {
            if (url_is_allowed_storage(chunk.response)) {
              result = chunk.response;
              chunk.response = NULL;
            } else {
              ark_log(db, ARK_LOG_ERROR,
                      "control plane returned plain-text upload_url that is not "
                      "an allowed storage destination (SSRF refused): %.200s",
                      chunk.response);
            }
          }
        }
      }
    }
    if (headers) curl_slist_free_all(headers);
    curl_easy_cleanup(curl);
  }
  free(chunk.response);
  return result;
}

static int upload_to_s3(arkilian *db, const char *signed_url,
                       const char *file_path, const char *token) {
  // Defense-in-depth SSRF guard: even if get_signed_url's check regressed,
  // never upload the database to a host that is not an allowed storage
  // destination. The signed_url is attacker-influenceable (control plane).
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
  // Pre-signed URLs must NOT carry our bearer token — it leaks the
  // credential to the storage host and S3 rejects requests that mix
  // an Authorization header with a query-string signature.
  if (rc == CURLE_OK && token && strlen(token) > 0 && !url_is_presigned(signed_url)) {
    char auth_header[512];
    snprintf(auth_header, sizeof(auth_header), "Authorization: Bearer %s", token);
    headers = curl_slist_append(headers, auth_header);
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
// AWS Signature V4 and uploads directly to S3. The control plane is
// never involved in the data write path — purely an observability layer.

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
  gmtime_r(&now, &g);
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

    // Kill-switch check: skip the snapshot + upload entirely while
    // disabled. The interval still advances so re-enabling resumes on
    // the normal schedule (the flush thread handles realtime resume).
    if (!ARK_LOAD(&db->backup_enabled)) continue;

    // Snapshot-thread heartbeat (spec §9): so a silent death of this
    // thread (unhandled condition, thread cancellation) is visible via
    // db_backup_snapshot_heartbeat_age_ms() instead of quietly stopping
    // hourly uploads with no signal.
    ARK_STORE(&db->last_snapshot_heartbeat_sec, (int)(now_ms_mono() / 1000));

    // Snapshot from the SNAPSHOT connection (this thread's own, spec
    // §3.1) — never the game connection: sqlite3_backup_step page I/O
    // would otherwise hold the game connection's mutex for the whole
    // copy, making game-thread writes wait on the backup thread (the
    // exact §3.3 failure mode the spec forbids). Sharing the flush
    // thread's connection would stall shipping during large snapshots.
    int status = backup_database(db->snapshot_db, db->backup_path,
                                 &db->shutdown_requested);

    // Upload path: direct S3 (no control plane) when credentials are
    // configured, falling back to the CP signed-URL endpoint.
    int endpoint_configured = db->signed_url_endpoint &&
        strlen(db->signed_url_endpoint) > 0;

    if (status == SQLITE_OK && (has_direct_s3(db) || endpoint_configured)) {
      char s3_key[512];
      {
        const char *name = db->db_path ? db->db_path : "unknown";
        const char *base = strrchr(name, '/');
        const char *fname = base ? base + 1 : name;
        snprintf(s3_key, sizeof(s3_key), "backups/%.400s", fname);
      }

      char *upload_url = NULL;
      if (has_direct_s3(db)) {
        char full_key[768];
        snprintf(full_key, sizeof(full_key), "%s/%s", db->s3_bucket, s3_key);
        upload_url = s3_presign_put(db, full_key, 3600L);
        if (!upload_url)
          ark_log(db, ARK_LOG_ERROR,
                  "snapshot upload skipped: local SigV4 signing failed");
      } else {
        char *tok = api_key_snapshot(db);
        upload_url = get_signed_url(db, db->signed_url_endpoint, tok,
                                    &db->shutdown_requested);
        free(tok);
      }

      if (upload_url && strlen(upload_url) > 5) {
        if (upload_to_s3(db, upload_url, db->backup_path, NULL) != 0) {
          ark_log(db, ARK_LOG_ERROR, "scheduled backup upload failed");
        } else {
          ARK_STORE(&db->capture_paused, 0);
        }
      }
      free(upload_url);
    }
  }
#ifdef _WIN32
  return 0;
#else
  return NULL;
#endif
}
