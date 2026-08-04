#ifndef ARKILIAN_H
#define ARKILIAN_H

#include "deps/sqlite/sqlite3.h"
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

// Public C API for Arkilian Managed SQLite Database Engine
//
// Thread-safety: the backup subsystem is fully thread-safe by design.
// The statement cursor (db_prepare/db_step/db_bind_*/db_column_*) is a
// single per-handle cursor: C callers using the cursor from multiple
// threads must serialize those calls themselves. The N-API bindings
// (src/arkilian.cc) do this automatically with a per-handle mutex, so
// Node.js worker_threads/Bun workers are safe. db_close must not race
// with any in-flight statement on the same handle (UB by contract).
typedef struct arkilian arkilian;

// Open a database and start the background backup subsystem.
//
// Returns 0 on success. On failure returns non-zero AND, when the
// failure occurred after the struct was allocated, still sets *db to a
// partially-initialized handle the caller MUST release with db_close()
// (to free the connections/mutexes/config that were set up before the
// failing step). A caller that does `if (db_init(&db, ...) != 0) return;`
// without closing leaks those resources. The Node binding honors this;
// C callers must too. Check the return code AND free *db on failure.
//
// `connection_url` is the on-disk database path (or NULL to use
// ARKILIAN_DB_PATH / the default). Backup is auto-disabled (not a hard
// failure) when WAL/trigger setup fails, the configured push endpoint is
// not HTTPS and not a local address, or the bearer token is malformed —
// the application keeps running; db_backup_is_healthy() surfaces the gap.
int db_init(arkilian **db, const char *connection_url);
void db_close(arkilian *db);
const char* db_errmsg(arkilian *db);
sqlite3* db_get_handle(arkilian *db);
int db_set_token(arkilian *db, const char *token);

int db_exec(arkilian *db, const char *sql);
int db_begin(arkilian *db);
int db_commit(arkilian *db);
int db_rollback(arkilian *db);
int db_wal_pending(arkilian *db);
void db_wal_flush(arkilian *db);

// Runtime kill-switch (spec §1): disable/enable all outbound backup
// activity without a restart. Capture keeps running while disabled —
// rows accumulate in _pending_backup (nothing is deleted, attempts stay
// 0) and shipping resumes exactly where it left off on re-enable.
void db_backup_set_enabled(arkilian *db, int enabled);
int db_backup_is_enabled(arkilian *db);

// Statement management — multiple statements can coexist.
int db_prepare(arkilian *db, const char *sql);
int db_use_stmt(arkilian *db, int index);
int db_stmt_count(arkilian *db);

int db_step(arkilian *db);
int db_finalize(arkilian *db);
int db_reset(arkilian *db);
int db_column_count(arkilian *db);
const char* db_column_name(arkilian *db, int col);
const char* db_column_text(arkilian *db, int col);
int db_column_int(arkilian *db, int col);
double db_column_double(arkilian *db, int col);
int db_bind_text(arkilian *db, int idx, const char *val);
int db_bind_int(arkilian *db, int idx, int val);
int db_bind_int64(arkilian *db, int idx, sqlite3_int64 val);
int db_bind_double(arkilian *db, int idx, double val);
int db_bind_null(arkilian *db, int idx);

int db_column_type(arkilian *db, int col);
sqlite3_int64 db_column_int64(arkilian *db, int col);
const void* db_column_blob(arkilian *db, int col);
int db_column_bytes(arkilian *db, int col);

int db_changes(arkilian *db);
sqlite3_int64 db_last_insert_rowid(arkilian *db);
const char* db_wal_last_sql(arkilian *db);

// Auto-generate SQL backup triggers for live tables
int sync_backup_triggers(sqlite3 *db, char **err_out);

// Re-run trigger generation on the live connection (call after external
// schema migrations that bypass db_exec). Returns SQLITE_OK on success.
int db_resync_triggers(arkilian *db);

// ── Structured logging ──────────────────────────────────────────────
// By default all diagnostics go to stderr. Applications can install a
// callback (e.g. to route into their own logger / JSON formatter); the
// callback is invoked from the thread that produced the message.

typedef enum {
  ARK_LOG_ERROR = 0,
  ARK_LOG_WARN  = 1,
  ARK_LOG_INFO  = 2,
  ARK_LOG_DEBUG = 3
} ark_log_level_t;

typedef void (*ark_log_fn_t)(ark_log_level_t level, const char *msg, void *ctx);

void db_set_log_callback(arkilian *db, ark_log_fn_t fn, void *ctx);

// Global sink used for messages that fire before a handle exists (e.g.
// configuration warnings inside db_init). Per-handle callbacks take
// precedence once set.
void db_set_default_log_callback(ark_log_fn_t fn, void *ctx);

// ── Monitoring & health (spec §9) ───────────────────────────────────
// Queue depth — rows in _pending_backup not yet delivered.
int db_backup_queue_depth(arkilian *db);
// Oldest pending row age in seconds — the realtime-lag metric. 0 when
// the queue is empty.
long long db_backup_oldest_pending_age_sec(arkilian *db);
// Rows dead-lettered after MAX_ATTEMPTS — every one needs investigation
// and replay (see tools/arkilian-dlq).
int db_backup_dead_letter_count(arkilian *db);
// Milliseconds since the flush thread's last heartbeat; -1 if the thread
// never beat (not running). An age far above the poll interval means the
// thread died silently.
long long db_backup_thread_heartbeat_age_ms(arkilian *db);
// Trigger coverage sanity check (spec §9): 0 = every PK-capable table
// has its 3 capture triggers, N>0 = N triggers missing.
int db_backup_trigger_coverage(arkilian *db);
// Tables that are NOT captured: real tables with no PRIMARY KEY
// (row-level replication cannot be replayed for them, so they are
// skipped loudly at sync time). Must be 0 in a fully-backed-up schema —
// every skipped table is data that never leaves the box.
int db_backup_skipped_table_count(arkilian *db);
// 1 when the backup subsystem is healthy: backup enabled, a push
// destination configured, flush thread alive, and queue depth below
// ARKILIAN_MAX_QUEUE_DEPTH (default 100000). 0 otherwise — including
// when the kill-switch is on or capture was disabled at init: a green
// light while nothing ships is a silent failure.
int db_backup_is_healthy(arkilian *db);

#ifdef __cplusplus
}
#endif

#endif // ARKILIAN_H
