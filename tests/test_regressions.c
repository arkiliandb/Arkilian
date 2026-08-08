// Arkilian Regression Tests — production audit fixes
//
// Covers:
//   - WITHOUT ROWID tables: DELETE triggers fire correctly (PK-based payload)
//   - Keyword/special table names: trigger sync no longer silently dies
//   - Leading-whitespace DDL: triggers still get synced
//   - DDL inside a batch transaction: triggers synced in ambient txn
//   - db_close() never blocks on a sleeping backup interval
//   - db_prepare("") rejected (no ghost statement slots)
//   - int64 binding round-trip (no 32-bit truncation)
//   - BLOB column accessors
//   - db_wal_last_sql is per-instance (no cross-instance leakage)
//
// Compile (macOS/Linux):
//   cc tests/test_regressions.c src/class.c src/deps/sqlite/sqlite3.c -Isrc -Isrc/deps/sqlite -lcurl -lpthread -o test_regressions

#include "class.h"
#include "ark_test_env.h"
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

static int tests_run = 0;
static int tests_passed = 0;

#define RUN_TEST(fn)                                                           \
  do {                                                                         \
    tests_run++;                                                                \
    printf("  [%02d] %-52s ", tests_run, #fn);                                 \
    fn();                                                                       \
    tests_passed++;                                                             \
    printf("PASS\n");                                                           \
  } while (0)

static double now_ms(void) {
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return (double)ts.tv_sec * 1000.0 + (double)ts.tv_nsec / 1000000.0;
}

static void cleanup(const char *path) {
  remove(path);
  char side[256];
  snprintf(side, sizeof(side), "%s-wal", path); remove(side);
  snprintf(side, sizeof(side), "%s-shm", path); remove(side);
  snprintf(side, sizeof(side), "%s-journal", path); remove(side);
}

// Failing push endpoint keeps payloads in _pending_backup for inspection.
static arkilian *open_db(const char *path) {
  cleanup(path); // idempotent across re-runs
  ark_setenv("ARKILIAN_ENABLE_BACKUP", "0", 1);
  ark_setenv("ARKILIAN_API_KEY", "test-key", 1);
  ark_setenv("ARKILIAN_SKIP_STARTUP_AUTH", "1", 1);
  ark_setenv("ARKILIAN_CONTROL_URL", "http://127.0.0.1:1", 1);
  arkilian *db = NULL;
  int rc = db_init(&db, path);
  assert(rc == 0 && "db_init failed");
  assert(db != NULL);
  return db;
}

// Count payloads in _pending_backup matching a LIKE pattern. -1 on error.
static int count_payloads(arkilian *db, const char *like_pattern) {
  char sql[512];
  snprintf(sql, sizeof(sql),
           "SELECT COUNT(*) FROM _pending_backup WHERE payload LIKE '%s'",
           like_pattern);
  if (db_prepare(db, sql) != SQLITE_OK) return -1;
  int count = -1;
  if (db_step(db) == SQLITE_ROW) count = db_column_int(db, 0);
  db_finalize(db);
  return count;
}

// ── WITHOUT ROWID ───────────────────────────────────────────────────

static void test_without_rowid_delete_works(void) {
  arkilian *db = open_db("test_reg_worowid.db");
  int rc = db_exec(db, "CREATE TABLE wr (id TEXT PRIMARY KEY, v TEXT) WITHOUT ROWID");
  assert(rc == SQLITE_OK);

  rc = db_exec(db, "INSERT INTO wr (id, v) VALUES ('k1', 'hello')");
  assert(rc == SQLITE_OK);

  // Previously this FAILED at trigger fire time: "no such column: OLD.rowid"
  rc = db_exec(db, "DELETE FROM wr WHERE id = 'k1'");
  assert(rc == SQLITE_OK);

  // Row is actually gone
  db_prepare(db, "SELECT COUNT(*) FROM wr");
  assert(db_step(db) == SQLITE_ROW);
  assert(db_column_int(db, 0) == 0);
  db_finalize(db);

  // DELETE payload captured, keyed on the PRIMARY KEY, not rowid
  assert(count_payloads(db, "DELETE FROM \"wr\" WHERE \"id\" = %") >= 1);
  assert(count_payloads(db, "DELETE FROM \"wr\" WHERE rowid%") == 0);

  db_close(db);
  cleanup("test_reg_worowid.db");
}

static void test_rowid_table_delete_payload_is_pk_keyed(void) {
  arkilian *db = open_db("test_reg_rowid.db");
  db_exec(db, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)");
  db_exec(db, "INSERT INTO t (v) VALUES ('x')");
  assert(db_exec(db, "DELETE FROM t WHERE id = 1") == SQLITE_OK);
  // PK-keyed, never rowid-keyed: REPLACE shifts destination rowids after
  // any UPDATE, so rowid-keyed deletes would remove the wrong row.
  assert(count_payloads(db, "DELETE FROM \"t\" WHERE \"id\" = %") >= 1);
  assert(count_payloads(db, "DELETE FROM \"t\" WHERE rowid%") == 0);
  db_close(db);
  cleanup("test_reg_rowid.db");
}

// ── Keyword / special table names ───────────────────────────────────

static void test_keyword_table_name_gets_triggers(void) {
  arkilian *db = open_db("test_reg_keyword.db");
  // Previously the unquoted %w made sync fail and ROLLBACK the whole
  // backup infrastructure — _pending_backup wouldn't even exist.
  int rc = db_exec(db, "CREATE TABLE \"order\" (id INTEGER PRIMARY KEY, item TEXT)");
  assert(rc == SQLITE_OK);

  rc = db_exec(db, "INSERT INTO \"order\" (item) VALUES ('widget')");
  assert(rc == SQLITE_OK);

  int n = count_payloads(db, "REPLACE INTO \"order\" (%");
  assert(n >= 1);

  // And the captured payload is replayable SQL
  const char *payload = db_wal_last_sql(db);
  assert(payload != NULL && strstr(payload, "REPLACE INTO \"order\"") != NULL);

  db_close(db);
  cleanup("test_reg_keyword.db");
}

static void test_table_name_with_spaces(void) {
  arkilian *db = open_db("test_reg_spaces.db");
  assert(db_exec(db, "CREATE TABLE \"my table\" (id INTEGER PRIMARY KEY)") == SQLITE_OK);
  assert(db_exec(db, "INSERT INTO \"my table\" (id) VALUES (1)") == SQLITE_OK);
  assert(count_payloads(db, "REPLACE INTO \"my table\"%") >= 1);
  db_close(db);
  cleanup("test_reg_spaces.db");
}

static void test_generated_columns_excluded_from_payloads(void) {
  arkilian *db = open_db("test_reg_gen.db");
  assert(db_exec(db, "CREATE TABLE g (id INTEGER PRIMARY KEY, a INT, "
                     "b INT GENERATED ALWAYS AS (a * 2) VIRTUAL)") == SQLITE_OK);
  assert(db_exec(db, "INSERT INTO g (a) VALUES (21)") == SQLITE_OK);

  // Payload must list only real columns — a generated column in a
  // REPLACE INTO column list fails on the replay side.
  assert(count_payloads(db, "REPLACE INTO \"g\" (\"id\", \"a\")%") >= 1);
  assert(count_payloads(db, "REPLACE INTO \"g\" (\"id\", \"a\", \"b\")%") == 0);

  // And the captured payload must actually replay cleanly.
  const char *payload = db_wal_last_sql(db);
  assert(payload != NULL && strstr(payload, "REPLACE INTO \"g\"") != NULL);
  assert(db_exec(db, payload) == SQLITE_OK);

  // Generated values still correct locally
  db_prepare(db, "SELECT b FROM g WHERE id = 1");
  assert(db_step(db) == SQLITE_ROW);
  assert(db_column_int(db, 0) == 42);
  db_finalize(db);

  db_close(db);
  cleanup("test_reg_gen.db");
}

// ── DDL detection ───────────────────────────────────────────────────

static void test_leading_whitespace_ddl_syncs_triggers(void) {
  arkilian *db = open_db("test_reg_ws.db");
  // Leading whitespace used to bypass the CREATE detection entirely.
  assert(db_exec(db, "   CREATE TABLE ws_t (id INTEGER PRIMARY KEY, v TEXT)") == SQLITE_OK);
  assert(db_exec(db, "INSERT INTO ws_t (v) VALUES ('x')") == SQLITE_OK);
  assert(count_payloads(db, "REPLACE INTO \"ws_t\"%") >= 1);
  db_close(db);
  cleanup("test_reg_ws.db");
}

static void test_leading_comment_ddl_syncs_triggers(void) {
  arkilian *db = open_db("test_reg_comment.db");
  assert(db_exec(db, "-- migration 42\nCREATE TABLE c_t (id INTEGER PRIMARY KEY)") == SQLITE_OK);
  assert(db_exec(db, "INSERT INTO c_t (id) VALUES (1)") == SQLITE_OK);
  assert(count_payloads(db, "REPLACE INTO \"c_t\"%") >= 1);
  db_close(db);
  cleanup("test_reg_comment.db");
}

static void test_ddl_inside_batch_txn_syncs_triggers(void) {
  arkilian *db = open_db("test_reg_batch.db");
  assert(db_begin(db) == 0);
  // sync_backup_triggers must join the ambient transaction, not fail
  // with "cannot start a transaction within a transaction".
  assert(db_exec(db, "CREATE TABLE bt (id INTEGER PRIMARY KEY, v TEXT)") == SQLITE_OK);
  assert(db_commit(db) == 0);
  assert(db_exec(db, "INSERT INTO bt (v) VALUES ('x')") == SQLITE_OK);
  assert(count_payloads(db, "REPLACE INTO \"bt\"%") >= 1);
  db_close(db);
  cleanup("test_reg_batch.db");
}

// ── Statement pool hygiene ──────────────────────────────────────────

static void test_prepare_empty_sql_rejected(void) {
  arkilian *db = open_db("test_reg_empty.db");
  int before = db_stmt_count(db);
  int rc = db_prepare(db, "");
  assert(rc == SQLITE_ERROR);
  assert(db_stmt_count(db) == before); // no ghost slot appended
  rc = db_prepare(db, "   ");
  assert(rc == SQLITE_ERROR);
  assert(db_stmt_count(db) == before);
  db_close(db);
  cleanup("test_reg_empty.db");
}

// ── Shutdown latency ────────────────────────────────────────────────

static void test_close_does_not_block_on_backup_interval(void) {
  ark_setenv("ARKILIAN_ENABLE_BACKUP", "1", 1);
  ark_setenv("ARKILIAN_BACKUP_INTERVAL", "3600", 1); // 1 hour
  ark_setenv("ARKILIAN_BACKUP_PATH", "test_reg_close_backup.sqlite", 1);
  arkilian *db = NULL;
  assert(db_init(&db, "test_reg_close.db") == 0);
  db_exec(db, "CREATE TABLE t (id INTEGER PRIMARY KEY)");

  double start = now_ms();
  db_close(db); // previously: hung up to 3600s inside pthread_join
  double elapsed = now_ms() - start;

  assert(elapsed < 10000.0); // must return in seconds, not an hour
  printf("(%.0f ms) ", elapsed);

  ark_setenv("ARKILIAN_ENABLE_BACKUP", "0", 1);
  cleanup("test_reg_close.db");
  remove("test_reg_close_backup.sqlite");
}

// ── int64 binding ───────────────────────────────────────────────────

static void test_bind_int64_roundtrip(void) {
  arkilian *db = open_db("test_reg_i64.db");
  db_exec(db, "CREATE TABLE big (ts INTEGER)");
  db_prepare(db, "INSERT INTO big (ts) VALUES (?)");
  sqlite3_int64 v = 1718400000123LL; // ~2024 epoch ms — overflows int32
  assert(db_bind_int64(db, 1, v) == SQLITE_OK);
  assert(db_step(db) == SQLITE_DONE);
  db_finalize(db);

  db_prepare(db, "SELECT ts FROM big");
  assert(db_step(db) == SQLITE_ROW);
  assert(db_column_int64(db, 0) == v);
  db_finalize(db);
  db_close(db);
  cleanup("test_reg_i64.db");
}

// ── BLOB accessors ──────────────────────────────────────────────────

static void test_blob_column_accessors(void) {
  arkilian *db = open_db("test_reg_blob.db");
  db_exec(db, "CREATE TABLE b (d BLOB)");
  // 0x41, 0x00, 0x42 — embedded NUL kills any text-based read path
  db_exec(db, "INSERT INTO b (d) VALUES (X'410042')");
  db_prepare(db, "SELECT d FROM b");
  assert(db_step(db) == SQLITE_ROW);
  assert(db_column_type(db, 0) == SQLITE_BLOB);
  assert(db_column_bytes(db, 0) == 3);
  const unsigned char *blob = (const unsigned char *)db_column_blob(db, 0);
  assert(blob != NULL);
  assert(blob[0] == 0x41 && blob[1] == 0x00 && blob[2] == 0x42);
  db_finalize(db);
  db_close(db);
  cleanup("test_reg_blob.db");
}

// ── Per-instance wal_last_sql ───────────────────────────────────────

static void test_wal_last_sql_is_per_instance(void) {
  arkilian *db1 = open_db("test_reg_inst1.db");
  arkilian *db2 = open_db("test_reg_inst2.db");

  db_exec(db1, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)");
  db_exec(db1, "INSERT INTO t (v) VALUES ('instance-one-data')");

  // db2 has no writes — must NOT observe db1's payload through a
  // shared static buffer (the old bug).
  const char *sql2 = db_wal_last_sql(db2);
  assert(sql2 == NULL);

  const char *sql1 = db_wal_last_sql(db1);
  assert(sql1 != NULL && strstr(sql1, "instance-one-data") != NULL);

  db_close(db1);
  db_close(db2);
  cleanup("test_reg_inst1.db");
  cleanup("test_reg_inst2.db");
}

// ── Trigger sync failure is visible ─────────────────────────────────

static void test_sync_success_leaves_errmsg_clean(void) {
  arkilian *db = open_db("test_reg_clean.db");
  // A normal schema syncs fine — no error may be recorded.
  const char *msg = db_errmsg(db);
  assert(msg != NULL && strcmp(msg, "not an error") == 0);
  db_close(db);
  cleanup("test_reg_clean.db");
}

// ── No destination configured → rows must survive ───────────────────
// Regression for a proven data-loss bug: ship_to_backup reported
// SHIP_OK when ARKILIAN_CONTROL_URL was unset, so the drain loop
// DELETED every captured row. Backup is enabled by default — the
// default configuration was quietly destroying data.

static void test_no_destination_rows_survive(void) {
  cleanup("test_reg_nodest.db");
  // An EXPLICITLY EMPTY control URL — not a missing one — is "no
  // destination" in a way that survives the ./.env file the repo ships
  // (load_env only sets a key when getenv returns NULL; "" is non-NULL so
  // .env cannot override it). This was the root cause of the Task-27
  // failure: a prior test set the URL, this test ark_unsetenv()'d it, but
  // db_init's load_env re-injected it from ./.env → push_url became
  // non-empty → the flush thread shipped and incremented attempts,
  // breaking the assert that none were attempted.
  ark_setenv("ARKILIAN_CONTROL_URL", "", 1);
  ark_setenv("ARKILIAN_ENABLE_BACKUP", "1", 1);
  ark_setenv("ARKILIAN_BACKUP_INTERVAL", "3600", 1);
  ark_setenv("ARKILIAN_SKIP_STARTUP_AUTH", "1", 1);
  arkilian *db = NULL;
  assert(db_init(&db, "test_reg_nodest.db") == 0);
  assert(db_exec(db, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)") == SQLITE_OK);
  assert(db_exec(db, "INSERT INTO t (v) VALUES ('a')") == SQLITE_OK);
  assert(db_exec(db, "INSERT INTO t (v) VALUES ('b')") == SQLITE_OK);

  sleep(3); // > poll interval — a buggy drain would have deleted by now
  int depth = db_backup_queue_depth(db);
  assert(depth == 3); // DDL + 2 inserts: everything preserved
  db_prepare(db, "SELECT COALESCE(SUM(attempts), 0) FROM _pending_backup");
  assert(db_step(db) == SQLITE_ROW);
  assert(db_column_int(db, 0) == 0); // never attempted, never dead-lettered
  db_finalize(db);

  db_close(db);
  cleanup("test_reg_nodest.db");
}

// ── Replay fidelity for non-INTEGER-PK rowid tables ─────────────────
// Regression for proven divergence: REPLACE INTO deletes+reinserts, so
// destination rowids shift after any UPDATE while the source's rowid
// stays — rowid-keyed deletes then hit the wrong row and stale copies
// remain. DELETEs are now keyed on the PRIMARY KEY, which survives
// REPLACE, so replay must converge exactly.

static void test_text_pk_replay_fidelity(void) {
  cleanup("test_reg_fid.db");
  ark_setenv("ARKILIAN_ENABLE_BACKUP", "1", 1);
  ark_setenv("ARKILIAN_CONTROL_URL", "http://127.0.0.1:1", 1); // keep rows in outbox
  ark_setenv("ARKILIAN_BACKUP_INTERVAL", "3600", 1);
  arkilian *db = NULL;
  assert(db_init(&db, "test_reg_fid.db") == 0);
  assert(db_exec(db, "CREATE TABLE kv (k TEXT PRIMARY KEY, v TEXT)") == SQLITE_OK);
  assert(db_exec(db, "INSERT INTO kv (k, v) VALUES ('k1', 'v1')") == SQLITE_OK);
  assert(db_exec(db, "INSERT INTO kv (k, v) VALUES ('k2', 'v2')") == SQLITE_OK);
  assert(db_exec(db, "UPDATE kv SET v = 'v1-updated' WHERE k = 'k1'") == SQLITE_OK);
  assert(db_exec(db, "DELETE FROM kv WHERE k = 'k2'") == SQLITE_OK);

  // Collect the captured payloads in id order.
  char payloads[8][512];
  int n = 0;
  db_prepare(db, "SELECT payload FROM _pending_backup ORDER BY id");
  while (db_step(db) == SQLITE_ROW && n < 8) {
    snprintf(payloads[n++], sizeof(payloads[0]), "%s", db_column_text(db, 0));
  }
  db_finalize(db);
  assert(n >= 4);

  // Replay onto a fresh destination with the identical schema.
  sqlite3 *dst = NULL;
  assert(sqlite3_open_v2("test_reg_fid_dst.db", &dst,
                         SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, NULL) == SQLITE_OK);
  assert(sqlite3_exec(dst, "CREATE TABLE kv (k TEXT PRIMARY KEY, v TEXT)",
                      NULL, NULL, NULL) == SQLITE_OK);
  for (int i = 0; i < n; i++) {
    // Skip the DDL capture row; replay only table payloads.
    if (strncmp(payloads[i], "REPLACE INTO \"kv\"", 17) == 0 ||
        strncmp(payloads[i], "DELETE FROM \"kv\"", 16) == 0) {
      assert(sqlite3_exec(dst, payloads[i], NULL, NULL, NULL) == SQLITE_OK);
    }
  }

  // Destination must converge EXACTLY with the source: one row, updated.
  sqlite3_stmt *st = NULL;
  sqlite3_prepare_v2(dst, "SELECT k, v FROM kv", -1, &st, NULL);
  assert(sqlite3_step(st) == SQLITE_ROW);
  assert(strcmp((const char *)sqlite3_column_text(st, 0), "k1") == 0);
  assert(strcmp((const char *)sqlite3_column_text(st, 1), "v1-updated") == 0);
  assert(sqlite3_step(st) == SQLITE_DONE); // no stale k2, no duplicate k1
  sqlite3_finalize(st);
  sqlite3_close(dst);

  db_close(db);
  cleanup("test_reg_fid.db");
  cleanup("test_reg_fid_dst.db");
}

// ── Keyless rowid tables are skipped loudly, not mis-replicated ─────

static void test_keyless_table_skipped(void) {
  cleanup("test_reg_keyless.db");
  ark_setenv("ARKILIAN_ENABLE_BACKUP", "1", 1);
  ark_setenv("ARKILIAN_CONTROL_URL", "http://127.0.0.1:1", 1);
  ark_setenv("ARKILIAN_BACKUP_INTERVAL", "3600", 1);
  arkilian *db = NULL;
  assert(db_init(&db, "test_reg_keyless.db") == 0);
  // Keyless rowid table (unreplayable: REPLACE appends, rowids drift).
  assert(db_exec(db, "CREATE TABLE ev (ts INTEGER, ev TEXT)") == SQLITE_OK);
  // A keyed table next to it still captures.
  assert(db_exec(db, "CREATE TABLE ok (id INTEGER PRIMARY KEY, v TEXT)") == SQLITE_OK);

  assert(db_backup_trigger_coverage(db) == 0); // only ok counts, fully covered
  // The skipped table is visible to monitoring — data that never leaves
  // the box must never read as "all covered".
  assert(db_backup_skipped_table_count(db) == 1);
  db_prepare(db, "SELECT COUNT(*) FROM sqlite_master "
                 "WHERE type='trigger' AND name LIKE 'trg\\_%' ESCAPE '\\'");
  assert(db_step(db) == SQLITE_ROW);
  assert(db_column_int(db, 0) == 3); // exactly ok's 3 triggers
  db_finalize(db);

  // Writes to the keyless table still work locally.
  assert(db_exec(db, "INSERT INTO ev (ts, ev) VALUES (1, 'x')") == SQLITE_OK);

  db_close(db);
  cleanup("test_reg_keyless.db");
}

// ── Dead-letter zombie row must be cleared, not spun on ─────────────
// If a dead-letter INSERT succeeds but the pending DELETE fails (BUSY),
// the row exists in BOTH tables with attempts >= MAX_ATTEMPTS. The next
// pass must (OR IGNORE) absorb the id-conflict and remove the pending
// copy — previously the PK conflict left the row forever and, reporting
// "work drained", hot-spun the flush loop with no sleep.

static void test_dead_letter_zombie_cleared(void) {
  cleanup("test_reg_zombie.db");
  ark_setenv("ARKILIAN_ENABLE_BACKUP", "1", 1);
  ark_setenv("ARKILIAN_CONTROL_URL", "http://127.0.0.1:1", 1);
  ark_setenv("ARKILIAN_BACKUP_INTERVAL", "3600", 1);
  // Key + skip-auth keep backup enabled so the flush thread actually runs
  // (the api_key guard in db_init disables backup otherwise, and the
  // zombie would never be cleaned).
  ark_setenv("ARKILIAN_API_KEY", "test-key", 1);
  ark_setenv("ARKILIAN_SKIP_STARTUP_AUTH", "1", 1);
  arkilian *db = NULL;
  assert(db_init(&db, "test_reg_zombie.db") == 0);

  // Craft the zombie state directly: pending row at MAX_ATTEMPTS AND an
  // already-dead-lettered copy (the "insert succeeded, delete failed"
  // residue). Make the zombie the queue head (drop the DDL capture row)
  // so strict stop-on-retry ordering doesn't hide it behind other rows.
  assert(db_exec(db, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)") == SQLITE_OK);
  assert(db_exec(db, "DELETE FROM _pending_backup") == SQLITE_OK);
  assert(db_exec(db,
      "INSERT INTO _pending_backup (id, payload, attempts, created_at) "
      "VALUES (42, 'REPLACE INTO \"t\" (\"id\", \"v\") VALUES (1, 1)', 10, 0)") == SQLITE_OK);
  assert(db_exec(db,
      "INSERT INTO _dead_backup (id, payload, attempts, failed_reason, created_at) "
      "VALUES (42, 'REPLACE INTO \"t\" (\"id\", \"v\") VALUES (1, 1)', 10, "
      "'max attempts exceeded', 0)") == SQLITE_OK);

  // Wake the flush thread and poll for the dead-letter (bounded): the
  // cleanup runs on the async flush thread, which shares wake_cond with
  // the snapshot thread — the single cond_signal from db_wal_flush may
  // wake the wrong waiter, delaying the drain by up to the 2s poll
  // interval. A fixed sleep is therefore racy across codegen/timing;
  // poll instead and fail loudly if the zombie is never cleaned.
  db_wal_flush(db);
  int zombie_cleared = 0;
  for (int wait_i = 0; wait_i < 50; wait_i++) {
    db_prepare(db, "SELECT COUNT(*) FROM _pending_backup WHERE id = 42");
    if (db_step(db) == SQLITE_ROW && db_column_int(db, 0) == 0) {
      zombie_cleared = 1;
    }
    db_finalize(db);
    if (zombie_cleared) break;
    usleep(200000); // 200ms
  }
  assert(zombie_cleared && "zombie row 42 never dead-lettered");

  // The zombie must be GONE from _pending_backup (moved to dead once),
  // and _dead_backup holds exactly one copy.
  db_prepare(db, "SELECT COUNT(*) FROM _pending_backup WHERE id = 42");
  assert(db_step(db) == SQLITE_ROW);
  assert(db_column_int(db, 0) == 0);
  db_finalize(db);
  db_prepare(db, "SELECT COUNT(*) FROM _dead_backup WHERE id = 42");
  assert(db_step(db) == SQLITE_ROW);
  assert(db_column_int(db, 0) == 1);
  db_finalize(db);

  db_close(db);
  cleanup("test_reg_zombie.db");
}

// ── Raw-handle DDL flags dirty & resync repairs (Risk #1) ───────────
//
// DDL through db_exec / db_prepare is intercepted and re-synced. DDL run
// on the raw handle from db_get_handle() (Prisma/Drizzle/raw sqlite3_exec)
// bypasses the wrapper; the schema authorizer must flag it so monitoring
// surfaces the desync, and db_resync_triggers() must repair coverage.

static void test_ddl_via_raw_handle_flags_dirty(void) {
  arkilian *db = open_db("test_reg_dirty.db");

  // Wrapped DDL: the authorizer fires during CREATE, but apply_ddl_capture
  // re-syncs and clears the dirty flag — coverage is full and dirty is 0.
  assert(db_exec(db, "CREATE TABLE wrapped (id INTEGER PRIMARY KEY, v TEXT)") == SQLITE_OK);
  assert(db_backup_trigger_coverage(db) == 0);
  assert(db_backup_triggers_dirty(db) == 0);

  // Raw-handle DDL bypasses the wrapper → the authorizer flags dirty.
  // The new table has NO triggers, so coverage reports the 3-trigger gap.
  sqlite3 *raw = db_get_handle(db);
  assert(raw != NULL);
  assert(sqlite3_exec(raw, "CREATE TABLE raw (id INTEGER PRIMARY KEY, v TEXT)",
                      NULL, NULL, NULL) == SQLITE_OK);
  assert(db_backup_triggers_dirty(db) == 1);
  assert(db_backup_trigger_coverage(db) > 0);

  // An explicit resync repairs coverage and clears the dirty flag.
  assert(db_resync_triggers(db) == SQLITE_OK);
  assert(db_backup_trigger_coverage(db) == 0);
  assert(db_backup_triggers_dirty(db) == 0);

  db_close(db);
  cleanup("test_reg_dirty.db");
}

// ── Local filesystem NOT flagged as network (Risk #3) ──────────────
//
// The network-filesystem guard must NOT false-positive on the local
// storage the dev/CI suite runs on — otherwise capture would be silently
// disabled. Backup-enabled init + a captured table with full coverage is
// the guard.

static void test_local_fs_capture_not_disabled(void) {
  cleanup("test_reg_fstype.db");
  // Explicit empty URL: no destination, no background shipping, yet backup
  // stays "enabled" (capture runs; the flush loop just has nowhere to ship).
  ark_setenv("ARKILIAN_ENABLE_BACKUP", "1", 1);
  ark_setenv("ARKILIAN_CONTROL_URL", "", 1);
  ark_setenv("ARKILIAN_SKIP_STARTUP_AUTH", "1", 1);
  ark_setenv("ARKILIAN_API_KEY", "test-key", 1);
  ark_setenv("ARKILIAN_BACKUP_INTERVAL", "3600", 1);
  arkilian *db = NULL;
  assert(db_init(&db, "test_reg_fstype.db") == 0);
  // capture_ok stayed 1 → backup not force-disabled by a false FS positive.
  assert(db_backup_is_enabled(db) == 1);
  assert(db_exec(db, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)") == SQLITE_OK);
  assert(db_backup_trigger_coverage(db) == 0); // 1 captured table, 3 triggers
  assert(db_backup_triggers_dirty(db) == 0);   // wrapped DDL auto-repaired
  db_close(db);
  cleanup("test_reg_fstype.db");
}

// ── Opt-in auto-resync for raw-handle DDL (Risk #1) ─────────────────
// With auto-resync ENABLED, raw DDL via db_get_handle followed by a
// wrapped INSERT triggers an automatic resync — no manual
// db_resync_triggers() needed. The INSERT is captured.

static void test_auto_resync_repairs_raw_ddl(void) {
  arkilian *db = open_db("test_reg_auto.db");
  // Enable auto-resync BEFORE any raw DDL.
  db_set_auto_resync_triggers(db, 1);
  assert(db_get_auto_resync_triggers(db) == 1);

  // Wrapped DDL: covers initial schema.
  assert(db_exec(db, "CREATE TABLE wrapped (id INTEGER PRIMARY KEY, v TEXT)") == SQLITE_OK);
  assert(db_backup_trigger_coverage(db) == 0);
  assert(db_backup_triggers_dirty(db) == 0);

  // Raw-handle DDL: authorizer flags dirty (no wrapped dispatch to repair).
  sqlite3 *raw = db_get_handle(db);
  assert(sqlite3_exec(raw, "CREATE TABLE raw (id INTEGER PRIMARY KEY, v TEXT)",
                      NULL, NULL, NULL) == SQLITE_OK);
  assert(db_backup_triggers_dirty(db) == 1);
  assert(db_backup_trigger_coverage(db) > 0); // raw table has no triggers

  // Now a wrapped INSERT — auto-resync fires at the top of db_exec before
  // the INSERT runs. The INSERT into "raw" IS captured (triggers now exist).
  assert(db_exec(db, "INSERT INTO raw (v) VALUES ('auto-captured')") == SQLITE_OK);

  // Triggers repaired + dirty cleared by the auto-resync.
  assert(db_backup_triggers_dirty(db) == 0);
  assert(db_backup_trigger_coverage(db) == 0);

  // The INSERT was captured in _pending_backup (proves the resync ran
  // before the row was written).
  assert(count_payloads(db, "REPLACE INTO \"raw\"%") >= 1);

  // Auto-resync off (restore default): raw DDL stays dirty.
  db_set_auto_resync_triggers(db, 0);
  assert(db_get_auto_resync_triggers(db) == 0);

  db_close(db);
  cleanup("test_reg_auto.db");
}

// ── Auto-resync off (default) → raw DDL stays dirty ──────────────────
// Without opting in, raw DDL leaves triggers_dirty set — the current
// contract is "raw DDL is an incident, repair explicitly."

static void test_auto_resync_off_by_default(void) {
  arkilian *db = open_db("test_reg_noresync.db");
  assert(db_get_auto_resync_triggers(db) == 0); // off by default

  assert(db_exec(db, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)") == SQLITE_OK);
  sqlite3 *raw = db_get_handle(db);
  assert(sqlite3_exec(raw, "CREATE TABLE t2 (id INTEGER PRIMARY KEY)", NULL, NULL, NULL) == SQLITE_OK);

  // Dirty stays set — no auto-repair. A wrapped INSERT doesn't clear it
  // (apply_ddl_capture only fires on wrapped DDL, not on non-DDL INSERT).
  assert(db_backup_triggers_dirty(db) == 1);
  assert(db_exec(db, "INSERT INTO t2 (id) VALUES (1)") == SQLITE_OK);
  assert(db_backup_triggers_dirty(db) == 1); // still dirty — not auto-repaired

  // Explicit resync still works.
  assert(db_resync_triggers(db) == SQLITE_OK);
  assert(db_backup_triggers_dirty(db) == 0);

  db_close(db);
  cleanup("test_reg_noresync.db");
}

// ── Capture-paused sticky signal (Risk #1) ──────────────────────────
// When the outbox hits ARKILIAN_MAX_QUEUE_DEPTH, capture pauses (the
// trigger's `WHERE count < cap` gate stops inserting). db_backup_capture_paused()
// returns 1. It stays 1 even after the queue drains (sticky) — the operator
// knows a gap occurred until the snapshot clears it.

static void test_capture_paused_at_cap(void) {
  cleanup("test_reg_paused.db");
  // No destination → flush thread can't drain → queue fills to cap.
  ark_setenv("ARKILIAN_ENABLE_BACKUP", "1", 1);
  ark_setenv("ARKILIAN_CONTROL_URL", "", 1);
  ark_setenv("ARKILIAN_SKIP_STARTUP_AUTH", "1", 1);
  ark_setenv("ARKILIAN_API_KEY", "test-key", 1);
  ark_setenv("ARKILIAN_BACKUP_INTERVAL", "3600", 1);
  ark_setenv("ARKILIAN_MAX_QUEUE_DEPTH", "5", 1); // tight cap
  arkilian *db = NULL;
  assert(db_init(&db, "test_reg_paused.db") == 0);

  // Not paused at start.
  assert(db_backup_capture_paused(db) == 0);

  assert(db_exec(db, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)") == SQLITE_OK);

  // Write past the cap — the flush thread can't drain (no destination),
  // so the queue hits cap. The flush thread's poll cycle will set
  // capture_paused after ~2s.
  for (int i = 0; i < 20; i++) {
    char sql[64];
    snprintf(sql, sizeof(sql), "INSERT INTO t (v) VALUES ('row%d')", i);
    assert(db_exec(db, sql) == SQLITE_OK);
  }

  // Wait for the flush thread to observe the cap.
  int waited = 0;
  while (!db_backup_capture_paused(db) && waited < 8000) {
    usleep(200 * 1000);
    waited += 200;
  }
  assert(db_backup_capture_paused(db) == 1);

  db_close(db);
  cleanup("test_reg_paused.db");
  ark_setenv("ARKILIAN_MAX_QUEUE_DEPTH", "100000", 1); // restore
}

// ── Main ────────────────────────────────────────────────────────────

int main(void) {
  ark_setenv("ARKILIAN_MAX_ATTEMPTS", "3", 1); // fast dead-lettering for tests
  printf("=== Arkilian Audit Regression Tests ===\n\n");

  printf("[WITHOUT ROWID]\n");
  RUN_TEST(test_without_rowid_delete_works);
  RUN_TEST(test_rowid_table_delete_payload_is_pk_keyed);

  printf("\n[Special Table Names]\n");
  RUN_TEST(test_keyword_table_name_gets_triggers);
  RUN_TEST(test_table_name_with_spaces);
  RUN_TEST(test_generated_columns_excluded_from_payloads);

  printf("\n[DDL Detection]\n");
  RUN_TEST(test_leading_whitespace_ddl_syncs_triggers);
  RUN_TEST(test_leading_comment_ddl_syncs_triggers);
  RUN_TEST(test_ddl_inside_batch_txn_syncs_triggers);

  printf("\n[Statement Pool]\n");
  RUN_TEST(test_prepare_empty_sql_rejected);

  printf("\n[Shutdown]\n");
  RUN_TEST(test_close_does_not_block_on_backup_interval);

  printf("\n[Types]\n");
  RUN_TEST(test_bind_int64_roundtrip);
  RUN_TEST(test_blob_column_accessors);

  printf("\n[Isolation]\n");
  RUN_TEST(test_wal_last_sql_is_per_instance);
  RUN_TEST(test_sync_success_leaves_errmsg_clean);

  printf("\n[Replay Integrity]\n");
  RUN_TEST(test_no_destination_rows_survive);
  RUN_TEST(test_text_pk_replay_fidelity);
  RUN_TEST(test_keyless_table_skipped);

  printf("\n[Dead-Letter Hygiene]\n");
  RUN_TEST(test_dead_letter_zombie_cleared);

  printf("\n[Schema Desync Detection]\n");
  RUN_TEST(test_ddl_via_raw_handle_flags_dirty);

  printf("\n[Auto-Resync]\n");
  RUN_TEST(test_auto_resync_repairs_raw_ddl);
  RUN_TEST(test_auto_resync_off_by_default);

  printf("\n[Filesystem Guard]\n");
  RUN_TEST(test_local_fs_capture_not_disabled);

  printf("\n[Capture Paused Signal]\n");
  RUN_TEST(test_capture_paused_at_cap);

  printf("\n=== Results: %d/%d passed ===\n", tests_passed, tests_run);
  return (tests_passed == tests_run) ? 0 : 1;
}
