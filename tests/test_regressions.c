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
//   cc tests/test_regressions.c src/class.c src/deps/sqlite/sqlite3.c \
//      -Isrc -Isrc/deps/sqlite -lcurl -lpthread -o test_regressions

#include "class.h"
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

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
  setenv("ARKILIAN_ENABLE_BACKUP", "0", 1);
  setenv("ARKILIAN_WAL_PUSH_URL", "http://127.0.0.1:1", 1);
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

static void test_rowid_table_delete_payload_unchanged(void) {
  arkilian *db = open_db("test_reg_rowid.db");
  db_exec(db, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)");
  db_exec(db, "INSERT INTO t (v) VALUES ('x')");
  assert(db_exec(db, "DELETE FROM t WHERE id = 1") == SQLITE_OK);
  assert(count_payloads(db, "DELETE FROM \"t\" WHERE rowid = %") >= 1);
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
  assert(db_exec(db, "CREATE TABLE g (a INT, b INT GENERATED ALWAYS AS (a * 2) VIRTUAL)") == SQLITE_OK);
  assert(db_exec(db, "INSERT INTO g (a) VALUES (21)") == SQLITE_OK);

  // Payload must list only real columns — a generated column in a
  // REPLACE INTO column list fails on the replay side.
  assert(count_payloads(db, "REPLACE INTO \"g\" (\"a\")%") >= 1);

  // And the captured payload must actually replay cleanly.
  const char *payload = db_wal_last_sql(db);
  assert(payload != NULL && strstr(payload, "REPLACE INTO \"g\"") != NULL);
  assert(db_exec(db, payload) == SQLITE_OK);

  // Generated values still correct locally
  db_prepare(db, "SELECT b FROM g WHERE a = 21");
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
  setenv("ARKILIAN_ENABLE_BACKUP", "1", 1);
  setenv("ARKILIAN_BACKUP_INTERVAL", "3600", 1); // 1 hour
  setenv("ARKILIAN_BACKUP_PATH", "test_reg_close_backup.sqlite", 1);
  arkilian *db = NULL;
  assert(db_init(&db, "test_reg_close.db") == 0);
  db_exec(db, "CREATE TABLE t (id INTEGER PRIMARY KEY)");

  double start = now_ms();
  db_close(db); // previously: hung up to 3600s inside pthread_join
  double elapsed = now_ms() - start;

  assert(elapsed < 10000.0); // must return in seconds, not an hour
  printf("(%.0f ms) ", elapsed);

  setenv("ARKILIAN_ENABLE_BACKUP", "0", 1);
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

// ── Main ────────────────────────────────────────────────────────────

int main(void) {
  printf("=== Arkilian Audit Regression Tests ===\n\n");

  printf("[WITHOUT ROWID]\n");
  RUN_TEST(test_without_rowid_delete_works);
  RUN_TEST(test_rowid_table_delete_payload_unchanged);

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

  printf("\n=== Results: %d/%d passed ===\n", tests_passed, tests_run);
  return (tests_passed == tests_run) ? 0 : 1;
}
