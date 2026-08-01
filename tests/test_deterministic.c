// Arkilian Deterministic SQL Expansion Tests
//
// Verify that non-deterministic SQL (datetime('now'), DEFAULT, random())
// is expanded into literal values before shipping to the WAL.
//
// Compile (macOS/Linux):
//   cc tests/test_deterministic.c src/class.c src/deps/sqlite/sqlite3.c \
//      -Isrc -Isrc/deps/sqlite -lcurl -lpthread -o test_deterministic

#include "../src/class.h"
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef _WIN32
#include <windows.h>
#else
#include <pthread.h>
#include <unistd.h>
#endif

#define TEST_DB "test_deterministic.db"

static int tests_run = 0;
static int tests_passed = 0;

#define RUN_TEST(fn)                                                           \
  do {                                                                         \
    tests_run++;                                                               \
    printf("  [%02d] %-50s ", tests_run, #fn);                                 \
    fn();                                                                      \
    tests_passed++;                                                            \
    printf("PASS\n");                                                          \
  } while (0)

#define ASSERT(cond) assert(cond)

// ── Helpers ─────────────────────────────────────────────────────────

static arkilian *open_test_db(void) {
  setenv("ARKILIAN_ENABLE_BACKUP", "0", 1);
  setenv("ARKILIAN_WAL_PUSH_URL", "http://127.0.0.1:9", 1);
  arkilian *db = NULL;
  int rc = db_init(&db, TEST_DB);
  ASSERT(rc == 0);
  ASSERT(db != NULL);
  return db;
}

static void cleanup_files(void) { remove(TEST_DB); }

static int str_contains(const char *haystack, const char *needle) {
  return strstr(haystack, needle) != NULL;
}

// ── Deterministic Expansion Tests (prepare/bind/step/finalize path) ──

static void test_datetime_now_is_expanded_prepare(void) {
  arkilian *db = open_test_db();
  db_exec(db, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, ts TEXT)");

  db_prepare(db, "INSERT INTO t (name, ts) VALUES (?, datetime('now'))");
  db_bind_text(db, 1, "alice");
  db_step(db);
  db_finalize(db);

  const char *sql = db_wal_last_sql(db);
  ASSERT(sql != NULL);
  // The shipped SQL should NOT contain "datetime('now')" — it should be
  // expanded
  ASSERT(!str_contains(sql, "datetime('now')"));
  // The shipped SQL should be a REPLACE with literal values
  ASSERT(str_contains(sql, "REPLACE INTO"));
  ASSERT(str_contains(sql, "alice"));

  db_close(db);
  cleanup_files();
}

static void test_default_value_is_expanded_prepare(void) {
  arkilian *db = open_test_db();
  db_exec(db,
          "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT DEFAULT 'anon')");

  db_prepare(db, "INSERT INTO t (id) VALUES (42)");
  db_step(db);
  db_finalize(db);

  const char *sql = db_wal_last_sql(db);
  ASSERT(sql != NULL);
  // The shipped SQL should contain the resolved DEFAULT value, not the literal
  // DEFAULT
  ASSERT(!str_contains(sql, "DEFAULT"));
  ASSERT(str_contains(sql, "REPLACE INTO"));
  ASSERT(str_contains(sql, "anon"));

  db_close(db);
  cleanup_files();
}

static void test_random_is_expanded_prepare(void) {
  arkilian *db = open_test_db();
  db_exec(db, "CREATE TABLE t (id INTEGER PRIMARY KEY, n INTEGER)");

  db_prepare(db, "INSERT INTO t (n) VALUES (abs(random() % 1000))");
  db_step(db);
  db_finalize(db);

  const char *sql = db_wal_last_sql(db);
  ASSERT(sql != NULL);
  // Should not contain random() — should be expanded to an integer literal
  ASSERT(!str_contains(sql, "random()"));
  ASSERT(str_contains(sql, "REPLACE INTO"));

  db_close(db);
  cleanup_files();
}

static void test_simple_insert_is_replaced_deterministically(void) {
  arkilian *db = open_test_db();
  db_exec(db,
          "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)");

  db_prepare(db, "INSERT INTO t (name, age) VALUES (?, ?)");
  db_bind_text(db, 1, "bob");
  db_bind_int(db, 2, 30);
  db_step(db);
  db_finalize(db);

  const char *sql = db_wal_last_sql(db);
  ASSERT(sql != NULL);
  ASSERT(str_contains(sql, "REPLACE INTO"));
  ASSERT(str_contains(sql, "bob"));
  ASSERT(str_contains(sql, "30"));

  db_close(db);
  cleanup_files();
}

static void test_update_is_replaced_deterministically(void) {
  arkilian *db = open_test_db();
  db_exec(db, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)");
  db_exec(db, "INSERT INTO t (name) VALUES ('old')");

  db_prepare(db, "UPDATE t SET name = ? WHERE id = 1");
  db_bind_text(db, 1, "new");
  db_step(db);
  db_finalize(db);

  const char *sql = db_wal_last_sql(db);
  ASSERT(sql != NULL);
  ASSERT(str_contains(sql, "REPLACE INTO"));
  ASSERT(str_contains(sql, "new"));
  ASSERT(!str_contains(sql, "old"));

  db_close(db);
  cleanup_files();
}

static void test_delete_is_rowid_deterministic(void) {
  arkilian *db = open_test_db();
  db_exec(db, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)");
  db_exec(db, "INSERT INTO t (name) VALUES ('x')");

  db_prepare(db, "DELETE FROM t WHERE name = 'x'");
  db_step(db);
  db_finalize(db);

  const char *sql = db_wal_last_sql(db);
  ASSERT(sql != NULL);
  ASSERT(str_contains(sql, "DELETE FROM"));
  ASSERT(str_contains(sql, "WHERE rowid ="));
  ASSERT(!str_contains(sql, "name = 'x'"));

  db_close(db);
  cleanup_files();
}

// ── Deterministic Expansion Tests (db_exec direct path) ─────────────

static void test_exec_insert_is_expanded(void) {
  arkilian *db = open_test_db();
  db_exec(db, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, n INTEGER)");

  db_exec(db, "INSERT INTO t (name, n) VALUES ('eve', abs(random() % 100))");

  const char *sql = db_wal_last_sql(db);
  ASSERT(sql != NULL);
  ASSERT(!str_contains(sql, "random()"));
  ASSERT(str_contains(sql, "REPLACE INTO"));
  ASSERT(str_contains(sql, "eve"));

  db_close(db);
  cleanup_files();
}

static void test_exec_datetime_is_expanded(void) {
  arkilian *db = open_test_db();
  db_exec(db, "CREATE TABLE t (id INTEGER PRIMARY KEY, ts TEXT)");

  db_exec(db, "INSERT INTO t (ts) VALUES (datetime('now'))");

  const char *sql = db_wal_last_sql(db);
  ASSERT(sql != NULL);
  ASSERT(!str_contains(sql, "datetime('now')"));
  ASSERT(str_contains(sql, "REPLACE INTO"));

  db_close(db);
  cleanup_files();
}

// ── DDL is passed through (ddl doesn't fire preupdate hook) ─────────

static void test_ddl_is_not_replaced(void) {
  arkilian *db = open_test_db();

  db_exec(db, "CREATE TABLE foo (x INT)");

  const char *sql = db_wal_last_sql(db);
  ASSERT(sql != NULL);
  ASSERT(str_contains(sql, "CREATE TABLE"));
  ASSERT(!str_contains(sql, "REPLACE INTO"));

  db_close(db);
  cleanup_files();
}

// ── Data integrity after expansion ───────────────────────────────────

static void test_data_integrity_after_expansion(void) {
  arkilian *db = open_test_db();
  db_exec(db, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, score REAL, "
              "ts TEXT)");

  db_prepare(db,
             "INSERT INTO t (name, score, ts) VALUES (?, ?, datetime('now'))");
  db_bind_text(db, 1, "charlie");
  db_bind_double(db, 2, 99.5);
  db_step(db);
  db_finalize(db);

  // Verify data was written correctly despite expanded shipping
  db_prepare(db, "SELECT name, score, ts FROM t WHERE id = 1");
  db_step(db);
  ASSERT(strcmp(db_column_text(db, 0), "charlie") == 0);
  ASSERT(db_column_double(db, 1) == 99.5);
  ASSERT(db_column_text(db, 2) != NULL); // ts should be set
  db_finalize(db);

  db_close(db);
  cleanup_files();
}

// ── Idempotency: verify shipped REPLACE can be replayed ─────────────

static void test_shipped_sql_can_be_replayed(void) {
  arkilian *db = open_test_db();
  db_exec(db,
          "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)");
  db_exec(db, "INSERT INTO t (name, age) VALUES ('dave', 25)");

  const char *shipped = db_wal_last_sql(db);
  ASSERT(shipped != NULL);
  ASSERT(str_contains(shipped, "REPLACE INTO"));

  // Replay the shipped SQL — should be idempotent
  int rc1 = db_exec(db, shipped);
  ASSERT(rc1 == SQLITE_OK);

  // Replay again — REPLACE should handle this
  int rc2 = db_exec(db, shipped);
  ASSERT(rc2 == SQLITE_OK);

  // Verify only one row exists (REPLACE upserts)
  db_prepare(db, "SELECT COUNT(*) FROM t");
  db_step(db);
  ASSERT(db_column_int(db, 0) == 1);
  db_finalize(db);

  db_close(db);
  cleanup_files();
}

// ── Main ─────────────────────────────────────────────────────────────

int main(void) {
  printf("\n=== Arkilian Deterministic SQL Expansion Tests ===\n\n");

  // Prepare / bind / step / finalize path
  RUN_TEST(test_simple_insert_is_replaced_deterministically);
  RUN_TEST(test_datetime_now_is_expanded_prepare);
  RUN_TEST(test_default_value_is_expanded_prepare);
  RUN_TEST(test_random_is_expanded_prepare);
  RUN_TEST(test_update_is_replaced_deterministically);
  RUN_TEST(test_delete_is_rowid_deterministic);

  // db_exec direct path
  RUN_TEST(test_exec_insert_is_expanded);
  RUN_TEST(test_exec_datetime_is_expanded);

  // DDL passthrough
  RUN_TEST(test_ddl_is_not_replaced);

  // Integrity tests
  RUN_TEST(test_data_integrity_after_expansion);
  RUN_TEST(test_shipped_sql_can_be_replayed);

  printf("\n  %d/%d tests passed\n\n", tests_passed, tests_run);
  return tests_passed == tests_run ? 0 : 1;
}
