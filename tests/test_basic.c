// Arkilian Wrapper Tests

#include "class.h"
#include <assert.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define TEST_DB "test_arkilian.db"
#define TEST_BACKUP "test_backup.sqlite"

static int tests_run = 0;
static int tests_passed = 0;

#define RUN_TEST(fn)                                                           \
  do {                                                                         \
    tests_run++;                                                                \
    printf("  [%02d] %-50s ", tests_run, #fn);                                 \
    fn();                                                                       \
    tests_passed++;                                                             \
    printf("PASS\n");                                                           \
  } while (0)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

static arkilian *open_test_db(void) {
#ifndef _WIN32
  setenv("ARKILIAN_ENABLE_BACKUP", "0", 1);
  setenv("ARKILIAN_BACKUP_PATH", TEST_BACKUP, 1);
#endif
  arkilian *db = NULL;
  int rc = db_init(&db, TEST_DB);
  assert(rc == 0 && "db_init failed");
  assert(db != NULL);
  return db;
}

static void cleanup_files(void) {
  remove(TEST_DB);
  remove(TEST_BACKUP);
}

// ---------------------------------------------------------------------------
// Lifecycle Tests
// ---------------------------------------------------------------------------

static void test_init_creates_db(void) {
  arkilian *db = open_test_db();
  assert(db != NULL);
  db_close(db);
  cleanup_files();
}

static void test_init_null_ptr_returns_error(void) {
  int rc = db_init(NULL, TEST_DB);
  assert(rc == 1);
  cleanup_files();
}

static void test_init_null_filename_uses_default(void) {
#ifndef _WIN32
  setenv("ARKILIAN_ENABLE_BACKUP", "0", 1);
  setenv("ARKILIAN_DB_PATH", TEST_DB, 1);
#endif
  arkilian *db = NULL;
  int rc = db_init(&db, NULL);
  assert(rc == 0);
  assert(db != NULL);
  db_close(db);
#ifndef _WIN32
  unsetenv("ARKILIAN_DB_PATH");
#endif
  cleanup_files();
}

static void test_close_null_is_safe(void) {
  db_close(NULL);
}

static void test_get_handle_returns_valid(void) {
  arkilian *db = open_test_db();
  sqlite3 *handle = db_get_handle(db);
  assert(handle != NULL);
  db_close(db);
  cleanup_files();
}

// ---------------------------------------------------------------------------
// Error Message Tests
// ---------------------------------------------------------------------------

static void test_errmsg_no_error(void) {
  arkilian *db = open_test_db();
  const char *msg = db_errmsg(db);
  assert(msg != NULL);
  assert(strcmp(msg, "not an error") == 0);
  db_close(db);
  cleanup_files();
}

static void test_errmsg_after_bad_sql(void) {
  arkilian *db = open_test_db();
  int rc = db_exec(db, "THIS IS NOT SQL");
  assert(rc != SQLITE_OK);
  const char *msg = db_errmsg(db);
  assert(msg != NULL);
  assert(strlen(msg) > 0);
  db_close(db);
  cleanup_files();
}

// ---------------------------------------------------------------------------
// db_exec Tests
// ---------------------------------------------------------------------------

static void test_exec_create_table(void) {
  arkilian *db = open_test_db();
  int rc = db_exec(db, "CREATE TABLE t1 (id INTEGER PRIMARY KEY, name TEXT)");
  assert(rc == SQLITE_OK);
  db_close(db);
  cleanup_files();
}

static void test_exec_insert(void) {
  arkilian *db = open_test_db();
  db_exec(db, "CREATE TABLE t1 (id INTEGER PRIMARY KEY, name TEXT)");
  int rc = db_exec(db, "INSERT INTO t1 (name) VALUES ('hello')");
  assert(rc == SQLITE_OK);
  db_close(db);
  cleanup_files();
}

static void test_exec_invalid_sql_returns_error(void) {
  arkilian *db = open_test_db();
  int rc = db_exec(db, "DROP TABLE nonexistent_table_xyz");
  assert(rc != SQLITE_OK);
  db_close(db);
  cleanup_files();
}

static void test_exec_null_db_returns_error(void) {
  int rc = db_exec(NULL, "SELECT 1");
  assert(rc == SQLITE_ERROR);
}

static void test_exec_null_sql_returns_error(void) {
  arkilian *db = open_test_db();
  int rc = db_exec(db, NULL);
  assert(rc == SQLITE_ERROR);
  db_close(db);
  cleanup_files();
}

static void test_exec_captures_error_msg(void) {
  arkilian *db = open_test_db();
  db_exec(db, "INSERT INTO nonexistent_table VALUES (1)");
  const char *msg = db_errmsg(db);
  assert(msg != NULL);
  assert(strstr(msg, "no such table") != NULL);
  db_close(db);
  cleanup_files();
}

// ---------------------------------------------------------------------------
// Prepare / Step / Finalize Workflow Tests
// ---------------------------------------------------------------------------

static void test_prepare_valid_sql(void) {
  arkilian *db = open_test_db();
  db_exec(db, "CREATE TABLE t1 (id INTEGER PRIMARY KEY, val TEXT)");
  int rc = db_prepare(db, "SELECT * FROM t1");
  assert(rc == SQLITE_OK);
  db_finalize(db);
  db_close(db);
  cleanup_files();
}

static void test_prepare_invalid_sql(void) {
  arkilian *db = open_test_db();
  int rc = db_prepare(db, "SELECTTTT GARBAGE");
  assert(rc != SQLITE_OK);
  const char *msg = db_errmsg(db);
  assert(msg != NULL && strlen(msg) > 0);
  db_close(db);
  cleanup_files();
}

static void test_prepare_null_db(void) {
  int rc = db_prepare(NULL, "SELECT 1");
  assert(rc == SQLITE_ERROR);
}

static void test_prepare_null_sql(void) {
  arkilian *db = open_test_db();
  int rc = db_prepare(db, NULL);
  assert(rc == SQLITE_ERROR);
  db_close(db);
  cleanup_files();
}

static void test_prepare_captures_error_msg(void) {
  arkilian *db = open_test_db();
  db_prepare(db, "SELECT * FROM ghost_table");
  const char *msg = db_errmsg(db);
  assert(msg != NULL);
  assert(strstr(msg, "no such table") != NULL);
  db_close(db);
  cleanup_files();
}

static void test_step_returns_row(void) {
  arkilian *db = open_test_db();
  db_exec(db, "CREATE TABLE t1 (id INTEGER PRIMARY KEY, val TEXT)");
  db_exec(db, "INSERT INTO t1 (val) VALUES ('row1')");
  int rc = db_prepare(db, "SELECT val FROM t1");
  assert(rc == SQLITE_OK);
  rc = db_step(db);
  assert(rc == SQLITE_ROW);
  db_finalize(db);
  db_close(db);
  cleanup_files();
}

static void test_step_returns_done_when_empty(void) {
  arkilian *db = open_test_db();
  db_exec(db, "CREATE TABLE t1 (id INTEGER PRIMARY KEY)");
  int rc = db_prepare(db, "SELECT * FROM t1");
  assert(rc == SQLITE_OK);
  rc = db_step(db);
  assert(rc == SQLITE_DONE);
  db_finalize(db);
  db_close(db);
  cleanup_files();
}

static void test_step_null_db(void) {
  int rc = db_step(NULL);
  assert(rc == SQLITE_ERROR);
}

static void test_finalize_clears_stmt(void) {
  arkilian *db = open_test_db();
  db_exec(db, "CREATE TABLE t1 (id INTEGER PRIMARY KEY)");
  db_prepare(db, "SELECT * FROM t1");
  int rc = db_finalize(db);
  assert(rc == SQLITE_OK);
  // Step after finalize should fail (stmt is NULL now)
  rc = db_step(db);
  assert(rc == SQLITE_ERROR);
  db_close(db);
  cleanup_files();
}

static void test_finalize_null_db(void) {
  int rc = db_finalize(NULL);
  assert(rc == SQLITE_ERROR);
}

static void test_reset_allows_re_step(void) {
  arkilian *db = open_test_db();
  db_exec(db, "CREATE TABLE t1 (id INTEGER PRIMARY KEY, val TEXT)");
  db_exec(db, "INSERT INTO t1 (val) VALUES ('a')");
  db_prepare(db, "SELECT val FROM t1");
  int rc = db_step(db);
  assert(rc == SQLITE_ROW);
  rc = db_step(db);
  assert(rc == SQLITE_DONE);
  rc = db_reset(db);
  assert(rc == SQLITE_OK);
  rc = db_step(db);
  assert(rc == SQLITE_ROW);
  db_finalize(db);
  db_close(db);
  cleanup_files();
}

static void test_reset_null_db(void) {
  int rc = db_reset(NULL);
  assert(rc == SQLITE_ERROR);
}

// ---------------------------------------------------------------------------
// Multi-Statement Tests (the bug fix)
// ---------------------------------------------------------------------------

static void test_two_prepares_both_accessible(void) {
  // Two prepares back-to-back — both remain live and steppable.
  arkilian *db = open_test_db();
  db_exec(db, "CREATE TABLE alpha (x INT)");
  db_exec(db, "CREATE TABLE beta (y TEXT)");
  db_exec(db, "INSERT INTO alpha VALUES (100)");
  db_exec(db, "INSERT INTO beta VALUES ('second')");

  db_prepare(db, "SELECT x FROM alpha");  // index 0
  db_prepare(db, "SELECT y FROM beta");   // index 1, now current

  assert(db_stmt_count(db) == 2);

  // Current is second — step it
  int rc = db_step(db);
  assert(rc == SQLITE_ROW);
  const char *val = db_column_text(db, 0);
  assert(val != NULL && strcmp(val, "second") == 0);
  assert(db_column_count(db) == 1);
  const char *col_name = db_column_name(db, 0);
  assert(col_name != NULL && strcmp(col_name, "y") == 0);
  assert(db_step(db) == SQLITE_DONE);

  // Switch to first — it was NOT lost
  rc = db_use_stmt(db, 0);
  assert(rc == SQLITE_OK);
  rc = db_step(db);
  assert(rc == SQLITE_ROW);
  assert(db_column_int(db, 0) == 100);
  assert(db_step(db) == SQLITE_DONE);

  db_use_stmt(db, 0);
  db_finalize(db);
  db_use_stmt(db, 1);
  db_finalize(db);
  db_close(db);
  cleanup_files();
}

static void test_many_prepares_all_survive(void) {
  // Fire 5 prepares back-to-back — ALL remain accessible.
  arkilian *db = open_test_db();
  db_exec(db, "CREATE TABLE m (id INT, label TEXT)");
  db_exec(db, "INSERT INTO m VALUES (1, 'first')");
  db_exec(db, "INSERT INTO m VALUES (2, 'second')");
  db_exec(db, "INSERT INTO m VALUES (3, 'third')");
  db_exec(db, "INSERT INTO m VALUES (4, 'fourth')");
  db_exec(db, "INSERT INTO m VALUES (5, 'fifth')");

  db_prepare(db, "SELECT label FROM m WHERE id = 1"); // index 0
  db_prepare(db, "SELECT label FROM m WHERE id = 2"); // index 1
  db_prepare(db, "SELECT label FROM m WHERE id = 3"); // index 2
  db_prepare(db, "SELECT label FROM m WHERE id = 4"); // index 3
  db_prepare(db, "SELECT label FROM m WHERE id = 5"); // index 4

  assert(db_stmt_count(db) == 5);

  // Verify each statement independently
  const char *expected[] = {"first", "second", "third", "fourth", "fifth"};
  for (int i = 0; i < 5; i++) {
    int rc = db_use_stmt(db, i);
    assert(rc == SQLITE_OK);
    rc = db_step(db);
    assert(rc == SQLITE_ROW);
    const char *v = db_column_text(db, 0);
    assert(v != NULL && strcmp(v, expected[i]) == 0);
    assert(db_step(db) == SQLITE_DONE);
  }

  // Finalize all
  for (int i = 0; i < 5; i++) {
    db_use_stmt(db, i);
    db_finalize(db);
  }
  db_close(db);
  cleanup_files();
}

static void test_prepare_pool_no_leak(void) {
  // Hammer db_prepare in a loop — all statements remain valid.
  arkilian *db = open_test_db();
  db_exec(db, "CREATE TABLE stress (v INT)");
  db_exec(db, "INSERT INTO stress VALUES (7)");

  for (int i = 0; i < 100; i++) {
    int rc = db_prepare(db, "SELECT v FROM stress");
    assert(rc == SQLITE_OK);
  }
  assert(db_stmt_count(db) == 100);

  // Step the first and last to prove they're both alive
  db_use_stmt(db, 0);
  assert(db_step(db) == SQLITE_ROW);
  assert(db_column_int(db, 0) == 7);

  db_use_stmt(db, 99);
  assert(db_step(db) == SQLITE_ROW);
  assert(db_column_int(db, 0) == 7);

  // db_close finalizes them all
  db_close(db);
  cleanup_files();
}

static void test_prepare_step_prepare_step_no_loss(void) {
  // Interleave: prepare+step (partial), then prepare+step again.
  // The first result set is NOT abandoned — we can go back to it.
  arkilian *db = open_test_db();
  db_exec(db, "CREATE TABLE t (n INT)");
  db_exec(db, "INSERT INTO t VALUES (10)");
  db_exec(db, "INSERT INTO t VALUES (20)");
  db_exec(db, "INSERT INTO t VALUES (30)");

  // Start iterating first query (index 0)
  db_prepare(db, "SELECT n FROM t ORDER BY n");
  assert(db_step(db) == SQLITE_ROW);
  assert(db_column_int(db, 0) == 10);

  // Prepare a second query (index 1, now current)
  db_prepare(db, "SELECT n FROM t WHERE n = 30");
  assert(db_step(db) == SQLITE_ROW);
  assert(db_column_int(db, 0) == 30);
  assert(db_step(db) == SQLITE_DONE);

  // Go back to first query — it should still be mid-iteration
  db_use_stmt(db, 0);
  assert(db_step(db) == SQLITE_ROW);
  assert(db_column_int(db, 0) == 20);
  assert(db_step(db) == SQLITE_ROW);
  assert(db_column_int(db, 0) == 30);
  assert(db_step(db) == SQLITE_DONE);

  db_use_stmt(db, 0);
  db_finalize(db);
  db_use_stmt(db, 1);
  db_finalize(db);
  db_close(db);
  cleanup_files();
}

static void test_use_stmt_invalid_index(void) {
  arkilian *db = open_test_db();
  assert(db_use_stmt(db, -1) == SQLITE_ERROR);
  assert(db_use_stmt(db, 0) == SQLITE_ERROR); // no stmts yet
  db_prepare(db, "SELECT 1");
  assert(db_use_stmt(db, 1) == SQLITE_ERROR); // out of bounds
  assert(db_use_stmt(db, 0) == SQLITE_OK);    // valid
  db_finalize(db);
  db_close(db);
  cleanup_files();
}

static void test_use_stmt_after_finalize(void) {
  arkilian *db = open_test_db();
  db_exec(db, "CREATE TABLE t (v INT)");
  db_exec(db, "INSERT INTO t VALUES (1)");
  db_prepare(db, "SELECT v FROM t"); // index 0
  db_prepare(db, "SELECT v FROM t"); // index 1
  // Finalize index 1 (current)
  db_finalize(db);
  // Trying to use a finalized slot should fail
  assert(db_use_stmt(db, 1) == SQLITE_ERROR);
  // But index 0 is still fine
  assert(db_use_stmt(db, 0) == SQLITE_OK);
  assert(db_step(db) == SQLITE_ROW);
  db_finalize(db);
  db_close(db);
  cleanup_files();
}

static void test_stmt_count(void) {
  arkilian *db = open_test_db();
  assert(db_stmt_count(db) == 0);
  db_prepare(db, "SELECT 1");
  assert(db_stmt_count(db) == 1);
  db_prepare(db, "SELECT 2");
  assert(db_stmt_count(db) == 2);
  db_prepare(db, "SELECT 3");
  assert(db_stmt_count(db) == 3);
  db_close(db);
  cleanup_files();
}

static void test_stmt_count_null_db(void) {
  assert(db_stmt_count(NULL) == 0);
}

// ---------------------------------------------------------------------------
// Column Access Tests
// ---------------------------------------------------------------------------

static void test_column_count(void) {
  arkilian *db = open_test_db();
  db_exec(db, "CREATE TABLE t1 (a INT, b TEXT, c REAL)");
  db_exec(db, "INSERT INTO t1 VALUES (1, 'hi', 3.14)");
  db_prepare(db, "SELECT a, b, c FROM t1");
  db_step(db);
  int count = db_column_count(db);
  assert(count == 3);
  db_finalize(db);
  db_close(db);
  cleanup_files();
}

static void test_column_count_no_stmt(void) {
  arkilian *db = open_test_db();
  int count = db_column_count(db);
  assert(count == 0);
  db_close(db);
  cleanup_files();
}

static void test_column_name(void) {
  arkilian *db = open_test_db();
  db_exec(db, "CREATE TABLE t1 (alpha INT, beta TEXT)");
  db_exec(db, "INSERT INTO t1 VALUES (1, 'x')");
  db_prepare(db, "SELECT alpha, beta FROM t1");
  db_step(db);
  const char *name0 = db_column_name(db, 0);
  const char *name1 = db_column_name(db, 1);
  assert(name0 != NULL && strcmp(name0, "alpha") == 0);
  assert(name1 != NULL && strcmp(name1, "beta") == 0);
  db_finalize(db);
  db_close(db);
  cleanup_files();
}

static void test_column_name_null_db(void) {
  const char *name = db_column_name(NULL, 0);
  assert(name == NULL);
}

static void test_column_text(void) {
  arkilian *db = open_test_db();
  db_exec(db, "CREATE TABLE t1 (val TEXT)");
  db_exec(db, "INSERT INTO t1 VALUES ('hello world')");
  db_prepare(db, "SELECT val FROM t1");
  db_step(db);
  const char *text = db_column_text(db, 0);
  assert(text != NULL && strcmp(text, "hello world") == 0);
  db_finalize(db);
  db_close(db);
  cleanup_files();
}

static void test_column_text_null_db(void) {
  const char *text = db_column_text(NULL, 0);
  assert(text == NULL);
}

static void test_column_int(void) {
  arkilian *db = open_test_db();
  db_exec(db, "CREATE TABLE t1 (val INT)");
  db_exec(db, "INSERT INTO t1 VALUES (42)");
  db_prepare(db, "SELECT val FROM t1");
  db_step(db);
  int val = db_column_int(db, 0);
  assert(val == 42);
  db_finalize(db);
  db_close(db);
  cleanup_files();
}

static void test_column_int_null_db(void) {
  int val = db_column_int(NULL, 0);
  assert(val == 0);
}

static void test_column_double(void) {
  arkilian *db = open_test_db();
  db_exec(db, "CREATE TABLE t1 (val REAL)");
  db_exec(db, "INSERT INTO t1 VALUES (3.14159)");
  db_prepare(db, "SELECT val FROM t1");
  db_step(db);
  double val = db_column_double(db, 0);
  assert(fabs(val - 3.14159) < 0.0001);
  db_finalize(db);
  db_close(db);
  cleanup_files();
}

static void test_column_double_null_db(void) {
  double val = db_column_double(NULL, 0);
  assert(val == 0.0);
}

// ---------------------------------------------------------------------------
// Bind Parameter Tests
// ---------------------------------------------------------------------------

static void test_bind_text(void) {
  arkilian *db = open_test_db();
  db_exec(db, "CREATE TABLE t1 (val TEXT)");
  db_prepare(db, "INSERT INTO t1 VALUES (?)");
  int rc = db_bind_text(db, 1, "bound_value");
  assert(rc == SQLITE_OK);
  rc = db_step(db);
  assert(rc == SQLITE_DONE);
  db_finalize(db);
  db_prepare(db, "SELECT val FROM t1");
  db_step(db);
  const char *text = db_column_text(db, 0);
  assert(text != NULL && strcmp(text, "bound_value") == 0);
  db_finalize(db);
  db_close(db);
  cleanup_files();
}

static void test_bind_text_null_db(void) {
  int rc = db_bind_text(NULL, 1, "x");
  assert(rc == SQLITE_ERROR);
}

static void test_bind_text_null_val(void) {
  arkilian *db = open_test_db();
  db_exec(db, "CREATE TABLE t1 (val TEXT)");
  db_prepare(db, "INSERT INTO t1 VALUES (?)");
  int rc = db_bind_text(db, 1, NULL);
  assert(rc == SQLITE_ERROR);
  db_finalize(db);
  db_close(db);
  cleanup_files();
}

static void test_bind_int(void) {
  arkilian *db = open_test_db();
  db_exec(db, "CREATE TABLE t1 (val INT)");
  db_prepare(db, "INSERT INTO t1 VALUES (?)");
  int rc = db_bind_int(db, 1, 99);
  assert(rc == SQLITE_OK);
  db_step(db);
  db_finalize(db);
  db_prepare(db, "SELECT val FROM t1");
  db_step(db);
  int val = db_column_int(db, 0);
  assert(val == 99);
  db_finalize(db);
  db_close(db);
  cleanup_files();
}

static void test_bind_int_null_db(void) {
  int rc = db_bind_int(NULL, 1, 5);
  assert(rc == SQLITE_ERROR);
}

static void test_bind_double(void) {
  arkilian *db = open_test_db();
  db_exec(db, "CREATE TABLE t1 (val REAL)");
  db_prepare(db, "INSERT INTO t1 VALUES (?)");
  int rc = db_bind_double(db, 1, 2.718);
  assert(rc == SQLITE_OK);
  db_step(db);
  db_finalize(db);
  db_prepare(db, "SELECT val FROM t1");
  db_step(db);
  double val = db_column_double(db, 0);
  assert(fabs(val - 2.718) < 0.001);
  db_finalize(db);
  db_close(db);
  cleanup_files();
}

static void test_bind_double_null_db(void) {
  int rc = db_bind_double(NULL, 1, 1.0);
  assert(rc == SQLITE_ERROR);
}

// ---------------------------------------------------------------------------
// Token Management Tests
// ---------------------------------------------------------------------------

static void test_set_token(void) {
  arkilian *db = open_test_db();
  int rc = db_set_api_key(db, "my-secret-api-key");
  assert(rc == 0);
  db_close(db);
  cleanup_files();
}

static void test_set_token_null_db(void) {
  int rc = db_set_api_key(NULL, "api_key");
  assert(rc == 1);
}

static void test_set_token_null_token(void) {
  arkilian *db = open_test_db();
  int rc = db_set_api_key(db, NULL);
  assert(rc == 1);
  db_close(db);
  cleanup_files();
}

static void test_set_token_replaces_previous(void) {
  arkilian *db = open_test_db();
  int rc = db_set_api_key(db, "first-key");
  assert(rc == 0);
  rc = db_set_api_key(db, "second-key");
  assert(rc == 0);
  db_close(db);
  cleanup_files();
}

// ---------------------------------------------------------------------------
// Integration Workflows
// ---------------------------------------------------------------------------

static void test_iterate_multiple_rows(void) {
  arkilian *db = open_test_db();
  db_exec(db, "CREATE TABLE t1 (id INT)");
  db_exec(db, "INSERT INTO t1 VALUES (1)");
  db_exec(db, "INSERT INTO t1 VALUES (2)");
  db_exec(db, "INSERT INTO t1 VALUES (3)");
  db_prepare(db, "SELECT id FROM t1 ORDER BY id");
  int row_count = 0;
  int sum = 0;
  while (db_step(db) == SQLITE_ROW) {
    sum += db_column_int(db, 0);
    row_count++;
  }
  assert(row_count == 3);
  assert(sum == 6);
  db_finalize(db);
  db_close(db);
  cleanup_files();
}

static void test_bind_and_insert_multiple(void) {
  arkilian *db = open_test_db();
  db_exec(db, "CREATE TABLE t1 (name TEXT, age INT)");
  db_prepare(db, "INSERT INTO t1 VALUES (?, ?)");
  db_bind_text(db, 1, "Alice");
  db_bind_int(db, 2, 30);
  assert(db_step(db) == SQLITE_DONE);
  db_reset(db);
  db_bind_text(db, 1, "Bob");
  db_bind_int(db, 2, 25);
  assert(db_step(db) == SQLITE_DONE);
  db_finalize(db);
  db_prepare(db, "SELECT name, age FROM t1 ORDER BY age");
  assert(db_step(db) == SQLITE_ROW);
  assert(strcmp(db_column_text(db, 0), "Bob") == 0);
  assert(db_column_int(db, 1) == 25);
  assert(db_step(db) == SQLITE_ROW);
  assert(strcmp(db_column_text(db, 0), "Alice") == 0);
  assert(db_column_int(db, 1) == 30);
  assert(db_step(db) == SQLITE_DONE);
  db_finalize(db);
  db_close(db);
  cleanup_files();
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

int main(void) {
  printf("=== Arkilian Wrapper Tests ===\n\n");

  printf("[Lifecycle]\n");
  RUN_TEST(test_init_creates_db);
  RUN_TEST(test_init_null_ptr_returns_error);
  RUN_TEST(test_init_null_filename_uses_default);
  RUN_TEST(test_close_null_is_safe);
  RUN_TEST(test_get_handle_returns_valid);

  printf("\n[Error Messages]\n");
  RUN_TEST(test_errmsg_no_error);
  RUN_TEST(test_errmsg_after_bad_sql);

  printf("\n[db_exec]\n");
  RUN_TEST(test_exec_create_table);
  RUN_TEST(test_exec_insert);
  RUN_TEST(test_exec_invalid_sql_returns_error);
  RUN_TEST(test_exec_null_db_returns_error);
  RUN_TEST(test_exec_null_sql_returns_error);
  RUN_TEST(test_exec_captures_error_msg);

  printf("\n[Prepare / Step / Finalize]\n");
  RUN_TEST(test_prepare_valid_sql);
  RUN_TEST(test_prepare_invalid_sql);
  RUN_TEST(test_prepare_null_db);
  RUN_TEST(test_prepare_null_sql);
  RUN_TEST(test_prepare_captures_error_msg);
  RUN_TEST(test_step_returns_row);
  RUN_TEST(test_step_returns_done_when_empty);
  RUN_TEST(test_step_null_db);
  RUN_TEST(test_finalize_clears_stmt);
  RUN_TEST(test_finalize_null_db);
  RUN_TEST(test_reset_allows_re_step);
  RUN_TEST(test_reset_null_db);

  printf("\n[Multi-Statement (bug fix)]\n");
  RUN_TEST(test_two_prepares_both_accessible);
  RUN_TEST(test_many_prepares_all_survive);
  RUN_TEST(test_prepare_pool_no_leak);
  RUN_TEST(test_prepare_step_prepare_step_no_loss);
  RUN_TEST(test_use_stmt_invalid_index);
  RUN_TEST(test_use_stmt_after_finalize);
  RUN_TEST(test_stmt_count);
  RUN_TEST(test_stmt_count_null_db);

  printf("\n[Column Access]\n");
  RUN_TEST(test_column_count);
  RUN_TEST(test_column_count_no_stmt);
  RUN_TEST(test_column_name);
  RUN_TEST(test_column_name_null_db);
  RUN_TEST(test_column_text);
  RUN_TEST(test_column_text_null_db);
  RUN_TEST(test_column_int);
  RUN_TEST(test_column_int_null_db);
  RUN_TEST(test_column_double);
  RUN_TEST(test_column_double_null_db);

  printf("\n[Bind Parameters]\n");
  RUN_TEST(test_bind_text);
  RUN_TEST(test_bind_text_null_db);
  RUN_TEST(test_bind_text_null_val);
  RUN_TEST(test_bind_int);
  RUN_TEST(test_bind_int_null_db);
  RUN_TEST(test_bind_double);
  RUN_TEST(test_bind_double_null_db);

  printf("\n[Token Management]\n");
  RUN_TEST(test_set_token);
  RUN_TEST(test_set_token_null_db);
  RUN_TEST(test_set_token_null_token);
  RUN_TEST(test_set_token_replaces_previous);

  printf("\n[Integration Workflows]\n");
  RUN_TEST(test_iterate_multiple_rows);
  RUN_TEST(test_bind_and_insert_multiple);

  printf("\n=== Results: %d/%d passed ===\n", tests_passed, tests_run);
  return (tests_passed == tests_run) ? 0 : 1;
}
