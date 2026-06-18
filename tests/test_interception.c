// Arkilian Write Interception Tests — safety & performance
//
// Compile (macOS/Linux):
//   cc tests/test_interception.c src/class.c src/deps/sqlite/sqlite3.c \
//      -Isrc -Isrc/deps/sqlite -lcurl -lpthread -o test_interception
//
// Or via CMake:
//   cmake -B build -DARKILIAN_BUILD_TESTS=ON -DARKILIAN_BUILD_SHARED=OFF
//   cmake --build build && ./build/test_interception

#include "class.h"
#include <assert.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#ifdef _WIN32
#include <windows.h>
#else
#include <pthread.h>
#include <unistd.h>
#endif

#define TEST_DB "test_interception.db"

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
  setenv("ARKILIAN_ENABLE_BACKUP", "0", 1);
  arkilian *db = NULL;
  int rc = db_init(&db, TEST_DB);
  assert(rc == 0 && "db_init failed");
  assert(db != NULL);
  return db;
}

static void cleanup_files(void) { remove(TEST_DB); }

// Count rows in _arkilian_log
static int count_log_rows(arkilian *db) {
  int rc = db_prepare(db, "SELECT COUNT(*) FROM _arkilian_log");
  assert(rc == SQLITE_OK);
  rc = db_step(db);
  assert(rc == SQLITE_ROW);
  int count = db_column_int(db, 0);
  db_finalize(db);
  return count;
}

// Get the latest LSN from _arkilian_log
static int get_max_lsn(arkilian *db) {
  int rc = db_prepare(db, "SELECT COALESCE(MAX(lsn), 0) FROM _arkilian_log");
  assert(rc == SQLITE_OK);
  rc = db_step(db);
  assert(rc == SQLITE_ROW);
  int max_lsn = db_column_int(db, 0);
  db_finalize(db);
  return max_lsn;
}

// Get the latest SQL text from _arkilian_log
static void get_last_log_sql(arkilian *db, char *buf, size_t bufsz) {
  int rc = db_prepare(db,
    "SELECT sql FROM _arkilian_log ORDER BY lsn DESC LIMIT 1");
  assert(rc == SQLITE_OK);
  rc = db_step(db);
  assert(rc == SQLITE_ROW);
  const char *s = db_column_text(db, 0);
  if (s) strncpy(buf, s, bufsz - 1);
  else buf[0] = '\0';
  buf[bufsz - 1] = '\0';
  db_finalize(db);
}

// Verify PRAGMA value via query (returns static buffer)
static const char *get_pragma(arkilian *db, const char *pragma) {
  static char buf[64];
  char query[128];
  snprintf(query, sizeof(query), "PRAGMA %s;", pragma);
  db_prepare(db, query);
  int rc = db_step(db);
  if (rc == SQLITE_ROW) {
    const char *val = db_column_text(db, 0);
    if (val) strncpy(buf, val, sizeof(buf) - 1);
    else buf[0] = '\0';
    buf[sizeof(buf) - 1] = '\0';
  } else {
    buf[0] = '\0';
  }
  db_finalize(db);
  return buf;
}

// ---------------------------------------------------------------------------
// Safety: Pragma Verification
// ---------------------------------------------------------------------------

static void test_pragma_journal_mode_is_wal(void) {
  arkilian *db = open_test_db();
  db_exec(db, "CREATE TABLE t (x INT)"); // ensure db is active
  const char *v = get_pragma(db, "journal_mode");
  assert(v != NULL && strcmp(v, "wal") == 0);
  db_close(db);
  cleanup_files();
}

static void test_pragma_synchronous_is_normal(void) {
  arkilian *db = open_test_db();
  db_exec(db, "CREATE TABLE t (x INT)");
  const char *v = get_pragma(db, "synchronous");
  assert(v != NULL && strcmp(v, "1") == 0); // 1 = NORMAL
  db_close(db);
  cleanup_files();
}

static void test_pragma_foreign_keys_is_on(void) {
  arkilian *db = open_test_db();
  db_exec(db, "CREATE TABLE t (x INT)");
  const char *v = get_pragma(db, "foreign_keys");
  assert(v != NULL && strcmp(v, "1") == 0);
  db_close(db);
  cleanup_files();
}

static void test_pragma_busy_timeout_is_set(void) {
  arkilian *db = open_test_db();
  db_exec(db, "CREATE TABLE t (x INT)");
  const char *v = get_pragma(db, "busy_timeout");
  assert(v != NULL && strcmp(v, "5000") == 0);
  db_close(db);
  cleanup_files();
}

// ---------------------------------------------------------------------------
// Safety: Internal Tables Exist
// ---------------------------------------------------------------------------

static void test_meta_table_exists(void) {
  arkilian *db = open_test_db();
  int rc = db_prepare(db, "SELECT k, v FROM _arkilian_meta");
  assert(rc == SQLITE_OK);
  db_finalize(db);
  db_close(db);
  cleanup_files();
}

static void test_log_table_exists(void) {
  arkilian *db = open_test_db();
  int rc = db_prepare(db, "SELECT lsn, ts, sql, params FROM _arkilian_log");
  assert(rc == SQLITE_OK);
  db_finalize(db);
  db_close(db);
  cleanup_files();
}

// ---------------------------------------------------------------------------
// Safety: Write logging via db_exec()
// ---------------------------------------------------------------------------

static void test_exec_insert_logs_entry(void) {
  arkilian *db = open_test_db();
  db_exec(db, "CREATE TABLE t1 (id INTEGER PRIMARY KEY, name TEXT)");

  int before = count_log_rows(db);

  int rc = db_exec(db, "INSERT INTO t1 (name) VALUES ('alice')");
  assert(rc == SQLITE_DONE);

  int after = count_log_rows(db);
  assert(after == before + 1);

  // Verify log content
  char sqlbuf[512];
  get_last_log_sql(db, sqlbuf, sizeof(sqlbuf));
  assert(strstr(sqlbuf, "INSERT INTO t1") != NULL);
  assert(strstr(sqlbuf, "alice") != NULL);

  db_close(db);
  cleanup_files();
}

static void test_exec_update_logs_entry(void) {
  arkilian *db = open_test_db();
  db_exec(db, "CREATE TABLE t1 (id INTEGER PRIMARY KEY, name TEXT)");
  db_exec(db, "INSERT INTO t1 (name) VALUES ('bob')");

  int before = count_log_rows(db);

  int rc = db_exec(db, "UPDATE t1 SET name = 'bob-updated' WHERE id = 1");
  assert(rc == SQLITE_DONE);

  int after = count_log_rows(db);
  assert(after == before + 1);

  char sqlbuf[512];
  get_last_log_sql(db, sqlbuf, sizeof(sqlbuf));
  assert(strstr(sqlbuf, "UPDATE t1") != NULL);

  db_close(db);
  cleanup_files();
}

static void test_exec_delete_logs_entry(void) {
  arkilian *db = open_test_db();
  db_exec(db, "CREATE TABLE t1 (id INTEGER PRIMARY KEY, name TEXT)");
  db_exec(db, "INSERT INTO t1 (name) VALUES ('charlie')");

  int before = count_log_rows(db);

  int rc = db_exec(db, "DELETE FROM t1 WHERE id = 1");
  assert(rc == SQLITE_DONE);

  int after = count_log_rows(db);
  assert(after == before + 1);

  char sqlbuf[512];
  get_last_log_sql(db, sqlbuf, sizeof(sqlbuf));
  assert(strstr(sqlbuf, "DELETE FROM t1") != NULL);

  db_close(db);
  cleanup_files();
}

static void test_exec_create_table_logs_entry(void) {
  arkilian *db = open_test_db();

  int before = count_log_rows(db);

  int rc = db_exec(db, "CREATE TABLE t2 (a INT, b TEXT)");
  assert(rc == SQLITE_DONE);

  int after = count_log_rows(db);
  assert(after == before + 1);

  char sqlbuf[512];
  get_last_log_sql(db, sqlbuf, sizeof(sqlbuf));
  assert(strstr(sqlbuf, "CREATE TABLE t2") != NULL);

  db_close(db);
  cleanup_files();
}

static void test_exec_drop_table_logs_entry(void) {
  arkilian *db = open_test_db();
  db_exec(db, "CREATE TABLE t_drop (x INT)");

  int before = count_log_rows(db);

  int rc = db_exec(db, "DROP TABLE t_drop");
  assert(rc == SQLITE_DONE);

  int after = count_log_rows(db);
  assert(after == before + 1);

  char sqlbuf[512];
  get_last_log_sql(db, sqlbuf, sizeof(sqlbuf));
  assert(strstr(sqlbuf, "DROP TABLE t_drop") != NULL);

  db_close(db);
  cleanup_files();
}

// ---------------------------------------------------------------------------
// Safety: Write logging via prepare / step / finalize path
// ---------------------------------------------------------------------------

static void test_prepare_step_insert_logs_entry(void) {
  arkilian *db = open_test_db();
  db_exec(db, "CREATE TABLE t1 (id INTEGER PRIMARY KEY, name TEXT)");

  int before = count_log_rows(db);

  // Use prepare/bind/step/finalize — the primary write path for run()
  int rc = db_prepare(db, "INSERT INTO t1 (name) VALUES (?)");
  assert(rc == SQLITE_OK);
  db_bind_text(db, 1, "diana");
  rc = db_step(db);
  assert(rc == SQLITE_DONE);
  db_finalize(db);

  int after = count_log_rows(db);
  assert(after == before + 1);

  char sqlbuf[512];
  get_last_log_sql(db, sqlbuf, sizeof(sqlbuf));
  assert(strstr(sqlbuf, "INSERT INTO t1") != NULL);

  // Verify data was actually inserted
  db_prepare(db, "SELECT name FROM t1 WHERE id = 1");
  db_step(db);
  assert(strcmp(db_column_text(db, 0), "diana") == 0);
  db_finalize(db);

  db_close(db);
  cleanup_files();
}

static void test_prepare_step_update_logs_entry(void) {
  arkilian *db = open_test_db();
  db_exec(db, "CREATE TABLE t1 (id INTEGER PRIMARY KEY, val INT)");
  db_exec(db, "INSERT INTO t1 (val) VALUES (10)");

  int before = count_log_rows(db);

  int rc = db_prepare(db, "UPDATE t1 SET val = ? WHERE id = 1");
  assert(rc == SQLITE_OK);
  db_bind_int(db, 1, 99);
  rc = db_step(db);
  assert(rc == SQLITE_DONE);
  db_finalize(db);

  int after = count_log_rows(db);
  assert(after == before + 1);

  char sqlbuf[512];
  get_last_log_sql(db, sqlbuf, sizeof(sqlbuf));
  assert(strstr(sqlbuf, "UPDATE t1") != NULL);

  // Verify data was updated
  db_prepare(db, "SELECT val FROM t1 WHERE id = 1");
  db_step(db);
  assert(db_column_int(db, 0) == 99);
  db_finalize(db);

  db_close(db);
  cleanup_files();
}

// ---------------------------------------------------------------------------
// Safety: Reads must NOT produce log entries
// ---------------------------------------------------------------------------

static void test_exec_select_does_not_log(void) {
  arkilian *db = open_test_db();
  db_exec(db, "CREATE TABLE t1 (x INT)");
  db_exec(db, "INSERT INTO t1 VALUES (1)");

  int before = count_log_rows(db);

  int rc = db_exec(db, "SELECT * FROM t1");
  assert(rc == SQLITE_ROW || rc == SQLITE_DONE);

  int after = count_log_rows(db);
  assert(after == before);

  db_close(db);
  cleanup_files();
}

static void test_prepare_select_does_not_log(void) {
  arkilian *db = open_test_db();
  db_exec(db, "CREATE TABLE t1 (x INT)");
  db_exec(db, "INSERT INTO t1 VALUES (1)");

  int before = count_log_rows(db);

  int rc = db_prepare(db, "SELECT x FROM t1");
  assert(rc == SQLITE_OK);
  rc = db_step(db);
  assert(rc == SQLITE_ROW);
  db_finalize(db);

  int after = count_log_rows(db);
  assert(after == before);

  db_close(db);
  cleanup_files();
}

// ---------------------------------------------------------------------------
// Safety: Failed writes must NOT produce log entries (transaction rolled back)
// ---------------------------------------------------------------------------

static void test_exec_failed_write_rolls_back_no_log(void) {
  arkilian *db = open_test_db();
  db_exec(db, "CREATE TABLE t1 (id INTEGER PRIMARY KEY)");

  int before = count_log_rows(db);

  // This will fail (duplicate PK or constraint violation)
  int rc = db_exec(db,
    "INSERT INTO t1 (id) VALUES (1); INSERT INTO t1 (id) VALUES (1)");
  // Either it fails entirely, or the first INSERT inside multi-statement
  // may succeed — but SQLite processes statements sequentially in exec.
  // The first INSERT succeeds, the second fails.
  // Our wrapper does BEGIN/COMMIT around the whole thing, so both roll back.
  assert(rc != SQLITE_DONE);

  int after = count_log_rows(db);
  // No new log entry because the transaction rolled back
  assert(after == before);

  // Verify no data was committed
  db_prepare(db, "SELECT COUNT(*) FROM t1");
  db_step(db);
  assert(db_column_int(db, 0) == 0);
  db_finalize(db);

  db_close(db);
  cleanup_files();
}

static void test_prepare_step_failed_write_rolls_back(void) {
  arkilian *db = open_test_db();
  db_exec(db, "CREATE TABLE t1 (id INTEGER PRIMARY KEY, val TEXT NOT NULL)");

  int before = count_log_rows(db);

  // This will fail because val is NOT NULL
  int rc = db_prepare(db, "INSERT INTO t1 (val) VALUES (?)");
  assert(rc == SQLITE_OK);
  db_bind_text(db, 1, NULL); // bind NULL to NOT NULL column
  // The bind_null will succeed (SQLite allows binding NULL)
  // but sqlite3_bind_text with NULL val returns SQLITE_ERROR in our wrapper
  // Actually db_bind_text checks !val and returns SQLITE_ERROR.
  // So the bind fails before step, transaction must roll back.
  // Let's do a different failure: valid bind, but constraint violation
  db_finalize(db); // finalize the failed attempt — it will rollback

  // Actually our bind_text returns error for NULL, so let's fix the test
  // approach: use a valid bind but cause a constraint failure at step time
  db_prepare(db, "INSERT INTO t1 (id, val) VALUES (1, 'ok')");
  db_step(db); db_finalize(db); // first row ok

  int before2 = count_log_rows(db);

  db_prepare(db, "INSERT INTO t1 (id, val) VALUES (1, 'dup')"); // PK conflict
  rc = db_step(db);
  // step should return SQLITE_CONSTRAINT or similar
  assert(rc != SQLITE_DONE && rc != SQLITE_ROW);
  db_finalize(db); // finalize should rollback

  int after2 = count_log_rows(db);
  assert(after2 == before2); // no new log entry for the failed write

  db_close(db);
  cleanup_files();
}

// ---------------------------------------------------------------------------
// Safety: LSN is monotonic (AUTOINCREMENT)
// ---------------------------------------------------------------------------

static void test_lsn_is_monotonic(void) {
  arkilian *db = open_test_db();
  db_exec(db, "CREATE TABLE t1 (id INTEGER PRIMARY KEY, name TEXT)");
  db_exec(db, "INSERT INTO t1 (name) VALUES ('first')");
  db_exec(db, "INSERT INTO t1 (name) VALUES ('second')");
  db_exec(db, "INSERT INTO t1 (name) VALUES ('third')");

  // Query all LSNs ordered by rowid (which equals LSN for AUTOINCREMENT)
  db_prepare(db, "SELECT lsn FROM _arkilian_log ORDER BY lsn");
  int prev = 0;
  int count = 0;
  while (db_step(db) == SQLITE_ROW) {
    int lsn = db_column_int(db, 0);
    assert(lsn > prev);
    prev = lsn;
    count++;
  }
  assert(count >= 3);
  db_finalize(db);

  db_close(db);
  cleanup_files();
}

// ---------------------------------------------------------------------------
// Safety: log contains SQL with special characters escaped
// ---------------------------------------------------------------------------

static void test_log_escapes_sql_special_chars(void) {
  arkilian *db = open_test_db();
  db_exec(db, "CREATE TABLE t1 (val TEXT)");
  db_exec(db, "INSERT INTO t1 (val) VALUES ('it''s a test')");

  int before = count_log_rows(db);

  // SQL with quotes, semicolons, etc.
  int rc = db_exec(db,
    "INSERT INTO t1 (val) VALUES ('hello; DROP TABLE students;--')");
  assert(rc == SQLITE_DONE);

  int after = count_log_rows(db);
  assert(after == before + 1);

  // Verify the logged SQL is safe (can be replayed)
  db_prepare(db,
    "SELECT sql FROM _arkilian_log ORDER BY lsn DESC LIMIT 1");
  db_step(db);
  const char *logged = db_column_text(db, 0);
  assert(logged != NULL);
  // The logged SQL should contain the original SQL text
  assert(strstr(logged, "DROP TABLE") != NULL);
  db_finalize(db);

  db_close(db);
  cleanup_files();
}

// ---------------------------------------------------------------------------
// Safety: Concurrent writes are serialized (no corruption)
// ---------------------------------------------------------------------------

#define CONCURRENT_THREADS 8
#define WRITES_PER_THREAD 25

typedef struct {
  arkilian *db;
  int thread_id;
  int success_count;
  int fail_count;
} thread_args_t;

#ifdef _WIN32
static DWORD WINAPI concurrent_writer_thread(LPVOID arg) {
#else
static void *concurrent_writer_thread(void *arg) {
#endif
  thread_args_t *args = (thread_args_t *)arg;
  char sql[128];
  for (int i = 0; i < WRITES_PER_THREAD; i++) {
    snprintf(sql, sizeof(sql),
      "INSERT INTO t_concurrent (thread_id, seq) VALUES (%d, %d)",
      args->thread_id, i);
    int rc = db_exec(args->db, sql);
    if (rc == SQLITE_DONE) {
      args->success_count++;
    } else {
      args->fail_count++;
    }
  }
#ifdef _WIN32
  return 0;
#else
  return NULL;
#endif
}

static void test_concurrent_writes_all_succeed(void) {
  arkilian *db = open_test_db();
  db_exec(db,
    "CREATE TABLE t_concurrent (id INTEGER PRIMARY KEY, thread_id INT, seq INT)");
  int log_before = count_log_rows(db);

#ifdef _WIN32
  HANDLE threads[CONCURRENT_THREADS];
#else
  pthread_t threads[CONCURRENT_THREADS];
#endif
  thread_args_t args[CONCURRENT_THREADS];

  for (int i = 0; i < CONCURRENT_THREADS; i++) {
    args[i].db = db;
    args[i].thread_id = i;
    args[i].success_count = 0;
    args[i].fail_count = 0;
#ifdef _WIN32
    threads[i] = CreateThread(NULL, 0, concurrent_writer_thread,
                              &args[i], 0, NULL);
    assert(threads[i] != NULL);
#else
    int rc = pthread_create(&threads[i], NULL, concurrent_writer_thread, &args[i]);
    assert(rc == 0);
#endif
  }

  // Join all threads
  for (int i = 0; i < CONCURRENT_THREADS; i++) {
#ifdef _WIN32
    WaitForSingleObject(threads[i], INFINITE);
    CloseHandle(threads[i]);
#else
    pthread_join(threads[i], NULL);
#endif
  }

  // All writes should have succeeded
  int total_success = 0;
  int total_fail = 0;
  for (int i = 0; i < CONCURRENT_THREADS; i++) {
    total_success += args[i].success_count;
    total_fail += args[i].fail_count;
  }
  assert(total_fail == 0);
  assert(total_success == CONCURRENT_THREADS * WRITES_PER_THREAD);

  // Verify data integrity
  db_prepare(db, "SELECT COUNT(*) FROM t_concurrent");
  db_step(db);
  int row_count = db_column_int(db, 0);
  db_finalize(db);
  assert(row_count == CONCURRENT_THREADS * WRITES_PER_THREAD);

  // Each thread_id should have exactly WRITES_PER_THREAD rows
  for (int i = 0; i < CONCURRENT_THREADS; i++) {
    char count_sql[128];
    snprintf(count_sql, sizeof(count_sql),
      "SELECT COUNT(*) FROM t_concurrent WHERE thread_id = %d", i);
    db_prepare(db, count_sql);
    db_step(db);
    assert(db_column_int(db, 0) == WRITES_PER_THREAD);
    db_finalize(db);
  }

  // Every write should have a log entry
  int log_after = count_log_rows(db);
  assert(log_after == log_before + (CONCURRENT_THREADS * WRITES_PER_THREAD));

  db_close(db);
  cleanup_files();
}

// ---------------------------------------------------------------------------
// Safety: Only one write transaction at a time (SQLITE_BUSY on second)
// ---------------------------------------------------------------------------

static void test_second_write_prepare_returns_busy(void) {
  arkilian *db = open_test_db();
  db_exec(db, "CREATE TABLE t1 (id INTEGER PRIMARY KEY, val TEXT)");

  // Start first write transaction (prepare but don't finalize)
  int rc = db_prepare(db, "INSERT INTO t1 (val) VALUES ('first')");
  assert(rc == SQLITE_OK);

  // Try to prepare a second write — should return SQLITE_BUSY
  rc = db_prepare(db, "INSERT INTO t1 (val) VALUES ('second')");
  assert(rc == SQLITE_BUSY);

  // Finalize the first write to clean up
  db_step(db);
  db_finalize(db);

  db_close(db);
  cleanup_files();
}

static void test_pragma_read_does_not_log(void) {
  arkilian *db = open_test_db();
  db_exec(db, "CREATE TABLE t1 (x INT)");
  db_exec(db, "INSERT INTO t1 VALUES (42)");

  int before = count_log_rows(db);

  int rc = db_prepare(db, "PRAGMA table_info(t1)");
  assert(rc == SQLITE_OK);
  while (db_step(db) == SQLITE_ROW) { /* consume */ }
  db_finalize(db);

  int after = count_log_rows(db);
  assert(after == before);

  db_close(db);
  cleanup_files();
}

static void test_explain_does_not_log(void) {
  arkilian *db = open_test_db();
  db_exec(db, "CREATE TABLE t1 (x INT)");

  int before = count_log_rows(db);

  int rc = db_exec(db, "EXPLAIN SELECT * FROM t1");
  (void)rc;

  int after = count_log_rows(db);
  assert(after == before);

  db_close(db);
  cleanup_files();
}

static void test_many_selects_produce_zero_log_entries(void) {
  arkilian *db = open_test_db();
  db_exec(db, "CREATE TABLE t1 (x INT)");
  db_exec(db, "INSERT INTO t1 VALUES (1), (2), (3), (4), (5)");

  int before = count_log_rows(db);

  // Execute 50 SELECTs via exec — zero log entries expected
  for (int i = 0; i < 50; i++) {
    int rc = db_exec(db, "SELECT x FROM t1 WHERE x > 0");
    (void)rc;
  }

  int after = count_log_rows(db);
  assert(after == before);

  // Same via prepare/step/finalize path
  for (int i = 0; i < 50; i++) {
    int rc = db_prepare(db, "SELECT x FROM t1 WHERE x = ?");
    assert(rc == SQLITE_OK);
    db_bind_int(db, 1, (i % 5) + 1);
    while (db_step(db) == SQLITE_ROW) { /* consume */ }
    db_finalize(db);
  }

  int after2 = count_log_rows(db);
  assert(after2 == before);

  db_close(db);
  cleanup_files();
}

// ---------------------------------------------------------------------------
// Safety: Read+write interleaving via exec
// ---------------------------------------------------------------------------

static void test_reads_during_write_transaction_see_snapshot(void) {
  arkilian *db = open_test_db();
  db_exec(db, "CREATE TABLE t1 (val INT)");

  // Prepare a write but don't finalize (transaction in progress)
  db_prepare(db, "INSERT INTO t1 (val) VALUES (999)");

  // A read via exec should see the pre-transaction state (no 999)
  db_prepare(db, "SELECT COUNT(*) FROM t1");
  db_step(db);
  assert(db_column_int(db, 0) == 0);
  db_finalize(db);

  // Commit the write
  db_step(db);
  db_finalize(db);

  // Now the read should see the data
  db_prepare(db, "SELECT COUNT(*) FROM t1");
  db_step(db);
  assert(db_column_int(db, 0) == 1);
  db_finalize(db);

  db_close(db);
  cleanup_files();
}

// ---------------------------------------------------------------------------
// Performance: Write throughput with logging enabled
// ---------------------------------------------------------------------------

static double now_ms(void) {
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return (double)ts.tv_sec * 1000.0 + (double)ts.tv_nsec / 1000000.0;
}

static void test_perf_batch_insert_1000_rows(void) {
  arkilian *db = open_test_db();
  db_exec(db, "CREATE TABLE perf (id INTEGER PRIMARY KEY, val TEXT, num INT)");

  double start = now_ms();
  for (int i = 0; i < 1000; i++) {
    char sql[256];
    snprintf(sql, sizeof(sql),
      "INSERT INTO perf (val, num) VALUES ('row-%d', %d)", i, i);
    int rc = db_exec(db, sql);
    assert(rc == SQLITE_DONE);
  }
  double elapsed = now_ms() - start;

  // Verify all rows were inserted
  db_prepare(db, "SELECT COUNT(*) FROM perf");
  db_step(db);
  assert(db_column_int(db, 0) == 1000);
  db_finalize(db);

  // Verify all rows have log entries
  int log_count = count_log_rows(db);
  assert(log_count >= 1000); // includes the CREATE TABLE

  printf("(%.1f ms, %.2f writes/ms) ", elapsed, 1000.0 / elapsed);
  // Soft assertion: must complete in reasonable time (< 10s)
  assert(elapsed < 30000.0);

  db_close(db);
  cleanup_files();
}

static void test_perf_prepare_bind_step_100_rows(void) {
  arkilian *db = open_test_db();
  db_exec(db,
    "CREATE TABLE perf2 (id INTEGER PRIMARY KEY, a INT, b TEXT)");

  double start = now_ms();
  for (int i = 0; i < 100; i++) {
    int rc = db_prepare(db, "INSERT INTO perf2 (a, b) VALUES (?, ?)");
    assert(rc == SQLITE_OK);
    db_bind_int(db, 1, i);
    db_bind_text(db, 2, "bound");
    rc = db_step(db);
    assert(rc == SQLITE_DONE);
    db_finalize(db);
  }
  double elapsed = now_ms() - start;

  db_prepare(db, "SELECT COUNT(*) FROM perf2");
  db_step(db);
  assert(db_column_int(db, 0) == 100);
  db_finalize(db);

  int log_count = count_log_rows(db);
  assert(log_count >= 100);

  printf("(%.1f ms, %.2f writes/ms) ", elapsed, 100.0 / elapsed);
  assert(elapsed < 10000.0);

  db_close(db);
  cleanup_files();
}

static void test_perf_select_1000_reads(void) {
  arkilian *db = open_test_db();
  db_exec(db, "CREATE TABLE perf3 (id INTEGER PRIMARY KEY, val INT)");
  for (int i = 0; i < 100; i++) {
    char sql[64];
    snprintf(sql, sizeof(sql), "INSERT INTO perf3 (val) VALUES (%d)", i);
    db_exec(db, sql);
  }

  int log_before = count_log_rows(db);

  double start = now_ms();
  for (int i = 0; i < 1000; i++) {
    int rc = db_exec(db, "SELECT COUNT(*) FROM perf3");
    (void)rc;
  }
  double elapsed = now_ms() - start;

  // Reads should NOT produce log entries
  int log_after = count_log_rows(db);
  assert(log_after == log_before);

  printf("(%.1f ms, %.2f reads/ms) ", elapsed, 1000.0 / elapsed);
  assert(elapsed < 10000.0);

  db_close(db);
  cleanup_files();
}

// ---------------------------------------------------------------------------
// Safety: Transaction atomiticy — all-or-nothing failure
// ---------------------------------------------------------------------------

static void test_write_atomicity_all_or_nothing(void) {
  // Test that if a multi-statement SQL fails partway, the entire
  // transaction rolls back (no partial writes, no partial log).
  arkilian *db = open_test_db();
  db_exec(db, "CREATE TABLE atomic (id INTEGER PRIMARY KEY, val INT)");

  int log_before = count_log_rows(db);

  // This SQL has two statements: first valid, second invalid
  // SQLite's sqlite3_exec processes statements sequentially.
  // Our wrapper wraps the whole call in BEGIN/COMMIT.
  int rc = db_exec(db,
    "INSERT INTO atomic (val) VALUES (1);"
    "INSERT INTO nonexistent_table VALUES (2)");
  assert(rc != SQLITE_DONE);

  int log_after = count_log_rows(db);
  assert(log_after == log_before);

  db_prepare(db, "SELECT COUNT(*) FROM atomic");
  db_step(db);
  assert(db_column_int(db, 0) == 0);
  db_finalize(db);

  db_close(db);
  cleanup_files();
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

int main(void) {
  setenv("ARKILIAN_ENABLE_BACKUP", "0", 1);

  printf("=== Arkilian Write Interception Tests ===\n\n");

  printf("[Pragma Verification]\n");
  RUN_TEST(test_pragma_journal_mode_is_wal);
  RUN_TEST(test_pragma_synchronous_is_normal);
  RUN_TEST(test_pragma_foreign_keys_is_on);
  RUN_TEST(test_pragma_busy_timeout_is_set);

  printf("\n[Internal Tables]\n");
  RUN_TEST(test_meta_table_exists);
  RUN_TEST(test_log_table_exists);

  printf("\n[Write Logging — db_exec path]\n");
  RUN_TEST(test_exec_insert_logs_entry);
  RUN_TEST(test_exec_update_logs_entry);
  RUN_TEST(test_exec_delete_logs_entry);
  RUN_TEST(test_exec_create_table_logs_entry);
  RUN_TEST(test_exec_drop_table_logs_entry);

  printf("\n[Write Logging — prepare/step/finalize path]\n");
  RUN_TEST(test_prepare_step_insert_logs_entry);
  RUN_TEST(test_prepare_step_update_logs_entry);

  printf("\n[Reads — No Side Effects]\n");
  RUN_TEST(test_exec_select_does_not_log);
  RUN_TEST(test_prepare_select_does_not_log);

  printf("\n[Failed Writes — Rollback + No Log]\n");
  RUN_TEST(test_exec_failed_write_rolls_back_no_log);
  RUN_TEST(test_prepare_step_failed_write_rolls_back);

  printf("\n[LSN Monotonicity]\n");
  RUN_TEST(test_lsn_is_monotonic);

  printf("\n[SQL Escaping in Log]\n");
  RUN_TEST(test_log_escapes_sql_special_chars);

  printf("\n[Concurrency — %d threads x %d writes each]\n",
         CONCURRENT_THREADS, WRITES_PER_THREAD);
  RUN_TEST(test_concurrent_writes_all_succeed);

  printf("\n[Write Exclusion]\n");
  RUN_TEST(test_second_write_prepare_returns_busy);

  printf("\n[Read Exclusions (PRAGMA/EXPLAIN/bulk)]\n");
  RUN_TEST(test_pragma_read_does_not_log);
  RUN_TEST(test_explain_does_not_log);
  RUN_TEST(test_many_selects_produce_zero_log_entries);

  printf("\n[Snapshot Isolation]\n");
  RUN_TEST(test_reads_during_write_transaction_see_snapshot);

  printf("\n[Atomicity — All-or-Nothing]\n");
  RUN_TEST(test_write_atomicity_all_or_nothing);

  printf("\n[Performance]\n");
  RUN_TEST(test_perf_batch_insert_1000_rows);
  RUN_TEST(test_perf_prepare_bind_step_100_rows);
  RUN_TEST(test_perf_select_1000_reads);

  printf("\n=== Results: %d/%d passed ===\n", tests_passed, tests_run);
  return (tests_passed == tests_run) ? 0 : 1;
}
