// test_sidecar_faults.c — P0: fault injection for the durable sidecar journal.
//
// Exercises every ARKILIAN_FAULT_SIDECAR injection point (open, write,
// fflush, fsync, fclose) and verifies the hard contract:
//   - On failure: sidecar_io_error=1, capture_paused=1, health RED
//   - Transaction remains in memory (unpersisted queue, not lost)
//   - No false durable promotion to pending queue or outbox
//   - Durable watermark is not advanced
//   - Sidecar journal file is retained on disk (not unlinked) on close
//   - Health flags reflect the error (ARK_HF_NO_CAPTURE_GAP cleared)

#include "class.h"
#include "ark_test_env.h"
#include "deps/sqlite/sqlite3.h"
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static int tests_run = 0, tests_passed = 0;
#define RUN_TEST(fn) do { \
  tests_run++; \
  printf("  [%02d] %-55s ", tests_run, #fn); \
  fflush(stdout); \
  fn(); \
  tests_passed++; \
  printf("PASS\n"); \
} while(0)

static void cleanup(const char *path) {
  remove(path);
  char s[512];
  snprintf(s, sizeof(s), "%s-wal", path); remove(s);
  snprintf(s, sizeof(s), "%s-shm", path); remove(s);
  snprintf(s, sizeof(s), "%s-journal", path); remove(s);
  snprintf(s, sizeof(s), "%s.arklock", path); remove(s);
  snprintf(s, sizeof(s), "%s.arkddlqueue", path); remove(s);
}

static void clear_fault(void) {
  ark_unsetenv("ARKILIAN_FAULT_SIDECAR");
}

static void set_fault(const char *op) {
  ark_setenv("ARKILIAN_FAULT_SIDECAR", op, 1);
}

static arkilian *open_hermetic(const char *path) {
  cleanup(path);
  clear_fault();
  ark_setenv("ARKILIAN_ENABLE_BACKUP", "1", 1);
  ark_setenv("ARKILIAN_S3_ENDPOINT", "", 1);
  ark_setenv("ARKILIAN_S3_BUCKET", "", 1);
  ark_setenv("ARKILIAN_S3_ACCESS_KEY", "", 1);
  ark_setenv("ARKILIAN_S3_SECRET_KEY", "", 1);
  ark_setenv("ARKILIAN_S3_PREFIX", "test", 1);
  ark_setenv("ARKILIAN_MANIFEST_HMAC_KEY", "test", 1);
  ark_setenv("ARKILIAN_BACKUP_INTERVAL", "3600", 1);
  arkilian *db = NULL;
  int rc = db_init(&db, path);
  assert(rc == 0 && db != NULL);
  return db;
}

static long long outbox_count(arkilian *db) {
  sqlite3 *h = db_get_handle(db);
  sqlite3_stmt *st = NULL;
  long long n = -1;
  if (sqlite3_prepare_v2(h, "SELECT COUNT(*) FROM _pending_backup",
                         -1, &st, NULL) == SQLITE_OK &&
      sqlite3_step(st) == SQLITE_ROW)
    n = sqlite3_column_int64(st, 0);
  sqlite3_finalize(st);
  return n;
}

// ── Core contract test: sidecar fault → health RED + tx in memory ───

static void run_autocommit_fault_test(const char *fault_op) {
  char path[128];
  snprintf(path, sizeof(path), "sidecar_fault_%s.db", fault_op);
  arkilian *db = open_hermetic(path);
  sqlite3 *h = db_get_handle(db);

  // Create a table via wrapper (triggers installed)
  assert(db_exec(db, "CREATE TABLE t1 (id INTEGER PRIMARY KEY, v TEXT);") == SQLITE_OK);
  long long before = outbox_count(db);

  // Inject fault BEFORE raw handle DDL
  set_fault(fault_op);

  // Execute raw DDL on raw handle — autocommit mode triggers sidecar_append_txn.
  int rc = sqlite3_exec(h, "CREATE TABLE t2 (id INTEGER PRIMARY KEY, name TEXT);",
                        NULL, NULL, NULL);
  assert(rc == SQLITE_OK);

  // 1. Health RED
  assert(db_backup_capture_paused(db) == 1);
  assert(db_backup_sidecar_io_error(db) == 1);
  unsigned flags = db_backup_health_flags(db);
  assert((flags & ARK_HF_NO_CAPTURE_GAP) == 0);
  assert(db_backup_is_healthy(db) == 0);

  // 2. Transaction NOT published to pending queue / outbox
  long long after = outbox_count(db);
  assert(after == before);

  // 3. Transaction exists in RAM (unpersisted queue)
  assert(db_backup_unpersisted_count(db) >= 1);

  clear_fault();
  db_close(db);
  cleanup(path);
}

static void test_open_failure_sets_health_red(void) {
  run_autocommit_fault_test("open");
}

static void test_write_failure_sets_health_red(void) {
  run_autocommit_fault_test("write");
}

static void test_fflush_failure_sets_health_red(void) {
  run_autocommit_fault_test("fflush");
}

static void test_fsync_failure_sets_health_red(void) {
  run_autocommit_fault_test("fsync");
}

static void test_fclose_failure_sets_health_red(void) {
  run_autocommit_fault_test("fclose");
}

// ── Autocommit: fault keeps node in unpersisted queue ───────────────

static void test_autocommit_fault_keeps_in_memory(void) {
  const char *path = "sidecar_autocommit_mem.db";
  arkilian *db = open_hermetic(path);
  sqlite3 *h = db_get_handle(db);

  long long baseline = outbox_count(db);

  // Inject write fault
  set_fault("write");

  // Raw autocommit DDL — captured by trace, sidecar write fails
  int rc = sqlite3_exec(h, "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT);",
                        NULL, NULL, NULL);
  assert(rc == SQLITE_OK);

  // Health should be RED
  assert(db_backup_capture_paused(db) == 1);
  assert(db_backup_sidecar_io_error(db) == 1);
  assert(db_backup_is_healthy(db) == 0);
  unsigned flags = db_backup_health_flags(db);
  assert((flags & ARK_HF_NO_CAPTURE_GAP) == 0);

  // Exists in memory, NOT published to outbox
  assert(db_backup_unpersisted_count(db) == 1);
  assert(outbox_count(db) == baseline);

  clear_fault();
  db_close(db);
  cleanup(path);
}

// ── Explicit txn: fault on COMMIT keeps txn in memory ───────────────

static void test_explicit_commit_fault_keeps_in_memory(void) {
  const char *path = "sidecar_explicit_mem.db";
  arkilian *db = open_hermetic(path);
  sqlite3 *h = db_get_handle(db);

  long long baseline = outbox_count(db);

  // Begin a raw transaction with DDL statements
  sqlite3_exec(h, "BEGIN;", NULL, NULL, NULL);
  sqlite3_exec(h, "CREATE TABLE orders (id INTEGER PRIMARY KEY, amt REAL);", NULL, NULL, NULL);
  sqlite3_exec(h, "CREATE TABLE order_items (id INTEGER PRIMARY KEY, item TEXT);", NULL, NULL, NULL);

  // Inject fault BEFORE commit
  set_fault("write");

  sqlite3_exec(h, "COMMIT;", NULL, NULL, NULL);

  // Health should be RED
  assert(db_backup_capture_paused(db) == 1);
  assert(db_backup_sidecar_io_error(db) == 1);
  assert(db_backup_is_healthy(db) == 0);

  // The 2 DDLs must exist in RAM (unpersisted queue)
  assert(db_backup_unpersisted_count(db) == 2);
  // Outbox must NOT have grown (not published)
  assert(outbox_count(db) == baseline);

  clear_fault();
  db_close(db);
  cleanup(path);
}

// ── Rollback fault logs error but correctly discards txn ────────────

static void test_rollback_fault_logs_error(void) {
  const char *path = "sidecar_rollback_fault.db";
  arkilian *db = open_hermetic(path);
  sqlite3 *h = db_get_handle(db);

  assert(db_exec(db, "CREATE TABLE rb_test (id INTEGER PRIMARY KEY, v TEXT);") == SQLITE_OK);
  long long baseline = outbox_count(db);

  // Begin a raw transaction
  sqlite3_exec(h, "BEGIN;", NULL, NULL, NULL);
  sqlite3_exec(h, "INSERT INTO rb_test (id, v) VALUES (1, 'discard');", NULL, NULL, NULL);

  // Inject fault BEFORE rollback
  set_fault("write");

  sqlite3_exec(h, "ROLLBACK;", NULL, NULL, NULL);

  // sidecar_io_error should be set (we now check rollback write return)
  assert(db_backup_sidecar_io_error(db) == 1);
  // Transaction buffer was discarded (no unpersisted leak)
  assert(db_backup_unpersisted_count(db) == 0);
  // Outbox did not grow
  assert(outbox_count(db) == baseline);

  // Verify the table is empty (rollback worked at SQLite level)
  sqlite3_stmt *st = NULL;
  assert(sqlite3_prepare_v2(h, "SELECT COUNT(*) FROM rb_test", -1, &st, NULL) == SQLITE_OK);
  assert(sqlite3_step(st) == SQLITE_ROW);
  assert(sqlite3_column_int(st, 0) == 0);
  sqlite3_finalize(st);

  clear_fault();
  db_close(db);
  cleanup(path);
}

// ── Durable watermark is NOT advanced on sidecar fault ──────────────

static void test_watermark_not_advanced_on_fault(void) {
  const char *path = "sidecar_wm_fault.db";
  arkilian *db = open_hermetic(path);
  sqlite3 *h = db_get_handle(db);

  // Initially, no sidecar watermark
  sqlite3_stmt *st = NULL;
  int rc = sqlite3_prepare_v2(h, "SELECT v FROM _arkilian_meta WHERE k = 'sidecar_promoted_txid'",
                              -1, &st, NULL);
  assert(rc == SQLITE_OK);
  int has_row = (sqlite3_step(st) == SQLITE_ROW);
  sqlite3_finalize(st);
  assert(!has_row);

  // Inject fault and run raw autocommit DDL
  set_fault("fsync");
  sqlite3_exec(h, "CREATE TABLE wm_test (id INT);", NULL, NULL, NULL);

  // Watermark must STILL not exist / not be advanced
  rc = sqlite3_prepare_v2(h, "SELECT v FROM _arkilian_meta WHERE k = 'sidecar_promoted_txid'",
                          -1, &st, NULL);
  assert(rc == SQLITE_OK);
  has_row = (sqlite3_step(st) == SQLITE_ROW);
  sqlite3_finalize(st);
  assert(!has_row);

  clear_fault();
  db_close(db);
  cleanup(path);
}

// ── Sidecar file is retained on disk if unpersisted nodes remain ────

static void test_sidecar_retained_on_fault(void) {
  const char *path = "sidecar_retained.db";
  arkilian *db = open_hermetic(path);
  sqlite3 *h = db_get_handle(db);

  char qpath[512];
  snprintf(qpath, sizeof(qpath), "%s.arkddlqueue", path);

  // Manually create a dummy sidecar file
  FILE *f = fopen(qpath, "wb");
  assert(f != NULL);
  fputs("dummy_sidecar_data", f);
  fclose(f);

  // Inject fault
  set_fault("write");
  sqlite3_exec(h, "CREATE TABLE retain_test (id INT);", NULL, NULL, NULL);

  assert(db_backup_unpersisted_count(db) > 0);

  // Closing db should NOT delete the sidecar file because unpersisted_head is non-NULL
  db_close(db);

  FILE *check = fopen(qpath, "rb");
  assert(check != NULL); // Sidecar file is retained!
  fclose(check);

  clear_fault();
  cleanup(path);
}

int main(void) {
  printf("=== Arkilian Sidecar Fault Injection Tests ===\n\n");

  printf("[Per-operation fault injection]\n");
  RUN_TEST(test_open_failure_sets_health_red);
  RUN_TEST(test_write_failure_sets_health_red);
  RUN_TEST(test_fflush_failure_sets_health_red);
  RUN_TEST(test_fsync_failure_sets_health_red);
  RUN_TEST(test_fclose_failure_sets_health_red);

  printf("\n[Transaction-level fault behavior]\n");
  RUN_TEST(test_autocommit_fault_keeps_in_memory);
  RUN_TEST(test_explicit_commit_fault_keeps_in_memory);
  RUN_TEST(test_rollback_fault_logs_error);
  RUN_TEST(test_watermark_not_advanced_on_fault);
  RUN_TEST(test_sidecar_retained_on_fault);

  printf("\n=== Results: %d/%d passed ===\n", tests_passed, tests_run);
  clear_fault();
  return (tests_passed == tests_run) ? 0 : 1;
}
