// Arkilian Kill-Switch Tests (spec §1 + §10.7)
//
// Covers:
//   - ARKILIAN_ENABLE_BACKUP=0 at startup: game CRUD works with backup
//     fully disabled; capture still queues rows but nothing is shipped
//     (attempts stay 0, rows are never deleted).
//   - Runtime kill-switch (db_backup_set_enabled): with a live 1:1 S3 mock
//     destination, rows ship and drain; disabling stops ALL shipping
//     (zero requests reach the destination, queue grows, attempts stay
//     0); re-enabling resumes exactly where the queue left off.

#include "class.h"
#include "ark_test_env.h"
#include "ark_stub_s3.h"
#include <assert.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
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

static void cleanup(const char *path) {
  remove(path);
  char side[256];
  snprintf(side, sizeof(side), "%s-wal", path); remove(side);
  snprintf(side, sizeof(side), "%s-shm", path); remove(side);
  snprintf(side, sizeof(side), "%s-journal", path); remove(side);
  snprintf(side, sizeof(side), "%s.arklock", path); remove(side);
}

// Poll _pending_backup until empty or timeout. 0 = drained.
static int wait_queue_empty(arkilian *db, int timeout_ms) {
  int waited = 0;
  while (db_wal_pending(db) > 0 && waited < timeout_ms) {
    usleep(100 * 1000);
    waited += 100;
  }
  return db_wal_pending(db) == 0 ? 0 : -1;
}

// Sum of `attempts` across _pending_backup — 0 proves no ship was ever
// attempted (attempts only increment after a failed ship_to_backup).
static int sum_attempts(arkilian *db) {
  int sum = -1;
  if (db_prepare(db, "SELECT COALESCE(SUM(attempts), 0) FROM _pending_backup") == SQLITE_OK) {
    if (db_step(db) == SQLITE_ROW) sum = db_column_int(db, 0);
    db_finalize(db);
  }
  return sum;
}

// ── Startup kill-switch ─────────────────────────────────────────────

static void test_disabled_at_startup(void) {
  cleanup("test_ks_off.db");
  clear_s3_env();
  ark_setenv("ARKILIAN_ENABLE_BACKUP", "0", 1);
  ark_setenv("ARKILIAN_BACKUP_INTERVAL", "3600", 1); // hermetic: no .env dependence
  arkilian *db = NULL;
  assert(db_init(&db, "test_ks_off.db") == 0);
  assert(db_backup_is_enabled(db) == 0);

  // Game runs correctly with the backup subsystem disabled (§10.7).
  assert(db_exec(db, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)") == SQLITE_OK);
  assert(db_exec(db, "INSERT INTO t (v) VALUES ('x')") == SQLITE_OK);
  db_prepare(db, "SELECT COUNT(*) FROM t");
  assert(db_step(db) == SQLITE_ROW);
  assert(db_column_int(db, 0) == 1);
  db_finalize(db);

  // Capture still runs (rows queued), but nothing is shipped: rows stay
  // in _pending_backup with attempts == 0.
  assert(db_wal_pending(db) >= 1);
  sleep(3); // > POLL_INTERVAL_MS (2s) — give a bogus-enabled thread time to act
  assert(db_wal_pending(db) >= 1);   // never deleted
  assert(sum_attempts(db) == 0);     // never attempted

  db_close(db);
  cleanup("test_ks_off.db");
}

// ── Runtime kill-switch: enable → disable → re-enable ───────────────

static void test_runtime_kill_switch(void) {
  cleanup("test_ks_toggle.db");
  stub_start();
  stub_reset();
  set_s3_env();

  ark_setenv("ARKILIAN_ENABLE_BACKUP", "1", 1);
  ark_setenv("ARKILIAN_BACKUP_INTERVAL", "3600", 1);

  arkilian *db = NULL;
  assert(db_init(&db, "test_ks_toggle.db") == 0);
  assert(db_backup_is_enabled(db) == 1);
  assert(db_exec(db, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)") == SQLITE_OK);

  // Phase 1 — enabled: rows ship, queue drains, destination receives them.
  for (int i = 0; i < 5; i++) {
    char sql[128];
    snprintf(sql, sizeof(sql), "INSERT INTO t (v) VALUES ('row%d')", i);
    assert(db_exec(db, sql) == SQLITE_OK);
  }
  assert(wait_queue_empty(db, 10000) == 0); // drained ⇒ every row 2xx-acked
  int shipped_enabled = atomic_load(&g_stub_put_count);
  assert(shipped_enabled >= 1);

  // Let the flush thread finish its pass and fall asleep in cond-wait
  // before flipping the switch, so no drain is mid-flight across the
  // toggle (an in-flight pass completes by design).
  sleep(2); // > POLL_INTERVAL_MS (2s)

  // Phase 2 — kill-switch off: nothing ships, nothing is deleted,
  // attempts stay 0 (queue just accumulates for later replay).
  // Disable BEFORE writing so no pass can start mid-stream.
  db_backup_set_enabled(db, 0);
  assert(db_backup_is_enabled(db) == 0);
  for (int i = 0; i < 5; i++) {
    char sql[128];
    snprintf(sql, sizeof(sql), "INSERT INTO t (v) VALUES ('off%d')", i);
    assert(db_exec(db, sql) == SQLITE_OK);
  }
  int baseline = atomic_load(&g_stub_put_count);
  sleep(3); // > poll interval
  assert(db_wal_pending(db) == 5);                         // queued, not drained
  assert(atomic_load(&g_stub_put_count) == baseline);       // zero new requests
  assert(sum_attempts(db) == 0);                           // zero ship attempts

  // Phase 3 — re-enabled: shipping resumes from the queue, no data lost.
  db_backup_set_enabled(db, 1);
  assert(db_backup_is_enabled(db) == 1);
  assert(wait_queue_empty(db, 10000) == 0);
  assert(atomic_load(&g_stub_put_count) > shipped_enabled); // resumed: ≥1 new chunk request

  db_close(db);
  stub_stop();
  clear_s3_env();
  cleanup("test_ks_toggle.db");
}

// ── Main ────────────────────────────────────────────────────────────

int main(void) {
  signal(SIGPIPE, SIG_IGN);

  printf("=== Arkilian Kill-Switch Tests ===\n\n");
  RUN_TEST(test_disabled_at_startup);
  RUN_TEST(test_runtime_kill_switch);
  printf("\n=== Results: %d/%d passed ===\n", tests_passed, tests_run);
  return (tests_passed == tests_run) ? 0 : 1;
}
