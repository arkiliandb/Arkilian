// Arkilian DLQ Tool Regression Tests — 5,000-business launch verification
// (Checklist #5: "Confirm tools/arkilian-dlq compiles cleanly across target
// platforms and that the recovery runbook is complete for tier-1 support".)
//
// Drives the `arkilian-dlq` CLI via system() against a crafted outbox and
// asserts the on-disk state transitions. Regression-protects the
// `--replay --id N` path which was broken at launch verification with
// "replay prepare failed: near \"AND\": syntax error" (a `FROM ... AND id`
// construction). Idempotency, single-row, and full-replay paths are all
// covered so the recovery runbook's commands are provably correct.
//
// Cross-platform: uses only system() + sqlite3 (no BSD sockets), so it
// runs on the MSYS2/MinGW CI leg alongside the rest of the suite.
//
// Compile (manual, macOS/Linux):
//   cc -O2 -Wall -Wextra tests/test_dlq.c src/deps/sqlite/sqlite3.c -Isrc/deps/sqlite -DARKILIAN_DLQ_BIN='"./arkilian-dlq"' -o test_dlq
// Assumes ./arkilian-dlq is already built next to the test.

#include "sqlite3.h"
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifndef ARKILIAN_DLQ_BIN
#define ARKILIAN_DLQ_BIN "./arkilian-dlq"
#endif

static const char *DB = "test_dlq_data.db";

static void cleanup_db(void) {
  remove(DB);
  char s[64];
  snprintf(s, sizeof s, "%s-wal", DB);     remove(s);
  snprintf(s, sizeof s, "%s-shm", DB);     remove(s);
  snprintf(s, sizeof s, "%s-journal", DB); remove(s);
}

// Invoke the dlq binary; returns the process exit status (0 = success).
static int run_dlq(const char *args) {
  char cmd[4096];
  snprintf(cmd, sizeof cmd, "\"%s\" \"%s\" %s", ARKILIAN_DLQ_BIN, DB, args);
  // On POSIX system() returns a wait status whose exit code is WEXITSTATUS;
  // on Windows it returns the exit code directly. A successful tool run
  // exits 0 in both, which is all we assert on.
  return system(cmd);
}

static sqlite3 *open_ro(void) {
  sqlite3 *h = NULL;
  sqlite3_open_v2(DB, &h, SQLITE_OPEN_READONLY, NULL);
  return h;
}

static long long count_in(const char *table) {
  sqlite3 *h = open_ro();
  if (!h) return -1;
  sqlite3_stmt *st = NULL;
  long long n = -1;
  char sql[128];
  snprintf(sql, sizeof sql, "SELECT COUNT(*) FROM %s", table);
  if (sqlite3_prepare_v2(h, sql, -1, &st, NULL) == SQLITE_OK &&
      sqlite3_step(st) == SQLITE_ROW) n = sqlite3_column_int64(st, 0);
  sqlite3_finalize(st);
  sqlite3_close(h);
  return n;
}

static int attempts_of(sqlite3_int64 id) {
  sqlite3 *h = open_ro();
  if (!h) return -1;
  sqlite3_stmt *st = NULL;
  int n = -1;
  if (sqlite3_prepare_v2(h, "SELECT attempts FROM _pending_backup WHERE id=?",
                         -1, &st, NULL) == SQLITE_OK) {
    sqlite3_bind_int64(st, 1, id);
    if (sqlite3_step(st) == SQLITE_ROW) n = sqlite3_column_int(st, 0);
  }
  sqlite3_finalize(st);
  sqlite3_close(h);
  return n;
}

static void seed(void) {
  cleanup_db();
  sqlite3 *h = NULL;
  int rc = sqlite3_open_v2(DB, &h,
      SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_FULLMUTEX, NULL);
  assert(rc == SQLITE_OK && h);
  char *err = NULL;
  sqlite3_exec(h,
    "CREATE TABLE _pending_backup(id INTEGER PRIMARY KEY,payload TEXT,"
    "attempts INTEGER,created_at INTEGER,last_attempt_at INTEGER);"
    "CREATE TABLE _dead_backup(id INTEGER PRIMARY KEY,payload TEXT,"
    "attempts INTEGER,failed_reason TEXT,created_at INTEGER,dead_lettered_at INTEGER);",
    0, 0, &err);
  assert(!err);
  // 4 dead rows (100,101,102,200). id 200 is ALSO already in _pending,
  // exercising the idempotency / "NOT IN (SELECT id FROM _pending_backup)"
  // branch of the replay query.
  sqlite3_exec(h,
    "INSERT INTO _dead_backup(id,payload,attempts,failed_reason,created_at,dead_lettered_at) VALUES"
    "(100,'PA',10,'max attempts exceeded',0,0),"
    "(101,'PB',10,'max attempts exceeded',0,0),"
    "(102,'PC',10,'max attempts exceeded',0,0),"
    "(200,'PD',5,'max attempts exceeded',0,0);"
    "INSERT INTO _pending_backup(id,payload,attempts,created_at) VALUES(200,'PD',0,0);",
    0, 0, &err);
  assert(!err);
  sqlite3_close(h);
}

static int tests_run = 0, tests_passed = 0;
#define RUN(fn) do { tests_run++; printf("  [%02d] %-52s ", tests_run, #fn); fflush(stdout); \
                     fn(); tests_passed++; printf("PASS\n"); } while (0)

static void test_count_reports_dead(void) {
  seed();
  assert(run_dlq("--count") == 0);
  assert(count_in("_dead_backup") == 4);
}

static void test_list_runs_clean(void) {
  seed();
  assert(run_dlq("--list --limit 2") == 0);
  assert(count_in("_dead_backup") == 4);
  assert(count_in("_pending_backup") == 1);
}

static void test_dry_run_no_mutation(void) {
  seed();
  assert(run_dlq("--replay --dry-run") == 0);
  assert(count_in("_dead_backup") == 4);     // unchanged
  assert(count_in("_pending_backup") == 1); // unchanged
}

static void test_replay_single_id(void) {
  // THE regression: `--replay --id N` used to fail with
  // "replay prepare failed: near \"AND\": syntax error".
  seed();
  assert(run_dlq("--replay --id 101") == 0);
  assert(count_in("_dead_backup") == 3);     // 101 removed
  assert(count_in("_pending_backup") == 2);  // 101 added (200 still there)
  assert(attempts_of(101) == 0);            // attempts reset on re-queue
}

static void test_replay_id_already_pending(void) {
  seed();
  assert(run_dlq("--replay --id 200") == 0);
  assert(count_in("_dead_backup") == 3);    // 200 removed from dead
  assert(count_in("_pending_backup") == 1); // not duplicated (INSERT OR IGNORE)
  assert(attempts_of(200) == 0);
}

static void test_replay_id_nonexistent(void) {
  seed();
  assert(run_dlq("--replay --id 99999") == 0);
  assert(count_in("_dead_backup") == 4);
  assert(count_in("_pending_backup") == 1);
}

static void test_replay_all(void) {
  seed();
  assert(run_dlq("--replay") == 0);
  assert(count_in("_dead_backup") == 0);     // all drained
  assert(count_in("_pending_backup") == 4);  // 100,101,102,200
  assert(attempts_of(100) == 0);
  assert(attempts_of(101) == 0);
  assert(attempts_of(102) == 0);
}

static void test_replay_idempotent_second(void) {
  seed();
  assert(run_dlq("--replay") == 0);
  assert(run_dlq("--replay") == 0); // second run: nothing left to replay
  assert(count_in("_dead_backup") == 0);
  assert(count_in("_pending_backup") == 4);
}

int main(void) {
  printf("=== Arkilian DLQ Tool Tests ===\n\n");
  RUN(test_count_reports_dead);
  RUN(test_list_runs_clean);
  RUN(test_dry_run_no_mutation);
  RUN(test_replay_single_id);
  RUN(test_replay_id_already_pending);
  RUN(test_replay_id_nonexistent);
  RUN(test_replay_all);
  RUN(test_replay_idempotent_second);
  cleanup_db();
  printf("\n=== Results: %d/%d passed ===\n", tests_passed, tests_run);
  return (tests_passed == tests_run) ? 0 : 1;
}
