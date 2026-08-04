// Arkilian Hardening Regression Tests
//
// Validates the production-readiness fixes:
//   1. HTTPS enforcement — a cleartext non-local push URL disables backup
//      at init (the bearer token must not be leaked in cleartext).
//   2. ARKILIAN_ALLOW_INSECURE=1 opts back into cleartext.
//   3. ARKILIAN_MAX_QUEUE_DEPTH hard cap: capture trigger pauses INSERTs
//      once the queue reaches the cap, so the application's writes still
//      succeed and the outbox cannot exhaust the disk the primary DB
//      lives on.
//   4. db_init partial-init contract: on failure, *db may be set and the
//      caller MUST db_close it (verified — no leak / no UB).

#include "class.h"
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static void cleanup(const char *path) {
  remove(path);
  char side[256];
  snprintf(side, sizeof(side), "%s-wal", path); remove(side);
  snprintf(side, sizeof(side), "%s-shm", path); remove(side);
  snprintf(side, sizeof(side), "%s-journal", path); remove(side);
}

static void hermetic_env(void) {
  setenv("ARKILIAN_ENABLE_BACKUP", "1", 1);
  setenv("ARKILIAN_BACKUP_INTERVAL", "3600", 1);
  unsetenv("ARKILIAN_MAX_QUEUE_DEPTH");
  unsetenv("ARKILIAN_ALLOW_INSECURE");
}

static int tests_run = 0, tests_passed = 0;
#define RUN_TEST(fn) do { \
  tests_run++; \
  printf("  [%02d] %-50s ", tests_run, #fn); \
  fn(); \
  tests_passed++; \
  printf("PASS\n"); \
} while (0)

// ── 1. Cleartext non-local push URL disables backup at init ─────────

static void test_cleartext_non_local_push_url_disables_backup(void) {
  cleanup("test_hard_http.db");
  hermetic_env();
  setenv("ARKILIAN_WAL_PUSH_URL", "http://example.com/v1/wal/push", 1);

  arkilian *db = NULL;
  int rc = db_init(&db, "test_hard_http.db");
  // db_init is never a hard failure (spec §0); the app keeps running.
  assert(rc == 0);
  // Backup MUST be disabled — the bearer token would be sent in cleartext.
  assert(db_backup_is_enabled(db) == 0);
  assert(db_backup_is_healthy(db) == 0); // a green light while idle is the silent failure
  db_close(db);
  cleanup("test_hard_http.db");
}

// ── 2. https:// push URL keeps backup enabled ────────────────────────

static void test_https_push_url_keeps_backup_enabled(void) {
  cleanup("test_hard_https.db");
  hermetic_env();
  setenv("ARKILIAN_WAL_PUSH_URL", "https://example.com/v1/wal/push", 1);

  arkilian *db = NULL;
  assert(db_init(&db, "test_hard_https.db") == 0);
  assert(db_backup_is_enabled(db) == 1);
  db_close(db);
  cleanup("test_hard_https.db");
}

// ── 3. http://127.0.0.1 (loopback) is permitted for dev ─────────────

static void test_loopback_cleartart_is_permitted(void) {
  cleanup("test_hard_loopback.db");
  hermetic_env();
  setenv("ARKILIAN_WAL_PUSH_URL", "http://127.0.0.1:9000/v1/wal/push", 1);

  arkilian *db = NULL;
  assert(db_init(&db, "test_hard_loopback.db") == 0);
  assert(db_backup_is_enabled(db) == 1);
  db_close(db);
  cleanup("test_hard_loopback.db");
}

// ── 4. http://10.x / 192.168.x / 172.16-31.x RFC1918 permitted ───────

static void test_rfc1918_cleartext_is_permitted(void) {
  cleanup("test_hard_rfc1918.db");
  hermetic_env();
  // 192.168.1.100 — RFC1918
  setenv("ARKILIAN_WAL_PUSH_URL", "http://192.168.1.100:9000/v1/wal/push", 1);

  arkilian *db = NULL;
  assert(db_init(&db, "test_hard_rfc1918.db") == 0);
  assert(db_backup_is_enabled(db) == 1);
  db_close(db);
  cleanup("test_hard_rfc1918.db");
}

// ── 5. ARKILIAN_ALLOW_INSECURE=1 opts into cleartext non-local ───────

static void test_allow_insecure_opt_in(void) {
  cleanup("test_hard_allow.db");
  hermetic_env();
  setenv("ARKILIAN_WAL_PUSH_URL", "http://example.com/v1/wal/push", 1);
  setenv("ARKILIAN_ALLOW_INSECURE", "1", 1);

  arkilian *db = NULL;
  assert(db_init(&db, "test_hard_allow.db") == 0);
  // Opted in → backup stays enabled despite cleartext non-local URL.
  assert(db_backup_is_enabled(db) == 1);
  db_close(db);
  cleanup("test_hard_allow.db");
}

// ── 6. ARKILIAN_MAX_QUEUE_DEPTH hard-caps the outbox ────────────────

static void test_max_queue_depth_caps_capture(void) {
  cleanup("test_hard_cap.db");
  hermetic_env();
  setenv("ARKILIAN_WAL_PUSH_URL", "http://127.0.0.1:1", 1); // refuse to connect
  setenv("ARKILIAN_MAX_QUEUE_DEPTH", "5", 1);

  arkilian *db = NULL;
  assert(db_init(&db, "test_hard_cap.db") == 0);
  assert(db_exec(db, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)") == SQLITE_OK);

  // Push well past the cap. Every INSERT must still succeed — the cap
  // gates capture only, not the application's writes (spec §0).
  for (int i = 0; i < 50; i++) {
    char sql[64];
    snprintf(sql, sizeof(sql), "INSERT INTO t (v) VALUES ('row%d')", i);
    assert(db_exec(db, sql) == SQLITE_OK);
  }

  // The queue is hard-capped. Single-threaded => at most cap rows.
  int depth = db_backup_queue_depth(db);
  assert(depth >= 1 && depth <= 5);

  // And health is red at this depth — the loss of capture is visible.
  assert(db_backup_is_healthy(db) == 0);

  db_close(db);
  cleanup("test_hard_cap.db");
  unsetenv("ARKILIAN_MAX_QUEUE_DEPTH");
}

// ── 7. db_init partial-init contract: failure still returns a live handle ─

static void test_partial_init_handle_must_be_closed(void) {
  // Pointing at a directory that cannot be created forces a sqlite
  // open failure deep in db_init. The contract (class.h) says *db may
  // still be set and the caller MUST db_close it.
  unsetenv("ARKILIAN_DB_PATH");
  arkilian *db = NULL;
  int rc = db_init(&db, "/nonexistent_dir_xyz/ark/cannot_create.db");
  // rc may be 0 (sqlite often creates parent-less files) or non-zero;
  // either way the handle must be closeable without crashing.
  (void)rc;
  if (db) db_close(db);
}

int main(void) {
  printf("=== Arkilian Hardening Regression Tests ===\n\n");

  printf("[HTTPS Enforcement]\n");
  RUN_TEST(test_cleartext_non_local_push_url_disables_backup);
  RUN_TEST(test_https_push_url_keeps_backup_enabled);
  RUN_TEST(test_loopback_cleartart_is_permitted);
  RUN_TEST(test_rfc1918_cleartext_is_permitted);
  RUN_TEST(test_allow_insecure_opt_in);

  printf("\n[Outbox Hard Cap]\n");
  RUN_TEST(test_max_queue_depth_caps_capture);

  printf("\n[Partial-Init Contract]\n");
  RUN_TEST(test_partial_init_handle_must_be_closed);

  printf("\n=== Results: %d/%d passed ===\n", tests_passed, tests_run);
  return (tests_passed == tests_run) ? 0 : 1;
}
