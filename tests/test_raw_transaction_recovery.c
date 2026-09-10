// test_raw_transaction_recovery.c — P1: Adversarial transaction recovery matrix
//
// Verifies source <-> remote logical equivalence across adversarial raw transaction scenarios:
//   Case A: Autocommit prepared INSERT with bound values
//   Case B: Failed prepared INSERT (NOT NULL constraint violation)
//   Case C: BEGIN -> CREATE TABLE -> INSERT -> COMMIT
//   Case D: BEGIN -> CREATE TABLE -> INSERT -> ROLLBACK
//   Case E: SAVEPOINT -> INSERT -> ROLLBACK TO -> INSERT -> RELEASE
//   Case F: Nested SAVEPOINTs (s1 -> s2 -> ROLLBACK TO s2 -> RELEASE s1)
//   Case G: Commit sidecar record written -> simulate crash -> restart recovery
//   Case H: Sidecar commit written, crash before outbox promotion
//   Case I: Partial promotion (watermark check skips already-promoted txid)
//   Case J: Truncated sidecar tail (corrupt/uncommitted tail discarded)
//
// The ultimate assertion for every test is tables_match(source, target, ...)
// after replaying the captured outbox stream via hydrate_replay_chunk().

#include "class.h"
#include "hydration.h"
#include "ark_test_env.h"
#include "deps/sqlite/sqlite3.h"
#include <assert.h>
#include <stdint.h>
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

static arkilian *open_hermetic(const char *path) {
  cleanup(path);
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

// ── Logical Equivalence Helper ──────────────────────────────────────

static int tables_match(sqlite3 *s1, sqlite3 *s2, const char *table) {
  sqlite3_stmt *st1 = NULL, *st2 = NULL;
  char check_sql[512];
  snprintf(check_sql, sizeof(check_sql),
           "SELECT sql FROM sqlite_master WHERE type='table' AND name='%s'", table);
  int ex1 = (sqlite3_prepare_v2(s1, check_sql, -1, &st1, NULL) == SQLITE_OK &&
             sqlite3_step(st1) == SQLITE_ROW);
  sqlite3_finalize(st1);
  int ex2 = (sqlite3_prepare_v2(s2, check_sql, -1, &st2, NULL) == SQLITE_OK &&
             sqlite3_step(st2) == SQLITE_ROW);
  sqlite3_finalize(st2);

  if (!ex1 && !ex2) return 1; // Neither has the table (e.g. rolled back DDL)
  if (ex1 != ex2) return 0;   // One has table, other doesn't

  // Compare row count
  char count_sql[512];
  snprintf(count_sql, sizeof(count_sql), "SELECT COUNT(*) FROM \"%s\"", table);
  assert(sqlite3_prepare_v2(s1, count_sql, -1, &st1, NULL) == SQLITE_OK);
  assert(sqlite3_step(st1) == SQLITE_ROW);
  long long cnt1 = sqlite3_column_int64(st1, 0);
  sqlite3_finalize(st1);

  assert(sqlite3_prepare_v2(s2, count_sql, -1, &st2, NULL) == SQLITE_OK);
  assert(sqlite3_step(st2) == SQLITE_ROW);
  long long cnt2 = sqlite3_column_int64(st2, 0);
  sqlite3_finalize(st2);

  if (cnt1 != cnt2) return 0;

  // Compare row-by-row, column-by-column
  char select_sql[512];
  snprintf(select_sql, sizeof(select_sql), "SELECT * FROM \"%s\" ORDER BY rowid", table);
  assert(sqlite3_prepare_v2(s1, select_sql, -1, &st1, NULL) == SQLITE_OK);
  assert(sqlite3_prepare_v2(s2, select_sql, -1, &st2, NULL) == SQLITE_OK);

  int cols = sqlite3_column_count(st1);
  if (cols != sqlite3_column_count(st2)) {
    sqlite3_finalize(st1); sqlite3_finalize(st2);
    return 0;
  }

  while (1) {
    int r1 = sqlite3_step(st1);
    int r2 = sqlite3_step(st2);
    if (r1 == SQLITE_DONE && r2 == SQLITE_DONE) break;
    if (r1 != SQLITE_ROW || r2 != SQLITE_ROW) {
      sqlite3_finalize(st1); sqlite3_finalize(st2);
      return 0;
    }
    for (int i = 0; i < cols; i++) {
      int t1 = sqlite3_column_type(st1, i);
      int t2 = sqlite3_column_type(st2, i);
      if (t1 != t2) { sqlite3_finalize(st1); sqlite3_finalize(st2); return 0; }
      if (t1 == SQLITE_INTEGER) {
        if (sqlite3_column_int64(st1, i) != sqlite3_column_int64(st2, i)) {
          sqlite3_finalize(st1); sqlite3_finalize(st2); return 0;
        }
      } else if (t1 == SQLITE_FLOAT) {
        if (sqlite3_column_double(st1, i) != sqlite3_column_double(st2, i)) {
          sqlite3_finalize(st1); sqlite3_finalize(st2); return 0;
        }
      } else if (t1 == SQLITE_TEXT) {
        const char *txt1 = (const char *)sqlite3_column_text(st1, i);
        const char *txt2 = (const char *)sqlite3_column_text(st2, i);
        if (strcmp(txt1, txt2) != 0) {
          sqlite3_finalize(st1); sqlite3_finalize(st2); return 0;
        }
      }
    }
  }
  sqlite3_finalize(st1);
  sqlite3_finalize(st2);
  return 1;
}

// Replay unapplied rows from source _pending_backup into target via hydrate_replay_chunk
static void replay_outbox_to_target(arkilian *src, sqlite3 *target, int64_t *last_id) {
  sqlite3 *h = db_get_handle(src);
  sqlite3_stmt *st = NULL;
  int64_t min_id = last_id ? *last_id + 1 : 0;
  int rc = sqlite3_prepare_v2(h, "SELECT id, payload FROM _pending_backup WHERE id >= ? ORDER BY id ASC",
                              -1, &st, NULL);
  assert(rc == SQLITE_OK);
  sqlite3_bind_int64(st, 1, min_id);
  while (sqlite3_step(st) == SQLITE_ROW) {
    int64_t id = sqlite3_column_int64(st, 0);
    const char *payload = (const char *)sqlite3_column_text(st, 1);
    if (payload && strlen(payload) > 0) {
      int hr = hydrate_replay_chunk(target, payload, id);
      assert(hr == 0);
    }
    if (last_id) *last_id = id;
  }
  sqlite3_finalize(st);
}

// Sidecar CRC/checksum calculation helper
static uint32_t test_sidecar_csum(uint64_t txid, uint8_t type, const char *sql) {
  uint32_t h = 2166136261u;
  for (int i = 0; i < 8; i++) { h ^= (uint8_t)(txid >> (i * 8)); h *= 16777619; }
  h ^= type; h *= 16777619;
  if (sql) for (const char *p = sql; *p; p++) { h ^= (uint8_t)*p; h *= 16777619; }
  return h;
}

static void write_sidecar_record(FILE *f, uint64_t txid, uint8_t type, const char *payload) {
  uint32_t len = payload ? (uint32_t)strlen(payload) : 0;
  uint32_t csum = test_sidecar_csum(txid, type, payload);
  fwrite(&txid, 1, 8, f);
  fwrite(&type, 1, 1, f);
  fwrite(&len, 1, 4, f);
  if (len > 0) fwrite(payload, 1, len, f);
  fwrite(&csum, 1, 4, f);
}

// ── Test Cases ──────────────────────────────────────────────────────

// Case A: Autocommit prepared INSERT with bound values
static void test_case_a_autocommit_prepared_insert(void) {
  const char *src_path = "recov_case_a_src.db";
  const char *tgt_path = "recov_case_a_tgt.db";
  cleanup(src_path); cleanup(tgt_path);

  arkilian *src = open_hermetic(src_path);
  assert(db_exec(src, "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INT);") == SQLITE_OK);

  sqlite3 *tgt = NULL;
  assert(sqlite3_open(tgt_path, &tgt) == SQLITE_OK);

  int64_t last_id = 0;
  // Replay initial CREATE TABLE to target
  replay_outbox_to_target(src, tgt, &last_id);

  // Execute prepared INSERT on raw handle with bound values
  sqlite3 *raw = db_get_handle(src);
  sqlite3_stmt *st = NULL;
  assert(sqlite3_prepare_v2(raw, "INSERT INTO users (id, name, age) VALUES (?, ?, ?);",
                            -1, &st, NULL) == SQLITE_OK);
  assert(sqlite3_bind_int64(st, 1, 101) == SQLITE_OK);
  assert(sqlite3_bind_text(st, 2, "Alice", -1, SQLITE_STATIC) == SQLITE_OK);
  assert(sqlite3_bind_int(st, 3, 30) == SQLITE_OK);
  assert(sqlite3_step(st) == SQLITE_DONE);
  sqlite3_finalize(st);

  // Resync triggers to ensure CDC is flushed
  db_resync_triggers(src);

  // Replay outbox to target
  replay_outbox_to_target(src, tgt, &last_id);

  // Assert logical equivalence
  assert(tables_match(raw, tgt, "users") == 1);

  sqlite3_close(tgt);
  db_close(src);
  cleanup(src_path); cleanup(tgt_path);
}

// Case B: Failed prepared INSERT (NOT NULL violation)
static void test_case_b_failed_prepared_insert(void) {
  const char *src_path = "recov_case_b_src.db";
  const char *tgt_path = "recov_case_b_tgt.db";
  cleanup(src_path); cleanup(tgt_path);

  arkilian *src = open_hermetic(src_path);
  assert(db_exec(src, "CREATE TABLE strict_users (id INTEGER PRIMARY KEY, name TEXT NOT NULL);") == SQLITE_OK);

  sqlite3 *tgt = NULL;
  assert(sqlite3_open(tgt_path, &tgt) == SQLITE_OK);

  int64_t last_id = 0;
  replay_outbox_to_target(src, tgt, &last_id);

  // Prepared INSERT that fails NOT NULL constraint
  sqlite3 *raw = db_get_handle(src);
  sqlite3_stmt *st = NULL;
  assert(sqlite3_prepare_v2(raw, "INSERT INTO strict_users (id, name) VALUES (?, ?);",
                            -1, &st, NULL) == SQLITE_OK);
  assert(sqlite3_bind_int64(st, 1, 1) == SQLITE_OK);
  assert(sqlite3_bind_null(st, 2) == SQLITE_OK);
  int step_rc = sqlite3_step(st);
  assert(step_rc == SQLITE_CONSTRAINT);
  sqlite3_finalize(st);

  db_resync_triggers(src);
  replay_outbox_to_target(src, tgt, &last_id);

  // Both source and target must match and have 0 rows
  assert(tables_match(raw, tgt, "strict_users") == 1);

  sqlite3_close(tgt);
  db_close(src);
  cleanup(src_path); cleanup(tgt_path);
}

// Case C: BEGIN -> CREATE TABLE -> INSERT -> COMMIT
static void test_case_c_explicit_commit(void) {
  const char *src_path = "recov_case_c_src.db";
  const char *tgt_path = "recov_case_c_tgt.db";
  cleanup(src_path); cleanup(tgt_path);

  arkilian *src = open_hermetic(src_path);
  sqlite3 *raw = db_get_handle(src);

  sqlite3 *tgt = NULL;
  assert(sqlite3_open(tgt_path, &tgt) == SQLITE_OK);

  // Execute explicit txn with DDL + DML on raw handle
  assert(sqlite3_exec(raw, "BEGIN;", NULL, NULL, NULL) == SQLITE_OK);
  assert(sqlite3_exec(raw, "CREATE TABLE items (id INTEGER PRIMARY KEY, desc TEXT);",
                      NULL, NULL, NULL) == SQLITE_OK);
  assert(sqlite3_exec(raw, "INSERT INTO items VALUES (1, 'widget_alpha');",
                      NULL, NULL, NULL) == SQLITE_OK);
  assert(sqlite3_exec(raw, "INSERT INTO items VALUES (2, 'widget_beta');",
                      NULL, NULL, NULL) == SQLITE_OK);
  assert(sqlite3_exec(raw, "COMMIT;", NULL, NULL, NULL) == SQLITE_OK);

  db_resync_triggers(src);
  replay_outbox_to_target(src, tgt, NULL);

  assert(tables_match(raw, tgt, "items") == 1);

  sqlite3_close(tgt);
  db_close(src);
  cleanup(src_path); cleanup(tgt_path);
}

// Case D: BEGIN -> CREATE TABLE -> INSERT -> ROLLBACK
static void test_case_d_explicit_rollback(void) {
  const char *src_path = "recov_case_d_src.db";
  const char *tgt_path = "recov_case_d_tgt.db";
  cleanup(src_path); cleanup(tgt_path);

  arkilian *src = open_hermetic(src_path);
  sqlite3 *raw = db_get_handle(src);

  sqlite3 *tgt = NULL;
  assert(sqlite3_open(tgt_path, &tgt) == SQLITE_OK);

  assert(sqlite3_exec(raw, "BEGIN;", NULL, NULL, NULL) == SQLITE_OK);
  assert(sqlite3_exec(raw, "CREATE TABLE discarded_tbl (id INT);", NULL, NULL, NULL) == SQLITE_OK);
  assert(sqlite3_exec(raw, "INSERT INTO discarded_tbl VALUES (99);", NULL, NULL, NULL) == SQLITE_OK);
  assert(sqlite3_exec(raw, "ROLLBACK;", NULL, NULL, NULL) == SQLITE_OK);

  db_resync_triggers(src);
  replay_outbox_to_target(src, tgt, NULL);

  // Table must not exist on either
  assert(tables_match(raw, tgt, "discarded_tbl") == 1);

  sqlite3_close(tgt);
  db_close(src);
  cleanup(src_path); cleanup(tgt_path);
}

// Case E: SAVEPOINT -> INSERT -> ROLLBACK TO -> INSERT -> RELEASE
static void test_case_e_savepoint_rollback_to(void) {
  const char *src_path = "recov_case_e_src.db";
  const char *tgt_path = "recov_case_e_tgt.db";
  cleanup(src_path); cleanup(tgt_path);

  arkilian *src = open_hermetic(src_path);
  assert(db_exec(src, "CREATE TABLE sp_items (id INTEGER PRIMARY KEY, v TEXT);") == SQLITE_OK);

  sqlite3 *tgt = NULL;
  assert(sqlite3_open(tgt_path, &tgt) == SQLITE_OK);

  int64_t last_id = 0;
  replay_outbox_to_target(src, tgt, &last_id);

  sqlite3 *raw = db_get_handle(src);
  assert(sqlite3_exec(raw, "SAVEPOINT s1;", NULL, NULL, NULL) == SQLITE_OK);
  assert(sqlite3_exec(raw, "INSERT INTO sp_items VALUES (1, 'rolled_back');", NULL, NULL, NULL) == SQLITE_OK);
  assert(sqlite3_exec(raw, "ROLLBACK TO s1;", NULL, NULL, NULL) == SQLITE_OK);
  assert(sqlite3_exec(raw, "INSERT INTO sp_items VALUES (2, 'committed');", NULL, NULL, NULL) == SQLITE_OK);
  assert(sqlite3_exec(raw, "RELEASE s1;", NULL, NULL, NULL) == SQLITE_OK);

  db_resync_triggers(src);
  replay_outbox_to_target(src, tgt, &last_id);

  assert(tables_match(raw, tgt, "sp_items") == 1);

  sqlite3_close(tgt);
  db_close(src);
  cleanup(src_path); cleanup(tgt_path);
}

// Case F: Nested SAVEPOINTs (s1 -> s2 -> ROLLBACK TO s2 -> RELEASE s1)
static void test_case_f_nested_savepoints(void) {
  const char *src_path = "recov_case_f_src.db";
  const char *tgt_path = "recov_case_f_tgt.db";
  cleanup(src_path); cleanup(tgt_path);

  arkilian *src = open_hermetic(src_path);
  assert(db_exec(src, "CREATE TABLE nest_items (id INTEGER PRIMARY KEY, val TEXT);") == SQLITE_OK);

  sqlite3 *tgt = NULL;
  assert(sqlite3_open(tgt_path, &tgt) == SQLITE_OK);

  int64_t last_id = 0;
  replay_outbox_to_target(src, tgt, &last_id);

  sqlite3 *raw = db_get_handle(src);
  assert(sqlite3_exec(raw, "SAVEPOINT s1;", NULL, NULL, NULL) == SQLITE_OK);
  assert(sqlite3_exec(raw, "INSERT INTO nest_items VALUES (1, 'A');", NULL, NULL, NULL) == SQLITE_OK);

  assert(sqlite3_exec(raw, "SAVEPOINT s2;", NULL, NULL, NULL) == SQLITE_OK);
  assert(sqlite3_exec(raw, "INSERT INTO nest_items VALUES (2, 'B');", NULL, NULL, NULL) == SQLITE_OK);
  assert(sqlite3_exec(raw, "INSERT INTO nest_items VALUES (3, 'C_discard');", NULL, NULL, NULL) == SQLITE_OK);
  assert(sqlite3_exec(raw, "ROLLBACK TO s2;", NULL, NULL, NULL) == SQLITE_OK);

  assert(sqlite3_exec(raw, "INSERT INTO nest_items VALUES (4, 'D');", NULL, NULL, NULL) == SQLITE_OK);
  assert(sqlite3_exec(raw, "RELEASE s1;", NULL, NULL, NULL) == SQLITE_OK);

  db_resync_triggers(src);
  replay_outbox_to_target(src, tgt, &last_id);

  assert(tables_match(raw, tgt, "nest_items") == 1);

  sqlite3_close(tgt);
  db_close(src);
  cleanup(src_path); cleanup(tgt_path);
}

// Case G: Commit sidecar record written -> simulate crash -> restart recovery
static void test_case_g_crash_restart_sidecar_recovery(void) {
  const char *src_path = "recov_case_g_src.db";
  const char *tgt_path = "recov_case_g_tgt.db";
  char qpath[512];
  snprintf(qpath, sizeof(qpath), "%s.arkddlqueue", src_path);
  cleanup(src_path); cleanup(tgt_path);

  // Initialize DB so _arkilian_meta is created
  arkilian *base = open_hermetic(src_path);
  db_close(base);

  // Synthesize committed transaction in sidecar on disk (simulating write before crash)
  FILE *f = fopen(qpath, "wb");
  assert(f != NULL);
  uint64_t txid = 201;
  write_sidecar_record(f, txid, 1, "BEGIN");
  write_sidecar_record(f, txid, 4, "CREATE TABLE crash_tbl (id INT PRIMARY KEY, msg TEXT);");
  write_sidecar_record(f, txid, 4, "INSERT INTO crash_tbl VALUES (10, 'survived_crash');");
  write_sidecar_record(f, txid, 2, "COMMIT");
  fclose(f);

  // Also apply the transaction to the SQLite db directly (representing committed state)
  sqlite3 *raw_pre = NULL;
  assert(sqlite3_open(src_path, &raw_pre) == SQLITE_OK);
  assert(sqlite3_exec(raw_pre, "CREATE TABLE crash_tbl (id INT PRIMARY KEY, msg TEXT);",
                      NULL, NULL, NULL) == SQLITE_OK);
  assert(sqlite3_exec(raw_pre, "INSERT INTO crash_tbl VALUES (10, 'survived_crash');",
                      NULL, NULL, NULL) == SQLITE_OK);
  sqlite3_close(raw_pre);

  // Simulate restart: db_init recovers committed transaction into pending_ddl
  arkilian *src = NULL;
  assert(db_init(&src, src_path) == 0 && src != NULL);

  // Resync triggers drains recovered transactions to _pending_backup
  db_resync_triggers(src);

  sqlite3 *tgt = NULL;
  assert(sqlite3_open(tgt_path, &tgt) == SQLITE_OK);
  replay_outbox_to_target(src, tgt, NULL);

  assert(tables_match(db_get_handle(src), tgt, "crash_tbl") == 1);

  sqlite3_close(tgt);
  db_close(src);
  cleanup(src_path); cleanup(tgt_path);
}

// Case H: Sidecar commit written, crash before outbox promotion
static void test_case_h_crash_before_outbox_promotion(void) {
  const char *src_path = "recov_case_h_src.db";
  const char *tgt_path = "recov_case_h_tgt.db";
  char qpath[512];
  snprintf(qpath, sizeof(qpath), "%s.arkddlqueue", src_path);
  cleanup(src_path); cleanup(tgt_path);

  arkilian *base = open_hermetic(src_path);
  db_close(base);

  // Sidecar has committed transaction txid 202
  FILE *f = fopen(qpath, "wb");
  assert(f != NULL);
  uint64_t txid = 202;
  write_sidecar_record(f, txid, 1, "BEGIN");
  write_sidecar_record(f, txid, 4, "CREATE TABLE unpromoted_tbl (id INT PRIMARY KEY, num INT);");
  write_sidecar_record(f, txid, 4, "INSERT INTO unpromoted_tbl VALUES (1, 42);");
  write_sidecar_record(f, txid, 2, "COMMIT");
  fclose(f);

  // Apply to local DB
  sqlite3 *raw_pre = NULL;
  assert(sqlite3_open(src_path, &raw_pre) == SQLITE_OK);
  assert(sqlite3_exec(raw_pre, "CREATE TABLE unpromoted_tbl (id INT PRIMARY KEY, num INT);",
                      NULL, NULL, NULL) == SQLITE_OK);
  assert(sqlite3_exec(raw_pre, "INSERT INTO unpromoted_tbl VALUES (1, 42);",
                      NULL, NULL, NULL) == SQLITE_OK);
  sqlite3_close(raw_pre);

  // Restart
  arkilian *src = NULL;
  assert(db_init(&src, src_path) == 0 && src != NULL);
  db_resync_triggers(src);

  sqlite3 *tgt = NULL;
  assert(sqlite3_open(tgt_path, &tgt) == SQLITE_OK);
  replay_outbox_to_target(src, tgt, NULL);

  assert(tables_match(db_get_handle(src), tgt, "unpromoted_tbl") == 1);

  sqlite3_close(tgt);
  db_close(src);
  cleanup(src_path); cleanup(tgt_path);
}

// Case I: Partial promotion (watermark check skips already-promoted txid)
static void test_case_i_partial_promotion_watermark(void) {
  const char *src_path = "recov_case_i_src.db";
  const char *tgt_path = "recov_case_i_tgt.db";
  char qpath[512];
  snprintf(qpath, sizeof(qpath), "%s.arkddlqueue", src_path);
  cleanup(src_path); cleanup(tgt_path);

  arkilian *base = open_hermetic(src_path);
  db_close(base);

  // Set watermark to 301 in _arkilian_meta
  sqlite3 *raw_setup = NULL;
  assert(sqlite3_open(src_path, &raw_setup) == SQLITE_OK);
  assert(sqlite3_exec(raw_setup,
      "INSERT OR REPLACE INTO _arkilian_meta (k, v) VALUES ('sidecar_promoted_txid', '301');",
      NULL, NULL, NULL) == SQLITE_OK);
  // Also create tbl_301 and tbl_302 in SQLite
  assert(sqlite3_exec(raw_setup, "CREATE TABLE tbl_301 (id INT PRIMARY KEY, v TEXT);", NULL, NULL, NULL) == SQLITE_OK);
  assert(sqlite3_exec(raw_setup, "INSERT INTO tbl_301 VALUES (1, 'already_promoted');", NULL, NULL, NULL) == SQLITE_OK);
  assert(sqlite3_exec(raw_setup, "CREATE TABLE tbl_302 (id INT PRIMARY KEY, v TEXT);", NULL, NULL, NULL) == SQLITE_OK);
  assert(sqlite3_exec(raw_setup, "INSERT INTO tbl_302 VALUES (2, 'new_promotion');", NULL, NULL, NULL) == SQLITE_OK);
  sqlite3_close(raw_setup);

  // Sidecar has both 301 and 302
  FILE *f = fopen(qpath, "wb");
  assert(f != NULL);
  // tx 301
  write_sidecar_record(f, 301, 1, "BEGIN");
  write_sidecar_record(f, 301, 4, "CREATE TABLE tbl_301 (id INT PRIMARY KEY, v TEXT);");
  write_sidecar_record(f, 301, 4, "INSERT INTO tbl_301 VALUES (1, 'already_promoted');");
  write_sidecar_record(f, 301, 2, "COMMIT");
  // tx 302
  write_sidecar_record(f, 302, 1, "BEGIN");
  write_sidecar_record(f, 302, 4, "CREATE TABLE tbl_302 (id INT PRIMARY KEY, v TEXT);");
  write_sidecar_record(f, 302, 4, "INSERT INTO tbl_302 VALUES (2, 'new_promotion');");
  write_sidecar_record(f, 302, 2, "COMMIT");
  fclose(f);

  // Target already received tbl_301 in the past
  sqlite3 *tgt = NULL;
  assert(sqlite3_open(tgt_path, &tgt) == SQLITE_OK);
  assert(sqlite3_exec(tgt, "CREATE TABLE tbl_301 (id INT PRIMARY KEY, v TEXT);", NULL, NULL, NULL) == SQLITE_OK);
  assert(sqlite3_exec(tgt, "INSERT INTO tbl_301 VALUES (1, 'already_promoted');", NULL, NULL, NULL) == SQLITE_OK);

  // Restart source
  arkilian *src = NULL;
  assert(db_init(&src, src_path) == 0 && src != NULL);
  db_resync_triggers(src);

  // Replay to target — should only have received tbl_302
  replay_outbox_to_target(src, tgt, NULL);

  assert(tables_match(db_get_handle(src), tgt, "tbl_301") == 1);
  assert(tables_match(db_get_handle(src), tgt, "tbl_302") == 1);

  sqlite3_close(tgt);
  db_close(src);
  cleanup(src_path); cleanup(tgt_path);
}

// Case J: Truncated sidecar tail (corrupt/uncommitted tail discarded)
static void test_case_j_truncated_sidecar_tail(void) {
  const char *src_path = "recov_case_j_src.db";
  const char *tgt_path = "recov_case_j_tgt.db";
  char qpath[512];
  snprintf(qpath, sizeof(qpath), "%s.arkddlqueue", src_path);
  cleanup(src_path); cleanup(tgt_path);

  arkilian *base = open_hermetic(src_path);
  db_close(base);

  // Sidecar has txid 401 (valid) and txid 402 (truncated tail)
  FILE *f = fopen(qpath, "wb");
  assert(f != NULL);
  write_sidecar_record(f, 401, 1, "BEGIN");
  write_sidecar_record(f, 401, 4, "CREATE TABLE robust_tbl (id INT PRIMARY KEY, name TEXT);");
  write_sidecar_record(f, 401, 4, "INSERT INTO robust_tbl VALUES (1, 'valid');");
  write_sidecar_record(f, 401, 2, "COMMIT");

  // Corrupt / truncated tx 402: write 12 garbage bytes
  uint64_t bad_tx = 402;
  fwrite(&bad_tx, 1, 8, f);
  fwrite("TRUNC", 1, 5, f);
  fclose(f);

  // Apply tx 401 to SQLite
  sqlite3 *raw_setup = NULL;
  assert(sqlite3_open(src_path, &raw_setup) == SQLITE_OK);
  assert(sqlite3_exec(raw_setup, "CREATE TABLE robust_tbl (id INT PRIMARY KEY, name TEXT);",
                      NULL, NULL, NULL) == SQLITE_OK);
  assert(sqlite3_exec(raw_setup, "INSERT INTO robust_tbl VALUES (1, 'valid');",
                      NULL, NULL, NULL) == SQLITE_OK);
  sqlite3_close(raw_setup);

  // Restart
  arkilian *src = NULL;
  assert(db_init(&src, src_path) == 0 && src != NULL);
  db_resync_triggers(src);

  sqlite3 *tgt = NULL;
  assert(sqlite3_open(tgt_path, &tgt) == SQLITE_OK);
  replay_outbox_to_target(src, tgt, NULL);

  assert(tables_match(db_get_handle(src), tgt, "robust_tbl") == 1);

  sqlite3_close(tgt);
  db_close(src);
  cleanup(src_path); cleanup(tgt_path);
}

int main(void) {
  printf("=== Arkilian Raw Transaction Recovery Adversarial Suite ===\n\n");

  printf("[Core Prepared Statement & Transaction Boundaries]\n");
  RUN_TEST(test_case_a_autocommit_prepared_insert);
  RUN_TEST(test_case_b_failed_prepared_insert);
  RUN_TEST(test_case_c_explicit_commit);
  RUN_TEST(test_case_d_explicit_rollback);

  printf("\n[Savepoint & Rollback-To Semantics]\n");
  RUN_TEST(test_case_e_savepoint_rollback_to);
  RUN_TEST(test_case_f_nested_savepoints);

  printf("\n[Crash Recovery & Watermark Boundaries]\n");
  RUN_TEST(test_case_g_crash_restart_sidecar_recovery);
  RUN_TEST(test_case_h_crash_before_outbox_promotion);
  RUN_TEST(test_case_i_partial_promotion_watermark);
  RUN_TEST(test_case_j_truncated_sidecar_tail);

  printf("\n=== Results: %d/%d passed ===\n", tests_passed, tests_run);
  return (tests_passed == tests_run) ? 0 : 1;
}
