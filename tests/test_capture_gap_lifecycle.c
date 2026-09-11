// tests/test_capture_gap_lifecycle.c — Regression test for capture gap lifecycle
// and transactional manifest publication.
//
// Invariants tested:
//   1. Outage -> queue reaches cap -> CDC rows dropped -> capture_paused=1 / health RED
//   2. Partial recovery -> incremental WAL chunks ship -> capture_paused MUST REMAIN 1 / health RED
//   3. Snapshot object upload succeeds, but manifest publish fails -> capture_paused MUST REMAIN 1
//   4. Snapshot object upload succeeds AND manifest publish succeeds -> capture_paused=0 / health GREEN
//   5. Hydrated target DB from S3 matches source DB logically with 100% equivalence

#include "class.h"
#include "hydration.h"
#include "sha256.h"
#include "ark_stub_s3.h"
#include "deps/sqlite/sqlite3.h"
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static void cleanup_files(void) {
  remove("cap_src_a.db");
  remove("cap_src_a.db-wal");
  remove("cap_src_a.db-shm");
  remove("cap_dst_a.db");
  remove("cap_src_b.db");
  remove("cap_src_b.db-wal");
  remove("cap_src_b.db-shm");
  remove("cap_dst_b.db");
  remove("cap_src_c.db");
  remove("cap_src_c.db-wal");
  remove("cap_src_c.db-shm");
  remove("cap_dst_c.db");
  stub_reset();
}

static void assert_logical_equivalence(const char *src_path, const char *dst_path,
                                       const char *table_name, int expected_count) {
  sqlite3 *s_src = NULL;
  sqlite3 *s_dst = NULL;
  assert(sqlite3_open_v2(src_path, &s_src, SQLITE_OPEN_READONLY, NULL) == SQLITE_OK);
  assert(sqlite3_open_v2(dst_path, &s_dst, SQLITE_OPEN_READONLY, NULL) == SQLITE_OK);

  // 1. Row count match
  char count_sql[256];
  snprintf(count_sql, sizeof(count_sql), "SELECT COUNT(*) FROM \"%s\"", table_name);
  sqlite3_stmt *st_src = NULL, *st_dst = NULL;
  assert(sqlite3_prepare_v2(s_src, count_sql, -1, &st_src, NULL) == SQLITE_OK);
  assert(sqlite3_prepare_v2(s_dst, count_sql, -1, &st_dst, NULL) == SQLITE_OK);
  assert(sqlite3_step(st_src) == SQLITE_ROW);
  assert(sqlite3_step(st_dst) == SQLITE_ROW);
  long long c_src = sqlite3_column_int64(st_src, 0);
  long long c_dst = sqlite3_column_int64(st_dst, 0);
  sqlite3_finalize(st_src);
  sqlite3_finalize(st_dst);

  assert(c_src == expected_count);
  assert(c_dst == expected_count);

  // 2. Schema match
  char schema_sql[256];
  snprintf(schema_sql, sizeof(schema_sql),
           "SELECT sql FROM sqlite_master WHERE type='table' AND name='%s'", table_name);
  assert(sqlite3_prepare_v2(s_src, schema_sql, -1, &st_src, NULL) == SQLITE_OK);
  assert(sqlite3_prepare_v2(s_dst, schema_sql, -1, &st_dst, NULL) == SQLITE_OK);
  assert(sqlite3_step(st_src) == SQLITE_ROW);
  assert(sqlite3_step(st_dst) == SQLITE_ROW);
  const char *sch_src = (const char *)sqlite3_column_text(st_src, 0);
  const char *sch_dst = (const char *)sqlite3_column_text(st_dst, 0);
  assert(sch_src && sch_dst);
  assert(strcmp(sch_src, sch_dst) == 0);
  sqlite3_finalize(st_src);
  sqlite3_finalize(st_dst);

  // 3. PK set and values match
  char select_sql[256];
  snprintf(select_sql, sizeof(select_sql), "SELECT id, name FROM \"%s\" ORDER BY id", table_name);
  assert(sqlite3_prepare_v2(s_src, select_sql, -1, &st_src, NULL) == SQLITE_OK);
  assert(sqlite3_prepare_v2(s_dst, select_sql, -1, &st_dst, NULL) == SQLITE_OK);

  int rows = 0;
  while (1) {
    int r1 = sqlite3_step(st_src);
    int r2 = sqlite3_step(st_dst);
    if (r1 == SQLITE_DONE && r2 == SQLITE_DONE) break;
    assert(r1 == SQLITE_ROW && r2 == SQLITE_ROW);
    int id1 = sqlite3_column_int(st_src, 0);
    int id2 = sqlite3_column_int(st_dst, 0);
    const char *v1 = (const char *)sqlite3_column_text(st_src, 1);
    const char *v2 = (const char *)sqlite3_column_text(st_dst, 1);
    assert(id1 == id2);
    assert(strcmp(v1, v2) == 0);
    rows++;
  }
  assert(rows == expected_count);
  sqlite3_finalize(st_src);
  sqlite3_finalize(st_dst);

  // 4. Hydration target state: last_applied_lsn <= manifest baseline
  char mkey[512];
  snprintf(mkey, sizeof(mkey), "%s/manifest.json", PREFIX);
  char *mbody = NULL;
  size_t mlen = 0;
  assert(stub_get(mkey, &mbody, &mlen));
  const char *b_str = strstr(mbody, "\"baseline_lsn\":");
  assert(b_str != NULL);
  uint64_t manifest_baseline = (uint64_t)strtoull(b_str + 15, NULL, 10);
  free(mbody);

  char meta_sql[] = "SELECT last_applied_lsn FROM _arkilian_hydration LIMIT 1";
  if (sqlite3_prepare_v2(s_dst, meta_sql, -1, &st_dst, NULL) == SQLITE_OK) {
    if (sqlite3_step(st_dst) == SQLITE_ROW) {
      uint64_t applied = (uint64_t)sqlite3_column_int64(st_dst, 0);
      assert(applied > 0);
      assert(applied <= manifest_baseline);
    }
    sqlite3_finalize(st_dst);
  }

  sqlite3_close(s_src);
  sqlite3_close(s_dst);
}

// ── Test A: Successful capture-gap recovery via snapshot ────────────
static void test_capture_gap_successful_recovery(void) {
  printf("  --- Test A: successful capture-gap recovery via snapshot ---\n");
  cleanup_files();
  stub_reset();
  set_s3_env();
  setenv("ARKILIAN_MAX_QUEUE_DEPTH", "10", 1);
  setenv("ARKILIAN_CHUNK_INTERVAL_SEC", "1", 1);
  setenv("ARKILIAN_MANIFEST_INTERVAL_SEC", "1", 1);
  setenv("ARKILIAN_BACKUP_INTERVAL", "999999", 1);

  arkilian *db = NULL;
  assert(db_init(&db, "cap_src_a.db") == 0);
  assert(db_exec(db, "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)") == SQLITE_OK);

  // Initially healthy
  assert(db_backup_capture_paused(db) == 0);

  // 1. Induce remote S3 outage: PUTs fail with 500
  stub_set_status_override_put(500);

  // 2. Fill queue to cap 10
  for (int i = 1; i <= 10; i++) {
    char sql[128];
    snprintf(sql, sizeof(sql), "INSERT INTO users (id, name) VALUES (%d, 'user-%d')", i, i);
    assert(db_exec(db, sql) == SQLITE_OK);
  }

  // Poll until queue_depth == 10 and capture_paused == 1
  int waited = 0;
  while ((db_backup_queue_depth(db) < 10 || !db_backup_capture_paused(db)) && waited < 10000) {
    usleep(50000);
    waited += 50;
  }
  assert(db_backup_queue_depth(db) >= 10);
  assert(db_backup_capture_paused(db) == 1);
  assert((db_backup_health_flags(db) & ARK_HF_NO_CAPTURE_GAP) == 0);
  assert(db_backup_is_healthy(db) == 0); // Health RED!

  // 3. Perform uncaptured writes (rows 11..20) — dropped by trigger CDC gate
  for (int i = 11; i <= 20; i++) {
    char sql[128];
    snprintf(sql, sizeof(sql), "INSERT INTO users (id, name) VALUES (%d, 'user-%d')", i, i);
    assert(db_exec(db, sql) == SQLITE_OK);
  }

  // Confirm capture_paused remains 1 and health RED
  assert(db_backup_capture_paused(db) == 1);
  assert(db_backup_is_healthy(db) == 0);

  // 4. Partial S3 recovery: clear PUT override
  stub_set_status_override_put(0);

  // Poll until queue drains below cap (incremental WAL chunk shipped)
  waited = 0;
  while (db_backup_queue_depth(db) >= 10 && waited < 10000) {
    usleep(50000);
    waited += 50;
  }
  assert(db_backup_queue_depth(db) < 10);

  // 5. INVARIANT ASSERTION: partial incremental chunk shipping SUCCEEDED,
  // but capture_paused MUST STILL BE 1 and health MUST STILL BE RED!
  assert(db_backup_capture_paused(db) == 1);
  assert((db_backup_health_flags(db) & ARK_HF_NO_CAPTURE_GAP) == 0);
  assert(db_backup_is_healthy(db) == 0);

  // Confirm chunk exists on S3
  assert(stub_contains("chunks/lsn_"));

  // 6. Run snapshot cycle covering the gap
  int rc = arkilian_run_snapshot_cycle(db);
  assert(rc == SQLITE_OK);

  // 7. INVARIANT ASSERTION: Snapshot and manifest publication succeeded.
  // capture_paused MUST BE CLEARED to 0, and health MUST BE GREEN!
  assert(db_backup_capture_paused(db) == 0);
  assert((db_backup_health_flags(db) & ARK_HF_NO_CAPTURE_GAP) != 0);
  assert(db_backup_is_healthy(db) == 1); // Health GREEN!

  // 8. Hydrate and verify logical equivalence
  remove("cap_dst_a.db");
  int hrc = arkilian_hydrate_s3("cap_dst_a.db", g_endpoint, BUCKET, "us-east-1",
                                "test-access", "test-secret", PREFIX, NULL, NULL);
  assert(hrc == HYDRATION_OK);

  assert_logical_equivalence("cap_src_a.db", "cap_dst_a.db", "users", 20);

  db_close(db);
  cleanup_files();
  printf("  --- Test A passed: OK ---\n");
}

// ── Test B: Failed manifest publication and safe retry ──────────────
static void test_capture_gap_manifest_failure_and_retry(void) {
  printf("  --- Test B: snapshot upload succeeds, manifest publish fails, retry succeeds ---\n");
  cleanup_files();
  stub_reset();
  set_s3_env();
  setenv("ARKILIAN_MAX_QUEUE_DEPTH", "10", 1);
  setenv("ARKILIAN_CHUNK_INTERVAL_SEC", "1", 1);
  setenv("ARKILIAN_MANIFEST_INTERVAL_SEC", "1", 1);
  setenv("ARKILIAN_BACKUP_INTERVAL", "999999", 1);

  arkilian *db = NULL;
  assert(db_init(&db, "cap_src_b.db") == 0);
  assert(db_exec(db, "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)") == SQLITE_OK);

  // 1. Induce S3 outage: PUTs fail
  stub_set_status_override_put(500);

  // 2. Fill queue to cap 10
  for (int i = 1; i <= 10; i++) {
    char sql[128];
    snprintf(sql, sizeof(sql), "INSERT INTO users (id, name) VALUES (%d, 'user-%d')", i, i);
    assert(db_exec(db, sql) == SQLITE_OK);
  }

  int waited = 0;
  while ((db_backup_queue_depth(db) < 10 || !db_backup_capture_paused(db)) && waited < 10000) {
    usleep(50000);
    waited += 50;
  }
  assert(db_backup_queue_depth(db) >= 10);
  assert(db_backup_capture_paused(db) == 1);
  assert(db_backup_is_healthy(db) == 0);

  // 3. Uncaptured writes (rows 11..20)
  for (int i = 11; i <= 20; i++) {
    char sql[128];
    snprintf(sql, sizeof(sql), "INSERT INTO users (id, name) VALUES (%d, 'user-%d')", i, i);
    assert(db_exec(db, sql) == SQLITE_OK);
  }

  // 4. Partial recovery: clear PUT override
  stub_set_status_override_put(0);

  // Poll until queue drains below cap
  waited = 0;
  while (db_backup_queue_depth(db) >= 10 && waited < 10000) {
    usleep(50000);
    waited += 50;
  }
  assert(db_backup_queue_depth(db) < 10);

  // Partial shipping succeeded, capture_paused must remain 1
  assert(db_backup_capture_paused(db) == 1);
  assert(db_backup_is_healthy(db) == 0);

  // 5. Target failure: snapshot PUT will succeed, but manifest PUT fails with 500!
  stub_set_status_override_manifest(500);

  // Run snapshot cycle: snapshot upload succeeds, but manifest publish fails
  int rc = arkilian_run_snapshot_cycle(db);
  (void)rc;

  // 6. INVARIANT ASSERTION:
  // - Snapshot backup.sqlite is on S3:
  assert(stub_contains("backup.sqlite"));
  // - capture_paused MUST REMAIN 1 because manifest publication failed!
  assert(db_backup_capture_paused(db) == 1);
  // - health MUST REMAIN RED!
  assert(db_backup_is_healthy(db) == 0);

  // 7. Clear manifest failure: simulate recovery
  stub_set_status_override_manifest(0);

  // 8. Retry snapshot cycle
  rc = arkilian_run_snapshot_cycle(db);
  assert(rc == SQLITE_OK);

  // 9. INVARIANT ASSERTION:
  // Manifest publication succeeded on retry.
  // capture_paused MUST BE 0 and health GREEN!
  assert(db_backup_capture_paused(db) == 0);
  assert(db_backup_is_healthy(db) == 1);

  // 10. Hydrate and verify logical equivalence
  remove("cap_dst_b.db");
  int hrc = arkilian_hydrate_s3("cap_dst_b.db", g_endpoint, BUCKET, "us-east-1",
                                "test-access", "test-secret", PREFIX, NULL, NULL);
  assert(hrc == HYDRATION_OK);

  assert_logical_equivalence("cap_src_b.db", "cap_dst_b.db", "users", 20);

  db_close(db);
  cleanup_files();
  printf("  --- Test B passed: OK ---\n");
}

// ── Test C: manifest.json succeeds, manifest.sig fails, retry succeeds ──
static void test_capture_gap_sig_failure_and_retry(void) {
  printf("  --- Test C: snapshot upload succeeds, manifest.json succeeds, manifest.sig fails, retry succeeds ---\n");
  cleanup_files();
  stub_reset();
  set_s3_env();
  setenv("ARKILIAN_MAX_QUEUE_DEPTH", "10", 1);
  setenv("ARKILIAN_CHUNK_INTERVAL_SEC", "1", 1);
  setenv("ARKILIAN_MANIFEST_INTERVAL_SEC", "1", 1);
  setenv("ARKILIAN_BACKUP_INTERVAL", "999999", 1);

  arkilian *db = NULL;
  assert(db_init(&db, "cap_src_c.db") == 0);
  assert(db_exec(db, "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)") == SQLITE_OK);

  // 1. Induce S3 outage: PUTs fail
  stub_set_status_override_put(500);

  // 2. Fill queue to cap 10
  for (int i = 1; i <= 10; i++) {
    char sql[128];
    snprintf(sql, sizeof(sql), "INSERT INTO users (id, name) VALUES (%d, 'user-%d')", i, i);
    assert(db_exec(db, sql) == SQLITE_OK);
  }

  int waited = 0;
  while ((db_backup_queue_depth(db) < 10 || !db_backup_capture_paused(db)) && waited < 10000) {
    usleep(50000);
    waited += 50;
  }
  assert(db_backup_queue_depth(db) >= 10);
  assert(db_backup_capture_paused(db) == 1);
  assert(db_backup_is_healthy(db) == 0);

  // 3. Uncaptured writes (rows 11..20)
  for (int i = 11; i <= 20; i++) {
    char sql[128];
    snprintf(sql, sizeof(sql), "INSERT INTO users (id, name) VALUES (%d, 'user-%d')", i, i);
    assert(db_exec(db, sql) == SQLITE_OK);
  }

  // 4. Partial recovery: clear PUT override
  stub_set_status_override_put(0);

  // Poll until queue drains below cap
  waited = 0;
  while (db_backup_queue_depth(db) >= 10 && waited < 10000) {
    usleep(50000);
    waited += 50;
  }
  assert(db_backup_queue_depth(db) < 10);
  assert(db_backup_capture_paused(db) == 1);
  assert(db_backup_is_healthy(db) == 0);

  // 5. Target failure: snapshot PUT and manifest.json PUT succeed,
  // but manifest.sig PUT fails with 500!
  stub_set_status_override_manifest_sig(500);

  // Run snapshot cycle: snapshot upload and manifest.json succeed, but manifest.sig fails
  int rc = arkilian_run_snapshot_cycle(db);
  (void)rc;

  // 6. INVARIANT ASSERTION:
  // - Snapshot backup.sqlite is on S3:
  assert(stub_contains("backup.sqlite"));
  // - manifest.json was uploaded:
  assert(stub_contains("manifest.json"));
  // - capture_paused MUST REMAIN 1 because the signature publication failed!
  assert(db_backup_capture_paused(db) == 1);
  // - health MUST REMAIN RED!
  assert(db_backup_is_healthy(db) == 0);

  // 7. Clear manifest.sig failure: simulate recovery
  stub_set_status_override_manifest_sig(0);

  // 8. Retry snapshot cycle
  rc = arkilian_run_snapshot_cycle(db);
  assert(rc == SQLITE_OK);

  // 9. INVARIANT ASSERTION:
  // Manifest & signature publication succeeded on retry.
  // capture_paused MUST BE 0 and health GREEN!
  assert(db_backup_capture_paused(db) == 0);
  assert(db_backup_is_healthy(db) == 1);

  // 10. Hydrate and verify logical equivalence
  remove("cap_dst_c.db");
  int hrc = arkilian_hydrate_s3("cap_dst_c.db", g_endpoint, BUCKET, "us-east-1",
                                "test-access", "test-secret", PREFIX, NULL, NULL);
  assert(hrc == HYDRATION_OK);

  assert_logical_equivalence("cap_src_c.db", "cap_dst_c.db", "users", 20);

  db_close(db);
  cleanup_files();
  printf("  --- Test C passed: OK ---\n");
}

int main(void) {
  printf("=== capture gap lifecycle regression tests ===\n\n");
  stub_start();

  test_capture_gap_successful_recovery();
  test_capture_gap_manifest_failure_and_retry();
  test_capture_gap_sig_failure_and_retry();

  printf("\nAll capture-gap lifecycle tests passed!\n");
  return 0;
}
