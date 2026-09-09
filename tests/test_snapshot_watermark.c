// P0 regression: the snapshot-baseline invariant.
//
// INVARIANT: if Arkilian publishes a snapshot with baseline LSN X, the
// snapshot file contains every change through X, and the manifest retains
// every chunk beyond X. The pre-fix code read the pruning watermark from
// the (mutable) manifest registry AFTER backup_database() returned, so a
// chunk shipped by the flush thread DURING the copy was declared "already
// inside the snapshot", pruned from the registry, and then skipped by
// hydration — a silent restore gap under exactly the workload this
// product exists for (continuous writes).
//
// This test drives the REAL snapshot cycle (arkilian_run_snapshot_cycle)
// against the in-process S3 stub, with a copy hook that records a chunk
// in the manifest registry INSIDE the copy window (what the flush thread
// does concurrently), then asserts:
//   1. published baseline == the PRE-copy watermark (not the post-copy one)
//   2. the mid-copy chunk record survived pruning
//   3. the pre-copy chunk record was pruned
//   4. full restore equivalence: hydrated DB = snapshot rows + mid-copy
//      chunk rows — nothing missing, count exact
// POSIX-only (stub server uses BSD sockets).

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

// Production copy step (class.c); the hook delegates to it after
// injecting the concurrent-shipping workload.
extern int backup_database(sqlite3 *pSource, const char *zFilename,
                           volatile int *shutdown_flag);

static arkilian *g_db = NULL;

// Runs INSIDE the copy window (the snapshot thread's copy step). Simulates
// the flush thread shipping rows 21..30 as chunk lsn 22..31 while the
// snapshot copy is in progress, then performs the real copy.
static int copy_hook(sqlite3 *src, const char *dest, volatile int *flag) {
  char body[2048];
  size_t off = 0;
  for (int i = 21; i <= 30; i++) {
    off += (size_t)snprintf(body + off, sizeof(body) - off,
                            "INSERT OR REPLACE INTO users (id, name) "
                            "VALUES (%d, 'user-%d');\n", i, i);
  }
  char key[512];
  snprintf(key, sizeof(key), "%s/chunks/lsn_0000000022_0000000031.sql",
           PREFIX);
  stub_put(key, body, off);
  char sha[65];
  ark_sha256_hex(body, off, sha);
  // The exact call the flush thread makes after a successful PUT.
  assert(arkilian_registry_record(g_db, key, sha, 22, 31) == 0);
  return backup_database(src, dest, flag);
}

static long long count_rows(const char *db_path, const char *table) {
  sqlite3 *db = NULL;
  if (sqlite3_open_v2(db_path, &db, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK)
    return -1;
  sqlite3_stmt *stmt = NULL;
  char sql[256];
  snprintf(sql, sizeof(sql), "SELECT COUNT(*) FROM \"%s\"", table);
  long long n = -1;
  if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) == SQLITE_OK &&
      sqlite3_step(stmt) == SQLITE_ROW)
    n = sqlite3_column_int64(stmt, 0);
  sqlite3_finalize(stmt);
  sqlite3_close(db);
  return n;
}

static void cleanup_files(void) {
  remove("wm_src.db");
  remove("wm_dst.db");
  stub_reset();
}

int main(void) {
  printf("=== snapshot watermark invariant tests ===\n");
  stub_start();
  set_s3_env();
  // Disable the automatic hourly thread (we drive cycles manually) and
  // make the flusher ship promptly.
  setenv("ARKILIAN_BACKUP_INTERVAL", "999999", 1);
  setenv("ARKILIAN_CHUNK_INTERVAL_SEC", "1", 1);

  cleanup_files();
  assert(db_init(&g_db, "wm_src.db") == 0);
  assert(db_exec(g_db, "CREATE TABLE users (id INTEGER PRIMARY KEY, "
                       "name TEXT)") == SQLITE_OK);
  char sql[128];
  for (int i = 1; i <= 20; i++) {
    snprintf(sql, sizeof(sql),
             "INSERT INTO users (id, name) VALUES (%d, 'user-%d')", i, i);
    assert(db_exec(g_db, sql) == SQLITE_OK);
  }

  // Outbox ids: 1 = the CREATE TABLE DDL capture, 2..21 = the inserts.
  // Wait until the flusher shipped through outbox id 21 and the manifest
  // names it — the pre-copy registry state this test anchors on.
  assert(stub_manifest_contains("\"lsn_end\":21}", 20));
  for (int i = 0; i < 200; i++) {
    if (db_backup_queue_depth(g_db) == 0) break;
    usleep(50000);
  }
  assert(db_backup_queue_depth(g_db) == 0);

  // Run ONE real snapshot cycle with the mid-copy injection.
  arkilian_snapshot_copy_hook = copy_hook;
  int rc = arkilian_run_snapshot_cycle(g_db);
  arkilian_snapshot_copy_hook = NULL;
  if (rc != SQLITE_OK) fprintf(stderr, "DIAG cycle rc=%d\n", rc);
  assert(rc == SQLITE_OK);

  // ── Invariant 1: published baseline == PRE-copy watermark (21). The
  // pre-fix code read the watermark after the copy → published 31.
  char mkey[512];
  snprintf(mkey, sizeof(mkey), "%s/manifest.json", PREFIX);
  char *body = NULL;
  size_t blen = 0;
  assert(stub_get(mkey, &body, &blen));
  assert(strstr(body, "\"baseline_lsn\":21") != NULL);
  // ── Invariant 2: the mid-copy chunk record survived pruning — its rows
  // are NOT in the snapshot, so hydration must still replay it.
  assert(strstr(body, "lsn_0000000022_0000000031.sql") != NULL);
  // ── Invariant 3: the pre-copy chunk record was pruned (covered by the
  // snapshot — replaying it would be redundant).
  assert(strstr(body, "\"lsn_end\":21}") == NULL);
  free(body);

  // ── Invariant 4: restore equivalence. Rows 1..20 come from the
  // snapshot, rows 21..30 from the retained mid-copy chunk: exactly 30,
  // none silently omitted.
  remove("wm_dst.db");
  int hrc = arkilian_hydrate_s3("wm_dst.db", g_endpoint, BUCKET, "us-east-1",
                                "test-access", "test-secret", PREFIX,
                                NULL, NULL);
  if (hrc != HYDRATION_OK) fprintf(stderr, "DIAG hydrate rc=%d\n", hrc);
  assert(hrc == HYDRATION_OK);
  assert(count_rows("wm_dst.db", "users") == 30);

  // Telemetry sanity: chunk_count is a real counter (1 real flush; the
  // hook-injected record is bookkeeping, not a flush).
  assert(db_backup_chunk_count(g_db) == 1);

  db_close(g_db);
  cleanup_files();
  printf("  watermark invariant + mid-copy retention + restore equivalence: OK\n");
  printf("\nAll snapshot-watermark tests passed!\n");
  return 0;
}
