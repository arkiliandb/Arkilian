// tests/test_retention.c — storage retention for snapshots and chunks.
//
// Field report (1.6.1, Cloudflare R2): a bucket grew to 34 GB in a few
// days. Since 1.6.0 every snapshot is written under its own immutable key
// and nothing was ever deleted, so storage grew by one full database image
// per snapshot interval.
//
// Invariants:
//   1. After each snapshot publish, snapshots older than the previous
//      baseline and chunks ending at or below it are deleted.
//   2. A restore that already read the PREVIOUS manifest can still fetch
//      every object it references.
//   3. Keys Arkilian did not write (other names, other prefixes, a prefix
//      that merely starts with ours) are never deleted.
//   4. The latest restore stays complete and byte-correct.
//   5. Nothing is deleted when the remote manifest is not the one this
//      process published, or when ARKILIAN_RETENTION=0.
//   6. A bucket already full of 1.6.0/1.6.1 leftovers is cleaned up.

#include "class.h"
#include "hydration.h"
#include "sha256.h"
#include "ark_stub_s3.h"
#include "ark_test_env.h"
#include "deps/sqlite/sqlite3.h"
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define KEY "retention-test-key"
static const char *SRC = "rt_src.db";
static const char *DST = "rt_dst.db";

static void rm_db(const char *p) {
  char b[256];
  remove(p);
  snprintf(b, sizeof(b), "%s-wal", p); remove(b);
  snprintf(b, sizeof(b), "%s-shm", p); remove(b);
}

static int exists(const char *key) {
  char *d = NULL;
  size_t l = 0;
  int ok = stub_get(key, &d, &l);
  free(d);
  return ok;
}

static void put(const char *key, const char *data) {
  assert(strlen(key) < 512);  // the stub stores keys in 1 KiB slots
  stub_put(key, data, strlen(data));
}

static int count_with(const char *needle) {
  int n = 0;
  pthread_mutex_lock(&g_stub_store_mutex);
  for (int i = 0; i < g_stub_object_count; i++)
    if (strstr(g_stub_objects[i].key, needle)) n++;
  pthread_mutex_unlock(&g_stub_store_mutex);
  return n;
}

static char *manifest(void) {
  char k[512], *d = NULL;
  size_t l = 0;
  snprintf(k, sizeof(k), "%s/manifest.json", PREFIX);
  if (!stub_get(k, &d, &l)) return NULL;
  char *s = malloc(l + 1);
  memcpy(s, d, l);
  s[l] = '\0';
  free(d);
  return s;
}

// Every "s3_key":"..." in a manifest.
static int manifest_keys(const char *m, char keys[][512], int max) {
  int n = 0;
  for (const char *p = strstr(m, "\"s3_key\":\""); p && n < max;
       p = strstr(p + 1, "\"s3_key\":\"")) {
    p += 10;
    const char *e = strchr(p, '"');
    size_t len = (size_t)(e - p);
    if (len >= 512) len = 511;
    memcpy(keys[n], p, len);
    keys[n][len] = '\0';
    n++;
  }
  return n;
}

static long long rows(const char *path) {
  sqlite3 *db = NULL;
  long long n = -1;
  if (sqlite3_open_v2(path, &db, SQLITE_OPEN_READONLY, NULL) == SQLITE_OK) {
    sqlite3_stmt *st = NULL;
    if (sqlite3_prepare_v2(db, "SELECT count(*) FROM t", -1, &st, NULL) == SQLITE_OK &&
        sqlite3_step(st) == SQLITE_ROW)
      n = sqlite3_column_int64(st, 0);
    sqlite3_finalize(st);
  }
  if (db) sqlite3_close(db);
  return n;
}

static int hydrate(void) {
  rm_db(DST);
  return arkilian_hydrate_s3(DST, g_endpoint, BUCKET, "us-east-1",
                             "test-access", "test-secret", PREFIX, NULL, NULL);
}

static int next_id = 1;
static void write_rows(arkilian *db, int n) {
  for (int i = 0; i < n; i++) {
    char sql[128];
    snprintf(sql, sizeof(sql), "INSERT INTO t (id, v) VALUES (%d, 'row-%d')", next_id, next_id);
    next_id++;
    assert(db_exec(db, sql) == SQLITE_OK);
  }
}

// Ship everything, then publish a new snapshot baseline (runs retention).
static void cycle(arkilian *db) {
  for (int i = 0; i < 200 && db_backup_queue_depth(db) > 0; i++) {
    db_wal_flush(db);
    usleep(20000);
  }
  assert(db_backup_queue_depth(db) == 0);
  assert(arkilian_run_snapshot_cycle(db) == SQLITE_OK);
}

int main(void) {
  printf("=== storage retention ===\n");
  stub_start();
  stub_set_list_page(4);  // exercise ListObjectsV2 pagination
  set_s3_env();
  setenv("ARKILIAN_MANIFEST_HMAC_KEY", KEY, 1);
  setenv("ARKILIAN_ENABLE_BACKUP", "1", 1);
  setenv("ARKILIAN_CHUNK_INTERVAL_SEC", "1", 1);
  setenv("ARKILIAN_MANIFEST_INTERVAL_SEC", "1", 1);
  setenv("ARKILIAN_BACKUP_INTERVAL", "999999", 1);
  setenv("ARKILIAN_BACKUP_PATH", "rt_backup.sqlite", 1);
  unsetenv("ARKILIAN_RETENTION");
  rm_db(SRC);

  char k[600];
  // Leftovers of a 1.6.0/1.6.1 deployment: superseded snapshots and the
  // pre-1.6.0 fixed snapshot key (no manifest references them).
  snprintf(k, sizeof(k), "%s/snapshots/00000000000000000000_oldwriter.backup.sqlite", PREFIX);
  put(k, "stale-snapshot-image");
  snprintf(k, sizeof(k), "%s/backup.sqlite", PREFIX);
  put(k, "legacy-fixed-snapshot");
  // Keys Arkilian never wrote: must survive.
  const char *keep[] = {"%s/notes.txt", "%s/snapshots/readme.txt",
                        "%s/chunks/lsn_x.sql", "%s-other/snapshots/00000000000000000000_w.backup.sqlite",
                        "other-app/%s/snapshots/00000000000000000000_w.backup.sqlite"};
  static char keep_keys[5][256];
  for (int i = 0; i < 5; i++) {
    snprintf(keep_keys[i], sizeof(keep_keys[i]), keep[i], PREFIX);
    put(keep_keys[i], "not-arkilian");
  }

  arkilian *db = NULL;
  assert(db_init(&db, SRC) == 0);
  assert(db_exec(db, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)") == SQLITE_OK);
  for (int i = 0; i < 200 && !(db_backup_health_flags(db) & ARK_HF_MANIFEST_RESOLVED); i++)
    usleep(50000);
  assert(db_backup_health_flags(db) & ARK_HF_MANIFEST_RESOLVED);

  // Several snapshot generations with writes in between.
  char *prev_manifest = NULL;
  for (int gen = 0; gen < 6; gen++) {
    write_rows(db, 25);
    free(prev_manifest);
    prev_manifest = manifest();
    cycle(db);
    // 2. Every object the PREVIOUS manifest referenced still exists.
    if (prev_manifest) {
      char refs[64][512];
      int n = manifest_keys(prev_manifest, refs, 64);
      for (int i = 0; i < n; i++) {
        if (!exists(refs[i])) {
          fprintf(stderr, "generation %d: previous manifest object deleted: %s\n", gen, refs[i]);
          assert(0);
        }
      }
    }
  }
  free(prev_manifest);
  int snaps = count_with("/snapshots/") - 3;  // minus the three foreign keys containing it
  printf("  snapshots kept after 6 generations: %d (current + previous)\n", snaps);
  assert(snaps <= 2);
  snprintf(k, sizeof(k), "%s/snapshots/00000000000000000000_oldwriter.backup.sqlite", PREFIX);
  assert(!exists(k));
  snprintf(k, sizeof(k), "%s/backup.sqlite", PREFIX);
  assert(!exists(k));
  printf("  1.6.x leftovers (stale snapshots, legacy backup.sqlite) deleted\n");

  // Chunks: only those the current or previous restore point can need.
  {
    char *m = manifest();
    assert(m);
    char refs[64][512];
    int n = manifest_keys(m, refs, 64);
    int chunks = count_with("/chunks/lsn_") - 1;  // minus the foreign lsn_x.sql
    printf("  chunk objects kept: %d (manifest references %d objects)\n", chunks, n);
    assert(chunks <= 6);
    free(m);
  }

  // 3. Foreign keys untouched.
  for (int i = 0; i < 5; i++) assert(exists(keep_keys[i]));
  printf("  keys Arkilian did not write are untouched\n");
  assert(stub_list_count() > 6);  // listed every cycle, paginated

  // 4. The latest restore is complete.
  write_rows(db, 10);
  for (int i = 0; i < 200 && db_backup_queue_depth(db) > 0; i++) { db_wal_flush(db); usleep(20000); }
  usleep(1500000);
  db_wal_flush(db);
  sleep(2);
  assert(hydrate() == HYDRATION_OK);
  printf("  restore after retention: %lld rows (source %d)\n", rows(DST), next_id - 1);
  assert(rows(DST) == next_id - 1);

  // 5a. Another writer published between our publish and retention: the
  //     re-read manifest is not ours, so nothing is deleted.
  {
    char *m = manifest();
    assert(m);
    char *sk = strstr(m, "/snapshots/");
    assert(sk);
    sk[12] = sk[12] == '9' ? '8' : '9';  // a different snapshot key
    char *h = strstr(m, ",\"hmac\":\"");
    assert(h);
    strcpy(h, "}");
    char sig[65];
    ark_hmac_sha256_hex((const uint8_t *)KEY, strlen(KEY), m, strlen(m), sig);
    size_t L = strlen(m);
    m[L - 1] = '\0';
    char *env = malloc(L + 100);
    snprintf(env, L + 100, "%s,\"hmac\":\"%s\"}", m, sig);
    // Plant objects retention would otherwise delete.
    snprintf(k, sizeof(k), "%s/snapshots/00000000000000000001_ghost.backup.sqlite", PREFIX);
    put(k, "would-be-deleted");
    char ck[600];
    snprintf(ck, sizeof(ck), "%s/chunks/lsn_0000000001_0000000001_ghost.sql", PREFIX);
    put(ck, "would-be-deleted");
    stub_set_manifest_get_body(env);
    write_rows(db, 5);
    cycle(db);
    stub_set_manifest_get_body(NULL);
    assert(exists(k));
    assert(exists(ck));
    printf("  manifest replaced by another writer: retention skipped, nothing deleted\n");
    // Once the manifest is ours again, the next cycle cleans them up.
    write_rows(db, 5);
    cycle(db);
    write_rows(db, 5);
    cycle(db);
    assert(!exists(k));
    assert(!exists(ck));
    free(env);
    free(m);
  }
  db_close(db);

  // 5b. ARKILIAN_RETENTION=0: nothing deleted.
  {
    stub_reset();
    rm_db(SRC);
    next_id = 1;
    setenv("ARKILIAN_RETENTION", "0", 1);
    snprintf(k, sizeof(k), "%s/backup.sqlite", PREFIX);
    put(k, "legacy-fixed-snapshot");
    assert(db_init(&db, SRC) == 0);
    assert(db_exec(db, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)") == SQLITE_OK);
    for (int i = 0; i < 200 && !(db_backup_health_flags(db) & ARK_HF_MANIFEST_RESOLVED); i++)
      usleep(50000);
    for (int gen = 0; gen < 4; gen++) { write_rows(db, 5); cycle(db); }
    int snaps_off = count_with("/snapshots/");
    printf("  ARKILIAN_RETENTION=0: %d snapshots kept, legacy key %s\n", snaps_off,
           exists(k) ? "kept" : "DELETED");
    assert(snaps_off >= 4);
    assert(exists(k));
    db_close(db);
    unsetenv("ARKILIAN_RETENTION");
  }

  rm_db(SRC);
  rm_db(DST);
  remove("rt_backup.sqlite");
  stub_stop();
  printf("=== PASS ===\n");
  return 0;
}
