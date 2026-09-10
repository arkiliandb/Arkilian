// Test cold-start hydration from MinIO / S3-compatible storage into a fresh SQLite DB
#include "hydration.h"
#include "deps/sqlite/sqlite3.h"
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>

static void cleanup_db(const char *path) {
  remove(path);
  char side[256];
  snprintf(side, sizeof(side), "%s-wal", path); remove(side);
  snprintf(side, sizeof(side), "%s-shm", path); remove(side);
  snprintf(side, sizeof(side), "%s-journal", path); remove(side);
  snprintf(side, sizeof(side), "%s.arklock", path); remove(side);
}

int main(void) {
  printf("=== Testing Cold-Start Hydration from MinIO / S3-compatible storage ===\n");
  cleanup_db("hydrated_stress.db");

  const char *endpoint    = getenv("ARKILIAN_S3_ENDPOINT");
  const char *bucket      = getenv("ARKILIAN_S3_BUCKET");
  const char *region      = getenv("ARKILIAN_S3_REGION");
  const char *access_key  = getenv("ARKILIAN_S3_ACCESS_KEY");
  const char *secret_key  = getenv("ARKILIAN_S3_SECRET_KEY");
  const char *prefix      = getenv("ARKILIAN_S3_PREFIX");
  if (!endpoint || !bucket || !access_key || !secret_key || !prefix) {
    printf("Set ARKILIAN_S3_ENDPOINT/BUCKET/REGION/ACCESS_KEY/SECRET_KEY/PREFIX "
           "(target the MinIO instance populated by test_minio_setup).\n");
    return 1;
  }
  if (!region) region = "us-east-1";

  printf("Requesting hydration from %s (prefix %s)...\n", endpoint, prefix);
  int rc = arkilian_hydrate_s3(
      "hydrated_stress.db",
      endpoint, bucket, region, access_key, secret_key, prefix,
      NULL, NULL);

  if (rc != 0) {
    fprintf(stderr, "FAIL: Hydration returned rc=%d\n", rc);
    cleanup_db("hydrated_stress.db");
    return 1;
  }

  printf("Hydration completed successfully (rc=0)!\n");
  sqlite3 *db = NULL;
  assert(sqlite3_open_v2("hydrated_stress.db", &db, SQLITE_OPEN_READONLY, NULL) == SQLITE_OK);
  assert(db != NULL);

  sqlite3_stmt *stmt = NULL;
  assert(sqlite3_prepare_v2(db, "SELECT COUNT(*) FROM stress_data", -1, &stmt, NULL) == SQLITE_OK);
  assert(sqlite3_step(stmt) == SQLITE_ROW);
  long long count = (long long)sqlite3_column_int64(stmt, 0);
  printf("Hydrated database contains %lld rows in 'stress_data' table!\n", count);
  sqlite3_finalize(stmt);
  sqlite3_close(db);

  assert(count > 0 && "Hydrated database has 0 rows in stress_data");
  cleanup_db("hydrated_stress.db");
  return 0;
}
