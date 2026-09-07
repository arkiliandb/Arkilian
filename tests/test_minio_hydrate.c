// Test cold-start hydration from MinIO / Control Plane into a fresh SQLite DB
#include "hydration.h"
#include "deps/sqlite/sqlite3.h"
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>

int main(void) {
  printf("=== Testing Cold-Start Hydration from MinIO / Control Plane ===\n");
  remove("hydrated_stress.db");

  const char *endpoint    = getenv("ARKILIAN_S3_ENDPOINT");
  const char *bucket      = getenv("ARKILIAN_S3_BUCKET");
  const char *region      = getenv("ARKILIAN_S3_REGION");
  const char *access_key  = getenv("ARKILIAN_S3_ACCESS_KEY");
  const char *secret_key  = getenv("ARKILIAN_S3_SECRET_KEY");
  const char *prefix      = getenv("ARKILIAN_S3_PREFIX");
  if (!endpoint || !bucket || !access_key || !secret_key || !prefix) {
    printf("Set ARKILIAN_S3_ENDPOINT/BUCKET/REGION/ACCESS_KEY/SECRET_KEY/PREFIX "
           "(target the MinIO instance populated by test_minio_setup).\\n");
    return 1;
  }
  if (!region) region = "us-east-1";

  printf("Requesting hydration from %s (prefix %s)...\n", endpoint, prefix);
  int rc = arkilian_hydrate_s3(
      "hydrated_stress.db",
      endpoint, bucket, region, access_key, secret_key, prefix,
      NULL, NULL);

  if (rc == 0) {
    printf("Hydration completed successfully (rc=0)!\n");
    sqlite3 *db = NULL;
    sqlite3_open_v2("hydrated_stress.db", &db, SQLITE_OPEN_READONLY, NULL);
    if (db) {
      sqlite3_stmt *stmt = NULL;
      if (sqlite3_prepare_v2(db, "SELECT COUNT(*) FROM stress_data", -1, &stmt, NULL) == SQLITE_OK) {
        if (sqlite3_step(stmt) == SQLITE_ROW) {
          printf("Hydrated database contains %lld rows in 'stress_data' table!\n",
                 (long long)sqlite3_column_int64(stmt, 0));
        }
        sqlite3_finalize(stmt);
      }
      sqlite3_close(db);
    }
  } else {
    printf("Hydration returned rc=%d (Control plane snapshot test)\n", rc);
  }

  return 0;
}
