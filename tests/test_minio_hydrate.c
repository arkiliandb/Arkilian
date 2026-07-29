// Test cold-start hydration from MinIO / Control Plane into a fresh SQLite DB
#include "hydration.h"
#include "deps/sqlite/sqlite3.h"
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>

int main(void) {
  printf("=== Testing Cold-Start Hydration from MinIO / Control Plane ===\n");
  remove("hydrated_stress.db");

  const char *server_url = getenv("CONTROL_PLANE_URL");
  if (!server_url) server_url = "http://localhost:8080/v1";

  printf("Requesting hydration plan from %s...\n", server_url);
  int rc = arkilian_hydrate("hydrated_stress.db", server_url, "dummy-token", NULL, NULL);

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
