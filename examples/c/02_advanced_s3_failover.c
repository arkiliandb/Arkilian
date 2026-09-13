/**
 * Arkilian C SDK - Advanced S3 Streaming & Cold-Start Hydration
 *
 * Demonstrates:
 * 1. Cloud-native WAL streaming to S3/MinIO via Arkilian C engine
 * 2. Monitoring sidecar telemetry (health, bitmask flags, queue depth)
 * 3. Zero-lag WAL chunk flushes via db_wal_flush()
 * 4. Catastrophic primary host loss simulation
 * 5. Cold-start disaster recovery via arkilian_hydrate_s3()
 * 6. Validating data integrity and parity on recovered instance
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "arkilian/class.h"
#include "arkilian/hydration.h"

#define PRIMARY_DB  "primary_production.sqlite"
#define RESTORED_DB "hydrated_recovery.sqlite"

static void cleanup_db(const char *path) {
    char buf[512];
    unlink(path);
    snprintf(buf, sizeof(buf), "%s-wal", path);
    unlink(buf);
    snprintf(buf, sizeof(buf), "%s-shm", path);
    unlink(buf);
}

static const char *get_env(const char *key, const char *fallback) {
    const char *val = getenv(key);
    return (val && strlen(val) > 0) ? val : fallback;
}

int main(void) {
    printf("=== Arkilian C SDK: Advanced S3 Streaming & Cold-Start Hydration ===\n\n");

    const char *endpoint = get_env("ARKILIAN_S3_ENDPOINT", "http://127.0.0.1:9000");
    const char *bucket = get_env("ARKILIAN_S3_BUCKET", "arkilian-test-bucket");
    const char *region = get_env("ARKILIAN_S3_REGION", "us-east-1");
    const char *access_key = get_env("ARKILIAN_S3_ACCESS_KEY", "minioadmin");
    const char *secret_key = get_env("ARKILIAN_S3_SECRET_KEY", "minioadmin");
    const char *prefix = get_env("ARKILIAN_S3_PREFIX", "c-demo");
    const char *hmac_key = get_env("ARKILIAN_MANIFEST_HMAC_KEY", "super-secret-hmac-key-for-manifests");

    // Configure environment for the Arkilian background sidecar worker
    setenv("ARKILIAN_ENABLE_BACKUP", "1", 1);
    setenv("ARKILIAN_S3_ENDPOINT", endpoint, 1);
    setenv("ARKILIAN_S3_BUCKET", bucket, 1);
    setenv("ARKILIAN_S3_REGION", region, 1);
    setenv("ARKILIAN_S3_ACCESS_KEY", access_key, 1);
    setenv("ARKILIAN_S3_SECRET_KEY", secret_key, 1);
    setenv("ARKILIAN_S3_PREFIX", prefix, 1);
    setenv("ARKILIAN_MANIFEST_HMAC_KEY", hmac_key, 1);
    setenv("ARKILIAN_CHUNK_INTERVAL_SEC", "1", 1);
    setenv("ARKILIAN_MANIFEST_INTERVAL_SEC", "1", 1);

    printf("[Config] Target S3 Endpoint: %s\n", endpoint);
    printf("[Config] Bucket: %s | Prefix: %s\n\n", bucket, prefix);

    cleanup_db(PRIMARY_DB);
    cleanup_db(RESTORED_DB);

    // -------------------------------------------------------------
    // Phase 1: Primary Database Workload with Real-Time S3 Streaming
    // -------------------------------------------------------------
    printf("[Phase 1] Opening Primary Node with S3 WAL Streaming...\n");
    arkilian *db = NULL;
    int rc = db_init(&db, PRIMARY_DB);
    if (rc != 0 || !db) {
        fprintf(stderr, "Failed to initialize primary database (rc=%d): %s\n",
                rc, db ? db_errmsg(db) : "allocation failed");
        if (db) db_close(db);
        return 1;
    }

    // Inspect sidecar telemetry
    printf("  Telemetry -> Sidecar Healthy: %s\n", db_backup_is_healthy(db) ? "true" : "false");
    printf("  Telemetry -> Health Flags: 0x%x\n", db_backup_health_flags(db));
    printf("  Telemetry -> Queue Depth: %d\n", db_backup_queue_depth(db));

    printf("\n[Phase 1] Creating schema and streaming flight telemetry...\n");
    const char *schema_sql =
        "CREATE TABLE IF NOT EXISTS flight_telemetry ("
        "  record_id INTEGER PRIMARY KEY AUTOINCREMENT,"
        "  flight_no TEXT NOT NULL,"
        "  altitude_ft REAL NOT NULL,"
        "  airspeed_knots REAL NOT NULL,"
        "  status TEXT NOT NULL,"
        "  logged_at DATETIME DEFAULT CURRENT_TIMESTAMP"
        ");";

    if (db_exec(db, schema_sql) != 0) {
        fprintf(stderr, "Schema creation error: %s\n", db_errmsg(db));
        db_close(db);
        return 1;
    }

    int total_records = 30;
    const char *insert_sql =
        "INSERT INTO flight_telemetry (flight_no, altitude_ft, airspeed_knots, status) VALUES (?, ?, ?, ?);";

    db_begin(db);
    for (int i = 1; i <= total_records; i++) {
        char flight_no[32];
        snprintf(flight_no, sizeof(flight_no), "ARK-%03d", 100 + (i % 6));
        double altitude = 15000.0 + (double)(i * 450);
        double speed = 420.0 + (double)(i * 5);
        const char *status = (i % 8 == 0) ? "CLIMB" : "CRUISE";

        db_prepare(db, insert_sql);
        db_bind_text(db, 1, flight_no);
        db_bind_double(db, 2, altitude);
        db_bind_double(db, 3, speed);
        db_bind_text(db, 4, status);
        db_step(db);
        db_finalize(db);
    }
    db_commit(db);
    printf("  ✓ Committed %d flight telemetry records in an atomic transaction.\n", total_records);

    // Flush WAL chunk to outbox & allow sidecar worker to ship to S3
    printf("\n[Phase 1] Triggering explicit WAL flush to S3 sidecar...\n");
    db_wal_flush(db);

    printf("  Waiting for S3 sidecar outbox to drain...\n");
    for (int attempt = 0; attempt < 20; attempt++) {
        usleep(500000); // 500ms
        if (db_backup_queue_depth(db) == 0) {
            break;
        }
    }
    // Allow manifest publish to finalize
    usleep(1500000); // 1.5s

    printf("  Telemetry -> Queue Depth: %d\n", db_backup_queue_depth(db));
    printf("  Telemetry -> Sidecar Healthy: %s\n", db_backup_is_healthy(db) ? "true" : "false");

    db_prepare(db, "SELECT COUNT(*) FROM flight_telemetry;");
    db_step(db);
    sqlite3_int64 primary_count = db_column_int64(db, 0);
    db_finalize(db);
    printf("  Primary DB confirmed record count: %lld\n", (long long)primary_count);

    printf("[Phase 1] Closing primary database connection.\n");
    db_close(db);

    // -------------------------------------------------------------
    // Phase 2: Disaster Simulation
    // -------------------------------------------------------------
    printf("\n[Phase 2] SIMULATING DISASTER: Destroying primary host!\n");
    printf("  Purging local files: %s...\n", PRIMARY_DB);
    cleanup_db(PRIMARY_DB);
    printf("  ✓ Primary local database destroyed. Zero local state remains.\n");

    // -------------------------------------------------------------
    // Phase 3: Cold-Start Hydration from MinIO S3
    // -------------------------------------------------------------
    printf("\n[Phase 3] Starting Cold-Start Hydration from MinIO S3...\n");
    printf("  Target path: %s\n", RESTORED_DB);

    int hyd_rc = arkilian_hydrate_s3(
        RESTORED_DB,
        endpoint,
        bucket,
        region,
        access_key,
        secret_key,
        prefix,
        NULL,
        NULL
    );

    if (hyd_rc != HYDRATION_OK) {
        fprintf(stderr, "  ✗ Hydration failed with error code: %d\n", hyd_rc);
        return 1;
    }
    printf("  ✓ Hydration completed successfully!\n");

    // -------------------------------------------------------------
    // Phase 4: Data Parity & Integrity Verification
    // -------------------------------------------------------------
    printf("\n[Phase 4] Opening Hydrated Database to verify integrity...\n");
    setenv("ARKILIAN_ENABLE_BACKUP", "0", 1);
    arkilian *recovered_db = NULL;
    rc = db_init(&recovered_db, RESTORED_DB);
    if (rc != 0 || !recovered_db) {
        fprintf(stderr, "Failed to open recovered database (rc=%d): %s\n",
                rc, recovered_db ? db_errmsg(recovered_db) : "allocation failed");
        if (recovered_db) db_close(recovered_db);
        return 1;
    }

    db_prepare(recovered_db, "SELECT COUNT(*) FROM flight_telemetry;");
    db_step(recovered_db);
    sqlite3_int64 recovered_count = db_column_int64(recovered_db, 0);
    db_finalize(recovered_db);
    printf("  Recovered DB record count: %lld\n", (long long)recovered_count);

    if (recovered_count != primary_count) {
        fprintf(stderr, "  ✗ Data mismatch! Expected %lld, got %lld\n",
                (long long)primary_count, (long long)recovered_count);
        db_close(recovered_db);
        return 1;
    }

    printf("\n  Sample records from recovered instance:\n");
    db_prepare(recovered_db, "SELECT record_id, flight_no, altitude_ft, airspeed_knots, status FROM flight_telemetry LIMIT 5;");
    while (db_step(recovered_db) == 100) {
        printf("    - Record #%02lld | Flight: %-8s | Alt: %7.0f ft | Spd: %5.0f kts | Status: %s\n",
               (long long)db_column_int64(recovered_db, 0),
               db_column_text(recovered_db, 1),
               db_column_double(recovered_db, 2),
               db_column_double(recovered_db, 3),
               db_column_text(recovered_db, 4));
    }
    db_finalize(recovered_db);

    db_close(recovered_db);
    cleanup_db(RESTORED_DB);

    printf("\n=== Disaster Recovery & Verification Finished Successfully! ===\n");
    return 0;
}
