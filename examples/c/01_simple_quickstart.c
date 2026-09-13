/**
 * Arkilian C SDK - Simple Quickstart Example
 *
 * Demonstrates:
 * 1. Initializing an Arkilian embedded database (db_init)
 * 2. Creating schema (db_exec)
 * 3. Parameterized inserts via prepared statements (db_prepare / db_bind_* / db_step)
 * 4. Querying result rows and reading typed columns
 * 5. Atomic transactions (db_begin / db_commit / db_rollback)
 * 6. Proper resource disposal (db_close)
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "arkilian/class.h"

#define DB_PATH "quickstart.sqlite"

static void cleanup_db(const char *path) {
    char buf[512];
    unlink(path);
    snprintf(buf, sizeof(buf), "%s-wal", path);
    unlink(buf);
    snprintf(buf, sizeof(buf), "%s-shm", path);
    unlink(buf);
}

int main(void) {
    printf("=== Arkilian C SDK: Simple Quickstart ===\n\n");

    cleanup_db(DB_PATH);

    // Disable backup warnings in quickstart local mode
    setenv("ARKILIAN_ENABLE_BACKUP", "0", 1);

    // 1. Initialize database
    printf("[1] Initializing Arkilian embedded database at '%s'...\n", DB_PATH);
    arkilian *db = NULL;
    int rc = db_init(&db, DB_PATH);
    if (rc != 0 || !db) {
        fprintf(stderr, "Failed to initialize database (rc=%d): %s\n",
                rc, db ? db_errmsg(db) : "allocation failed");
        if (db) db_close(db);
        return 1;
    }

    // 2. Create schema
    printf("[2] Creating schema...\n");
    const char *schema_sql =
        "CREATE TABLE IF NOT EXISTS sensor_readings ("
        "  id INTEGER PRIMARY KEY AUTOINCREMENT,"
        "  sensor_id TEXT NOT NULL,"
        "  temperature REAL NOT NULL,"
        "  humidity REAL NOT NULL,"
        "  recorded_at DATETIME DEFAULT CURRENT_TIMESTAMP"
        ");";

    if (db_exec(db, schema_sql) != 0) {
        fprintf(stderr, "Schema creation failed: %s\n", db_errmsg(db));
        db_close(db);
        return 1;
    }
    printf("    ✓ Table 'sensor_readings' created.\n");

    // 3. Prepared statement insert
    printf("[3] Inserting sample sensor telemetry...\n");
    const char *insert_sql =
        "INSERT INTO sensor_readings (sensor_id, temperature, humidity) VALUES (?, ?, ?);";

    struct {
        const char *sensor_id;
        double temp;
        double humidity;
    } samples[] = {
        {"sensor-alpha", 22.4, 45.2},
        {"sensor-beta",  26.1, 58.7},
        {"sensor-gamma", 19.8, 41.0}
    };

    for (int i = 0; i < 3; i++) {
        if (db_prepare(db, insert_sql) != 0) {
            fprintf(stderr, "Prepare insert error: %s\n", db_errmsg(db));
            db_close(db);
            return 1;
        }
        db_bind_text(db, 1, samples[i].sensor_id);
        db_bind_double(db, 2, samples[i].temp);
        db_bind_double(db, 3, samples[i].humidity);
        db_step(db);
        db_finalize(db);
    }
    printf("    ✓ 3 sensor readings successfully inserted.\n");

    // 4. Query all records
    printf("\n[4] Querying all sensor readings:\n");
    const char *query_sql =
        "SELECT id, sensor_id, temperature, humidity FROM sensor_readings ORDER BY id ASC;";

    if (db_prepare(db, query_sql) != 0) {
        fprintf(stderr, "Prepare query error: %s\n", db_errmsg(db));
        db_close(db);
        return 1;
    }

    while (db_step(db) == 100 /* SQLITE_ROW */) {
        sqlite3_int64 id = db_column_int64(db, 0);
        const char *sensor_id = db_column_text(db, 1);
        double temp = db_column_double(db, 2);
        double hum = db_column_double(db, 3);
        printf("    - ID %lld | Sensor: %-14s | Temp: %5.1f°C | Humidity: %5.1f%%\n",
               (long long)id, sensor_id, temp, hum);
    }
    db_finalize(db);

    // 5. Atomic transaction demonstration
    printf("\n[5] Executing atomic temperature calibration transaction...\n");
    if (db_begin(db) != 0) {
        fprintf(stderr, "Begin transaction failed: %s\n", db_errmsg(db));
        db_close(db);
        return 1;
    }

    int u1 = db_exec(db, "UPDATE sensor_readings SET temperature = temperature + 1.5 WHERE sensor_id = 'sensor-alpha';");
    int u2 = db_exec(db, "UPDATE sensor_readings SET humidity = humidity - 3.0 WHERE sensor_id = 'sensor-beta';");

    if (u1 == 0 && u2 == 0) {
        db_commit(db);
        printf("    ✓ Calibration transaction committed successfully.\n");
    } else {
        db_rollback(db);
        printf("    ✗ Transaction rolled back.\n");
    }

    // 6. Verify calibrated values
    printf("\n[6] Verifying calibrated readings:\n");
    db_prepare(db, "SELECT sensor_id, temperature, humidity FROM sensor_readings WHERE sensor_id IN ('sensor-alpha', 'sensor-beta') ORDER BY id ASC;");
    while (db_step(db) == 100) {
        printf("    - Sensor: %-14s | New Temp: %5.1f°C | New Humidity: %5.1f%%\n",
               db_column_text(db, 0), db_column_double(db, 1), db_column_double(db, 2));
    }
    db_finalize(db);

    // 7. Clean closure
    printf("\n[7] Closing database connection cleanly.\n");
    db_close(db);
    cleanup_db(DB_PATH);

    printf("\n=== Quickstart Completed Successfully ===\n");
    return 0;
}
