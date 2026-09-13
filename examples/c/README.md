# Arkilian C / C++ SDK Examples

This directory contains executable, production-grade examples demonstrating how to use the official **Arkilian C SDK** (produced in release `v1.5.0`).

---

## Directory Structure

```text
examples/c/
├── include/
│   └── arkilian/
│       ├── class.h       # Core Arkilian C API (db_init, db_exec, db_prepare, telemetry)
│       ├── hydration.h   # Cold-start S3 recovery API (arkilian_hydrate_s3)
│       └── sha256.h      # Content digest helpers
├── lib/
│   ├── libarkilian.dylib # Shared dynamic library (macOS) / .so (Linux)
│   └── libarkilian.a     # Static archive
├── bin/
│   └── arkilian-dlq      # Dead-letter queue utility binary
├── 01_simple_quickstart.c
├── 02_advanced_s3_failover.c
├── Makefile
└── README.md
```

---

## Prerequisites

- **C Compiler**: Clang or GCC supporting C99 or later
- **libcurl**: System curl development headers (standard on macOS and Linux)
- **POSIX Threads**: `pthread`
- **Docker** (optional, for local S3/MinIO cloud testing)

---

## Compilation

Build all targets using `make`:

```bash
cd examples/c
make
```

To clean build artifacts:
```bash
make clean
```

---

## Example 1: Simple Quickstart (`01_simple_quickstart.c`)

A foundational guide demonstrating:
- Opening an embedded database: `db_init(&db, "quickstart.sqlite")`
- Executing DDL table definitions: `db_exec(db, ...)`
- Parameterized inserts using prepared statements (`db_prepare`, `db_bind_text`, `db_bind_double`, `db_step`, `db_finalize`)
- Stepping through result rows (`while (db_step(db) == 100)`) and accessing typed column values
- ACID transactions with `db_begin(db)`, `db_commit(db)`, and `db_rollback(db)`
- Safe database closure: `db_close(db)`

### Run:
```bash
./01_simple_quickstart
```

### Expected Output:
```text
=== Arkilian C SDK: Simple Quickstart ===

[1] Initializing Arkilian embedded database at 'quickstart.sqlite'...
[2] Creating schema...
    ✓ Table 'sensor_readings' created.
[3] Inserting sample sensor telemetry...
    ✓ 3 sensor readings successfully inserted.

[4] Querying all sensor readings:
    - ID 1 | Sensor: sensor-alpha   | Temp:  22.4°C | Humidity:  45.2%
    - ID 2 | Sensor: sensor-beta    | Temp:  26.1°C | Humidity:  58.7%
    - ID 3 | Sensor: sensor-gamma   | Temp:  19.8°C | Humidity:  41.0%

[5] Executing atomic temperature calibration transaction...
    ✓ Calibration transaction committed successfully.

[6] Verifying calibrated readings:
    - Sensor: sensor-alpha   | New Temp:  23.9°C | New Humidity:  45.2%
    - Sensor: sensor-beta    | New Temp:  26.1°C | New Humidity:  55.7%

[7] Closing database connection cleanly.

=== Quickstart Completed Successfully ===
```

---

## Example 2: Advanced S3 Streaming & Cold-Start Hydration (`02_advanced_s3_failover.c`)

An advanced operational demonstration that shows:
1. **Cloud-Native WAL Streaming**: Streaming CDC events directly to MinIO S3 object storage.
2. **Telemetry State Machine**: Checking `db_backup_is_healthy(db)`, bitmask flags `db_backup_health_flags(db)`, and outbox depth `db_backup_queue_depth(db)`.
3. **Explicit WAL Flushes**: Triggering `db_wal_flush(db)` to force buffer flushes before failover.
4. **Catastrophic Host Failure Simulation**: Deleting primary local database files.
5. **Instant Cold-Start Disaster Recovery**: Using `arkilian_hydrate_s3(...)` to download the signed manifest, restore baseline snapshots, and replay incremental chunks.
6. **Data Parity Verification**: Reopening the recovered database and validating exact row counts and column values.

### Start Local MinIO (if not running):
```bash
docker run -d --name arkilian-minio-demo \
  -p 9000:9000 -p 9001:9001 \
  -e MINIO_ROOT_USER=minioadmin \
  -e MINIO_ROOT_PASSWORD=minioadmin \
  minio/minio server /data --console-address ":9001"
```

### Run:
```bash
./02_advanced_s3_failover
```

### Expected Output:
```text
=== Arkilian C SDK: Advanced S3 Streaming & Cold-Start Hydration ===

[Config] Target S3 Endpoint: http://127.0.0.1:9000
[Config] Bucket: arkilian-test-bucket | Prefix: c-demo

[Phase 1] Opening Primary Node with S3 WAL Streaming...
  Telemetry -> Sidecar Healthy: true
  Telemetry -> Queue Depth: 0

[Phase 1] Creating schema and streaming flight telemetry...
  ✓ Committed 30 flight telemetry records in an atomic transaction.

[Phase 1] Triggering explicit WAL flush to S3 sidecar...
  Waiting for S3 sidecar outbox to drain...
  Telemetry -> Queue Depth: 0
  Telemetry -> Sidecar Healthy: true
  Primary DB confirmed record count: 30
[Phase 1] Closing primary database connection.

[Phase 2] SIMULATING DISASTER: Destroying primary host!
  Purging local files: primary_production.sqlite...
  ✓ Primary local database destroyed. Zero local state remains.

[Phase 3] Starting Cold-Start Hydration from MinIO S3...
  Target path: hydrated_recovery.sqlite
  ✓ Hydration completed successfully!

[Phase 4] Opening Hydrated Database to verify integrity...
  Recovered DB record count: 30

  Sample records from recovered instance:
    - Record #01 | Flight: ARK-101  | Alt:   15450 ft | Spd:   425 kts | Status: CRUISE
    - Record #02 | Flight: ARK-102  | Alt:   15900 ft | Spd:   430 kts | Status: CRUISE
    - Record #03 | Flight: ARK-103  | Alt:   16350 ft | Spd:   435 kts | Status: CRUISE
    ...
=== Disaster Recovery & Verification Finished Successfully! ===
```

---

## Important Architectural Notes

1. **Idempotent DDL**: Always declare tables with `CREATE TABLE IF NOT EXISTS` to ensure replay compatibility when replaying incremental chunks over baseline snapshots.
2. **Resource Management**: Always pair `db_prepare()` with `db_finalize()` before preparing new statements, and ensure `db_close()` is called on process shutdown.
3. **Queue Draining**: In graceful shutdowns, trigger `db_wal_flush(db)` and poll `while (db_backup_queue_depth(db) > 0)` before closing the connection.
