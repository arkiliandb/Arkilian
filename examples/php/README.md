# Arkilian PHP SDK Examples

This directory contains executable, production-grade examples demonstrating how to use the official **Arkilian PHP SDK** (produced in release `v1.5.0`).

---

## Prerequisites

- **PHP**: Version 8.1, 8.2, 8.3, or 8.4+
- **PHP FFI Extension**: Must be enabled. You can enable it per command via `-d ffi.enable=1` or globally in `php.ini` (`ffi.enable=true`).
- **Arkilian Shared Library**: `libarkilian.dylib` (macOS), `libarkilian.so` (Linux), or `arkilian.dll` (Windows). The `Arkilian.php` wrapper automatically searches `../c/lib/` or the path in `ARKILIAN_LIB_PATH`.
- **Docker** (optional, for local S3/MinIO cloud testing)

---

## Installation & Setup

1. Copy or require `Arkilian.php`:
```php
require_once __DIR__ . '/Arkilian.php';
```

2. (Optional) Set the explicit library path if `libarkilian` is installed in a non-standard location:
```bash
export ARKILIAN_LIB_PATH=/path/to/libarkilian.dylib
```

---

## Example 1: Simple Quickstart (`01_simple_quickstart.php`)

A foundational guide demonstrating:
- Opening an embedded database: `$db = new Arkilian('quickstart.sqlite');`
- Executing DDL table definitions: `$db->exec(...)`
- Parameterized inserts with `$db->run($sql, [$param1, $param2, ...])`
- Querying structured rows as associative arrays with `$db->all($sql, [$params])`
- ACID transactions with `$db->begin()`, `$db->commit()`, and `$db->rollback()`
- Safe database closure with `$db->close()`

### Run:
```bash
cd examples/php
php -d ffi.enable=1 01_simple_quickstart.php
```

### Expected Output:
```text
=== Arkilian PHP SDK: Simple Quickstart ===

[1] Initializing Arkilian database at 'quickstart.sqlite'...
[2] Creating schema...
    ✓ Table 'articles' created successfully.
[3] Inserting sample articles...
    ✓ 3 articles inserted.

[4] Querying all articles:
    - ID 2 | Continuous Database Replication to S3        | Author: CloudArchitect  | Views: 3200
    - ID 3 | Zero Data-Loss Disaster Recovery Patterns    | Author: SiteReliability | Views: 2890
    - ID 1 | Building Resilient APIs with Arkilian        | Author: DevAdvocate     | Views: 1450

[5] Executing atomic views-increment transaction...
    ✓ Transaction committed successfully.

[6] Verifying updated views:
    - Building Resilient APIs with Arkilian: 1950 views

[7] Closing database connection...
    ✓ Database closed cleanly.

=== Quickstart Completed Successfully ===
```

---

## Example 2: Advanced S3 Streaming & Cold-Start Hydration (`02_advanced_s3_failover.php`)

An advanced operational demonstration that shows:
1. **Cloud-Native WAL Streaming**: Streaming CDC events directly to MinIO S3 object storage.
2. **Telemetry State Machine**: Checking `$db->backupIsHealthy()`, bitmask flags `$db->backupHealthFlags()`, and outbox depth `$db->backupQueueDepth()`.
3. **Explicit WAL Flushes**: Triggering `$db->walFlush()` to force buffer flushes before failover.
4. **Catastrophic Host Failure Simulation**: Deleting primary local database files.
5. **Instant Cold-Start Disaster Recovery**: Using `Arkilian::hydrateS3(...)` to download the signed manifest, restore baseline snapshots, and replay incremental chunks.
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
cd examples/php
php -d ffi.enable=1 02_advanced_s3_failover.php
```

### Expected Output:
```text
=== Arkilian PHP SDK: Advanced S3 Streaming & Cold-Start Hydration ===

[Config] Target S3 Endpoint: http://127.0.0.1:9000
[Config] Bucket: arkilian-test-bucket | Prefix: php-demo

[Phase 1] Opening Primary Node with S3 WAL Streaming...
  Telemetry -> Sidecar Healthy: true
  Telemetry -> Queue Depth: 0

[Phase 1] Creating schema and streaming billing events...
  ✓ Successfully committed 25 billing records in an atomic transaction.

[Phase 1] Triggering explicit WAL flush to S3 sidecar...
  Waiting for S3 sidecar outbox to drain...
  Telemetry -> Queue Depth: 0
  Telemetry -> Sidecar Healthy: true
  Primary DB confirmed record count: 25
[Phase 1] Closing primary database connection.

[Phase 2] SIMULATING DISASTER: Destroying primary host!
  Purging local files: primary_production.sqlite...
  ✓ Primary local database destroyed. Zero local state remains.

[Phase 3] Starting Cold-Start Hydration from MinIO S3...
  Target path: hydrated_recovery.sqlite
  ✓ Hydration completed successfully!

[Phase 4] Opening Hydrated Database to verify integrity...
  Recovered DB record count: 25

  Sample records from recovered instance:
    - Bill #01 | Account: acc_cust_1   | USD 31.49 | Status: PAID
    - Bill #02 | Account: acc_cust_2   | USD 42.99 | Status: PAID
    - Bill #03 | Account: acc_cust_3   | USD 54.49 | Status: PENDING
    - Bill #04 | Account: acc_cust_4   | USD 65.99 | Status: PAID
    - Bill #05 | Account: acc_cust_0   | USD 77.49 | Status: PAID

=== Disaster Recovery & Verification Finished Successfully! ===
```

---

## Important Best Practices

1. **Idempotent DDL**: Always declare tables with `CREATE TABLE IF NOT EXISTS` to ensure replay compatibility when cold-start hydration restores incremental chunks over baseline snapshots.
2. **PHP FFI Preloading**: For high-throughput production (e.g. PHP-FPM / Swoole / RoadRunner), preload `Arkilian.php` in your `php.ini` via `ffi.preload` for maximum performance.
3. **Graceful Draining**: Before switching nodes, trigger `$db->walFlush()` and verify `$db->backupQueueDepth() === 0`.
