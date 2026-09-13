# Arkilian Python SDK Examples

This directory contains executable, production-grade examples demonstrating how to use the official **Arkilian Python SDK** (produced in release `v1.5.0`).

---

## Prerequisites

- **Python**: Version 3.8 or newer (tested on 3.10, 3.12, and 3.14)
- **C Foreign Function Interface**: `cffi` (installed automatically via pip)
- **Arkilian Shared Library**: `libarkilian.dylib` (macOS), `libarkilian.so` (Linux), or `arkilian.dll` (Windows). The examples automatically detect the library in `../c/lib/` or through `ARKILIAN_LIB_PATH`.
- **Docker** (optional, for local S3/MinIO testing)

---

## Installation & Setup

1. Create and activate a Python virtual environment:
```bash
cd examples/python
python3 -m venv venv
source venv/bin/activate
```

2. Install the Arkilian wheel from release `v1.5.0`:
```bash
pip install ./arkilian-1.5.0-py3-none-any.whl
```

*(Note: On systems where `libarkilian` is in a custom path, export `ARKILIAN_LIB_PATH=/path/to/libarkilian.dylib`)*

---

## Example 1: Simple Quickstart (`01_simple_quickstart.py`)

A foundational introduction demonstrating:
- Context manager database connection (`with Arkilian('quickstart.sqlite') as db:`)
- DDL table creation
- Parameterized inserts with `db.run(sql, [params])`
- Querying structured rows as Python dictionaries using `db.all(sql, [params])`
- Atomic transactions with `db.begin()`, `db.commit()`, and `db.rollback()`
- Automatic resource cleanup

### Run:
```bash
python3 01_simple_quickstart.py
```

### Expected Output:
```text
=== Arkilian Python SDK: Simple Quickstart ===

[1] Opening Arkilian database at 'quickstart.sqlite'...
[2] Creating schema...
    ✓ Table 'inventory' created.
[3] Inserting sample inventory items...
    ✓ 3 inventory records inserted.

[4] Querying all inventory items:
    - ID 1: Mechanical Keyboard (SKU-101) | Price: $129.99 | In Stock: 45
    - ID 2: Ergonomic Mouse (SKU-102) | Price: $69.50 | In Stock: 120
    - ID 3: 4K Ultra-Wide Monitor (SKU-103) | Price: $449.00 | In Stock: 18

[5] Querying item by SKU ('SKU-102')...
    Found: Ergonomic Mouse (Stock: 120, Price: $69.50)

[6] Executing atomic stock update transaction...
    ✓ Transaction committed successfully.
    Updated Stock Levels:
    - Mechanical Keyboard (SKU-101): 40 units
    - 4K Ultra-Wide Monitor (SKU-103): 68 units

[7] Database closed safely by context manager.

=== Quickstart Completed Successfully ===
```

---

## Example 2: Advanced S3 Streaming & Cold-Start Hydration (`02_advanced_s3_failover.py`)

An advanced operational demonstration demonstrating:
1. **Cloud-Native WAL Streaming**: Streaming CDC events directly to MinIO S3 object storage.
2. **Telemetry Inspection**: Checking `db.is_healthy`, health flags `db.backup_health_flags`, and outbox depth `db.backup_queue_depth`.
3. **Explicit WAL Flushes**: Triggering `db.wal_flush()` to force buffer flushes before failover.
4. **Catastrophic Host Failure Simulation**: Deleting primary local database files.
5. **Instant Cold-Start Disaster Recovery**: Using `Arkilian.hydrate_s3(...)` to download the signed manifest, restore baseline snapshots, and replay incremental chunks.
6. **Integrity Validation**: Reopening the restored database and validating exact row counts and column values.

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
python3 02_advanced_s3_failover.py
```

### Expected Output:
```text
=== Arkilian Python SDK: Advanced S3 Streaming & Cold-Start Hydration ===

[Config] Target S3 Endpoint: http://127.0.0.1:9000
[Config] Bucket: arkilian-test-bucket | Prefix: python-demo

[Phase 1] Opening Primary Node with S3 WAL Streaming...
  Telemetry -> Sidecar Healthy: True
  Telemetry -> Queue Depth: 0

[Phase 1] Creating schema and streaming order audit log...
  ✓ Committed 25 audit log entries in an atomic transaction.

[Phase 1] Triggering explicit WAL flush to S3 sidecar...
  Waiting for S3 sidecar outbox to drain...
  Telemetry -> Queue Depth: 0
  Telemetry -> Sidecar Healthy: True
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

  Sample records from recovered database:
    - Order #1: Customer cust_1 | $32.75 | Status: PENDING
    - Order #2: Customer cust_2 | $40.50 | Status: COMPLETED
    ...
=== Disaster Recovery & Verification Finished Successfully! ===
```

---

## Important Tips

1. **Schema DDL**: Use `CREATE TABLE IF NOT EXISTS` to ensure replay compatibility across baseline snapshots and incremental log chunks.
2. **Context Manager**: Using `with Arkilian(...) as db:` guarantees graceful `db.close()` on script exit or unhandled exceptions.
3. **Manifest HMAC**: Always configure `ARKILIAN_MANIFEST_HMAC_KEY` in production for cryptographic integrity verification of your manifests.
