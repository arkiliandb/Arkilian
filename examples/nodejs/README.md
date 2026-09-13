# Arkilian Node.js / TypeScript SDK Examples

This directory contains executable, production-grade examples demonstrating how to use the official **Arkilian Node.js SDK** (produced in release `v1.5.0`).

---

## Prerequisites

- **Node.js**: Version 18, 20, 22, or 25+
- **Docker** (optional, required for running the local S3/MinIO cloud test)

---

## Installation

You can install Arkilian from the release tarball `arkilian-1.5.0.tgz`:

```bash
cd examples/nodejs
npm install ./arkilian-1.5.0.tgz
```

Or when published to npm:

```bash
npm install arkilian
```

---

## Example 1: Simple Quickstart (`01_simple_quickstart.js`)

A foundational guide covering:
- Creating an embedded database connection (`new Arkilian('quickstart.sqlite')`)
- Creating tables and running DDL
- Parameterized inserts with `db.run(sql, [params])`
- Querying multiple records with `db.all(sql, [params])`
- ACID transactions with automatic rollback on error via `db.transaction(fn)`
- Clean database teardown

### Run:
```bash
node 01_simple_quickstart.js
```

### Expected Output:
```text
=== Arkilian Node.js SDK: Simple Quickstart ===

[1] Initializing Arkilian database at './quickstart.sqlite'...
[2] Creating schema...
    ✓ Table "users" created successfully.
[3] Inserting sample records...
    ✓ 3 users inserted.

[4] Querying all users:
┌─────────┬────┬───────────┬────────────────────────┬─────────┐
│ (index) │ id │ username  │ email                  │ balance │
├─────────┼────┼───────────┼────────────────────────┼─────────┤
│ 0       │ 1  │ 'alice'   │ 'alice@arkilian.dev'   │ 150.5   │
│ 1       │ 2  │ 'bob'     │ 'bob@arkilian.dev'     │ 80      │
│ 2       │ 3  │ 'charlie' │ 'charlie@arkilian.dev' │ 220     │
└─────────┴────┴───────────┴────────────────────────┴─────────┘
[5] Querying single user (username = "alice"):
    Found user: ID=1, Email=alice@arkilian.dev, Balance=$150.5

[6] Running atomic transfer transaction ($30 from Alice to Bob)...
    ✓ Transaction committed successfully.
    Updated Balances:
┌─────────┬──────────┬─────────┐
│ (index) │ username │ balance │
├─────────┼──────────┼─────────┤
│ 0       │ 'alice'  │ 120.5   │
│ 1       │ 'bob'    │ 110     │
└─────────┴──────────┴─────────┘
[7] Closing database connection...
    ✓ Database closed cleanly.

=== Quickstart Completed Successfully ===
```

---

## Example 2: Advanced S3 Streaming & Cold-Start Disaster Recovery (`02_advanced_s3_failover.js`)

An advanced operational demonstration that shows:
1. **Sidecar S3 WAL Streaming**: Continuous CDC and WAL shipping to MinIO / AWS S3.
2. **Real-time Telemetry**: Monitoring sidecar health (`db.backupHealthy`), health flags bitmask (`db.backupHealthFlags`), and pending outbox queue depth (`db.backupQueueDepth`).
3. **Explicit WAL Flushes**: Flushing pending frames to ensure zero recovery lag via `db.walFlush()`.
4. **Catastrophic Host Failure Simulation**: Deleting primary local database and WAL/SHM files.
5. **Instant Cold-Start Disaster Recovery**: Using `Arkilian.hydrateS3()` to download the signed manifest, restore the baseline snapshot, replay incremental chunks, and verify HMAC signatures.
6. **Data Parity Verification**: Reopening the restored database and validating exact row count and data fidelity.

### Start Local MinIO (if not already running):
```bash
docker run -d --name arkilian-minio-demo \
  -p 9000:9000 -p 9001:9001 \
  -e MINIO_ROOT_USER=minioadmin \
  -e MINIO_ROOT_PASSWORD=minioadmin \
  minio/minio server /data --console-address ":9001"
```

Create bucket `arkilian-test-bucket` (or use AWS CLI / MinIO Console at `http://localhost:9001`):
```bash
docker exec arkilian-minio-demo mc alias set local http://localhost:9000 minioadmin minioadmin
docker exec arkilian-minio-demo mc mb local/arkilian-test-bucket || true
```

### Run:
```bash
node 02_advanced_s3_failover.js
```

### Expected Output:
```text
=== Arkilian Node.js SDK: Advanced S3 Streaming & Cold-Start Hydration ===

[Config] Target S3 Endpoint: http://127.0.0.1:9000
[Config] Bucket: arkilian-test-bucket | Prefix: nodejs-demo

[Phase 1] Initializing Primary Node with S3 WAL Streaming...
  Telemetry -> Sidecar Healthy: true
  Telemetry -> Queue Depth: 0

[Phase 1] Creating schema and streaming financial ledger entries...
  ✓ Successfully committed 20 ledger entries.

[Phase 1] Triggering explicit WAL flush to S3 sidecar...
  Waiting for S3 sidecar outbox to drain...
  Telemetry -> Queue Depth: 0
  Telemetry -> Sidecar Healthy: true
  Primary DB confirmed record count: 20
[Phase 1] Closing primary database connection.

[Phase 2] SIMULATING DISASTER: Primary host destroyed!
  Purging local files: ./primary_production.sqlite...
  ✓ Primary local database destroyed. Zero local state remains.

[Phase 3] Starting Cold-Start Hydration from MinIO S3...
  Target path: ./hydrated_recovery.sqlite
  ✓ Hydration completed successfully!

[Phase 4] Opening Hydrated Database to verify integrity...
  Recovered DB record count: 20

  Sample records from recovered instance:
┌─────────┬──────────┬────────────┬───────┬─────────────────────────────┐
│ (index) │ entry_id │ account_id │ delta │ note                        │
├─────────┼──────────┼────────────┼───────┼─────────────────────────────┤
│ 0       │ 1        │ 'acc_1'    │ 12.5  │ 'Transaction batch item #1' │
│ 1       │ 2        │ 'acc_2'    │ 25    │ 'Transaction batch item #2' │
│ 2       │ 3        │ 'acc_3'    │ 37.5  │ 'Transaction batch item #3' │
...
└─────────┴──────────┴────────────┴───────┴─────────────────────────────┘

=== Disaster Recovery & Verification Finished Successfully! ===
```

---

## Important Architectural Best Practices

1. **Idempotent DDL**: Always use `CREATE TABLE IF NOT EXISTS` for table schemas. In continuous replication setups, baseline snapshots and incremental replay chunks may both contain DDL definitions.
2. **Manifest Signing**: In production, always set `ARKILIAN_MANIFEST_HMAC_KEY` to prevent unsigned manifest publication and ensure tamper-proof restore.
3. **Queue Draining**: Before terminating an active writer node during graceful maintenance, call `db.walFlush()` and verify `db.backupQueueDepth === 0` to ensure no transactions are left in the local outbox.
