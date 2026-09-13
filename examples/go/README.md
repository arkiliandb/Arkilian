# Arkilian Go SDK Examples

This directory contains executable, production-grade examples demonstrating how to use the official **Arkilian Go SDK** (produced in release `v1.5.0`).

---

## Prerequisites

- **Go**: 1.21 or newer
- **C Compiler**: GCC or Clang (required for CGo)
- **libcurl**: System curl development headers (standard on macOS and Linux)
- **Docker** (optional, for local S3/MinIO testing)

---

## Installation & Module Setup

The Go SDK is packaged under `github.com/arkiliandb/Arkilian/bindings/go/arkilian`.

In your `go.mod`:

```go
module my-arkilian-app

go 1.21

require github.com/arkiliandb/Arkilian/bindings/go v1.5.0
```

When building against the local repository or extracted release tarball:
```go
replace github.com/arkiliandb/Arkilian/bindings/go => ../../bindings/go
```

---

## Example 1: Simple Quickstart (`01_simple_quickstart`)

A foundational introduction demonstrating:
- Opening an embedded database connection (`arkilian.OpenDB("quickstart.sqlite")`)
- Running DDL queries via `db.Exec(...)`
- Parameterized inserts using prepared statements (`db.Prepare(...)`, `stmt.BindText(...)`, etc.)
- Stepping through result rows with `stmt.Step()` and reading typed columns
- ACID transactions with `db.Begin()`, `db.Commit()`, and `db.Rollback()`
- Clean database teardown

### Run:
```bash
cd examples/go
go run ./01_simple_quickstart
```

### Expected Output:
```text
=== Arkilian Go SDK: Simple Quickstart ===

[1] Initializing Arkilian embedded database at 'quickstart.sqlite'...
[2] Creating schema...
    ✓ Table 'products' created.
[3] Inserting products with prepared statement...
    ✓ 3 products successfully inserted.

[4] Querying all products:
    - ID 1: Gopher Plush Toy [GO-101] | $24.95 | In Stock: 150
    - ID 2: Concurrency in Go Book [GO-102] | $39.99 | In Stock: 85
    - ID 3: Mechanical Keyboard (Go Blue) [GO-103] | $149.00 | In Stock: 30

[5] Executing atomic stock adjustment transaction...
    ✓ Transaction committed successfully.

[6] Verifying updated stock:
    - GO-101: 140 units remaining
    - GO-103: 45 units remaining

[7] Closing database connection cleanly.

=== Quickstart Completed Successfully ===
```

---

## Example 2: Advanced S3 Streaming & Cold-Start Hydration (`02_advanced_s3_failover`)

An advanced operational demonstration that shows:
1. **Sidecar S3 WAL Streaming**: Continuous replication directly to MinIO / AWS S3.
2. **Telemetry Inspection**: Monitoring `db.BackupIsHealthy()`, `db.BackupHealthFlags()`, and outbox depth `db.BackupQueueDepth()`.
3. **Explicit WAL Flushes**: Flushing pending frames with `db.FlushWAL()`.
4. **Catastrophic Host Failure Simulation**: Deleting primary local database and WAL files.
5. **Instant Cold-Start Disaster Recovery**: Using `arkilian.HydrateS3(...)` to download the signed manifest, restore baseline snapshots, and replay incremental chunks.
6. **Data Parity Verification**: Reopening the recovered database and validating exact row counts and values.

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
cd examples/go
go run ./02_advanced_s3_failover
```

### Expected Output:
```text
=== Arkilian Go SDK: Advanced S3 Streaming & Cold-Start Hydration ===

[Config] Target S3 Endpoint: http://127.0.0.1:9000
[Config] Bucket: arkilian-test-bucket | Prefix: go-demo

[Phase 1] Opening Primary Node with S3 WAL Streaming...
  Telemetry -> Sidecar Healthy: true
  Telemetry -> Queue Depth: 0

[Phase 1] Creating schema and streaming server telemetry log...
  ✓ Successfully committed 25 metric records.

[Phase 1] Triggering explicit WAL flush to S3 sidecar...
  Waiting for S3 sidecar outbox to drain...
  Telemetry -> Queue Depth: 0
  Telemetry -> Sidecar Healthy: true
  Primary DB confirmed record count: 25
[Phase 1] Closing primary database connection.

[Phase 2] SIMULATING DISASTER: Primary host destroyed!
  Purging local files: primary_production.sqlite...
  ✓ Primary local database destroyed. Zero local state remains.

[Phase 3] Starting Cold-Start Hydration from MinIO S3...
  Target path: hydrated_recovery.sqlite
  ✓ Hydration completed successfully!

[Phase 4] Opening Hydrated Database to verify integrity...
  Recovered DB record count: 25

  Sample records from recovered instance:
    - ID 1 | Host: node-01 | CPU: 17.3% | RAM: 4224 MB | Status: HEALTHY
    - ID 2 | Host: node-02 | CPU: 19.6% | RAM: 4352 MB | Status: HEALTHY
    ...
=== Disaster Recovery & Verification Finished Successfully! ===
```

---

## Important Best Practices

1. **Idempotent DDL**: Use `CREATE TABLE IF NOT EXISTS` to ensure replay compatibility when replaying incremental chunks onto baseline snapshots.
2. **Resource Cleanup**: Always call `stmt.Finalize()` on prepared statements and `db.Close()` on database connections.
3. **Queue Draining**: In graceful shutdown or failover scenarios, call `db.FlushWAL()` and wait until `db.BackupQueueDepth() == 0`.
