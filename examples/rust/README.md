# Arkilian Rust SDK Examples

This directory contains executable, production-grade examples demonstrating how to use the official **Arkilian Rust SDK** (produced in release `v1.5.0`).

---

## Prerequisites

- **Rust toolchain**: 1.70 or newer (`cargo`, `rustc`)
- **libcurl**: System curl development headers (standard on macOS and Linux)
- **Arkilian C Library**: `libarkilian.dylib` (macOS), `libarkilian.so` (Linux), or `arkilian.dll` (Windows). The `build.rs` script automatically links the shared library from `../c/lib/` or `../../build/`.
- **Docker** (optional, for running local S3/MinIO cloud testing)

---

## Installation & Cargo Setup

In your `Cargo.toml`:

```toml
[dependencies]
arkilian = { path = "../../bindings/rust/arkilian" }
```

Or when consuming the published crate:

```toml
[dependencies]
arkilian = "1.5.0"
```

---

## Example 1: Simple Quickstart (`src/bin/01_simple_quickstart.rs`)

A foundational guide demonstrating:
- Opening an embedded database: `Database::new("quickstart.sqlite")`
- Creating tables and running DDL via `db.exec(...)`
- Parameterized inserts with typed bindings (`db.bind_text`, `db.bind_int`, `db.bind_double`)
- Querying rows and stepping through results (`while db.step() == SQLITE_ROW`)
- ACID transactions with `db.begin()`, `db.commit()`, and `db.rollback()`
- RAII resource management (closing via `Drop` or `db.close()`)

### Run:
```bash
cd examples/rust
cargo run --bin simple_quickstart
```

### Expected Output:
```text
=== Arkilian Rust SDK: Simple Quickstart ===

[1] Initializing Arkilian embedded database at 'quickstart.sqlite'...
[2] Creating schema...
    ✓ Table 'game_saves' created.
[3] Inserting sample player save records...
    ✓ 3 player saves inserted.

[4] Querying all player saves:
    - Rank: ID 3 | Player: CyberPaladin   | Level 55 | Score: 32100.75
    - Rank: ID 1 | Player: Valkyrie       | Level 42 | Score: 18500.50
    - Rank: ID 2 | Player: ShadowSniper   | Level 37 | Score: 14200.00

[5] Executing atomic level-up transaction for 'ShadowSniper'...
    ✓ Transaction committed successfully.

[6] Verifying updated player stats:
    - Player: ShadowSniper | New Level: 38 | New Score: 16700.00

[7] Database closed cleanly.

=== Quickstart Completed Successfully ===
```

---

## Example 2: Advanced S3 Streaming & Cold-Start Hydration (`src/bin/02_advanced_s3_failover.rs`)

An advanced operational demonstration that shows:
1. **Cloud-Native WAL Streaming**: Streaming CDC events directly to MinIO S3 object storage.
2. **Telemetry State Machine**: Checking `db.backup_is_healthy()`, bitmask flags `db.backup_health_flags()`, and outbox depth `db.backup_queue_depth()`.
3. **Explicit WAL Flushes**: Triggering `db.wal_flush()` to force buffer flushes before failover.
4. **Catastrophic Host Failure Simulation**: Deleting primary local database files.
5. **Instant Cold-Start Disaster Recovery**: Using `arkilian::hydrate_s3(...)` to download the signed manifest, restore baseline snapshots, and replay incremental chunks.
6. **Strict LSN Protocol Validation**: Demonstrating the wire-protocol validation that guarantees non-overlapping, monotonically increasing LSN sequences.
7. **Integrity Validation**: Reopening the restored database and validating exact row counts and values.

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
cd examples/rust
cargo run --bin advanced_s3_failover
```

### Expected Output:
```text
=== Arkilian Rust SDK: Advanced S3 Streaming & Cold-Start Hydration ===

[Config] Target S3 Endpoint: http://127.0.0.1:9000
[Config] Bucket: arkilian-test-bucket | Prefix: rust-demo

[Phase 1] Opening Primary Node with S3 WAL Streaming...
  Telemetry -> Sidecar Healthy: true
  Telemetry -> Queue Depth: 0

[Phase 1] Creating schema and streaming mission event logs...
  ✓ Committed 30 mission events in an atomic transaction.

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
    - Event #1  | Module: telemetry_subsystem_1  | Severity: INFO     | Value:    14.25
    - Event #2  | Module: telemetry_subsystem_2  | Severity: INFO     | Value:    28.50
    - Event #3  | Module: telemetry_subsystem_3  | Severity: WARN     | Value:    42.75
    ...
=== Disaster Recovery & Verification Finished Successfully! ===
```

---

## Important Rust Best Practices

1. **Idempotent DDL**: Always declare tables with `CREATE TABLE IF NOT EXISTS` to ensure replay compatibility between baseline snapshots and incremental log chunks.
2. **Statement Lifecycles**: Always finalize prepared statements with `db.finalize()` before opening new statements on the same thread.
3. **Queue Draining**: In graceful failovers, call `db.wal_flush()` and wait until `db.backup_queue_depth() == 0` before closing the database.
