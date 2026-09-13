# Arkilian SDK Examples Suite

Comprehensive, runnable examples teaching how to integrate and operate **Arkilian** across all supported languages using the official release packages from `v1.5.0`.

Every language folder contains:
1. **Simple Quickstart**: Embedded SQLite replacement, DDL execution, parameterized queries, and atomic ACID transactions.
2. **Advanced S3 Streaming & Cold-Start Hydration**: Real-time WAL streaming to MinIO / AWS S3, telemetry and health monitoring, explicit zero-lag WAL flushes, simulated primary catastrophe, and instant cold-start disaster recovery.
3. **Dedicated `README.md`**: Step-by-step prerequisites, package installation, and execution instructions.

---

## SDK Language Matrix

| Language | Directory | Release Package Artifact | Quickstart Example | Advanced Cloud / DR Example |
| :--- | :--- | :--- | :--- | :--- |
| **Node.js / TS** | [`examples/nodejs/`](./nodejs/) | `arkilian-1.5.0.tgz` | `node 01_simple_quickstart.js` | `node 02_advanced_s3_failover.js` |
| **Python** | [`examples/python/`](./python/) | `arkilian-1.5.0-py3-none-any.whl` | `python3 01_simple_quickstart.py` | `python3 02_advanced_s3_failover.py` |
| **Go** | [`examples/go/`](./go/) | `arkilian-go-v1.5.0.tar.gz` | `go run ./01_simple_quickstart` | `go run ./02_advanced_s3_failover` |
| **Rust** | [`examples/rust/`](./rust/) | `arkilian-rust-v1.5.0.crate` | `cargo run --bin simple_quickstart` | `cargo run --bin advanced_s3_failover` |
| **PHP** | [`examples/php/`](./php/) | `arkilian-php-v1.5.0.tar.gz` | `php -d ffi.enable=1 01_simple_quickstart.php` | `php -d ffi.enable=1 02_advanced_s3_failover.php` |
| **C / C++** | [`examples/c/`](./c/) | `arkilian-c-v1.5.0-<os>-<arch>.tar.gz` | `./01_simple_quickstart` | `./02_advanced_s3_failover` |

---

## 1-Minute Local Cloud Setup (Docker MinIO)

All advanced examples replicate changes in real-time to an S3-compatible object store. You can start a local MinIO container with:

```bash
docker run -d --name arkilian-minio-demo \
  -p 9000:9000 -p 9001:9001 \
  -e MINIO_ROOT_USER=minioadmin \
  -e MINIO_ROOT_PASSWORD=minioadmin \
  minio/minio server /data --console-address ":9001"
```

Initialize the target test bucket:
```bash
docker exec arkilian-minio-demo mc alias set local http://localhost:9000 minioadmin minioadmin
docker exec arkilian-minio-demo mc mb local/arkilian-test-bucket || true
```

MinIO Console will be accessible at [http://localhost:9001](http://localhost:9001) (`minioadmin` / `minioadmin`).

---

## Core Architectural Concepts

### 1. Dual-Buffer CDC & S3 Sidecar Shipping
Arkilian captures change-data events into a local SQLite outbox table (`_pending_backup`) transactionally. A dedicated background sidecar worker packages changes into compressed SQL chunks and uploads them to object storage along with atomic manifest records.

### 2. Live Telemetry & Health Monitoring
Each SDK exposes health telemetry to monitor replication reliability:
- **`backupHealthy` / `is_healthy`**: Boolean indicating that all core subsystem requirements are satisfied.
- **`backupHealthFlags`**: Bitmask flag exposing specific states (e.g. queue below cap, schema in sync, no capture gap, no dead letters).
- **`backupQueueDepth`**: The count of pending CDC log rows awaiting shipment to S3.

### 3. Explicit Zero-Lag WAL Flushing (`walFlush`)
In graceful failover or maintenance windows, calling `walFlush()` immediately forces the current in-memory WAL buffer to the outbox and signals the background worker to ship pending chunks to S3 without waiting for batch timeout intervals.

### 4. Cold-Start Hydration Disaster Recovery (`hydrateS3`)
When a database node suffers catastrophic storage loss or when launching a new read replica in a cold container:
1. `hydrateS3` reaches out to the S3 bucket using locally generated AWS SigV4 presigned URLs.
2. It fetches and validates `{prefix}/manifest.json` and its HMAC-SHA256 signature (`{prefix}/manifest.sig`).
3. It installs the baseline snapshot (`backup.sqlite`) and validates SQLite file integrity.
4. It iterates and executes all incremental SQL chunks starting from the baseline LSN up to the latest committed chunk.
5. The local database file is ready for immediate reads and writes without requiring connection to a central coordinator.

### 5. Idempotent Schema Design
Always define tables with `CREATE TABLE IF NOT EXISTS`. In continuous replication pipelines, baseline snapshots and incremental replay chunks may both contain DDL definitions; idempotency ensures seamless recovery.
