<br/>
<h1 align="center">Arkilian</h1>  
<p align="center">
  <a href="https://github.com/arkiliandb/Arkilian">
    <img src="https://avatars.githubusercontent.com/u/261335565?s=88&v=4" alt="Arkilian Database">
  </a>
</p>

[![PRs Welcome](https://img.shields.io/badge/PRs-welcome-brightgreen.svg)](https://github.com/arkiliandb/Arkilian/blob/main/CONTRIBUTING.md)
![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)
[![Stargazers](https://img.shields.io/github/stars/arkiliandb/Arkilian?style=social)](https://github.com/arkiliandb/Arkilian)

# Arkilian

Arkilian is an embedded SQLite database engine written in C99 that streams row-level change data capture (CDC) and periodic point-in-time snapshots directly to S3-compatible object storage.

### Features
* **Asynchronous CDC Streaming:** Batches row-level changes from a local outbox (`_pending_backup`) into SHA-256-verified SQL chunks uploaded via AWS SigV4 presigned PUTs.
* **Point-in-Time Snapshots:** Periodic online database backups (`sqlite3_backup_step`) uploaded to S3 (`backup.sqlite`).
* **Cold-Start Hydration:** Reconstructs the database from S3 (`manifest.json`, baseline snapshot, and incremental chunks) with HMAC-SHA256 and SHA-256 verification.
* **Dedicated Connections:** Three independent SQLite connections (`handle`, `backup_db`, and `snapshot_db`) eliminate cross-thread lock contention (Spec §3.1).
* **Non-Blocking Capture:** If the outbox exceeds capacity (`ARKILIAN_MAX_QUEUE_DEPTH`, default 100,000), CDC capture pauses while application writes proceed without blocking (Spec §0).
* **Multi-Language SDKs:** Prebuilt bindings for Node.js/TypeScript (N-API), Python (CFFI), Go (cgo), Rust, PHP (FFI), and C/C++.


---

## Architecture

<p align="center">
  <img src="./arkilian_architecture.svg" alt="Arkilian Engine Architecture" width="100%">
</p>

Arkilian operates with **three dedicated SQLite connections** (`db->handle`, `db->backup_db`, and `db->snapshot_db`) to eliminate cross-thread lock contention. Application writes are captured via automated DML triggers (`sync_backup_triggers`) and a transaction-aware sidecar journal (`.arkddlqueue`). A background **Flush Thread** batches outbox rows and uploads SHA-256 verified SQL chunks to S3 alongside an HMAC-SHA256 authenticated manifest (`manifest.json` + `manifest.sig`), while an independent **Snapshot Thread** performs periodic non-blocking online snapshots (`backup.sqlite`). Client processes can cold-start restore from object storage at any time using `arkilian_hydrate_s3()`.

---

## Getting Started

### Prerequisites
* A C99 compliant compiler (GCC, Clang, or MSVC)
* CMake 3.10 or higher
* `libcurl` (e.g. `libcurl4-openssl-dev` on Debian/Ubuntu, native Xcode SDK on macOS, or vcpkg on Windows)

### Build Instructions

Build the shared and static libraries using CMake:

```bash
# Clone repository
git clone https://github.com/arkiliandb/Arkilian.git
cd Arkilian

# Generate build files
cmake -B build -S . -DCMAKE_BUILD_TYPE=Release

# Compile library
cmake --build build --config Release

# Install to system (optional)
sudo cmake --install build
```

### Build Options

| Option | Default | Description |
|--------|---------|-------------|
| `ARKILIAN_BUILD_SHARED` | `ON` | Build shared library (`libarkilian.so` / `libarkilian.dylib` / `arkilian.dll`) |
| `ARKILIAN_BUILD_STATIC` | `ON` | Build static library (`libarkilian.a` / `arkilian_static.lib`) |
| `ARKILIAN_BUILD_EXAMPLES` | `ON` | Build example programs (`build/arkilian_example`) |
| `ARKILIAN_BUILD_TESTS` | `OFF` | Build test suites (enables CTest runner) |
| `ARKILIAN_BUILD_NAPI` | `OFF` | Build Node.js N-API addon (`arkilian.node`) |

### Configuration

Arkilian reads configuration from process environment variables or a `./.env` file in the working directory (environment variables take precedence over `.env`). By default, S3 endpoint settings are empty and backup threads remain dormant until configured.

| Variable | Default | Description |
|----------|---------|-------------|
| `ARKILIAN_DB_PATH` | `app.sqlite` | Path to the primary SQLite database file |
| `ARKILIAN_BACKUP_PATH` | `backup.sqlite` | Local staging path for point-in-time snapshot copies |
| `ARKILIAN_BACKUP_INTERVAL` | `3600` | Snapshot backup interval in seconds (minimum 1) |
| `ARKILIAN_CHUNK_INTERVAL_SEC` | `1` | Interval in seconds for packaging and shipping outbox CDC rows to S3 chunks |
| `ARKILIAN_MANIFEST_INTERVAL_SEC` | `30` | Minimum interval in seconds between publishing updated manifests to S3 |
| `ARKILIAN_S3_ENDPOINT` | *(empty)* | S3 endpoint URL (e.g. `https://s3.amazonaws.com` or `https://minio.internal:9000`). If unset, shipping is disabled |
| `ARKILIAN_S3_BUCKET` | *(empty)* | Target bucket for snapshot backups, chunks, and manifests |
| `ARKILIAN_S3_REGION` | `us-east-1` | AWS SigV4 request signing region |
| `ARKILIAN_S3_ACCESS_KEY` | *(empty)* | AWS SigV4 access key ID (used only for local signing) |
| `ARKILIAN_S3_SECRET_KEY` | *(empty)* | AWS SigV4 secret access key (used only for local signing, never transmitted) |
| `ARKILIAN_S3_PREFIX` | `db_default` | Object key prefix isolating database instances within a shared bucket |
| `ARKILIAN_ENABLE_BACKUP` | `1` | `1` enables background backup; `0` disables outbound shipping (toggled via `db_backup_set_enabled`) |
| `ARKILIAN_OUTBOX_DURABLE` | `1` | `1` sets `PRAGMA synchronous=FULL;` so committed outbox records are durably flushed according to SQLite's full-synchronous durability semantics. `0` uses `NORMAL` for maximum throughput |
| `ARKILIAN_MAX_QUEUE_DEPTH` | `100000` | Maximum pending outbox rows before capture pauses to protect application writes |
| `ARKILIAN_MAX_ATTEMPTS` | `100` | Maximum upload retry attempts per chunk before moving rows to `_dead_backup` (DLQ) |
| `ARKILIAN_MANIFEST_HMAC_KEY` | *(empty)* | Secret key for HMAC-SHA256 **manifest authentication**. REQUIRED for manifest publishing and cold-start hydration (fail closed) |
| `ARKILIAN_ALLOW_INSECURE` | `0` | Set `1` to allow cleartext `http://` endpoints for non-loopback hosts during development |
| `ARKILIAN_STORAGE_HOSTS` | *(empty)* | Comma-separated allowlist of custom storage hostnames to prevent SSRF vulnerabilities |

Example `.env` file:
```ini
ARKILIAN_DB_PATH=app.sqlite
ARKILIAN_BACKUP_PATH=backup.sqlite
ARKILIAN_BACKUP_INTERVAL=3600
ARKILIAN_CHUNK_INTERVAL_SEC=1
ARKILIAN_MANIFEST_INTERVAL_SEC=30
ARKILIAN_S3_ENDPOINT=https://s3.amazonaws.com
ARKILIAN_S3_BUCKET=my-app-backups
ARKILIAN_S3_REGION=us-east-1
ARKILIAN_S3_ACCESS_KEY=AKIAIOSFODNN7EXAMPLE
ARKILIAN_S3_SECRET_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
ARKILIAN_S3_PREFIX=tenant-production
ARKILIAN_MANIFEST_HMAC_KEY=0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
ARKILIAN_OUTBOX_DURABLE=1
ARKILIAN_ENABLE_BACKUP=1
```

---

## Language Bindings & Usage

### 1 — Node.js / Bun (npm)

The `arkilian` npm package includes prebuilt N-API binaries for Linux (x64, arm64, glibc, musl/Alpine), macOS (x64, arm64), and Windows (x64). No C compiler or libcurl headers are required at install time.

```bash
npm install arkilian
```

```javascript
import Arkilian from 'arkilian';

// Optional: Cold-start hydration from S3 before opening the database
// Downloads verified snapshot and replays incremental chunks.
// Requires ARKILIAN_MANIFEST_HMAC_KEY set in the environment.
/*
Arkilian.hydrateS3('app.sqlite', {
  endpoint: 'https://s3.amazonaws.com',
  bucket: 'my-app-backups',
  region: 'us-east-1',
  accessKey: process.env.ARKILIAN_S3_ACCESS_KEY,
  secretKey: process.env.ARKILIAN_S3_SECRET_KEY,
  prefix: 'tenant-production',
});
*/

// Initialize database instance (reads environment / .env)
const db = new Arkilian('app.sqlite');

// DDL execution automatically wires row-level capture triggers for tables with PRIMARY KEY
db.exec(`CREATE TABLE IF NOT EXISTS users (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL,
  email TEXT NOT NULL UNIQUE
)`);

// Parameterized insert using run()
db.run('INSERT INTO users (name, email) VALUES (?, ?)', ['Alice', 'alice@example.com']);
console.log('Inserted user ID:', db.lastInsertRowid);
console.log('Rows modified:', db.changes);

// Querying rows using all()
const users = db.all('SELECT id, name, email FROM users');
console.log('Users:', users);

// Atomic transactions with automatic rollback on exception
db.transaction((tx) => {
  tx.run('INSERT INTO users (name, email) VALUES (?, ?)', ['Bob', 'bob@example.com']);
  tx.run('INSERT INTO users (name, email) VALUES (?, ?)', ['Charlie', 'charlie@example.com']);
});

// Health metrics & telemetry
console.log('Healthy:', db.backupHealthy);
console.log('Health flags:', db.backupHealthFlags.toString(2));
console.log('Outbox queue depth:', db.backupQueueDepth);
console.log('Oldest pending row age (s):', db.backupOldestPendingAgeSec);
console.log('Flushed chunks count:', db.backupChunkCount);

// Clean shutdown
db.close();
```

---

### 2 — Python (CFFI)

The Python binding (`bindings/python`) provides an idiomatic class wrapping the core engine via CFFI:

```bash
cd bindings/python
pip install -e .
```

```python
from arkilian import Arkilian

# Initialize database
db = Arkilian("app.sqlite")

# Execute DDL
db.exec("CREATE TABLE IF NOT EXISTS orders (id INTEGER PRIMARY KEY, item TEXT, qty INT)")

# Parameterized execution
db.run("INSERT INTO orders (item, qty) VALUES (?, ?)", ["Widget", 5])
print("Last insert ID:", db.last_insert_rowid)

# Fetch all rows as list of dicts
orders = db.all("SELECT id, item, qty FROM orders")
print("Orders:", orders)

# Transactions
db.begin()
try:
    db.run("INSERT INTO orders (item, qty) VALUES (?, ?)", ["Gadget", 10])
    db.commit()
except Exception:
    db.rollback()
    raise

# Health monitoring
print("Subsystem healthy:", db.backup_is_healthy)
print("Queue depth:", db.backup_queue_depth)

db.close()
```

---

### 3 — Go (cgo)

The Go package (`bindings/go/arkilian`) compiles SQLite and Arkilian directly via cgo:

```go
package main

import (
	"fmt"
	"log"

	"github.com/arkiliandb/arkilian/bindings/go/arkilian"
)

func main() {
	// Open database instance
	db, err := arkilian.OpenDB("app.sqlite")
	if err != nil {
		log.Fatalf("Open failed: %v", err)
	}
	defer db.Close()

	// Execute DDL
	err = db.Exec(`CREATE TABLE IF NOT EXISTS players (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		name TEXT NOT NULL,
		score INTEGER NOT NULL DEFAULT 0
	)`)
	if err != nil {
		log.Fatalf("Exec failed: %v", err)
	}

	// Prepare and execute statement
	stmt, err := db.Prepare("INSERT INTO players (name, score) VALUES (?, ?)")
	if err != nil {
		log.Fatalf("Prepare failed: %v", err)
	}
	defer stmt.Finalize()

	stmt.BindText(1, "PlayerOne")
	stmt.BindInt(2, 4200)
	if _, err := stmt.Step(); err != nil {
		log.Fatalf("Step failed: %v", err)
	}

	fmt.Println("Changes:", db.Changes())
	fmt.Println("Queue depth:", db.BackupQueueDepth())
	fmt.Println("Subsystem healthy:", db.BackupIsHealthy())
}
```

---

### 4 — Rust

The `arkilian` crate (`bindings/rust/arkilian`) provides safe, high-level Rust abstractions over `arkilian-sys`:

```rust
use arkilian::Database;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut db = Database::new("app.sqlite")?;

    db.exec("CREATE TABLE IF NOT EXISTS scores (id INTEGER PRIMARY KEY, player TEXT, points INT)")?;

    db.run(
        "INSERT INTO scores (player, points) VALUES (?, ?)",
        &[&"Hero" as &dyn arkilian::ToSql, &100_i32],
    )?;

    let rows = db.all("SELECT player, points FROM scores", &[])?;
    for row in rows {
        println!("Row: {:?}", row);
    }

    println!("Subsystem healthy: {}", db.backup_is_healthy());
    Ok(())
}
```

---

### 5 — PHP (FFI)

PHP 7.4+ users can link directly using PHP's FFI extension (`bindings/php/Arkilian.php`):

```php
<?php
require_once 'Arkilian.php';

$db = new Arkilian('app.sqlite');

$db->exec("CREATE TABLE IF NOT EXISTS messages (id INTEGER PRIMARY KEY, text TEXT)");
$db->run("INSERT INTO messages (text) VALUES (?)", ["Hello from PHP"]);

$messages = $db->all("SELECT id, text FROM messages");
print_r($messages);

echo "Healthy: " . ($db->backupIsHealthy() ? "yes" : "no") . "\n";
echo "Queue depth: " . $db->backupQueueDepth() . "\n";

$db->close();
```

---

### 6 — C/C++ Native API

```c
#include "class.h"
#include <stdio.h>

int main(void) {
    arkilian *db = NULL;

    // Initialize database context (loads configuration from env or .env)
    if (db_init(&db, "app.sqlite") != 0) {
        fprintf(stderr, "Initialization failed: %s\n", db ? db_errmsg(db) : "Allocation error");
        if (db) db_close(db);
        return 1;
    }

    // Execute DDL with automatic trigger creation
    db_exec(db, "CREATE TABLE IF NOT EXISTS items (id INTEGER PRIMARY KEY, title TEXT);");

    // Prepared statement
    db_prepare(db, "INSERT INTO items (title) VALUES (?);");
    db_bind_text(db, 1, "Toolbox");
    db_step(db);
    db_finalize(db);

    printf("Queue depth: %d\n", db_backup_queue_depth(db));
    printf("Is healthy: %d\n", db_backup_is_healthy(db));

    // Access underlying sqlite3 handle if needed
    sqlite3 *raw = db_get_handle(db);
    (void)raw;

    db_close(db);
    return 0;
}
```

Compile against static library:
```bash
gcc -I/usr/local/include/arkilian myapp.c -L/usr/local/lib -larkilian -lcurl -lpthread -lm -o myapp
```

---

## Performance & Benchmarks

Arkilian was benchmarked directly against raw SQLite (amalgamation 3.46.1) using [`tests/bench_1m.c`](tests/bench_1m.c) on the same machine, schema, and connection settings (Intel Core i7-9750H, macOS, `-O2`, `journal_mode=WAL`, `synchronous=NORMAL`):

### Single-Row Throughput (100,000 Operations)

| Operation | Raw SQLite | Arkilian | Overhead |
| :--- | :---: | :---: | :---: |
| **INSERT (Autocommit)** | 5,262 ops/s | 5,144 ops/s | **-2.2%** |
| **UPDATE (by PK)** | 5,030 ops/s | 5,018 ops/s | **-0.2%** |
| **SELECT (Point by PK)** | 139,147 ops/s | 135,875 ops/s | **-2.4%** |
| **SELECT (Range 100 rows)** | 6,182 ops/s | 6,017 ops/s | **-2.7%** |

### Batched INSERT Throughput (100,000 Operations)

| Batch Size | Raw SQLite | Arkilian | Difference |
| :--- | :---: | :---: | :---: |
| **Batch 1 (Autocommit)** | 1,672 ops/s | 1,760 ops/s | +5.3% |
| **Batch 10** | 10,354 ops/s | 10,516 ops/s | +1.6% |
| **Batch 100** | 22,975 ops/s | 23,231 ops/s | +1.1% |
| **Batch 1,000** | 28,604 ops/s | 28,383 ops/s | -0.8% |
| **Batch 10,000** | 29,358 ops/s | 29,600 ops/s | +0.8% |
| **Batch 100,000** | 29,304 ops/s | 29,432 ops/s | +0.4% |

### Latency Percentiles (50,000 Operations)

| Operation | Percentile | Raw SQLite | Arkilian |
| :--- | :--- | :---: | :---: |
| **INSERT** | P50 / P95 / P99 | 256 µs / 256 µs / 512 µs | 256 µs / 256 µs / 512 µs |
| **SELECT (PK)** | P50 / P95 / P99 | 8 µs / 16 µs / 16 µs | 8 µs / 16 µs / 16 µs |

Full benchmark report, batched scalability curves, memory RSS telemetry, and reproduction instructions are available in [BENCHMARK.md](BENCHMARK.md).

---

## Architectural Guarantees

* **Ordering:** CDC outbox rows are shipped in strict `_pending_backup` sequence order (LSN order). Any transient S3 network error halts the drain and triggers retries with exponential backoff so changes are never uploaded out of order.
* **Delivery:** At-least-once. If an upload completes but the local outbox deletion is interrupted by crash or power loss, rows are safely re-shipped in a subsequent chunk. Replay statements use idempotent SQL (`REPLACE INTO ...` and `DELETE FROM ...`), making replay of overlapping ranges completely safe.
* **Durability:** With `ARKILIAN_OUTBOX_DURABLE=1` (the default), Arkilian configures `PRAGMA synchronous=FULL;` on the application connection so committed outbox records are durably flushed according to SQLite's full-synchronous durability semantics. Operators prioritizing maximum write throughput over power-loss durability can opt in to `ARKILIAN_OUTBOX_DURABLE=0` (`PRAGMA synchronous=NORMAL;`).
* **Integrity:** Every uploaded chunk and snapshot is content-addressed and digest-verified. The SHA-256 hash is recorded in the manifest, and hydrators strictly verify object content before applying any SQL chunks.
* **Authenticity:** Manifest publication and hydration require HMAC-SHA256 signing via `ARKILIAN_MANIFEST_HMAC_KEY`. A companion signature `{prefix}/manifest.sig` is stored alongside `{prefix}/manifest.json`. Hydration strictly refuses unauthenticated, missing, or mismatched manifest signatures (fail closed).

---

## Monitoring & Telemetry

Arkilian exposes extensive operational signals via C functions and language getters:

| Getter (Node.js) | C API | Description |
|---|---|---|
| `backupQueueDepth` | `db_backup_queue_depth` | Rows in `_pending_backup` waiting to be flushed to S3 |
| `backupOldestPendingAgeSec` | `db_backup_oldest_pending_age_sec` | Age in seconds of the oldest pending outbox row (real-time lag) |
| `backupDeadLetterCount` | `db_backup_dead_letter_count` | Poison rows moved to `_dead_backup` after exceeding max upload attempts |
| `backupThreadHeartbeatAgeMs` | `db_backup_thread_heartbeat_age_ms` | Milliseconds since the flush thread heartbeat (-1 if not running) |
| `backupSnapshotHeartbeatAgeMs` | `db_backup_snapshot_heartbeat_age_ms` | Milliseconds since the snapshot thread heartbeat (-1 if not running) |
| `backupTriggerCoverage` | `db_backup_trigger_coverage` | Sanity check: 0 if all PK-capable tables have CDC triggers; >0 if triggers are missing |
| `backupSkippedTableCount` | `db_backup_skipped_table_count` | Number of tables lacking a PRIMARY KEY (skipped by capture) |
| `backupHealthy` | `db_backup_is_healthy` | 1 if all core health conditions pass; 0 if degraded |
| `backupHealthFlags` | `db_backup_health_flags` | Bitmask of `ARK_HF_*` status flags identifying exact health status |
| `triggersDirty` | `db_backup_triggers_dirty` | 1 if raw-handle DDL bypassed wrappers and desynchronized triggers |
| `capturePaused` | `db_backup_capture_paused` | Sticky flag: 1 if CDC rows were dropped due to outbox hitting capacity limit |
| `autoResyncTriggers` | `db_get_auto_resync_triggers` | Boolean indicating if auto-repair of triggers on raw DDL is enabled |
| `backupChunkCount` | `db_backup_chunk_count` | Total count of CDC chunks successfully uploaded to S3 |
| `backupLastChunkFlushAgeMs` | `db_backup_last_chunk_flush_age_ms` | Milliseconds elapsed since the last successful chunk upload |

### Health State Machine Bitmask (`ARK_HF_*`)

`db_backup_health_flags()` returns a 32-bit integer bitmask isolating specific subsystem states:

```
Bit 0 (0x001): ARK_HF_BACKUP_ENABLED     — Backup enabled (kill-switch off)
Bit 1 (0x002): ARK_HF_DEST_CONFIGURED    — S3 credentials & bucket configured
Bit 2 (0x004): ARK_HF_FLUSH_ALIVE        — Flush thread heartbeat is fresh
Bit 3 (0x008): ARK_HF_SNAPSHOT_ALIVE     — Snapshot thread heartbeat is fresh
Bit 4 (0x010): ARK_HF_QUEUE_BELOW_CAP    — Outbox queue depth is below cap
Bit 5 (0x020): ARK_HF_SCHEMA_IN_SYNC     — Triggers are in sync with user tables
Bit 6 (0x040): ARK_HF_NO_DEAD_LETTER     — Dead letter queue is empty
Bit 7 (0x080): ARK_HF_MANIFEST_RESOLVED  — Manifest registry resolved and active
Bit 8 (0x100): ARK_HF_NO_CAPTURE_GAP     — No unclosed CDC drop gap pending snapshot
Bit 9 (0x200): ARK_HF_DURABLE_CAPTURE    — Outbox synchronous=FULL (informational)
Core Mask (0x1FF): ARK_HF_ALL_CORE       — All 9 core operational flags active
```

---

## Dead-Letter Queue (DLQ) Management

If an unrecoverable upload error occurs repeatedly, poisonous rows are preserved in `_dead_backup` to prevent blocking the CDC pipeline. Inspect and replay rows using `arkilian-dlq`:

```bash
# Compile the standalone tool (uses bundled SQLite amalgamation, zero extra dependencies)
cc tools/arkilian-dlq.c src/deps/sqlite/sqlite3.c -Isrc/deps/sqlite -o arkilian-dlq

# Check dead-letter row count
./arkilian-dlq app.sqlite --count

# Inspect dead-letter payloads and error reasons
./arkilian-dlq app.sqlite --list

# Test replay without modifying database
./arkilian-dlq app.sqlite --replay --dry-run

# Re-queue rows into _pending_backup for re-transmission
./arkilian-dlq app.sqlite --replay
```

---

## Running Tests

Build all 21 test suites with CMake:

```bash
cmake -B build -S . -DCMAKE_BUILD_TYPE=Debug -DARKILIAN_BUILD_TESTS=ON
cmake --build build --config Debug

# Run all 21 test suites via CTest
ctest --output-on-failure
```

Run the production client stress harness (exercises mock S3, outbox congestion, DLQ tool, and throughput benchmarks):

```bash
bash scripts/stress.sh --client-only
```

Run tests under AddressSanitizer and LeakSanitizer:

```bash
cmake -B build-asan -S . -DCMAKE_BUILD_TYPE=Debug -DARKILIAN_BUILD_TESTS=ON \
  -DCMAKE_C_FLAGS="-fsanitize=address,undefined -g" \
  -DCMAKE_EXE_LINKER_FLAGS="-fsanitize=address,undefined"
cmake --build build-asan
ctest --test-dir build-asan --output-on-failure
```

---

## Contributing

Please review [CONTRIBUTING.md](CONTRIBUTING.md) for contribution guidelines, coding standards, and verification requirements.

## License

Arkilian is licensed under the [MIT License](LICENSE).