<br/>
<h1 align="center">Arkilian</h1>  
<p align="center">
  <a href="https://github.com/arkiliandb/Arkilian">
    <img src="https://avatars.githubusercontent.com/u/261335565?s=88&v=4" alt="Arkilian Database"   
    >
  </a>
</p>

[![PRs Welcome](https://img.shields.io/badge/PRs-welcome-brightgreen.svg)](https://github.com/arkiliandb/Arkilian/blob/next/contributing.md)
![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)
[![Stargazers](https://img.shields.io/github/stars/arkiliandb/Arkilian?style=social)](https://github.com/arkiliandb/Arkilian)


# Arkilian

Arkilian is a managed embedded database engine that wraps SQLite in C, extending it with automated, real-time CDC chunk streaming to S3-compatible object storage and periodic full snapshot backups.

> **Key Capabilities:** Continuous row-level CDC shipping, hourly consistent snapshots via `backup.sqlite`, cold-start S3 hydration with SHA-256 verification and mandatory HMAC manifest authenticity, deep multi-flag health telemetry, and native bindings across Node.js, Python, Go, Rust, and PHP.

### Key Features
* **Simplified SQLite Binding:** Exposes SQLite session and statement management alongside direct raw handle extraction (`db_get_handle`).
* **Direct-to-S3 Data Protection:** Two integrated background threads — a flush thread that packages row-level CDC writes into content-addressed, SHA-256-verified SQL chunks uploaded via AWS SigV4 presigned PUTs, and a snapshot thread that produces point-in-time full database backups.
* **Deterministic Manifest Protocol:** Manages a versioned, atomic manifest registry (`manifest.json` and HMAC-SHA256 `manifest.sig`) on S3 to guarantee restore ordering, zero capture gap leakage, and tamper resistance.
* **Cross-platform:** Native C99 compiling on macOS, Linux, and Windows (MSVC and MinGW).
* **Multi-language Support:** Complete feature parity across Node.js (N-API with prebuilt binaries), Python (ctypes), Go (cgo), Rust, and PHP with cold S3 hydration and deep health monitoring.
* **Environment-based Configuration:** All settings configurable via `ARKILIAN_` prefixed environment variables or a local `.env` file.

## Getting Started

### Prerequisites
* A C99 compliant compiler (GCC, Clang, or MSVC)
* CMake 3.10 or higher
* `libcurl` (e.g., `libcurl4-openssl-dev` on Debian/Ubuntu, or native via Xcode SDK on macOS, or vcpkg on Windows)

### Build Instructions

You can build the library using CMake. Both static and shared libraries are built by default.

```bash
# Clone the repository
git clone https://github.com/arkiliandb/Arkilian.git
cd Arkilian

# Generate build files
cmake -B build -S . -DCMAKE_BUILD_TYPE=Release

# Compile the target
cmake --build build --config Release

# Install to system (optional)
sudo cmake --install build
```

### Configuration

Arkilian uses environment variables with the `ARKILIAN_` prefix for configuration
(read from the process environment or a `./.env` file in the working directory; real
environment variables always take precedence over `.env` values). The S3 endpoint
defaults to empty; nothing ships or phones home unless explicitly configured.

| Variable | Default | Description |
|----------|---------|-------------|
| `ARKILIAN_DB_PATH` | `app.sqlite` | Path to the primary SQLite database file |
| `ARKILIAN_BACKUP_PATH` | `backup.sqlite` | Local path for point-in-time snapshot copies |
| `ARKILIAN_BACKUP_INTERVAL` | `3600` | Snapshot backup interval in seconds (min 1) |
| `ARKILIAN_CHUNK_INTERVAL_SEC` | `1` | Cadence in seconds for shipping pending outbox CDC rows to S3 chunks |
| `ARKILIAN_MANIFEST_INTERVAL_SEC` | `30` | Minimum interval in seconds between manifest publishing to avoid excessive metadata PUTs |
| `ARKILIAN_S3_ENDPOINT` | (none) | Base URL of the S3-compatible endpoint (path-style addressing, e.g. `https://s3.amazonaws.com` or `https://minio.internal:9000`). When unset, shipping is disabled |
| `ARKILIAN_S3_BUCKET` | (none) | Destination bucket holding `backup.sqlite`, `chunks/…`, and `manifest.json` |
| `ARKILIAN_S3_REGION` | `us-east-1` | AWS SigV4 request signing region |
| `ARKILIAN_S3_ACCESS_KEY` | (none) | SigV4 access key (used exclusively for local signing) |
| `ARKILIAN_S3_SECRET_KEY` | (none) | SigV4 secret key (used exclusively for local signing, never transmitted) |
| `ARKILIAN_S3_PREFIX` | `db_default` | Key prefix (e.g. `tenant-42-db`) isolating tenants within a shared bucket |
| `ARKILIAN_ENABLE_BACKUP` | `1` | `0`/`false` disables outbound shipping at startup; togglable at runtime via `db_backup_set_enabled()` |
| `ARKILIAN_OUTBOX_DURABLE` | `1` | `1` sets `PRAGMA synchronous=FULL;` on the application connection ensuring outbox CDC rows are durable at commit with zero loss on power failure. Set `0` (`PRAGMA synchronous=NORMAL;`) to prioritize high write throughput |
| `ARKILIAN_MAX_QUEUE_DEPTH` | `100000` | Soft ceiling on `_pending_backup` rows. If the queue reaches this limit, capture triggers pause inserts into the outbox so the application's own writes continue uninterrupted (spec §0 rule). Surfaces via `db_backup_capture_paused()` and clears on next successful snapshot |
| `ARKILIAN_MAX_ATTEMPTS` | `100` | Maximum upload retries for an outbox chunk before poison rows are dead-lettered to `_dead_backup` (inspectable via `arkilian-dlq`) |
| `ARKILIAN_MANIFEST_HMAC_KEY` | *(empty)* | HMAC-SHA-256 secret key for **manifest authenticity**. REQUIRED for publishing and S3 hydration: manifests are accompanied by `{prefix}/manifest.sig`. Hydration enforces fail-closed verification (unauthenticated or mismatched manifests are strictly refused) |
| `ARKILIAN_ALLOW_INSECURE` | `0` | Opt-in for cleartext `http://` endpoints that are NOT loopback / RFC1918. Default `0` refuses cleartext remote endpoints at startup to prevent leaking signatures |
| `ARKILIAN_STORAGE_HOSTS` | (none) | Comma-separated allowlist of custom storage hostnames (e.g. `minio.corp.internal,s3.example.com`). Prevents SSRF attacks to cloud metadata or internal networks |

Example `.env` file:
```
ARKILIAN_DB_PATH=myapp.db
ARKILIAN_BACKUP_PATH=/backups/myapp-backup.db
ARKILIAN_BACKUP_INTERVAL=7200
ARKILIAN_CHUNK_INTERVAL_SEC=1
ARKILIAN_MANIFEST_INTERVAL_SEC=30
ARKILIAN_S3_ENDPOINT=https://s3.amazonaws.com
ARKILIAN_S3_BUCKET=myapp-backups
ARKILIAN_S3_REGION=us-east-1
ARKILIAN_S3_ACCESS_KEY=AKIA...
ARKILIAN_S3_SECRET_KEY=...
ARKILIAN_S3_PREFIX=myapp
ARKILIAN_MANIFEST_HMAC_KEY=your-32-byte-secret-hmac-key
ARKILIAN_ENABLE_BACKUP=1
ARKILIAN_OUTBOX_DURABLE=1
```

### Build Options

| Option | Default | Description |
|--------|---------|-------------|
| `ARKILIAN_BUILD_SHARED` | `ON` | Build shared library for FFI (Node.js/Python) |
| `ARKILIAN_BUILD_STATIC` | `ON` | Build static library for embedded use |
| `ARKILIAN_BUILD_EXAMPLES` | `ON` | Build example programs |
| `ARKILIAN_BUILD_TESTS` | `OFF` | Build test programs |

## Usage Examples

### C/C++ Static Linking

```c
#include "class.h"
#include <stdio.h>

int main(void) {
    arkilian *db = NULL;
    
    // Initialize Arkilian database context (loads config from env / .env)
    if (db_init(&db, "app.sqlite") != 0) {
        fprintf(stderr, "Initialization failed: %s\n", 
                db ? db_errmsg(db) : "Allocation error");
        if (db) db_close(db);
        return 1;
    }

    // Execute SQL with automated CDC trigger wiring
    int rc = db_exec(db, "CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT);");
    if (rc != SQLITE_OK) {
        fprintf(stderr, "SQL Execution failed: %s\n", db_errmsg(db));
    }

    // Direct raw handle extraction for third-party libraries / raw SQLite APIs
    sqlite3 *raw_db = db_get_handle(db);
    (void)raw_db;
    // Note: DDL executed directly on raw_db sets db_backup_triggers_dirty(db).
    // Call db_resync_triggers(db) or enable db_set_auto_resync_triggers(db, 1).

    // Release resources gracefully
    db_close(db);
    return 0;
}
```

Compile with static library:
```bash
gcc -I/usr/local/include/arkilian -L/usr/local/lib -larkilian myapp.c -o myapp
```

### Node.js / Bun (npm package)

Arkilian ships as a **prebuilt N-API addon** — no C compiler or `libcurl-dev` required at install time.

```bash
npm install arkilian
```

```js
import Arkilian from 'arkilian';

// Optional: Cold-start restore from S3 before opening the database
// Requires ARKILIAN_MANIFEST_HMAC_KEY configured in environment.
/*
Arkilian.hydrateS3('app.sqlite', {
  endpoint: 'https://s3.amazonaws.com',
  bucket: 'myapp-backups',
  region: 'us-east-1',
  accessKey: process.env.ARKILIAN_S3_ACCESS_KEY,
  secretKey: process.env.ARKILIAN_S3_SECRET_KEY,
  prefix: 'myapp',
});
*/

// Initialize database instance
const db = new Arkilian('app.sqlite');

// Execute SQL
db.exec('CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT)');

// Prepared statements with binding
db.prepare('INSERT INTO users (name) VALUES (?)');
db.bindText(1, 'Alice');
db.step();
db.finalize();

// Helper query execution
db.run('INSERT INTO users (name) VALUES (?)', ['Bob']);
console.log('Last insert rowid:', db.lastInsertRowid);

// Check health metrics
console.log('Subsystem healthy:', db.backupHealthy);
console.log('Outbox queue depth:', db.backupQueueDepth);

db.close();
```

### FFI (C shared library)

The shared library (`libarkilian.so` / `libarkilian.dylib` / `arkilian.dll`) exports all C functions listed in `src/class.h` and can be called from any language with a C FFI (Python `ctypes`, Ruby `fiddle`, Go `cgo`, etc.).

```python
import ctypes, os

lib = ctypes.CDLL('./libarkilian.so' if os.name != 'nt' else './arkilian.dll')

lib.db_init.restype = ctypes.c_int
lib.db_init.argtypes = [ctypes.POINTER(ctypes.c_void_p), ctypes.c_char_p]

db = ctypes.c_void_p()
lib.db_init(ctypes.byref(db), b"app.sqlite")
```

## NPM Package

Prebuilt native addons (`.node`) for `linux-x64`, `linux-arm64` (glibc &
musl/Alpine), `darwin-x64`, `darwin-arm64`, and `win32-x64` are bundled
inside the npm package via [`prebuildify`](https://github.com/prebuild/prebuildify).
At runtime [`node-gyp-build`](https://github.com/prebuild/node-gyp-build)
selects the correct prebuild for your platform — **no C compiler, no
`libcurl-dev` headers, and no network download at install time**. This
makes `npm install arkilian` work on minimal Alpine containers, AWS
Lambda, and serverless environments that lack a build toolchain.

If no prebuilt binary matches your platform (e.g. a rare arch/libc
combination), the install script falls back to a source build via
`node-gyp`, which requires `gcc`/`clang` and `libcurl-dev`. On Windows,
provide libcurl through vcpkg and point MSVC at it before installing:

```pwsh
vcpkg install curl:x64-windows-static-md
$env:INCLUDE = "$env:VCPKG_ROOT\installed\x64-windows-static-md\include;$env:INCLUDE"
$env:LIB      = "$env:VCPKG_ROOT\installed\x64-windows-static-md\lib;$env:LIB"
npm install arkilian --build-from-source
```

## Real-World Examples

### 1 — Multi-tenant SaaS: one database per tenant, zero ops

Each tenant gets their own isolated SQLite file. Arkilian runs inside every
container instance and streams row changes to S3-compatible storage in real time.
If an instance is torn down, the next cold start calls `Arkilian.hydrateS3()` and
is back to the exact state it left off — including every write that shipped
while the old instance was live.

```js
// server.js
import Arkilian from 'arkilian';

// Configuration: ARKILIAN_S3_* env vars (see Configuration above)
const db = new Arkilian('app.sqlite');

// Schema is auto-created; capture triggers are wired automatically.
db.exec(`CREATE TABLE IF NOT EXISTS orders (
  id    INTEGER PRIMARY KEY,
  item  TEXT    NOT NULL,
  qty   INTEGER NOT NULL DEFAULT 1,
  ts    INTEGER NOT NULL DEFAULT (unixepoch())
)`);

// Every INSERT is captured and shipped within chunk interval (default 1s).
export function placeOrder(item, qty) {
  db.run('INSERT INTO orders (item, qty) VALUES (?, ?)', [item, qty]);
  return db.lastInsertRowid;
}

// Health endpoint.
export function health() {
  return {
    healthy:       db.backupHealthy,
    queueDepth:    db.backupQueueDepth,
    deadLetters:   db.backupDeadLetterCount,
    flushThreadMs: db.backupThreadHeartbeatAgeMs,
  };
}

process.on('SIGTERM', () => db.close());
```

---

### 2 — Real-time CDC Pipeline

Configure the background worker with the `ARKILIAN_S3_*` environment variables (see Configuration above) to stream raw row operations to S3-compatible storage in real time.

```js
import Arkilian from 'arkilian';

// Configuration comes from ARKILIAN_S3_* env vars (or ./.env)
const db = new Arkilian('app.sqlite');

db.exec(`CREATE TABLE IF NOT EXISTS users (
  id    INTEGER PRIMARY KEY,
  email TEXT    NOT NULL UNIQUE
)`);
```

---

### 3 — Offline-first Go Backend

Link the native library directly into your Go binaries using standard cgo and environment configuration.

```go
// main.go
package main

/*
#cgo LDFLAGS: -L./lib -larkilian -lcurl
#include "class.h"
#include <stdlib.h>
*/
import "C"
import (
    "log"
    "unsafe"
)

func main() {
    var db *C.arkilian
    path := C.CString("app.sqlite")
    defer C.free(unsafe.Pointer(path))

    // Initialize using environment variables (ARKILIAN_S3_*)
    if C.db_init(&db, path) != 0 {
        log.Fatal("db_init failed")
    }
    defer C.db_close(db)

    sql := C.CString(`CREATE TABLE IF NOT EXISTS players (
        id    INTEGER PRIMARY KEY,
        name  TEXT    NOT NULL,
        score INTEGER NOT NULL DEFAULT 0
    )`)
    defer C.free(unsafe.Pointer(sql))
    C.db_exec(db, sql)
}
```

---

### 4 — Incident Response: Kill-Switch & Diagnostics

Manage backups dynamically without restarting the application process.

```js
import Arkilian from 'arkilian';

// Configuration comes from ARKILIAN_S3_* env vars (or ./.env)
const db = new Arkilian('app.sqlite');

// Pause all outbound backup traffic instantly during an upstream outage.
db.setBackupEnabled(false);

// Resume normal operations.
db.setBackupEnabled(true);

db.close();
```

## System Constraints and Design Choices
Unlike complex distributed SQLite systems (e.g., LiteFS or rqlite), Arkilian embraces single-writer architectures partitioned by micro-datasets. It purposefully avoids:
* Virtual File System (VFS) complexities.
* Multi-writer coordination overhead and distributed consensus mechanisms.

## Guarantees, explicitly

* **Ordering** — delivery is strictly in `_pending_backup` id order; a
  retryable failure stops the drain so the first unshipped row is
  retried first (never skip-and-continue).
* **Delivery** — at-least-once. A crash between the storage ack and the
  local outbox delete re-ships the rows in a new chunk. Replay is
  idempotent (chunks use REPLACE/DELETE statements), so overlapping LSN
  ranges after a restart are safely replayed.
* **Durability** — by default, `ARKILIAN_OUTBOX_DURABLE=1` sets `PRAGMA synchronous=FULL;`
  on the application connection, guaranteeing committed outbox CDC rows are fsynced to disk
  before commit returns. Operators prioritizing maximum write throughput over power-loss
  durability can set `ARKILIAN_OUTBOX_DURABLE=0` (`PRAGMA synchronous=NORMAL;`).
* **Authenticity** — every manifest is authenticated with HMAC-SHA256 via `ARKILIAN_MANIFEST_HMAC_KEY`.
  Hydrators fail closed and refuse unauthenticated or tampered manifests.

## Monitoring & Operations

The client exposes spec §9 monitoring signals as C APIs and Node.js getters:

| Getter (Node.js) | C API | Description |
|---|---|---|
| `backupQueueDepth` | `db_backup_queue_depth` | Rows in outbox not yet delivered to S3 |
| `backupOldestPendingAgeSec` | `db_backup_oldest_pending_age_sec` | Realtime-lag metric (seconds since oldest pending row); 0 when queue is empty |
| `backupDeadLetterCount` | `db_backup_dead_letter_count` | Rows dead-lettered into `_dead_backup` after exceeding max retry attempts |
| `backupThreadHeartbeatAgeMs` | `db_backup_thread_heartbeat_age_ms` | Flush thread heartbeat age in ms (-1 if not running) |
| `backupSnapshotHeartbeatAgeMs` | `db_backup_snapshot_heartbeat_age_ms` | Snapshot thread heartbeat age in ms (-1 if not running) |
| `backupTriggerCoverage` | `db_backup_trigger_coverage` | Trigger sanity check: 0 = all PK-capable tables covered; N > 0 = missing triggers |
| `backupSkippedTableCount` | `db_backup_skipped_table_count` | Tables with no PRIMARY KEY skipped by capture (must be 0) |
| `backupHealthy` | `db_backup_is_healthy` | 1 = subsystem fully healthy; 0 = degraded |
| `backupHealthFlags` | `db_backup_health_flags` | Bitmask of `ARK_HF_*` status flags isolating specific degraded conditions |
| `triggersDirty` | `db_backup_triggers_dirty` | 1 = raw-handle DDL bypassed the wrapper and desynchronized triggers |
| `capturePaused` | `db_backup_capture_paused` | Sticky flag: 1 = CDC rows were dropped due to queue hitting `ARKILIAN_MAX_QUEUE_DEPTH` (cleared on snapshot) |
| `autoResyncTriggers` | `db_get_auto_resync_triggers` | Boolean: whether auto-repair of raw DDL triggers is enabled |
| `backupChunkCount` | `db_backup_chunk_count` | Total number of chunks successfully flushed to S3 |
| `backupLastChunkFlushAgeMs` | `db_backup_last_chunk_flush_age_ms` | Milliseconds elapsed since the last successful chunk flush |

Diagnostics are routed through `db_set_log_callback()` / `setLogCallback(fn)` (level, message).

Dead-lettered rows are inspected and replayed with the bundled CLI:

```sh
cc tools/arkilian-dlq.c src/deps/sqlite/sqlite3.c -Isrc/deps/sqlite -o arkilian-dlq
./arkilian-dlq app.sqlite --list
./arkilian-dlq app.sqlite --replay --dry-run
./arkilian-dlq app.sqlite --replay
```

## Running Tests

Build the test suites with CMake:

```bash
cmake -B build -S . -DCMAKE_BUILD_TYPE=Debug -DARKILIAN_BUILD_TESTS=ON
cmake --build build --config Debug

# Run all 19 test suites via CTest
ctest --output-on-failure
```

To run the client-only production stress harness:

```bash
bash scripts/stress.sh --client-only
```

## Contributing
Please see `CONTRIBUTING.md` for details on submitting patches and the contribution workflow.

## License
Arkilian is licensed under the MIT License. See the `LICENSE` file for details.
```check out arkilian.com for more detailed info beyond what we can provide here```