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


Arkilian is a managed embedded database that wraps SQLite and is written in C, designed to extend SQLite with automated cloud backup functionality and horizontal scaling (in the coming updates).

### Key Features
* **Simplified SQLite Binding:** Exposes fundamental SQLite session management alongside fully permissive raw handle extraction.
* **Background Data Protection:** Features two integrated background threads — a flush thread that continuously ships row-level changes to a push endpoint, and a snapshot thread that uploads full hourly backups to S3 via presigned URLs.
* **Cross-platform:** Compiles natively on macOS, Linux, and Windows (MSVC and MinGW) without a POSIX compatibility layer.
* **Multi-language Support:** Build as a shared library for FFI or static library for embedded C/C++ applications. A prebuilt N-API addon is published to npm for Node.js/Bun.
* **Environment-based Configuration:** All settings configurable via `ARKILIAN_` prefixed environment variables.

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
(read from the environment or a `./.env` file in the working directory — real
environment variables always win over `.env` values). Both endpoint variables
default to empty; nothing phones home unless explicitly configured.

| Variable | Default | Description |
|----------|---------|-------------|
| `ARKILIAN_DB_PATH` | `app.sqlite` | Path to the SQLite database file |
| `ARKILIAN_BACKUP_PATH` | `backup.sqlite` | Local path for hourly snapshot copies |
| `ARKILIAN_BACKUP_INTERVAL` | `3600` | Hourly snapshot interval in seconds (min 1) |
| `ARKILIAN_WAL_PUSH_URL` | (none) | Realtime destination for row changes — every write is shipped here as replayable SQL (e.g. control plane `POST /v1/wal/push`) |
| `ARKILIAN_SIGNED_URL_ENDPOINT` | (none) | Signed-URL issuer for hourly snapshot uploads (e.g. control plane `POST /v1/upload/request`). Independent of `ARKILIAN_WAL_PUSH_URL` — they are different endpoints |
| `ARKILIAN_API_KEY` | (none) | Bearer token sent with both endpoints (never attached to pre-signed storage URLs) |
| `ARKILIAN_ENABLE_BACKUP` | `1` | `0`/`false` disables outbound backup at startup; can be toggled at runtime with `db_backup_set_enabled()` |
| `ARKILIAN_MAX_QUEUE_DEPTH` | `100000` | Soft ceiling on `_pending_backup` rows. Once the queue reaches this depth the capture triggers pause INSERTs into the outbox (the application's own writes are unaffected, per the spec §0 "backup must never break the application" rule), and `db_backup_is_healthy()` flips to 0 so the loss of capture is visible via monitoring. Shipping drains the queue and capture resumes automatically when the depth drops back below the cap |
| `ARKILIAN_ALLOW_INSECURE` | `0` | Opt-in for cleartext `http://` endpoints that are NOT loopback / RFC1918 (e.g. an internal-but-public corporate aggregator). Default `0`: a non-HTTPS non-local endpoint is refused at startup and backup is disabled, so a misconfiguration cannot leak the bearer token in cleartext. Loopback (`127.x`, `::1`, `localhost`) and RFC1918 / link-local / ULA addresses are always permitted for dev without opt-in |
| `ARKILIAN_STORAGE_HOSTS` | (none) | Comma-separated suffix-allowlist of self-hosted storage hosts (e.g. `minio.internal.corp,s3.example.com`). The SSRF guard refuses to upload a snapshot or download a hydration chunk to/from a host that is not a well-known storage provider (AWS S3, GCS, Azure Blob, Backblaze B2, Cloudflare R2, Wasabi, DigitalOcean Spaces), a loopback / RFC1918 address, or in this allowlist. Prevents a compromised control plane from exfiltrating the database to cloud metadata or an internal service |

Example `.env` file:
```
ARKILIAN_DB_PATH=myapp.db
ARKILIAN_BACKUP_PATH=/backups/myapp-backup.db
ARKILIAN_BACKUP_INTERVAL=7200
ARKILIAN_WAL_PUSH_URL=https://api.example.com/v1/wal/push
ARKILIAN_SIGNED_URL_ENDPOINT=https://api.example.com/v1/upload/request
ARKILIAN_API_KEY=ak_...
ARKILIAN_ENABLE_BACKUP=1
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
    
    // Initialize Arkilian database context
    if (db_init(&db, "app.sqlite") != 0) {
        fprintf(stderr, "Initialization failed: %s\n", 
                db ? db_errmsg(db) : "Memory allocation error");
        if (db) db_close(db);
        return 1;
    }

    // Execute SQL directly through the wrapper
    int rc = db_exec(db, "CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT);");
    
    if (rc != SQLITE_OK) {
        fprintf(stderr, "SQL Execution failed: %s\n", db_errmsg(db));
    }

    // Or extract the raw sqlite3 handle for direct SQLite API access
    sqlite3 *raw_db = db_get_handle(db);
    // Note: DDL via the raw handle bypasses capture triggers.
    // Call db_resync_triggers(db) afterwards to re-sync them.

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

const db = new Arkilian('your-api-key', 'app.sqlite');

// Execute SQL
db.exec('CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT)');

// Prepared statements
db.prepare('INSERT INTO users (name) VALUES (?)');
db.bindText(1, 'Alice');
db.step();
db.finalize();

// Cold-start restore from the control plane (call before new Arkilian())
Arkilian.hydrate('app.sqlite', 'https://api.arkilian.com', 'your-api-key');

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
`node-gyp`, which requires `gcc`/`clang` and `libcurl-dev`.

## Real-World Examples

### 1 — Multi-tenant SaaS: one database per tenant, zero ops

Each tenant gets their own isolated SQLite file. Arkilian runs inside every
Cloud Run instance and streams row changes to your control plane in real time.
If an instance is torn down, the next cold start calls `Arkilian.hydrate()` and
is back to the exact state it left off — including every write that shipped
while the old instance was live.

```js
// server.js
import Arkilian from 'arkilian';

// Get your API token from https://arkilian.com
const API_TOKEN = process.env.ARKILIAN_API_KEY;
const db = new Arkilian(API_TOKEN, 'app.sqlite');

// Schema is auto-created; capture triggers are wired automatically.
db.exec(`CREATE TABLE IF NOT EXISTS orders (
  id    INTEGER PRIMARY KEY,
  item  TEXT    NOT NULL,
  qty   INTEGER NOT NULL DEFAULT 1,
  ts    INTEGER NOT NULL DEFAULT (unixepoch())
)`);

// Every INSERT is captured and shipped in < 2 s.
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

Configure the background worker with your `ARKILIAN_API_KEY` and endpoints obtained from [arkilian.com](https://arkilian.com) to stream raw row operations in real time.

```js
import Arkilian from 'arkilian';

// Get your configuration and API token from https://arkilian.com
const token = process.env.ARKILIAN_API_KEY;
const db = new Arkilian(token, 'app.sqlite');

db.exec(`CREATE TABLE IF NOT EXISTS users (
  id    INTEGER PRIMARY KEY,
  email TEXT    NOT NULL UNIQUE
)`);
```

---

### 3 — Offline-first Go Backend

Link the native library directly into your Go binaries. Acquire the client SDK assets and environment configuration templates from [arkilian.com](https://arkilian.com).

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
    "fmt"
    "log"
    "unsafe"
)

func main() {
    var db *C.arkilian
    path := C.CString("app.sqlite")
    defer C.free(unsafe.Pointer(path))

    // Initialize using configuration obtained from https://arkilian.com
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

// Retrieve your API token from https://arkilian.com
const db = new Arkilian(process.env.ARKILIAN_API_KEY, 'app.sqlite');

// Pause all outbound backup traffic instantly during an upstream outage.
db.setBackupEnabled(false);

// Resume normal operations.
db.setBackupEnabled(true);

db.close();
```
```

## System Constraints and Design Choices
Unlike complex distributed SQLite systems (e.g., LiteFS or rqlite), Arkilian embraces single-writer architectures partitioned by micro-datasets. It purposefully avoids:
* Virtual File System (VFS) complexities.
* Multi-writer coordination overhead and distributed consensus mechanisms.

## Guarantees, explicitly

* **Ordering** — delivery is strictly in `_pending_backup` id order; a
  retryable failure stops the drain so the first unshipped row is
  retried first (never skip-and-continue). This is a reviewed decision
  (spec §8.1): if your destination does not require ordering,
  skip-and-continue is the higher-throughput alternative.
* **Delivery** — at-least-once. A crash between destination ack and
  local delete re-ships the row. The destination MUST dedupe on the
  `X-Arkilian-Payload-Id` header (the bundled control plane does:
  `ON CONFLICT(db_id, payload_id) DO NOTHING`).
* **Durability** — `PRAGMA synchronous=NORMAL` (WAL): durable across
  process crashes; the most recent transactions can be lost on OS
  crash/power loss (spec §3.2). If that window is unacceptable for your
  data, set `synchronous=FULL` on the game connection — the backup
  connection's setting matters less since it only deletes already-durable
  rows.

## Monitoring & operations

The client exposes spec §9 monitoring signals as C APIs and Node getters:

| Getter (Node.js) | C API | Description |
|---|---|---|
| `backupQueueDepth` | `db_backup_queue_depth` | Rows in outbox not yet delivered |
| `backupOldestPendingAgeSec` | `db_backup_oldest_pending_age_sec` | Realtime-lag metric; 0 when queue is empty |
| `backupDeadLetterCount` | `db_backup_dead_letter_count` | Rows dead-lettered after max retries |
| `backupThreadHeartbeatAgeMs` | `db_backup_thread_heartbeat_age_ms` | Flush thread liveness; -1 if not running |
| `backupSnapshotHeartbeatAgeMs` | `db_backup_snapshot_heartbeat_age_ms` | Snapshot thread liveness; -1 if not running |
| `backupTriggerCoverage` | `db_backup_trigger_coverage` | 0 = all tables covered; N = N triggers missing |
| `backupSkippedTableCount` | `db_backup_skipped_table_count` | Tables with no PK skipped by capture (must be 0) |
| `backupHealthy` | `db_backup_is_healthy` | 1 = subsystem fully healthy; 0 = investigate |
| `triggersDirty` | `db_backup_triggers_dirty` | 1 = raw-handle DDL desynchronized triggers |
| `capturePaused` | `db_backup_capture_paused` | Sticky: CDC rows dropped since last snapshot |

Diagnostics are routed through `db_set_log_callback()` / `setLogCallback(fn)` (level, message).

Dead-lettered rows are inspected and replayed with the bundled CLI:

```sh
cc tools/arkilian-dlq.c src/deps/sqlite/sqlite3.c -Isrc/deps/sqlite -o arkilian-dlq
./arkilian-dlq app.sqlite --list
./arkilian-dlq app.sqlite --replay --dry-run
./arkilian-dlq app.sqlite --replay
```

See `docs/operations.md` for alert thresholds, the dead-letter runbook,
the kill-switch procedure, and incident response.

## Running Tests

```bash
cmake -B build -S . -DCMAKE_BUILD_TYPE=Debug -DARKILIAN_BUILD_TESTS=ON
cmake --build build --config Debug

# Run all 11 test suites
for t in test_basic test_interception test_regressions test_monitoring \
          test_deterministic test_virtual_tables test_hardening \
          test_kill_switch test_kill_resilience test_load_contention \
          test_dst_backpressure; do
  ./build/$t
done
```

## Contributing
Please see `CONTRIBUTING.md` for details on submitting patches and the contribution workflow.

## License
Arkilian is licensed under the MIT License. See the `LICENSE` file for details.

<!-- check out arkilian.com for more detailed info beyond what we can provide here-->