<br/>
<h1 align="center">Arkilian</h1>  
<p align="center">
  <a href="https://github.com/CodeDynasty-dev/birth-of-Arkilian">
    <img src="https://avatars.githubusercontent.com/u/261335565?s=88&v=4" alt="Arkilian Database"   
    >
  </a>
</p>

[![PRs Welcome](https://img.shields.io/badge/PRs-welcome-brightgreen.svg)](https://github.com/CodeDynasty-dev/birth-of-Arkilian/blob/next/contributing.md)
![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)
[![Stargazers](https://img.shields.io/github/stars/CodeDynasty-dev/birth-of-Arkilian?style=social)](https://github.com/CodeDynasty-dev/birth-of-Arkilian)


# Arkilian


Arkilian is a managed embedded databas that wraps SQLite and is written in C, designed to extend SQLite with  automated cloud backup functionality and horizontal scaling (in the coming updates).

### Key Features
* **Simplified SQLite Binding:** Exposes fundamental SQLite session management alongside fully permissive raw handle extraction.
* **Background Data Protection:** Features an integrated background thread that continuously executes unblocking online snapshots and securely replicates the database to AWS S3 using presigned URLs. 
* **Cross-platform CMake Integration:** Configured to compile seamlessly across macOS, Linux, and Windows.
* **Multi-language Support:** Build as shared library for Node.js/Python FFI or static library for embedded C/C++ applications.
* **Environment-based Configuration:** All settings configurable via `ARKILIAN_` prefixed environment variables.

## Getting Started

### Prerequisites
* A C99 compliant compiler (GCC, Clang, or MSVC)
* CMake 3.10 or higher
* `libcurl` (e.g., `libcurl4-openssl-dev` on Debian/Ubuntu, or native via Xcode SDK on macOS)
* A POSIX environment or compatibility layer (for Windows)

### Build Instructions

You can build the library using CMake. Both static and shared libraries are built by default.

```bash
# Clone the repository
git clone https://github.com/CodeDynasty-dev/birth-of-Arkilian.git
cd birth-of-Arkilian

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
| `ARKILIAN_DATABASE_TOKEN` | (none) | Bearer token sent with both endpoints (never attached to pre-signed storage URLs) |
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
ARKILIAN_DATABASE_TOKEN=ak_...
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
#include <sqlite3.h>

int main(void) {
    arkilian *db = NULL;
    
    // Initialize Arkilian database context
    if (db_init(&db, "app.sqlite") != 0) {
        fprintf(stderr, "Initialization failed: %s\n", 
                db ? db_errmsg(db) : "Memory allocation error");
        if (db) db_close(db);
        return 1;
    }

    // Extract the raw sqlite3 handle to execute arbitrary statements
    sqlite3 *raw_db = db_get_handle(db);
    int rc = sqlite3_exec(raw_db, "CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT);", 0, 0, NULL);
    
    if (rc != SQLITE_OK) {
        fprintf(stderr, "SQL Execution failed: %s\n", db_errmsg(db));
    }

    // Release resources gracefully
    db_close(db);
    return 0;
}
```

Compile with static library:
```bash
gcc -I/usr/local/include/arkilian -L/usr/local/lib -larkilian myapp.c -o myapp
```

### Node.js FFI (using node-ffi or similar)

The shared library (`libarkilian.so`/`libarkilian.dylib`/`arkilian.dll`) exports C functions that can be called from Node.js using FFI libraries like `ffi-napi` or `koffi`.

### Python FFI (using ctypes)

```python
import ctypes
import os

# Load the shared library
if os.name == 'nt':  # Windows
    arkilian = ctypes.CDLL('./libarkilian.dll')
else:  # Unix-like
    arkilian = ctypes.CDLL('./libarkilian.so')

# Define function signatures
arkilian.db_init.restype = ctypes.c_int
arkilian.db_init.argtypes = [ctypes.POINTER(ctypes.c_void_p), ctypes.c_char_p]

# Use the library
db = ctypes.c_void_p()
ret = arkilian.db_init(ctypes.byref(db), b"app.sqlite")
```

## NPM Package

For Node.js projects, you can install via npm:

```bash
npm install arkilian
```

The package builds the native addon from source on install (requires a C
toolchain and libcurl development headers). Prebuilt binaries are not
published yet — CI produces them on releases, but the npm install path
currently compiles.

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

The client exposes spec §9 monitoring signals as C APIs and Node
getters: `backupQueueDepth`, `backupOldestPendingAgeSec` (the realtime-lag
metric), `backupDeadLetterCount`, `backupThreadHeartbeatAgeMs`,
`backupTriggerCoverage`, and `backupHealthy`. Diagnostics are routed
through `db_set_log_callback()` / `setLogCallback(fn)` (level, message).

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
./build/test_basic
```

## Contributing
Please see `CONTRIBUTING.md` for details on submitting patches and the contribution workflow.

## License
Arkilian is licensed under the MIT License. See the `LICENSE` file for details.
