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

Arkilian uses environment variables with the `ARKILIAN_` prefix for configuration:

| Variable | Default | Description |
|----------|---------|-------------|
| `ARKILIAN_DB_PATH` | `app.sqlite` | Path to the SQLite database file |
| `ARKILIAN_BACKUP_PATH` | `backup.sqlite` | Path for backup files |
| `ARKILIAN_BACKUP_INTERVAL` | `3600` | Backup interval in seconds (default: 1 hour) |
| `ARKILIAN_SIGNED_URL_ENDPOINT` | (none) | API endpoint for getting S3 presigned URLs |
| `ARKILIAN_ENABLE_BACKUP` | `1` | Set to `0` to disable automated backups |

Example `.env` file:
```
ARKILIAN_DB_PATH=myapp.db
ARKILIAN_BACKUP_PATH=/backups/myapp-backup.db
ARKILIAN_BACKUP_INTERVAL=7200
ARKILIAN_SIGNED_URL_ENDPOINT=https://myapi.com/get-signed-url
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

The package will attempt to download prebuilt binaries for your platform. If no prebuilt binary is available, it will fall back to building from source using cmake-js.

## System Constraints and Design Choices
Unlike complex distributed SQLite systems (e.g., LiteFS or rqlite), Arkilian embraces single-writer architectures partitioned by micro-datasets. It purposefully avoids:
* Virtual File System (VFS) complexities.
* Multi-writer coordination overhead and distributed consensus mechanisms.

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
