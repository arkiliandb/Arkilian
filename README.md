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

<!-- ![Arkilian](https://img.shields.io/github/v/release/CodeDynasty-dev/birth-of-Arkilian) -->


# Arkilian


Arkilian is a scalability and durability architecture that wraps SQLite and is written in C, designed to extend SQLite with seamless capabilities, like automated cloud backup functionality, and horizontal scaling (in the coming updates). Arkilian can be integrated into applications distributed across multiple environments.

### Key Features
* **Simplified SQLite Binding:** Exposes fundamental SQLite session management alongside fully permissive raw handle extraction.
* **Background Data Protection:** Features an integrated background thread that continuously executes unblocking online snapshots and securely replicates the database to AWS S3 using presigned URLs. 
* **Cross-platform CMake Integration:** Configured to compile seamlessly across macOS & Linux.

## Getting Started

### Prerequisites
* A C99 compliant compiler (GCC, Clang, or MSVC)
* CMake 3.10 or higher
* `libcurl` (e.g., `libcurl4-openssl-dev` on Debian/Ubuntu, or native via Xcode SDK on macOS)
* A POSIX environment or compatibility layer (for Windows)

### Build Instructions

You can build the library using CMake.

```bash
# Clone the repository
git clone https://github.com/CodeDynasty-dev/birth-of-Arkilian.git
cd birth-of-Arkilian

# Generate build files
cmake -B build -S . -DCMAKE_BUILD_TYPE=Release

# Compile the target
cmake --build build --config Release
```

## Usage Example

The library exposes the `class.h` public header for straightforward integration. Initializing Arkilian automatically enables the background backup system.

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

## System Constraints and Design Choices
Unlike complex distributed SQLite systems (e.g., LiteFS or rqlite), Arkilian embraces single-writer architectures partitioned by micro-datasets. It purposefully avoids:
* Virtual File System (VFS) complexities.
* Multi-writer coordination overhead and distributed consensus mechanisms.

## Contributing
Please see `CONTRIBUTING.md` for details on submitting patches and the contribution workflow.

## License
Arkilian is licensed under the MIT License. See the `LICENSE` file for details.
