# Arkilian Rust Bindings

Safe, idiomatic Rust abstractions on top of the Arkilian managed SQLite database engine (`arkilian-sys`).

## Features

- **Automated S3 Replication:** Point-in-time full snapshots and real-time CDC SQL chunk streaming directly to S3-compatible storage.
- **Fail-Safe Isolation:** CDC capture never blocks or fails application transactions.
- **Idiomatic Rust API:** Safe statements, parameter bindings, transactions, and row iteration.
- **Deep Telemetry:** Query outbox queue depth, flush and snapshot thread heartbeats, and 32-bit health bitmasks (`ARK_HF_*`).
- **Cold S3 Hydration:** Restore a database from S3 before opening with full HMAC manifest verification and SHA-256 digest checks.

## Installation

Add `arkilian` to your `Cargo.toml`:

```toml
[dependencies]
arkilian = { path = "bindings/rust/arkilian" }
```

## Quick Start

```rust
use arkilian::Database;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Open database (loads ARKILIAN_* environment variables or .env)
    let mut db = Database::new("app.sqlite")?;

    // Execute DDL (automatically installs CDC capture triggers)
    db.exec("CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT, age INT)")?;

    // Parameterized inserts
    db.run(
        "INSERT INTO users (name, age) VALUES (?, ?)",
        &[&"Alice" as &dyn arkilian::ToSql, &30_i32],
    )?;

    // Query rows
    let rows = db.all("SELECT id, name, age FROM users", &[])?;
    for row in rows {
        println!("User: {:?}", row);
    }

    // Health diagnostics
    println!("Healthy: {}", db.backup_is_healthy());
    println!("Queue depth: {}", db.backup_queue_depth());

    Ok(())
}
```

## Configuration

Arkilian reads its configuration from the process environment or a local `.env` file:

- `ARKILIAN_DB_PATH`: Local database path (default: `app.sqlite`)
- `ARKILIAN_S3_ENDPOINT`: S3-compatible endpoint URL (e.g. `https://s3.amazonaws.com`)
- `ARKILIAN_S3_BUCKET`: Target bucket name
- `ARKILIAN_S3_REGION`: SigV4 signing region (default: `us-east-1`)
- `ARKILIAN_S3_ACCESS_KEY`: SigV4 access key ID
- `ARKILIAN_S3_SECRET_KEY`: SigV4 secret access key
- `ARKILIAN_S3_PREFIX`: Object key prefix (default: `db_default`)
- `ARKILIAN_MANIFEST_HMAC_KEY`: Secret HMAC key for manifest authenticity (REQUIRED for publishing and hydration)
- `ARKILIAN_OUTBOX_DURABLE`: `1` (default) for `PRAGMA synchronous=FULL;`, `0` for `NORMAL`
- `ARKILIAN_ENABLE_BACKUP`: `1` (default) enables background streaming

## License

Licensed under the MIT License.
