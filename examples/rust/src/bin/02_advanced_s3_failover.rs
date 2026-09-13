//! Arkilian Rust SDK - Advanced S3 Streaming & Cold-Start Hydration
//!
//! Demonstrates:
//! 1. Continuous WAL streaming to MinIO / AWS S3
//! 2. Telemetry inspection (sidecar health, bitmask health flags, outbox queue depth)
//! 3. Zero-lag WAL flushes via db.wal_flush()
//! 4. Primary host disaster simulation (deleting local database files)
//! 5. Cold-start disaster recovery via arkilian::hydrate_s3()
//! 6. Data parity and integrity validation on the recovered database

use std::env;
use std::fs;
use std::path::Path;
use std::thread;
use std::time::Duration;
use arkilian::{hydrate_s3, Database, S3Config, SQLITE_ROW};

const PRIMARY_DB: &str = "primary_production.sqlite";
const RESTORED_DB: &str = "hydrated_recovery.sqlite";

fn cleanup_db(path: &str) {
    for ext in &["", "-wal", "-shm"] {
        let file = format!("{}{}", path, ext);
        if Path::new(&file).exists() {
            let _ = fs::remove_file(file);
        }
    }
}

fn get_env(key: &str, fallback: &str) -> String {
    env::var(key).unwrap_or_else(|_| fallback.to_string())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Arkilian Rust SDK: Advanced S3 Streaming & Cold-Start Hydration ===\n");

    let endpoint = get_env("ARKILIAN_S3_ENDPOINT", "http://127.0.0.1:9000");
    let bucket = get_env("ARKILIAN_S3_BUCKET", "arkilian-test-bucket");
    let region = get_env("ARKILIAN_S3_REGION", "us-east-1");
    let access_key = get_env("ARKILIAN_S3_ACCESS_KEY", "minioadmin");
    let secret_key = get_env("ARKILIAN_S3_SECRET_KEY", "minioadmin");
    let prefix = get_env("ARKILIAN_S3_PREFIX", "rust-demo");
    let hmac_key = get_env("ARKILIAN_MANIFEST_HMAC_KEY", "super-secret-hmac-key-for-manifests");

    // Configure environment variables for the Arkilian C sidecar worker
    env::set_var("ARKILIAN_ENABLE_BACKUP", "1");
    env::set_var("ARKILIAN_S3_ENDPOINT", &endpoint);
    env::set_var("ARKILIAN_S3_BUCKET", &bucket);
    env::set_var("ARKILIAN_S3_REGION", &region);
    env::set_var("ARKILIAN_S3_ACCESS_KEY", &access_key);
    env::set_var("ARKILIAN_S3_SECRET_KEY", &secret_key);
    env::set_var("ARKILIAN_S3_PREFIX", &prefix);
    env::set_var("ARKILIAN_MANIFEST_HMAC_KEY", &hmac_key);
    env::set_var("ARKILIAN_CHUNK_INTERVAL_SEC", "1");
    env::set_var("ARKILIAN_MANIFEST_INTERVAL_SEC", "1");

    println!("[Config] Target S3 Endpoint: {}", endpoint);
    println!("[Config] Bucket: {} | Prefix: {}\n", bucket, prefix);

    cleanup_db(PRIMARY_DB);
    cleanup_db(RESTORED_DB);

    // -------------------------------------------------------------
    // Phase 1: Primary Database Workload with Real-Time S3 Streaming
    // -------------------------------------------------------------
    println!("[Phase 1] Opening Primary Node with S3 WAL Streaming...");
    let mut db = Database::new(PRIMARY_DB).map_err(|e| format!("Open error: {}", e))?;

    // Verify sidecar telemetry
    println!("  Telemetry -> Sidecar Healthy: {}", db.backup_is_healthy());
    println!("  Telemetry -> Health Flags: 0x{:x}", db.backup_health_flags());
    println!("  Telemetry -> Queue Depth: {}", db.backup_queue_depth());

    println!("\n[Phase 1] Creating schema and streaming mission event logs...");
    let schema_sql = r#"
        CREATE TABLE IF NOT EXISTS mission_events (
            event_id INTEGER PRIMARY KEY AUTOINCREMENT,
            module_name TEXT NOT NULL,
            severity TEXT NOT NULL,
            payload_value REAL NOT NULL,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
        );
    "#;
    db.exec(schema_sql).map_err(|e| format!("Schema error: {}", e))?;

    let total_records = 30;
    let insert_sql = "INSERT INTO mission_events (module_name, severity, payload_value) VALUES (?, ?, ?);";

    db.begin().map_err(|e| format!("Begin error: {}", e))?;
    for i in 1..=total_records {
        db.prepare(insert_sql).map_err(|e| format!("Prepare error: {}", e))?;
        db.bind_text(1, &format!("telemetry_subsystem_{}", i % 5))?;
        let severity = if i % 7 == 0 { "CRITICAL" } else if i % 3 == 0 { "WARN" } else { "INFO" };
        db.bind_text(2, severity)?;
        db.bind_double(3, (i as f64) * 14.25)?;
        let _ = db.step();
        db.finalize().map_err(|e| format!("Finalize error: {}", e))?;
    }
    db.commit().map_err(|e| format!("Commit error: {}", e))?;
    println!("  ✓ Committed {} mission events in an atomic transaction.", total_records);

    // Explicit WAL flush: wake up flush thread and drain to S3
    println!("\n[Phase 1] Triggering explicit WAL flush to S3 sidecar...");
    db.wal_flush();

    println!("  Waiting for S3 sidecar outbox to drain...");
    for _ in 0..20 {
        thread::sleep(Duration::from_millis(500));
        if db.backup_queue_depth() == 0 {
            break;
        }
    }
    // Allow manifest publish to finalize
    thread::sleep(Duration::from_millis(1500));

    println!("  Telemetry -> Queue Depth: {}", db.backup_queue_depth());
    println!("  Telemetry -> Sidecar Healthy: {}", db.backup_is_healthy());

    // Record count on primary
    db.prepare("SELECT COUNT(*) FROM mission_events;").map_err(|e| format!("Prepare error: {}", e))?;
    let _ = db.step();
    let primary_count = db.column_int64(0);
    db.finalize().map_err(|e| format!("Finalize error: {}", e))?;
    println!("  Primary DB confirmed record count: {}", primary_count);

    println!("[Phase 1] Closing primary database connection.");
    db.close();

    // -------------------------------------------------------------
    // Phase 2: Disaster Simulation
    // -------------------------------------------------------------
    println!("\n[Phase 2] SIMULATING DISASTER: Destroying primary host!");
    println!("  Purging local files: {}...", PRIMARY_DB);
    cleanup_db(PRIMARY_DB);
    println!("  ✓ Primary local database destroyed. Zero local state remains.");

    // -------------------------------------------------------------
    // Phase 3: Cold-Start Hydration from MinIO S3
    // -------------------------------------------------------------
    println!("\n[Phase 3] Starting Cold-Start Hydration from MinIO S3...");
    println!("  Target path: {}", RESTORED_DB);

    let s3_config = S3Config {
        endpoint: endpoint.clone(),
        bucket: bucket.clone(),
        region: region.clone(),
        access_key_id: access_key.clone(),
        secret_access_key: secret_key.clone(),
        session_token: None,
        use_ssl: false,
        timeout_ms: 15_000,
    };

    if let Err(e) = hydrate_s3(RESTORED_DB, &prefix, &s3_config) {
        eprintln!("  ✗ Hydration failed: {}", e);
        std::process::exit(1);
    }
    println!("  ✓ Hydration completed successfully!");

    // -------------------------------------------------------------
    // Phase 4: Data Parity & Integrity Verification
    // -------------------------------------------------------------
    println!("\n[Phase 4] Opening Hydrated Database to verify integrity...");
    env::set_var("ARKILIAN_ENABLE_BACKUP", "0");
    let mut recovered_db = Database::new(RESTORED_DB).map_err(|e| format!("Open error: {}", e))?;

    recovered_db.prepare("SELECT COUNT(*) FROM mission_events;").map_err(|e| format!("Prepare error: {}", e))?;
    let _ = recovered_db.step();
    let recovered_count = recovered_db.column_int64(0);
    recovered_db.finalize().map_err(|e| format!("Finalize error: {}", e))?;
    println!("  Recovered DB record count: {}", recovered_count);

    if recovered_count != primary_count {
        eprintln!("  ✗ Data mismatch! Expected {}, got {}", primary_count, recovered_count);
        std::process::exit(1);
    }

    println!("\n  Sample records from recovered instance:");
    recovered_db.prepare("SELECT event_id, module_name, severity, payload_value FROM mission_events LIMIT 5;")
        .map_err(|e| format!("Prepare error: {}", e))?;
    while recovered_db.step() == SQLITE_ROW {
        println!("    - Event #{:<2} | Module: {:<22} | Severity: {:<8} | Value: {:>8.2}",
            recovered_db.column_int64(0),
            recovered_db.column_text(1).unwrap_or_default(),
            recovered_db.column_text(2).unwrap_or_default(),
            recovered_db.column_double(3));
    }
    recovered_db.finalize().map_err(|e| format!("Finalize error: {}", e))?;

    recovered_db.close();
    cleanup_db(RESTORED_DB);

    println!("\n=== Disaster Recovery & Verification Finished Successfully! ===");
    Ok(())
}
