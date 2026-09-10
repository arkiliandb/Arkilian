use arkilian::Database;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db_file = "test_rust.db";

    // Open database
    let mut db = Database::new(db_file)?;
    db.set_token("dummy-test-token-00000000-0000-0000-0000-000000000000")?;
    println!("✓ Database opened with token");

    // Create table
    db.exec("DROP TABLE IF EXISTS users")?;
    db.exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")?;
    println!("✓ Table created");

    // Insert with parameters using String
    let alice = String::from("Alice");
    db.run("INSERT INTO users (name, age) VALUES (?, ?)", &[&alice as &dyn arkilian::ToSql, &30])?;
    println!("✓ Inserted Alice");

    let bob = String::from("Bob");
    db.run("INSERT INTO users (name, age) VALUES (?, ?)", &[&bob as &dyn arkilian::ToSql, &25])?;
    println!("✓ Inserted Bob");

    let charlie = String::from("Charlie");
    db.run("INSERT INTO users (name, age) VALUES (?, ?)", &[&charlie as &dyn arkilian::ToSql, &35])?;
    println!("✓ Inserted Charlie");

    // Query all rows
    let all_users = db.all("SELECT * FROM users", &[])?;
    println!("✓ All users:");
    for row in &all_users {
        for (name, value) in row {
            println!("    {}={}", name, value);
        }
    }

    // Query with parameters
    let age_filter = 28_i32;
    let older = db.all("SELECT name, age FROM users WHERE age > ?", &[&age_filter as &dyn arkilian::ToSql])?;
    println!("✓ Users older than 28:");
    for row in &older {
        for (name, value) in row {
            println!("    {}={}", name, value);
        }
    }

    // Update
    let new_age = 31_i32;
    db.run("UPDATE users SET age = ? WHERE name = ?", &[&new_age as &dyn arkilian::ToSql, &alice])?;
    println!("✓ Updated Alice's age");

    // Delete
    db.run("DELETE FROM users WHERE name = ?", &[&charlie as &dyn arkilian::ToSql])?;
    println!("✓ Deleted Charlie");

    // Final state
    let final_rows = db.all("SELECT * FROM users ORDER by id", &[])?;
    println!("✓ Final state:");
    for row in &final_rows {
        for (name, value) in row {
            println!("    {}={}", name, value);
        }
    }

    // ── Test New Core 1:1 Parity Features ─────────────────────────────────

    // 1. Transactions & Changes
    db.begin()?;
    db.exec("INSERT INTO users (name, age) VALUES ('RollbackMe', 99)")?;
    assert_eq!(db.changes(), 1);
    let rollback_id = db.last_insert_rowid();
    assert!(rollback_id > 0);
    db.rollback()?;
    println!("✓ Transactions (begin/rollback/changes/last_insert_rowid) working");

    // 2. Prepared Statement typed blobs & int64
    db.exec("CREATE TABLE blobs (id INTEGER PRIMARY KEY, bin BLOB, big INT)")?;
    db.prepare("INSERT INTO blobs (bin, big) VALUES (?, ?)")?;
    let payload = vec![0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0x01];
    db.bind_blob(1, &payload)?;
    db.bind_int64(2, 888888888888888)?;
    assert_eq!(db.step(), arkilian::SQLITE_DONE);
    db.finalize()?;

    db.prepare("SELECT bin, big FROM blobs WHERE id = 1")?;
    assert_eq!(db.step(), arkilian::SQLITE_ROW);
    assert_eq!(db.column_type(0), arkilian::SQLITE_BLOB);
    assert_eq!(db.column_blob(0), payload);
    assert_eq!(db.column_int64(1), 888888888888888);
    db.finalize()?;
    println!("✓ Typed BLOB and int64 column/binding working");

    // 3. WAL & Shipping
    let pending = db.wal_pending();
    println!("✓ WAL pending count: {}", pending);
    let _ = db.wal_flush();
    if let Some(sql) = db.wal_last_sql() {
        println!("✓ WAL last SQL: {}", sql);
    }

    // 4. Backup & Trigger Controls
    db.backup_set_enabled(true);
    assert!(db.backup_is_enabled());
    db.set_auto_resync_triggers(true);
    assert!(db.auto_resync_triggers());
    db.resync_triggers()?;
    let _ = db.backup_triggers_dirty();
    let _ = db.backup_capture_paused();
    println!("✓ Backup & trigger controls working");

    // 5. Monitoring & Health Flags
    let _ = db.backup_queue_depth();
    let _ = db.backup_oldest_pending_age_sec();
    let _ = db.backup_dead_letter_count();
    let _ = db.backup_thread_heartbeat_age_ms();
    let _ = db.backup_snapshot_heartbeat_age_ms();
    let _ = db.backup_trigger_coverage();
    let _ = db.backup_skipped_table_count();
    let _ = db.backup_chunk_count();
    let _ = db.backup_last_chunk_flush_age_ms();
    let flags = db.backup_health_flags();
    let _ = db.backup_is_healthy();
    println!("✓ Backup health flags: 0x{:04X}", flags);
    assert_eq!(arkilian::ARK_HF_ALL_CORE, 0x3FF);

    // db auto-closes on drop
    drop(db);
    println!("✓ Database closed");

    // Cleanup
    std::fs::remove_file(db_file)?;
    println!("\n✅ All Rust tests passed!");

    Ok(())
}