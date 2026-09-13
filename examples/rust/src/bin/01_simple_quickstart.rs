//! Arkilian Rust SDK - Simple Quickstart Example
//!
//! Demonstrates:
//! 1. Initializing an embedded Arkilian database
//! 2. Creating tables (DDL)
//! 3. Prepared statements and typed parameter binding
//! 4. Stepping through result rows and reading typed columns
//! 5. Atomic transactions (begin / commit / rollback)
//! 6. Automatic RAII cleanup

use std::fs;
use std::path::Path;
use arkilian::{Database, SQLITE_ROW};

const DB_PATH: &str = "quickstart.sqlite";

fn cleanup_db(path: &str) {
    for ext in &["", "-wal", "-shm"] {
        let file = format!("{}{}", path, ext);
        if Path::new(&file).exists() {
            let _ = fs::remove_file(file);
        }
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Arkilian Rust SDK: Simple Quickstart ===\n");

    cleanup_db(DB_PATH);

    // In embedded-only mode, disable backup warnings
    std::env::set_var("ARKILIAN_ENABLE_BACKUP", "0");

    // 1. Open database
    println!("[1] Initializing Arkilian embedded database at '{}'...", DB_PATH);
    let mut db = Database::new(DB_PATH).map_err(|e| format!("Open error: {}", e))?;

    // 2. Create schema
    println!("[2] Creating schema...");
    let schema_sql = r#"
        CREATE TABLE IF NOT EXISTS game_saves (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            player_name TEXT UNIQUE NOT NULL,
            level INTEGER NOT NULL,
            score REAL NOT NULL,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
        );
    "#;
    db.exec(schema_sql).map_err(|e| format!("Exec error: {}", e))?;
    println!("    ✓ Table 'game_saves' created.");

    // 3. Insert records using prepared statements
    println!("[3] Inserting sample player save records...");
    let insert_sql = "INSERT INTO game_saves (player_name, level, score) VALUES (?, ?, ?);";
    
    let players = [
        ("Valkyrie", 42, 18500.5),
        ("ShadowSniper", 37, 14200.0),
        ("CyberPaladin", 55, 32100.75),
    ];

    for &(name, level, score) in &players {
        db.prepare(insert_sql).map_err(|e| format!("Prepare error: {}", e))?;
        db.bind_text(1, name).map_err(|e| format!("Bind text error: {}", e))?;
        db.bind_int(2, level).map_err(|e| format!("Bind int error: {}", e))?;
        db.bind_double(3, score).map_err(|e| format!("Bind double error: {}", e))?;
        let _ = db.step();
        db.finalize().map_err(|e| format!("Finalize error: {}", e))?;
    }
    println!("    ✓ {} player saves inserted.", players.len());

    // 4. Query all records
    println!("\n[4] Querying all player saves:");
    let query_sql = "SELECT id, player_name, level, score FROM game_saves ORDER BY score DESC;";
    db.prepare(query_sql).map_err(|e| format!("Prepare error: {}", e))?;

    while db.step() == SQLITE_ROW {
        let id = db.column_int64(0);
        let name = db.column_text(1).unwrap_or_default();
        let level = db.column_int(2);
        let score = db.column_double(3);
        println!("    - Rank: ID {} | Player: {:<14} | Level {:>2} | Score: {:>8.2}", id, name, level, score);
    }
    db.finalize().map_err(|e| format!("Finalize error: {}", e))?;

    // 5. Atomic transaction demonstration
    println!("\n[5] Executing atomic level-up transaction for 'ShadowSniper'...");
    db.begin().map_err(|e| format!("Begin error: {}", e))?;
    let update_res = db.exec("UPDATE game_saves SET level = level + 1, score = score + 2500.0 WHERE player_name = 'ShadowSniper';");
    if update_res.is_ok() {
        db.commit().map_err(|e| format!("Commit error: {}", e))?;
        println!("    ✓ Transaction committed successfully.");
    } else {
        let _ = db.rollback();
        println!("    ✗ Transaction rolled back.");
    }

    // 6. Verify updated record
    println!("\n[6] Verifying updated player stats:");
    db.prepare("SELECT player_name, level, score FROM game_saves WHERE player_name = 'ShadowSniper';")
        .map_err(|e| format!("Prepare error: {}", e))?;
    if db.step() == SQLITE_ROW {
        println!("    - Player: {} | New Level: {} | New Score: {:.2}",
            db.column_text(0).unwrap_or_default(),
            db.column_int(1),
            db.column_double(2));
    }
    db.finalize().map_err(|e| format!("Finalize error: {}", e))?;

    // 7. Clean shutdown via Drop
    db.close();
    println!("\n[7] Database closed cleanly.");
    cleanup_db(DB_PATH);

    println!("\n=== Quickstart Completed Successfully ===");
    Ok(())
}
