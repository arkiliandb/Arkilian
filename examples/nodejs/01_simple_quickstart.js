/**
 * Arkilian Node.js SDK - Simple Quickstart Example
 * 
 * Demonstrates:
 * 1. Opening an Arkilian embedded database
 * 2. Creating tables (DDL)
 * 3. Inserting records with parameterized queries
 * 4. Querying multiple records (all) and single records (get)
 * 5. Atomic transactions
 * 6. Clean resource shutdown
 */

const fs = require('fs');
const Arkilian = require('arkilian').default || require('arkilian');

const DB_PATH = './quickstart.sqlite';

function main() {
    console.log('=== Arkilian Node.js SDK: Simple Quickstart ===\n');

    // Clean up any previous test artifact
    if (fs.existsSync(DB_PATH)) {
        fs.unlinkSync(DB_PATH);
    }

    console.log(`[1] Initializing Arkilian database at '${DB_PATH}'...`);
    const db = new Arkilian(DB_PATH);

    // 1. Create table
    console.log('[2] Creating schema...');
    db.exec(`
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT NOT NULL UNIQUE,
            email TEXT NOT NULL,
            balance REAL DEFAULT 0.0,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        );
    `);
    console.log('    ✓ Table "users" created successfully.');

    // 2. Insert records using parameterized queries
    console.log('[3] Inserting sample records...');
    const insertStmt = 'INSERT INTO users (username, email, balance) VALUES (?, ?, ?)';
    db.run(insertStmt, ['alice', 'alice@arkilian.dev', 150.50]);
    db.run(insertStmt, ['bob', 'bob@arkilian.dev', 80.00]);
    db.run(insertStmt, ['charlie', 'charlie@arkilian.dev', 220.00]);
    console.log('    ✓ 3 users inserted.');

    // 3. Query records
    console.log('\n[4] Querying all users:');
    const users = db.all('SELECT id, username, email, balance FROM users ORDER BY id ASC');
    console.table(users);

    // 4. Query single record
    console.log('[5] Querying single user (username = "alice"):');
    const [alice] = db.all('SELECT * FROM users WHERE username = ? LIMIT 1', ['alice']);
    console.log(`    Found user: ID=${alice.id}, Email=${alice.email}, Balance=$${alice.balance}`);

    // 5. Atomic transaction demonstration
    console.log('\n[6] Running atomic transfer transaction ($30 from Alice to Bob)...');
    try {
        db.transaction(() => {
            db.run('UPDATE users SET balance = balance - 30.0 WHERE username = ?', ['alice']);
            db.run('UPDATE users SET balance = balance + 30.0 WHERE username = ?', ['bob']);
        });
        console.log('    ✓ Transaction committed successfully.');
    } catch (err) {
        console.error('    ✗ Transaction rolled back:', err.message);
    }

    const updatedBalances = db.all('SELECT username, balance FROM users WHERE username IN (?, ?)', ['alice', 'bob']);
    console.log('    Updated Balances:');
    console.table(updatedBalances);

    // 6. Graceful close
    console.log('[7] Closing database connection...');
    db.close();
    console.log('    ✓ Database closed cleanly.\n');
    console.log('=== Quickstart Completed Successfully ===');
}

main();
