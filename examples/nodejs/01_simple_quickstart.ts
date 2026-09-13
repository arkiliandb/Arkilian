/**
 * Arkilian Node.js / TypeScript SDK - Simple Quickstart Example
 * 
 * Demonstrates:
 * 1. Initializing an Arkilian embedded database
 * 2. Creating tables with idempotent DDL
 * 3. Parameterized inserts and queries with typed interfaces
 * 4. Querying multiple records (all<T>) and single records
 * 5. Atomic ACID transactions
 * 6. Clean resource disposal
 */

import * as fs from 'node:fs';
import Arkilian from 'arkilian';

const DB_PATH = './quickstart.sqlite';

interface User {
  id: number;
  username: string;
  email: string;
  balance: number;
  created_at?: string;
}

interface UserBalance {
  username: string;
  balance: number;
}

function main(): void {
  console.log('=== Arkilian TypeScript SDK: Simple Quickstart ===\n');

  // Clean up any previous test artifact
  if (fs.existsSync(DB_PATH)) {
    fs.unlinkSync(DB_PATH);
  }

  // In embedded-only mode, disable cloud backup warnings
  process.env.ARKILIAN_ENABLE_BACKUP = '0';

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

  // 3. Query records with typed generic all<T>()
  console.log('\n[4] Querying all users (typed User[]):');
  const users: User[] = db.all<User>('SELECT id, username, email, balance FROM users ORDER BY id ASC');
  console.table(users);

  // 4. Query single record
  console.log('[5] Querying single user (username = "alice"):');
  const [alice] = db.all<User>('SELECT * FROM users WHERE username = ? LIMIT 1', ['alice']);
  if (alice) {
    console.log(`    Found user: ID=${alice.id}, Email=${alice.email}, Balance=$${alice.balance.toFixed(2)}`);
  }

  // 5. Atomic transaction demonstration
  console.log('\n[6] Running atomic transfer transaction ($30 from Alice to Bob)...');
  try {
    db.transaction(() => {
      db.run('UPDATE users SET balance = balance - 30.0 WHERE username = ?', ['alice']);
      db.run('UPDATE users SET balance = balance + 30.0 WHERE username = ?', ['bob']);
    });
    console.log('    ✓ Transaction committed successfully.');
  } catch (err: unknown) {
    const error = err as Error;
    console.error('    ✗ Transaction rolled back:', error.message);
  }

  const updatedBalances: UserBalance[] = db.all<UserBalance>(
    'SELECT username, balance FROM users WHERE username IN (?, ?)',
    ['alice', 'bob']
  );
  console.log('    Updated Balances:');
  console.table(updatedBalances);

  // 6. Graceful close
  console.log('[7] Closing database connection...');
  db.close();
  console.log('    ✓ Database closed cleanly.\n');
  console.log('=== Quickstart Completed Successfully ===');
}

main();
