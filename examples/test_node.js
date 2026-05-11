// Arkilian — Node.js usage example
// Run: node examples/test_node.js

const { Arkilian } = require('../bindings/node/index.js');

// Open database
const db = new Arkilian('test_node.sqlite');
console.log('✓ Database opened');

// Create table
db.exec('CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)');
console.log('✓ Table created');

// Insert with parameters
const r1 = db.run('INSERT INTO users (name, age) VALUES (?, ?)', 'Alice', 30);
console.log(`✓ Inserted Alice (rowid: ${r1.lastInsertRowid})`);

const r2 = db.run('INSERT INTO users (name, age) VALUES (?, ?)', 'Bob', 25);
console.log(`✓ Inserted Bob (rowid: ${r2.lastInsertRowid})`);

const r3 = db.run('INSERT INTO users (name, age) VALUES (?, ?)', 'Charlie', 35);
console.log(`✓ Inserted Charlie (rowid: ${r3.lastInsertRowid})`);

// Query all rows
const allUsers = db.all('SELECT * FROM users');
console.log('✓ All users:', allUsers);

// Query with parameters
const older = db.all('SELECT name, age FROM users WHERE age > ?', 28);
console.log('✓ Users older than 28:', older);

// Update
const updated = db.run('UPDATE users SET age = ? WHERE name = ?', 31, 'Alice');
console.log(`✓ Updated ${updated.changes} row(s)`);

// Delete
const deleted = db.run('DELETE FROM users WHERE name = ?', 'Charlie');
console.log(`✓ Deleted ${deleted.changes} row(s)`);

// Final state
const final = db.all('SELECT * FROM users ORDER BY id');
console.log('✓ Final state:', final);

// Close
db.close();
console.log('✓ Database closed');

// Cleanup test file
const fs = require('fs');
fs.unlinkSync('test_node.sqlite');
console.log('\n✅ All tests passed!');
