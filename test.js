'use strict';

const Arkilian = require('./index.js');
const path = require('path');

console.log('Testing Arkilian Node.js bindings...\n');

const dbPath = path.join(__dirname, 'test.db');
const db = new Arkilian(dbPath);

console.log('1. Create table...');
db.exec('CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)');
console.log('   OK');

console.log('2. Insert data with run()...');
db.run('INSERT INTO users (name, email) VALUES (?, ?)', ['Alice', 'alice@example.com']);
db.run('INSERT INTO users (name, email) VALUES (?, ?)', ['Bob', 'bob@example.com']);
console.log('   OK');

console.log('3. Query data with all()...');
const users = db.all('SELECT * FROM users');
console.log(`   Found ${users.length} users:`);
users.forEach(u => console.log(`   - id: ${u.id}, name: ${u.name}, email: ${u.email}`));

console.log('\n4. Query with params...');
const alice = db.all('SELECT * FROM users WHERE name = ?', ['Alice']);
console.log(`   Found ${alice.length} Alice(s)`);

console.log('\n5. Update data...');
db.run('UPDATE users SET email = ? WHERE name = ?', ['bob@newdomain.com', 'Bob']);

console.log('\n6. Verify update...');
const bob = db.all('SELECT * FROM users WHERE name = ?', ['Bob']);
console.log(`   Bob's email: ${bob[0].email}`);

console.log('\n7. Delete data...');
db.run('DELETE FROM users WHERE name = ?', ['Alice']);
const remaining = db.all('SELECT * FROM users');
console.log(`   Remaining users: ${remaining.length}`);

db.close();

console.log('\nAll tests passed!');