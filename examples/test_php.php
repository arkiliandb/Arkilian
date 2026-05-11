<?php
/**
 * Arkilian — PHP integration test
 * Run: php examples/test_php.php
 */

require_once __DIR__ . '/../bindings/php/src/Arkilian.php';

use Arkilian\Arkilian;

$dbFile = 'test_php.sqlite';

// Open database
$db = new Arkilian($dbFile);
echo "✓ Database opened\n";

// Create table
$db->exec('CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)');
echo "✓ Table created\n";

// Insert with parameters
$r1 = $db->run('INSERT INTO users (name, age) VALUES (?, ?)', 'Alice', 30);
echo "✓ Inserted Alice (rowid: {$r1['lastInsertRowid']})\n";

$r2 = $db->run('INSERT INTO users (name, age) VALUES (?, ?)', 'Bob', 25);
echo "✓ Inserted Bob (rowid: {$r2['lastInsertRowid']})\n";

$r3 = $db->run('INSERT INTO users (name, age) VALUES (?, ?)', 'Charlie', 35);
echo "✓ Inserted Charlie (rowid: {$r3['lastInsertRowid']})\n";

// Query all rows
$allUsers = $db->all('SELECT * FROM users');
echo "✓ All users:\n";
foreach ($allUsers as $user) {
    echo "    id={$user['id']}, name={$user['name']}, age={$user['age']}\n";
}

// Query with parameters
$older = $db->all('SELECT name, age FROM users WHERE age > ?', 28);
echo "✓ Users older than 28:\n";
foreach ($older as $user) {
    echo "    name={$user['name']}, age={$user['age']}\n";
}

// Update
$updated = $db->run('UPDATE users SET age = ? WHERE name = ?', 31, 'Alice');
echo "✓ Updated {$updated['changes']} row(s)\n";

// Delete
$deleted = $db->run('DELETE FROM users WHERE name = ?', 'Charlie');
echo "✓ Deleted {$deleted['changes']} row(s)\n";

// Final state
$final = $db->all('SELECT * FROM users ORDER BY id');
echo "✓ Final state:\n";
foreach ($final as $user) {
    echo "    id={$user['id']}, name={$user['name']}, age={$user['age']}\n";
}

// Close
$db->close();
echo "✓ Database closed\n";

// Cleanup
unlink($dbFile);
echo "\n✅ All PHP tests passed!\n";
