<?php
require_once __DIR__ . '/Arkilian.php';

$testDbPath = sys_get_temp_dir() . '/test_arkilian_' . uniqid() . '.sqlite';
if (file_exists($testDbPath)) {
    unlink($testDbPath);
}

echo "=== Arkilian PHP Bindings 1:1 Parity Test ===\n";

try {
    // 1. Open Database
    $db = new Arkilian($testDbPath);
    echo "✓ Database opened successfully\n";

    // 2. Handle
    $handle = $db->getHandle();
    if ($handle === null) {
        throw new Exception("Expected non-null SQLite handle");
    }
    echo "✓ getHandle() returned valid handle\n";

    // 3. DDL & Exec
    $db->exec("CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, bio BLOB, score INT, balance REAL)");
    echo "✓ Table created\n";

    // 4. Transactions & Changes
    $db->begin();
    $db->exec("INSERT INTO users (name, score, balance) VALUES ('Alice', 100, 50.5)");
    if ($db->changes() !== 1) {
        throw new Exception("Expected 1 change, got: " . $db->changes());
    }
    $aliceId = $db->lastInsertRowId();
    if ($aliceId !== 1) {
        throw new Exception("Expected last insert rowid 1, got: " . $aliceId);
    }
    $db->commit();
    echo "✓ Transaction committed: Alice id={$aliceId}\n";

    // Rollback test
    $db->begin();
    $db->exec("INSERT INTO users (name, score) VALUES ('RollbackMe', 999)");
    $db->rollback();
    $check = $db->all("SELECT * FROM users WHERE name = 'RollbackMe'");
    if (count($check) !== 0) {
        throw new Exception("Expected 0 rows after rollback, got: " . count($check));
    }
    echo "✓ Transaction rollback verified\n";

    // 5. Prepared statement with typed BLOB, int64, float, null
    $db->prepare("INSERT INTO users (name, bio, score, balance) VALUES (?, ?, ?, ?)");
    $db->bindText(1, "Bob");
    $blobData = "\x00\xFF\xFE\x42\x00\x01BinaryData";
    $db->bindBlob(2, $blobData);
    $db->bindInt64(3, 9876543210123);
    $db->bindDouble(4, 123.456);
    $rc = $db->step();
    if ($rc !== SQLITE_DONE) {
        throw new Exception("Expected SQLITE_DONE, got: " . $rc);
    }
    $db->finalize();
    echo "✓ Prepared statement with typed BLOB/int64/double executed\n";

    // 6. Query with typed columns
    $db->prepare("SELECT name, bio, score, balance FROM users WHERE name = 'Bob'");
    if ($db->step() !== SQLITE_ROW) {
        throw new Exception("Expected row for Bob");
    }
    if ($db->columnType(0) !== SQLITE_TEXT) {
        throw new Exception("Expected SQLITE_TEXT for name");
    }
    if ($db->columnType(1) !== SQLITE_BLOB) {
        throw new Exception("Expected SQLITE_BLOB for bio");
    }
    $retrievedBlob = $db->columnBlob(1);
    if ($retrievedBlob !== $blobData) {
        throw new Exception("BLOB mismatch: length got " . strlen($retrievedBlob) . ", expected " . strlen($blobData));
    }
    if ($db->columnInt64(2) !== 9876543210123) {
        throw new Exception("Int64 mismatch: got " . $db->columnInt64(2));
    }
    $val = $db->columnValue(1);
    if ($val !== $blobData) {
        throw new Exception("columnValue BLOB mismatch");
    }
    $db->finalize();
    echo "✓ Typed column access (columnType, columnBlob, columnInt64, columnValue) verified\n";

    // 7. WAL methods
    $walPending = $db->walPending();
    echo "✓ WAL pending count: {$walPending}\n";
    $db->walFlush();
    $lastSql = $db->walLastSql();
    echo "✓ WAL last SQL: " . ($lastSql ?? 'none') . "\n";

    // 8. Backup and Trigger controls
    $db->setBackupEnabled(true);
    if (!$db->isBackupEnabled()) {
        throw new Exception("Expected backup to be enabled");
    }
    $db->setAutoResyncTriggers(true);
    if (!$db->getAutoResyncTriggers()) {
        throw new Exception("Expected auto resync triggers true");
    }
    $db->resyncTriggers();
    $dirty = $db->backupTriggersDirty();
    $paused = $db->backupCapturePaused();
    echo "✓ Backup and trigger controls verified (dirty=" . ($dirty ? '1' : '0') . ", paused=" . ($paused ? '1' : '0') . ")\n";

    // 9. Monitoring & Health Flags
    $queueDepth = $db->backupQueueDepth();
    $flags = $db->backupHealthFlags();
    $healthy = $db->backupIsHealthy();
    echo "✓ Backup health flags: 0x" . dechex($flags) . " (queue depth={$queueDepth}, healthy=" . ($healthy ? 'true' : 'false') . ")\n";
    if (ARK_HF_ALL_CORE !== 0x3FF) {
        throw new Exception("Expected ARK_HF_ALL_CORE === 0x3FF");
    }

    // 10. High level run / all
    $db->run("INSERT INTO users (name, score) VALUES (?, ?)", ['Charlie', 777]);
    $all = $db->all("SELECT id, name, score FROM users ORDER BY id");
    if (count($all) !== 3) {
        throw new Exception("Expected 3 users, got: " . count($all));
    }
    echo "✓ all() returned " . count($all) . " rows\n";

    // Close
    $db->close();
    echo "✓ Database closed successfully\n";

    echo "\n🎉 ALL PHP BINDING TESTS PASSED!\n";
} finally {
    if (file_exists($testDbPath)) {
        unlink($testDbPath);
    }
}
