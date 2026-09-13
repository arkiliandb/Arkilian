<?php
/**
 * Arkilian PHP SDK - Simple Quickstart Example
 *
 * Demonstrates:
 * 1. Initializing an Arkilian embedded database
 * 2. Creating tables (DDL)
 * 3. Parameterized inserts and queries
 * 4. Fetching structured result arrays
 * 5. Atomic transactions (begin / commit / rollback)
 * 6. Clean database resource closure
 *
 * Run with:
 *   php -d ffi.enable=1 01_simple_quickstart.php
 */

require_once __DIR__ . '/Arkilian.php';

const DB_PATH = __DIR__ . '/quickstart.sqlite';

function cleanupDB(string $path): void {
    foreach (['', '-wal', '-shm'] as $ext) {
        $file = $path . $ext;
        if (file_exists($file)) {
            @unlink($file);
        }
    }
}

echo "=== Arkilian PHP SDK: Simple Quickstart ===\n\n";

cleanupDB(DB_PATH);

// Disable sidecar destination warning for local-only quickstart
putenv('ARKILIAN_ENABLE_BACKUP=0');

// 1. Initialize embedded database
echo "[1] Initializing Arkilian database at '" . basename(DB_PATH) . "'...\n";
$db = new Arkilian(DB_PATH);

// 2. Create schema
echo "[2] Creating schema...\n";
$db->exec("
    CREATE TABLE IF NOT EXISTS articles (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        title TEXT NOT NULL,
        author TEXT NOT NULL,
        views INTEGER NOT NULL DEFAULT 0,
        published_at DATETIME DEFAULT CURRENT_TIMESTAMP
    );
");
echo "    ✓ Table 'articles' created successfully.\n";

// 3. Parameterized inserts
echo "[3] Inserting sample articles...\n";
$insertSql = "INSERT INTO articles (title, author, views) VALUES (?, ?, ?)";

$articles = [
    ["Building Resilient APIs with Arkilian", "DevAdvocate", 1450],
    ["Continuous Database Replication to S3", "CloudArchitect", 3200],
    ["Zero Data-Loss Disaster Recovery Patterns", "SiteReliability", 2890],
];

foreach ($articles as $row) {
    $db->run($insertSql, $row);
}
echo "    ✓ " . count($articles) . " articles inserted.\n";

// 4. Query records
echo "\n[4] Querying all articles:\n";
$rows = $db->all("SELECT id, title, author, views FROM articles ORDER BY views DESC");
foreach ($rows as $article) {
    printf("    - ID %d | %-44s | Author: %-15s | Views: %d\n",
        $article['id'],
        $article['title'],
        $article['author'],
        $article['views']
    );
}

// 5. Atomic transaction demonstration
echo "\n[5] Executing atomic views-increment transaction...\n";
try {
    $db->begin();
    $db->run("UPDATE articles SET views = views + 500 WHERE author = ?", ["DevAdvocate"]);
    $db->commit();
    echo "    ✓ Transaction committed successfully.\n";
} catch (Throwable $e) {
    $db->rollback();
    echo "    ✗ Transaction rolled back: " . $e->getMessage() . "\n";
}

// 6. Verify updated view count
echo "\n[6] Verifying updated views:\n";
$updated = $db->all("SELECT title, views FROM articles WHERE author = ?", ["DevAdvocate"]);
foreach ($updated as $item) {
    printf("    - %s: %d views\n", $item['title'], $item['views']);
}

// 7. Clean closure
echo "\n[7] Closing database connection...\n";
$db->close();
cleanupDB(DB_PATH);
echo "    ✓ Database closed cleanly.\n";

echo "\n=== Quickstart Completed Successfully ===\n";
