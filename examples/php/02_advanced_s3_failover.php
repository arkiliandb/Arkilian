<?php
/**
 * Arkilian PHP SDK - Advanced S3 Streaming & Cold-Start Hydration
 *
 * Demonstrates:
 * 1. Cloud-native WAL streaming to S3/MinIO via Arkilian sidecar engine
 * 2. Real-time telemetry monitoring (health state, bitmask flags, outbox queue depth)
 * 3. Zero-lag WAL flushes via $db->walFlush()
 * 4. Simulating complete primary host loss (local file destruction)
 * 5. Cold-start disaster recovery via Arkilian::hydrateS3()
 * 6. Integrity and row parity validation on the recovered instance
 *
 * Run with:
 *   php -d ffi.enable=1 02_advanced_s3_failover.php
 */

require_once __DIR__ . '/Arkilian.php';

const PRIMARY_DB  = __DIR__ . '/primary_production.sqlite';
const RESTORED_DB = __DIR__ . '/hydrated_recovery.sqlite';

function cleanupDB(string $path): void {
    foreach (['', '-wal', '-shm'] as $ext) {
        $file = $path . $ext;
        if (file_exists($file)) {
            @unlink($file);
        }
    }
}

$s3Config = [
    'endpoint'   => getenv('ARKILIAN_S3_ENDPOINT') ?: 'http://127.0.0.1:9000',
    'bucket'     => getenv('ARKILIAN_S3_BUCKET') ?: 'arkilian-test-bucket',
    'region'     => getenv('ARKILIAN_S3_REGION') ?: 'us-east-1',
    'accessKey'  => getenv('ARKILIAN_S3_ACCESS_KEY') ?: 'minioadmin',
    'secretKey'  => getenv('ARKILIAN_S3_SECRET_KEY') ?: 'minioadmin',
    'prefix'     => getenv('ARKILIAN_S3_PREFIX') ?: 'php-demo',
];

$hmacKey = getenv('ARKILIAN_MANIFEST_HMAC_KEY') ?: 'super-secret-hmac-key-for-manifests';

// Configure environment for the Arkilian C sidecar worker
putenv('ARKILIAN_ENABLE_BACKUP=1');
putenv("ARKILIAN_S3_ENDPOINT={$s3Config['endpoint']}");
putenv("ARKILIAN_S3_BUCKET={$s3Config['bucket']}");
putenv("ARKILIAN_S3_REGION={$s3Config['region']}");
putenv("ARKILIAN_S3_ACCESS_KEY={$s3Config['accessKey']}");
putenv("ARKILIAN_S3_SECRET_KEY={$s3Config['secretKey']}");
putenv("ARKILIAN_S3_PREFIX={$s3Config['prefix']}");
putenv("ARKILIAN_MANIFEST_HMAC_KEY={$hmacKey}");
putenv('ARKILIAN_CHUNK_INTERVAL_SEC=1');
putenv('ARKILIAN_MANIFEST_INTERVAL_SEC=1');

echo "=== Arkilian PHP SDK: Advanced S3 Streaming & Cold-Start Hydration ===\n\n";
echo "[Config] Target S3 Endpoint: {$s3Config['endpoint']}\n";
echo "[Config] Bucket: {$s3Config['bucket']} | Prefix: {$s3Config['prefix']}\n\n";

cleanupDB(PRIMARY_DB);
cleanupDB(RESTORED_DB);

// -------------------------------------------------------------
// Phase 1: Primary Database Workload with Real-Time S3 Streaming
// -------------------------------------------------------------
echo "[Phase 1] Opening Primary Node with S3 WAL Streaming...\n";
$db = new Arkilian(PRIMARY_DB);

// Telemetry verification
printf("  Telemetry -> Sidecar Healthy: %s\n", $db->backupIsHealthy() ? 'true' : 'false');
printf("  Telemetry -> Health Flags: 0x%x\n", $db->backupHealthFlags());
printf("  Telemetry -> Queue Depth: %d\n", $db->backupQueueDepth());

echo "\n[Phase 1] Creating schema and streaming billing events...\n";
$db->exec("
    CREATE TABLE IF NOT EXISTS customer_billing (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        account_id TEXT NOT NULL,
        amount REAL NOT NULL,
        currency TEXT NOT NULL DEFAULT 'USD',
        status TEXT NOT NULL,
        billed_at DATETIME DEFAULT CURRENT_TIMESTAMP
    );
");

$totalRecords = 25;
$insertSql = "INSERT INTO customer_billing (account_id, amount, currency, status) VALUES (?, ?, ?, ?)";

$db->begin();
for ($i = 1; $i <= $totalRecords; $i++) {
    $amt = 19.99 + ($i * 11.50);
    $status = ($i % 3 === 0) ? 'PENDING' : 'PAID';
    $db->run($insertSql, ["acc_cust_" . ($i % 5), round($amt, 2), 'USD', $status]);
}
$db->commit();
echo "  ✓ Successfully committed {$totalRecords} billing records in an atomic transaction.\n";

// Explicit WAL flush
echo "\n[Phase 1] Triggering explicit WAL flush to S3 sidecar...\n";
$db->walFlush();

echo "  Waiting for S3 sidecar outbox to drain...\n";
for ($attempt = 0; $attempt < 20; $attempt++) {
    usleep(500000); // 500ms
    if ($db->backupQueueDepth() === 0) {
        break;
    }
}
// Allow manifest publish to finalize
usleep(1500000); // 1.5s

printf("  Telemetry -> Queue Depth: %d\n", $db->backupQueueDepth());
printf("  Telemetry -> Sidecar Healthy: %s\n", $db->backupIsHealthy() ? 'true' : 'false');

$primaryCount = (int)$db->all("SELECT COUNT(*) as cnt FROM customer_billing")[0]['cnt'];
echo "  Primary DB confirmed record count: {$primaryCount}\n";

echo "[Phase 1] Closing primary database connection.\n";
$db->close();

// -------------------------------------------------------------
// Phase 2: Disaster Simulation
// -------------------------------------------------------------
echo "\n[Phase 2] SIMULATING DISASTER: Destroying primary host!\n";
echo "  Purging local files: " . basename(PRIMARY_DB) . "...\n";
cleanupDB(PRIMARY_DB);
echo "  ✓ Primary local database destroyed. Zero local state remains.\n";

// -------------------------------------------------------------
// Phase 3: Cold-Start Hydration from MinIO S3
// -------------------------------------------------------------
echo "\n[Phase 3] Starting Cold-Start Hydration from MinIO S3...\n";
echo "  Target path: " . basename(RESTORED_DB) . "\n";

try {
    Arkilian::hydrateS3(RESTORED_DB, $s3Config['prefix'], $s3Config);
    echo "  ✓ Hydration completed successfully!\n";
} catch (Throwable $e) {
    fwrite(STDERR, "  ✗ Hydration failed: " . $e->getMessage() . "\n");
    exit(1);
}

// -------------------------------------------------------------
// Phase 4: Data Parity & Integrity Verification
// -------------------------------------------------------------
echo "\n[Phase 4] Opening Hydrated Database to verify integrity...\n";
putenv('ARKILIAN_ENABLE_BACKUP=0');
$recoveredDb = new Arkilian(RESTORED_DB);

$recoveredCount = (int)$recoveredDb->all("SELECT COUNT(*) as cnt FROM customer_billing")[0]['cnt'];
echo "  Recovered DB record count: {$recoveredCount}\n";

if ($recoveredCount !== $primaryCount) {
    fwrite(STDERR, "  ✗ Data mismatch! Expected {$primaryCount}, got {$recoveredCount}\n");
    exit(1);
}

echo "\n  Sample records from recovered instance:\n";
$samples = $recoveredDb->all("SELECT id, account_id, amount, currency, status FROM customer_billing LIMIT 5");
foreach ($samples as $row) {
    printf("    - Bill #%02d | Account: %-12s | %s %.2f | Status: %s\n",
        $row['id'],
        $row['account_id'],
        $row['currency'],
        $row['amount'],
        $row['status']
    );
}

$recoveredDb->close();
cleanupDB(RESTORED_DB);

echo "\n=== Disaster Recovery & Verification Finished Successfully! ===\n";
