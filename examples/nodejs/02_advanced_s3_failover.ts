/**
 * Arkilian Node.js / TypeScript SDK - Advanced S3 Replication & Disaster Recovery
 * 
 * Demonstrates:
 * 1. Cloud-native WAL streaming to S3/MinIO
 * 2. Live telemetry monitoring: health status, health flags, outbox queue depth
 * 3. Explicit WAL flush to ensure zero lag
 * 4. Simulating complete primary host loss (local DB deletion)
 * 5. Instant cold-start disaster recovery via Arkilian.hydrateS3()
 * 6. Validating data integrity on the recovered instance
 */

import * as fs from 'node:fs';
import Arkilian, { S3Config } from 'arkilian';

interface LedgerEntry {
  entry_id: number;
  account_id: string;
  delta: number;
  note: string;
  timestamp?: string;
}

interface CountResult {
  count: number;
}

const S3_CONFIG: S3Config = {
  endpoint: process.env.ARKILIAN_S3_ENDPOINT || 'http://127.0.0.1:9000',
  bucket: process.env.ARKILIAN_S3_BUCKET || 'arkilian-test-bucket',
  region: process.env.ARKILIAN_S3_REGION || 'us-east-1',
  accessKey: process.env.ARKILIAN_S3_ACCESS_KEY || 'minioadmin',
  secretKey: process.env.ARKILIAN_S3_SECRET_KEY || 'minioadmin',
  prefix: process.env.ARKILIAN_S3_PREFIX || 'nodejs-ts-demo',
};

// Configure environment for the Arkilian C engine & background sidecar
process.env.ARKILIAN_ENABLE_BACKUP = '1';
process.env.ARKILIAN_S3_ENDPOINT = S3_CONFIG.endpoint;
process.env.ARKILIAN_S3_BUCKET = S3_CONFIG.bucket;
process.env.ARKILIAN_S3_REGION = S3_CONFIG.region;
process.env.ARKILIAN_S3_ACCESS_KEY = S3_CONFIG.accessKey;
process.env.ARKILIAN_S3_SECRET_KEY = S3_CONFIG.secretKey;
process.env.ARKILIAN_S3_PREFIX = S3_CONFIG.prefix;
process.env.ARKILIAN_MANIFEST_HMAC_KEY = 'super-secret-hmac-key-for-manifests';
process.env.ARKILIAN_CHUNK_INTERVAL_SEC = '1';
process.env.ARKILIAN_MANIFEST_INTERVAL_SEC = '1';

const PRIMARY_DB = './primary_production.sqlite';
const RESTORED_DB = './hydrated_recovery.sqlite';

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function cleanupFile(filePath: string): void {
  for (const ext of ['', '-wal', '-shm']) {
    const p = filePath + ext;
    if (fs.existsSync(p)) fs.unlinkSync(p);
  }
}

async function main(): Promise<void> {
  console.log('=== Arkilian TypeScript SDK: Advanced S3 Streaming & Cold-Start Hydration ===\n');
  console.log(`[Config] Target S3 Endpoint: ${S3_CONFIG.endpoint}`);
  console.log(`[Config] Bucket: ${S3_CONFIG.bucket} | Prefix: ${S3_CONFIG.prefix}\n`);

  cleanupFile(PRIMARY_DB);
  cleanupFile(RESTORED_DB);

  // -------------------------------------------------------------
  // Phase 1: Primary Database Workload with Real-Time Streaming
  // -------------------------------------------------------------
  console.log('[Phase 1] Initializing Primary Node with S3 WAL Streaming...');
  const db = new Arkilian(PRIMARY_DB);

  // Verify sidecar telemetry
  console.log(`  Telemetry -> Sidecar Healthy: ${db.backupHealthy}`);
  console.log(`  Telemetry -> Health Flags: 0x${db.backupHealthFlags.toString(16)}`);
  console.log(`  Telemetry -> Queue Depth: ${db.backupQueueDepth}`);

  console.log('\n[Phase 1] Creating schema and streaming financial ledger entries...');
  db.exec(`
    CREATE TABLE IF NOT EXISTS ledger (
      entry_id INTEGER PRIMARY KEY AUTOINCREMENT,
      account_id TEXT NOT NULL,
      delta REAL NOT NULL,
      note TEXT NOT NULL,
      timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
    );
  `);

  // Insert 20 transactions
  const totalTransactions = 20;
  const insertStmt = 'INSERT INTO ledger (account_id, delta, note) VALUES (?, ?, ?)';

  db.transaction(() => {
    for (let i = 1; i <= totalTransactions; i++) {
      const delta = (i * 12.5).toFixed(2);
      db.run(insertStmt, [`acc_${i % 5}`, parseFloat(delta), `Transaction batch item #${i}`]);
    }
  });
  console.log(`  ✓ Successfully committed ${totalTransactions} ledger entries.`);

  // Flush WAL chunk to outbox & allow sidecar worker to ship to S3
  console.log('\n[Phase 1] Triggering explicit WAL flush to S3 sidecar...');
  db.walFlush();

  // Poll sidecar queue until it drains and uploads chunk to S3
  console.log('  Waiting for S3 sidecar outbox to drain...');
  for (let attempt = 0; attempt < 20; attempt++) {
    await sleep(500);
    if (db.backupQueueDepth === 0) {
      break;
    }
  }
  // Give manifest publish a brief moment to finish commit
  await sleep(1500);

  console.log(`  Telemetry -> Queue Depth: ${db.backupQueueDepth}`);
  console.log(`  Telemetry -> Sidecar Healthy: ${db.backupHealthy}`);

  // Verify records count
  const [primaryCount] = db.all<CountResult>('SELECT COUNT(*) as count FROM ledger');
  console.log(`  Primary DB confirmed record count: ${primaryCount.count}`);

  console.log('[Phase 1] Closing primary database connection.');
  db.close();

  // -------------------------------------------------------------
  // Phase 2: Disaster Simulation (Host Catastrophe)
  // -------------------------------------------------------------
  console.log('\n[Phase 2] SIMULATING DISASTER: Primary host destroyed!');
  console.log(`  Purging local files: ${PRIMARY_DB}...`);
  cleanupFile(PRIMARY_DB);
  console.log('  ✓ Primary local database destroyed. Zero local state remains.');

  // -------------------------------------------------------------
  // Phase 3: Cold-Start Hydration from S3
  // -------------------------------------------------------------
  console.log('\n[Phase 3] Starting Cold-Start Hydration from MinIO S3...');
  console.log(`  Target path: ${RESTORED_DB}`);

  try {
    Arkilian.hydrateS3(RESTORED_DB, S3_CONFIG);
    console.log('  ✓ Hydration completed successfully!');
  } catch (err) {
    console.error('  ✗ Hydration failed:', err);
    process.exit(1);
  }

  // -------------------------------------------------------------
  // Phase 4: Data Parity & Integrity Verification
  // -------------------------------------------------------------
  console.log('\n[Phase 4] Opening Hydrated Database to verify integrity...');
  process.env.ARKILIAN_ENABLE_BACKUP = '0';
  const recoveredDb = new Arkilian(RESTORED_DB);

  const [recoveredCount] = recoveredDb.all<CountResult>('SELECT COUNT(*) as count FROM ledger');
  console.log(`  Recovered DB record count: ${recoveredCount.count}`);

  if (recoveredCount.count !== primaryCount.count) {
    console.error(`  ✗ Data mismatch! Expected ${primaryCount.count}, got ${recoveredCount.count}`);
    process.exit(1);
  }

  console.log('\n  Sample records from recovered instance:');
  const sampleRows = recoveredDb.all<LedgerEntry>(
    'SELECT entry_id, account_id, delta, note FROM ledger LIMIT 5'
  );
  console.table(sampleRows);

  recoveredDb.close();
  cleanupFile(RESTORED_DB);
  console.log('\n=== Disaster Recovery & Verification Finished Successfully! ===');
}

main().catch((err) => {
  console.error('Fatal execution error:', err);
  process.exit(1);
});
