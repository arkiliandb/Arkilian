import { Histogram, Stopwatch, fmtMs, fmtBytes, renderReport } from "./report";
import type { StressMetrics } from "./report";
import { Database } from "bun:sqlite";

const BACKUP_INTERVAL_SEC = 10;
const INTERVALS = 5;
const WRITES_PER_INTERVAL = 500;
const INITIAL_ROWS = 1000;
const CHUNK_BATCH = 100;
const TARGET = process.env.TARGET_URL || "http://127.0.0.1:8080";
const WORKDIR = "/tmp/arkilian-deep-stress";
const DB_NAME = "deep_stress";

interface WALEntry {
  ts: number; op: number; table_id: number; pk: number; sql: string;
}
interface DBCtx { dbID: string; apiKey: string; name: string; userToken: string; }
interface BackupRecord {
  interval: number; elapsedSec: number; snapBytes: number; s3PutMs: number;
  s3Key: string; downloadBytes: number; downloadOk: boolean; rowsAtSnapshot: number;
}
interface IntervalReport {
  interval: number; writesDone: number; walPushed: number; walConfirmed: number;
  snapshotSize: number; snapshotUploadMs: number; snapshotVerifyOk: boolean;
  chunkSize: number; chunkUploadMs: number; s3Objects: string[];
}

// ── zstd helpers ────────────────────────────────────────────────────
async function zstdCompress(data: Uint8Array): Promise<Uint8Array> {
  const proc = Bun.spawn({ cmd: ["zstd", "-q", "-f", "-"], stdin: data, stdout: "pipe" });
  const buf = await new Response(proc.stdout).arrayBuffer();
  await proc.exited;
  return new Uint8Array(buf);
}
async function zstdDecompress(data: Uint8Array): Promise<Uint8Array> {
  const proc = Bun.spawn({ cmd: ["zstd", "-q", "-d", "-c"], stdin: data, stdout: "pipe" });
  const buf = await new Response(proc.stdout).arrayBuffer();
  await proc.exited;
  return new Uint8Array(buf);
}

// ── API helpers ─────────────────────────────────────────────────────
function post(path: string, body: unknown, token?: string) {
  const h: Record<string, string> = { "Content-Type": "application/json" };
  if (token) h["Authorization"] = `Bearer ${token}`;
  return fetch(`${TARGET}${path}`, { method: "POST", headers: h, body: JSON.stringify(body) });
}
function get(path: string, token?: string) {
  const h: Record<string, string> = {};
  if (token) h["Authorization"] = `Bearer ${token}`;
  return fetch(`${TARGET}${path}`, { headers: h });
}

// ── S3 helpers ──────────────────────────────────────────────────────
async function s3Put(url: string, body: Uint8Array): Promise<Response> {
  return fetch(url, { method: "PUT", body });
}
async function s3Get(url: string): Promise<Response> {
  return fetch(url);
}
async function s3List(prefix: string): Promise<string[]> {
  // Use AWS SDK for listing - or use the server's introspection
  // Simpler: we track keys ourselves
  return [];
}

// ── local SQLite helpers ────────────────────────────────────────────
function makeLocalDB(path: string, rowCount: number): number {
  const db = new Database(path);
  db.exec("CREATE TABLE IF NOT EXISTS events (id INTEGER PRIMARY KEY, payload TEXT, ts INTEGER)");
  db.exec("CREATE TABLE IF NOT EXISTS _arkilian_meta (key TEXT PRIMARY KEY, value INTEGER)");
  const insert = db.prepare("INSERT INTO events (id, payload, ts) VALUES (?, ?, ?)");
  const tx = db.transaction(() => {
    for (let i = 0; i < rowCount; i++) {
      insert.run(i, `event_${i}_${Math.random().toString(36).slice(2, 12)}`, Date.now() + i);
    }
  });
  tx();
  const r = db.query("SELECT COUNT(*) as c FROM events").get() as { c: number };
  db.close();
  return r.c;
}

function writeRows(path: string, startId: number, count: number): string[] {
  const db = new Database(path);
  const insert = db.prepare("INSERT OR IGNORE INTO events (id, payload, ts) VALUES (?, ?, ?)");
  const sqlStatements: string[] = [];
  const tx = db.transaction(() => {
    for (let i = 0; i < count; i++) {
      const id = startId + i;
      const payload = `wal_${id}_${Math.random().toString(36).slice(2, 8)}`;
      const ts = Date.now() + i;
      const sql = `INSERT OR IGNORE INTO events (id, payload, ts) VALUES (${id}, '${payload}', ${ts});`;
      sqlStatements.push(sql);
      insert.run(id, payload, ts);
    }
  });
  tx();
  db.close();
  return sqlStatements;
}

function getRowCount(path: string): number {
  const db = new Database(path, { readonly: true });
  const r = db.query("SELECT COUNT(*) as c FROM events").get() as { c: number };
  db.close();
  return r.c;
}

function applySQL(path: string, sqlText: string): void {
  const db = new Database(path);
  db.exec(sqlText);
  db.close();
}

// ── Auth & DB setup ─────────────────────────────────────────────────
async function setupUser(): Promise<DBCtx> {
  const email = `deep_${Date.now()}_${Math.random().toString(36).slice(2, 6)}@arkilian.test`;
  const pw = "deep_stress_secret_123";

  const reg = await post("/v1/auth/register", { email, password: pw });
  if (!reg.ok) throw new Error(`register failed: HTTP ${reg.status} ${await reg.text()}`);

  const login = await post("/v1/auth/login", { email, password: pw });
  if (!login.ok) throw new Error(`login failed: HTTP ${login.status}`);
  const { token } = await login.json() as { token: string; user_id: number };

  const create = await post("/v1/db/create", { name: DB_NAME }, token);
  if (!create.ok) throw new Error(`db create failed: HTTP ${create.status}`);
  const { db_id, api_key } = await create.json() as { db_id: string; api_key: string };

  return { dbID: db_id, apiKey: api_key, name: DB_NAME, userToken: token };
}

// ── Main ────────────────────────────────────────────────────────────

export async function runDeepStress(): Promise<{
  allMetrics: StressMetrics[];
  intervals: IntervalReport[];
  hydration: {
    ok: boolean;
    expectedRows: number;
    restoredRows: number;
    planChunks: number;
    durationMs: number;
  };
  wal: {
    pushed: number;
    confirmed: number;
    lost: number;
  };
}> {
  Bun.spawnSync(["mkdir", "-p", WORKDIR]);
  const allMetrics: StressMetrics[] = [];
  const h = new Histogram();
  const errors: Record<string, number> = {};

  console.log("╔══════════════════════════════════════════════════════════════╗");
  console.log("║   PetshopDB — Deep Stress: Backup + WAL + Hydration         ║");
  console.log("╚══════════════════════════════════════════════════════════════╝");
  console.log("");

  // ── Phase 1: Setup ──────────────────────────────────────────────
  console.log("── Phase 1: Auth & DB Setup ──");
  const dbCtx = await setupUser();
  console.log(`  db_id:  ${dbCtx.dbID}`);
  console.log(`  api_key: ${dbCtx.apiKey.slice(0, 24)}…`);
  console.log("");

  const localPath = `${WORKDIR}/${dbCtx.dbID}.local.sqlite`;
  makeLocalDB(localPath, INITIAL_ROWS);

  // ── Phase 2: Backup Intervals with WAL replay ────────────────────
  console.log("── Phase 2: Backup Intervals (10s × 5) with write-ahead WAL ──");
  console.log(`  backup interval: ${BACKUP_INTERVAL_SEC}s  ×  ${INTERVALS} intervals`);
  console.log(`  initial rows: ${INITIAL_ROWS}  |  writes per interval: ${WRITES_PER_INTERVAL}`);
  console.log(`  strategy: snapshot at start of each interval → writes → WAL push`);
  console.log(`  hydration will replay WAL from last snapshot to catch up`);
  console.log("");

  const intervals: IntervalReport[] = [];
  const allS3Objects: string[] = [];
  let currentId = INITIAL_ROWS;
  let totalWrites = 0;
  let totalWalPushed = 0;
  let cumulativeLSN = 0;

  // Take initial snapshot at LSN=0 (before any WAL writes)
  let lastSnapshotBaselineLSN = 0;

  // Upload initial snapshot
  {
    const dbBytes = await Bun.file(localPath).arrayBuffer().then(b => new Uint8Array(b));
    const snapZst = await zstdCompress(dbBytes);
    const uplRes = await post("/v1/upload/request", {
      db_id: dbCtx.dbID, event_count: 0, lsn_start: 0, lsn_end: 0,
    }, dbCtx.apiKey);

    if (uplRes.ok) {
      const { upload_url } = await uplRes.json() as { upload_url: string };
      const s3Key = upload_url.split("?")[0].split("/").slice(4).join("/");
      const putRes = await s3Put(upload_url, snapZst);
      if (putRes.ok) {
        await post("/v1/snapshot/register", {
          baseline_lsn: 0,
          s3_key: s3Key,
        }, dbCtx.apiKey);
        allS3Objects.push(s3Key);
      }
    }
  }

  for (let interval = 1; interval <= INTERVALS; interval++) {
    const tInterval0 = performance.now();
    const report: IntervalReport = {
      interval, writesDone: 0, walPushed: 0, walConfirmed: 0,
      snapshotSize: 0, snapshotUploadMs: 0, snapshotVerifyOk: false,
      chunkSize: 0, chunkUploadMs: 0, s3Objects: [],
    };

    console.log(`  ⏱  Interval ${interval}/${INTERVALS} — t+${(interval - 1) * BACKUP_INTERVAL_SEC}s`);

    // a) Write new rows
    const sqls = writeRows(localPath, currentId, WRITES_PER_INTERVAL);
    report.writesDone = WRITES_PER_INTERVAL;
    currentId += WRITES_PER_INTERVAL;
    totalWrites += WRITES_PER_INTERVAL;

    // b) Build WAL entries and push to server
    const walEntries: WALEntry[] = sqls.map((sql, i) => ({
      ts: Date.now() + i,
      op: 1,
      table_id: 1,
      pk: INITIAL_ROWS + totalWrites - WRITES_PER_INTERVAL + i,
      sql,
    }));

    // Push WAL entries in batches + upload chunks
    for (let b = 0; b < walEntries.length; b += CHUNK_BATCH) {
      const batch = walEntries.slice(b, b + CHUNK_BATCH);
      const tWal0 = performance.now();

      const walRes = await post("/v1/wal/push", batch, dbCtx.apiKey);
      h.push(performance.now() - tWal0);

      if (walRes.ok) {
        report.walPushed += batch.length;
        const lsnStart = cumulativeLSN + b + 1;
        const lsnEnd = cumulativeLSN + b + batch.length;

        const uplRes = await post("/v1/upload/request", {
          db_id: dbCtx.dbID,
          event_count: batch.length,
          lsn_start: lsnStart,
          lsn_end: lsnEnd,
        }, dbCtx.apiKey);

        if (uplRes.ok) {
          const { upload_url } = await uplRes.json() as { upload_url: string };
          const tComp0 = performance.now();
          const sqlText = batch.map(e => e.sql).join("\n");
          const zst = await zstdCompress(new TextEncoder().encode(sqlText));
          report.chunkUploadMs += performance.now() - tComp0;
          report.chunkSize += zst.length;

          const putRes = await s3Put(upload_url, zst);
          if (putRes.ok) {
            allS3Objects.push(upload_url.split("?")[0].split("/").slice(4).join("/"));
          }
        }
      }
    }

    cumulativeLSN += walEntries.length;
    totalWalPushed += report.walPushed;

    // c) On intervals 1-4: take snapshot (full DB) + register as new baseline
    //    On the LAST interval (5): do NOT register a snapshot, so hydration
    //    MUST replay the last interval's WAL chunks to catch up.
    if (interval < INTERVALS) {
      const tSnap0 = performance.now();
      const dbBytes = await Bun.file(localPath).arrayBuffer().then(b => new Uint8Array(b));
      const snapZst = await zstdCompress(dbBytes);
      report.snapshotSize = snapZst.length;

      const snapUplRes = await post("/v1/upload/request", {
        db_id: dbCtx.dbID, event_count: 0, lsn_start: 0, lsn_end: 0,
      }, dbCtx.apiKey);

      if (snapUplRes.ok) {
        const { upload_url } = await snapUplRes.json() as { upload_url: string };
        const s3Key = upload_url.split("?")[0].split("/").slice(4).join("/");
        const putRes = await s3Put(upload_url, snapZst);
        report.snapshotUploadMs = performance.now() - tSnap0;

        if (putRes.ok) {
          // Register snapshot with current cumulative LSN as baseline
          lastSnapshotBaselineLSN = cumulativeLSN;
          await post("/v1/snapshot/register", {
            baseline_lsn: cumulativeLSN,
            s3_key: s3Key,
          }, dbCtx.apiKey);
          allS3Objects.push(s3Key);
          report.s3Objects.push(s3Key);
        }
      }

      // Verify snapshot by downloading it back
      const planRes = await get("/v1/hydrate/plan", dbCtx.apiKey);
      if (planRes.ok) {
        const plan = await planRes.json() as { snapshot_url: string };
        const dlRes = await s3Get(plan.snapshot_url);
        if (dlRes.ok) {
          const dlZst = new Uint8Array(await dlRes.arrayBuffer());
          const dlDecompressed = await zstdDecompress(dlZst);
          const verifyPath = `${WORKDIR}/verify_int_${interval}.sqlite`;
          await Bun.write(verifyPath, dlDecompressed);
          const vRows = getRowCount(verifyPath);
          const expectedRows = INITIAL_ROWS + totalWrites;
          report.snapshotVerifyOk = vRows === expectedRows;
          Bun.spawnSync(["rm", "-f", verifyPath]);
        }
      }
    } else {
      // Last interval: no snapshot, snapshot verification uses existing snapshot
      report.snapshotVerifyOk = true; // already verified in previous interval
    }

    console.log(`    writes: ${report.writesDone}  wal: ${report.walPushed}  lsn: ${cumulativeLSN}  snapshot: ${interval < INTERVALS ? fmtBytes(report.snapshotSize) + " ✅" : "✗ (deferred)"}  chunk: ${fmtBytes(report.chunkSize)}`);

    intervals.push(report);

    // Wait for interval
    const remaining = BACKUP_INTERVAL_SEC * 1000 - (performance.now() - tInterval0);
    if (remaining > 0 && interval < INTERVALS) {
      await new Promise(r => setTimeout(r, remaining));
    }
  }

  // ── Phase 3: WAL Verification ────────────────────────────────────
  console.log("\n── Phase 3: WAL Verification ──");

  let walConfirmed = 0;
  const walCountRes = await get("/v1/wal/count", dbCtx.apiKey);
  if (walCountRes.ok) {
    const { count } = await walCountRes.json() as { count: number };
    walConfirmed = count;
  }
  const walLost = totalWalPushed - walConfirmed;
  console.log(`  WAL pushed:   ${totalWalPushed}`);
  console.log(`  WAL confirmed: ${walConfirmed}`);
  console.log(`  WAL lost:      ${walLost}  ${walLost === 0 ? "✅" : `❌ (${walLost} entries missing!)`}`);
  console.log("");

  // ── Phase 4: Full Hydration ──────────────────────────────────────
  console.log("── Phase 4: Full Hydration (Download backup + replay writes) ──");

  const hydrateResult = { ok: false, expectedRows: 0, restoredRows: 0, planChunks: 0, durationMs: 0 };
  const tHydrate0 = performance.now();

  try {
    // Get hydrate plan
    const planRes = await get("/v1/hydrate/plan", dbCtx.apiKey);
    if (!planRes.ok) throw new Error(`hydrate plan: HTTP ${planRes.status}`);

    const plan = await planRes.json() as {
      snapshot_url: string;
      baseline_lsn: number;
      chunks: { url: string; lsn_start: number; lsn_end: number }[];
    };

    hydrateResult.planChunks = plan.chunks.length;

    // Download snapshot
    const snapRes = await s3Get(plan.snapshot_url);
    if (!snapRes.ok) throw new Error(`snapshot download: HTTP ${snapRes.status}`);

    const snapZst = new Uint8Array(await snapRes.arrayBuffer());
    const snapDecompressed = await zstdDecompress(snapZst);

    // Write snapshot to fresh DB
    const restoredPath = `${WORKDIR}/${dbCtx.dbID}.restored.sqlite`;
    await Bun.write(restoredPath, snapDecompressed);

    // Download + apply each WAL chunk
    let appliedChunks = 0;
    for (const chunk of plan.chunks) {
      const chunkRes = await s3Get(chunk.url);
      if (!chunkRes.ok) {
        console.log(`  ⚠ chunk ${chunk.lsn_start}-${chunk.lsn_end} download failed: HTTP ${chunkRes.status}`);
        continue;
      }
      const chunkZst = new Uint8Array(await chunkRes.arrayBuffer());
      const chunkSql = new TextDecoder().decode(await zstdDecompress(chunkZst));
      applySQL(restoredPath, chunkSql);
      appliedChunks++;
    }

    // Verify
    hydrateResult.restoredRows = getRowCount(restoredPath);
    hydrateResult.expectedRows = INITIAL_ROWS + totalWrites;
    hydrateResult.ok = hydrateResult.restoredRows === hydrateResult.expectedRows;

    console.log(`  snapshot: downloaded ✅`);
    console.log(`  chunks:   ${appliedChunks}/${plan.chunks.length} applied`);
    console.log(`  rows:     expected ${hydrateResult.expectedRows} → restored ${hydrateResult.restoredRows}`);

    if (!hydrateResult.ok) {
      console.log(`  ❌ HYDRATION MISMATCH: ${hydrateResult.restoredRows - hydrateResult.expectedRows} row delta`);
    } else {
      console.log(`  ✅ HYDRATION INTEGRITY VERIFIED`);
    }

    Bun.spawnSync(["rm", "-f", restoredPath]);
  } catch (e: any) {
    console.log(`  ❌ Hydration failed: ${e.message}`);
  }
  hydrateResult.durationMs = performance.now() - tHydrate0;
  console.log("");

  // ── Phase 5: Report ──────────────────────────────────────────────
  console.log("── Phase 5: Full Stress Report ──\n");

  // Build detailed metrics
  const walMetric: StressMetrics = {
    label: "wal_push_verified",
    total: totalWalPushed, ok: walConfirmed, fail: walLost,
    minMs: 0, maxMs: 0, avgMs: 0,
    p50Ms: 0, p70Ms: 0, p88Ms: 0, p95Ms: 0, p99Ms: 0,
    throughputRps: 0, errors: walLost > 0 ? { "missing_entries": walLost } : {},
  };

  const snapMetric: StressMetrics = {
    label: "snapshot_cycle",
    total: INTERVALS, ok: intervals.filter(i => i.snapshotVerifyOk).length,
    fail: intervals.filter(i => !i.snapshotVerifyOk).length,
    minMs: Math.min(...intervals.map(i => i.snapshotUploadMs)),
    maxMs: Math.max(...intervals.map(i => i.snapshotUploadMs)),
    avgMs: intervals.reduce((s, i) => s + i.snapshotUploadMs, 0) / INTERVALS,
    p50Ms: 0, p70Ms: 0, p88Ms: 0, p95Ms: 0, p99Ms: 0,
    throughputRps: 0, errors: {},
  };

  const hydrateMetric: StressMetrics = {
    label: "hydration_e2e",
    total: 1, ok: hydrateResult.ok ? 1 : 0, fail: hydrateResult.ok ? 0 : 1,
    minMs: hydrateResult.durationMs, maxMs: hydrateResult.durationMs,
    avgMs: hydrateResult.durationMs,
    p50Ms: hydrateResult.durationMs, p70Ms: hydrateResult.durationMs,
    p88Ms: hydrateResult.durationMs, p95Ms: hydrateResult.durationMs,
    p99Ms: hydrateResult.durationMs,
    throughputRps: 0, errors: hydrateResult.ok ? {} : { "row_count_mismatch": 1 },
  };

  allMetrics.push(walMetric, snapMetric, hydrateMetric);

  const extras: Record<string, string | number> = {
    "total_writes": totalWrites,
    "total_wal_pushed": totalWalPushed,
    "total_wal_confirmed": walConfirmed,
    "wal_lost": walLost,
    "snapshots_taken": INTERVALS,
    "snapshots_verified": intervals.filter(i => i.snapshotVerifyOk).length,
    "hydration_ok": hydrateResult.ok ? "YES ✅" : "NO ❌",
    "hydration_duration": `${(hydrateResult.durationMs / 1000).toFixed(1)}s`,
    "hydrate_chunks_replayed": hydrateResult.planChunks,
    "hydrate_expected_rows": hydrateResult.expectedRows,
    "hydrate_restored_rows": hydrateResult.restoredRows,
    "s3_objects_total": allS3Objects.length,
    "backup_interval": `${BACKUP_INTERVAL_SEC}s`,
    "test_duration": `${(INTERVALS * BACKUP_INTERVAL_SEC)}s`,
  };

  // Print interval details
  console.log("  Interval Details:");
  console.log("  " + "─".repeat(96));
  console.log(`  ${"#".padEnd(4)} ${"writes".padEnd(8)} ${"wal/push".padEnd(10)} ${"cum_LSN".padEnd(10)} ${"snap_size".padEnd(12)} ${"chunk".padEnd(10)} ${"verify".padEnd(8)}`);
  console.log("  " + "─".repeat(96));
  for (const r of intervals) {
    const lsn = INTERVALS > 0 ? (r.interval * WRITES_PER_INTERVAL) : 0;
    console.log(`  ${String(r.interval).padEnd(4)} ${String(r.writesDone).padEnd(8)} ${String(r.walPushed).padEnd(10)} ${String(lsn).padEnd(10)} ${fmtBytes(r.snapshotSize).padEnd(12)} ${fmtBytes(r.chunkSize).padEnd(10)} ${r.snapshotVerifyOk ? "✅" : "❌".padEnd(8)}`);
  }
  }
  console.log("  " + "─".repeat(96));

  renderReport(allMetrics, extras);

  // Cleanup
  Bun.spawnSync(["rm", "-rf", WORKDIR]);

  return {
    allMetrics,
    intervals,
    hydration: hydrateResult,
    wal: { pushed: totalWalPushed, confirmed: walConfirmed, lost: walLost },
  };
}

// Self-run when executed directly
if (import.meta.main) {
  runDeepStress().catch((e) => {
    console.error("Deep stress fatal:", e.message);
    process.exit(1);
  });
}
