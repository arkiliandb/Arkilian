// Arkilian Control Plane — full end-to-end stress test.
//
// Simulates the real backup + hydration pipeline:
//   1. Create users + databases
//   2. Generate real SQLite databases with synthetic data
//   3. Push WAL entries (metadata) to server
//   4. Request signed PUT URLs and actually upload compressed SQL chunks to S3
//   5. Register snapshots (compressed SQLite binary) to S3
//   6. Simulate hydration: fetch hydrate plan, download snapshot+chunks from S3,
//      apply SQL mutations to a fresh local DB, verify data integrity
//   7. Concurrent mixed workload exercising the full pipeline
//
// Usage:
//   bun run server/client.ts [BASE_URL] [--json] [--keep-files]
//
// Env: TARGET_URL, STRESS_USERS, STRESS_DBS, STRESS_WAL_PER_DB,
//      STRESS_SNAPSHOT_INTERVAL, STRESS_CONCURRENCY.

const TARGET = process.argv
  .slice(2)
  .find((a) => a.startsWith("http://") || a.startsWith("https://"))
  || process.env.TARGET_URL
  || "http://localhost:8080";

const JSON_ONLY = process.argv.includes("--json");
const KEEP_FILES = process.argv.includes("--keep-files");
const CONCURRENCY = parseInt(process.env.STRESS_CONCURRENCY || "10", 10);
const USER_COUNT = parseInt(process.env.STRESS_USERS || "3", 10);
const DB_PER_USER = parseInt(process.env.STRESS_DBS || "2", 10);
const WAL_PER_DB = parseInt(process.env.STRESS_WAL_PER_DB || "500", 10);
const SNAPSHOT_INTERVAL = parseInt(process.env.STRESS_SNAPSHOT_INTERVAL || "250", 10);
const ROWS_PER_SNAPSHOT = parseInt(process.env.STRESS_ROWS_PER_SNAPSHOT || "1000", 10);
const WORKDIR = process.env.STRESS_WORKDIR || "/tmp/arkilian-stress";

import { Database } from "bun:sqlite";
import { spawn } from "bun";

// ── types ───────────────────────────────────────────────────────────

interface StressResult {
  label: string;
  total: number;
  ok: number;
  fail: number;
  minMs: number;
  maxMs: number;
  avgMs: number;
  p50Ms: number;
  p95Ms: number;
  p99Ms: number;
  throughputRps: number;
  errors: Record<string, number>;
}

interface LoginResponse {
  token: string;
  user_id: number;
}

interface CreateDBResponse {
  db_id: string;
  api_key: string;
  name: string;
}

interface UploadResponse {
  upload_url: string;
  expires_at: number;
}

interface HydratePlanResponse {
  snapshot_url: string;
  baseline_lsn: number;
  expires_at: number;
  chunks: { url: string; lsn_start: number; lsn_end: number; expires_at: number }[];
}

interface WALEntry {
  ts: number;
  op: number;
  table_id: number;
  pk: number;
  sql: string;
}

// ── timing infra ────────────────────────────────────────────────────

class Times {
  private data: number[] = [];
  push(v: number) { this.data.push(v); }
  pct(n: number): number {
    if (this.data.length === 0) return 0;
    const s = [...this.data].sort((a, b) => a - b);
    return s[Math.ceil((n / 100) * s.length) - 1] ?? s[s.length - 1];
  }
  avg() { return this.data.length ? this.data.reduce((a, b) => a + b, 0) / this.data.length : 0; }
}

interface Flight {
  label: string;
  moments: number[];
  ok: number;
  fail: number;
  t0: number;
  errors: Record<string, number>;
  running: number;
}

const flights = new Map<string, Flight>();

function send(label: string, fn: () => Promise<Response>) {
  let f = flights.get(label);
  if (!f) {
    f = { label, moments: [], ok: 0, fail: 0, t0: performance.now(), errors: {}, running: 0 };
    flights.set(label, f);
  }
  f.running++;
  const t0 = performance.now();
  return fn()
    .then((res) => {
      f!.moments.push(performance.now() - t0);
      if (res.ok) { f!.ok++; return res; }
      f!.fail++;
      const code = `HTTP_${res.status}`;
      f!.errors[code] = (f!.errors[code] || 0) + 1;
      return res;
    })
    .catch((e: any) => {
      f!.fail++;
      const code = e.cause ? String(e.cause.code || "ERR").slice(0, 30) : String(e.message || "ERR").slice(0, 30);
      f!.errors[code] = (f!.errors[code] || 0) + 1;
      return new Response(null, { status: 502 });
    })
    .finally(() => { f!.running--; });
}

function report(label: string): StressResult {
  const f = flights.get(label);
  if (!f) throw new Error(`no flight: ${label}`);
  const elapsed = (performance.now() - f.t0) / 1000;
  const t = new Times();
  for (const v of f.moments) t.push(v);
  return {
    label, total: f.ok + f.fail, ok: f.ok, fail: f.fail,
    minMs: f.moments.length ? Math.min(...f.moments) : 0,
    maxMs: f.moments.length ? Math.max(...f.moments) : 0,
    avgMs: t.avg(), p50Ms: t.pct(50), p95Ms: t.pct(95), p99Ms: t.pct(99),
    throughputRps: elapsed > 0 ? (f.ok + f.fail) / elapsed : 0,
    errors: { ...f.errors },
  };
}

// ── API wrappers ────────────────────────────────────────────────────

function post(path: string, body: unknown, token?: string): Promise<Response> {
  const headers: Record<string, string> = { "Content-Type": "application/json" };
  if (token) headers["Authorization"] = `Bearer ${token}`;
  return fetch(`${TARGET}${path}`, { method: "POST", headers, body: JSON.stringify(body) });
}

function get(path: string, token?: string): Promise<Response> {
  const headers: Record<string, string> = {};
  if (token) headers["Authorization"] = `Bearer ${token}`;
  return fetch(`${TARGET}${path}`, { headers });
}

async function put(url: string, body: BodyInit): Promise<Response> {
  return fetch(url, { method: "PUT", body });
}

// ── zstd helpers (use system binary) ────────────────────────────────

async function zstdCompress(data: Uint8Array): Promise<Uint8Array> {
  const proc = spawn({
    cmd: ["zstd", "-q", "-f", "-"],
    stdin: data,
    stdout: "pipe",
  });
  const out = await new Response(proc.stdout).arrayBuffer();
  await proc.exited;
  return new Uint8Array(out);
}

async function zstdDecompress(data: Uint8Array): Promise<Uint8Array> {
  const proc = spawn({
    cmd: ["zstd", "-q", "-d", "-c"],
    stdin: data,
    stdout: "pipe",
  });
  const out = await new Response(proc.stdout).arrayBuffer();
  await proc.exited;
  return new Uint8Array(out);
}

// ── async pool ──────────────────────────────────────────────────────

function asyncPool(max: number) {
  let running = 0;
  const queue: (() => void)[] = [];
  const pump = () => {
    while (running < max && queue.length) {
      const next = queue.shift()!;
      running++;
      next();
    }
  };
  return {
    add(fn: () => Promise<unknown>) {
      return new Promise<void>((resolve) => {
        queue.push(() => {
          fn().finally(() => { running--; pump(); resolve(); });
        });
        pump();
      });
    },
    done: async () => {
      while (running > 0 || queue.length > 0) {
        await new Promise((r) => setTimeout(r, 10));
      }
    },
  };
}

// ── local SQLite helpers ────────────────────────────────────────────

function makeLocalDB(path: string, rowCount: number): { rows: number; bytes: number } {
  const db = new Database(path);
  db.exec("CREATE TABLE IF NOT EXISTS events (id INTEGER PRIMARY KEY, payload TEXT, ts INTEGER)");
  db.exec("CREATE TABLE IF NOT EXISTS _arkilian_meta (key TEXT PRIMARY KEY, value INTEGER)");
  const insert = db.prepare("INSERT INTO events (id, payload, ts) VALUES (?, ?, ?)");
  const tx = db.transaction(() => {
    for (let i = 0; i < rowCount; i++) {
      insert.run(i, `event_${i}_${Math.random().toString(36).slice(2)}`, Date.now() + i);
    }
  });
  tx();
  const rowCountDb = db.query("SELECT COUNT(*) as c FROM events").get() as { c: number };
  db.close();
  const stat = Bun.file(path).size ?? 0;
  return { rows: rowCountDb.c, bytes: stat };
}

function applySQL(localPath: string, sqlText: string): void {
  const db = new Database(localPath);
  db.exec(sqlText);
  db.close();
}

function getRowCount(path: string): number {
  const db = new Database(path, { readonly: true });
  const r = db.query("SELECT COUNT(*) as c FROM events").get() as { c: number };
  db.close();
  return r.c;
}

// ── main ────────────────────────────────────────────────────────────

interface UserCtx { email: string; password: string; token: string; userID: number; }
interface DBCtx { dbID: string; apiKey: string; name: string; user: UserCtx; }

async function main() {
  await Bun.$`mkdir -p ${WORKDIR}`.quiet();

  log("╔══════════════════════════════════════════════════════════════╗");
  log("║   Arkilian Control Plane — Full E2E Stress Test             ║");
  log("╚══════════════════════════════════════════════════════════════╝");
  log(`target:    ${TARGET}`);
  log(`users:     ${USER_COUNT}  dbs/user: ${DB_PER_USER}  total dbs: ${USER_COUNT * DB_PER_USER}`);
  log(`wal/db:    ${WAL_PER_DB}  snapshot every: ${SNAPSHOT_INTERVAL} entries  rows/snap: ${ROWS_PER_SNAPSHOT}`);
  log(`workdir:   ${WORKDIR}`);
  log(`minio:     ${TARGET.replace(/:9090|:8080/, ":9000")}`);
  log("");

  // ── 1. Auth ─────────────────────────────────────────────────────
  const users = await setupUsers(USER_COUNT);
  log(`✓ registered & logged in ${users.length} users`);

  // ── 2. Create databases ─────────────────────────────────────────
  const dbs = await setupDatabases(users, DB_PER_USER);
  log(`✓ created ${dbs.length} databases`);

  // ── 3. Full backup pipeline: local SQLite → WAL chunks → snapshot
  const backupResults = await runFullBackupPipeline(dbs);
  log(`✓ backup pipeline: ${backupResults.totalChunks} chunks uploaded, ${backupResults.totalSnapshots} snapshots uploaded`);
  log(`  total bytes uploaded to S3: ${(backupResults.totalBytes / 1024 / 1024).toFixed(2)} MB`);

  // ── 4. Hydration: download + apply + verify ─────────────────────
  const hydrationResults = await runHydrationAndVerify(dbs);
  log(`✓ hydrated ${hydrationResults.hydrated} / ${dbs.length} databases correctly`);

  // ── 5. Concurrent mixed workload ────────────────────────────────
  await runMixedWorkload(dbs);

  printReport({ backupResults, hydrationResults });
}

// ── 1. User setup ──────────────────────────────────────────────────

async function setupUsers(n: number): Promise<UserCtx[]> {
  log(`[auth] registering ${n} users + login…`);
  const users: UserCtx[] = [];
  for (let i = 0; i < n; i++) {
    const email = `load_${Date.now()}_${i}_${Math.random().toString(36).slice(2, 6)}@test.arkilian`;
    const password = `pass_${i}_${Math.random().toString(36).slice(2, 8)}`;

    const reg = await send("auth_register", () => post("/v1/auth/register", { email, password }));
    if (!reg.ok) continue;

    const login = await send("auth_login", () => post("/v1/auth/login", { email, password }));
    if (!login.ok) continue;
    const body: LoginResponse = await login.json();
    users.push({ email, password, token: body.token, userID: body.user_id });
  }
  return users;
}

// ── 2. Database setup ──────────────────────────────────────────────

async function setupDatabases(users: UserCtx[], perUser: number): Promise<DBCtx[]> {
  log(`[db] creating ${users.length * perUser} databases…`);
  const dbs: DBCtx[] = [];
  const pool = asyncPool(CONCURRENCY);
  for (const u of users) {
    for (let i = 0; i < perUser; i++) {
      pool.add(async () => {
        const name = `db_${u.userID}_${i}`;
        const res = await send("db_create", () => post("/v1/db/create", { name }, u.token));
        if (res.ok) {
          const body: CreateDBResponse = await res.json();
          dbs.push({ dbID: body.db_id, apiKey: body.api_key, name, user: u });
        }
      });
    }
  }
  await pool.done();

  for (const u of users.slice(0, 5)) {
    await send("db_list", () => get("/v1/db/list", u.token));
  }
  return dbs;
}

// ── 3. Full backup pipeline ────────────────────────────────────────

interface BackupResult {
  totalChunks: number;
  totalSnapshots: number;
  totalBytes: number;
  chunkUploadBytes: number;
  snapshotUploadBytes: number;
  avgChunkMs: number;
  avgSnapshotMs: number;
  avgCompressMs: number;
}

async function runFullBackupPipeline(dbs: DBCtx[]): Promise<BackupResult> {
  log(`[backup] simulating full backup pipeline for ${dbs.length} dbs…`);

  let totalChunks = 0;
  let totalSnapshots = 0;
  let totalBytes = 0;
  let chunkBytes = 0;
  let snapBytes = 0;
  const chunkTimes: number[] = [];
  const snapTimes: number[] = [];
  const compressTimes: number[] = [];

  for (const db of dbs) {
    log(`  → ${db.name} (${db.dbID.slice(0, 16)}…): ${WAL_PER_DB} WAL ops, snapshots every ${SNAPSHOT_INTERVAL}`);

    // 1. Create the initial local SQLite database
    const localPath = `${WORKDIR}/${db.dbID}.local.sqlite`;
    const initialRows = ROWS_PER_SNAPSHOT;
    const init = makeLocalDB(localPath, initialRows);
    const initialFileBytes = await Bun.file(localPath).size;

    // 2. Take initial snapshot: compress + upload + register
    {
      const t0 = performance.now();
      const zst = await zstdCompress(await Bun.file(localPath).arrayBuffer().then(b => new Uint8Array(b)));
      compressTimes.push(performance.now() - t0);

      const upl = await send("upload_request", () =>
        post("/v1/upload/request", {
          db_id: db.dbID,
          event_count: 0,
          lsn_start: 0,
          lsn_end: 0,
        }, db.apiKey)
      );
      if (!upl.ok) { log(`    ✗ upload request failed: HTTP ${upl.status}`); continue; }
      const { upload_url }: UploadResponse = await upl.json();

      const putRes = await send("s3_put_snapshot", () => put(upload_url, zst));
      snapBytes += zst.length;
      totalBytes += zst.length;
      const tPut = performance.now();
      snapTimes.push(tPut - t0);
      if (!putRes.ok) { log(`    ✗ S3 PUT snapshot failed: HTTP ${putRes.status}`); continue; }

      const reg = await send("snapshot_register", () =>
        post("/v1/snapshot/register", {
          baseline_lsn: 0,
          s3_key: upload_url.split("?")[0].split("/").slice(4).join("/"),
        }, db.apiKey)
      );
      if (!reg.ok) { log(`    ✗ snapshot register failed: HTTP ${reg.status}`); continue; }
      totalSnapshots++;
    }

    // 3. Push WAL entries + upload chunks in batches
    const localDb = new Database(localPath);
    const insert = localDb.prepare("INSERT INTO events (id, payload, ts) VALUES (?, ?, ?)");

    let lsn = 1;
    const walEntries: WALEntry[] = [];
    const batchSize = SNAPSHOT_INTERVAL;
    let entriesInBatch = 0;

    for (let i = 0; i < WAL_PER_DB; i++) {
      const id = initialRows + i;
    const sql = `INSERT OR IGNORE INTO events (id, payload, ts) VALUES (${id}, 'wal_${i}_${Math.random().toString(36).slice(2, 6)}', ${Date.now() + i})`;
    walEntries.push({ ts: Date.now() + i, op: 1, table_id: 1, pk: id, sql: sql + ";" });
      insert.run(id, `wal_${i}_${Math.random().toString(36).slice(2, 6)}`, Date.now() + i);
      entriesInBatch++;
      lsn++;

      if (entriesInBatch >= batchSize) {
        // Push WAL metadata to server
        await send("wal_push", () => post("/v1/wal/push", walEntries, db.apiKey));

        // Request signed URL for chunk
        const upl = await send("upload_request", () =>
          post("/v1/upload/request", {
            db_id: db.dbID,
            event_count: walEntries.length,
            lsn_start: lsn - walEntries.length,
            lsn_end: lsn - 1,
          }, db.apiKey)
        );
        if (!upl.ok) { log(`    ✗ chunk upload request failed`); continue; }
        const { upload_url }: UploadResponse = await upl.json();

        // Compress SQL text and upload
        const tComp = performance.now();
        const sqlText = walEntries.map(e => e.sql).join("\n");
        const zst = await zstdCompress(new TextEncoder().encode(sqlText));
        compressTimes.push(performance.now() - tComp);

        const putRes = await send("s3_put_chunk", () => put(upload_url, zst));
        chunkBytes += zst.length;
        totalBytes += zst.length;
        chunkTimes.push(performance.now() - tComp);
        if (!putRes.ok) { log(`    ✗ S3 PUT chunk failed: HTTP ${putRes.status}`); }

        walEntries.length = 0;
        entriesInBatch = 0;
        totalChunks++;
      }
    }

    // Final batch
    if (walEntries.length > 0) {
      await send("wal_push", () => post("/v1/wal/push", walEntries, db.apiKey));
      const upl = await send("upload_request", () =>
        post("/v1/upload/request", {
          db_id: db.dbID,
          event_count: walEntries.length,
          lsn_start: lsn - walEntries.length,
          lsn_end: lsn - 1,
        }, db.apiKey)
      );
      if (upl.ok) {
        const { upload_url }: UploadResponse = await upl.json();
        const tComp = performance.now();
        const sqlText = walEntries.map(e => e.sql).join("\n");
        const zst = await zstdCompress(new TextEncoder().encode(sqlText));
        compressTimes.push(performance.now() - tComp);
        const putRes = await send("s3_put_chunk", () => put(upload_url, zst));
        chunkBytes += zst.length;
        totalBytes += zst.length;
        chunkTimes.push(performance.now() - tComp);
        totalChunks++;
      }
    }

    localDb.close();
    log(`    ✓ ${totalChunks} chunks, ${totalSnapshots} snap, ${(totalBytes / 1024).toFixed(0)} KB`);
  }

  const avg = (a: number[]) => a.length ? a.reduce((x, y) => x + y, 0) / a.length : 0;
  return {
    totalChunks,
    totalSnapshots,
    totalBytes,
    chunkUploadBytes: chunkBytes,
    snapshotUploadBytes: snapBytes,
    avgChunkMs: avg(chunkTimes),
    avgSnapshotMs: avg(snapTimes),
    avgCompressMs: avg(compressTimes),
  };
}

// ── 4. Hydration + integrity check ─────────────────────────────────

interface HydrationResult {
  attempted: number;
  hydrated: number;
  dataIntegrityPassed: number;
  dataIntegrityFailed: number;
  avgHydrateMs: number;
  errors: string[];
}

async function runHydrationAndVerify(dbs: DBCtx[]): Promise<HydrationResult> {
  log(`[hydrate] simulating cold-start hydration for ${dbs.length} dbs…`);

  const result: HydrationResult = {
    attempted: 0, hydrated: 0, dataIntegrityPassed: 0, dataIntegrityFailed: 0,
    avgHydrateMs: 0, errors: [],
  };
  const times: number[] = [];

  for (const db of dbs) {
    result.attempted++;
    const t0 = performance.now();
    try {
      // 1. Get hydrate plan
      const planRes = await send("hydrate_plan", () => get("/v1/hydrate/plan", db.apiKey));
      if (!planRes.ok) {
        result.errors.push(`${db.name}: hydrate_plan HTTP ${planRes.status}`);
        continue;
      }
      const plan: HydratePlanResponse = await planRes.json();

      // 2. Download snapshot from S3
      const snapRes = await send("s3_get_snapshot", () => fetch(plan.snapshot_url));
      if (!snapRes.ok) {
        result.errors.push(`${db.name}: snapshot download HTTP ${snapRes.status}`);
        continue;
      }
      const snapZst = new Uint8Array(await snapRes.arrayBuffer());
      const snapDecompressed = await zstdDecompress(snapZst);

      // 3. Write snapshot to fresh local file
      const restoredPath = `${WORKDIR}/${db.dbID}.restored.sqlite`;
      await Bun.write(restoredPath, snapDecompressed);

      // 4. Download + apply each chunk
      let appliedChunks = 0;
      for (const chunk of plan.chunks) {
        const chunkRes = await send("s3_get_chunk", () => fetch(chunk.url));
        if (!chunkRes.ok) {
          result.errors.push(`${db.name}: chunk ${chunk.lsn_start} HTTP ${chunkRes.status}`);
          continue;
        }
        const chunkZst = new Uint8Array(await chunkRes.arrayBuffer());
        const chunkSql = new TextDecoder().decode(await zstdDecompress(chunkZst));
        applySQL(restoredPath, chunkSql);
        appliedChunks++;
      }

      // 5. Verify data integrity
      const restoredRows = getRowCount(restoredPath);
      // The original has: initialRows + all WAL entries that were applied
      // We can verify row count matches what we expect
      const expectedRows = ROWS_PER_SNAPSHOT + WAL_PER_DB;

      if (restoredRows === expectedRows) {
        result.dataIntegrityPassed++;
        result.hydrated++;
      } else {
        result.dataIntegrityFailed++;
        result.errors.push(`${db.name}: row count mismatch — expected ${expectedRows}, got ${restoredRows}`);
      }

      if (!KEEP_FILES) {
        await Bun.$`rm -f ${restoredPath}`.quiet();
      }
    } catch (e: any) {
      result.errors.push(`${db.name}: ${e.message}`);
    }
    times.push(performance.now() - t0);
  }

  result.avgHydrateMs = times.length ? times.reduce((a, b) => a + b, 0) / times.length : 0;
  return result;
}

// ── 5. Mixed workload ──────────────────────────────────────────────

async function runMixedWorkload(dbs: DBCtx[]) {
  log(`[mixed] concurrent pipeline flood (200 ops)…`);
  const pool = asyncPool(CONCURRENCY);
  const apis = dbs.flatMap(db => [
    () => get("/v1/wal/count", db.apiKey),
    () => get("/v1/hydrate/plan", db.apiKey),
    () => get("/health"),
  ]);
  for (let i = 0; i < 200; i++) {
    const fn = apis[Math.floor(Math.random() * apis.length)];
    pool.add(() => send("mixed", fn));
  }
  await pool.done();
}

// ── reporting ───────────────────────────────────────────────────────

function log(msg: string) {
  if (!JSON_ONLY) console.log(msg);
}

function fmtMs(v: number) { return v.toFixed(1).padStart(7); }
function fmtBytes(b: number) { return b < 1024 ? `${b}B` : b < 1024 * 1024 ? `${(b / 1024).toFixed(1)}KB` : `${(b / 1024 / 1024).toFixed(2)}MB`; }

function printReport(ctx: { backupResults: BackupResult; hydrationResults: HydrationResult }) {
  if (JSON_ONLY) {
    const out: Record<string, StressResult> = {};
    for (const [k] of flights) out[k] = report(k);
    console.log(JSON.stringify({ ...out, ...ctx }, null, 2));
    return;
  }

  console.log("\n" + "═".repeat(80));
  console.log("  Arkilian Control Plane — Full E2E Stress Test Report");
  console.log("═".repeat(80));

  const results: StressResult[] = [];
  for (const [k] of flights) results.push(report(k));

  const totalOk = results.reduce((s, r) => s + r.ok, 0);
  const totalFail = results.reduce((s, r) => s + r.fail, 0);
  const totalReq = totalOk + totalFail;

  console.log(`\n  Summary:  ${totalReq} requests  |  ✅ ${totalOk} ok  |  ❌ ${totalFail} failed  (${totalReq ? ((totalFail / totalReq) * 100).toFixed(2) : "0"}% error rate)`);
  console.log("─".repeat(80));

  console.log(
    `  ${"endpoint".padEnd(22)} ${"total".padStart(7)} ${"ok".padStart(7)} ${"fail".padStart(7)} ${"avg".padStart(7)} ${"p50".padStart(7)} ${"p95".padStart(7)} ${"p99".padStart(7)} ${"rps".padStart(8)}`
  );
  console.log("─".repeat(80));

  for (const r of results) {
    console.log(
      `  ${r.label.padEnd(22)} ${String(r.total).padStart(7)} ${String(r.ok).padStart(7)} ${String(r.fail).padStart(7)} ${fmtMs(r.avgMs)} ${fmtMs(r.p50Ms)} ${fmtMs(r.p95Ms)} ${fmtMs(r.p99Ms)} ${r.throughputRps.toFixed(1).padStart(8)}`
    );
    if (Object.keys(r.errors).length > 0) {
      for (const [err, count] of Object.entries(r.errors)) {
        console.log(`    ↳ ${err}: ${count}`);
      }
    }
  }

  console.log("─".repeat(80));
  console.log("\n  Backup Pipeline:");
  console.log(`    chunks uploaded:     ${ctx.backupResults.totalChunks}  (${fmtBytes(ctx.backupResults.chunkUploadBytes)})`);
  console.log(`    snapshots uploaded:  ${ctx.backupResults.totalSnapshots}  (${fmtBytes(ctx.backupResults.snapshotUploadBytes)})`);
  console.log(`    total to S3:         ${fmtBytes(ctx.backupResults.totalBytes)}`);
  console.log(`    avg compress:        ${ctx.backupResults.avgCompressMs.toFixed(1)}ms`);
  console.log(`    avg chunk upload:    ${ctx.backupResults.avgChunkMs.toFixed(1)}ms`);
  console.log(`    avg snap upload:     ${ctx.backupResults.avgSnapshotMs.toFixed(1)}ms`);

  console.log("\n  Hydration + Integrity:");
  console.log(`    databases hydrated:  ${ctx.hydrationResults.hydrated} / ${ctx.hydrationResults.attempted}`);
  console.log(`    data integrity:      ✅ ${ctx.hydrationResults.dataIntegrityPassed}  ❌ ${ctx.hydrationResults.dataIntegrityFailed}`);
  console.log(`    avg hydrate time:    ${ctx.hydrationResults.avgHydrateMs.toFixed(1)}ms`);

  if (ctx.hydrationResults.errors.length > 0) {
    console.log(`\n  Hydration errors (first 10):`);
    for (const err of ctx.hydrationResults.errors.slice(0, 10)) {
      console.log(`    ✗ ${err}`);
    }
  }

  // Bottlenecks
  const slowest = [...results].sort((a, b) => b.p95Ms - a.p95Ms);
  console.log(`\n  Top 5 slowest (p95):`);
  for (const r of slowest.slice(0, 5)) {
    console.log(`    ${r.label.padEnd(22)} p95=${r.p95Ms.toFixed(1)}ms`);
  }

  // S3 transfer rates
  const s3Puts = results.find(r => r.label === "s3_put_chunk");
  const s3Gets = results.find(r => r.label === "s3_get_chunk");
  if (s3Puts) {
    const avgPutBytes = ctx.backupResults.chunkUploadBytes / Math.max(1, s3Puts.ok);
    const mbps = (avgPutBytes / 1024) / (s3Puts.avgMs / 1000) / 1024;
    console.log(`\n  S3 PUT throughput (chunks): ${mbps.toFixed(2)} MB/s per request, ${fmtBytes(ctx.backupResults.chunkUploadBytes / s3Puts.ok)} avg per chunk`);
  }
  if (s3Gets) {
    console.log(`  S3 GET throughput (chunks): ${s3Gets.avgMs.toFixed(1)}ms per chunk`);
  }

  console.log("\n" + "═".repeat(80) + "\n");
}

// ── run ─────────────────────────────────────────────────────────────

main().catch((e) => {
  console.error("stress test fatal:", e);
  process.exit(1);
});
