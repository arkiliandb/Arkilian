import Arkilian from "../index.js";
import { Database } from "bun:sqlite";
import { Histogram, Stopwatch, fmtBytes } from "./report";
import type { StressMetrics } from "./report";

interface ClientStressOpts {
  rowCount: number;
  concurrency: number;
  batchSize: number;
  workdir: string;
  walMode: boolean;
}

const DEFAULTS: ClientStressOpts = {
  rowCount: 50_000,
  concurrency: 8,
  batchSize: 500,
  workdir: "/tmp/arkilian-stress-client",
  walMode: true,
};

export async function runClientStress(opts: Partial<ClientStressOpts> = {}): Promise<{
  metrics: StressMetrics[];
}> {
  const cfg = { ...DEFAULTS, ...opts };
  Bun.spawnSync(["mkdir", "-p", cfg.workdir]);

  const metrics: StressMetrics[] = [];

  // ── 1. Arkilian native — sequential inserts ──────────────────────
  metrics.push(await testArkilianSequential(cfg));
  // ── 2. Arkilian native — concurrent read/write ───────────────────
  metrics.push(await testArkilianConcurrent(cfg));
  // ── 3. Arkilian native — prepared statement reuse ────────────────
  metrics.push(await testArkilianPrepared(cfg));
  // ── 4. Arkilian native — bulk transactions ───────────────────────
  metrics.push(await testArkilianBulkTx(cfg));
  // ── 5. Arkilian native — mixed queries ───────────────────────────
  metrics.push(await testArkilianMixedQueries(cfg));
  // ── 6. Connection churn (open/close) ─────────────────────────────
  metrics.push(await testConnectionChurn(cfg));
  // ── 7. Large payload ─────────────────────────────────────────────
  metrics.push(await testLargePayload(cfg));
  // ── 8. Multi-database concurrent ─────────────────────────────────
  metrics.push(await testMultiDatabase(cfg));
  // ── 9. WAL mode comparison ───────────────────────────────────────
  metrics.push(await testWALMode(cfg));
  // ── 10. Memory pressure ──────────────────────────────────────────
  metrics.push(await testMemoryPressure(cfg));
  // ── 11. Arkilian vs bun:sqlite insert throughput ─────────────────
  metrics.push(await testCompareThroughput(cfg));

  return { metrics };
}

// ── Helpers ────────────────────────────────────────────────────────

function openArk(path: string): Arkilian {
  return new Arkilian("petshop-stress-token", path);
}

async function closeArk(db: Arkilian): Promise<void> {
  try { await db.close(); } catch {}
}

// ── 1. Arkilian sequential inserts ─────────────────────────────────

async function testArkilianSequential(cfg: ClientStressOpts): Promise<StressMetrics> {
  const path = `${cfg.workdir}/ark_seq.sqlite`;
  const h = new Histogram();
  let ok = 0, fail = 0;
  const errors: Record<string, number> = {};
  let bytes = 0;
  const sw = new Stopwatch();

  const db = openArk(path);
  await db.exec("CREATE TABLE IF NOT EXISTS stress_test (id INTEGER PRIMARY KEY, val TEXT, ts INTEGER)");

  for (let i = 0; i < cfg.rowCount; i++) {
    const t0 = performance.now();
    try {
      await db.run("INSERT INTO stress_test (id, val, ts) VALUES (?, ?, ?)",
        [i, `val_${i}_${Math.random().toString(36).slice(2, 8)}`, Date.now() + i]);
      h.push(performance.now() - t0);
      ok++;
      bytes += 64;
    } catch (e: any) {
      fail++;
      errors[e.message?.slice(0, 30) || "ERR"] = (errors[e.message?.slice(0, 30) || "ERR"] || 0) + 1;
    }
  }

  const elapsed = sw.elapsed() / 1000;
  await closeArk(db);
  Bun.spawnSync(["rm", "-f", path]);

  return {
    label: "ark_seq_inserts",
    total: ok + fail, ok, fail,
    minMs: h.min(), maxMs: h.max(), avgMs: h.avg(),
    p50Ms: h.pct(50), p95Ms: h.pct(95), p99Ms: h.pct(99),
    throughputRps: elapsed > 0 ? (ok + fail) / elapsed : 0,
    errors, bytesProcessed: bytes,
  };
}

// ── 2. Arkilian concurrent read/write ──────────────────────────────

async function testArkilianConcurrent(cfg: ClientStressOpts): Promise<StressMetrics> {
  const path = `${cfg.workdir}/ark_concurrent.sqlite`;
  const h = new Histogram();
  let ok = 0, fail = 0;
  const errors: Record<string, number> = {};

  {
    const db = openArk(path);
    await db.exec("CREATE TABLE IF NOT EXISTS items (id INTEGER PRIMARY KEY, name TEXT, score REAL)");
    for (let i = 0; i < 10_000; i++) {
      await db.run("INSERT OR IGNORE INTO items (id, name, score) VALUES (?, ?, ?)",
        [i, `item_${i}`, Math.random() * 100]);
    }
    await closeArk(db);
  }

  const workers: Promise<void>[] = [];
  const opsPerWorker = Math.ceil(cfg.rowCount / cfg.concurrency);
  const mu = { ok: 0, fail: 0 };

  for (let w = 0; w < cfg.concurrency; w++) {
    workers.push((async () => {
      for (let i = 0; i < opsPerWorker; i++) {
        const t0 = performance.now();
        try {
          const db = openArk(path);
          const op = (w * opsPerWorker + i) % 3;
          if (op === 0) {
            const r = await db.all("SELECT * FROM items WHERE id = ?", [i % 10000]);
            void r;
          } else if (op === 1) {
            const uid = 100000 + w * opsPerWorker + i;
            await db.exec(`INSERT OR IGNORE INTO items (id, name, score) VALUES (${uid}, 'concurrent_${i}', ${Math.random() * 100})`);
          } else {
            const r = await db.all("SELECT COUNT(*) as c FROM items WHERE score > ?", [50]);
            void r;
          }
          h.push(performance.now() - t0);
          mu.ok++;
          await closeArk(db);
        } catch (e: any) {
          mu.fail++;
          errors[e.message?.slice(0, 30) || "ERR"] = (errors[e.message?.slice(0, 30) || "ERR"] || 0) + 1;
        }
      }
    })());
  }

  const sw = new Stopwatch();
  await Promise.all(workers);
  const elapsed = sw.elapsed() / 1000;
  ok = mu.ok; fail = mu.fail;

  Bun.spawnSync(["rm", "-f", path]);

  return {
    label: "ark_concurrent_rw",
    total: ok + fail, ok, fail,
    minMs: h.min(), maxMs: h.max(), avgMs: h.avg(),
    p50Ms: h.pct(50), p95Ms: h.pct(95), p99Ms: h.pct(99),
    throughputRps: elapsed > 0 ? (ok + fail) / elapsed : 0,
    errors,
  };
}

// ── 3. Arkilian prepared statement reuse ───────────────────────────

async function testArkilianPrepared(cfg: ClientStressOpts): Promise<StressMetrics> {
  const path = `${cfg.workdir}/ark_prepared.sqlite`;
  const h = new Histogram();
  let ok = 0, fail = 0;
  const errors: Record<string, number> = {};
  const sw = new Stopwatch();

  const db = openArk(path);
  await db.exec("CREATE TABLE IF NOT EXISTS prep_test (id INTEGER PRIMARY KEY, a INT, b INT, c TEXT)");

  const cycles = Math.min(cfg.rowCount, 10_000);

  for (let i = 0; i < cycles; i++) {
    const t0 = performance.now();
    try {
      await db.run("INSERT INTO prep_test (id, a, b, c) VALUES (?, ?, ?, ?)",
        [i, i % 100, i * 2, `prep_${i}`]);
      h.push(performance.now() - t0);
      ok++;
    } catch (e: any) {
      fail++;
      errors[e.message?.slice(0, 30) || "ERR"] = (errors[e.message?.slice(0, 30) || "ERR"] || 0) + 1;
    }
  }

  for (let i = 0; i < 1000; i++) {
    try {
      const t0 = performance.now();
      const r = await db.all("SELECT * FROM prep_test WHERE a = ?", [i % 100]);
      h.push(performance.now() - t0);
      ok++;
      void r;
    } catch { fail++; }
  }

  for (let i = 0; i < 500; i++) {
    try {
      const t0 = performance.now();
      await db.run("UPDATE prep_test SET b = ? WHERE id = ?", [i * 3, i]);
      h.push(performance.now() - t0);
      ok++;
    } catch { fail++; }
  }

  for (let i = 0; i < 500; i++) {
    try {
      const t0 = performance.now();
      await db.run("DELETE FROM prep_test WHERE id = ?", [i]);
      h.push(performance.now() - t0);
      ok++;
    } catch { fail++; }
  }

  const elapsed = sw.elapsed() / 1000;
  await closeArk(db);
  Bun.spawnSync(["rm", "-f", path]);

  return {
    label: "ark_prepared_stmts",
    total: ok + fail, ok, fail,
    minMs: h.min(), maxMs: h.max(), avgMs: h.avg(),
    p50Ms: h.pct(50), p95Ms: h.pct(95), p99Ms: h.pct(99),
    throughputRps: elapsed > 0 ? (ok + fail) / elapsed : 0,
    errors,
  };
}

// ── 4. Arkilian bulk transactions ──────────────────────────────────

async function testArkilianBulkTx(cfg: ClientStressOpts): Promise<StressMetrics> {
  const path = `${cfg.workdir}/ark_bulk.sqlite`;
  const h = new Histogram();
  let ok = 0, fail = 0;
  const errors: Record<string, number> = {};
  let bytes = 0;
  const sw = new Stopwatch();

  const db = openArk(path);
  await db.exec("CREATE TABLE IF NOT EXISTS bulk_test (id INTEGER PRIMARY KEY, val TEXT)");

  const batches = Math.ceil(cfg.rowCount / cfg.batchSize);

  for (let b = 0; b < batches; b++) {
    const t0 = performance.now();
    try {
      await db.exec("BEGIN");
      for (let i = 0; i < cfg.batchSize; i++) {
        const id = b * cfg.batchSize + i;
        await db.run("INSERT INTO bulk_test (id, val) VALUES (?, ?)",
          [id, `bulk_${id}_${Math.random().toString(36).slice(2)}`]);
      }
      await db.exec("COMMIT");
      h.push(performance.now() - t0);
      ok += cfg.batchSize;
      bytes += cfg.batchSize * 48;
    } catch (e: any) {
      await db.exec("ROLLBACK").catch(() => {});
      fail += cfg.batchSize;
      errors[e.message?.slice(0, 30) || "ERR"] = (errors[e.message?.slice(0, 30) || "ERR"] || 0) + 1;
    }
  }

  const elapsed = sw.elapsed() / 1000;
  await closeArk(db);
  Bun.spawnSync(["rm", "-f", path]);

  return {
    label: "ark_bulk_tx",
    total: ok + fail, ok, fail,
    minMs: h.min(), maxMs: h.max(), avgMs: h.avg(),
    p50Ms: h.pct(50), p95Ms: h.pct(95), p99Ms: h.pct(99),
    throughputRps: elapsed > 0 ? (ok + fail) / elapsed : 0,
    errors, bytesProcessed: bytes,
  };
}

// ── 5. Arkilian mixed queries ──────────────────────────────────────

async function testArkilianMixedQueries(cfg: ClientStressOpts): Promise<StressMetrics> {
  const path = `${cfg.workdir}/ark_queries.sqlite`;
  const h = new Histogram();
  let ok = 0, fail = 0;
  const errors: Record<string, number> = {};
  const sw = new Stopwatch();

  const db = openArk(path);
  await db.exec("CREATE TABLE IF NOT EXISTS orders (id INTEGER PRIMARY KEY, user_id INT, total REAL, created_at INT)");
  await db.exec("CREATE TABLE IF NOT EXISTS users_t (id INTEGER PRIMARY KEY, name TEXT, email TEXT)");

  for (let i = 0; i < 5000; i++) {
    await db.run("INSERT INTO orders (id, user_id, total, created_at) VALUES (?, ?, ?, ?)",
      [i, i % 100, Math.random() * 1000, Date.now() + i]);
    await db.run("INSERT INTO users_t (id, name, email) VALUES (?, ?, ?)",
      [i, `user_${i}`, `user_${i}@test.com`]);
  }

  const queries = [
    `SELECT COUNT(*) as c FROM orders WHERE total > 500`,
    `SELECT user_id, COUNT(*) as cnt, AVG(total) as avg_total FROM orders GROUP BY user_id`,
    `SELECT u.name, o.total FROM users_t u JOIN orders o ON u.id = o.user_id WHERE o.total > 800 ORDER BY o.total DESC LIMIT 50`,
    `SELECT * FROM orders WHERE total = (SELECT MAX(total) FROM orders)`,
    `SELECT u.id, u.name, COALESCE(SUM(o.total), 0) as lifetime FROM users_t u LEFT JOIN orders o ON u.id = o.user_id GROUP BY u.id ORDER BY lifetime DESC LIMIT 20`,
    `SELECT total FROM orders WHERE total > (SELECT AVG(total) * 1.5 FROM orders)`,
  ];

  for (let iter = 0; iter < 20; iter++) {
    for (const q of queries) {
      const t0 = performance.now();
      try {
        const r = await db.all(q);
        h.push(performance.now() - t0);
        ok++;
        void r;
      } catch (e: any) {
        fail++;
        errors[e.message?.slice(0, 30) || "ERR"] = (errors[e.message?.slice(0, 30) || "ERR"] || 0) + 1;
      }
    }
  }

  const elapsed = sw.elapsed() / 1000;
  await closeArk(db);
  Bun.spawnSync(["rm", "-f", path]);

  return {
    label: "ark_mixed_queries",
    total: ok + fail, ok, fail,
    minMs: h.min(), maxMs: h.max(), avgMs: h.avg(),
    p50Ms: h.pct(50), p95Ms: h.pct(95), p99Ms: h.pct(99),
    throughputRps: elapsed > 0 ? (ok + fail) / elapsed : 0,
    errors,
  };
}

// ── 6. Connection churn ────────────────────────────────────────────

async function testConnectionChurn(cfg: ClientStressOpts): Promise<StressMetrics> {
  const h = new Histogram();
  let ok = 0, fail = 0;
  const errors: Record<string, number> = {};
  const cycles = Math.min(cfg.rowCount, 2000);
  const sw = new Stopwatch();

  for (let i = 0; i < cycles; i++) {
    const path = `${cfg.workdir}/churn_${i}.sqlite`;
    const t0 = performance.now();
    try {
      const db = openArk(path);
      await db.exec("CREATE TABLE IF NOT EXISTS t (id INT PRIMARY KEY, val TEXT)");
      await db.run("INSERT INTO t (id, val) VALUES (?, ?)", [1, "hello"]);
      const r = await db.all("SELECT val FROM t WHERE id = ?", [1]);
      h.push(performance.now() - t0);
      ok++;
      void r;
      await closeArk(db);
    } catch (e: any) {
      fail++;
      errors[e.message?.slice(0, 30) || "ERR"] = (errors[e.message?.slice(0, 30) || "ERR"] || 0) + 1;
    }
    Bun.spawnSync(["rm", "-f", path]);
  }

  const elapsed = sw.elapsed() / 1000;

  return {
    label: "connection_churn",
    total: ok + fail, ok, fail,
    minMs: h.min(), maxMs: h.max(), avgMs: h.avg(),
    p50Ms: h.pct(50), p95Ms: h.pct(95), p99Ms: h.pct(99),
    throughputRps: elapsed > 0 ? (ok + fail) / elapsed : 0,
    errors,
  };
}

// ── 7. Large payload ───────────────────────────────────────────────

async function testLargePayload(cfg: ClientStressOpts): Promise<StressMetrics> {
  const path = `${cfg.workdir}/ark_large.sqlite`;
  const h = new Histogram();
  let ok = 0, fail = 0;
  const errors: Record<string, number> = {};
  let bytes = 0;
  const sw = new Stopwatch();

  const db = openArk(path);
  await db.exec("CREATE TABLE IF NOT EXISTS large_test (id INTEGER PRIMARY KEY, blob_data TEXT)");

  const sizes = [1024, 4096, 16384, 65536];
  for (let iter = 0; iter < 10; iter++) {
    for (const size of sizes) {
      const t0 = performance.now();
      try {
        const payload = "x".repeat(size);
        await db.run("INSERT INTO large_test (id, blob_data) VALUES (?, ?)",
          [iter * sizes.length + sizes.indexOf(size), payload]);
        h.push(performance.now() - t0);
        ok++;
        bytes += size;
      } catch (e: any) {
        fail++;
        errors[e.message?.slice(0, 30) || "ERR"] = (errors[e.message?.slice(0, 30) || "ERR"] || 0) + 1;
      }
    }
  }

  const elapsed = sw.elapsed() / 1000;
  await closeArk(db);
  Bun.spawnSync(["rm", "-f", path]);

  return {
    label: "large_payload",
    total: ok + fail, ok, fail,
    minMs: h.min(), maxMs: h.max(), avgMs: h.avg(),
    p50Ms: h.pct(50), p95Ms: h.pct(95), p99Ms: h.pct(99),
    throughputRps: elapsed > 0 ? (ok + fail) / elapsed : 0,
    errors, bytesProcessed: bytes,
  };
}

// ── 8. Multi-database concurrent ───────────────────────────────────

async function testMultiDatabase(cfg: ClientStressOpts): Promise<StressMetrics> {
  const h = new Histogram();
  let ok = 0, fail = 0;
  const errors: Record<string, number> = {};
  let bytes = 0;
  const dbCount = Math.min(cfg.concurrency, 8);
  const opsPerDb = Math.min(cfg.rowCount, 2000);
  const sw = new Stopwatch();
  const mu = { ok: 0, fail: 0 };
  const workers: Promise<void>[] = [];

  for (let d = 0; d < dbCount; d++) {
    workers.push((async () => {
      const path = `${cfg.workdir}/ark_multi_${d}.sqlite`;
      try {
        const db = openArk(path);
        await db.exec("CREATE TABLE IF NOT EXISTS data (id INTEGER PRIMARY KEY, val TEXT)");
        for (let i = 0; i < opsPerDb; i++) {
          const t0 = performance.now();
          await db.run("INSERT INTO data (id, val) VALUES (?, ?)", [i, `multi_db_${d}_${i}`]);
          h.push(performance.now() - t0);
          mu.ok++;
          bytes += 32;
        }
        await closeArk(db);
      } catch (e: any) {
        mu.fail += opsPerDb;
        errors[e.message?.slice(0, 30) || "ERR"] = (errors[e.message?.slice(0, 30) || "ERR"] || 0) + 1;
      }
      Bun.spawnSync(["rm", "-f", path]);
    })());
  }

  await Promise.all(workers);
  const elapsed = sw.elapsed() / 1000;
  ok = mu.ok; fail = mu.fail;

  return {
    label: "multi_database",
    total: ok + fail, ok, fail,
    minMs: h.min(), maxMs: h.max(), avgMs: h.avg(),
    p50Ms: h.pct(50), p95Ms: h.pct(95), p99Ms: h.pct(99),
    throughputRps: elapsed > 0 ? (ok + fail) / elapsed : 0,
    errors, bytesProcessed: bytes,
  };
}

// ── 9. WAL mode comparison ─────────────────────────────────────────

async function testWALMode(cfg: ClientStressOpts): Promise<StressMetrics> {
  const modes = ["DELETE", "WAL", "MEMORY"] as const;
  const h = new Histogram();
  let ok = 0, fail = 0;
  const errors: Record<string, number> = {};
  let bytes = 0;
  const count = Math.min(cfg.rowCount, 5000);
  const sw = new Stopwatch();

  for (const mode of modes) {
    const path = `${cfg.workdir}/wal_${mode.toLowerCase()}.sqlite`;
    const db = openArk(path);
    await db.exec(`PRAGMA journal_mode=${mode}`);
    await db.exec("CREATE TABLE IF NOT EXISTS wal_test (id INTEGER PRIMARY KEY, val TEXT, ts INTEGER)");

    for (let i = 0; i < count; i++) {
      const t0 = performance.now();
      try {
        await db.run("INSERT INTO wal_test (id, val, ts) VALUES (?, ?, ?)",
          [i, `wal_mode_${i}`, Date.now() + i]);
        h.push(performance.now() - t0);
        ok++;
        bytes += 32;
      } catch (e: any) {
        fail++;
        errors[e.message?.slice(0, 30) || "ERR"] = (errors[e.message?.slice(0, 30) || "ERR"] || 0) + 1;
      }
    }
    await closeArk(db);
    Bun.spawnSync(["rm", "-f", path]);
  }

  const elapsed = sw.elapsed() / 1000;

  return {
    label: "wal_mode_compare",
    total: ok + fail, ok, fail,
    minMs: h.min(), maxMs: h.max(), avgMs: h.avg(),
    p50Ms: h.pct(50), p95Ms: h.pct(95), p99Ms: h.pct(99),
    throughputRps: elapsed > 0 ? (ok + fail) / elapsed : 0,
    errors, bytesProcessed: bytes,
  };
}

// ── 10. Memory pressure ────────────────────────────────────────────

async function testMemoryPressure(cfg: ClientStressOpts): Promise<StressMetrics> {
  const path = `${cfg.workdir}/ark_mem.sqlite`;
  const h = new Histogram();
  let ok = 0, fail = 0;
  const errors: Record<string, number> = {};
  const sw = new Stopwatch();

  const db = openArk(path);
  await db.exec("PRAGMA cache_size=-2000");
  await db.exec("CREATE TABLE IF NOT EXISTS mem_test (id INTEGER PRIMARY KEY, val TEXT)");

  const count = Math.min(cfg.rowCount, 15_000);
  for (let i = 0; i < count; i++) {
    const t0 = performance.now();
    try {
      await db.run("INSERT INTO mem_test (id, val) VALUES (?, ?)", [i, "x".repeat(512)]);
      h.push(performance.now() - t0);
      ok++;
    } catch (e: any) {
      fail++;
      errors[e.message?.slice(0, 30) || "ERR"] = (errors[e.message?.slice(0, 30) || "ERR"] || 0) + 1;
    }
  }

  const elapsed = sw.elapsed() / 1000;
  await closeArk(db);
  Bun.spawnSync(["rm", "-f", path]);

  return {
    label: "memory_pressure",
    total: ok + fail, ok, fail,
    minMs: h.min(), maxMs: h.max(), avgMs: h.avg(),
    p50Ms: h.pct(50), p95Ms: h.pct(95), p99Ms: h.pct(99),
    throughputRps: elapsed > 0 ? (ok + fail) / elapsed : 0,
    errors, bytesProcessed: ok * 512,
  };
}

// ── 11. Arkilian vs bun:sqlite throughput comparison ────────────────

async function testCompareThroughput(cfg: ClientStressOpts): Promise<StressMetrics> {
  const count = Math.min(cfg.rowCount, 20_000);
  const h = new Histogram();
  let ok = 0, fail = 0;
  const errors: Record<string, number> = {};
  let bytes = 0;
  const sw = new Stopwatch();

  // Phase 1: bun:sqlite inserts
  {
    const path = `${cfg.workdir}/compare_bun.sqlite`;
    const db = new Database(path);
    db.exec("PRAGMA journal_mode=WAL");
    db.exec("CREATE TABLE IF NOT EXISTS compare_test (id INTEGER PRIMARY KEY, val TEXT, ts INTEGER)");
    const insert = db.prepare("INSERT INTO compare_test (id, val, ts) VALUES (?, ?, ?)");
    for (let i = 0; i < count; i++) {
      const t0 = performance.now();
      try {
        insert.run(i, `bun_${i}`, Date.now() + i);
        h.push(performance.now() - t0);
        ok++;
        bytes += 32;
      } catch (e: any) {
        fail++;
        errors[e.message?.slice(0, 30) || "ERR"] = (errors[e.message?.slice(0, 30) || "ERR"] || 0) + 1;
      }
    }
    insert.finalize();
    db.close();
    Bun.spawnSync(["rm", "-f", path]);
  }

  // Phase 2: Arkilian inserts (same count)
  {
    const path = `${cfg.workdir}/compare_ark.sqlite`;
    const db = openArk(path);
    await db.exec("CREATE TABLE IF NOT EXISTS compare_test (id INTEGER PRIMARY KEY, val TEXT, ts INTEGER)");
    for (let i = 0; i < count; i++) {
      const t0 = performance.now();
      try {
        await db.run("INSERT INTO compare_test (id, val, ts) VALUES (?, ?, ?)",
          [i, `ark_${i}`, Date.now() + i]);
        h.push(performance.now() - t0);
        ok++;
        bytes += 32;
      } catch (e: any) {
        fail++;
        errors[e.message?.slice(0, 30) || "ERR"] = (errors[e.message?.slice(0, 30) || "ERR"] || 0) + 1;
      }
    }
    await closeArk(db);
    Bun.spawnSync(["rm", "-f", path]);
  }

  const elapsed = sw.elapsed() / 1000;
  return {
    label: "ark_vs_bun_throughput",
    total: ok + fail, ok, fail,
    minMs: h.min(), maxMs: h.max(), avgMs: h.avg(),
    p50Ms: h.pct(50), p95Ms: h.pct(95), p99Ms: h.pct(99),
    throughputRps: elapsed > 0 ? (ok + fail) / elapsed : 0,
    errors, bytesProcessed: bytes,
  };
}
