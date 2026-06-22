import { Histogram, Stopwatch, fmtBytes } from "./report";
import type { StressMetrics } from "./report";

interface ServerStressOpts {
  baseUrl: string;
  concurrency: number;
  userCount: number;
  dbPerUser: number;
  walEntriesPerDb: number;
  mixedOps: number;
  snapshotCount: number;
  quiet?: boolean;
}

const noop = () => {};
function log(msg: string, quiet?: boolean) {
  if (!quiet) console.log(msg);
}

const DEFAULTS: ServerStressOpts = {
  baseUrl: "http://localhost:8080",
  concurrency: 10,
  userCount: 5,
  dbPerUser: 3,
  walEntriesPerDb: 500,
  mixedOps: 100,
  snapshotCount: 10,
};

interface AuthCtx { email: string; password: string; token: string; userID: number; }
interface DbCtx { dbID: string; apiKey: string; name: string; user: AuthCtx; }

type SendFn = () => Promise<Response>;

class Flight {
  moments: number[] = [];
  ok = 0;
  fail = 0;
  errors: Record<string, number> = {};
  running = 0;
  constructor(public label: string) {}
}

export async function runServerStress(opts: Partial<ServerStressOpts> = {}): Promise<{
  metrics: StressMetrics[];
  users: AuthCtx[];
  dbs: DbCtx[];
}> {
  const cfg = { ...DEFAULTS, ...opts };
  const flights = new Map<string, Flight>();

  function send(label: string, fn: SendFn): Promise<Response> {
    let f = flights.get(label);
    if (!f) { f = new Flight(label); flights.set(label, f); }
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

  function report(label: string): StressMetrics {
    const f = flights.get(label)!;
    const elapsed = f.moments.length > 0 ? (f.moments.reduce((a, b) => a + b, 0) / f.moments.length * f.moments.length) / 1000 : 0;
    const totalElapsed = f.moments.length > 0 ? Math.max(...f.moments) / 1000 : 0;
    const h = new Histogram();
    for (const v of f.moments) h.push(v);
    return {
      label, total: f.ok + f.fail, ok: f.ok, fail: f.fail,
      minMs: h.min(), maxMs: h.max(), avgMs: h.avg(),
      p50Ms: h.pct(50), p95Ms: h.pct(95), p99Ms: h.pct(99),
      throughputRps: totalElapsed > 0 ? (f.ok + f.fail) / totalElapsed : 0,
      errors: { ...f.errors },
    };
  }

  function post(path: string, body: unknown, token?: string): Promise<Response> {
    const headers: Record<string, string> = { "Content-Type": "application/json" };
    if (token) headers["Authorization"] = `Bearer ${token}`;
    return fetch(`${cfg.baseUrl}${path}`, { method: "POST", headers, body: JSON.stringify(body) });
  }

  function get(path: string, token?: string): Promise<Response> {
    const headers: Record<string, string> = {};
    if (token) headers["Authorization"] = `Bearer ${token}`;
    return fetch(`${cfg.baseUrl}${path}`, { headers });
  }

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

  // ── 1. Auth stress ───────────────────────────────────────────────
  log("[server] registering + logging in users…", cfg.quiet);
  const users: AuthCtx[] = [];
  for (let i = 0; i < cfg.userCount; i++) {
    const email = `stress_${Date.now()}_${i}_${Math.random().toString(36).slice(2, 6)}@petshop.arkilian`;
    const password = `pw_${i}_${Math.random().toString(36).slice(2, 8)}`;
    await send("auth_register", () => post("/v1/auth/register", { email, password }));
    const loginRes = await send("auth_login", () => post("/v1/auth/login", { email, password }));
    if (loginRes.ok) {
      const body = await loginRes.json() as { token: string; user_id: number };
      users.push({ email, password, token: body.token, userID: body.user_id });
    }
  }

  // ── 2. Database creation stress ──────────────────────────────────
  log("[server] creating databases…", cfg.quiet);
  const dbs: DbCtx[] = [];
  const pool = asyncPool(cfg.concurrency);
  for (const u of users) {
    for (let i = 0; i < cfg.dbPerUser; i++) {
      pool.add(async () => {
        const name = `petshop_${u.userID}_${i}`;
        const res = await send("db_create", () => post("/v1/db/create", { name }, u.token));
        if (res.ok) {
          const body = await res.json() as { db_id: string; api_key: string; name: string };
          dbs.push({ dbID: body.db_id, apiKey: body.api_key, name, user: u });
        }
      });
    }
  }
  await pool.done();

  // ── 3. DB list stress ────────────────────────────────────────────
  log("[server] listing databases…", cfg.quiet);
  for (const u of users) {
    await send("db_list", () => get("/v1/db/list", u.token));
  }

  // ── 4. WAL push stress ───────────────────────────────────────────
  log("[server] pushing WAL entries…", cfg.quiet);
  for (const dbCtx of dbs) {
    const entries = [];
    for (let i = 0; i < cfg.walEntriesPerDb; i++) {
      entries.push({
        ts: Date.now() + i,
        op: 1,
        table_id: 1,
        pk: i,
        sql: `INSERT INTO events (id, payload, ts) VALUES (${i}, 'wal_${i}', ${Date.now() + i});`,
      });
    }
    // Push in batches of 100
    for (let i = 0; i < entries.length; i += 100) {
      const batch = entries.slice(i, i + 100);
      await send("wal_push", () => post("/v1/wal/push", batch, dbCtx.apiKey));
    }
  }

  // ── 5. Upload request stress ─────────────────────────────────────
  log("[server] requesting upload URLs…", cfg.quiet);
  for (const dbCtx of dbs) {
    for (let i = 0; i < 5; i++) {
      await send("upload_request", () =>
        post("/v1/upload/request", {
          db_id: dbCtx.dbID,
          event_count: 100,
          lsn_start: i * 100 + 1,
          lsn_end: (i + 1) * 100,
        }, dbCtx.apiKey)
      );
    }
  }

  // ── 6. WAL count stress ──────────────────────────────────────────
  log("[server] querying WAL counts…", cfg.quiet);
  for (const dbCtx of dbs) {
    await send("wal_count", () => get("/v1/wal/count", dbCtx.apiKey));
  }

  // ── 7. Snapshot register stress ──────────────────────────────────
  log("[server] registering snapshots…", cfg.quiet);
  for (const dbCtx of dbs.slice(0, Math.min(dbs.length, cfg.snapshotCount))) {
    await send("snapshot_register", () =>
      post("/v1/snapshot/register", {
        baseline_lsn: 0,
        s3_key: `petshop_snapshots/${dbCtx.dbID}/snap_0.sqlite.zst`,
      }, dbCtx.apiKey)
    );
  }

  // ── 8. Hydrate plan stress ───────────────────────────────────────
  log("[server] requesting hydrate plans…", cfg.quiet);
  for (const dbCtx of dbs) {
    await send("hydrate_plan", () => get("/v1/hydrate/plan", dbCtx.apiKey));
  }

  // ── 9. Mixed concurrent workload ─────────────────────────────────
  log("[server] mixed concurrent workload…", cfg.quiet);
  const mixedPool = asyncPool(cfg.concurrency);
  const endpoints = dbs.flatMap(dbCtx => [
    () => get("/v1/wal/count", dbCtx.apiKey),
    () => get("/v1/db/list", dbCtx.user.token),
    () => get("/health"),
    () => post("/v1/db/create", { name: `mixed_${Date.now()}_${Math.random().toString(36).slice(2)}` }, dbCtx.user.token),
    () => post("/v1/wal/push", [{ ts: Date.now(), op: 1, table_id: 1, pk: Math.floor(Math.random() * 1000000), sql: "INSERT INTO t VALUES (1);" }], dbCtx.apiKey),
  ]);
  for (let i = 0; i < cfg.mixedOps; i++) {
    const fn = endpoints[Math.floor(Math.random() * endpoints.length)];
    mixedPool.add(() => send("mixed_workload", fn));
  }
  await mixedPool.done();

  // ── 10. Health check burst ───────────────────────────────────────
  log("[server] health check burst…", cfg.quiet);
  const healthPool = asyncPool(50);
  for (let i = 0; i < 500; i++) {
    healthPool.add(() => send("health_check", () => get("/health")));
  }
  await healthPool.done();

  // ── Collect metrics ──────────────────────────────────────────────
  const metrics: StressMetrics[] = [];
  for (const [label] of flights) {
    metrics.push(report(label));
  }

  return { metrics, users, dbs };
}
