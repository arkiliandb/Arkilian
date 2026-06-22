interface StressMetrics {
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
  bytesProcessed?: number;
  peakMemMB?: number;
}

class Histogram {
  private data: number[] = [];
  push(v: number) { this.data.push(v); }
  pct(n: number): number {
    if (this.data.length === 0) return 0;
    const s = [...this.data].sort((a, b) => a - b);
    return s[Math.ceil((n / 100) * s.length) - 1] ?? s[s.length - 1];
  }
  avg() { return this.data.length ? this.data.reduce((a, b) => a + b, 0) / this.data.length : 0; }
  min() { return this.data.length ? Math.min(...this.data) : 0; }
  max() { return this.data.length ? Math.max(...this.data) : 0; }
}

class Stopwatch {
  private t0 = performance.now();
  private laps: number[] = [];
  elapsed() { return performance.now() - this.t0; }
  lap() { const now = performance.now(); this.laps.push(now - this.t0); return now - this.t0; }
  reset() { this.t0 = performance.now(); this.laps = []; }
  getLaps() { return this.laps; }
}

function fmtMs(v: number): string {
  if (v < 1) return `${(v * 1000).toFixed(0)}μs`;
  if (v < 1000) return `${v.toFixed(1)}ms`;
  return `${(v / 1000).toFixed(2)}s`;
}

function fmtBytes(b: number): string {
  if (b < 1024) return `${b}B`;
  if (b < 1024 * 1024) return `${(b / 1024).toFixed(1)}KB`;
  if (b < 1024 * 1024 * 1024) return `${(b / 1024 / 1024).toFixed(2)}MB`;
  return `${(b / 1024 / 1024 / 1024).toFixed(2)}GB`;
}

function renderReport(metrics: StressMetrics[], extras?: Record<string, string | number>): void {
  const totalOk = metrics.reduce((s, m) => s + m.ok, 0);
  const totalFail = metrics.reduce((s, m) => s + m.fail, 0);
  const totalReq = totalOk + totalFail;
  const totalBytes = metrics.reduce((s, m) => s + (m.bytesProcessed ?? 0), 0);

  const W = process.stdout.columns || 100;
  console.log("\n" + "═".repeat(Math.min(W, 100)));
  console.log("  🐾 PetshopDB — Arkilian Stress Test Report");
  console.log("═".repeat(Math.min(W, 100)));

  console.log(`\n  Summary:`);
  console.log(`    requests:     ${totalReq.toLocaleString()}`);
  console.log(`    ✅ passed:    ${totalOk.toLocaleString()}`);
  console.log(`    ❌ failed:    ${totalFail.toLocaleString()}  (${totalReq ? ((totalFail / totalReq) * 100).toFixed(2) : "0"}%)`);
  if (totalBytes > 0) console.log(`    data volume:  ${fmtBytes(totalBytes)}`);
  console.log("");

  const sorted = [...metrics].sort((a, b) => a.p95Ms - b.p95Ms);

  console.log("─".repeat(Math.min(W, 100)));
  const h = `  ${"test".padEnd(28)} ${"total".padStart(7)} ${"ok".padStart(7)} ${"fail".padStart(6)} ${"avg".padStart(9)} ${"p50".padStart(9)} ${"p95".padStart(9)} ${"p99".padStart(9)} ${"rps".padStart(8)}`;
  console.log(h);
  console.log("─".repeat(Math.min(W, 100)));

  for (const m of sorted) {
    const line = `  ${m.label.padEnd(28)} ${String(m.total).padStart(7)} ${String(m.ok).padStart(7)} ${String(m.fail).padStart(6)} ${fmtMs(m.avgMs).padStart(9)} ${fmtMs(m.p50Ms).padStart(9)} ${fmtMs(m.p95Ms).padStart(9)} ${fmtMs(m.p99Ms).padStart(9)} ${m.throughputRps.toFixed(1).padStart(8)}`;
    console.log(line);
    if (Object.keys(m.errors).length > 0) {
      for (const [err, count] of Object.entries(m.errors)) {
        console.log(`    ↳ ${err}: ${count}`);
      }
    }
  }

  console.log("─".repeat(Math.min(W, 100)));

  // Slowest endpoints
  const slowest = [...sorted].sort((a, b) => b.p95Ms - a.p95Ms);
  console.log(`\n  ⚡ Top 5 slowest (p95):`);
  for (const r of slowest.slice(0, 5)) {
    console.log(`    ${r.label.padEnd(28)} p95=${fmtMs(r.p95Ms)}  avg=${fmtMs(r.avgMs)}  max=${fmtMs(r.maxMs)}`);
  }

  // Fastest endpoints
  const fastest = [...sorted].sort((a, b) => a.p95Ms - b.p95Ms);
  console.log(`\n  ⚡ Top 5 fastest (p95):`);
  for (const r of fastest.slice(0, 5)) {
    console.log(`    ${r.label.padEnd(28)} p95=${fmtMs(r.p95Ms)}  avg=${fmtMs(r.avgMs)}  min=${fmtMs(r.minMs)}`);
  }

  // Bottlenecks
  const highFail = sorted.filter(m => m.fail > 0);
  if (highFail.length > 0) {
    console.log(`\n  🔴 Endpoints with failures:`);
    for (const r of highFail) {
      console.log(`    ${r.label.padEnd(28)} fail=${r.fail}/${r.total}  (${(r.fail / r.total * 100).toFixed(1)}%)`);
    }
  }

  if (extras) {
    console.log(`\n  📊 Additional Metrics:`);
    for (const [k, v] of Object.entries(extras)) {
      console.log(`    ${k}: ${v}`);
    }
  }

  console.log("\n" + "═".repeat(Math.min(W, 100)) + "\n");
}

export type { StressMetrics };
export { Histogram, Stopwatch, fmtMs, fmtBytes, renderReport };
