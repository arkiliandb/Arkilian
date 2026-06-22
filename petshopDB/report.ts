interface StressMetrics {
  label: string;
  total: number;
  ok: number;
  fail: number;
  minMs: number;
  maxMs: number;
  avgMs: number;
  p50Ms: number;
  p70Ms?: number;
  p88Ms?: number;
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
  len() { return this.data.length; }
  stddev(): number {
    if (this.data.length < 2) return 0;
    const mean = this.avg();
    const sqDiffs = this.data.map(v => (v - mean) ** 2);
    return Math.sqrt(sqDiffs.reduce((a, b) => a + b, 0) / this.data.length);
  }
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
  if (v < 0.001) return `${(v * 1000000).toFixed(0)}ns`;
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

function renderReport(metrics: StressMetrics[], extras?: Record<string, string | number>) {
  const totalOk = metrics.reduce((s, m) => s + m.ok, 0);
  const totalFail = metrics.reduce((s, m) => s + m.fail, 0);
  const totalReq = totalOk + totalFail;
  const totalBytes = metrics.reduce((s, m) => s + (m.bytesProcessed ?? 0), 0);
  const errorRate = totalReq ? ((totalFail / totalReq) * 100).toFixed(2) : "0";

  const W = Math.min(process.stdout.columns || 100, 120);
  console.log("\n" + "═".repeat(W));
  console.log("  🐾 PetshopDB — Arkilian Stress Test Report");
  console.log("═".repeat(W));

  console.log(`\n  Summary:`);
  console.log(`    Total requests:    ${totalReq.toLocaleString()}`);
  console.log(`    ✅  Passed:        ${totalOk.toLocaleString()}`);
  console.log(`    ❌  Failed:        ${totalFail.toLocaleString()}  (${errorRate}%)`);
  if (totalBytes > 0) console.log(`    Data volume:       ${fmtBytes(totalBytes)}`);
  console.log("");

  // Per-test detail table with full percentile breakdown
  const sorted = [...metrics].sort((a, b) => a.p95Ms - b.p95Ms || a.avgMs - b.avgMs);

  console.log("─".repeat(Math.min(W, 136)));
  const hdr = `  ${"test".padEnd(26)} ${"total".padStart(7)} ${"fail".padStart(6)} ${"avg".padStart(10)} ${"p50".padStart(9)} ${"p70".padStart(9)} ${"p88".padStart(9)} ${"p95".padStart(9)} ${"p99".padStart(9)} ${"⌀rps".padStart(8)}`;
  console.log(hdr);
  console.log("─".repeat(Math.min(W, 136)));

  for (const m of sorted) {
    const status = m.fail > 0 ? "❌" : "✅";
    const line = `  ${status} ${m.label.padEnd(24)} ${String(m.total).padStart(7)} ${String(m.fail).padStart(6)} ${fmtMs(m.avgMs).padStart(10)} ${fmtMs(m.p50Ms).padStart(9)} ${fmtMs(m.p70Ms ?? 0).padStart(9)} ${fmtMs(m.p88Ms ?? 0).padStart(9)} ${fmtMs(m.p95Ms).padStart(9)} ${fmtMs(m.p99Ms).padStart(9)} ${m.throughputRps.toFixed(0).padStart(8)}`;
    console.log(line);
    if (Object.keys(m.errors).length > 0) {
      for (const [err, count] of Object.entries(m.errors)) {
        console.log(`       └ ${err}: ${count}`);
      }
    }
  }
  console.log("─".repeat(Math.min(W, 136)));

  // Slowest by p95
  const slowest = [...sorted].sort((a, b) => b.p95Ms - a.p95Ms);
  console.log(`\n  🔴  Top 5 Slowest (p95):`);
  for (const r of slowest.slice(0, 5)) {
    console.log(`      ${r.label.padEnd(26)} p70=${fmtMs(r.p70Ms ?? 0)}  p88=${fmtMs(r.p88Ms ?? 0)}  p95=${fmtMs(r.p95Ms)}  p99=${fmtMs(r.p99Ms)}  max=${fmtMs(r.maxMs)}`);
  }

  // Fastest by p50
  const fastest = [...sorted].sort((a, b) => a.p50Ms - b.p50Ms);
  console.log(`\n  🟢  Top 5 Fastest (p50):`);
  for (const r of fastest.slice(0, 5)) {
    console.log(`      ${r.label.padEnd(26)} min=${fmtMs(r.minMs)}  p50=${fmtMs(r.p50Ms)}  avg=${fmtMs(r.avgMs)}`);
  }

  // Failures
  const highFail = sorted.filter(m => m.fail > 0);
  if (highFail.length > 0) {
    console.log(`\n  ❌  Endpoints with Failures:`);
    for (const r of highFail) {
      console.log(`      ${r.label.padEnd(26)} fail=${r.fail}/${r.total}  (${(r.fail / Math.max(r.total, 1) * 100).toFixed(1)}%)`);
    }
  }

  // Latency distribution summary
  console.log(`\n  📊  Latency Distribution Summary (all endpoints):`);
  const allLatencies: number[] = [];
  for (const m of metrics) {
    if (m.avgMs > 0) allLatencies.push(m.avgMs);
  }
  if (allLatencies.length > 0) {
    allLatencies.sort((a, b) => a - b);
    const n = allLatencies.length;
    console.log(`      samples: ${n}  │  min: ${fmtMs(allLatencies[0])}  │  max: ${fmtMs(allLatencies[n - 1])}`);
    console.log(`      p50: ${fmtMs(allLatencies[Math.floor(n * 0.5)])}  │  p70: ${fmtMs(allLatencies[Math.floor(n * 0.7)])}  │  p88: ${fmtMs(allLatencies[Math.floor(n * 0.88)])}`);
    console.log(`      p95: ${fmtMs(allLatencies[Math.floor(n * 0.95)])}  │  p99: ${fmtMs(allLatencies[Math.floor(n * 0.99)])}`);
  }

  if (extras && Object.keys(extras).length > 0) {
    console.log(`\n  📋  Additional Metrics:`);
    for (const [k, v] of Object.entries(extras)) {
      console.log(`      ${k}: ${v}`);
    }
  }

  console.log("\n" + "═".repeat(W) + "\n");
}

export type { StressMetrics };
export { Histogram, Stopwatch, fmtMs, fmtBytes, renderReport };
