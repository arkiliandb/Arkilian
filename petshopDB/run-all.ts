#!/usr/bin/env bun
/**
 * petshopDB — Arkilian Full Stress Test Suite
 *
 * Stress-tests both:
 *   1. Client (local SQLite via bun:sqlite)
 *   2. Server (Arkilian Control Plane HTTP API)
 *
 * Usage:
 *   bun run petshopDB/run-all.ts                    # defaults (no server)
 *   bun run petshopDB/run-all.ts --server           # includes server tests
 *   bun run petshopDB/run-all.ts --server --url http://localhost:8080
 *   bun run petshopDB/run-all.ts --client-only      # only client tests
 *   bun run petshopDB/run-all.ts --server-only      # only server tests
 *   bun run petshopDB/run-all.ts --light            # quick smoke test (fewer ops)
 *   bun run petshopDB/run-all.ts --json             # JSON output
 *
 * Env:
 *   STRESS_CONCURRENCY, STRESS_ROWS, STRESS_BATCH,
 *   TARGET_URL, STRESS_USERS, STRESS_DBS, STRESS_WAL
 */

import { renderReport } from "./report";
import type { StressMetrics } from "./report";

const args = process.argv.slice(2);
const JSON_OUTPUT = args.includes("--json");
const RUN_SERVER = args.includes("--server") || args.includes("--server-only");
const RUN_CLIENT = !args.includes("--server-only");
const LIGHT = args.includes("--light");
const TARGET_URL = args.find(a => a.startsWith("http://") || a.startsWith("https://"))
  || process.env.TARGET_URL || "http://localhost:8080";

if (!JSON_OUTPUT) {
  console.log("╔══════════════════════════════════════════════════════════════╗");
  console.log("║   🐾 PetshopDB — Full Stress Test Suite                     ║");
  console.log("║   Arkilian Client (SQLite) + Server (Control Plane)         ║");
  console.log("╚══════════════════════════════════════════════════════════════╝");
  console.log(`  mode:     ${LIGHT ? "LIGHT (smoke)" : "FULL"}`);
  console.log(`  client:   ${RUN_CLIENT ? "YES" : "no"}`);
  console.log(`  server:   ${RUN_SERVER ? "YES" : "no"}`);
  if (RUN_SERVER) console.log(`  target:   ${TARGET_URL}`);
  console.log("");
}

const allMetrics: StressMetrics[] = [];
const extras: Record<string, string | number> = {};

// ── Client stress ──────────────────────────────────────────────────
if (RUN_CLIENT) {
  if (!JSON_OUTPUT) console.log("━".repeat(50));
  if (!JSON_OUTPUT) console.log("  📦 Client Stress Test (local SQLite)");
  if (!JSON_OUTPUT) console.log("━".repeat(50));

  const { runClientStress } = await import("./stress-client");
  const clientOpts: Record<string, unknown> = {};
  if (LIGHT) {
    clientOpts.rowCount = 5000;
    clientOpts.concurrency = 4;
    clientOpts.batchSize = 100;
  } else {
    clientOpts.rowCount = parseInt(process.env.STRESS_ROWS || "50000", 10);
    clientOpts.concurrency = parseInt(process.env.STRESS_CONCURRENCY || "8", 10);
    clientOpts.batchSize = parseInt(process.env.STRESS_BATCH || "500", 10);
  }

  const clientStart = performance.now();
  const clientResult = await runClientStress(clientOpts);
  const clientElapsed = (performance.now() - clientStart) / 1000;

  allMetrics.push(...clientResult.metrics);
  extras["client_duration"] = `${clientElapsed.toFixed(1)}s`;
  const clientTotalOps = clientResult.metrics.reduce((s, m) => s + m.total, 0);
  const clientTotalOk = clientResult.metrics.reduce((s, m) => s + m.ok, 0);
  extras["client_total_ops"] = clientTotalOps.toLocaleString();
  extras["client_total_ok"] = clientTotalOk.toLocaleString();
}

// ── Server stress ──────────────────────────────────────────────────
if (RUN_SERVER) {
  if (!JSON_OUTPUT) console.log("\n" + "━".repeat(50));
  if (!JSON_OUTPUT) console.log("  🌐 Server Stress Test (Control Plane API)");
  if (!JSON_OUTPUT) console.log("━".repeat(50));

  const { runServerStress } = await import("./stress-server");
  const serverOpts: Record<string, unknown> = { baseUrl: TARGET_URL, quiet: JSON_OUTPUT };
  if (LIGHT) {
    serverOpts.userCount = 2;
    serverOpts.dbPerUser = 1;
    serverOpts.walEntriesPerDb = 50;
    serverOpts.mixedOps = 20;
  } else {
    serverOpts.userCount = parseInt(process.env.STRESS_USERS || "5", 10);
    serverOpts.dbPerUser = parseInt(process.env.STRESS_DBS || "3", 10);
    serverOpts.walEntriesPerDb = parseInt(process.env.STRESS_WAL || "500", 10);
    serverOpts.concurrency = parseInt(process.env.STRESS_CONCURRENCY || "10", 10);
    serverOpts.mixedOps = 100;
  }

  const serverStart = performance.now();
  try {
    const serverResult = await runServerStress(serverOpts);
    const serverElapsed = (performance.now() - serverStart) / 1000;
    allMetrics.push(...serverResult.metrics);
    extras["server_duration"] = `${serverElapsed.toFixed(1)}s`;
    extras["server_total_ops"] = serverResult.metrics.reduce((s, m) => s + m.total, 0);
    extras["server_users_created"] = serverResult.users.length;
    extras["server_databases_created"] = serverResult.dbs.length;
  } catch (e: any) {
    if (!JSON_OUTPUT) console.error(`  ✗ Server stress failed: ${e.message}`);
    extras["server_error"] = e.message;
  }
}

// ── Report ─────────────────────────────────────────────────────────
if (JSON_OUTPUT) {
  const finalReport: Record<string, unknown> = {
    metrics: allMetrics,
    extras,
    timestamp: new Date().toISOString(),
    mode: LIGHT ? "light" : "full",
  };
  const out: Record<string, StressMetrics> = {};
  for (const m of allMetrics) out[m.label] = m;
  console.log(JSON.stringify({ ...out, extras, timestamp: new Date().toISOString(), mode: LIGHT ? "light" : "full" }, null, 2));
} else {
  renderReport(allMetrics, extras);
}
