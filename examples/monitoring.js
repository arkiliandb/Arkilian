// Arkilian Prometheus /metrics Exporter — launch Checklist #4
//
// A ready-to-run monitoring endpoint that exposes every Arkilian health
// signal as Prometheus metrics. Customer DevOps teams point a Prometheus
// scrape target (or Grafana Agent / OpenTelemetry Collector) at this
// process and alert on:
//
//   arkilian_healthy == 0                          → backup unhealthy
//   arkilian_triggers_dirty == 1                   → raw-handle DDL desync
//   arkilian_queue_depth (sustained growth)        → destination backlog
//   arkilian_oldest_pending_age_seconds > RPO      → replication lag
//   arkilian_dead_letter_count (any non-zero)      → undelivered customer data
//   arkilian_thread_heartbeat_age_ms > 10000       → flush thread died
//   arkilian_snapshot_heartbeat_age_ms > threshold  → snapshot thread died
//   arkilian_trigger_coverage > 0                  → a table lost triggers
//   arkilian_skipped_table_count > 0              → PK-less table not captured
//
// Usage:
//   node examples/monitoring.js <db.sqlite> [port]
//
// Defaults to port 9100. Scraped by:
//   prometheus --config.file=prometheus.yml
//
// prometheus.yml:
//   scrape_configs:
//     - job_name: 'arkilian'
//       static_configs:
//         - targets: ['localhost:9100']
//
// No external dependencies — uses only Node's built-in http module + the
// arkilian native addon. The metrics endpoint is GET /metrics in the
// standard Prometheus text exposition format.

import http from "node:http";
import { Arkilian } from "../index.js";

const dbPath = process.argv[2] || "app.sqlite";
const port = parseInt(process.argv[3] || "9100", 10);

const db = new Arkilian(process.env.ARKILIAN_API_KEY || "monitoring", dbPath);

// Prometheus text exposition format. Every metric is GAUGE type (the
// Arkilian signals are instantaneous snapshots, not counters) unless
// noted. The # HELP / # TYPE lines follow the standard so Prometheus,
// Grafana, and the OpenTelemetry Collector all parse them correctly.
function renderMetrics() {
  const healthy = db.backupHealthy ? 1 : 0;
  const enabled = db.backupEnabled ? 1 : 0;
  const triggersDirty = db.triggersDirty ? 1 : 0;
  const capturePaused = db.capturePaused ? 1 : 0;
  const autoResync = db.autoResyncTriggers ? 1 : 0;
  const queueDepth = db.backupQueueDepth;
  const oldestAge = db.backupOldestPendingAgeSec;
  const deadLetters = db.backupDeadLetterCount;
  const hbAge = db.backupThreadHeartbeatAgeMs;
  const snapHbAge = db.backupSnapshotHeartbeatAgeMs;
  const triggerCoverage = db.backupTriggerCoverage;
  const skippedTables = db.backupSkippedTableCount;

  const lines = [
    "# HELP arkilian_healthy 1 when the backup subsystem is healthy (enabled, destination configured, flush thread alive, queue under cap). 0 = investigate.",
    "# TYPE arkilian_healthy gauge",
    `arkilian_healthy ${healthy}`,
    "",
    "# HELP arkilian_backup_enabled 1 when backup is enabled (runtime kill-switch on). 0 = killed.",
    "# TYPE arkilian_backup_enabled gauge",
    `arkilian_backup_enabled ${enabled}`,
    "",
    "# HELP arkilian_queue_depth Rows in _pending_backup not yet delivered to the control plane.",
    "# TYPE arkilian_queue_depth gauge",
    `arkilian_queue_depth ${queueDepth}`,
    "",
    "# HELP arkilian_oldest_pending_age_seconds Age in seconds of the oldest undelivered row — the realtime replication lag metric.",
    "# TYPE arkilian_oldest_pending_age_seconds gauge",
    `arkilian_oldest_pending_age_seconds ${oldestAge}`,
    "",
    "# HELP arkilian_dead_letter_count Rows dead-lettered after MAX_ATTEMPTS. Every non-zero row is customer data that did not reach the destination.",
    "# TYPE arkilian_dead_letter_count gauge",
    `arkilian_dead_letter_count ${deadLetters}`,
    "",
    "# HELP arkilian_flush_thread_heartbeat_age_ms Milliseconds since the flush thread's last heartbeat. > 10000 = thread died silently.",
    "# TYPE arkilian_flush_thread_heartbeat_age_ms gauge",
    `arkilian_flush_thread_heartbeat_age_ms ${hbAge}`,
    "",
    "# HELP arkilian_snapshot_thread_heartbeat_age_ms Milliseconds since the hourly snapshot thread's last heartbeat. Stale = snapshots quietly stopped.",
    "# TYPE arkilian_snapshot_thread_heartbeat_age_ms gauge",
    `arkilian_snapshot_thread_heartbeat_age_ms ${snapHbAge}`,
    "",
    "# HELP arkilian_trigger_coverage N triggers missing from PK-capable tables. 0 = full coverage. > 0 = a table lost its capture triggers.",
    "# TYPE arkilian_trigger_coverage gauge",
    `arkilian_trigger_coverage ${triggerCoverage}`,
    "",
    "# HELP arkilian_skipped_table_count Real tables with no PRIMARY KEY — unreplayable, not captured. Every skipped table is data that never leaves the box.",
    "# TYPE arkilian_skipped_table_count gauge",
    `arkilian_skipped_table_count ${skippedTables}`,
    "",
    "# HELP arkilian_triggers_dirty 1 when raw-handle DDL (Prisma/Drizzle/TypeORM/raw sqlite3_exec) has desynchronized capture triggers. Alert and call db.resyncTriggers() to repair.",
    "# TYPE arkilian_triggers_dirty gauge",
    `arkilian_triggers_dirty ${triggersDirty}`,
    "",
    "# HELP arkilian_capture_paused Sticky: 1 when the outbox hit ARKILIAN_MAX_QUEUE_DEPTH and CDC rows are being dropped (only the hourly snapshot will recover them). Stays 1 after drain; clears on successful snapshot upload.",
    "# TYPE arkilian_capture_paused gauge",
    `arkilian_capture_paused ${capturePaused}`,
    "",
    "# HELP arkilian_auto_resync_triggers 1 when opt-in post-commit auto-resync is enabled (raw-handle DDL users).",
    "# TYPE arkilian_auto_resync_triggers gauge",
    `arkilian_auto_resync_triggers ${autoResync}`,
    "",
  ];

  return lines.join("\n");
}

const server = http.createServer((req, res) => {
  if (req.url === "/metrics" && req.method === "GET") {
    try {
      const body = renderMetrics();
      res.writeHead(200, {
        "Content-Type": "text/plain; version=0.0.4; charset=utf-8",
        "Content-Length": Buffer.byteLength(body),
      });
      res.end(body);
    } catch (err) {
      res.writeHead(500, { "Content-Type": "text/plain" });
      res.end(`# metrics render error: ${err.message}\n`);
    }
  } else if (req.url === "/" && req.method === "GET") {
    res.writeHead(200, { "Content-Type": "text/plain" });
    res.end("Arkilian monitoring endpoint — scrape /metrics\n");
  } else {
    res.writeHead(404, { "Content-Type": "text/plain" });
    res.end("Not found\n");
  }
});

server.listen(port, () => {
  console.log(`arkilian monitoring: scraping /metrics on http://0.0.0.0:${port}/metrics`);
  console.log(`arkilian monitoring: db=${dbPath}`);
  console.log("arkilian monitoring: press Ctrl+C to stop");
});

// On shutdown, close the database cleanly so the flush + snapshot threads
// join and no in-flight ships are left hanging.
process.on("SIGINT", () => {
  console.log("\narkilian monitoring: shutting down...");
  db.close();
  server.close();
  process.exit(0);
});
process.on("SIGTERM", () => {
  db.close();
  server.close();
  process.exit(0);
});
