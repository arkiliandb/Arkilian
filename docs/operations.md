# Arkilian Operations Guide

Operational runbook for teams running Arkilian in production. The client
is an embedded SQLite wrapper that replicates every write to a control
plane; this guide covers monitoring, dead-letter handling, and incident
response.

---

## 1. Monitoring signals (spec §9)

All four signals are exposed by the C API and the Node binding:

| Signal | API | Alert when |
|--------|-----|------------|
| Queue depth | `db_backup_queue_depth()` | Sustained growth across checks |
| Realtime lag | `db_backup_oldest_pending_age_sec()` | Age exceeds your RPO window (default suggestion: > 5 min sustained) |
| Dead letters | `db_backup_dead_letter_count()` | Any non-zero growth — every row is a change that failed to ship |
| Thread liveness | `db_backup_thread_heartbeat_age_ms()` | Age > 10,000 ms (5 poll intervals) — thread died silently |
| Trigger coverage | `db_backup_trigger_coverage()` | Any non-zero value — a table lost its capture triggers |
| Skipped tables | `db_backup_skipped_table_count()` | Any non-zero value — tables with no PRIMARY KEY are not captured (see §7) |
| Health | `db_backup_is_healthy()` | Returns 0 — backup disabled, no destination configured, thread dead, OR queue above `ARKILIAN_MAX_QUEUE_DEPTH` (default 100,000) |

Node.js:

```js
const db = new Arkilian(token, "app.sqlite");
setInterval(() => {
  const healthy = db.backupHealthy;
  const lag = db.backupOldestPendingAgeSec;
  const dead = db.backupDeadLetterCount;
  // push {healthy, lag, dead} to your metrics endpoint
}, 15000);
```

Trigger coverage sanity check (the spec's §9 3x rule):

```sql
-- should equal 3 × (count of non-reserved tables)
SELECT COUNT(*) FROM sqlite_master
WHERE type = 'trigger' AND name LIKE 'trg\_%' ESCAPE '\';
```

## 2. Structured logging

All diagnostics are routed through a callback instead of stderr:

```c
static void on_log(ark_log_level_t level, const char *msg, void *ctx) {
  // forward to JSON logger / syslog / metric tags
}
db_set_log_callback(db, on_log, NULL);
db_set_default_log_callback(on_log, NULL); // pre-init warnings too
```

Levels: `ARK_LOG_ERROR`, `ARK_LOG_WARN`, `ARK_LOG_INFO`, `ARK_LOG_DEBUG`.
Without a callback, messages go to stderr (unchanged behavior).

## 3. Dead-letter queue (DLQ)

A row lands in `_dead_backup` after `MAX_ATTEMPTS` (default 100, override
with `ARKILIAN_MAX_ATTEMPTS`) failed deliveries. **Every dead-lettered row
is customer data that did not reach the backup destination.**

The `arkilian-dlq` tool is shipped as a prebuilt binary with each GitHub
release (no toolchain required) and also builds from source:
`cc tools/arkilian-dlq.c src/deps/sqlite/sqlite3.c -Isrc/deps/sqlite -o arkilian-dlq`

### Investigate first

```sh
# how many, and which rows are stuck
arkilian-dlq app.sqlite --count
arkilian-dlq app.sqlite --list --limit 50
# inspect a single row by id
arkilian-dlq app.sqlite --list --id 42
```

`failed_reason` is always `max attempts exceeded` — the real question is
*why the destination rejected/never acked*. Check:

1. Destination health (control plane up? auth token valid?)
2. Network path (DNS, firewall, TLS)
3. Payload validity — replay the SQL from `--list` against a scratch DB
   to confirm it applies cleanly

### Replay

Resolve the root cause first, then re-queue the rows — they ship the
moment the destination is healthy again (original ids preserved, attempts
reset):

```sh
# preview (all rows)
arkilian-dlq app.sqlite --replay --dry-run
# re-queue everything
arkilian-dlq app.sqlite --replay
# re-queue a single row by id (attempts reset to 0)
arkilian-dlq app.sqlite --replay --id 42
```

Replay is idempotent: rows already present in `_pending_backup` are
skipped; only re-queued rows leave `_dead_backup`. The flush thread (or
any later process opening the same file) picks them up automatically.

**Ownership:** the on-call engineer for the database/control-plane team
owns DLQ investigation and replay. Every replay must be preceded by a
root-cause note in the incident log.

## 4. Kill-switch (incident response)

`db_backup_set_enabled(db, 0)` stops ALL outbound backup activity without
a restart:

- No payloads ship; the queue keeps accumulating locally (nothing is
  deleted, attempts stay 0)
- No hourly snapshots upload
- The application keeps running normally

Use it when: the destination is compromised, the push pipeline is
misbehaving, or you need to stop data egress immediately. Re-enable with
`db_backup_set_enabled(db, 1)` — shipping resumes from where the queue
left off.

Node: `db.setBackupEnabled(false)` / `db.backupEnabled`.

## 5. Incident response

### "Backup queue growing / lag rising"

1. Check destination health (control plane `/health`, S3 reachability).
2. Check `db.backupThreadHeartbeatAgeMs` — if > 10 s the flush thread is
   dead: restart the process (the queue is safe on disk, any process can
   drain it).
3. Check logs for `ship_to_backup`, `backup upload`, or `dead-lettered`
   entries — they name the failing component.
4. If the destination is down: keep the app running (the queue is the
   buffer, spec §8.4), fix the destination, watch lag fall.
5. If dead letters accumulate: investigate (see §3), then replay.

### "Dead letters appearing"

Follow §3. Do NOT replay before the root cause is fixed — rows will just
dead-letter again.

### "Hourly snapshots not uploading"

Check `ARKILIAN_CONTROL_URL` is set and the control plane's
`/v1/upload/request` returns 200 with an `upload_url`. Verify an object
exists in the bucket. The client derives both the realtime push
(`/v1/wal/push`) and the signed-URL endpoint (`/v1/upload/request`) from
the single `ARKILIAN_CONTROL_URL` base.

### "Backup enabled but nothing ships"

`db_backup_is_enabled()` returns 1 but `ARKILIAN_CONTROL_URL` is empty —
the startup log line warns:

```
arkilian: [warn] backup is enabled (ARKILIAN_ENABLE_BACKUP) but
ARKILIAN_CONTROL_URL is not set — rows will accumulate in _pending_backup and never ship
```

Set the URL and restart. Rows captured while misconfigured ship once the
destination is configured.

## 6. Configuration quick reference

| Variable | Purpose |
|----------|---------|
| `ARKILIAN_CONTROL_URL` | Control-plane base URL (e.g. `https://api.arkilian.com`); client derives `/v1/wal/push` and `/v1/upload/request` |
| `ARKILIAN_API_KEY` | The ONLY credential — sent as `Authorization: Bearer <key>` to every control-plane endpoint |
| `ARKILIAN_ENABLE_BACKUP` | `1/0/true/false`; runtime toggle via `db_backup_set_enabled` |
| `ARKILIAN_BACKUP_INTERVAL` | Hourly snapshot interval (min 1 s) |
| `ARKILIAN_MAX_QUEUE_DEPTH` | Queue ceiling for `db_backup_is_healthy()` (default 100000) |
| `ARKILIAN_MAX_ATTEMPTS` | Dead-letter threshold (default 20; lower for faster DLQ in test) |
| `ARKILIAN_SKIP_STARTUP_AUTH` | Skip the startup API-key validation (test only — do NOT set in production) |
| `ARKILIAN_ALLOW_INSECURE` | Opt-in for non-HTTPS non-local endpoints (default 0 — never leak the key in cleartext) |
| `ARKILIAN_OUTBOX_DURABLE` | `synchronous=FULL` for the outbox (default 1); set 0 for throughput over power-loss durability |
| `ARKILIAN_DB_PATH` / `ARKILIAN_BACKUP_PATH` | Database and snapshot file paths |
| `ARKILIAN_STORAGE_HOSTS` | Allowlist of custom storage hosts (comma-separated, suffix-matched) for SSRF guard |

Real environment variables always win over a `./.env` file.

## 7. Design decisions (reviewed, spec §8.1 / §11)

- **Payload format is replayable SQL text, not JSON.** The spec's §6 JSON
  sketch (`{id, table, op, data}`) was a suggestion; the shipped format is
  the SQL statement itself (`REPLACE INTO "t" (...) VALUES (...)` /
  `DELETE FROM "t" WHERE <pk> = ...`), which any SQLite can apply
  directly. One format, one destination. The deduplication key is the
  `X-Arkilian-Payload-Id` header, not an `id` field in the body — the
  destination MUST dedupe on that header (the bundled control plane
  does).
- **DELETE payloads are keyed on the PRIMARY KEY, never rowid.** REPLACE
  INTO deletes + reinserts, so destination rowids shift after any UPDATE
  while the source's stays — rowid-keyed deletes would remove the wrong
  row (proven divergence) and leave stale copies. PK values survive
  REPLACE, so PK-keyed deletes stay correct for INTEGER, TEXT, and
  composite keys. Tables with **no PRIMARY KEY at all** (plain rowid
  tables) are unreplayable and are skipped at trigger-generation time
  with a loud warning — they are never silently mis-replicated.
- **No destination configured ⇒ rows are preserved, never deleted.**
  With backup enabled but `ARKILIAN_WAL_PUSH_URL` unset, the flush
  thread does not drain: rows accumulate in `_pending_backup` with
  attempts 0 (never dead-lettered) until a destination is configured.
  A startup warning names the misconfiguration.
- **One connection per thread** (§3.1): game connection, flush-thread
  connection, and a third connection owned by the hourly snapshot
  thread. Sharing one handle between the two backup threads would make
  shipping contend with the snapshot file copy.
- **`db_set_token()` is thread-safe** — readers snapshot the token under
  a mutex, so rotating it mid-request can never free memory a backup
  thread is reading (use-after-free).
- **`db_close()` is bounded** — the snapshot copy aborts on the shutdown
  flag between retry steps (no 10-minute join under persistent
  `SQLITE_BUSY`), and in-flight curl transfers abort via a progress
  callback that observes shutdown.
- **Ordering is strict** (§8.1): the drain stops on a retryable failure
  so the first unshipped row is always retried first. If your destination
  doesn't need ordering, skip-and-continue is the higher-throughput
  alternative — change it deliberately, not by accident.
- **Wake hook takes a mutex on the game thread.** This deviates from
  §5's lock-free sketch to eliminate the lost-wakeup race; the cost is an
  uncontended ~20 ns lock. The load-contention suite (spec §10.3) runs
  the game write path under a 20 ms/destination backup load and measures
  P99 write latency ≈ 0.6 ms (vs 1.3 ms baseline) — no measurable
  impact. Re-benchmark before changing it.
- **`db_close` aborts in-flight transfers** via a libcurl progress
  callback that observes the shutdown flag — shutdown never waits out a
  full 10 s/30 s curl timeout.
- **Init never fails on backup errors** (spec §0/§1): WAL failure,
  trigger-sync failure, or thread-creation failure logs loudly and drops
  the subsystem into the kill-switch's disabled state — the game always
  starts. `db_backup_is_enabled()` + `db_backup_is_healthy()` + the
  monitoring signals make the outage visible.
- **DDL is intercepted on every wrapper path** (spec §1): `db_exec` and
  DDL run through `db_prepare`/`db_step` both re-sync capture triggers
  and record the DDL itself in the outbox, so the destination mirror
  applies the schema before the rows it creates. Only raw
  `db_get_handle()` use bypasses this — treat hand-edited schema as an
  incident, and repair with `db_resync_triggers()`.
- **Bulk operations generate one outbox row per written row** (spec
  §4.3). For known large bulk paths (imports, migrations), prefer:
  flip the kill-switch off (`db_backup_set_enabled(0)`), run the bulk,
  re-enable — the queue drains afterward and the hourly snapshot covers
  the window. Never leave the kill-switch off for long; the skipped
  tables / queue-depth / lag monitors will tell you if you do.

## 8. Large BLOB & payload size guidance (launch Checklist #3)

Arkilian's capture triggers copy every column of every changed row into a
SQL text payload in `_pending_backup`. The HTTP push path caps the payload
at 16 MB (`CURLOPT_MAXFILESIZE_LARGE`) — rows exceeding this fail to ship
and eventually dead-letter.

**Do NOT store large binary objects (> 1 MB) directly in PK-backed
SQLite tables.** A single INSERT of a 5 MB BLOB produces a >5 MB outbox
payload; at 100 writes/sec that's >500 MB/sec of outbox growth + network
egress — the queue cap is hit almost instantly and capture pauses.

Instead, use the **external-blob pattern**:

```sql
-- BAD: the entire file enters _pending_backup on every change
CREATE TABLE images (id INTEGER PRIMARY KEY, data BLOB);

-- GOOD: store a reference; the small row ships instantly
CREATE TABLE images (
  id INTEGER PRIMARY KEY,
  storage_url TEXT,    -- S3/GCS/R2 presigned URL or object key
  sha256 TEXT,         -- content hash for integrity
  size_bytes INTEGER,
  content_type TEXT
);
```

Upload the binary to S3/Cloud Storage/Cloudflare R2 (or a presigned-URL
flow), then INSERT only the reference row. The capture payload is a few
hundred bytes; the large object is already durable in object storage.

## 9. Monitoring & alert hooks (launch Checklist #4)

All monitoring signals are exposed via the C API, the Node.js binding,
and a ready-to-run Prometheus exporter.

### Prometheus / Grafana

Run the bundled exporter:

```bash
node examples/monitoring.js app.sqlite 9100
```

Scrape it with Prometheus:

```yaml
# prometheus.yml
scrape_configs:
  - job_name: 'arkilian'
    static_configs:
      - targets: ['localhost:9100']
```

Alert rules (`alerts.yml`):

```yaml
groups:
  - name: arkilian
    rules:
      - alert: ArkilianBackupUnhealthy
        expr: arkilian_healthy == 0
        for: 2m
        labels: { severity: critical }
        annotations:
          summary: "Arkilian backup is unhealthy"
      - alert: ArkilianTriggersDirty
        expr: arkilian_triggers_dirty == 1
        for: 1m
        labels: { severity: warning }
        annotations:
          summary: "Raw-handle DDL desynchronized capture triggers — run db_resync_triggers()"
      - alert: ArkilianQueueBacklog
        expr: arkilian_queue_depth > 0
        for: 5m
        labels: { severity: warning }
        annotations:
          summary: "Arkilian outbox backlog sustained — destination may be down"
      - alert: ArkilianReplicationLag
        expr: arkilian_oldest_pending_age_seconds > 300
        for: 1m
        labels: { severity: warning }
        annotations:
          summary: "Arkilian replication lag exceeds 5 min RPO"
      - alert: ArkilianDeadLetters
        expr: arkilian_dead_letter_count > 0
        for: 1m
        labels: { severity: critical }
        annotations:
          summary: "Arkilian has dead-lettered rows — investigate and replay"
      - alert: ArkilianFlushThreadDead
        expr: arkilian_flush_thread_heartbeat_age_ms > 30000
        for: 1m
        labels: { severity: critical }
        annotations:
          summary: "Arkilian flush thread appears dead"
      - alert: ArkilianTriggerCoverageGap
        expr: arkilian_trigger_coverage > 0
        for: 1m
        labels: { severity: warning }
        annotations:
          summary: "Arkilian trigger coverage gap — a table lost its capture triggers"
```

### Node.js (custom logger / OpenTelemetry)

```js
setInterval(() => {
  const metrics = {
    healthy: db.backupHealthy,
    triggersDirty: db.triggersDirty,
    queueDepth: db.backupQueueDepth,
    lagSec: db.backupOldestPendingAgeSec,
    deadLetters: db.backupDeadLetterCount,
    hbAgeMs: db.backupThreadHeartbeatAgeMs,
    triggerCoverage: db.backupTriggerCoverage,
    skippedTables: db.backupSkippedTableCount,
  };
  // forward to Datadog / OTel / your metrics pipeline
}, 15000);
```

## 10. Control-plane ingestion capacity verification (launch Checklist #1)

Because each captured row ships as one HTTP POST to `ARKILIAN_CONTROL_URL/v1/wal/push`,
at 5,000 businesses averaging 20–100 writes/sec the control plane must
ingest **100,000–500,000 RPS**. Before launch:

1. **Stress-test the ingestion path** (NGINX/Envoy → Kafka/NATS stream)
   to confirm it handles 500k RPS with sub-50ms ACK latency. Use a load
   generator (vegeta, wrk, k6) against the `/v1/wal/push` endpoint.
2. **Verify client-side buffering.** The Arkilian client buffers up to
   `ARKILIAN_MAX_QUEUE_DEPTH` (default 100,000) rows in `_pending_backup`
   when the backend returns 429/503. Local database writes are never
   blocked — capture pauses at the cap, but the application keeps running
   (spec §0). This is verified by `test_dst_backpressure.c`.
3. **Monitor for sustained backlog.** Alert when `db_backup_queue_depth`
   grows across consecutive checks (see §9) — that's the signal the
   backend is rejecting/ timing out, not a transient blip.
4. **Micro-batching is the forward path.** The current one-row-per-POST
   design is intentional for strict ordering (spec §8.1) but is the
   ceiling on ingestion efficiency. When the control plane supports
   batch payloads, client-side micro-batching (100 rows or 50ms window,
   gzip/zstd) reduces RPS by ~100× — see Risk #2 in the production
   readiness review.
