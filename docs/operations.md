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
| Health | `db_backup_is_healthy()` | Returns 0 — thread dead OR queue above `ARKILIAN_MAX_QUEUE_DEPTH` (default 100,000) |

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

A row lands in `_dead_backup` after `MAX_ATTEMPTS` (10) failed deliveries.
**Every dead-lettered row is customer data that did not reach the backup
destination.**

### Investigate first

```sh
# how many, and which rows are stuck
arkilian-dlq app.sqlite --count
arkilian-dlq app.sqlite --list --limit 50
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
# preview
arkilian-dlq app.sqlite --replay --dry-run
# re-queue everything (or a single row with --id N)
arkilian-dlq app.sqlite --replay
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

Check `ARKILIAN_SIGNED_URL_ENDPOINT` is set (it is independent of
`ARKILIAN_WAL_PUSH_URL`) and the control plane's `/v1/upload/request`
returns 200 with an `upload_url`. Verify an object exists in the bucket.

### "Backup enabled but nothing ships"

`db_backup_is_enabled()` returns 1 but `ARKILIAN_WAL_PUSH_URL` is empty —
the startup log line warns:

```
arkilian: [warn] backup is enabled (ARKILIAN_ENABLE_BACKUP) but
ARKILIAN_WAL_PUSH_URL is not set — rows will accumulate in _pending_backup and never ship
```

Set the URL and restart. Rows captured while misconfigured ship once the
destination is configured.

## 6. Configuration quick reference

| Variable | Purpose |
|----------|---------|
| `ARKILIAN_WAL_PUSH_URL` | Realtime destination for row changes (`/v1/wal/push`) |
| `ARKILIAN_SIGNED_URL_ENDPOINT` | Signed-URL issuer for hourly snapshots (`/v1/upload/request`) |
| `ARKILIAN_DATABASE_TOKEN` | Bearer token for both endpoints |
| `ARKILIAN_ENABLE_BACKUP` | `1/0/true/false`; runtime toggle via `db_backup_set_enabled` |
| `ARKILIAN_BACKUP_INTERVAL` | Hourly snapshot interval (min 1 s) |
| `ARKILIAN_MAX_QUEUE_DEPTH` | Queue ceiling for `db_backup_is_healthy()` (default 100000) |
| `ARKILIAN_DB_PATH` / `ARKILIAN_BACKUP_PATH` | Database and snapshot file paths |

Real environment variables always win over a `./.env` file.

## 7. Design decisions (reviewed, spec §8.1 / §11)

- **Payload format is replayable SQL text, not JSON.** The spec's §6 JSON
  sketch (`{id, table, op, data}`) was a suggestion; the shipped format is
  the SQL statement itself (`REPLACE INTO "t" (...) VALUES (...)` /
  `DELETE FROM "t" WHERE rowid = ...`), which any SQLite can apply
  directly. One format, one destination. The deduplication key is the
  `X-Arkilian-Payload-Id` header, not an `id` field in the body — the
  destination MUST dedupe on that header (the bundled control plane
  does).
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
