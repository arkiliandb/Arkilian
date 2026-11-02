# Changelog

## 1.6.2 — manifest publish fix

### Fixed

- **A restart could freeze shipping for good with "manifest HMAC MISMATCH".**
  The manifest was published as two objects, `manifest.json` then
  `manifest.sig`. A process killed between the two PUTs (a platform SIGKILL
  during the shutdown drain, an OOM kill) left the new manifest next to the
  previous signature. Every later start refused the prefix as tampered and
  froze shipping and publishing, and restores of that prefix failed. The only
  way out was `ARKILIAN_MANIFEST_REBASE=1`. The signature is now embedded in
  `manifest.json`, so one PUT publishes both atomically; `manifest.sig` is
  still written for older readers. Covered by `tests/test_manifest_torn.c`.
- A writer frozen on an unreadable remote manifest re-reads it every
  `ARKILIAN_MANIFEST_RECHECK_SEC` (default 60 s) and resumes shipping once it
  verifies, instead of staying frozen until a restart.

### Upgrade notes

- Readers of 1.6.2 accept manifests from older writers. Readers older than
  1.6.2 accept manifests from 1.6.2 writers through `manifest.sig`.
- A prefix already torn by an older writer still has a mismatched signature.
  Start the writer once with `ARKILIAN_MANIFEST_REBASE=1` (after confirming
  no other writer uses the prefix), wait for the next snapshot to publish,
  then remove the variable.

## 1.6.1 — DDL capture fix

### Fixed

- **DDL through `prepare`/`step` (Node `run()`) could commit without being
  captured.** When the connection still had a statement open and another
  connection (the flush thread, a second process) committed in between, the
  trigger resync and the outbox insert after the DDL both failed with
  `database is locked`: the replica never got the schema change and later rows
  of that table were captured with the old column list. DDL through `db_step`
  now runs with its capture in one savepoint, as `db_exec` already did, so the
  capture holds the write lock and cannot be refused. A DDL that cannot get the
  write lock fails as a whole and can be retried. If a resync fails for any
  other reason a capture gap is recorded (health RED, snapshot rebase) instead
  of logging and carrying on. Covered by `tests/test_ddl_concurrency.c`.
- DDL prepared through the wrapper no longer logs the raw-handle "schema change
  bypassed the backup wrapper" warning (the authorizer runs at prepare time),
  and capturing DDL no longer changes the application's `last_insert_rowid`.
- Node `all(sql, params, maxRows)` returned an error ("another row available")
  whenever the result had more rows than `maxRows`; it now returns the first
  `maxRows` rows.

## 1.6.0 — replication correctness release

Every fix below closes a defect that was reproduced against 1.5.0 and is now
covered by `tests/test_p0_replication.c` (or an updated existing suite).

### Fixed — data correctness (P0)

- **Restore → keep writing → restore again failed.** Hydration reset the outbox
  sequence, so a restored node restarted LSNs at 1 and published an overlapping
  manifest that every later restore rejected, while health stayed green. The
  restored node now continues the prefix's LSN sequence (verified across four
  failover generations, including a snapshot taken on a restored node, which
  previously also failed with a stale `last_applied_lsn`).
- **An UPDATE that changed a PRIMARY KEY left a ghost row** in the replica
  (INTEGER, TEXT and composite WITHOUT ROWID keys). The update trigger now ships
  a `DELETE` of the old key before the `REPLACE` of the new one.
- **DDL near a snapshot made the restore fail.** The snapshot baseline sat below
  the image's real position, so `CREATE TABLE` / `ALTER TABLE` / `CREATE INDEX`
  were replayed onto a schema that already had them. Snapshots now record their
  exact LSN inside the copy's read transaction, chunk statements carry LSN tags,
  hydration skips exactly the statements the image contains, and wrapped DDL
  commits atomically with its capture row.
- **A full outbox silently dropped part of a transaction.** Overflow now writes
  a durable capture-gap marker in the same transaction; health turns RED
  immediately and no manifest is published until a snapshot rebases. New strict
  mode `ARKILIAN_CAPTURE_OVERFLOW=abort` fails the write instead.

### Fixed — reliability

- **Split-brain guard:** a node whose LSN head is behind the remote manifest
  (a second writer, or a node not restored from the prefix) freezes shipping and
  publishing with a clear error instead of interleaving histories. Chunk and
  snapshot keys carry a random per-process writer id, so writers can never
  overwrite each other's objects. `ARKILIAN_MANIFEST_REBASE=1` is the explicit
  operator override.
- **Snapshots use immutable per-cycle keys**
  (`{prefix}/snapshots/<lsn>_<writer>.backup.sqlite`). Overwriting a fixed
  `backup.sqlite` broke the previous manifest whenever a new publish failed.
- **Snapshot staging is unique per cycle.** Instances sharing the default
  `./backup.sqlite` staging path overwrote each other's image mid-upload.
- **Clean shutdown publishes its tail.** Final uploads were aborted by the
  shutdown flag (`CURLE_ABORTED_BY_CALLBACK`), so the last chunks never reached
  the manifest. `db_close()` now drains the whole outbox in full chunks within
  `ARKILIAN_SHUTDOWN_DRAIN_SEC` (default 10 s) and publishes immediately
  (found by the end-to-end benchmark: a large final backlog was cut short).
- The outbox cap could be exceeded under load in an earlier 1.6 build: the
  flush thread cleared the outbox-full flag at 90% while the guard raised it
  lower, so it could undo a fresh raise. The clear point is now derived from
  the guard's headroom (capture was never lost; only the cap overshot).
- Shipping waits for the startup manifest read, so adopted and new chunk
  records stay in LSN order; an endpoint refused by the SSRF guard is reported
  at startup.
- The startup snapshot is retried on a short backoff until it publishes,
  instead of waiting a full `ARKILIAN_BACKUP_INTERVAL`.

### Performance

Measured with `bench/bench_cmp.c` (raw SQLite vs Arkilian, prepared
statements, identical pragmas, median of 3). On a MacBook (i7-9750H),
default `synchronous=FULL`:

| Workload | SQLite | Arkilian 1.6.0 | Slower by |
| --- | --- | --- | --- |
| INSERT, one row per transaction | 7,080/s | 5,262/s | 1.35x |
| UPDATE, one row per transaction | 18,202/s | 12,017/s | 1.5x |
| INSERT, 1000 rows per transaction | 554,109 rows/s | 63,927 rows/s | 8.7x |
| Point SELECT | 233,467/s | 221,128/s | 1.06x |

On a Linux VM against 1.5.0 (`synchronous=NORMAL`): single-row INSERT went
from 7.3x to 3.2x slower than SQLite, UPDATE from 4.6x to 2.5x, 1000-row
transactions from 57x to 12.5x. On the MacBook with `synchronous=NORMAL`:
INSERT 2.8x, UPDATE 2.1x, 1000-row transactions 9.8x slower than SQLite.

- The flush thread is no longer woken on every changed row (the update hook
  fired per row, before commit, so the thread spun on empty drains in
  lock-step with the writer). Wake-ups are coalesced to at most one per
  100 ms and the thread polls every 100 ms while writes flow.
- The outbox cap is no longer measured on every captured row. A guard
  statement at the end of each capture trigger checks occupancy on a small
  fraction of rows with enough headroom that the cap is never exceeded
  (proved by a mixed-workload test that checks after every operation);
  capture triggers test a single flag row. Overflow still records a capture
  gap in the same transaction, or fails the write in strict mode.
- Write cost no longer grows with the unshipped backlog (1.5.0 ran a
  `COUNT(*)` over the outbox per row: about 10x slower at 90k pending rows).
- The `created_at` outbox index is dropped from the capture hot path.
- Remaining overhead is inherent to trigger-based row capture: every captured
  row also writes an outbox row with its replay SQL.

### Build, tooling and docs

- `bench/bench_cmp.c`: fair local benchmark against raw SQLite (prepared
  statements, identical pragmas, exact latency percentiles).
- `bench/minio/run.sh`: one-command end-to-end benchmark against a local MinIO
  container: live-shipping throughput, drain time, snapshot, shutdown
  publish, restore with byte-level verification, and a storage outage.
- A data race in the `test_kill_resilience` mock server (read while its
  thread was still recording) is fixed.
- `src/class.c` compiles warning-free under GCC and Clang at `-O2` and `-O3`
  with `-Werror` (the Node addon builds with `-O3 -Werror`).

- The documented default build works on a fresh clone (the example target
  pointed at a missing `examples/main.c`).
- README guarantees rewritten to match behavior, including the recovery-point
  objective. `BENCHMARK.md` figures are marked superseded pending a GCE + GCS
  benchmark.

### Upgrade notes

- New chunk and snapshot object keys; manifests written by 1.5.0 remain
  readable and restorable.
- A node that starts against a prefix whose manifest is ahead of its local
  database now refuses to ship. Restore it with `arkilian_hydrate_s3()` first,
  use a different `ARKILIAN_S3_PREFIX`, or set `ARKILIAN_MANIFEST_REBASE=1` if
  this database is knowingly the authority.
- Deployments that were restored under 1.5.0 and kept writing have a reset LSN
  sequence and will hit that guard after upgrading: their remote history was
  already unrestorable. Set `ARKILIAN_MANIFEST_REBASE=1` once on the live writer;
  the next snapshot republishes a clean restore point.
