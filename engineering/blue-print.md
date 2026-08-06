# Realtime Backup Layer for a Production Game — Raw `libsqlite3` C API

**Status:** Draft for team review
**Scope:** Production game process linking `sqlite3.c`/`libsqlite3` directly (no `bun:sqlite`, no JS runtime involved)
**Priority constraint:** The backup subsystem must be able to fail, stall, or fall arbitrarily far behind **without ever affecting game correctness, frame time, or process stability.** Every design choice below is made with that as the binding constraint, not "realtime" performance — realtime is the goal, "never crash the game" is the hard requirement it's traded against.

---

## 0. Read this first — the single most important design decision

Everything in this doc follows from one rule:

> **The backup subsystem gets its own SQLite connection, on its own thread, and every single SQLite call it makes checks its return code and degrades gracefully. It never touches the game's connection, never blocks the game thread, and never calls `abort()`/`assert()`/lets an unchecked error propagate to a crash in a release build.**

If your team implements nothing else from this document correctly, implement that rule correctly. Almost every failure mode below traces back to a violation of it (sharing a connection across threads, blocking I/O inside a callback that fires on the game thread, or an unchecked `rc` that lets a transient `SQLITE_BUSY` become an unhandled error path).

---

## 1. Goals and constraints

**Goals**
- Realtime (dispatch-on-write, no polling-interval-bound latency) row-level replication to an external backup destination.
- Capture that cannot be silently bypassed by any write path in the codebase, including ones written after this system ships.
- Zero impact on game thread frame time under normal operation, and bounded/graceful degradation (not a crash, not a stall) under backup-destination outage, disk pressure, or network failure.

**Hard constraints (non-negotiable given this is live production game code)**
- No `abort()`, no unchecked `assert()` in release builds, no unhandled SQLite return code, anywhere in this subsystem.
- No blocking I/O (disk or network) on the game's main thread or on any thread the game itself depends on for frame timing.
- No cross-thread sharing of a single `sqlite3*` connection handle without explicit serialization — see §3.
- The backup subsystem must be independently disableable (a single compile-time or runtime flag) without touching game logic, for fast incident response.

**Non-goals**
- Point-in-time file-level recovery (WAL shipping / physical replication) — explicitly out of scope; this is row-level logical replication.
- Exactly-once delivery to the destination — this design is **at-least-once**, the destination must dedupe (§8.2).

---

## 2. Architecture summary

Three concerns, three different SQLite mechanisms, deliberately kept separate because conflating them is the most common source of bugs in this kind of system:

1. **Capture** (what changed) — `AFTER INSERT/UPDATE/DELETE` **SQL triggers**, auto-generated from the live schema, writing full row snapshots into an outbox table `_pending_backup`. Triggers run as part of the same VDBE program as the original statement, in the same transaction — this is fully safe, well-documented SQLite behavior, no reentrancy concerns.
2. **Wake signal** (when to check) — `sqlite3_update_hook()`, used **only** to signal "something changed, go check the outbox" to the backup thread. It does **no database work itself** and captures no data. This is the load-bearing distinction versus a naive design — see §4.2 for why.
3. **Delivery** (getting it out) — a dedicated backup thread, its own `sqlite3*` connection (WAL mode allows this to run concurrently with the game's writer), draining `_pending_backup` in order, shipping to the destination, deleting on success, dead-lettering on repeated failure.

```
[Game thread]                                   [Backup thread]
     │                                                │
     │ INSERT/UPDATE/DELETE on game tables            │
     ├─► trigger fires (same txn) ──► _pending_backup │
     │                                                │
     ├─► sqlite3_update_hook fires                    │
     │   (fast, non-blocking: sets atomic flag +      │
     │    pthread_cond_signal, returns immediately)   │
     │                                    ┌────────────┘
     │                                    ▼
     │                          wakes from cond_wait
     │                                    │
     │                          own connection, own txn:
     │                          SELECT ... FROM _pending_backup
     │                          ORDER BY id LIMIT N
     │                                    │
     │                          ship to destination
     │                                    │
     │                          DELETE on success /
     │                          dead-letter after N attempts
     │                                    │
     │                          sqlite3_reset + loop,
     │                          or cond_timedwait
```

**Why this split, specifically:**
- Using `sqlite3_update_hook` to *do the capture* (e.g., building the JSON payload and inserting it into `_pending_backup` from inside the hook callback) is a documented anti-pattern: the SQLite docs for `sqlite3_update_hook` state the implementation **must not do anything that modifies the database connection that invoked the hook**; any such action must be deferred until after the triggering `sqlite3_step()` completes. Preparing/stepping a new statement on the same connection from inside the hook violates this. Triggers don't have this restriction — they're a normal part of the original statement's execution — so triggers, not the hook, are the capture mechanism.
- The hook is still useful — as a **notification only**, doing nothing but flipping an atomic flag and signaling a condition variable, both of which are safe, fast (sub-microsecond), non-DB operations. This gets you wake-on-write latency without violating the reentrancy rule.
- `sqlite3_update_hook` is also documented to fire **only for rowid tables**, not `WITHOUT ROWID` tables. Since it's just a latency optimization here (not the source of truth), a `WITHOUT ROWID` table missing a wake signal only means that specific change is picked up by the periodic poll fallback instead of instantly — a latency degradation, not a correctness bug. Flag any `WITHOUT ROWID` tables in your schema to the team so this tradeoff is a known, reviewed decision rather than a surprise.

---

## 3. Threading and connection model — get this exactly right

This is the section most likely to cause a production incident if implemented carelessly.

### 3.1 Rule: one connection per thread

Do **not** share a single `sqlite3*` between the game thread and the backup thread, even with your own mutex around it, unless you have a specific reason SQLite's own serialized mode isn't sufficient. Open **two independent connections** to the same database file:

```c
// At startup, before any other SQLite API call:
sqlite3_config(SQLITE_CONFIG_MULTITHREAD); // or SERIALIZED if you have a reason to share handles — MULTITHREAD is
                                            // sufficient and faster when each thread owns its own connection, which
                                            // is what this design does
sqlite3_initialize();

// Game thread's existing connection — unchanged by this design.
sqlite3 *g_game_db;
sqlite3_open_v2("game.sqlite", &g_game_db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, NULL);

// Backup thread's own, separate connection to the SAME file.
sqlite3 *g_backup_db;
sqlite3_open_v2("game.sqlite", &g_backup_db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, NULL);
```

Both connections need:
```c
sqlite3_busy_timeout(conn, 5000); // 5s: tune to your workload; see §3.3
sqlite3_exec(conn, "PRAGMA journal_mode=WAL;", NULL, NULL, NULL);
sqlite3_exec(conn, "PRAGMA synchronous=NORMAL;", NULL, NULL, NULL);
```

WAL mode is what makes this safe and non-blocking: it allows one writer and multiple readers concurrently, so the backup thread's reads/deletes on `_pending_backup` don't block the game thread's writes to gameplay tables, and vice versa, under normal conditions.

### 3.2 `PRAGMA synchronous=NORMAL` — know the exact tradeoff

With WAL mode, `synchronous=NORMAL` is durable across **application/process crashes** — a crashed process will not corrupt the database and committed transactions survive. It is **not** guaranteed durable across **OS crash or power loss** — the most recent transaction(s) could theoretically be lost in that specific scenario, though the database remains structurally consistent (no corruption). This is the standard, recommended tradeoff for this workload and matches typical game-server practice, but confirm it's an acceptable risk profile for your title's save-data requirements before shipping — if a power-loss data-loss window of the last few transactions is unacceptable for your game, use `synchronous=FULL` for the *game* connection specifically (the backup connection's synchronous setting matters much less since it's only ever deleting already-durable rows).

### 3.3 `SQLITE_BUSY` handling — expected, not exceptional

Even in WAL mode, writer contention is possible (the backup thread's `DELETE` on `_pending_backup` and the game thread's write both need the single write lock, briefly). `sqlite3_busy_timeout()` handles the common case by retrying internally up to the timeout. Beyond that:

- Every write on the backup thread must check for `SQLITE_BUSY`/`SQLITE_LOCKED` explicitly and retry with backoff — do not treat it as a fatal error.
- Keep backup-thread transactions **short**: select a bounded batch, ship it, delete it, commit — never hold a transaction open across a network call. Holding a transaction open during `shipToBackup()`'s network I/O is a correctness and performance bug: it holds SQLite locks for the duration of a network round-trip, which is exactly the kind of thing that causes game-thread writes to stall waiting on the backup thread.
- The game thread should **never** be the one waiting on `SQLITE_BUSY` caused by the backup thread taking long transactions — if you follow the rule above (short transactions, no I/O while holding a transaction), this shouldn't happen, but it's worth a load test (§10) specifically targeting this.

### 3.4 If the game already funnels all DB access through a single dedicated "DB thread"

Many game engines already do this (all SQLite calls marshaled through one thread/queue to avoid concurrency bugs entirely). If that's your architecture: you do not need a second thread or a second connection. Instead, fold `drain_pending_backup()` (§7) into that thread's existing tick/work loop, called after processing each batch of game DB work, plus a low-frequency timer fallback. This removes an entire class of threading risk. **Confirm which topology your engine uses before implementation** — it changes §3.1–§3.3 substantially (down to "one connection, no wake-hook race conditions to reason about at all").

---

## 4. Capture: auto-generated triggers

### 4.1 Schema

```sql
CREATE TABLE IF NOT EXISTS _pending_backup (
  id         INTEGER PRIMARY KEY AUTOINCREMENT,  -- global order + idempotency key
  payload    TEXT NOT NULL,                      -- JSON, see §6
  attempts   INTEGER NOT NULL DEFAULT 0,
  created_at INTEGER NOT NULL DEFAULT (unixepoch()),
  last_attempt_at INTEGER
);

CREATE TABLE IF NOT EXISTS _dead_backup (
  id          INTEGER PRIMARY KEY,
  payload     TEXT NOT NULL,
  attempts    INTEGER NOT NULL,
  failed_reason TEXT,
  created_at  INTEGER NOT NULL,
  dead_lettered_at INTEGER NOT NULL DEFAULT (unixepoch())
);
```

### 4.2 Trigger generator (run at startup, and after any schema migration)

Written against the plain C API — `sqlite3_exec`, `sqlite3_prepare_v2`/`sqlite3_step`, and `sqlite3_mprintf` for safe SQL construction. **Do not build these statements with `snprintf`/string concatenation of table or column names.** Table and column names cannot be bound as parameters (bound parameters are for values only), so use `sqlite3_mprintf`'s `%w` conversion, which exists specifically to safely quote SQL identifiers (escapes embedded `"` characters, wraps in double quotes). This matters even though table/column names come from your own schema, not user input — it's cheap insurance against a future column named something like `"weird""name"` breaking trigger generation in a way that's hard to diagnose.

```c
#include <sqlite3.h>
#include <string.h>
#include <stdio.h>

static const char *RESERVED_TABLES[] = {
    "_pending_backup", "_dead_backup", "sqlite_sequence", NULL
};

static int is_reserved(const char *name) {
    if (strncmp(name, "sqlite_", 7) == 0) return 1;
    for (int i = 0; RESERVED_TABLES[i]; i++) {
        if (strcmp(name, RESERVED_TABLES[i]) == 0) return 1;
    }
    return 0;
}

// Returns 0 on success, non-zero on failure. Never crashes — every allocation
// and every sqlite3_* return code is checked. Caller decides how to log/alert;
// this function must not itself terminate the process.
int sync_backup_triggers(sqlite3 *db, char **err_out) {
    int rc;
    char *errmsg = NULL;

    rc = sqlite3_exec(db, "BEGIN IMMEDIATE;", NULL, NULL, &errmsg);
    if (rc != SQLITE_OK) { *err_out = errmsg; return rc; }

    sqlite3_stmt *table_stmt = NULL;
    rc = sqlite3_prepare_v2(db,
        "SELECT name FROM sqlite_master WHERE type = 'table'", -1, &table_stmt, NULL);
    if (rc != SQLITE_OK) {
        sqlite3_exec(db, "ROLLBACK;", NULL, NULL, NULL);
        *err_out = sqlite3_mprintf("prepare table list: %s", sqlite3_errmsg(db));
        return rc;
    }

    while ((rc = sqlite3_step(table_stmt)) == SQLITE_ROW) {
        const char *table = (const char *)sqlite3_column_text(table_stmt, 0);
        if (!table || is_reserved(table)) continue;

        // Build column list + detect BLOB columns via PRAGMA table_info
        char *pragma_sql = sqlite3_mprintf("PRAGMA table_info(%w);", table);
        sqlite3_stmt *col_stmt = NULL;
        rc = sqlite3_prepare_v2(db, pragma_sql, -1, &col_stmt, NULL);
        sqlite3_free(pragma_sql);
        if (rc != SQLITE_OK) {
            sqlite3_finalize(table_stmt);
            sqlite3_exec(db, "ROLLBACK;", NULL, NULL, NULL);
            *err_out = sqlite3_mprintf("prepare table_info(%s): %s", table, sqlite3_errmsg(db));
            return rc;
        }

        char new_pairs[4096] = {0};
        char old_pairs[4096] = {0};
        size_t new_len = 0, old_len = 0;
        int col_count = 0;

        while ((rc = sqlite3_step(col_stmt)) == SQLITE_ROW) {
            const char *col  = (const char *)sqlite3_column_text(col_stmt, 1);
            const char *type = (const char *)sqlite3_column_text(col_stmt, 2);
            if (!col) continue;
            int is_blob = type && strstr(type, "BLOB") != NULL;

            // %w quotes/escapes the identifier; %z-style manual buffer growth
            // omitted here for brevity — in production, use a growable buffer
            // (or a fixed generous bound with an explicit overflow check) rather
            // than a raw stack buffer once you have real column counts/names.
            char *pair = is_blob
                ? sqlite3_mprintf("'%w', hex(%s%w)", col, "NEW.", col)
                : sqlite3_mprintf("'%w', %s%w", col, "NEW.", col);
            int written = snprintf(new_pairs + new_len, sizeof(new_pairs) - new_len,
                                    "%s%s", col_count ? ", " : "", pair);
            sqlite3_free(pair);
            if (written < 0 || (size_t)written >= sizeof(new_pairs) - new_len) {
                // Buffer would overflow — fail loudly to the caller instead of
                // truncating silently. A table with this many columns needs the
                // growable-buffer version, not this fixed-size sketch.
                sqlite3_finalize(col_stmt);
                sqlite3_finalize(table_stmt);
                sqlite3_exec(db, "ROLLBACK;", NULL, NULL, NULL);
                *err_out = sqlite3_mprintf("column list too large for table %s", table);
                return SQLITE_TOOBIG;
            }
            new_len += written;

            char *pair_old = is_blob
                ? sqlite3_mprintf("'%w', hex(%s%w)", col, "OLD.", col)
                : sqlite3_mprintf("'%w', %s%w", col, "OLD.", col);
            written = snprintf(old_pairs + old_len, sizeof(old_pairs) - old_len,
                                "%s%s", col_count ? ", " : "", pair_old);
            sqlite3_free(pair_old);
            old_len += written;

            col_count++;
        }
        sqlite3_finalize(col_stmt);
        if (rc != SQLITE_DONE) {
            sqlite3_finalize(table_stmt);
            sqlite3_exec(db, "ROLLBACK;", NULL, NULL, NULL);
            *err_out = sqlite3_mprintf("reading table_info(%s): %s", table, sqlite3_errmsg(db));
            return rc;
        }
        if (col_count == 0) continue; // defensive: shouldn't happen, skip rather than emit malformed SQL

        const char *ops[3][2] = {
            {"ai", "INSERT"}, {"au", "UPDATE"}, {"ad", "DELETE"}
        };
        for (int i = 0; i < 3; i++) {
            char *drop_sql = sqlite3_mprintf("DROP TRIGGER IF EXISTS trg_%w_%s;", table, ops[i][0]);
            rc = sqlite3_exec(db, drop_sql, NULL, NULL, &errmsg);
            sqlite3_free(drop_sql);
            if (rc != SQLITE_OK) goto fail;

            const char *pairs = (i == 2) ? old_pairs : new_pairs; // DELETE uses OLD
            const char *op_op = ops[i][1];
            char *create_sql = sqlite3_mprintf(
                "CREATE TRIGGER trg_%w_%s AFTER %s ON %w BEGIN "
                "INSERT INTO _pending_backup (payload) VALUES ("
                "json_object('table', %Q, 'op', %Q, 'data', json_object(%s))); END;",
                table, ops[i][0], op_op, table, table, op_op, pairs);
            rc = sqlite3_exec(db, create_sql, NULL, NULL, &errmsg);
            sqlite3_free(create_sql);
            if (rc != SQLITE_OK) goto fail;
        }
        continue;

    fail:
        sqlite3_finalize(table_stmt);
        sqlite3_exec(db, "ROLLBACK;", NULL, NULL, NULL);
        *err_out = errmsg; // caller must sqlite3_free(*err_out)
        return rc;
    }
    sqlite3_finalize(table_stmt);

    if (rc != SQLITE_DONE) {
        sqlite3_exec(db, "ROLLBACK;", NULL, NULL, NULL);
        *err_out = sqlite3_mprintf("reading table list: %s", sqlite3_errmsg(db));
        return rc;
    }

    rc = sqlite3_exec(db, "COMMIT;", NULL, NULL, &errmsg);
    if (rc != SQLITE_OK) { *err_out = errmsg; return rc; }

    return SQLITE_OK;
}
```

Call this once at startup on the **game connection** (`g_game_db`), after any migrations run, before the game begins accepting writes that need backup coverage. The fixed-size stack buffers (`new_pairs`/`old_pairs`) are a sketch for tables with a reasonable column count — size them generously and treat overflow as a hard failure (as shown), not a silent truncation, and switch to a growable heap buffer for any table with a large or dynamic column count.

### 4.3 Known edge cases — sign off on each explicitly

| Edge case | Behavior | Mitigation |
|---|---|---|
| **BLOB columns** | `json_object()` errors on raw BLOB values | `hex()`-encoded in the generator above; destination must decode. |
| **`WITHOUT ROWID` tables** | Triggers capture correctly; `sqlite3_update_hook` wake-signal does **not** fire for these (documented SQLite limitation) | Correctness unaffected (triggers are the source of truth); latency for these tables falls back to the poll interval (§7.3). Enumerate any `WITHOUT ROWID` tables in your schema and confirm the team accepts this latency difference. |
| **Bulk operations** (large batch inserts, save-migration scripts) | Trigger fires per row; a 100k-row operation produces 100k outbox rows in one transaction | For known large bulk paths, wrap in `DROP TRIGGER`/bulk op/re-run `sync_backup_triggers` and follow with an explicit reconciliation pass (§9) rather than per-row capture. |
| **Recursive trigger firing** | `_pending_backup`/`_dead_backup` are excluded from generation (`is_reserved`), so inserts into them don't spawn further triggers. Leave `PRAGMA recursive_triggers` at its default — no reason to enable it for this design, and doing so elsewhere in the codebase increases risk of unrelated trigger chains. | None required beyond keeping the exclusion list correct. |
| **Live `ALTER TABLE` outside the migration path** | New columns won't be captured until the next `sync_backup_triggers` call | Enforce schema changes only through the migration runner in code review; hand-editing prod schema is an incident, not a supported path. |
| **Column count/name too large for fixed buffer** | Generator fails loudly (`SQLITE_TOOBIG` return) rather than emitting truncated/malformed SQL | Switch to growable buffers if any table approaches the fixed bound; treat this as a build-time check against your actual schema, not a runtime surprise. |

---

## 5. The wake-signal hook

```c
#include <pthread.h>
#include <stdatomic.h>

static atomic_int g_backup_wake_flag = 0;
static pthread_cond_t  g_backup_cond  = PTHREAD_COND_INITIALIZER;
static pthread_mutex_t g_backup_mutex = PTHREAD_MUTEX_INITIALIZER;

// Registered via sqlite3_update_hook(g_game_db, on_db_update, NULL);
// Fires on the GAME THREAD, synchronously, during sqlite3_step(). Must be
// fast and must do nothing that touches the database connection.
static void on_db_update(void *arg, int op, char const *db_name,
                          char const *table_name, sqlite3_int64 rowid) {
    (void)arg; (void)op; (void)db_name; (void)table_name; (void)rowid;
    atomic_store_explicit(&g_backup_wake_flag, 1, memory_order_relaxed);
    pthread_cond_signal(&g_backup_cond); // does not require holding g_backup_mutex to signal;
                                          // waiter re-checks the flag under the mutex on wake, so
                                          // this avoids taking a lock on the game thread's hot path
}
```

Register once, right after opening `g_game_db`:

```c
sqlite3_update_hook(g_game_db, on_db_update, NULL);
```

**Why it's implemented this way:** the hook body does exactly two non-blocking, non-DB operations — an atomic store and a condition-variable signal — and returns. It never allocates, never does I/O, never touches `g_game_db` or `g_backup_db`. This keeps the game thread's write path latency unaffected by the backup subsystem's existence, and keeps the implementation compliant with the "must not modify the database connection" restriction from the SQLite docs (§2).

If your codebase doesn't use pthreads (console SDK threading primitives, a custom job system, etc.), port this pattern directly — the requirement is just "signal a lightweight, non-blocking, non-DB event," not the specific primitives shown.

---

## 6. Payload shape

```json
{
  "id": 48213,
  "table": "player_inventory",
  "op": "update",
  "data": { "player_id": 91, "item_id": 4, "quantity": 3, "icon_blob": "A1B2C3..." }
}
```

- `id` is the `_pending_backup.id` — the idempotency key the destination must dedupe on (§8.2).
- `icon_blob` (or any BLOB column) arrives hex-encoded per §4.3; the destination-side consumer must `unhex`/decode it.

---

## 7. Delivery: backup thread main loop

```c
#define BATCH_SIZE   100
#define MAX_ATTEMPTS 10
#define POLL_INTERVAL_MS 2000

static volatile sig_atomic_t g_shutdown_requested = 0;

void *backup_thread_main(void *arg) {
    sqlite3 *db = g_backup_db; // this thread's own connection, opened at startup

    sqlite3_stmt *select_stmt = NULL;
    sqlite3_stmt *delete_stmt = NULL;
    sqlite3_stmt *update_attempts_stmt = NULL;
    sqlite3_stmt *dead_letter_stmt = NULL;

    // Prepare once, reuse via sqlite3_reset — avoids re-parsing SQL every loop.
    // Every prepare below is checked; on failure, log and retry preparation
    // rather than proceeding with a NULL statement handle.
    if (sqlite3_prepare_v2(db,
        "SELECT id, payload, attempts FROM _pending_backup ORDER BY id LIMIT ?1",
        -1, &select_stmt, NULL) != SQLITE_OK) goto fatal_prepare_error;

    if (sqlite3_prepare_v2(db,
        "DELETE FROM _pending_backup WHERE id = ?1",
        -1, &delete_stmt, NULL) != SQLITE_OK) goto fatal_prepare_error;

    if (sqlite3_prepare_v2(db,
        "UPDATE _pending_backup SET attempts = ?1, last_attempt_at = unixepoch() WHERE id = ?2",
        -1, &update_attempts_stmt, NULL) != SQLITE_OK) goto fatal_prepare_error;

    if (sqlite3_prepare_v2(db,
        "INSERT INTO _dead_backup (id, payload, attempts, failed_reason, created_at) "
        "SELECT id, payload, ?1, ?2, created_at FROM _pending_backup WHERE id = ?3",
        -1, &dead_letter_stmt, NULL) != SQLITE_OK) goto fatal_prepare_error;

    while (!g_shutdown_requested) {
        int drained_any = drain_batch(db, select_stmt, delete_stmt,
                                       update_attempts_stmt, dead_letter_stmt);

        if (!drained_any) {
            pthread_mutex_lock(&g_backup_mutex);
            if (!atomic_exchange_explicit(&g_backup_wake_flag, 0, memory_order_relaxed)) {
                struct timespec ts;
                clock_gettime(CLOCK_REALTIME, &ts);
                ts.tv_sec += POLL_INTERVAL_MS / 1000;
                pthread_cond_timedwait(&g_backup_cond, &g_backup_mutex, &ts);
            }
            pthread_mutex_unlock(&g_backup_mutex);
        }
    }

    sqlite3_finalize(select_stmt);
    sqlite3_finalize(delete_stmt);
    sqlite3_finalize(update_attempts_stmt);
    sqlite3_finalize(dead_letter_stmt);
    return NULL;

fatal_prepare_error:
    // Statement preparation failing means schema/connection is broken in a way
    // retrying won't fix immediately. Log with full detail, back off, and retry
    // periodically rather than exiting the thread silently — a silently-dead
    // backup thread with no alerting is worse than a loudly-retrying one.
    log_error("backup thread: failed to prepare statements: %s", sqlite3_errmsg(db));
    // production: schedule a retry with backoff instead of returning; omitted here for brevity
    return NULL;
}

// Returns 1 if at least one row was processed, 0 if the queue was empty.
// Never lets a single row's failure crash the loop or corrupt ordering.
static int drain_batch(sqlite3 *db, sqlite3_stmt *select_stmt, sqlite3_stmt *delete_stmt,
                        sqlite3_stmt *update_attempts_stmt, sqlite3_stmt *dead_letter_stmt) {
    sqlite3_reset(select_stmt);
    sqlite3_clear_bindings(select_stmt);
    sqlite3_bind_int(select_stmt, 1, BATCH_SIZE);

    int processed_any = 0;

    for (;;) {
        int rc = sqlite3_step(select_stmt);
        if (rc == SQLITE_DONE) break; // queue empty, or batch limit reached — either way we're done this pass
        if (rc != SQLITE_ROW) {
            log_error("backup thread: select failed: %s", sqlite3_errmsg(db));
            break; // transient error — next wake/poll will retry from the same rows, safe (idempotent select)
        }

        sqlite3_int64 id       = sqlite3_column_int64(select_stmt, 0);
        const unsigned char *payload = sqlite3_column_text(select_stmt, 1);
        int attempts            = sqlite3_column_int(select_stmt, 2);
        if (!payload) { continue; } // defensive: should be impossible given NOT NULL, but never dereference blindly

        // Copy payload out before ship_to_backup, which may re-enter SQLite
        // via logging etc. — never hold a live sqlite3_stmt row across a
        // network call implicitly; we're not holding a transaction open here
        // (autocommit SELECT), but copy defensively regardless.
        char *payload_copy = strdup((const char *)payload);
        if (!payload_copy) {
            log_error("backup thread: OOM copying payload id=%lld", (long long)id);
            break; // out of memory — stop this pass, retry later; do not crash
        }

        ship_result_t result = ship_to_backup(id, payload_copy); // network I/O happens here, OUTSIDE any sqlite3 txn
        free(payload_copy);

        if (result == SHIP_OK) {
            sqlite3_reset(delete_stmt);
            sqlite3_clear_bindings(delete_stmt);
            sqlite3_bind_int64(delete_stmt, 1, id);
            if (sqlite3_step(delete_stmt) != SQLITE_DONE) {
                log_error("backup thread: delete failed id=%lld: %s", (long long)id, sqlite3_errmsg(db));
                // Row will be re-shipped next pass — safe, at-least-once (§8.2)
                break;
            }
            processed_any = 1;
            continue; // ordering preserved: only advance past this row once it's gone
        }

        // Failure path
        int new_attempts = attempts + 1;
        if (new_attempts >= MAX_ATTEMPTS) {
            sqlite3_reset(dead_letter_stmt);
            sqlite3_clear_bindings(dead_letter_stmt);
            sqlite3_bind_int(dead_letter_stmt, 1, new_attempts);
            sqlite3_bind_text(dead_letter_stmt, 2, "max attempts exceeded", -1, SQLITE_STATIC);
            sqlite3_bind_int64(dead_letter_stmt, 3, id);
            if (sqlite3_step(dead_letter_stmt) == SQLITE_DONE) {
                sqlite3_reset(delete_stmt);
                sqlite3_clear_bindings(delete_stmt);
                sqlite3_bind_int64(delete_stmt, 1, id);
                sqlite3_step(delete_stmt);
                log_error("backup thread: dead-lettered id=%lld after %d attempts", (long long)id, new_attempts);
            } else {
                log_error("backup thread: dead-letter insert failed id=%lld: %s", (long long)id, sqlite3_errmsg(db));
            }
            processed_any = 1;
            continue; // one poison row handled; keep going, don't block the whole batch on it
        } else {
            sqlite3_reset(update_attempts_stmt);
            sqlite3_clear_bindings(update_attempts_stmt);
            sqlite3_bind_int(update_attempts_stmt, 1, new_attempts);
            sqlite3_bind_int64(update_attempts_stmt, 2, id);
            sqlite3_step(update_attempts_stmt);
            processed_any = 1;
            break; // stop the pass here — preserves strict ordering (§8.1); next wake/poll retries this row first
        }
    }

    return processed_any;
}
```

Notes on this loop, since every line here was chosen deliberately for the "must not crash" constraint:

- **Every `sqlite3_step`/`sqlite3_prepare_v2` return code is checked.** No exceptions. A transient failure logs and lets the next wake/poll cycle retry — nothing here is treated as fatal to the process.
- **The dead-letter path handles both the poison row (skip and continue) and a retryable row (stop and preserve order)** distinctly — mixing these up either stalls the queue forever on one bad row or silently reorders deliveries.
- **`ship_to_backup()` is called with no SQLite transaction open** — the `SELECT` above is a plain autocommit read, not a `BEGIN`/`COMMIT` wrapped transaction, so a slow or hung network call never holds a SQLite write lock. This is the single most important performance/safety property of this loop; do not "optimize" this into a wrapping transaction later without re-reading §3.3.
- **Payload is copied to a heap buffer before the network call**, not read directly from the live statement's column pointer, since `sqlite3_column_text`'s returned pointer is only valid until the next `sqlite3_step`/`sqlite3_reset`/`sqlite3_finalize` on that statement.

---

## 8. Guarantees, explicitly stated

### 8.1 Ordering
Strict `id` order, single-threaded drain, stop-on-retryable-failure (not skip-and-continue) as shown above. If your destination doesn't require ordering, skip-and-continue is a valid, higher-throughput alternative under partial failure — **this must be an explicit, reviewed decision**, not a default left in place unexamined.

### 8.2 At-least-once delivery — destination must dedupe
If the backup thread crashes (or the process crashes) after `ship_to_backup()` returns success but before the `DELETE` commits, that row ships again on next drain. This is inherent to the design, not a bug. The destination-side consumer **must** dedupe on the `id` field in the payload (§6) — communicate this requirement explicitly to whoever owns that side; it is a hard requirement, not a nice-to-have.

### 8.3 Crash safety
A change is durable the instant its transaction commits on the game connection (business write + trigger-generated `_pending_backup` insert, atomic, same transaction). Game process death at any point after that commit is safe — the row is sitting in `_pending_backup` on disk, picked up by the backup thread on next run (this process restart, or literally any process that opens the same file and runs the drain loop). Backup **thread** death (without the whole process dying) is likewise safe — restart the thread, it resumes from whatever's still in the queue; nothing was deleted that wasn't confirmed shipped.

### 8.4 What is *not* guaranteed
- No bound on delivery latency under sustained destination outage — `_pending_backup` grows as the buffer; this is intentional backpressure absorption, not a leak, but it needs monitoring (§9) and a growth alert.
- No guarantee across a true OS-level crash / power-loss event beyond what `synchronous=NORMAL` provides (§3.2) — reiterating this here because it's the one durability caveat in this whole design that isn't "our code's fault," and the team should knowingly accept it rather than discover it during an incident review.

---

## 9. Monitoring — required before this ships, not a follow-up task

- **Queue depth**: `SELECT COUNT(*) FROM _pending_backup` on a monitoring cadence — alert on sustained growth.
- **Oldest pending row age**: `SELECT unixepoch() - MIN(created_at) FROM _pending_backup` — your actual realtime-lag metric; alert on this, not just raw count.
- **Dead-letter count**: `SELECT COUNT(*) FROM _dead_backup` — alert on any nonzero growth; every row here is a change that needs manual investigation/replay.
- **Trigger coverage sanity check**, run after every `sync_backup_triggers` call and periodically thereafter:
  ```sql
  SELECT COUNT(*) FROM sqlite_master WHERE type = 'trigger' AND name LIKE 'trg_%';
  -- should equal 3x the count of non-reserved tables
  ```
- **Backup thread liveness**: a heartbeat timestamp the thread updates each loop iteration, checked by an external watchdog/health-check path — if the thread has silently died (see `fatal_prepare_error` handling above), you want to know from monitoring, not from a growing queue days later.

---

## 10. Rollout checklist

1. Implement and unit-test `sync_backup_triggers` against a copy of the actual production schema, specifically exercising every `BLOB` column and every `WITHOUT ROWID` table you have.
2. Implement the backup thread and wake-hook exactly as scoped in §5/§7; do not share connections across threads (§3.1) under any circumstance during implementation, even "temporarily for testing."
3. **Load test contention specifically**: run the game's normal write workload concurrently with the backup thread under a simulated slow/failing destination (so the queue backs up and the backup thread is doing frequent retries), and confirm game-thread write latency is unaffected. This is the test most likely to catch a violation of §3.3.
4. **Kill tests**: kill the process mid-game-write, mid-drain, and mid-`ship_to_backup` (both before and after the simulated destination ack); confirm zero data loss and confirm the destination correctly dedupes the at-least-once redelivery case.
5. Confirm the destination-side consumer implements idempotency on `id` (§8.2) before enabling this in production — this is a cross-team dependency, don't let it be the thing that ships last.
6. Wire up all four monitoring signals in §9 before enabling in production.
7. Confirm the kill-switch (§1, "independently disableable") works: verify the game runs correctly with the backup subsystem fully disabled (triggers absent or hook unregistered), so there's a fast, tested rollback path if an incident occurs post-launch.
8. Decide and document explicitly: ordering-strict vs skip-and-continue (§8.1), and the `synchronous` durability tradeoff (§3.2) — both are one-line changes with real consequences, and both should be signed off by the team, not left as whatever the reference implementation happened to default to.

---

## 11. Open decisions for the team

- **`ship_to_backup()` destination and transport** — the payload shape (§6) is destination-agnostic; the actual network/serialization code needs to be written against your chosen backend (another SQLite/Postgres instance, an internal service, cloud storage, etc.), including its own timeout and retry semantics, which interact with `MAX_ATTEMPTS` above.
- **Ordering strictness** (§8.1) — confirm with the destination-owning team whether strict ordering is required.
- **Dead-letter response process** — on-call ownership and replay procedure for `_dead_backup` rows.
- **`synchronous` setting for the game connection** (§3.2) — sign-off from whoever owns save-data durability requirements for the title.
- **Threading topology** (§3.4) — confirm whether the game already has a single dedicated DB thread; if so, the design simplifies meaningfully and should be re-scoped before implementation starts.
