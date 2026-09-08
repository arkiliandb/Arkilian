#!/usr/bin/env bash
# Arkilian Production Stress Suite — local + Docker/MinIO on this machine.
#
# Phases:
#   0. Build every C binary (tests, stress)
#   1. Local C test suites (unit, regression, kill-switch, load, kill-resilience)
#   2. Dead-letter tool (arkilian-dlq) count/list/replay smoke
#   3. Throughput stress (stress_200m, configurable size)
#   4. Teardown
#
# Shipping is S3-only: the throughput phase points the client at a
# connection-refusing loopback S3 endpoint (with dummy credentials) so
# every flush attempt fails fast and retries — exercising the degraded
# path (retry backoff, outbox accumulation) the client must survive.
# Point ARKILIAN_S3_ENDPOINT at a real MinIO (see
# docker-compose.stress.yml) to also exercise the happy path (chunk PUTs,
# manifest publishing, snapshot uploads).
#
# Tuning env vars:
#   STRESS_WRITES=200000 writes for the throughput stress
#
# Exit 0 = everything green. Any failure = exit 1 with logs.

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
WORK="$(mktemp -d /tmp/arkilian-stress.XXXXXX)"
BIN="$WORK/bin"
mkdir -p "$BIN"

log() { printf '\n\033[1;34m=== %s ===\033[0m\n' "$*"; }
ok()  { printf '\033[1;32m  OK\033[0m %s\n' "$*"; }
fail(){ printf '\033[1;31m  FAIL\033[0m %s\n' "$*"; exit 1; }

cleanup() {
  rm -rf "$WORK"
}
trap cleanup EXIT

# ── Helpers ─────────────────────────────────────────────────────────

# The sqlite3 amalgamation is 9.4MB — compile it ONCE into an object and
# link every test against it, otherwise the build phase takes ~20 minutes.
build_c() {
  local out="$1" src="$2" extra="${3:-}"
  # shellcheck disable=SC2086
  cc -O2 "$src" src/class.c src/sha256.c "$BIN/sqlite3.o" \
     -Isrc -Isrc/deps/sqlite -lcurl -lpthread -lm $extra -o "$BIN/$out"
}

# C binaries must never see the repo-root .env (load_env() would
# override their configuration) — run them from the clean workdir.
run_in_work() { (cd "$WORK" && "$@"); }

# ── Phase 0: build ──────────────────────────────────────────────────

log "Phase 0: building binaries"
cd "$ROOT"
# FTS5 ships inside the amalgamation; enabled for the whole build so the
# virtual-table regression test links against the same object.
cc -O2 -c src/deps/sqlite/sqlite3.c -Isrc -Isrc/deps/sqlite \
   -DSQLITE_ENABLE_FTS5 -o "$BIN/sqlite3.o"
build_c test_basic       tests/test_basic.c
build_c test_interception tests/test_interception.c
build_c test_regressions tests/test_regressions.c
build_c test_deterministic tests/test_deterministic.c "-DSQLITE_ENABLE_PREUPDATE_HOOK"
build_c test_kill_switch tests/test_kill_switch.c
build_c test_load_contention tests/test_load_contention.c
build_c test_kill_resilience tests/test_kill_resilience.c
build_c test_monitoring    tests/test_monitoring.c
build_c test_virtual_tables tests/test_virtual_tables.c "-DSQLITE_ENABLE_FTS5"
build_c stress_200m      tests/stress_200m.c
# Hydration links hydration.c, not class.c
cc -O2 tests/test_hydration.c src/hydration.c src/sha256.c "$BIN/sqlite3.o" \
   -Isrc -Isrc/deps/sqlite -lcurl -lpthread -o "$BIN/test_hydration"
cc -O2 tools/arkilian-dlq.c "$BIN/sqlite3.o" -Isrc -Isrc/deps/sqlite -o "$BIN/arkilian-dlq"
ok "11 binaries built"

# ── Phase 1: local test suites ──────────────────────────────────────

log "Phase 1: local C test suites"
for t in test_basic test_interception test_regressions test_deterministic \
         test_kill_switch test_load_contention test_kill_resilience \
         test_monitoring test_virtual_tables test_hydration; do
  if run_in_work "$BIN/$t" > "$WORK/$t.log" 2>&1; then
    ok "$t"
  else
    tail -40 "$WORK/$t.log"
    fail "$t"
  fi
done

# Dead-letter tool smoke: craft an outbox db, replay, verify.
log "Phase 1.5: dead-letter tool (arkilian-dlq)"
DLQ_DB="$WORK/dlq-smoke.db"
sqlite3 "$DLQ_DB" "
  CREATE TABLE _pending_backup (id INTEGER PRIMARY KEY AUTOINCREMENT, payload TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0, created_at INTEGER NOT NULL DEFAULT (strftime('%s','now')), last_attempt_at INTEGER);
  CREATE TABLE _dead_backup (id INTEGER PRIMARY KEY, payload TEXT NOT NULL, attempts INTEGER NOT NULL, failed_reason TEXT, created_at INTEGER NOT NULL DEFAULT (strftime('%s','now')), dead_lettered_at INTEGER NOT NULL DEFAULT (strftime('%s','now')));
  INSERT INTO _dead_backup (id, payload, attempts, failed_reason) VALUES (7, 'REPLACE INTO \"t\" (\"a\") VALUES (1)', 10, 'max attempts exceeded');
  INSERT INTO _dead_backup (id, payload, attempts, failed_reason) VALUES (8, 'DELETE FROM \"t\" WHERE rowid = 3', 10, 'max attempts exceeded');
"
CNT="$(run_in_work "$BIN/arkilian-dlq" "$DLQ_DB" --count)"
[ "$CNT" = "2" ] || fail "dlq count=$CNT (expected 2)"
run_in_work "$BIN/arkilian-dlq" "$DLQ_DB" --replay > /dev/null
CNT="$(run_in_work "$BIN/arkilian-dlq" "$DLQ_DB" --count)"
[ "$CNT" = "0" ] || fail "dlq replay left $CNT rows"
PN="$(sqlite3 "$DLQ_DB" "SELECT COUNT(*) FROM _pending_backup;")"
[ "$PN" = "2" ] || fail "dlq replay queued $PN rows (expected 2)"
ok "arkilian-dlq count/list/replay verified"

# ── Phase 3: throughput stress ───────────────────────────────────────

log "Phase 3: throughput stress (stress_200m)"
STRESS_WRITES="${STRESS_WRITES:-200000}"
# The loopback endpoint is permitted cleartext by the hardening guard, so
# backup stays enabled; every flush attempt fails fast (connection
# refused) and retries with backoff — exercising exactly the degraded
# path the client must survive: flush-thread liveness, attempt tracking,
# outbox accumulation, and dead-lettering. Point ARKILIAN_S3_ENDPOINT at
# a real MinIO (see docker-compose.stress.yml) to instead exercise the
# happy path — chunk PUTs, manifest publishing, snapshot uploads.
if run_in_work env \
     ARKILIAN_ENABLE_BACKUP=1 \
     ARKILIAN_S3_ENDPOINT="${ARKILIAN_S3_ENDPOINT:-http://127.0.0.1:1}" \
     ARKILIAN_S3_BUCKET="${ARKILIAN_S3_BUCKET:-stress-bucket}" \
     ARKILIAN_S3_REGION="us-east-1" \
     ARKILIAN_S3_ACCESS_KEY="${ARKILIAN_S3_ACCESS_KEY:-stress-access}" \
     ARKILIAN_S3_SECRET_KEY="${ARKILIAN_S3_SECRET_KEY:-stress-secret}" \
     ARKILIAN_S3_PREFIX="db_stress" \
     ARKILIAN_BACKUP_INTERVAL=14400 \
     ARKILIAN_BACKUP_PATH="$WORK/stress_backup.sqlite" \
     "$BIN/stress_200m" "$STRESS_WRITES"; then
  ok "throughput stress passed ($STRESS_WRITES ops)"
else
  fail "throughput stress"
fi

# ── Phase 4: teardown (EXIT trap) ───────────────────────────────────

log "=== STRESS SUITE PASSED — all phases green ==="
