#!/usr/bin/env bash
# Arkilian Production Stress Suite — local + Docker/MinIO on this machine.
#
# Phases:
#   0. Build every C binary (tests, stress, e2e validator)
#   1. Local C test suites (unit, regression, kill-switch, load, kill-resilience)
#   2. Stack: MinIO + control plane via docker compose (falls back to a
#      local control-plane binary when Docker is unavailable)
#   3. Tenant setup (register → create database → api_key)
#   4. End-to-end stress: real client -> control plane -> MinIO,
#      with server-side wal_entries + hydrate-plan verification
#   5. Throughput stress (stress_200m, configurable size)
#   6. Server-side totals + dashboard smoke check
#   7. Teardown
#
# Tuning env vars:
#   E2E_WRITES=10000     writes for the end-to-end validator
#   STRESS_WRITES=200000 writes for the throughput stress
#
# Exit 0 = everything green. Any failure = exit 1 with logs.

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
WORK="$(mktemp -d /tmp/arkilian-stress.XXXXXX)"
BIN="$WORK/bin"
mkdir -p "$BIN"
CP_PID=""
COMPOSE_UP=0
CP_URL=""

log() { printf '\n\033[1;34m=== %s ===\033[0m\n' "$*"; }
ok()  { printf '\033[1;32m  OK\033[0m %s\n' "$*"; }
fail(){ printf '\033[1;31m  FAIL\033[0m %s\n' "$*"; exit 1; }

cleanup() {
  if [ "$COMPOSE_UP" = "1" ]; then
    log "Teardown: docker compose down"
    docker compose -f "$ROOT/docker-compose.stress.yml" down -v 2>/dev/null || true
  fi
  if [ -n "$CP_PID" ]; then kill "$CP_PID" 2>/dev/null || true; fi
  rm -rf "$WORK"
}
trap cleanup EXIT

# ── Helpers ─────────────────────────────────────────────────────────

# docker_available: bounded check — `docker info` can hang while the
# Desktop VM boots; give it up to 15s then give up.
docker_available() {
  local pid
  (docker info >/dev/null 2>&1) &
  pid=$!
  for _ in $(seq 1 15); do
    if ! kill -0 "$pid" 2>/dev/null; then wait "$pid" 2>/dev/null; return 0; fi
    sleep 1
  done
  kill -9 "$pid" 2>/dev/null || true
  wait "$pid" 2>/dev/null || true
  return 1
}

wait_for_http() {
  local url="$1" tries="$2" i=0
  while ! curl -sf -o /dev/null "$url" 2>/dev/null; do
    i=$((i + 1))
    if [ "$i" -ge "$tries" ]; then return 1; fi
    sleep 1
  done
}

json_field() { python3 -c "import sys,json;print(json.load(sys.stdin)[\"$1\"])" 2>/dev/null || echo ""; }

build_c() {
  local out="$1" src="$2" extra="${3:-}"
  # shellcheck disable=SC2086
  cc -O2 "$src" src/class.c src/deps/sqlite/sqlite3.c \
     -Isrc -Isrc/deps/sqlite -lcurl -lpthread -lm $extra -o "$BIN/$out"
}

# C binaries must never see the repo-root .env (load_env() would
# override their configuration) — run them from the clean workdir.
run_in_work() { (cd "$WORK" && "$@"); }

# ── Phase 0: build ──────────────────────────────────────────────────

log "Phase 0: building binaries"
cd "$ROOT"
build_c test_basic       tests/test_basic.c
build_c test_interception tests/test_interception.c
build_c test_regressions tests/test_regressions.c
build_c test_deterministic tests/test_deterministic.c "-DSQLITE_ENABLE_PREUPDATE_HOOK"
build_c test_kill_switch tests/test_kill_switch.c
build_c test_load_contention tests/test_load_contention.c
build_c test_kill_resilience tests/test_kill_resilience.c
build_c test_e2e_stress tests/test_e2e_stress.c
build_c stress_200m      tests/stress_200m.c
ok "9 binaries built"

# ── Phase 1: local test suites ──────────────────────────────────────

log "Phase 1: local C test suites"
for t in test_basic test_interception test_regressions test_deterministic \
         test_kill_switch test_load_contention test_kill_resilience; do
  if run_in_work "$BIN/$t" > "$WORK/$t.log" 2>&1; then
    ok "$t"
  else
    tail -40 "$WORK/$t.log"
    fail "$t"
  fi
done

# ── Phase 2: stack (docker, fallback local) ─────────────────────────

log "Phase 2: infrastructure stack"
if docker_available && docker compose -f "$ROOT/docker-compose.stress.yml" \
     up -d --build minio createbucket control-plane > "$WORK/compose.log" 2>&1; then
  COMPOSE_UP=1
  if wait_for_http "http://localhost:8080/health" 60; then
    CP_URL="http://localhost:8080"
    ok "docker stack up (MinIO :9000, control plane :8080)"
  else
    docker compose -f "$ROOT/docker-compose.stress.yml" logs control-plane 2>/dev/null | tail -30 || true
    fail "control plane did not become healthy"
  fi
else
  log "Docker unavailable/failed — falling back to a local control plane (no MinIO)"
  (cd "$ROOT/server" && go build -o "$BIN/arkilian-server" .)
  ARKILIAN_DB_PATH="$WORK/cp.db" PORT=18080 \
    "$BIN/arkilian-server" > "$WORK/cp.log" 2>&1 &
  CP_PID=$!
  if wait_for_http "http://localhost:18080/health" 30; then
    CP_URL="http://localhost:18080"
    ok "local control plane on :18080"
  else
    tail -30 "$WORK/cp.log"
    fail "local control plane did not start"
  fi
fi

# ── Phase 3: tenant setup ───────────────────────────────────────────

log "Phase 3: tenant setup"
EMAIL="stress@arkilian.local"
PASS="secret123"
# Registration may already exist on re-runs — 409 is fine.
curl -s -o /dev/null -X POST "$CP_URL/v1/auth/register" \
  -H 'Content-Type: application/json' -d "{\"email\":\"$EMAIL\",\"password\":\"$PASS\"}" || true
LOGIN="$(curl -s -X POST "$CP_URL/v1/auth/login" \
  -H 'Content-Type: application/json' -d "{\"email\":\"$EMAIL\",\"password\":\"$PASS\"}")"
TOKEN="$(echo "$LOGIN" | json_field token)"
[ -n "$TOKEN" ] || fail "login failed: $LOGIN"
CREATE="$(curl -s -X POST "$CP_URL/v1/db/create" \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{"name":"stress-e2e"}')"
API_KEY="$(echo "$CREATE" | json_field api_key)"
DB_ID="$(echo "$CREATE" | json_field db_id)"
[ -n "$API_KEY" ] && [ -n "$DB_ID" ] || fail "db create failed: $CREATE"
ok "tenant $EMAIL / db $DB_ID"

# ── Phase 4: end-to-end stress ──────────────────────────────────────

log "Phase 4: end-to-end stress (client -> control plane -> storage)"
E2E_WRITES="${E2E_WRITES:-10000}"
if run_in_work "$BIN/test_e2e_stress" --url "$CP_URL" --key "$API_KEY" \
     --db "$DB_ID" --writes "$E2E_WRITES"; then
  ok "e2e stress passed ($E2E_WRITES writes)"
else
  fail "e2e stress"
fi

# ── Phase 5: throughput stress ──────────────────────────────────────

log "Phase 5: throughput stress (stress_200m)"
STRESS_WRITES="${STRESS_WRITES:-200000}"
if run_in_work env \
     ARKILIAN_ENABLE_BACKUP=1 \
     ARKILIAN_WAL_PUSH_URL="$CP_URL/v1/wal/push" \
     ARKILIAN_DATABASE_TOKEN="$API_KEY" \
     ARKILIAN_BACKUP_INTERVAL=14400 \
     ARKILIAN_BACKUP_PATH="$WORK/stress_backup.sqlite" \
     "$BIN/stress_200m" "$STRESS_WRITES"; then
  ok "throughput stress passed ($STRESS_WRITES ops)"
else
  fail "throughput stress"
fi

# ── Phase 6: server-side verification ───────────────────────────────

log "Phase 6: server-side totals + dashboard smoke"
COUNT="$(curl -s "$CP_URL/v1/wal/count" -H "Authorization: Bearer $API_KEY" | json_field count)"
ok "control plane wal_entries for $DB_ID: $COUNT"
SUMMARY="$(curl -s "$CP_URL/v1/monitor/summary" -H "Authorization: Bearer $TOKEN")"
ok "dashboard summary: $SUMMARY"
HTTP_CODE="$(curl -s -o /dev/null -w '%{http_code}' "$CP_URL/")"
[ "$HTTP_CODE" = "200" ] || fail "dashboard / returned $HTTP_CODE"
ok "dashboard served (HTTP $HTTP_CODE)"

log "=== STRESS SUITE PASSED — all phases green ==="
