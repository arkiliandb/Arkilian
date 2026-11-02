#!/usr/bin/env bash
# Arkilian benchmark on this machine:
#   1. local capture overhead vs raw SQLite (bench/bench_cmp.c)
#   2. end-to-end against a real MinIO container (bench/minio/minio_bench.c):
#      live shipping, drain-to-storage time, snapshot, shutdown publish,
#      restore + byte-level verification, and a 15 s storage outage.
# Usage: bash bench/minio/run.sh            (from the repository root)
# Results: bench/minio/results-<timestamp>.txt (+ results.csv)
set -euo pipefail
cd "$(dirname "$0")/../.."
ROOT="$PWD"
OUT_DIR="$ROOT/bench/minio"
STAMP="$(date +%Y%m%d-%H%M%S)"
LOG="$OUT_DIR/results-$STAMP.txt"
WORK="$(mktemp -d "${TMPDIR:-/tmp}/arkbench.XXXXXX")"
PORT="${ARKBENCH_PORT:-19000}"
NAME="arkilian-bench-minio"
USER_KEY="arkbench"
SECRET_KEY="arkbench-local-secret-key"   # local throwaway container only
log() { echo "$@" | tee -a "$LOG"; }

command -v docker >/dev/null || { echo "docker not found"; exit 1; }
IMG="${ARKBENCH_MINIO_IMAGE:-$(docker images --format '{{.Repository}}:{{.Tag}}' | grep -i minio | grep -v '<none>' | head -1)}"
[ -n "$IMG" ] || { echo "no MinIO image found locally (set ARKBENCH_MINIO_IMAGE)"; exit 1; }

CFLAGS="-O2 -DNDEBUG -Isrc -Isrc/deps/sqlite -DSQLITE_ENABLE_PREUPDATE_HOOK -D_DEFAULT_SOURCE"
[ "$(uname)" = "Darwin" ] && CFLAGS="$CFLAGS -D_DARWIN_C_SOURCE"
SRCS="src/class.c src/hydration.c src/sha256.c src/deps/sqlite/sqlite3.c"
log "== Arkilian benchmark $STAMP on $(uname -sm), $(sysctl -n machdep.cpu.brand_string 2>/dev/null || grep -m1 'model name' /proc/cpuinfo | cut -d: -f2)"
log "== building (one-time, ~1 min for the SQLite amalgamation)"
cc $CFLAGS bench/bench_cmp.c $SRCS -lcurl -lpthread -lm -o "$WORK/bench_cmp"
cc $CFLAGS bench/minio/minio_bench.c $SRCS -lcurl -lpthread -lm -o "$WORK/minio_bench"

log ""
log "== 1. local capture overhead (no storage), median of 3 runs, CSV: engine,sync,workload,ops_per_s,p50_us,p95_us,p99_us"
cd "$WORK"
for sync in NORMAL FULL; do
  n=20000; [ "$sync" = FULL ] && n=3000
  for rep in 1 2 3; do
    ./bench_cmp $n $sync raw sqlite 2>/dev/null | tee -a "$LOG"
    ./bench_cmp $n $sync ark arkilian-1.6.2 2>/dev/null | tee -a "$LOG"
  done
done

log ""
log "== 2. end-to-end against MinIO ($IMG) on 127.0.0.1:$PORT"
docker rm -f "$NAME" >/dev/null 2>&1 || true
docker run -d --rm --name "$NAME" -p "$PORT:9000" \
  -e MINIO_ROOT_USER="$USER_KEY" -e MINIO_ROOT_PASSWORD="$SECRET_KEY" \
  "$IMG" server /data >/dev/null
trap 'docker rm -f "$NAME" >/dev/null 2>&1 || true; rm -rf "$WORK"' EXIT
for i in $(seq 1 60); do curl -fs "http://127.0.0.1:$PORT/minio/health/live" >/dev/null && break; sleep 1; done
./minio_bench "http://127.0.0.1:$PORT" arkbench "$USER_KEY" "$SECRET_KEY" 5000 --outage 45 > e2e.txt 2>&1 &
BENCH=$!
for i in $(seq 1 600); do grep -q 'outage run' e2e.txt 2>/dev/null && break; sleep 1; done
sleep 10; docker pause "$NAME" >/dev/null; echo "  [driver] MinIO paused" >> e2e.txt
sleep 15; docker unpause "$NAME" >/dev/null; echo "  [driver] MinIO resumed" >> e2e.txt
wait $BENCH || true
grep -v '^arkilian: \[' e2e.txt | tee -a "$LOG"
cp results.csv "$OUT_DIR/results-$STAMP.csv" 2>/dev/null || true
log ""
log "== done: $LOG"
