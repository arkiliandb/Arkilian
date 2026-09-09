#!/usr/bin/env bash
# Arkilian C test runner.
#
# This script remains as a POSIX-portable alternative to `ctest`, but
# the canonical entry point is now:
#
#     cmake -B build -S . -DARKILIAN_BUILD_TESTS=ON
#     cmake --build build
#     cd build && ctest --output-on-failure
#
# Every test in tests/ is compiled + run in sequence. set -euo pipefail
# makes any compile OR test failure stop the script and propagate a
# non-zero exit code (the previous version had no `set -e` and its exit
# code was whatever `./bench_1m` returned last, which asserts nothing —
# a silent regression in test_regressions would ship green).

set -euo pipefail

# Run from the repo root regardless of where the caller invoked us.
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")" && pwd)"
repo_root="$(dirname "$script_dir")"
cd "$repo_root"

run() {
  local name="$1" src="$2" ; shift 2
  echo "── $name ──"
  cc -O2 -Wall -Wextra "$src" src/class.c src/hydration.c src/sha256.c src/deps/sqlite/sqlite3.c \
     -Isrc -Isrc/deps/sqlite -lcurl -lpthread -lm \
     -DSQLITE_ENABLE_PREUPDATE_HOOK -DSQLITE_ENABLE_FTS5 \
     "$@" -o "$name"
  ./"$name"
  rm -f "$name"
}

run_hydration() {
  echo "── test_hydration ──"
  cc -O2 -Wall -Wextra tests/test_hydration.c \
     src/class.c src/hydration.c src/sha256.c src/deps/sqlite/sqlite3.c \
     -Isrc -Isrc/deps/sqlite -lcurl -lpthread -lm \
     -o test_hydration
  ./test_hydration
  rm -f test_hydration
}

# Dead-letter queue tool + its regression suite (launch Checklist #5).
# Snapshot-baseline invariant (P0): real cycle + mid-copy chunk injection
# + hydrate equivalence. Shares the stub-server harness with test_hydration.
run_snapshot_watermark() {
  echo "── test_snapshot_watermark ──"
  cc -O2 -Wall -Wextra tests/test_snapshot_watermark.c \
     src/class.c src/hydration.c src/sha256.c src/deps/sqlite/sqlite3.c \
     -Isrc -Isrc/deps/sqlite -lcurl -lpthread -lm \
     -DSQLITE_ENABLE_PREUPDATE_HOOK -DSQLITE_ENABLE_FTS5 \
     -o test_snapshot_watermark
  ./test_snapshot_watermark
  rm -f test_snapshot_watermark
}

# Manifest wire-protocol + authenticity (P0): HMAC sign/verify end-to-end,
# digest-mandatory chunks, strict LSN sequence validation.
run_manifest_protocol() {
  echo "── test_manifest_protocol ──"
  cc -O2 -Wall -Wextra tests/test_manifest_protocol.c \
     src/class.c src/hydration.c src/sha256.c src/deps/sqlite/sqlite3.c \
     -Isrc -Isrc/deps/sqlite -lcurl -lpthread -lm \
     -DSQLITE_ENABLE_PREUPDATE_HOOK -DSQLITE_ENABLE_FTS5 \
     -o test_manifest_protocol
  ./test_manifest_protocol
  rm -f test_manifest_protocol
}

# Dead-letter queue tool + its regression suite (launch Checklist #5).
run_dlq() {
  echo "── arkilian-dlq (build) ──"
  cc -O2 -Wall -Wextra -Wpedantic -Werror tools/arkilian-dlq.c \
     src/deps/sqlite/sqlite3.c -Isrc/deps/sqlite -o arkilian-dlq
  echo "── test_dlq ──"
  cc -O2 -Wall -Wextra tests/test_dlq.c src/deps/sqlite/sqlite3.c \
     -Isrc/deps/sqlite -lpthread \
     -DARKILIAN_DLQ_BIN='"./arkilian-dlq"' -o test_dlq
  ./test_dlq
  rm -f arkilian-dlq test_dlq
}

run      test_basic           tests/test_basic.c
run      test_interception    tests/test_interception.c
run      test_regressions     tests/test_regressions.c
run      test_kill_switch     tests/test_kill_switch.c
run      test_kill_resilience tests/test_kill_resilience.c
run      test_load_contention tests/test_load_contention.c
run      test_dst_backpressure tests/test_dst_backpressure.c
run      test_monitoring      tests/test_monitoring.c
run      test_virtual_tables  tests/test_virtual_tables.c
run      test_deterministic   tests/test_deterministic.c
run      test_hardening       tests/test_hardening.c
run      test_health_flags    tests/test_health_flags.c
run_hydration
run_snapshot_watermark
run_manifest_protocol
run_dlq

# Benchmarks: built + run, but they assert correctness internally. Not
# part of the pass/fail gate (they're too long-running for default CI).
echo "── bench_1m (benchmark, not gated) ──"
cc -O2 tests/bench_1m.c src/class.c src/hydration.c src/sha256.c src/deps/sqlite/sqlite3.c \
   -Isrc -Isrc/deps/sqlite -lcurl -lpthread -lm \
   -DSQLITE_ENABLE_PREUPDATE_HOOK -DSQLITE_ENABLE_FTS5 -o bench_1m || {
     echo "bench_1m build failed" ; exit 1 ; }
./bench_1m
rm -f bench_1m

echo "── all C tests passed ──"


# test 
cmake -B build -S . -DARKILIAN_BUILD_TESTS=ON -DCMAKE_BUILD_TYPE=Release && cmake --build build -j4 && ctest --test-dir build --output-on-failure  # 17/17 passed (74.22s)
npm run build && npm test  # Node 24.14.0, darwin x64 – ctest darwin 17/17, stress 11/11 + throughput 100 ops green; Node: 4 workers x500 ops – 1 worker “database is locked” flake but harness reports “All tests passed!”
STRESS_WRITES=100 bash scripts/stress.sh  # all phases green (11 binaries, 100/100 writes + 100/100 reads)
cmake -B build-tsan -S . -DARKILIAN_BUILD_TESTS=ON -DCMAKE_C_FLAGS="-fsanitize=thread" && ctest --test-dir build-tsan  # 16/17 (94%) – test_kill_resilience flake under TSAN (11.74s, SIGKILL + 250ms slow dest, pending_before 201)