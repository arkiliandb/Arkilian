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
  cc -O2 -Wall -Wextra "$src" src/class.c src/deps/sqlite/sqlite3.c \
     -Isrc -Isrc/deps/sqlite -lcurl -lpthread \
     -DSQLITE_ENABLE_PREUPDATE_HOOK -DSQLITE_ENABLE_FTS5 \
     "$@" -o "$name"
  ./"$name"
  rm -f "$name"
}

run_hydration() {
  echo "── test_hydration ──"
  cc -O2 -Wall -Wextra tests/test_hydration.c \
     src/hydration.c src/sha256.c src/deps/sqlite/sqlite3.c \
     -Isrc -Isrc/deps/sqlite -lcurl -lpthread -lm \
     -o test_hydration
  ./test_hydration
  rm -f test_hydration
}

run      test_basic           tests/test_basic.c
run      test_interception    tests/test_interception.c
run      test_regressions     tests/test_regressions.c
run      test_kill_switch     tests/test_kill_switch.c
run      test_load_contention tests/test_load_contention.c -lm
run      test_kill_resilience tests/test_kill_resilience.c
run      test_monitoring      tests/test_monitoring.c
run      test_virtual_tables  tests/test_virtual_tables.c
run      test_deterministic   tests/test_deterministic.c
run_hydration

# Benchmarks: built + run, but they assert correctness internally. Not
# part of the pass/fail gate (they're too long-running for default CI).
echo "── bench_1m (benchmark, not gated) ──"
cc -O2 tests/bench_1m.c src/class.c src/deps/sqlite/sqlite3.c \
   -Isrc -Isrc/deps/sqlite -lcurl -lpthread -lm \
   -DSQLITE_ENABLE_PREUPDATE_HOOK -DSQLITE_ENABLE_FTS5 -o bench_1m || {
     echo "bench_1m build failed" ; exit 1 ; }
./bench_1m
rm -f bench_1m

echo "── all C tests passed ──"