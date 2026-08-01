#!/bin/bash

 # Test basic database operations
 cc tests/test_basic.c src/class.c src/deps/sqlite/sqlite3.c \
                                                       -Isrc -Isrc/deps/sqlite -lcurl -lpthread -o test_basic
 ./test_basic 


 # Test SQL statement interception
 cc tests/test_interception.c src/class.c src/deps/sqlite/sqlite3.c \
                                                       -Isrc -Isrc/deps/sqlite -lcurl -lpthread -o test_interception
 ./test_interception

 # Regression tests for production audit fixes
 cc tests/test_regressions.c src/class.c src/deps/sqlite/sqlite3.c \
                                                       -Isrc -Isrc/deps/sqlite -lcurl -lpthread -o test_regressions
 ./test_regressions

 # Kill-switch tests (runtime disable/enable of the backup subsystem)
 cc tests/test_kill_switch.c src/class.c src/deps/sqlite/sqlite3.c \
                                                       -Isrc -Isrc/deps/sqlite -lcurl -lpthread -o test_kill_switch
 ./test_kill_switch

 # Load-contention test (game-thread latency under backup pressure)
 cc tests/test_load_contention.c src/class.c src/deps/sqlite/sqlite3.c \
                                                       -Isrc -Isrc/deps/sqlite -lcurl -lpthread -lm -o test_load_contention
 ./test_load_contention

 # Kill-resilience tests (SIGKILL mid-write/drain/ship)
 cc tests/test_kill_resilience.c src/class.c src/deps/sqlite/sqlite3.c \
                                                       -Isrc -Isrc/deps/sqlite -lcurl -lpthread -o test_kill_resilience
 ./test_kill_resilience

 # Monitoring tests (queue depth, lag, dead letters, heartbeat, health, logging)
 cc tests/test_monitoring.c src/class.c src/deps/sqlite/sqlite3.c \
                                                       -Isrc -Isrc/deps/sqlite -lcurl -lpthread -o test_monitoring
 ./test_monitoring

 # Test hydration
 cc tests/test_hydration.c src/hydration.c \
                                    -Isrc -Isrc/deps/sqlite -lcurl -lsqlite3 -o test_hydration
 ./test_hydration

 cc tests/test_deterministic.c src/class.c src/deps/sqlite/sqlite3.c \
    -Isrc -Isrc/deps/sqlite -lcurl -lpthread \
    -DSQLITE_ENABLE_PREUPDATE_HOOK -o test_deterministic
 ./test_deterministic

 # Test server
#  cd server
#  go test -v ./...
#  cd ..

 # Test benchmark with 1M insertions
 cc -O2 tests/bench_1m.c src/class.c src/deps/sqlite/sqlite3.c \
                                                          -Isrc -Isrc/deps/sqlite -lcurl -lpthread -lm \
                                                          -DSQLITE_ENABLE_PREUPDATE_HOOK -o bench_1m
 ./bench_1m

