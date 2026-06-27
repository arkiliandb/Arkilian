#!/bin/bash

 # Test basic database operations
 cc tests/test_basic.c src/class.c src/deps/sqlite/sqlite3.c \
                                                       -Isrc -Isrc/deps/sqlite -lcurl -lpthread -o test_basic
 ./test_basic 


 # Test SQL statement interception
 cc tests/test_interception.c src/class.c src/deps/sqlite/sqlite3.c \
                                                       -Isrc -Isrc/deps/sqlite -lcurl -lpthread -o test_interception
 ./test_interception

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
#  cc -O2 tests/bench_1m.c src/class.c src/deps/sqlite/sqlite3.c \
#                                                           -Isrc -Isrc/deps/sqlite -lcurl -lpthread -o bench_1m
#  ./bench_1m

