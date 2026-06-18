#!/bin/bash

 # Test basic database operations
 cc tests/test_basic.c src/class.c src/deps/sqlite/sqlite3.c \
                                                       -Isrc -Isrc/deps/sqlite -lcurl -lpthread -o test_basic
 ./test_basic 


 # Test SQL statement interception
 cc tests/test_interception.c src/class.c src/deps/sqlite/sqlite3.c \
                                                       -Isrc -Isrc/deps/sqlite -lcurl -lpthread -o test_interception
 . /test_interception

 # Test benchmark with 1M insertions
 cc -O2 tests/bench_1m.c src/class.c src/deps/sqlite/sqlite3.c \
                                                          -Isrc -Isrc/deps/sqlite -lcurl -lpthread -o bench_1m
 ./bench_1m