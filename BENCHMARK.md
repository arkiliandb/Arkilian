# Arkilian Performance Benchmark Report

**Benchmark Suite:** `tests/bench_1m.c`  
**Test Hardware:** Intel(R) Core(TM) i7-9750H CPU @ 2.60GHz, 6 Cores / 12 Threads, macOS 24.6.0  
**Target Database:** SQLite 3.46.1 (amalgamation)  
**Configuration:** `journal_mode=WAL`, `synchronous=NORMAL`, `busy_timeout=5000`, `foreign_keys=ON`  
**Operations per Benchmark:** 100,000 operations (warmup: 10,000)  
**Compiler:** Apple Clang `-O2`  
**Measurement:** `clock_gettime(CLOCK_MONOTONIC)` with high-precision nanosecond timer  
**Reproducibility:** Deterministic PRNG (`xorshift32`, seed=42); both engines execute against the same table schema, connection parameters, and transaction boundaries.

---

## 1. Single-Row Throughput (1 Transaction per Operation)

Evaluates individual statement overhead when executing single operations outside explicit multi-operation batch transactions.

| Operation | Workload | Raw SQLite Throughput | Arkilian Throughput | Overhead |
| :--- | :--- | :---: | :---: | :---: |
| **INSERT** | 100,000 prepared single-row inserts | **5,262 ops/s** | **5,144 ops/s** | **-2.2%** |
| **UPDATE** | 100,000 PK-targeted single-row updates | **5,030 ops/s** | **5,018 ops/s** | **-0.2%** |
| **SELECT (PK)** | 100,000 point lookups by primary key | **139,147 ops/s** | **135,875 ops/s** | **-2.4%** |
| **SELECT (Range)** | 10,000 range scans (100 rows per scan) | **6,182 ops/s** | **6,017 ops/s** | **-2.7%** |

*Note: For single INSERT and UPDATE operations, Arkilian introduces ~0.2% to 2.2% overhead for executing the row-level DML triggers and writing changes to the internal `_pending_backup` outbox table.*

---

## 2. Batched INSERT Throughput

Evaluates write throughput when grouping inserts into explicit transactions of varying batch sizes (100,000 operations total per test).

| Batch Size | Raw SQLite Throughput | Arkilian Throughput | Difference |
| :--- | :---: | :---: | :---: |
| **1 (Autocommit)** | 1,672 ops/s | 1,760 ops/s | +5.3% |
| **10** | 10,354 ops/s | 10,516 ops/s | +1.6% |
| **100** | 22,975 ops/s | 23,231 ops/s | +1.1% |
| **1,000** | 28,604 ops/s | 28,383 ops/s | -0.8% |
| **10,000** | 29,358 ops/s | 29,600 ops/s | +0.8% |
| **100,000** | 29,304 ops/s | 29,432 ops/s | +0.4% |
| **1,000,000** | 29,327 ops/s | 29,286 ops/s | -0.1% |

*Throughput saturates at ~29,500 ops/sec once batch size reaches 1,000+ rows per transaction.*

---

## 3. Latency Distribution (50,000 Operations)

Measures statement execution latency across P50, P95, and P99 percentiles.

| Operation | Metric | Raw SQLite | Arkilian | Difference |
| :--- | :--- | :---: | :---: | :---: |
| **INSERT** | P50 | **256 µs** | **256 µs** | 0 µs |
| | P95 | **256 µs** | **256 µs** | 0 µs |
| | P99 | **512 µs** | **512 µs** | 0 µs |
| **SELECT (PK)** | P50 | **8 µs** | **8 µs** | 0 µs |
| | P95 | **16 µs** | **16 µs** | 0 µs |
| | P99 | **16 µs** | **16 µs** | 0 µs |

---

## 4. Process Memory Footprint (RSS)

Measured via `getrusage(RUSAGE_SELF)` / `mach_task_basic_info`.

| Phase | Memory Usage (RSS) | Description |
| :--- | :---: | :--- |
| **Baseline (Process Init)** | **1,812 KB** | Clean process after `db_init` |
| **Post-Seed (50,000 Rows)** | **29,024 KB** | SQLite B-tree pages loaded into cache |
| **Post-Benchmark Run** | **47,776 KB** | Full run across all 100K benchmarks |
| **Net Growth** | **18,752 KB** | SQLite page cache + WAL index expansion |

*Arkilian's streaming ring buffer is lazily allocated and incurs 0 bytes until S3 shipping is enabled.*

---

## 5. End-to-End S3 Pipeline & Hydration Verification

Run via `./bench_1m --s3-verify`:

```text
  ── S3 API Compliance Check ──────────────────────────────────
  S3 presigned URL validation: linked (hardened stub active)
  S3 HEAD support: enabled (stub handles HEAD 200/404)
  S3 100-continue: enabled
  S3 env: endpoint=http://127.0.0.1:52495 bucket=test-bucket prefix=user-42-appdb HMAC=set
  db_init S3 mode: healthy=0 queue=0
  Writing 1000 rows with S3 streaming enabled...
  after 1000 inserts queue=860 healthy=1 pending=860
  Polling S3 for chunks/manifest/snapshot (waiting for queue drain)...
  post-drain queue=0
  S3: chunks found (PUTs=12)
  S3: manifest.json found
  S3: manifest.sig found
  S3: snapshot found
  manifest: {"version":3,"prefix":"user-42-appdb","snapshot":{"s3_key":"user-42-appdb/backup.sqlite",...}}
  Hydration verify: src rows=1000 qty_sum=51218 total_sum=263575212
                    dst rows=1000 qty_sum=51218 total_sum=263575212
  Hydration: PASS (checksums match)
  Hardened S3 verification: OK (full API emulated)
```

---

## How to Reproduce

Compile the benchmark binary:

```bash
cc -O2 tests/bench_1m.c src/class.c src/hydration.c src/sha256.c src/deps/sqlite/sqlite3.c \
  -Isrc -Isrc/deps/sqlite -lcurl -lpthread -lm -o bench_1m
```

Execute:

```bash
# Full 100,000-operation benchmark
./bench_1m 100000

# Quick run (10,000 operations)
./bench_1m 10000

# Standalone end-to-end S3 pipeline and cold-start hydration test
./bench_1m --s3-verify
```
