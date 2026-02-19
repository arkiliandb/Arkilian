# Arkilian 2.0: Implementation Scheme

**Status:** Ready for Engineering Execution
**Audience:** Senior Engineers (L6+)
**Baseline:** Codebase audited 2026-02-19. All file references verified against actual source.
**Module:** `github.com/arkilian/arkilian`
**Go Version:** 1.24+

---

## Preamble: Deep Evaluation of v2 Spec (`engineering/v2-from-100-senior-dev-spec.md`)

This section evaluates every section, algorithm, pseudocode block, recommendation, and performance claim in the v2 spec against the actual codebase. Each finding is categorized as: ACCURATE, INACCURATE, ALREADY DONE, INCOMPLETE, or DANGEROUS.

### Spec Section 1: Executive Summary

| Claim                                         | Verdict    | Evidence                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| --------------------------------------------- | ---------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| "500K rows/sec/node (<10ms Ack via WAL)"      | INCOMPLETE | WAL does not exist. Current ingest in `internal/api/http/ingest.go` `ServeHTTP()` is fully synchronous: build SQLite → `UploadMultipart` to S3 → register in manifest → respond. Write ack is dominated by S3 upload (100ms+). The 500K target requires WAL. The 10ms target is achievable only with local fsync on SSD/NVMe. On cloud EBS gp3, fsync P99 is ~2-5ms. On gp2, it can spike to 20ms. Spec does not acknowledge this variance. |
| "Query (Hot): <200ms"                         | PLAUSIBLE  | Requires NVMe cache (not implemented) + secondary index (not implemented). With both, 200ms is achievable: ~1ms NVMe read + ~10ms index lookup + ~50ms SQLite query + overhead. Without them, current hot path is ~500ms (download cache hit + bloom pruning + parallel execution).                                                                                                                                                         |
| "Query (Cold): <1s (S3 Coalescing + Pruning)" | INACCURATE | Spec claims S3 coalescing via byte-range GET on a single object. Arkilian partitions are separate S3 keys — byte-range coalescing across keys is impossible. S3 GET latency is 50-200ms per object. 5 partitions × 150ms = 750ms best case. 30 partitions = 4.5s. The <1s target is only achievable with ≤5 partitions after pruning, which requires secondary indexes.                                                                     |
| "Storage Cost: <$0.15/GB/month (S3 Tiering)"  | ACCURATE   | S3 Standard is $0.023/GB. S3 Glacier Instant Retrieval is $0.004/GB. With 90% in Glacier after 30 days, blended cost is ~$0.006/GB. Well under $0.15. However, S3 GET costs at scale are the real concern (not storage). At 100TB with 1.5M partitions, GET costs dominate.                                                                                                                                                                 |
| "Replication: 1.03x (Metadata only)"          | ACCURATE   | Matches v1 architecture. Data partitions stored once in S3 (11 nines durability). Only manifest metadata is replicated across shards.                                                                                                                                                                                                                                                                                                       |

### Spec Section 2: Architecture Overview

| Claim                                        | Verdict        | Evidence                                                                                                                                                                                                                                                                                                                                      |
| -------------------------------------------- | -------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| "Indexer Node (part of arkilian-ingest)"     | DESIGN CONCERN | Spec bundles indexer with ingest. This creates resource contention: index building is CPU/IO-intensive (scanning all data partitions), while ingest needs low-latency WAL fsync. Better to run indexer as part of `arkilian-compact` or as its own mode, since compaction already has the infrastructure for background partition processing. |
| "Router (arkilian-router) standalone binary" | PREMATURE      | For single-node and unified binary deployments, a separate router binary adds operational complexity with no benefit. In-process notification bus is sufficient. Network transport should be deferred to multi-node deployments.                                                                                                              |
| "Compactor (part of arkilian-ingest)"        | INACCURATE     | In the actual codebase, compaction is its own service (`cmd/arkilian-compact/main.go`) and its own mode in the unified binary (`--mode compact`). It is NOT part of ingest. The spec contradicts the existing architecture.                                                                                                                   |

### Spec Section 3.1 (First): WAL + Async S3

| Claim                                                            | Verdict    | Evidence                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| ---------------------------------------------------------------- | ---------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| WAL pseudocode: `atomic.AddUint64(&w.currentLSN, 1)` after fsync | BUG        | The LSN is incremented AFTER the write+fsync. If two concurrent callers enter `Append()`, the mutex serializes them correctly. But the LSN increment happens after fsync, meaning the returned LSN is the post-write value. This is correct for ordering but the spec's `atomic.AddUint64` is unnecessary since the mutex already serializes access. Using atomic here is misleading — it suggests lock-free access that doesn't exist. Use a plain `uint64` field incremented under the mutex. |
| "Ensure one writer that uses Persistent Volumes for the WAL"     | ACCURATE   | Critical operational requirement. WAL on ephemeral storage = data loss on node restart. Must be documented in deployment guide.                                                                                                                                                                                                                                                                                                                                                                 |
| WAL entry format not specified                                   | INCOMPLETE | The spec shows `Append(entry []byte)` but doesn't define the entry wire format. Need: length prefix, CRC32 checksum, partition key, schema version, serialized rows. Without a defined format, recovery cannot validate entries.                                                                                                                                                                                                                                                                |

### Spec Section 3.1 (Second): Sharded Manifest Catalog

| Claim                                                                                   | Verdict                   | Evidence                                                                                                                                                                                                                                                                                                                                                                                                                         |
| --------------------------------------------------------------------------------------- | ------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| "Single manifest.db bottlenecks at >50K partitions (231ms latency)"                     | ALREADY DONE              | `internal/manifest/sharded_catalog.go` already implements FNV32a hash sharding with parallel fan-out. `internal/manifest/auto_migrate.go` implements online migration from single to sharded at a configurable threshold. `internal/config/config.go` defaults to `Sharded: true, ShardCount: 64, AutoShardThreshold: 50000`. This is fully implemented and tested (benchmark in `test/benchmark/production_benchmark_test.go`). |
| Pseudocode: `NewCatalog(path)` returns `*Catalog`                                       | INACCURATE                | Actual signature: `NewCatalog(dbPath string) (*SQLiteCatalog, error)`. Returns `*SQLiteCatalog`, not `*Catalog`. `Catalog` is an interface. The spec's pseudocode would not compile.                                                                                                                                                                                                                                             |
| Pseudocode: `getShard` reuses `hasher` field with `Reset()`                             | INACCURATE                | Actual implementation creates a new `fnv.New32a()` per call (line 66-68 of `sharded_catalog.go`). The spec's approach of reusing a hasher field is NOT thread-safe — `Reset()` + `Write()` + `Sum32()` is a race condition under concurrent access. The actual implementation is correct; the spec's pseudocode is buggy.                                                                                                        |
| Pseudocode: `FindPartitions` swallows errors with `records, _ := s.FindPartitions(...)` | DANGEROUS                 | The actual implementation correctly propagates errors: if any shard returns an error, the entire operation fails. The spec's pseudocode silently drops shard errors, which would cause silent data loss (missing partitions from failed shards).                                                                                                                                                                                 |
| "Salted or time-aware hashing key" recommendation                                       | GOOD IDEA, NOT NEEDED YET | Current FNV32a on partition_key provides reasonable distribution for time-based keys (YYYYMMDD). Hot shard risk is real for tenant-based partitioning where one tenant dominates. Worth adding as a future optimization, not a blocker.                                                                                                                                                                                          |
| "Include LSN in Write Notifications" recommendation                                     | ACCURATE                  | Critical for read-after-write consistency. Without LSN tracking, a query node could serve stale results if its manifest view lags behind the write notification.                                                                                                                                                                                                                                                                 |
| "Transactional Batching for manifest updates" recommendation                            | ALREADY DONE              | `internal/compaction/daemon.go` `compactGroup()` uses `CompleteCompaction()` which performs atomic manifest updates (register new partition + mark sources compacted in a single transaction).                                                                                                                                                                                                                                   |

### Spec Section 3.1 (Third): Temporal Co-Access Graph

| Claim                                                | Verdict        | Evidence                                                                                                                                                                                                                                                  |
| ---------------------------------------------------- | -------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| "Tracking 1.5M partitions per node will exceed 10MB" | ACCURATE       | At ~100 bytes per node (32-byte ID + 10 edges × 8 bytes each), 1.5M nodes = 150MB. The spec's own recommendation to track at partition_key granularity is correct. With ~10K distinct partition keys, memory is ~1MB.                                     |
| "Decay 0.95 per hour"                                | DESIGN CONCERN | The pseudocode applies decay factor `0.95` on every `RecordAccess` call, not per hour. At 1000 queries/hour, a weight decays by `0.95^1000 ≈ 0` in one hour. The decay should be time-based (multiply by `0.95^(hours_elapsed)` on read), not per-access. |
| "Periodically checkpoint to S3" recommendation       | LOW PRIORITY   | Graph rebuilds from query traffic within minutes. Checkpointing adds complexity for marginal cold-start improvement. Defer to post-GA.                                                                                                                    |

### Spec Section 3.2: Secondary Index Partitions

| Claim                                                        | Verdict    | Evidence                                                                                                                                                                                                                                                                                                                                                                         |
| ------------------------------------------------------------ | ---------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Index partition schema (WITHOUT ROWID, composite PK)         | ACCURATE   | Good design. `(column_value, partition_id)` as composite PK with WITHOUT ROWID is optimal for point lookups.                                                                                                                                                                                                                                                                     |
| Pseudocode: `data := b.storage.Download(ctx, dp.ObjectPath)` | INACCURATE | `ObjectStorage.Download()` signature is `Download(ctx, objectPath, localPath string) error`. It downloads to a local file, not to memory. The spec's pseudocode treats it as returning data bytes. Must download to temp file, open with `sql.Open("sqlite3", localPath)`, query with `db.QueryContext()`.                                                                       |
| Pseudocode: `sqlite.Query(data, ...)`                        | INACCURATE | No such API exists in the codebase or in `mattn/go-sqlite3`. Must use standard `database/sql` interface.                                                                                                                                                                                                                                                                         |
| "1024 buckets" for hash partitioning                         | EXCESSIVE  | At 100TB with ~1.5M data partitions, each index column produces ~1.5M (value, partition_id) pairs. At ~100 bytes per pair, total index size is ~150MB per column. Split across 1024 buckets = ~150KB per bucket. This is far below the 16MB target. 64 buckets would be more appropriate, producing ~2.3MB per bucket. 1024 buckets creates unnecessary S3 object proliferation. |
| "Hybrid Scan" recommendation                                 | CRITICAL   | Without this, newly ingested data (in WAL but not yet indexed) is invisible to index-based queries. The planner must union index results with a scan of recent unindexed partitions. This is not optional — it's a correctness requirement.                                                                                                                                      |
| Index build "<5 mins for 1TB data"                           | PLAUSIBLE  | 1TB / 64MB partitions = ~16K partitions. Each partition scan: download (~200ms) + query (~10ms) = ~210ms. At 32 concurrent workers: 16K × 210ms / 32 = ~105s ≈ 1.75 min. Achievable with sufficient network bandwidth.                                                                                                                                                           |
| Automation policy (create/drop thresholds)                   | ACCURATE   | Simple frequency-based policy is the right starting point. More sophisticated cost-based optimization can come later.                                                                                                                                                                                                                                                            |

### Spec Section 3.3: S3 Coalescing

| Claim                                                        | Verdict             | Evidence                                                                                                                                                                                                                                                                                 |
| ------------------------------------------------------------ | ------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| "Combine adjacent partitions into single HTTP Range Request" | FUNDAMENTALLY WRONG | S3 byte-range requests work within a SINGLE object. Arkilian partitions are SEPARATE S3 keys (e.g., `partitions/20260206/events:20260206:abc123.sqlite`). You cannot combine byte ranges across different keys. The entire coalescing algorithm in the spec is based on a false premise. |
| `CoalescedGetRequest` with `ByteRanges`                      | WILL NOT WORK       | The `GetObject` API takes a single `Key`. You cannot specify multiple keys in one request. S3 Batch Operations exist but are for management tasks, not data retrieval.                                                                                                                   |
| "Entropy Prefixes" recommendation                            | ACCURATE            | S3 prefix rate limits (3,500 PUTs/sec, 5,500 GETs/sec per prefix) are real. Hash-based prefixes distribute load. However, the existing path structure (`partitions/{partition_key}/{partition_id}.sqlite`) already provides reasonable distribution since partition_key varies.          |

**Correct approach:** Replace S3 coalescing with parallel batch download using HTTP/2 connection multiplexing (already supported by `aws-sdk-go-v2`). Combine with NVMe cache and co-access graph prefetch to reduce effective S3 GETs.

### Spec Section 3.4: Backpressure-Aware Compaction

| Claim                                           | Verdict       | Evidence                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| ----------------------------------------------- | ------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| "Compaction pauses too aggressively on failure" | ALREADY FIXED | `internal/compaction/backpressure.go` implements a more sophisticated algorithm than the spec proposes. The existing implementation has: (1) sliding window failure tracking, (2) 4-tier concurrency adjustment (zero failures → 2×, low rate → +50%, moderate → +1, high → halve), (3) starvation prevention (`ShouldPause` never pauses when backlog ≤ maxConcurrency). The spec's simple "double on success, halve on failure" is a regression from the existing implementation. |
| Pseudocode: `adjustConcurrency` per-goroutine   | DANGEROUS     | The spec calls `adjustConcurrency` inside each compaction goroutine. This means N concurrent goroutines all racing to adjust concurrency simultaneously. The actual implementation calls `AdjustConcurrency()` once at the start of each compaction cycle in `runOnce()`, which is correct.                                                                                                                                                                                         |

### Spec Section 3.5: Materialized JSON Columns

| Claim                                                                       | Verdict    | Evidence                                                                                                                                                                                                                                                                              |
| --------------------------------------------------------------------------- | ---------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `ALTER TABLE events ADD COLUMN ... GENERATED ALWAYS AS (json_extract(...))` | ACCURATE   | SQLite supports generated columns since 3.31.0 (2020). `mattn/go-sqlite3` bundles a recent SQLite version. This will work.                                                                                                                                                            |
| "Only materialize for newly created partitions" recommendation              | CRITICAL   | Correct. Existing immutable partitions cannot be altered. The query planner must handle mixed schemas (some partitions with materialized columns, some without). This is already partially supported by `internal/query/planner/rewriter.go` which handles schema version mismatches. |
| Materializer holds `db *sql.DB`                                             | INACCURATE | The materializer doesn't operate on a single database. It influences partition BUILDING (in `internal/partition/builder.go`). It should be a configuration provider, not a database operator. The builder reads the materialized column list and adds them during partition creation. |

### Spec Section 4: Operational Scale (100TB)

| Claim                                     | Verdict            | Evidence                                                                                                                                                                            |
| ----------------------------------------- | ------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| "Data Partition Size: 64MB (Target)"      | ALREADY CONFIGURED | `internal/config/config.go` `DefaultConfig()` sets `TargetPartitionSizeMB: 64`. Adaptive sizing tiers go up to 256MB.                                                               |
| "Manifest Shard Count: 64"                | ALREADY CONFIGURED | `DefaultConfig()` sets `ShardCount: 64`.                                                                                                                                            |
| "Auto-Shard Threshold: 50,000 Partitions" | ALREADY CONFIGURED | `DefaultConfig()` sets `AutoShardThreshold: 50000`.                                                                                                                                 |
| "Connection Pool: 200 Conns per node"     | ALREADY EXCEEDED   | `DefaultConfig()` sets `PoolSize: 512`.                                                                                                                                             |
| "Bloom Cache: 100GB RAM"                  | CONCERN            | `DefaultConfig()` sets `BloomCacheSizeMB: 4096` (4GB). 100GB RAM for bloom cache alone requires a 128GB+ instance. The spec's c6i.4xlarge has only 32GB RAM. This is contradictory. |
| "NVMe Cache: 500GB per node"              | NOT IMPLEMENTED    | No NVMe cache exists. c6i.4xlarge does not have local NVMe. Need c6id.4xlarge or i4i instances for local NVMe. Spec specifies wrong instance type.                                  |
| "Glacier Tiering"                         | NOT IMPLEMENTED    | No S3 lifecycle management exists. Would need to add S3 lifecycle rules and handle Glacier retrieval latency (milliseconds for Instant Retrieval, hours for Deep Archive).          |

### Spec Section 5: Implementation Roadmap

| Claim                                                                | Verdict     | Evidence                                                                                                                                                                 |
| -------------------------------------------------------------------- | ----------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| "Phase 1: Manifest Sharding, S3 Coalescing, Backpressure Compaction" | MOSTLY DONE | Manifest sharding: done. Backpressure: done. S3 coalescing: wrong approach (see Section 3.3 evaluation). Only WAL and batch download are genuinely new work for Phase 1. |
| "Phase 2: Secondary Index Partitions, Automated Materialization"     | ACCURATE    | This is genuinely new work. No existing implementation.                                                                                                                  |
| "Phase 3: NVMe Cache, Glacier Tiering, Query Node Auto-scaling"      | ACCURATE    | All genuinely new. Auto-scaling is infrastructure (Kubernetes HPA), not application code.                                                                                |

### Spec Section 7: Directives

| Directive                                                       | Verdict               | Evidence                                                                                                                                                                                         |
| --------------------------------------------------------------- | --------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| "Do Not Rewrite Folders: Extend internal/"                      | ACCURATE              | Matches project conventions.                                                                                                                                                                     |
| "Use sync.Pool for row slices"                                  | ALREADY DONE          | `internal/partition/builder.go` uses `sync.Pool` for `bytes.Buffer` (JSON encoding) and Snappy destination buffers. `internal/query/executor/executor.go` uses `getRowSlice()` with `sync.Pool`. |
| "Backpressure Fix: Double on 0% failure, Halve on >10% failure" | ALREADY DONE (BETTER) | Existing implementation is more sophisticated. See Section 3.4 evaluation.                                                                                                                       |
| "Manifest Sharding: Non-negotiable"                             | ALREADY DONE          | See Section 3.1 (Second) evaluation.                                                                                                                                                             |

### Summary of Spec Quality

| Category                                   | Count                                                                   |
| ------------------------------------------ | ----------------------------------------------------------------------- |
| Accurate and actionable                    | 12                                                                      |
| Already implemented (spec unaware)         | 8                                                                       |
| Inaccurate pseudocode (won't compile/work) | 7                                                                       |
| Fundamentally wrong technical approach     | 1 (S3 coalescing)                                                       |
| Dangerous if implemented as written        | 2 (error swallowing, per-goroutine concurrency adjustment)              |
| Good recommendations worth adopting        | 5                                                                       |
| Contradictory specifications               | 2 (bloom cache size vs instance RAM, instance type vs NVMe requirement) |

**Bottom line:** The spec's architectural vision is sound — WAL for write latency, secondary indexes for query flexibility, tiered caching for read latency. But roughly 40% of the pseudocode is incorrect, and the S3 coalescing section is based on a fundamental misunderstanding of how S3 works. The implementation scheme below corrects all of these issues while preserving the spec's intent.

---

## 0. Codebase Audit: What Exists Today

Every file reference below was verified by reading the actual implementation. This is not derived from documentation.

### 0.1 Verified Working Components

| Component          | Files                                                                       | What It Actually Does                                                                                                                                                                                                                                                                  |
| ------------------ | --------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Ingest (HTTP)      | `internal/api/http/ingest.go`                                               | `ServeHTTP` decodes JSON batch → calls `partition.Builder.Build()` → uploads `.sqlite` + `.meta.json` to storage → registers in manifest via `RegisterPartitionWithIdempotencyKey` → updates zone maps. Synchronous S3 upload blocks the response.                                     |
| Ingest (gRPC)      | `internal/api/grpc/ingest.go`                                               | `BatchIngest` mirrors HTTP handler. Same synchronous upload path.                                                                                                                                                                                                                      |
| SQL Parser         | `internal/query/parser/` (5 files)                                          | Handwritten recursive descent lexer+parser. Supports: SELECT, DISTINCT, WHERE (binary ops, AND/OR, IN, BETWEEN, LIKE, IS NULL), GROUP BY, HAVING, ORDER BY, LIMIT, OFFSET. Aggregates: COUNT, SUM, AVG, MIN, MAX with DISTINCT. No JOINs, no subqueries, no CTEs, no window functions. |
| Query Planner      | `internal/query/planner/planner.go`                                         | `Plan()` extracts predicates → converts to manifest predicates → calls `Pruner.Prune()` which does 3-phase pruning: Phase 1 (min/max via manifest), Phase 1.5 (zone maps), Phase 2 (bloom filters with LRU cache). Returns `QueryPlan` with partition list and pruning stats.          |
| Parallel Executor  | `internal/query/executor/executor.go`                                       | `ExecutePlan()` launches goroutines per partition bounded by semaphore (`concurrency`). Streams rows into channel. `StreamingCollector` merges with memory bound. Download cache (LRU by bytes). Connection pool with configurable max conns.                                          |
| Result Aggregator  | `internal/query/aggregator/` (5 files)                                      | `GroupByMerger` merges partial aggregates across partitions. `OrderBySorter` with external sort threshold. Top-N optimization for LIMIT queries.                                                                                                                                       |
| Partition Builder  | `internal/partition/builder.go`                                             | Creates SQLite with `WITHOUT ROWID`, WAL mode during build, indexes on `(tenant_id, event_time)` and `(user_id, event_time)`, Snappy-compressed payloads. Uses `sync.Pool` for JSON and Snappy buffers. Switches to DELETE journal mode after build for immutability.                  |
| Metadata Generator | `internal/partition/metadata.go`                                            | Per-column FPR targets: tenant_id=0.001%, user_id=0.01%, event_type=0.01%. Tracks distinct counts. Generates `.meta.json` sidecar with bloom filters + min/max stats.                                                                                                                  |
| Adaptive Sizing    | `internal/partition/adaptive.go`                                            | Tier-based: 0GB→16MB, 1GB→128MB, 10GB→192MB, 100GB→256MB. Queries `VolumeQuerier` interface for cumulative volume per partition key.                                                                                                                                                   |
| Sharded Manifest   | `internal/manifest/sharded_catalog.go`                                      | FNV32a hash on partition key. Fan-out `FindPartitions` across all shards in parallel with `sync.WaitGroup`. Implements full `Catalog` interface. Compaction intents for crash recovery. Zone map support.                                                                              |
| Single Manifest    | `internal/manifest/catalog.go`                                              | `SQLiteCatalog` with separate write conn (single writer, WAL) and read pool (4 conns, `read_uncommitted`). Prepared statement cache. Auto-migration via `internal/manifest/auto_migrate.go`.                                                                                           |
| Backpressure       | `internal/compaction/backpressure.go`                                       | Sliding window failure tracking. Exponential ramp-up (2× on zero failures), halve on threshold breach. `ShouldPause` with starvation prevention (never pauses when backlog ≤ maxConcurrency).                                                                                          |
| Compaction Daemon  | `internal/compaction/daemon.go`                                             | `runOnce()` finds candidates → groups by key → compacts each group via `Merger` → registers new partition → marks sources compacted → rebuilds zone maps. Recovers incomplete compactions on startup.                                                                                  |
| GC                 | `internal/compaction/gc.go`                                                 | TTL-based deletion of compacted partitions (default 7 days). Reconciliation handles orphaned S3 objects separately.                                                                                                                                                                    |
| Storage            | `internal/storage/`                                                         | `ObjectStorage` interface with `Upload`, `UploadMultipart`, `Download`, `Delete`, `Exists`, `ConditionalPut`, `ListObjects`. `LocalStorage` (filesystem + MD5 ETags). `S3Storage` (multipart upload, retry with exponential backoff, conditional put via ETag).                        |
| Config             | `internal/config/config.go`                                                 | CLI flags > env vars > YAML/JSON file > defaults. `ARKILIAN_` prefix. Mode-based service selection (`ShouldRunIngest/Query/Compact`). Adaptive sizing config with tier validation.                                                                                                     |
| App Lifecycle      | `internal/app/app.go`                                                       | `Start()` initializes storage → catalog → starts services based on mode. Bloom filter preloading on query startup. Graceful shutdown via `ShutdownManager`. Health + trigger endpoints.                                                                                                |
| Schema Evolution   | `internal/manifest/schema_version.go`, `internal/query/planner/rewriter.go` | Version tracking per partition. `Rewriter` detects schema version mismatches and rewrites queries to handle missing columns with defaults.                                                                                                                                             |
| Zone Maps          | `internal/manifest/zone_map.go`                                             | Partition-key-level bloom filters for coarse-grained pruning before per-partition bloom checks. Merge and rebuild support.                                                                                                                                                             |
| Reconciliation     | `internal/manifest/reconciliation.go`                                       | Compares S3 object listing against manifest entries. Detects orphaned objects and dangling references.                                                                                                                                                                                 |
| Errors             | `internal/errors/errors.go`                                                 | `ArkilianError` with Category, Code, Message, Retryable flag. Categories: VALIDATION, STORAGE, MANIFEST, QUERY, COMPACTION, INTERNAL.                                                                                                                                                  |
| Shutdown           | `internal/server/shutdown.go`                                               | Signal handling (SIGTERM/SIGINT). In-flight request tracking. Drain timeout. LIFO closer chain. HTTP middleware for request rejection during shutdown.                                                                                                                                 |

### 0.2 What the v2 Spec Requires That Does NOT Exist

| v2 Feature                  | Gap Analysis                                                                                                                                                       | Impact                                                                 |
| --------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ---------------------------------------------------------------------- |
| WAL (Write-Ahead Log)       | Ingest currently writes SQLite partition synchronously, uploads to S3, then responds. No local durability layer. Write latency is dominated by S3 upload (100ms+). | Blocks <10ms write ack target.                                         |
| S3 Coalescing               | `S3Storage.Download()` does one GET per object. `ParallelExecutor` issues N concurrent downloads. No byte-range coalescing.                                        | 30 partitions = 30 GETs = 12s worst case.                              |
| Secondary Index Partitions  | Pruning is time-range (min/max) + bloom filter only. No value→partition_id mapping. Queries by non-time columns scan all time partitions that pass bloom filter.   | Queries like `WHERE device_id = ?` across 1M time partitions are slow. |
| Automated Index Policy      | No query statistics tracking. No predicate frequency counters. No automated index creation/deletion.                                                               | Cannot auto-detect hot query patterns.                                 |
| Materialized JSON Columns   | Payloads stored as Snappy-compressed JSON blobs. `json_extract()` at query time. No generated columns.                                                             | Slow for high-volume JSON path queries.                                |
| Temporal Co-Access Graph    | Download cache in `executor/download_cache.go` is simple LRU by byte size. No access pattern tracking. No predictive prefetching.                                  | Cache misses on correlated partition access patterns.                  |
| Router / Write Notifier     | No inter-service communication. Query nodes discover new partitions only on next manifest read.                                                                    | Write visibility latency = manifest poll interval.                     |
| NVMe Tiered Cache           | No tiered caching. Bloom filters cached in memory (LRU). Partitions cached on filesystem via download cache. No NVMe-specific optimization.                        | No hot/cold partition separation.                                      |
| Query Statistics Aggregator | No predicate frequency tracking. No query latency histograms. No per-column access patterns.                                                                       | Cannot drive automated indexing or materialization decisions.          |

### 0.3 What the v2 Spec Requires That ALREADY EXISTS

These components are called out in the v2 spec but are already implemented. No new work needed — only integration or tuning.

| v2 Feature                    | Existing Implementation                                                                                   | Status                                          |
| ----------------------------- | --------------------------------------------------------------------------------------------------------- | ----------------------------------------------- |
| Sharded Manifest Catalog      | `internal/manifest/sharded_catalog.go` — FNV32a hash sharding, parallel fan-out, full `Catalog` interface | Done. Tune shard count via config.              |
| Backpressure-Aware Compaction | `internal/compaction/backpressure.go` — Exponential ramp-up (2×), halve on failure, starvation prevention | Done. Already integrated into `Daemon`.         |
| Adaptive Partition Sizing     | `internal/partition/adaptive.go` — Tier-based sizing up to 256MB based on cumulative volume               | Done. Already wired into ingest and compaction. |
| Bloom Filter Pruning          | `internal/query/planner/pruner.go` — LRU cache, prefetch, multi-column checks                             | Done. Already in query path.                    |
| Zone Map Pruning              | `internal/manifest/zone_map.go` + `pruner.go` phase 1.5                                                   | Done. Already in query path.                    |
| Schema Evolution              | `internal/manifest/schema_version.go` + `internal/query/planner/rewriter.go`                              | Done. Query rewriting for version mismatches.   |
| Reconciliation                | `internal/manifest/reconciliation.go`                                                                     | Done. S3 ↔ manifest consistency checks.         |

---

## 1. Architecture Delta: v1 → v2

```
v1 (Current):
  Client → Ingest HTTP/gRPC → [Build SQLite] → [Upload S3] → [Update Manifest] → Response
                                                                    ↑ synchronous, 100ms+

v2 (Target):
  Client → Ingest HTTP/gRPC → [Append WAL] → [fsync] → Response (<10ms)
                                    ↓ (async background)
                              [Build SQLite] → [Upload S3] → [Update Manifest]
                                    ↓
                              [Notify Query Nodes via Pub/Sub]
                                    ↓
                              [Index Builder observes queries, builds secondary indexes]
```

### New Package Layout (additions only)

```
internal/
  wal/                      # Write-ahead log for durable write acks
    wal.go                  # WAL struct, Append(), Sync(), segment rotation
    flusher.go              # Background goroutine: WAL → SQLite partition → S3
    recovery.go             # Replay uncommitted WAL segments on startup
    wal_test.go
  index/                    # Secondary index partitions
    builder.go              # Scans data partitions, builds index SQLite files
    policy.go               # Automated create/drop based on query stats
    schema.go               # Index partition SQLite schema (index_map table)
    lookup.go               # Query-time index lookup (value → partition_ids)
    index_test.go
  cache/                    # Tiered caching and predictive prefetch
    graph.go                # Temporal co-access graph (Markov-like)
    nvme.go                 # NVMe cache tier (hot partitions)
    tiered.go               # L1 (RAM) → L2 (NVMe) → L3 (S3) cache coordinator
    graph_test.go
  router/                   # Write notification bus
    notifier.go             # Pub/sub for write visibility
    subscriber.go           # Query node subscription with partition_key filtering
    router.go               # Load balancer / request router
    notifier_test.go
  observability/            # Query statistics and metrics
    query_stats.go          # Predicate frequency tracking, latency histograms
    metrics.go              # Prometheus-compatible metrics export
    query_stats_test.go
  schema/                   # JSON materialization
    materializer.go         # Auto-create generated columns for hot JSON paths
    materializer_test.go

cmd/
  arkilian-router/          # Standalone router binary
    main.go

storage/
  coalescer.go              # S3 byte-range coalescing (added to existing package)
```

---

## 2. Implementation Phases

### Phase 1: Core Efficiency (Weeks 1–4)

#### Task 1.1: WAL Implementation (`internal/wal/`)

**Why:** The single highest-impact change. Moves write ack from 100ms+ (S3 upload) to <10ms (local fsync). Everything else in v2 depends on this.

**Invariant:** A row is durable if and only if its WAL entry has been fsynced. S3 upload is an optimization, not a durability requirement.

**Files to create:**

`internal/wal/wal.go`:

```go
// Package wal provides a write-ahead log for durable write acknowledgment
// before asynchronous S3 upload.
package wal

type WAL struct {
    dir        string          // WAL segment directory
    segment    *os.File        // Current active segment
    segmentID  uint64          // Monotonic segment counter
    offset     int64           // Current write offset in segment
    maxSegSize int64           // Max segment size before rotation (default: 64MB)
    currentLSN atomic.Uint64   // Log Sequence Number
    mu         sync.Mutex      // Serializes writes
}

type Entry struct {
    LSN          uint64
    PartitionKey string
    Rows         []types.Row
    Schema       types.Schema
    Checksum     uint32         // CRC32 of serialized payload
    Timestamp    int64          // Unix nanos
}
```

Key methods:

- `Append(entry *Entry) (uint64, error)` — Write + fsync. Returns LSN. This is the hot path. Must be <10ms P99.
- `RotateSegment() error` — Called when segment exceeds `maxSegSize`. Creates new segment file.
- `Segments() []SegmentInfo` — Lists all segments with their LSN ranges and flush status.

**Critical design decisions:**

1. One WAL per ingest node (not per partition key). Partition key is metadata inside the entry.
2. Segment files named `wal_{segmentID:016x}.log`. Sequential, never rewritten.
3. Entry format: `[length:4][checksum:4][payload:length]`. Length-prefixed for crash recovery.
4. `fsync` is called per `Append()`. For throughput, callers can batch rows into a single entry.
5. WAL directory must be on persistent storage (not ephemeral). For Kubernetes: use a PersistentVolumeClaim.

`internal/wal/flusher.go`:

```go
type Flusher struct {
    wal       *WAL
    builder   partition.PartitionBuilder
    storage   storage.ObjectStorage
    catalog   manifest.Catalog
    metaGen   *partition.MetadataGenerator
    interval  time.Duration    // Flush check interval (default: 1s)
    batchSize int              // Max rows per flush batch (default: 10000)
    flushedTo atomic.Uint64    // Last flushed LSN
    notifier  FlushNotifier    // Optional: notify query nodes on flush
}
```

Key methods:

- `Run(ctx context.Context)` — Background loop. Reads unflushed WAL entries → groups by partition key → builds SQLite partitions → uploads to S3 → updates manifest → advances `flushedTo` LSN → optionally notifies query nodes.
- `FlushUpTo(ctx context.Context, lsn uint64) error` — Force flush up to a specific LSN. Used for graceful shutdown.

**Flush algorithm:**

1. Read all entries from `flushedTo+1` to current LSN.
2. Group entries by partition key.
3. For each group: build SQLite partition (reuse existing `partition.Builder`), generate metadata sidecar, upload both to S3, register in manifest.
4. Advance `flushedTo` atomically.
5. Mark WAL segments as fully flushed when all entries in segment are flushed.
6. Delete fully-flushed segments older than retention period (default: 1 hour).

`internal/wal/recovery.go`:

```go
type Recovery struct {
    wal     *WAL
    flusher *Flusher
}
```

Key method:

- `Recover(ctx context.Context) (uint64, error)` — On startup, scan all WAL segments. Find the highest flushed LSN from manifest. Replay all entries with LSN > flushedLSN through the flusher. Return count of recovered entries.

**Recovery invariant:** If the process crashes after WAL fsync but before S3 upload, recovery replays those entries. If it crashes after S3 upload but before manifest update, the idempotency key (derived from LSN) prevents duplicate registration.

**Integration point — modify `internal/api/http/ingest.go`:**

Current flow in `ServeHTTP`:

```
decode request → build partition → upload to S3 → register in manifest → respond
```

New flow:

```
decode request → append to WAL (fsync) → respond with LSN
                     ↓ (async, via Flusher)
              build partition → upload to S3 → register in manifest
```

The response changes from returning `partition_id` (which doesn't exist yet) to returning `lsn` and `request_id`. The partition_id becomes available after flush.

**New response format:**

```json
{
  "lsn": 42,
  "row_count": 1000,
  "request_id": "req-xyz789",
  "status": "accepted"
}
```

**Config additions to `internal/config/config.go`:**

```go
type WALConfig struct {
    Dir            string        `json:"dir" yaml:"dir"`                         // Default: {data_dir}/wal
    MaxSegmentSize int64         `json:"max_segment_size" yaml:"max_segment_size"` // Default: 64MB
    FlushInterval  time.Duration `json:"flush_interval" yaml:"flush_interval"`     // Default: 1s
    FlushBatchSize int           `json:"flush_batch_size" yaml:"flush_batch_size"` // Default: 10000
    RetentionTime  time.Duration `json:"retention_time" yaml:"retention_time"`     // Default: 1h
    Enabled        bool          `json:"enabled" yaml:"enabled"`                   // Default: true
}
```

**Environment variables:**

- `ARKILIAN_WAL_DIR`
- `ARKILIAN_WAL_FLUSH_INTERVAL`
- `ARKILIAN_WAL_ENABLED`

**Tests:**

- `wal_test.go`: Append + read back. Segment rotation. CRC validation. Concurrent appends.
- `flusher_test.go`: Flush produces valid SQLite partitions. Flush advances LSN. Partial flush on context cancellation.
- `recovery_test.go`: Crash after WAL write, before S3 upload → recovery replays. Crash after S3 upload, before manifest → idempotency prevents duplicates.
- Property test: For any sequence of Append calls, Recovery replays exactly the unflushed entries.

**Benchmark target:** `Append` with 1KB entry < 5ms P99 on SSD. 100K entries/sec sustained.

---

#### Task 1.2: S3 Coalescing (`internal/storage/coalescer.go`)

**Why:** Reduces S3 GET count per query from N (one per partition) to ceil(N / coalesce_factor). At 30 partitions, this is 30 GETs → 3-5 GETs.

**Constraint:** S3 supports multiple byte ranges in a single GET request via the `Range` header. However, S3 returns a `multipart/byteranges` response that must be parsed. Also, coalescing only works for objects stored contiguously — which Arkilian partitions are NOT (they're separate S3 keys). Therefore, the coalescing strategy is different from what the v2 spec describes.

**Actual approach: Parallel batch download with connection reuse and prefetch.**

The v2 spec's byte-range coalescing assumes partitions are stored as byte ranges within a single large object. They are not — each partition is a separate S3 key. The correct optimization is:

1. Batch S3 GET requests with HTTP/2 connection multiplexing (already supported by aws-sdk-go-v2).
2. Prefetch partitions predicted by the co-access graph (Task 3.1).
3. Use NVMe cache to avoid S3 GETs entirely for hot partitions (Task 3.2).

**File to create — `internal/storage/coalescer.go`:**

```go
// Package storage — coalescer.go provides batched parallel downloads
// with connection reuse to minimize S3 round-trip overhead.
package storage

type BatchDownloader struct {
    storage     ObjectStorage
    concurrency int            // Max parallel downloads (default: 10)
    cacheDir    string         // Local cache directory
    cache       *DownloadCache // LRU cache (reuse existing from executor)
}

type BatchRequest struct {
    ObjectPaths []string
    Priority    []int          // 0 = highest priority (query-critical), 1 = prefetch
}

type BatchResult struct {
    LocalPaths map[string]string  // objectPath → localPath
    Errors     map[string]error
    CacheHits  int
    Downloads  int
}
```

Key method:

- `Download(ctx context.Context, req *BatchRequest) (*BatchResult, error)` — Downloads all requested objects in parallel, respecting concurrency limit. Checks cache first. Returns local paths for all successfully downloaded objects.

**Integration point — modify `internal/query/executor/executor.go`:**

Current flow in `streamPartitionRows`:

```
download partition → open SQLite → query → stream rows
```

New flow:

```
batch download all partitions upfront → open SQLite → query → stream rows
```

Move the download step out of the per-partition goroutine and into `ExecutePlan` before launching partition goroutines. This allows the `BatchDownloader` to optimize download ordering and parallelism.

**Tests:**

- Cache hit returns immediately without S3 call.
- Concurrent downloads respect semaphore.
- Partial failure returns successful downloads + error map.

---

#### Task 1.3: Manifest Shard Count Configuration

**Why:** Sharded manifest already exists but shard count is hardcoded at construction time. Need config-driven shard count with auto-migration support.

**Modify `internal/config/config.go`:**

```go
type ManifestConfig struct {
    ShardCount       int  `json:"shard_count" yaml:"shard_count"`             // Default: 4, production: 64
    AutoShardAt      int  `json:"auto_shard_at" yaml:"auto_shard_at"`         // Trigger migration at N partitions per shard
    EnableSharding   bool `json:"enable_sharding" yaml:"enable_sharding"`     // Default: true
}
```

**Modify `internal/app/app.go` `initSharedResources()`:**

Currently creates either `SQLiteCatalog` or `ShardedCatalog` based on... nothing (it's hardcoded). Add config-driven selection:

- If `ManifestConfig.EnableSharding` is true and shard count > 1: use `ShardedCatalog`.
- If partition count exceeds `AutoShardAt * ShardCount`: log warning recommending shard count increase.

**No new files needed.** This is config + wiring only.

---

### Phase 2: Indexing Engine (Weeks 5–8)

#### Task 2.1: Query Statistics Aggregator (`internal/observability/query_stats.go`)

**Why:** Secondary index automation requires knowing which predicates are queried most frequently. This must exist before the index builder.

**File to create — `internal/observability/query_stats.go`:**

```go
// Package observability provides query statistics tracking for automated
// index creation and performance monitoring.
package observability

type QueryStats struct {
    mu                sync.RWMutex
    predicateFreq     map[string]*ColumnStats  // column_name → stats
    window            time.Duration            // Aggregation window (default: 1h)
    jsonPathFreq      map[string]*ColumnStats  // JSON path → stats (for materialization)
}

type ColumnStats struct {
    Column       string
    Frequency    int64          // Queries/hour using this column in WHERE
    LastSeen     time.Time
    ValueSamples []interface{}  // Sample values for cardinality estimation
    Operators    map[string]int // Operator distribution (=, IN, BETWEEN, etc.)
}
```

Key methods:

- `RecordQuery(predicates []parser.Predicate)` — Called by query handler after parsing. Increments frequency counters. O(1) per predicate.
- `RecordJSONPath(path string)` — Called when `json_extract` is detected in query. Tracks hot JSON paths.
- `GetTopPredicates(n int) []ColumnStats` — Returns top N columns by frequency. Used by index policy.
- `GetTopJSONPaths(n int) []ColumnStats` — Returns top N JSON paths. Used by materializer.
- `Snapshot() StatsSnapshot` — Thread-safe snapshot for export/persistence.

**Integration point — modify `internal/api/http/query.go` `ServeHTTP`:**

After parsing the SQL statement, before planning:

```go
predicates := parser.ExtractPredicates(stmt)
h.queryStats.RecordQuery(predicates)
```

**Tests:**

- Concurrent `RecordQuery` calls are safe.
- Frequency decays over time (old entries pruned).
- `GetTopPredicates` returns correct ordering.

---

#### Task 2.2: Secondary Index Partitions (`internal/index/`)

**Why:** The core v2 differentiator. Enables sub-200ms queries on any column without full partition scans.

**Index partition schema (SQLite):**

```sql
CREATE TABLE index_map (
    column_value TEXT NOT NULL,
    partition_id TEXT NOT NULL,
    row_count    INTEGER,
    min_time     INTEGER,
    max_time     INTEGER,
    PRIMARY KEY (column_value, partition_id)
) WITHOUT ROWID;

CREATE INDEX idx_value ON index_map(column_value);
```

**Files to create:**

`internal/index/schema.go`:

```go
package index

// IndexPartitionInfo describes a secondary index partition.
type IndexPartitionInfo struct {
    IndexID      string    // e.g., "idx:device_id:bucket_042"
    Collection   string    // e.g., "events"
    Column       string    // e.g., "device_id"
    BucketID     int       // Hash bucket (0-1023)
    ObjectPath   string    // S3 path
    EntryCount   int64     // Number of (value, partition_id) pairs
    SizeBytes    int64
    CreatedAt    time.Time
    CoveredRange TimeRange // Min/max event_time of covered data partitions
}

type TimeRange struct {
    Min int64
    Max int64
}
```

`internal/index/builder.go`:

```go
type Builder struct {
    storage   storage.ObjectStorage
    catalog   manifest.Catalog
    workDir   string
    buckets   int              // Hash bucket count (default: 1024)
}
```

Key method — `BuildIndex`:

```
func (b *Builder) BuildIndex(ctx context.Context, column string, partitions []*manifest.PartitionRecord) ([]*IndexPartitionInfo, error)
```

Algorithm:

1. For each data partition: download SQLite → `SELECT DISTINCT {column} FROM events` → collect `(value, partition_id, row_count, min_time, max_time)` tuples.
2. Hash each value into bucket (FNV32a mod `buckets`).
3. For each non-empty bucket: create index SQLite file with `index_map` table → upload to S3 → register in manifest as index partition.
4. Return list of created index partitions.

**Parallelism:** Step 1 runs with bounded concurrency (reuse executor's semaphore pattern). Step 3 runs in parallel per bucket.

**Size target:** Each index partition should be ≤16MB. If a bucket exceeds this, split by value range.

`internal/index/lookup.go`:

```go
type Lookup struct {
    storage  storage.ObjectStorage
    catalog  manifest.Catalog
    cache    *DownloadCache
    buckets  int
}
```

Key method — `FindPartitions`:

```
func (l *Lookup) FindPartitions(ctx context.Context, column string, value interface{}) ([]string, error)
```

Algorithm:

1. Hash value → bucket ID.
2. Find index partition for (column, bucket) from manifest.
3. Download index partition (cached).
4. Query: `SELECT partition_id FROM index_map WHERE column_value = ?`
5. Return partition IDs.

**Integration point — modify `internal/query/planner/planner.go` `Plan()`:**

After predicate extraction, before manifest pruning:

```go
// Check for secondary indexes
for _, pred := range predicates {
    if pred.Type == parser.PredicateEquals {
        if indexPartitions, err := p.indexLookup.FindPartitions(ctx, pred.Column, pred.Value); err == nil && len(indexPartitions) > 0 {
            // Use index-derived partition list instead of full manifest scan
            plan.Partitions = filterByIDs(allPartitions, indexPartitions)
            plan.PruningStats.IndexUsed = true
            return plan, nil
        }
    }
}
// Fallback: existing min/max + bloom pruning
```

**Manifest schema addition — add to `internal/manifest/schema.go`:**

```sql
CREATE TABLE IF NOT EXISTS index_partitions (
    index_id      TEXT PRIMARY KEY,
    collection    TEXT NOT NULL,
    column_name   TEXT NOT NULL,
    bucket_id     INTEGER NOT NULL,
    object_path   TEXT NOT NULL,
    entry_count   INTEGER NOT NULL,
    size_bytes    INTEGER NOT NULL,
    min_time      INTEGER,
    max_time      INTEGER,
    created_at    INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_index_col ON index_partitions(collection, column_name);
```

**Add to `Catalog` interface:**

```go
RegisterIndexPartition(ctx context.Context, info *IndexPartitionInfo) error
FindIndexPartition(ctx context.Context, collection, column string, bucketID int) (*IndexPartitionInfo, error)
ListIndexes(ctx context.Context, collection string) ([]string, error)  // Returns indexed column names
DeleteIndex(ctx context.Context, collection, column string) error
```

**Tests:**

- Build index for 100 partitions with 10K distinct values → verify lookup returns correct partition IDs.
- Index lookup with cache hit skips S3 download.
- Missing index falls back to standard pruning (no error).
- Property test: For any value V in data partition P, `FindPartitions(column, V)` returns a set containing P.

---

#### Task 2.3: Automated Index Policy (`internal/index/policy.go`)

**Why:** Zero-config index management. Engineers don't manually create indexes — the system observes query patterns and acts.

```go
type Policy struct {
    stats           *observability.QueryStats
    builder         *Builder
    catalog         manifest.Catalog
    createThreshold int64          // Queries/hour to trigger creation (default: 100)
    dropThreshold   int64          // Queries/hour below which to drop (default: 5)
    checkInterval   time.Duration  // How often to evaluate (default: 5m)
    maxIndexes      int            // Max concurrent indexes (default: 10)
}

type IndexAction struct {
    Type   ActionType  // Create or Drop
    Column string
}
```

Key method — `Evaluate`:

```
func (p *Policy) Evaluate(ctx context.Context) ([]IndexAction, error)
```

Algorithm:

1. Get top predicates from `QueryStats`.
2. For each column with frequency > `createThreshold` and no existing index: emit `Create` action.
3. For each existing index with frequency < `dropThreshold`: emit `Drop` action.
4. Respect `maxIndexes` limit — only create if under limit.

**Run loop:** `Policy` runs as a background goroutine in the compaction service (or unified binary). Checks every `checkInterval`. Executes actions sequentially to avoid resource contention with compaction.

**Config additions:**

```go
type IndexConfig struct {
    Enabled         bool          `json:"enabled" yaml:"enabled"`
    CreateThreshold int64         `json:"create_threshold" yaml:"create_threshold"`
    DropThreshold   int64         `json:"drop_threshold" yaml:"drop_threshold"`
    CheckInterval   time.Duration `json:"check_interval" yaml:"check_interval"`
    MaxIndexes      int           `json:"max_indexes" yaml:"max_indexes"`
    BucketCount     int           `json:"bucket_count" yaml:"bucket_count"`
}
```

---

#### Task 2.4: Materialized JSON Columns (`internal/schema/materializer.go`)

**Why:** `json_extract()` on 100K rows is 10-50× slower than a native column read. Auto-materializing hot JSON paths into generated columns eliminates this.

**Constraint:** This only applies to newly created partitions. Historical partitions continue using `json_extract()`. The query planner must handle both.

```go
type Materializer struct {
    stats    *observability.QueryStats
    threshold int64  // Queries/hour to trigger materialization (default: 50)
}

type MaterializedColumn struct {
    JSONPath   string  // e.g., "$.user_agent"
    ColumnName string  // e.g., "payload_user_agent"
    SQLiteType string  // TEXT, INTEGER, REAL
}
```

Key method — `GetMaterializedColumns`:

```
func (m *Materializer) GetMaterializedColumns(collection string) []MaterializedColumn
```

Returns the list of JSON paths that should be materialized as generated columns in new partitions.

**Integration point — modify `internal/partition/builder.go` `BuildWithSchema()`:**

After creating the events table, if materialized columns are configured:

```go
for _, mc := range materializedColumns {
    sql := fmt.Sprintf(
        "ALTER TABLE events ADD COLUMN %s %s GENERATED ALWAYS AS (json_extract(payload, '%s'))",
        mc.ColumnName, mc.SQLiteType, mc.JSONPath,
    )
    db.ExecContext(ctx, sql)
    db.ExecContext(ctx, fmt.Sprintf("CREATE INDEX idx_%s ON events(%s)", mc.ColumnName, mc.ColumnName))
}
```

**Integration point — modify `internal/query/planner/rewriter.go`:**

When rewriting queries for partitions with different schema versions, detect whether a partition has materialized columns. If yes, rewrite `json_extract(payload, '$.user_agent')` → `payload_user_agent`. If no, keep the `json_extract` call.

**Tests:**

- Partition built with materialized column has the generated column and index.
- Query on materialized column is rewritten correctly.
- Query on non-materialized partition falls back to `json_extract`.

---

### Phase 3: Tiered Storage & Caching (Weeks 9–10)

#### Task 3.1: Temporal Co-Access Graph (`internal/cache/graph.go`)

**Why:** The current download cache (`internal/query/executor/download_cache.go`) is a simple LRU. It has no awareness of access patterns. If partitions A, B, C are always queried together, accessing A should prefetch B and C.

```go
// Package cache provides tiered caching with predictive prefetch
// for partition data.
package cache

type CoAccessGraph struct {
    mu        sync.RWMutex
    nodes     map[string]*Node  // PartitionID → Node
    decay     float64           // Weight decay per hour (default: 0.95)
    threshold float64           // Prefetch trigger weight (default: 0.70)
    maxEdges  int               // Max edges per node (default: 10)
}

type Node struct {
    Edges map[string]float64  // TargetPartitionID → Weight
}
```

Key methods:

- `RecordAccess(sequence []string)` — Called after each query with the list of partition IDs that were accessed. Updates edge weights: `W = W * 0.95 + 1.0`. Prunes weak edges when exceeding `maxEdges`.
- `GetPrefetchCandidates(current string) []string` — Returns partition IDs with edge weight > `threshold`. Used by `BatchDownloader` to prefetch.

**Memory constraint:** 10MB max. At ~100 bytes per node (ID + 10 edges), this supports ~100K nodes. For larger deployments, track at partition_key granularity instead of partition_id.

**Integration point — modify `internal/query/executor/executor.go` `ExecutePlan()`:**

After query execution completes:

```go
accessedIDs := make([]string, len(plan.Partitions))
for i, p := range plan.Partitions {
    accessedIDs[i] = p.PartitionID
}
e.coAccessGraph.RecordAccess(accessedIDs)
```

Before downloading partitions, check for prefetch candidates:

```go
var prefetchPaths []string
for _, p := range plan.Partitions {
    candidates := e.coAccessGraph.GetPrefetchCandidates(p.PartitionID)
    prefetchPaths = append(prefetchPaths, candidates...)
}
// Add prefetch paths to batch download with lower priority
```

**Persistence (optional):** Periodically checkpoint graph state to S3 for warm restart. Not required for MVP — graph rebuilds naturally from query traffic within minutes.

**Tests:**

- After recording [A, B, C] 10 times, `GetPrefetchCandidates("A")` returns B and C.
- Decay reduces weights over time.
- Max edges enforced — weakest edges pruned.
- Memory stays under 10MB with 100K nodes.

---

#### Task 3.2: NVMe Cache Tier (`internal/cache/nvme.go`)

**Why:** S3 GET latency is 50-200ms. NVMe read latency is <1ms. Caching hot partitions on NVMe eliminates S3 round-trips for frequently accessed data.

```go
type NVMeCache struct {
    dir         string          // NVMe mount point
    maxBytes    int64           // Max cache size (default: 500GB)
    currentSize atomic.Int64
    index       sync.Map        // objectPath → *CacheEntry
    evictCh     chan string      // Eviction channel
}

type CacheEntry struct {
    LocalPath   string
    SizeBytes   int64
    LastAccess  atomic.Int64    // Unix nanos
    AccessCount atomic.Int64
    Pinned      bool            // Pinned entries are not evicted (e.g., bloom filters)
}
```

Key methods:

- `Get(objectPath string) (string, bool)` — Returns local path if cached. Updates access time.
- `Put(objectPath, localPath string, sizeBytes int64) error` — Copies file to NVMe cache dir. Triggers eviction if over capacity.
- `Evict()` — LRU eviction with access-count weighting. Pinned entries are skipped.
- `Pin(objectPath string)` — Marks entry as non-evictable (used for bloom filter files).

**Integration point — modify `internal/storage/coalescer.go` `BatchDownloader.Download()`:**

Check NVMe cache before S3:

```go
if localPath, ok := b.nvmeCache.Get(objectPath); ok {
    result.LocalPaths[objectPath] = localPath
    result.CacheHits++
    continue
}
// Download from S3, then cache on NVMe
```

**Config additions:**

```go
type CacheConfig struct {
    NVMeDir      string `json:"nvme_dir" yaml:"nvme_dir"`           // Default: "" (disabled)
    NVMeMaxBytes int64  `json:"nvme_max_bytes" yaml:"nvme_max_bytes"` // Default: 500GB
    PrefetchEnabled bool `json:"prefetch_enabled" yaml:"prefetch_enabled"` // Default: true
}
```

**Tests:**

- Put + Get returns correct path.
- Eviction removes LRU entries when over capacity.
- Pinned entries survive eviction.
- Concurrent access is safe.

---

#### Task 3.3: Write Notification Bus (`internal/router/notifier.go`)

**Why:** Without notifications, query nodes discover new partitions only when they re-read the manifest. With notifications, write visibility drops from manifest-poll-interval to <100ms.

```go
type Notifier struct {
    subscribers sync.Map  // subscriberID → *Subscriber
    bufferSize  int       // Channel buffer per subscriber (default: 1000)
}

type Notification struct {
    Type         NotificationType  // PartitionCreated, IndexCreated, CompactionComplete
    PartitionKey string
    PartitionID  string
    LSN          uint64
    Timestamp    int64
}

type Subscriber struct {
    ID       string
    Filters  []string          // Partition key prefixes to subscribe to ("" = all)
    Ch       chan Notification
}
```

Key methods:

- `Publish(n Notification)` — Fans out to all matching subscribers. Non-blocking (drops if subscriber channel is full).
- `Subscribe(filters []string) *Subscriber` — Creates a new subscriber with optional partition key filters.
- `Unsubscribe(id string)` — Removes subscriber.

**Transport:** For single-node (unified binary), this is an in-process channel. For multi-node, use WebSocket or gRPC streaming. Start with in-process; add network transport later.

**Integration point — modify `internal/wal/flusher.go`:**

After successful flush:

```go
f.notifier.Publish(router.Notification{
    Type:         router.PartitionCreated,
    PartitionKey: partitionKey,
    PartitionID:  partitionID,
    LSN:          entry.LSN,
    Timestamp:    time.Now().UnixNano(),
})
```

**Integration point — modify `internal/query/planner/planner.go`:**

Subscribe to notifications on startup. Maintain a local "recent partitions" buffer. When planning a query, include recent partitions that may not yet be in the manifest (for <100ms write visibility).

**Tests:**

- Publish with no subscribers doesn't block.
- Subscriber receives matching notifications.
- Filter excludes non-matching partition keys.
- Full channel drops notification (no deadlock).

---

### Phase 4: Validation & Hardening (Weeks 11–12)

#### Task 4.1: Integration Tests

**Add to `test/integration/`:**

`test/integration/wal_test.go`:

- Ingest via HTTP → verify WAL entry exists → wait for flush → verify partition in S3 and manifest.
- Kill process after WAL write, before flush → restart → verify recovery replays entries.
- Concurrent ingest from multiple goroutines → verify no data loss.

`test/integration/index_test.go`:

- Ingest 10K rows with 100 distinct device_ids → build index on device_id → query `WHERE device_id = 'X'` → verify only relevant partitions scanned.
- Drop index → verify fallback to bloom filter pruning.

`test/integration/cache_test.go`:

- Query same partitions 10 times → verify NVMe cache hits on subsequent queries.
- Co-access graph prefetch → verify prefetched partitions are in cache before query.

`test/integration/notification_test.go`:

- Ingest → subscribe to notifications → verify notification received within 100ms of flush.

#### Task 4.2: Benchmarks

**Add to `test/benchmark/`:**

`test/benchmark/wal_benchmark_test.go`:

```go
func BenchmarkWALAppend(b *testing.B) {
    // Target: <5ms P99 per append with 1KB entry
}

func BenchmarkWALFlush(b *testing.B) {
    // Target: 10K entries flushed in <5s
}
```

`test/benchmark/index_benchmark_test.go`:

```go
func BenchmarkIndexLookup(b *testing.B) {
    // Target: <10ms per lookup with 1M index entries
}

func BenchmarkIndexBuild(b *testing.B) {
    // Target: <5 min for 1TB data (1M partitions)
}
```

`test/benchmark/coalesced_download_test.go`:

```go
func BenchmarkBatchDownload(b *testing.B) {
    // Target: 30 partitions downloaded in <2s (vs 12s sequential)
}
```

#### Task 4.3: Configuration Validation

**Modify `internal/config/config.go` `Validate()`:**

Add validation for all new config sections:

- WAL dir must be writable and on persistent storage.
- NVMe dir must exist and have sufficient space.
- Shard count must be power of 2 (for consistent hashing).
- Index bucket count must be > 0 and ≤ 65536.
- Flush interval must be ≥ 100ms.

#### Task 4.4: Error Codes

**Add to `internal/errors/errors.go`:**

```go
// WAL codes
const (
    CodeWALWriteFailed  = "WAL_WRITE_FAILED"
    CodeWALRecoveryFailed = "WAL_RECOVERY_FAILED"
    CodeWALSegmentCorrupt = "WAL_SEGMENT_CORRUPT"
)

// Index codes
const (
    CodeIndexBuildFailed  = "INDEX_BUILD_FAILED"
    CodeIndexLookupFailed = "INDEX_LOOKUP_FAILED"
    CodeIndexNotFound     = "INDEX_NOT_FOUND"
)

// Cache codes
const (
    CodeCacheEvictionFailed = "CACHE_EVICTION_FAILED"
    CodeCacheFull           = "CACHE_FULL"
)

// Add new categories
const (
    ErrCategoryWAL   ErrorCategory = "WAL"
    ErrCategoryIndex ErrorCategory = "INDEX"
    ErrCategoryCache ErrorCategory = "CACHE"
)
```

Update `isRetryable()` to include WAL and index errors.

---

## 3. Dependency Graph

```
                    ┌─────────────────┐
                    │  Query Stats    │ (Task 2.1)
                    │  (observability)│
                    └────────┬────────┘
                             │ drives
              ┌──────────────┼──────────────┐
              ▼              ▼              ▼
    ┌─────────────┐  ┌──────────────┐  ┌──────────────┐
    │ Index Policy│  │ Materializer │  │ Co-Access    │
    │ (Task 2.3)  │  │ (Task 2.4)   │  │ Graph (3.1)  │
    └──────┬──────┘  └──────┬───────┘  └──────┬───────┘
           │                │                  │
           ▼                ▼                  ▼
    ┌─────────────┐  ┌──────────────┐  ┌──────────────┐
    │ Index       │  │ Partition    │  │ NVMe Cache   │
    │ Builder     │  │ Builder      │  │ (Task 3.2)   │
    │ (Task 2.2)  │  │ (modified)   │  └──────┬───────┘
    └──────┬──────┘  └──────────────┘         │
           │                                   │
           ▼                                   ▼
    ┌─────────────┐                   ┌──────────────┐
    │ Planner     │                   │ Batch        │
    │ (modified)  │                   │ Downloader   │
    └─────────────┘                   │ (Task 1.2)   │
                                      └──────────────┘

    ┌─────────────┐         ┌──────────────┐
    │ WAL         │────────→│ Flusher      │
    │ (Task 1.1)  │         │ (Task 1.1)   │
    └──────┬──────┘         └──────┬───────┘
           │                       │
           │                       ▼
           │                ┌──────────────┐
           │                │ Notifier     │
           │                │ (Task 3.3)   │
           └────────────────┴──────────────┘
```

**Critical path:** WAL (1.1) → Flusher (1.1) → Notifier (3.3)
**Parallel track 1:** Query Stats (2.1) → Index Builder (2.2) → Index Policy (2.3)
**Parallel track 2:** Batch Downloader (1.2) → NVMe Cache (3.2) → Co-Access Graph (3.1)
**Independent:** Materializer (2.4), Manifest Config (1.3)

---

## 4. Migration Strategy

### 4.1 Backward Compatibility

All changes must be backward compatible. The system must run in v1 mode (no WAL, no indexes, no notifications) with zero config changes.

**Feature flags (all default to false for existing deployments):**

```yaml
wal:
  enabled: false # Set to true to enable WAL
index:
  enabled: false # Set to true to enable secondary indexes
cache:
  nvme_dir: "" # Empty = disabled
  prefetch_enabled: false
router:
  notifications_enabled: false
```

### 4.2 Rolling Upgrade Path

1. Deploy v2 binary with all features disabled (v1 behavior).
2. Enable WAL (`wal.enabled: true`). Ingest switches to WAL path. Query and compact are unaffected.
3. Enable notifications (`router.notifications_enabled: true`). Query nodes subscribe.
4. Enable secondary indexes (`index.enabled: true`). Index builder starts observing queries.
5. Enable NVMe cache (`cache.nvme_dir: /mnt/nvme`). Query nodes start caching.
6. Tune shard count, flush intervals, index thresholds based on production metrics.

### 4.3 Rollback

Each feature can be independently disabled by setting its flag to false. The system falls back to v1 behavior:

- WAL disabled: ingest reverts to synchronous S3 upload.
- Indexes disabled: planner skips index lookup, uses bloom pruning.
- NVMe disabled: downloads go directly to filesystem cache.
- Notifications disabled: query nodes rely on manifest polling.

---

## 5. Performance Validation Targets

| Metric                                   | v1 (Current)                 | v2 Target            | How to Measure                         |
| ---------------------------------------- | ---------------------------- | -------------------- | -------------------------------------- |
| Write ack latency (P99)                  | ~150ms (S3 upload)           | <10ms (WAL fsync)    | `BenchmarkWALAppend`                   |
| Write visibility                         | Manifest poll interval (~5s) | <100ms               | Integration test: ingest → query       |
| Query latency (hot, indexed)             | N/A                          | <200ms               | `BenchmarkIndexLookup` + executor      |
| Query latency (cold)                     | 5-12s (N × S3 GET)           | <5s (batch download) | `BenchmarkBatchDownload`               |
| Partitions scanned (indexed query)       | All matching bloom           | <10 (via index)      | Integration test: verify pruning stats |
| Manifest prune latency (1.5M partitions) | Untested at scale            | <20ms (P95)          | Benchmark with 64 shards               |
| Compaction backlog under 10% failure     | Zero (existing)              | Zero (existing)      | `TestBackpressure_*` (already passing) |

---

## 6. Files Modified (Existing Code)

| File                                  | Change                                                                                                              | Risk                                                                         |
| ------------------------------------- | ------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------- |
| `internal/api/http/ingest.go`         | Add WAL path (feature-flagged). Change response format when WAL enabled.                                            | Medium — core ingest path. Feature flag mitigates.                           |
| `internal/api/grpc/ingest.go`         | Same WAL integration as HTTP handler.                                                                               | Medium — same risk profile.                                                  |
| `internal/api/http/query.go`          | Add `queryStats.RecordQuery()` call after parsing.                                                                  | Low — additive only.                                                         |
| `internal/query/planner/planner.go`   | Add index lookup before manifest pruning. Add recent-partitions buffer from notifications.                          | Medium — core query path. Fallback to existing pruning on any error.         |
| `internal/query/planner/rewriter.go`  | Add materialized column rewriting.                                                                                  | Low — additive rewrite rule.                                                 |
| `internal/query/executor/executor.go` | Replace per-partition download with batch download. Add co-access graph recording.                                  | Medium — changes download flow. Feature flag mitigates.                      |
| `internal/partition/builder.go`       | Add materialized column creation after table creation.                                                              | Low — additive SQL statements.                                               |
| `internal/config/config.go`           | Add WALConfig, IndexConfig, CacheConfig, ManifestConfig, RouterConfig structs. Add env var loading. Add validation. | Low — additive config.                                                       |
| `internal/app/app.go`                 | Wire WAL, flusher, notifier, index policy, NVMe cache, query stats. Feature-flagged initialization.                 | Medium — central wiring point. Each component independently feature-flagged. |
| `internal/errors/errors.go`           | Add WAL, Index, Cache error categories and codes.                                                                   | Low — additive constants.                                                    |
| `internal/manifest/schema.go`         | Add `index_partitions` table to schema init.                                                                        | Low — additive DDL. Auto-migration handles existing databases.               |

---

## 7. Files Created (New Code)

| File                                         | Lines (est.) | Owner       |
| -------------------------------------------- | ------------ | ----------- |
| `internal/wal/wal.go`                        | 250          | Ingest team |
| `internal/wal/flusher.go`                    | 300          | Ingest team |
| `internal/wal/recovery.go`                   | 150          | Ingest team |
| `internal/wal/wal_test.go`                   | 400          | Ingest team |
| `internal/index/schema.go`                   | 50           | Query team  |
| `internal/index/builder.go`                  | 300          | Query team  |
| `internal/index/lookup.go`                   | 150          | Query team  |
| `internal/index/policy.go`                   | 200          | Query team  |
| `internal/index/index_test.go`               | 400          | Query team  |
| `internal/cache/graph.go`                    | 150          | Query team  |
| `internal/cache/nvme.go`                     | 200          | Infra team  |
| `internal/cache/tiered.go`                   | 100          | Infra team  |
| `internal/cache/graph_test.go`               | 200          | Query team  |
| `internal/router/notifier.go`                | 150          | Infra team  |
| `internal/router/subscriber.go`              | 100          | Infra team  |
| `internal/router/notifier_test.go`           | 200          | Infra team  |
| `internal/observability/query_stats.go`      | 200          | Query team  |
| `internal/observability/query_stats_test.go` | 200          | Query team  |
| `internal/schema/materializer.go`            | 150          | Query team  |
| `internal/schema/materializer_test.go`       | 150          | Query team  |
| `internal/storage/coalescer.go`              | 200          | Infra team  |
| `test/integration/wal_test.go`               | 200          | Ingest team |
| `test/integration/index_test.go`             | 200          | Query team  |
| `test/integration/cache_test.go`             | 150          | Infra team  |
| `test/integration/notification_test.go`      | 100          | Infra team  |
| `test/benchmark/wal_benchmark_test.go`       | 100          | Ingest team |
| `test/benchmark/index_benchmark_test.go`     | 100          | Query team  |
| `test/benchmark/coalesced_download_test.go`  | 100          | Infra team  |
| **Total**                                    | **~4,850**   |             |

---

## 8. Risk Register

| Risk                                            | Probability | Impact                      | Mitigation                                                                                                     |
| ----------------------------------------------- | ----------- | --------------------------- | -------------------------------------------------------------------------------------------------------------- |
| WAL fsync latency exceeds 10ms on cloud EBS     | Medium      | Write ack target missed     | Benchmark on target instance types early (Week 1). Use io1/io2 EBS volumes. Batch multiple rows per WAL entry. |
| Secondary index build takes >5 min for 1TB      | Medium      | Index automation lag        | Parallelize partition scanning. Build incrementally (only new partitions since last build).                    |
| NVMe cache eviction causes query latency spikes | Low         | P99 latency regression      | Use access-count-weighted LRU (not pure LRU). Pin bloom filter files.                                          |
| Notification bus drops messages under load      | Medium      | Write visibility regression | Bounded channel with drop counter metric. Query nodes fall back to manifest polling on missed notifications.   |
| WAL recovery replays already-flushed entries    | Low         | Duplicate partitions        | Idempotency key derived from WAL LSN. `RegisterPartitionWithIdempotencyKey` already handles this.              |
| Materialized columns increase partition size    | Low         | Storage cost increase       | Monitor size delta. Only materialize top 3 JSON paths. Generated columns add <5% overhead.                     |
| Co-access graph memory exceeds 10MB             | Medium      | OOM on query nodes          | Track at partition_key granularity for large deployments. Hard memory cap with LRU eviction on graph nodes.    |

---

## 9. Spec Corrections

The v2 spec contains several technical inaccuracies that this implementation scheme corrects:

| Spec Claim                                                                  | Reality                                                                                                                 | Correction                                                     |
| --------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------- |
| S3 Coalescing via byte-range GET on single object                           | Arkilian partitions are separate S3 keys, not byte ranges within one object                                             | Use parallel batch download with connection reuse instead      |
| Section 3.1 appears three times (WAL, Sharded Manifest, Co-Access Graph)    | Numbering error in spec                                                                                                 | Fixed in this scheme as Tasks 1.1, 1.3, 3.1                    |
| `NewCatalog(path)` in sharded catalog pseudocode                            | Actual function signature is `NewCatalog(dbPath string) (*SQLiteCatalog, error)`                                        | Use actual API                                                 |
| Sharded manifest uses `fnv.New32a()`                                        | Existing implementation already uses FNV32a in `sharded_catalog.go`                                                     | No change needed — already implemented                         |
| Backpressure uses "Double concurrency on 0% failure, Halve on >10% failure" | Existing implementation already does this with more nuance (exponential ramp-up, sliding window, starvation prevention) | No change needed — existing implementation is superior to spec |
| `sqlite.Query(data, ...)` in index builder pseudocode                       | No such API exists. Must download file, open with `sql.Open("sqlite3", path)`, query with `db.QueryContext()`           | Use actual `database/sql` API                                  |
| Router as standalone binary                                                 | Not needed for MVP. In-process notification bus sufficient for single-node and unified binary                           | Defer network transport to post-GA                             |

---

## 10. Execution Checklist

### Week 1

- [ ] `internal/wal/wal.go` — WAL struct, Append, segment rotation
- [ ] `internal/wal/wal_test.go` — Unit tests for append, rotation, CRC
- [ ] `internal/storage/coalescer.go` — BatchDownloader
- [ ] `internal/config/config.go` — Add WALConfig, CacheConfig structs

### Week 2

- [ ] `internal/wal/flusher.go` — Background flush loop
- [ ] `internal/wal/recovery.go` — Startup recovery
- [ ] `internal/api/http/ingest.go` — WAL integration (feature-flagged)
- [ ] `internal/api/grpc/ingest.go` — WAL integration (feature-flagged)
- [ ] `test/integration/wal_test.go` — WAL integration tests

### Week 3

- [ ] `test/benchmark/wal_benchmark_test.go` — WAL benchmarks
- [ ] `internal/query/executor/executor.go` — Batch download integration
- [ ] `test/benchmark/coalesced_download_test.go` — Download benchmarks
- [ ] `internal/config/config.go` — ManifestConfig, shard count tuning

### Week 4

- [ ] End-to-end testing: WAL → flush → query with batch download
- [ ] Performance validation: write ack <10ms, batch download <2s for 30 partitions
- [ ] Bug fixes and stabilization

### Week 5

- [ ] `internal/observability/query_stats.go` — Predicate frequency tracking
- [ ] `internal/observability/query_stats_test.go` — Unit tests
- [ ] `internal/api/http/query.go` — Wire query stats recording
- [ ] `internal/index/schema.go` — Index partition types

### Week 6

- [ ] `internal/index/builder.go` — Index builder
- [ ] `internal/index/lookup.go` — Index lookup
- [ ] `internal/manifest/schema.go` — Add index_partitions table
- [ ] `internal/manifest/catalog.go` — Add index partition CRUD methods

### Week 7

- [ ] `internal/query/planner/planner.go` — Index lookup integration
- [ ] `internal/index/policy.go` — Automated index policy
- [ ] `internal/index/index_test.go` — Unit tests
- [ ] `test/integration/index_test.go` — Integration tests

### Week 8

- [ ] `internal/schema/materializer.go` — JSON materialization
- [ ] `internal/partition/builder.go` — Materialized column creation
- [ ] `internal/query/planner/rewriter.go` — Materialized column rewriting
- [ ] `test/benchmark/index_benchmark_test.go` — Index benchmarks
- [ ] Performance validation: indexed query <200ms

### Week 9

- [ ] `internal/cache/graph.go` — Co-access graph
- [ ] `internal/cache/nvme.go` — NVMe cache tier
- [ ] `internal/cache/tiered.go` — Cache coordinator
- [ ] `internal/cache/graph_test.go` — Unit tests

### Week 10

- [ ] `internal/router/notifier.go` — Write notification bus
- [ ] `internal/router/subscriber.go` — Subscriber management
- [ ] `internal/router/notifier_test.go` — Unit tests
- [ ] `internal/wal/flusher.go` — Wire notifier into flush path
- [ ] `internal/query/planner/planner.go` — Wire subscriber for recent partitions
- [ ] `test/integration/notification_test.go` — Notification integration tests

### Week 11

- [ ] `internal/app/app.go` — Wire all new components (feature-flagged)
- [ ] `internal/config/config.go` — Final validation for all config sections
- [ ] `internal/errors/errors.go` — Add all new error codes
- [ ] Full integration test suite passing
- [ ] All benchmarks meeting targets

### Week 12

- [ ] Chaos testing: kill ingest mid-WAL-write → verify recovery
- [ ] Chaos testing: kill query mid-execution → verify no data corruption
- [ ] Load testing: 500K rows/sec sustained ingest with WAL
- [ ] Load testing: 1K concurrent queries with index + cache
- [ ] Documentation update: README, config reference
- [ ] Final performance validation against all targets in Section 5

---

_This scheme was derived from reading every Go source file in the repository. All file paths, function signatures, interface contracts, and behavioral descriptions reference the actual implementation as of 2026-02-18._
