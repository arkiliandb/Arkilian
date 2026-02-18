# Project Arkilian 2.0: Final Architecture & Operational Scale Specification 

## 1. Executive Summary

Arkilian 2.0 is a **distributed operational database** designed for high-efficiency storage and querying of data at petabyte scale. It combines MongoDB-like schema flexibility (JSON payloads) with SQL query power, optimized for workloads where write throughput and cost efficiency are paramount, and query latency targets are sub-second (hot) to few-seconds (cold).

**Core Philosophy:**  
1.  **Micro-Partitions:** Data is written once to SQLite files (8-64MB) and stored in object storage (S3).  
2.  **Decoupled Access Paths:** Physical data layout is time-optimized; query efficiency is achieved via **Automated Secondary Index Partitions**.  
3.  **Stateless Compute:** Query and Ingest nodes are ephemeral; state resides in S3 and the Sharded Manifest.

**Key Metrics (100TB Target):**  
*   **Ingest:** 500K rows/sec/node (<10ms Ack via WAL).  
*   **Query (Hot):** <200ms (NVMe Cache + Secondary Index).  
*   **Query (Cold):** <1s (S3 Coalescing + Pruning).  
*   **Storage Cost:** <$0.15/GB/month (S3 Tiering).  
*   **Replication:** 1.03x (Metadata only).  

---

## 2. Revised Architecture Overview

### 2.1 High-Level Diagram

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              Client Applications                                 │
│                         (HTTP/gRPC/SQL Consumers)                                │
└───────────────────────────────────┬─────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                    Central Load Balancer / Router                               │
│  ┌─────────────────┐  ┌─────────────────┐           
│  │ Query Router    │  │ Write Notifier  │              
│  │ (Read Traffic)  │  │ (Pub/Sub)       │                  
│  └────────┬────────┘  └────────┬────────┘                  
│           │                    │                           
│           └────────────────────┼────────────
│                                │                                               │
│           ┌────────────────────┼────────────────────┐                          │
│           ▼                    ▼                    ▼                          │
│  ┌─────────────────────┐      ┌─────────────────┐
│  │   Ingest Node       │      │   Query Node    │
│  │   (WAL + S3)        │      │   (Cache + SQL) │
│  │(Also Indexer Node)  │      │   (Cache + SQL) │
│  └────────┬────────────┘      └────────┬────────┘
│           │                            │
│           └────────────────────────────┼
│                                │                                               │
│                                ▼                                               │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │                    Sharded Manifest Catalog (S3 + Local Cache)          │   │
│  │  • Tracks Data Partitions & Index Partitions                            │   │
│  │  • Auto-Sharded at 50K Partitions                                       │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                │                                               │
│                                ▼                                               │
└───────────────────────────────────┬─────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         Tiered Object Storage (S3)                              │
│  ┌─────────────────────┐  ┌─────────────────────┐  ┌─────────────────────┐     │
│  │   Data Partitions   │  │  Index Partitions   │  │   Manifest Shards   │     │
│  │   (SQLite 64MB)     │  │   (SQLite 16MB)     │  │   (SQLite 100MB)    │     │
│  │   • Time-Ordered    │  │   • Value -> IDs    │  │   • Metadata        │     │
│  └─────────────────────┘  └─────────────────────┘  └─────────────────────┘     │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 2.2 Component Roles

| Component | Binary | Responsibility | Key Algorithm |
| :--- | :--- | :--- | :--- |
| **Ingest Node** | `arkilian-ingest` (standalone binary) | Accept writes, WAL fsync, Async S3 flush. | WAL Durability + Batch Partitioning. |
| **Query Node** | `arkilian-query` (standalone binary) | SQL parsing, Pruning, Execution, Caching. | 2-Phase Pruning + S3 Coalescing. |
| **Indexer Node** | `arkilian-indexer` (part of `arkilian-ingest`) | Observe queries, Build Secondary Indexes. | Automated Index Partition Creation. |
| **Compactor** | `arkilian-compact` (part of `arkilian-ingest`) | Merge small partitions, GC, Tiering. | Backpressure-Aware Compaction. |
| **Router** | `arkilian-router` (standalone binary) | Load balancing, Write Notifications. | WebSocket Pub/Sub. |

**Recommendation (Advanced):** To prevent a "Notification Storm" at 500K rows/sec, implement Subscription Filtering so Query Nodes only receive updates for their active `partition_keys`.

---

## 3. Core Algorithms & Implementation

### 3.1 Durability: WAL + Async S3
**Problem:** S3 upload is too slow for write acks (100ms+).  
**Solution:** Ack after WAL fsync (<10ms). Flush to S3 asynchronously.

**Implementation (`internal/wal/wal.go`):**
```go
type WAL struct {
    dir       string
    segment   *os.File
    offset    int64
    mu        sync.Mutex
}

// Append writes to WAL and fsyncs. Returns LSN.
func (w *WAL) Append(entry []byte) (uint64, error) {
    w.mu.Lock()
    defer w.mu.Unlock()
    
    // 1. Write header + payload
    if _, err := w.segment.Write(entry); err != nil {
        return 0, err
    }
    
    // 2. FSYNC for durability (Critical)
    if err := w.segment.Sync(); err != nil {
        return 0, err
    }
    
    // 3. Return LSN immediately (Do not wait for S3)
    lsn := atomic.AddUint64(&w.currentLSN, 1)
    return lsn, nil
}
```
**Directive:** Ingest API must return 200 OK **only after WAL fsync**. S3 upload happens in a background goroutine.

**Recommendation:** To prevent data loss on ephemeral nodes ensure one writer that uses Persistent Volumes for the WAL.

### 3.1 Sharded Manifest Catalog (Critical for Scale)
**Problem:** Single `manifest.db` bottlenecks at >50K partitions (231ms latency).  
**Solution:** Consistent hashing to shard manifest into N SQLite files.

```go
// internal/manifest/sharded_catalog.go

type ShardedCatalog struct {
    shardCount int
    shards     []*Catalog // Each shard is a separate SQLite DB
    hasher     hash.Hash32
}

func NewShardedCatalog(basePath string, shardCount int) (*ShardedCatalog, error) {
    shards := make([]*Catalog, shardCount)
    for i := 0; i < shardCount; i++ {
        path := fmt.Sprintf("%s/manifest_shard_%03d.db", basePath, i)
        shards[i] = NewCatalog(path) // Reuse existing Catalog impl
    }
    return &ShardedCatalog{shardCount: shardCount, shards: shards, hasher: fnv.New32a()}, nil
}

// getShard determines which shard a partition key belongs to
func (c *ShardedCatalog) getShard(partitionKey string) *Catalog {
    c.hasher.Reset()
    c.hasher.Write([]byte(partitionKey))
    shardID := int(c.hasher.Sum32()) % c.shardCount
    return c.shards[shardID]
}

// FindPartitions fans out to relevant shards based on predicates
func (c *ShardedCatalog) FindPartitions(ctx context.Context, predicates []Predicate) ([]*PartitionRecord, error) {
    // Optimization: If predicates include partition_key, route to specific shard
    // Otherwise, fan out to all shards in parallel
    var wg sync.WaitGroup
    results := make(chan []*PartitionRecord, c.shardCount)
    
    for _, shard := range c.shards {
        wg.Add(1)
        go func(s *Catalog) {
            defer wg.Done()
            records, _ := s.FindPartitions(ctx, predicates)
            results <- records
        }(shard)
    }
    
    go func() { wg.Wait(); close(results) }()
    
    var allRecords []*PartitionRecord
    for records := range results {
        allRecords = append(allRecords, records...)
    }
    return allRecords, nil
}

**Recommendation:** To avoid "Hot Shards" for high-volume tenants, use a salted or time-aware hashing key (e.g., `hash(partition_key + year_month)`) for manifest distribution.

**Recommendation 2:** To avoid "Lost Reads" from S3 consistency jitter, include the LSN in Write Notifications and ensure Query Nodes wait for their local manifest view to reach that LSN before finalizing queries.

**Recommendation 3:** To prevent Manifest Shard lock contention during heavy maintenance, use Transactional Batching for manifest updates (e.g., one atomic commit per compaction cycle).
```

### 3.1 Temporal Co-Access Graph
Efficiency: Temporal Co-Access Graph
Problem: current Blind caching is inefficient.
Solution: Lightweight Markov-like graph tracking partition access sequences. Amplify frequent paths, degrade unused ones, limit to 10 edges per node to prevent memory exhaustion.

```go
// Implementation (internal/cache/graph.go):

type CoAccessGraph struct {
    mu        sync.RWMutex
    nodes     map[string]*Node // PartitionID -> Node
    decay     float64          // 0.95 per hour
    threshold float64          // 0.70 (Prefetch trigger)
}

type Node struct {
    Edges map[string]float64 // TargetID -> Weight
}

// RecordAccess updates weights based on query sequence [P1, P2, P3...]
func (g *CoAccessGraph) RecordAccess(sequence []string) {
    g.mu.Lock()
    defer g.mu.Unlock()
    
    for i := 0; i < len(sequence)-1; i++ {
        src, dst := sequence[i], sequence[i+1]
        if g.nodes[src] == nil { g.nodes[src] = &Node{Edges: make(map[string]float64)} }
        
        // Amplify: W = W * 0.95 + 1.0
        g.nodes[src].Edges[dst] = g.nodes[src].Edges[dst]*0.95 + 1.0
        
        // Prune weak edges (Max 10 edges per node)
        if len(g.nodes[src].Edges) > 10 { g.pruneWeak(g.nodes[src]) }
    }
}

// GetPrefetchCandidates returns partitions to load into NVMe cache
func (g *CoAccessGraph) GetPrefetchCandidates(current string) []string {
    g.mu.RLock()
    defer g.mu.RUnlock()
    
    var candidates []string
    if node, ok := g.nodes[current]; ok {
        for target, weight := range node.Edges {
            if weight > g.threshold { candidates = append(candidates, target) }
        }
    }
    return candidates
}
```

Directive: This graph lives in-memory on Query Nodes. It is not persisted. Reconstruction happens via warm-up queries. Memory cap: 10MB max.

**Recommendation:** Tracking 1.5M partitions per node will exceed 10MB. Track at a higher abstraction (e.g., `partition_key`).

**Recommendation (Advanced):** To prevent performance "dips" after restarts (Cold-Start), periodically checkpoint the co-access graph state to S3 so nodes can restore predictive intelligence instantly.



### 3.2 Automated Secondary Index Partitions
**Problem:** Physical data layout is time-ordered. Queries by `user_id` scan all time partitions.  
**Solution:** Build immutable index partitions mapping `value -> partition_ids`.

```go
// internal/index/builder.go

type IndexBuilder struct {
    storage storage.ObjectStorage
    manifest *manifest.ShardedCatalog
}

// BuildIndex creates index partitions for a specific column (e.g., user_id)
func (b *IndexBuilder) BuildIndex(ctx context.Context, collection, column string, dataPartitions []*PartitionRecord) error {
    // 1. Scan data partitions to extract (value, partition_id) mappings
    mappings := make(map[string][]string) // value -> [partition_ids]
    
    for _, dp := range dataPartitions {
        data := b.storage.Download(ctx, dp.ObjectPath)
        rows := sqlite.Query(data, fmt.Sprintf("SELECT DISTINCT %s FROM events", column))
        for _, row := range rows {
            val := row.Value.(string)
            mappings[val] = append(mappings[val], dp.PartitionID)
        }
    }
    
    // 2. Group mappings into Index Partitions (Hash-Partitioned by value)
    // Target size: 16MB per index partition
    indexBuckets := make(map[int]*IndexPartitionBuilder)
    
    for val, pIDs := range mappings {
        bucket := hashValue(val) % 1024 // 1024 buckets
        if indexBuckets[bucket] == nil {
            indexBuckets[bucket] = NewIndexPartitionBuilder(bucket)
        }
        indexBuckets[bucket].Add(val, pIDs)
    }
    
    // 3. Flush Index Partitions to S3
    for _, builder := range indexBuckets {
        if builder.Size() > 0 {
            part := builder.Finish()
            path := fmt.Sprintf("indexes/%s/%s/%d.sqlite", collection, column, builder.BucketID)
            b.storage.Upload(ctx, part.Path, path)
            b.manifest.RegisterIndexPartition(collection, column, part)
        }
    }
    return nil
}
```


 
## 3.2.1 The Solution: Automated Secondary Index Partitions

Instead of rewriting data to match queries, we build **Secondary Index Partitions** that map query values to primary data partitions. These indexes are themselves immutable SQLite micro-partitions, managed by the same compaction engine.

### 3.2.2 Core Architecture

| Component | Role | Layout | Mutability |
| :--- | :--- | :--- | :--- |
| **Primary Data** | Store actual rows | Time-Partitioned (`YYYYMMDD`) | Immutable |
| **Secondary Index** | Map `column_value` → `partition_ids` | Hash-Partitioned (`column_value % N`) | Immutable |
| **Manifest** | Track both data & index partitions | Sharded Catalog | Append-Only |

**Why This Works:**
1.  **Dynamic:** Indexes are created on-demand based on query load. No manual config.
2.  **Scalable:** Indexes are small (just mappings). They compact independently.
3.  **Immutable:** Fits Arkilian's core model. No VFS hacks, no SQLite mods.
4.  **General:** Works for any column (`user_id`, `device_id`, `status`, etc.).

### 3.2.3 Automation Loop (Zero Config)

1.  **Observe:** Query Planner logs predicate frequency (e.g., `WHERE device_id = ?` seen 1000x/hour).
2.  **Decide:** If frequency > threshold, trigger **Index Builder**.
3.  **Build:** Index Builder scans recent primary partitions → creates `idx_device_id_*.sqlite` partitions.
4.  **Register:** New index partitions added to Manifest.
5.  **Prune:** Future queries use index to skip 99% of data partitions.
6.  **GC:** If predicate frequency drops, index partitions are compacted/deleted.

**Recommendation:** To maintain <100ms visibility, implement a "Hybrid Scan" that combines results from secondary indexes with a direct scan of the latest non-indexed WAL/partitions.

---
 

### 3.2.4 Index Partition Schema

Index partitions are standard SQLite files, just like data partitions.

```sql
-- Index Partition Schema (e.g., idx_device_id_20260216.sqlite)
CREATE TABLE index_map (
    column_value TEXT NOT NULL,   -- e.g., "device-123"
    partition_id TEXT NOT NULL,   -- e.g., "data:20260216:001"
    row_count INTEGER,            -- Hint for cardinality
    min_time INTEGER,             -- Hint for time pruning
    max_time INTEGER,
    PRIMARY KEY (column_value, partition_id)
) WITHOUT ROWID;
```

### 3.2.5 Index Builder Service (`arkilian-indexer`)

New binary (or mode within `arkilian-compact`).

```go
// internal/index/builder.go

type IndexBuilder struct {
    manifest  *manifest.Catalog
    storage   storage.ObjectStorage
    threshold int // Queries per hour to trigger build
}

// BuildIndex creates secondary index partitions for a column
func (b *IndexBuilder) BuildIndex(ctx context.Context, collection, column string) error {
    // 1. Scan recent data partitions (last 24h)
    partitions, _ := b.manifest.GetRecentPartitions(collection, 24*time.Hour)
    
    // 2. Extract column values & map to partition IDs
    indexEntries := make(map[string][]string) // value -> [partition_ids]
    for _, p := range partitions {
        data := b.storage.Download(p.ObjectPath)
        rows := sqlite.Query(data, fmt.Sprintf("SELECT DISTINCT %s FROM events", column))
        for _, row := range rows {
            indexEntries[row.Value] = append(indexEntries[row.Value], p.PartitionID)
        }
    }
    
    // 3. Build Index Partitions (Hash-Partitioned by value)
    for bucket := 0; bucket < 1024; bucket++ {
        idxPartition := b.createIndexPartition(bucket, indexEntries)
        b.storage.Upload(idxPartition)
        b.manifest.RegisterIndexPartition(idxPartition)
    }
    
    return nil
}
```

### 3.2.6 Query Planner Integration

Query Planner checks for relevant indexes before scanning data partitions.

```go
// internal/query/planner/planner.go

func (p *Planner) Plan(query *Query) (*Plan, error) {
    // 1. Extract predicates
    predicates := p.extractPredicates(query)
    
    // 2. Check for Secondary Indexes
    for _, pred := range predicates {
        if idx := p.manifest.FindIndex(query.Collection, pred.Column); idx != nil {
            // 3. Use Index to Prune Data Partitions
            candidatePartitions := p.queryIndex(idx, pred.Value)
            return p.generatePlan(candidatePartitions), nil
        }
    }
    
    // 4. Fallback: Time-Based Pruning (Slower)
    partitions := p.manifest.FindPartitions(query.Collection, predicates)
    return p.generatePlan(partitions), nil
}
```

### 3.2.7 Automation Policy

```go
// internal/index/policy.go

type IndexPolicy struct {
    CreateThreshold int // Queries/hour to trigger index creation
    DropThreshold   int // Queries/hour to trigger index deletion
    TTL             time.Duration // How long to keep index
}

func (p *IndexPolicy) Evaluate(stats *QueryStats) []IndexAction {
    var actions []IndexAction
    for column, freq := range stats.PredicateFrequency {
        if freq > p.CreateThreshold {
            actions = append(actions, IndexAction{Type: Create, Column: column})
        } else if freq < p.DropThreshold {
            actions = append(actions, IndexAction{Type: Drop, Column: column})
        }
    }
    return actions
}
```

---

## 3.2.8 Why This Solves the Dynamic Pattern Problem

| Scenario | Adaptive Layout (Old) | Secondary Indexes (New) |
| :--- | :--- | :--- |
| **Sudden Spike in `device_id` Queries** | Compaction reorders data (hours). Too slow. | Index Builder creates index (minutes). Immediate relief. |
| **Query Pattern Shifts to `user_id`** | Physical layout is now wrong. Waste. | `device_id` index dropped. `user_id` index created. Efficient. |
| **Multiple Concurrent Patterns** | Impossible (data can only be sorted one way). | Multiple indexes coexist (`idx_device`, `idx_user`). |
| **Storage Overhead** | Low (data rewritten). | Low (indexes are small mappings). |
| **Write Impact** | High (compaction I/O). | Low (async background build). |

---

## 3.2.9 Revised Performance Targets

| Metric | Target | Mechanism |
| :--- | :--- | :--- |
| **Index Creation Latency** | <5 mins for 1TB data | Parallel index partition builds |
| **Query Latency (Indexed)** | <200ms | Index → Partition Pruning → Fetch |
| **Query Latency (Unindexed)** | <5s | Time-Based Pruning → Scan |
| **Index Storage Overhead** | <10% of data size | Compact index partitions |
| **Automation Lag** | <1 hour | Query stats aggregation window |

---

## 3.2.10 Updated Implementation Roadmap

| steps | Task 
| :--- | :--- |
| **1** | Implement Index Partition Schema & Builder 
| **2** | Integrate Index Lookup into Query Planner 
| **3** | Build Query Statistics Aggregator 
| **4** | Implement Automation Policy (Create/Drop) 
| **5** | Benchmark Dynamic Pattern Shifts 

---

### 3.3 S3 Coalescing (Latency Reduction)
**Problem:** 30 partitions = 30 S3 GETs = 12s latency.  
**Solution:** Combine adjacent partitions into single HTTP Range Request.

```go
// internal/storage/s3_coalescer.go

type CoalescedGetRequest struct {
    Bucket     string
    Prefix     string
    ByteRanges []ByteRange
}

type ByteRange struct {
    Start       int64
    End         int64
    PartitionID string
}

func (s *S3Storage) CoalescedGet(ctx context.Context, req CoalescedGetRequest) (map[string][]byte, error) {
    // 1. Build multipart range header: "bytes=0-67108863,134217728-201326591"
    ranges := make([]string, len(req.ByteRanges))
    for i, br := range req.ByteRanges {
        ranges[i] = fmt.Sprintf("%d-%d", br.Start, br.End)
    }
    
    // 2. Single GET request with multiple ranges
    resp, err := s.client.GetObject(ctx, &s3.GetObjectInput{
        Bucket: &req.Bucket,
        Key:    &req.Prefix,
        Range:  aws.String("bytes=" + strings.Join(ranges, ",")),
    })
    if err != nil { return nil, err }
    
    // 3. Parse multipart response and map back to PartitionIDs
    return s.parseMultipartResponse(resp, req.ByteRanges)
}

**Recommendation:** Implement a "Max Gap Threshold" (e.g., 8MB). If the byte gap between adjacent partitions exceeds this limit, use separate HTTP requests to avoid over-fetching overhead.

**Recommendation:** To avoid S3 prefix rate-limits (3,500 PUTs/sec), use Entropy Prefixes (e.g., `s3://bucket/h4s7/data/...`) derived from a hash of the partition ID.
```

### 3.4 Backpressure-Aware Compaction
**Problem:** Compaction pauses too aggressively on failure, causing backlog explosion.  
**Solution:** Exponential ramp-up on success, halving on failure.

```go
// internal/compaction/daemon.go

type CompactionDaemon struct {
    concurrency   int
    maxConcurrency int
    failureRate   float64
    mu            sync.RWMutex
}

func (d *CompactionDaemon) adjustConcurrency(success bool) {
    d.mu.Lock()
    defer d.mu.Unlock()
    
    if success {
        // Exponential ramp-up (Double)
        d.concurrency = d.concurrency * 2
        if d.concurrency > d.maxConcurrency {
            d.concurrency = d.maxConcurrency
        }
    } else {
        // Halve on failure
        d.concurrency = d.concurrency / 2
        if d.concurrency < 1 {
            d.concurrency = 1
        }
    }
}

func (d *CompactionDaemon) runCycle(ctx context.Context) {
    candidates := d.getCandidates()
    semaphore := make(chan struct{}, d.concurrency)
    
    for _, c := range candidates {
        semaphore <- struct{}{}
        go func(candidate Partition) {
            err := d.compact(candidate)
            d.adjustConcurrency(err == nil)
            <-semaphore
        }(c)
    }
}
```

### 3.5 Materialized JSON Columns (Performance)
**Problem:** `json_extract()` is slow on 100K rows.  
**Solution:** Auto-create generated columns for frequent JSON paths.

```go
// internal/schema/materializer.go

type Materializer struct {
    db *sql.DB
    queryStats *observability.QueryStats
}

func (m *Materializer) AnalyzeAndMaterialize(collection string) {
    // Get top queried JSON paths (e.g., "$.user_id")
    paths := m.queryStats.GetTopJSONPaths(collection, 10)
    
    for _, path := range paths {
        colName := jsonPathToColumn(path) // "$.user_id" -> "user_id_mat"
        
        // Check if column exists
        if !m.columnExists(collection, colName) {
            // Create generated column
            sql := fmt.Sprintf(
                "ALTER TABLE events ADD COLUMN %s INTEGER GENERATED ALWAYS AS (json_extract(payload, '%s'))",
                colName, path,
            )
            m.db.Exec(sql)
            m.db.Exec(fmt.Sprintf("CREATE INDEX idx_%s ON events(%s)", colName, colName))
        }
    }
}

**Recommendation:** To avoid massive I/O storms, only materialize columns for **newly created partitions**. For historical data, continue using the `json_extract` or index path until natural compaction occurs.

**Recommendation (Advanced):** To handle mixed-schema performance, the Query Planner must generate two-stage plans that switch between columnar access (new data) and `json_extract` (historical data) based on manifest metadata.
```

---

## 4. Operational Scale Specification (100TB)

### 4.1 Storage Configuration

| Parameter | Value | Rationale |
| :--- | :--- | :--- |
| **Data Partition Size** | **64MB** (Target) | Reduces partition count 4x vs 16MB. Critical for S3 GET reduction. |
| **Index Partition Size** | **16MB** | Indexes are smaller, high cardinality. |
| **Manifest Shard Size** | **100MB** | Keep shards manageable for quick loading. |
| **Compression** | **Snappy** | Balanced CPU/Storage tradeoff. |
| **Storage Classes** | **NVMe (Hot), S3 Std (Warm), Glacier (Cold)** | Cost optimization. 90% data in Glacier after 30 days. |
| **Durability** | **WAL fsync + S3 11x9s** | WAL ensures write durability before ack. S3 ensures persistence. |

### 4.2 Compute Configuration (Query Nodes)

| Parameter | Value | Rationale |
| :--- | :--- | :--- |
| **Instance Type** | **c6i.4xlarge (16 vCPU, 32GB RAM)** | Balanced compute/memory for SQL execution. |
| **NVMe Cache** | **500GB per node** | Stores hot partitions (last 24h) to bypass S3. |
| **Bloom Cache** | **100GB RAM + 500GB NVMe** | Tiered cache (L1 RAM, L2 NVMe) for filters. |
| **Connection Pool** | **200 Conns per node** | Prevents S3 throttling during parallel scans. |
| **Scale-Out Trigger** | **CPU > 70% or Queue Depth > 100** | Auto-scaling based on query load. |

### 4.3 Manifest Configuration

| Parameter | Value | Rationale |
| :--- | :--- | :--- |
| **Shard Count** | **64 Shards** | Distributes 1.56M partitions (100TB) across shards. |
| **Auto-Shard Threshold** | **50,000 Partitions** | Triggers migration before latency cliff. |
| **Pruning Latency Target** | **<20ms (P95)** | Achieved via sharding + indexing. |
| **Isolation** | **Logical (`tenant_id` predicate)** | Shared manifest for operational density. RLS enforced at router. |

### 4.4 Performance Targets

| Metric | Target | Validation Method |
| :--- | :--- | :--- |
| **Write Acknowledgment** | **<10ms (P99)** | WAL fsync complete. |
| **Write Visibility** | **<100ms (P95)** | Notification bus propagation. |
| **Query Latency (Hot)** | **<200ms (P95)** | NVMe Cache Hit + Index. |
| **Query Latency (Cold)** | **<5s (P95)** | S3 Fetch + Coalescing. |
| **Manifest Prune** | **<20ms (P95)** | Sharded Catalog. |
| **S3 GETs per Query** | **<3** | Coalescing + Co-Location. |
| **Compaction Backlog** | **Zero** | Under sustained 10% failure rate. |

---

## 5. Implementation Roadmap (12 Weeks)

| Phase | Duration | Deliverables | Success Metric |
| :--- | :--- | :--- | :--- |
| **1. Core Efficiency** | Weeks 1-4 | Manifest Sharding, S3 Coalescing, Backpressure Compaction | Manifest prune <20ms, S3 GETs reduced 10x. |
| **2. Indexing Engine** | Weeks 5-8 | Secondary Index Partitions, Automated Materialization | Index build <5 mins, Query latency <200ms (hot). |
| **3. Tiered Storage** | Weeks 9-10 | NVMe Cache, Glacier Tiering, Query Node Auto-scaling | Hot cache hit >80%, Cost <$0.15/GB. |
| **4. 100TB Validation** | Weeks 11-12 | Full Scale Test, Chaos Engineering, Cost Optimization | All targets met at 100TB. |

---

## 6. Final Validation Checklist

Before declaring General Availability (GA) for 100TB workloads:

- [ ] **Manifest Sharding:** Auto-migration triggers at 50K partitions without downtime.
- [ ] **Secondary Indexes:** Index build completes <5 mins for 1TB data.
- [ ] **S3 Coalescing:** Average S3 GETs per query <3.
- [ ] **Cache Hit Ratio:** NVMe partition cache >80%, Bloom filter cache >95%.
- [ ] **Write Visibility:** End-to-end write-to-query latency <100ms.
- [ ] **Compaction Backlog:** Zero backlog under sustained 10% failure rate.
- [ ] **Cost Efficiency:** Total monthly cost < $15,000 for 100TB.
- [ ] **Durability:** Zero data loss in 30-day chaos testing.
- [ ] **JSON Performance:** Materialized columns used for >90% of JSON queries.
- [ ] **Query Latency:** Cold queries <5s, Hot queries <200ms (Live S3).

---


## 7. Directives for Implementation

1.  **Do Not Rewrite Folders:** Extend `internal/`. Create new packages for WAL, Graph, Router.
2.  **Brutal Efficiency:** If an allocation happens in the hot path, justify it. Use `sync.Pool` for row slices.
3.  **No HTAP Label:** This is an Operational Document Store. Analytics is a secondary capability.
4.  **Benchmark Realism:** Test with **Live S3**, not simulated latency. If it fails on real S3, it fails.
5.  **Single Codebase:** Use `cmd/arkilian-ingest`, `cmd/arkilian-query`, etc. Share `internal/` code.
6.  **Backpressure Fix:** The latest benchmark showed compaction pausing too aggressively. Implement **exponential ramp-up** (Double concurrency on 0% failure, Halve on >10% failure).
7.  **Manifest Sharding:** This is non-negotiable. 100TB = 1.5M partitions. Single file will die at 50K. Shard by `partition_key` hash.

--- 

> *"Arkilian 2.0 is a cloud-native operational database engineered for 100TB scale. By combining micro-partitions with automated secondary indexing and S3 coalescing, we deliver MongoDB-like flexibility with SQL power at 75% lower cost than managed warehouses. We target operational workloads where sub-second write visibility and <5s query latency are acceptable, providing a viable alternative for event sourcing, analytics, and logging at scale."*

**This specification represents the convergence of architectural honesty and engineering innovation. It acknowledges the physics of S3 latency and mitigates them through caching, coalescing, and intelligent indexing, ensuring viability at scale.**

**Execute.**