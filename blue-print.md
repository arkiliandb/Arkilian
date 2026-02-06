# Project Arkilian: Distributed SQLite via Immutable Micro-Partitions  

**RFC v1.0 — For Engineering Team Review & Critical Evaluation**  
*Date: 2026-02-05 | Author: Friday/Codedynasty*

---

## 1. Executive Summary

We propose **Arkilian**: a distributed analytical database that treats SQLite files as immutable 8–16MB micro-partitions stored in object storage (S3/GCS), with a stateless query federation layer providing Snowflake-like compute/storage separation. Unlike monolithic SQLite distribution attempts (rqlite, LiteFS), Arkilian embraces SQLite’s single-node nature and partitions *at the dataset without* VFS complexity, WAL coordination hazards, and storage duplication. Target workloads: time-series analytics, multi-tenant SaaS event stores, and edge-to-cloud sync with strong eventual consistency.

> **Why this matters**: We get SQLite’s embedded simplicity + Snowflake’s elasticity without violating either system’s design constraints. No forks of SQLite core. No distributed transactions. No consensus overhead on writes.

---

## 2. Problem Statement & Goals

### Core Problem
Current SQLite distribution approaches fail for cloud-native workloads:
- **LiteFS/rqlite**: Full DB replication → N× storage cost, slow replica spin-up
- **VFS hacks**: `xShm*` coordination over network → subtle correctness bugs, high latency
- **Monolithic DBs**: Single-writer bottleneck, no horizontal scale for ingestion

### Success Criteria (SMART)
| Goal | Metric | Target | Measurement |
|------|--------|--------|-------------|
| **Storage efficiency** | Replication factor | 1.0× (vs. 3–5× in LiteFS) | S3 storage cost / logical data size |
| **Query latency** | P95 for point query (1M partitions) | <500ms | Synthetic benchmark w/ bloom pruning |
| **Write throughput** | Ingest rate (parallel writers) | 50K rows/sec/node | YCSB-like workload |
| **Recovery time** | Failed primary → new writer | <10s | Chaos engineering test |
| **Operational simplicity** | Lines of custom C code | 0 (no SQLite core mods) | Code audit |

### Exclusions
- Distributed ACID transactions across partitions
- Strong consistency (we embrace eventual consistency)
- Specific real-time OLTP (<10ms latency requirements)
- Schema-less ingestion (schema must be declared per partition key) 

---

## 3. Architecture Overview

### 3.1 High-Level Diagram
```
┌──────────────────────────────────────────────────────────────────────────────┐
│                         Query Federation Layer (Stateless)                   │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────────┐  │
│  │ Query Parser │→ │ Partition    │→ │ Bloom Filter │→ │ Parallel Executor│  │
│  │ & Planner    │  │ Pruner       │  │ Pruner       │  │ (per-partition)  │  │
│  └──────────────┘  └──────────────┘  └──────────────┘  └──────────────────┘  │
└───────────────────────────────┬──────────────────────────────────────────────┘
                                │ (pruned partition list)
        ┌───────────────────────┼───────────────────────┐
        ▼                       ▼                       ▼
┌───────────────┐       ┌───────────────┐       ┌───────────────┐
│ Compute       │       │ Compute       │       │ Compute       │
│ Cluster A     │       │ Cluster B     │       │ Cluster C     │
│ (us-east-1)   │       │ (eu-west-1)   │       │ (ap-south-1)  │
└───────────────┘       └───────────────┘       └───────────────┘
        │                       │                       │
        └───────────────────────┼───────────────────────┘
                                ▼
                ┌──────────────────────────────────┐
                │   Shared Immutable Storage       │
                │   s3://Arkilian-prod/partitions/  │
                │                                  │
                │  • events_20260205_001.sqlite    │ ← 12.4 MB
                │  • events_20260205_001.meta.json │ ← 98 KB (bloom + stats)
                │  • events_20260205_002.sqlite    │
                │  • ...                           │
                └──────────────────────────────────┘
```

### 3.2 Core Principles
1. **Immutability**: Once written, SQLite files never change. Compaction produces *new* files; old files remain queryable until TTL expiry.
2. **Partition autonomy**: Each SQLite file is a self-contained B-tree with indexes. No cross-file coordination required at query time.
3. **Metadata-driven pruning**: Global catalog (`manifest.db`) + per-file bloom filters eliminate >99% of irrelevant I/O before network fetch.
4. **Write isolation**: Writers operate on disjoint partition keys (time buckets, tenant IDs). No distributed locking required.

---

## 4. Component Specifications

### 4.1 Ingestion Service (`Arkilian-ingest`)
| Responsibility | Interface | SLA |
|----------------|-----------|-----|
| Accept batch writes (HTTP/gRPC) | `POST /v1/ingest?partition_key=20260205` | P99 <100ms |
| Generate SQLite micro-partition | Input: JSON rows → Output: `.sqlite` + `.meta.json` | <5s for 100K rows |
| Upload to object storage | Atomic multipart upload (S3) | 99.99% durability |
| Update manifest catalog | Idempotent UPSERT to `manifest.db` | Linearizable via S3 conditional PUT |

**Critical path**:
```python
def ingest_batch(rows: List[Row], partition_key: str):
    # 1. Validate schema against partition_key's schema version
    # 2. Write to temp SQLite file with indexes optimized for query patterns
    # 3. Compute min/max stats + build bloom filter (user_id, tenant_id)
    # 4. Upload .sqlite + .meta.json to S3 (atomic via multipart ETag)
    # 5. Update manifest.db ONLY after S3 upload succeeds (avoid dangling refs)
    # 6. Emit CloudWatch metric: partition_created{size_bytes=12400000}
```

### 4.2 Manifest Catalog (`manifest.db`)
A small SQLite file (≤1GB) acting as the **source of truth** for partition metadata:
```sql
CREATE TABLE partitions (
  partition_id TEXT PRIMARY KEY,      -- e.g., "events:20260205:001"
  partition_key TEXT NOT NULL,        -- e.g., "20260205" (for pruning)
  object_path TEXT NOT NULL,          -- s3://bucket/partitions/events_20260205_001.sqlite
  min_user_id INTEGER,
  max_user_id INTEGER,
  min_event_time INTEGER,
  max_event_time INTEGER,
  row_count INTEGER NOT NULL,
  size_bytes INTEGER NOT NULL,
  schema_version INTEGER NOT NULL,    -- enables schema evolution
  created_at INTEGER NOT NULL,        -- Unix epoch
  compacted_into TEXT                 -- NULL if active; points to new partition if compacted
);

-- Critical indexes for pruning
CREATE INDEX idx_time ON partitions(min_event_time, max_event_time) 
  WHERE compacted_into IS NULL;
CREATE INDEX idx_user ON partitions(min_user_id, max_user_id)
  WHERE compacted_into IS NULL;
```
- **Durability**: Backed up to S3 every 5s via Litestream
- **Availability**: Read-replicated to 3 AZs via SQLite WAL + rsync
- **Consistency**: Single writer (ingest service leader) → avoids distributed consensus

### 4.3 Query Federation Layer (`Arkilian-query`)
| Responsibility | Algorithm | Optimization |
|----------------|-----------|--------------|
| SQL parsing | Handwritten recursive descent (avoid ANTLR bloat) | Cache parsed ASTs |
| Partition pruning | 2-phase: (1) manifest min/max → (2) bloom filter | Bloom filters loaded into RAM on startup |
| Query dispatch | Per-partition subquery → parallel execution pool | Connection pooling (100 conn/node) |
| Result merging | Stream-oriented UNION ALL | Early termination on LIMIT |

**Query flow**:
```sql
-- User query
SELECT user_id, COUNT(*) FROM events 
WHERE tenant_id = 'acme' AND event_time BETWEEN 1700000000 AND 1700010000
GROUP BY user_id ORDER BY COUNT(*) DESC LIMIT 10;
```
1. **Parse**: Extract predicates (`tenant_id='acme'`, time range)
2. **Prune phase 1**: Query `manifest.db` → 120 candidate partitions
3. **Prune phase 2**: Load bloom filters for `tenant_id` → eliminate 115 partitions (96% reduction)
4. **Dispatch**: Execute 5 subqueries in parallel:
   ```sql
   SELECT user_id, COUNT(*) FROM partition_042 
   WHERE tenant_id = 'acme' AND event_time BETWEEN ...
   GROUP BY user_id
   ```
5. **Merge**: Combine partial aggregates → final top-10

### 4.4 Compaction Daemon (`Arkilian-compact`)
- **Trigger**: Partitions <8MB OR >100 partitions/day for same key
- **Process**: 
  1. Read N small partitions via S3 GET
  2. Merge sorted by primary key (minimizes B-tree fragmentation)
  3. Write new compacted partition → upload to S3
  4. Atomically update `manifest.db` (`compacted_into = new_id`)
  5. Schedule GC for old partitions after 7-day TTL
- **Safety**: Never deletes source partitions until new partition is queryable + validated

---

## 5. Data Model & Partitioning Strategy

### 5.1 Partition Key Design
| Workload Type | Partition Key | Target Size | Rationale |
|---------------|---------------|-------------|-----------|
| Time-series events | `YYYYMMDD` (daily) | 16–64 MB | Aligns with query patterns; avoids hot partitions |
| Multi-tenant SaaS | `tenant_id` | 8–32 MB | Isolates noisy neighbors; enables tenant-level TTL |
| High-cardinality IDs | `user_id % 1024` | 4–16 MB | Prevents skew; requires client-side routing |

### 5.2 Per-Partition Schema
```sql
-- Example: events table (per partition)
CREATE TABLE events (
  event_id BLOB PRIMARY KEY,  -- ULID (time-ordered UUID)
  tenant_id TEXT NOT NULL,
  user_id INTEGER NOT NULL,
  event_time INTEGER NOT NULL,  -- Unix epoch ms
  event_type TEXT NOT NULL,
  payload BLOB NOT NULL         -- Snappy-compressed JSON
) WITHOUT ROWID;

-- Indexes optimized for common queries
CREATE INDEX idx_tenant_time ON events(tenant_id, event_time);
CREATE INDEX idx_user_time ON events(user_id, event_time);

-- Statistics table (for future query optimization)
CREATE TABLE _stats (
  table_name TEXT,
  column_name TEXT,
  null_count INTEGER,
  min_value BLOB,
  max_value BLOB,
  histogram BLOB  -- Serialized 100-bucket histogram
);
```

### 5.3 Metadata Sidecar Format (`.meta.json`)
```json
{
  "partition_id": "events:20260205:001",
  "schema_version": 3,
  "stats": {
    "row_count": 842193,
    "size_bytes": 12401888,
    "min_event_time": 1706918400000,
    "max_event_time": 1706921999999,
    "min_user_id": 1000001,
    "max_user_id": 1842193
  },
  "bloom_filters": {
    "tenant_id": {
      "algorithm": "murmur3_128",
      "num_bits": 10485760,
      "num_hashes": 7,
      "base64_data": "AQIDBAU...=="
    },
    "user_id": {
      "algorithm": "murmur3_128",
      "num_bits": 20971520,
      "num_hashes": 7,
      "base64_data": "GhijKlM...=="
    }
  },
  "created_at": 1707004800
}
```

---

## 6. Failure Modes & Recovery

| Failure Scenario | Detection | Recovery Action | RTO | Data Loss Risk |
|------------------|-----------|-----------------|-----|----------------|
| Ingest node crash mid-batch | Heartbeat timeout (30s) | Client retries with idempotency key | <1m | None (idempotent writes) |
| S3 upload partial failure | Multipart ETag mismatch | Abort + retry; never update manifest | <5m | None |
| Manifest DB corruption | Checksum validation on open | Restore from Litestream backup (5s lag) | <10s | ≤5s of partition metadata |
| Query node OOM | OS kill signal | Restart; bloom filters reload from S3 | <30s | None (stateless) |
| S3 region outage | S3 error rate >1% | Failover to secondary region (pre-warmed cache) | <5m | None (S3 cross-region replication) |
| Compaction deletes source early | Validation checksum mismatch | Halt compaction; alert SRE | Manual | None (TTL safety buffer) |

**Critical invariant**: *Manifest catalog only references partitions that exist in S3.* Enforced via:
1. S3 conditional PUT (`If-Match: *`) before manifest update
2. Daily reconciliation job scans S3 → repairs dangling manifest entries

---

## 7. Performance Targets & Benchmarks

### 7.1 Baseline Workload (TPC-H-inspired)
- 1B rows (events table)
- 100K partitions (10K/day over 10 days)
- Query: `SELECT COUNT(*) FROM events WHERE user_id = ? AND event_time BETWEEN ? AND ?`

### 7.2 Projected Performance
| Metric | Target | How Achieved |
|--------|--------|--------------|
| Partition pruning ratio | 99.5% | Bloom filter (FPR=0.01) + time-range min/max |
| Avg partitions scanned/query | 5 | From 100K → 500 (min/max) → 5 (bloom) |
| P95 query latency | 420ms | 5 × (80ms S3 GET + 4ms SQLite query) |
| Ingest throughput | 45K rows/sec/node | Batch 10K rows → 12MB partition in 220ms |
| Storage efficiency | 1.03× | 3% overhead for indexes + metadata |

### 7.3 Validation Plan
1. **Microbenchmarks**: 
   - Bloom filter false positive rate (target ≤1.1%)
   - SQLite query latency on 16MB file (cold vs warm cache)
2. **Macrobenchmarks**:
   - 10-node ingest cluster → 500K rows/sec sustained
   - 50-node query cluster → 1K concurrent queries, P99 <1s
3. **Chaos tests**:
   - Kill random ingest nodes during peak load
   - Simulate S3 latency spikes (1s → 5s) → measure query degradation

---

## 8. Risks & Mitigations

| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|------------|
| **Bloom filter false positives** | Medium | Query latency spikes | Tune FPR per column; monitor actual FPR in prod |
| **Partition skew** | Medium | Hot partitions → slow queries | Adaptive partitioning (split >64MB partitions) |
| **S3 GET costs at scale** | High | $10K+/mo for 10B GETs | Aggressive bloom pruning; partition coalescing |
| **Schema evolution complexity** | Medium | Broken queries during migration | Versioned schemas; query layer rewrites old→new |
| **SQLite file corruption** | Low | Unreadable partitions | Per-file checksums; S3 versioning for rollback |
| **Team overestimates simplicity** | High | Missed edge cases | Build prototype in 4 weeks → validate core assumptions |

**Red team challenge**: *"What breaks if we get 10× more partitions than anticipated?"*  
→ Answer: Bloom filter memory pressure. Mitigation: Tiered bloom filters (hot partitions in RAM, cold on SSD cache).

---

## 9. Alternatives Considered (Honest Tradeoffs)

| Alternative | Why Rejected | When It Would Be Better |
|-------------|--------------|-------------------------|
| **LiteFS** | 3–5× storage cost; slow replica spin-up | Single-region deployments with <100GB data |
| **rqlite** | Raft consensus → 2–3× write latency; no multi-primary | Strong consistency required (banking workloads) |
| **DuckDB + MotherDuck** | Proprietary SaaS; no self-hosted option | Teams unwilling to operate distributed systems |
| **Apache Iceberg + Trino** | Heavyweight (JVM); 100ms+ query latency | Petabyte-scale data lakes; complex ETL pipelines |
| **Modify SQLite VFS** | `xShm*` coordination over network → subtle bugs | Academic research; not production-critical workloads |
| **TimescaleDB** | PostgreSQL complexity; no embedded option | Existing PG ecosystem; need full SQL compliance |

**Key insight**: Arkilian wins on *operational simplicity* for <100TB analytical workloads where eventual consistency is acceptable. It loses for transactional workloads or teams needing ANSI SQL compliance.

---

## 10. Success Metrics & Evaluation Plan

### 10.1 Phase 1: Prototype (Weeks 1–4)
- [ ] Ingest 1M rows → 100 partitions in S3
- [ ] Query with bloom pruning → 95%+ pruning ratio
- [ ] Chaos test: kill ingest node mid-batch → verify no data loss
- **Go/No-Go**: If pruning ratio <90% → re-evaluate bloom filter strategy

### 10.2 Phase 2: Production Pilot (Weeks 5–12)
- [ ] Deploy to non-critical workload (e.g., internal analytics)
- [ ] Sustain 10K rows/sec ingest for 7 days
- [ ] Achieve P95 query latency <1s for 95% of queries
- **Go/No-Go**: If S3 costs exceed $0.50/TB scanned → optimize partition sizing

### 10.3 Phase 3: General Availability (Week 13+)
- [ ] Replace legacy analytics DB for 1 product team
- [ ] Achieve 99.95% query availability (4h downtime/year)
- [ ] Document operational runbooks (SRE team sign-off required)

---

## 11. References & Prior Art

1. **Snowflake** – [The Snowflake Elastic Data Warehouse (SIGMOD 2016)](https://dl.acm.org/doi/10.1145/2882903.2903741) – Micro-partition design
2. **DuckDB** – [DuckDB: Embeddable Analytical Database (VLDB 2023)](https://dl.acm.org/doi/10.14778/3638529.3638530) – SQLite-compatible execution
3. **Litestream** – [litestream.io](https://litestream.io) – WAL replication without VFS hacks
4. **Apache Iceberg** – [iceberg.apache.org](https://iceberg.apache.org) – Metadata layer for immutable files
5. **Bloom Filters** – [Bloom (1970)](https://dl.acm.org/doi/10.1145/362686.362692) – Space-efficient membership queries
6. **SQLite VFS** – [SQLite VFS Documentation](https://www.sqlite.org/vfs.html) – Why we avoid modifying it

---

## 12. Open Questions for Team Discussion

1. Should we support *range partitioning* (e.g., `user_id 0–9999`) alongside time partitioning? Adds routing complexity but enables point queries without scanning all time partitions.
2. What’s the right bloom filter false positive rate? 1% gives good pruning but 100K partitions → 1K false positives. Is 0.1% worth 3.3× memory?
3. How do we handle GDPR "right to be forgotten"? Partition immutability conflicts with deletion requirements. Options: (a) logical deletion flag, (b) re-compaction with filtered rows.
4. Should the manifest catalog itself be partitioned? At 1M partitions, `manifest.db` hits 1GB → SQLite performance degrades. Sharding by partition_key?
5. What’s our escape hatch if this architecture fails? Can we migrate partitions to Iceberg format without downtime?

---

## Approval Signatures

| Role | Name | Date | Decision |
|------|------|------|----------|
| Engineering Lead | | | ☐ Approve ☐ Reject ☐ Revise |
| SRE Lead | | | ☐ Approve ☐ Reject ☐ Revise |
| Security Lead | | | ☐ Approve ☐ Reject ☐ Revise |
| Product Lead | | | ☐ Approve ☐ Reject ☐ Revise |

---

> **Final note**: This architecture embraces constraints rather than fighting them. We get distribution *not* by making SQLite distributed, but by partitioning the *problem* into SQLite-sized pieces. If your use case requires distributed transactions or strong consistency, **do not build this**—use PostgreSQL or Spanner. But for analytical workloads where eventual consistency is acceptable, Arkilian offers a uniquely simple path to cloud-native scale.