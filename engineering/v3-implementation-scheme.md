# Arkilian 3.0: Implementation Scheme

---

## Preamble: Synthesis of V3 Proposals

This implementation scheme is the product of deep evaluation of three independent V3 architectural proposals (Claude, ChatGPT, Gemini). It is not a copy of any single proposal. It takes the strongest elements from each and resolves their contradictions:

| Decision                                                                              | Source                                         | Rationale                                                                                                                                                                                                                   |
| :------------------------------------------------------------------------------------ | :--------------------------------------------- | :-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Document structure, WAL design, engineering directives, validation checklist, roadmap | Claude                                         | Most complete, most implementation-ready, most prescriptive                                                                                                                                                                 |
| Multi-column micro-partition files (abandon per-column-file storage)                  | Gemini (critique) + ChatGPT (ColFile)          | Per-column files cause S3 object explosion at exabyte scale. 100 columns × billions of partitions = hundreds of billions of objects. PUT/GET API costs alone are devastating                                                |
| Hybrid sort strategy (primary cluster key + hot-column sorted runs)                   | ChatGPT                                        | Sorting every column independently (Claude) causes catastrophic write amplification during hourly compaction. Sort by primary cluster key by default; create sorted runs only for hot query columns via automated detection |
| PGM/learned indexes + Roaring bitmap intersections                                    | ChatGPT                                        | Genuinely novel additions. PGM indexes map key → page offset with tiny memory footprint. Roaring bitmaps enable sub-millisecond posting list intersections for multi-predicate queries                                      |
| ArkFormat binary spec (adapted to multi-column)                                       | Claude (adapted)                               | 256-byte header, zone maps, bloom filters, 64KB block structure are all sound. Adapted from one-file-per-column to columns-as-sections-within-one-file                                                                      |
| Default file size 128MB (configurable down to 128MB)                                  | Gemini (256MB default) + Claude (128MB option) | 128MB better amortizes S3 GET fixed costs at exabyte scale. 256MB available for latency-sensitive workloads                                                                                                                 |
| Vectorized execution engine                                                           | Gemini                                         | Columnar micro-partitions return column arrays. SIMD vectorized batch processing is mandatory for competitive analytical query performance                                                                                  |

**What was thrown out:**

- Claude's per-column-file storage model (S3 object explosion at scale)
- Claude's "sort every column" strategy (write amplification destroys throughput)
- Gemini's "just use Parquet" recommendation (too conservative for a system targeting Snowflake replacement; we need control over footer layout, page indexing, and range GET optimization)
- Claude's Bloom-filter-optional stance (Bloom filters remain critical for non-cluster-key columns where binary search is unavailable)

---

## 1. Executive Summary

Arkilian 3.0 is a ground-up redesign of the storage engine. SQLite — the foundational file format of v2 — is deleted. In its place: a custom binary columnar mutable micro-partition format (ArkFormat), a shared distributed WAL, a hybrid sort strategy with automated hot-column detection, and a multi-layer pruning stack that makes search operations provably efficient at exabyte scale.

This is not an incremental patch. Arkilian 3.0 is a new class of database: non-relational in storage layout, SQL in query interface, and columnar in physical organization.

### Key Metrics (Exabyte Target)

| Metric              | V2 Target          | V3 Target              | Mechanism                                                        |
| :------------------ | :----------------- | :--------------------- | :--------------------------------------------------------------- |
| Ingest Throughput   | 500K rows/sec/node | 5M rows/sec/node       | Shared WAL, batch columnar encode                                |
| Write Ack Latency   | <10ms (P99)        | <50ns (P99)            | WAL fsync — no SQLite overhead                                   |
| Point Lookup (hot)  | <200ms             | <10ms                  | PGM index + zone map on cluster key sorted files                 |
| Point Lookup (cold) | <5s                | <400ms                 | Multi-layer pruning + S3 range GET prefetch                      |
| Range Scan (cold)   | <5s                | <600ms                 | Zone maps + column skip pruning + vectorized exec                |
| Full Column Scan    | Not optimized      | <5s/TB                 | Sequential ArkFormat read at line speed, SIMD vectorized         |
| Storage Cost        | <$0.15/GB/month    | <$0.06/GB/month        | Hourly S3 batch writes, ZSTD compression, Glacier tiering        |
| Scale               | 100TB validated    | Exabyte (EB) by design | Sharded WAL + sharded catalog namespace                          |
| Search Complexity   | O(partitions × N)  | O(log F + log P)       | Hybrid: cluster-key binary search + PGM index + zone map pruning |

**V3 Thesis:** A database where data is clustered by a primary key, hot columns get their own sorted runs with learned indexes, and every query is answered through a multi-layer pruning stack that eliminates 99%+ of I/O before touching data. Write costs are batched hourly. The WAL is shared.

---

## 2. Design Philosophy & Departure from V2

### 2.1 Why SQLite Is Eliminated

SQLite served v2 as a convenient row-store with B-tree indexing. At 100TB it began showing fundamental limits. At exabyte scale, SQLite is architecturally incompatible:

- SQLite B-tree pages (4KB default) cause catastrophic write amplification at column-scan workloads. Each random write reads an entire 4KB page, modifies one row, and rewrites the entire page.
- SQLite's VFS abstraction does not support the zero-copy, SIMD-accelerated sequential reads that columnar workloads demand.
- SQLite files bundle all columns into a single row-oriented file, forcing read amplification when only 2 of 200 columns are needed.
- SQLite WAL is per-file, per-process. V3 requires a shared, distributed WAL that multiple ingest nodes write to simultaneously.
- SQLite file sizes (8-64MB in v2) are row-store sizes. Columnar micro-partitions at 128MB are a fundamentally different granularity.

### 2.2 Snowflake Inspiration & Key Divergences

| Dimension             | Snowflake                                     | Arkilian 3.0                                                                                                            |
| :-------------------- | :-------------------------------------------- | :---------------------------------------------------------------------------------------------------------------------- |
| File Format           | Proprietary PAX (Partition Attributes Across) | ArkFormat: custom binary, multi-column micro-partition                                                                  |
| Query Language        | SQL                                           | SQL (ANSI SQL:2016)                                                                                                     |
| Partition Granularity | Multi-column micro-partition (50-500MB)       | Multi-column micro-partition (256MB default, 256MB configurable)                                                        |
| WAL Model             | Proprietary (opaque)                          | Shared distributed WAL (open spec)                                                                                      |
| Sort Strategy         | Cluster keys (manual config)                  | Primary cluster key (auto) + hot-column sorted runs (automated)                                                         |
| Search Mechanism      | Metadata pruning + full-partition scan        | 6-layer pruning stack: manifest → secondary index → zone maps → bloom filters → PGM index → Roaring bitmap intersection |
| Compaction Frequency  | Background, continuous                        | Hourly batch — S3 write cost optimization                                                                               |
| Learned Indexes       | No                                            | PGM-index per sorted column for O(1) page lookup                                                                        |
| Scale                 | Multi-petabyte (managed service)              | Exabyte (self-hosted, commodity hardware)                                                                               |
| Cost Model            | Serverless credits ($$)                       | S3 + compute (<$0.06/GB/month)                                                                                          |

### 2.3 Core Design Axioms

1. Data is clustered by a primary key. Hot columns get independent sorted runs. Search is never a full scan.
2. The WAL is the source of truth. Compaction produces read-optimized derivatives.
3. Files are completely mutable just like mongodb. Durability comes from WAL fsync + S3 11×9s.
4. Write costs to S3 are batched hourly. Real-time acknowledgment is WAL-only.
5. SQL is the query interface. The physical layout is columnar.
6. Micro-partition files are fixed at 128MB — optimal for NVMe, S3, and network interfaces. Columns are sections within a single file, not separate files.
7. Exabyte scale is achieved through namespace sharding, not vertical scaling.
8. The query engine is vectorized. Column arrays are processed in SIMD batches, not row-by-row.

---

## 3. Architecture Overview

### 3.1 High-Level Diagram

```
┌──────────────────────────────────────────────────────────────────────────┐
│                         CLIENT APPLICATIONS                              │
│                    (HTTP / gRPC / JDBC / SQL Console)                    │
└────────────────────────────┬─────────────────────────────────────────────┘
                             │
                             ▼
┌──────────────────────────────────────────────────────────────────────────┐
│                    GLOBAL ROUTER LAYER                                   │
│   ┌──────────────────────┐      ┌──────────────────────────────────┐    │
│   │  SQL Query Router    │      │  Write Router (WAL Coordinator)  │    │
│   │  (Read Traffic)      │      │  (Raft + LSN Sequencer)          │    │
│   └──────────┬───────────┘      └──────────────┬───────────────────┘    │
└──────────────┼─────────────────────────────────┼────────────────────────┘
               │                                  │
    ┌──────────▼──────────┐           ┌───────────▼───────────────────────┐
    │   QUERY NODE POOL   │           │   SHARED DISTRIBUTED WAL          │
    │  • SQL Planner      │           │  • Multi-writer atomic append      │
    │  • Column Pruner    │           │  • Raft consensus (3/5 nodes)      │
    │  • 6-Layer Pruning  │           │  • fsync < 5ms (group commit)      │
    │  • PGM Index Lookup │           │  • 16MB segments, replicated 3x    │
    │  • Vectorized Exec  │           │  • Read nodes subscribe via LSN    │
    │  • NVMe Cache       │           └───────────┬───────────────────────┘
    └──────────┬──────────┘                       │
               │              ┌────────────────────▼────────────────────┐
               │              │     HOURLY COMPACTION ENGINE            │
               │              │  • Sort by primary cluster key          │
               │              │  • Hot-column sorted run generation     │
               │              │  • ZSTD encode, ArkFormat write         │
               │              │  • Zone map + bloom filter + PGM gen    │
               │              │  • Manifest update                      │
               │              └────────────────────┬────────────────────┘
               │                                   │
    ┌──────────▼───────────────────────────────────▼──────────────────────┐
    │                   TIERED OBJECT STORAGE (S3)                        │
    │                                                                      │
    │  tables/{table}/                                                     │
    │    wal/                           ← WAL segments (durability)        │
    │    partitions/                    ← Multi-column ArkFormat files     │
    │      L0/{partition_seq}.ark       ← Fresh compaction (64-128MB)     │
    │      L1/{partition_seq}.ark       ← Merged, sorted (256MB fixed)    │
    │      L2/{partition_seq}.ark       ← Major compaction (256MB fixed)  │
    │    indexes/                       ← Hot-column sorted runs          │
    │      {col_name}/L0/*.ark          ← Per-column sorted run files     │
    │      {col_name}/L1/*.ark          ← Merged sorted runs              │
    │    manifest/catalog.arkmeta       ← Partition metadata catalog      │
    └──────────────────────────────────────────────────────────────────────┘
```

### 3.2 Component Roles

| Component         | Binary           | Responsibility                                     | Key Innovation vs V2                                   |
| :---------------- | :--------------- | :------------------------------------------------- | :----------------------------------------------------- |
| Ingest Node       | arkilian-ingest  | Accept writes, batch to Shared WAL                 | WAL is shared — no per-node WAL silos                  |
| Query Node        | arkilian-query   | SQL parse, 6-layer pruning, vectorized execution   | PGM indexes + Roaring bitmaps replace partition scan   |
| WAL Coordinator   | arkilian-wal     | Raft consensus, sequence LSNs, manage WAL segments | New in v3 — distributed WAL with Raft leader election  |
| Compaction Engine | arkilian-compact | Hourly WAL → ArkFormat columnar micro-partitions   | Hourly batch + hybrid sort (cluster key + hot columns) |
| Catalog Service   | arkilian-catalog | Serve partition metadata at exabyte scale          | Hierarchical sharding — exabyte capable, in-memory     |
| Router            | arkilian-router  | Load balance reads/writes, pub/sub WAL events      | WAL-event subscription for read-freshness              |

---

## 4. ArkFormat: Custom Binary Columnar Micro-Partition Format

### 4.1 Motivation — Why a Custom Format?

Existing columnar formats (Apache Parquet, Apache ORC, Apache Arrow IPC) are excellent general-purpose formats. ArkFormat departs from them for specific reasons:

- Parquet's footer and page index structure add decode overhead incompatible with sub-20ms lookup targets. ArkFormat uses block-aligned value arrays for SIMD-accelerated scans.
- ArkFormat files are always exactly 128MB (L1/L2) or 64-128MB (L0) — fixed sizes tuned for NVMe sequential read bandwidth and S3 GET amortization.
- ArkFormat embeds per-column bloom filters, zone maps, and optional PGM indexes in the file footer, making metadata reads free on any file prefetch.
- ArkFormat's column directory enables single-GET partial reads via HTTP byte-range requests — fetch only the columns you need from a single S3 object.
- Unlike Parquet, ArkFormat's page layout is optimized for coalesced range GETs with predictable page offsets and minimal footer parsing.

### 4.2 Critical Design Decision: Multi-Column Files (Not Per-Column Files)

The Claude proposal stored each column as its own file. At exabyte scale with 200 columns, this creates 200× the S3 object count — hundreds of billions of objects. S3 PUT costs ($0.005/1K) and GET costs ($0.0004/1K) would bankrupt the project. Reconstructing a single row requires 200 separate HTTP connections.

ArkFormat bundles all columns for a partition into a single file. Columns are stored as contiguous sections within the file, addressable via the column directory in the header. This gives us:

- Column-skip capability (only read the columns you need via byte-range GET)
- Single S3 object per partition (manageable object count at exabyte scale)
- Atomic partition writes (one PUT = one complete partition)
- Simple manifest (one entry per partition, not one per column per partition)

### 4.3 File Format Specification

#### 4.3.1 ArkFormat (.ark) Binary Layout

```
┌─────────────────────────────────────────────────────────────────────┐
│  FILE HEADER  (512 bytes fixed)                                     │
│  ┌──────────────────────────────────────────────────────────┐       │
│  │ magic:            8 bytes  0x41524B494C4E5633 ('ARKILNV3')      │
│  │ format_version:   2 bytes  (currently 0x0001)                   │
│  │ flags:            2 bytes  (cluster_sorted:bit0,                │
│  │                            compressed:bit1, has_bloom:bit2,     │
│  │                            has_pgm:bit3, has_roaring:bit4)      │
│  │ column_count:     2 bytes  (uint16 — number of columns)        │
│  │ row_count:        8 bytes  (uint64 — total rows in partition)   │
│  │ cluster_key_col:  2 bytes  (uint16 — column index of cluster   │
│  │                            key, 0xFFFF if none)                 │
│  │ compression:      1 byte   (NONE=0, ZSTD=1, LZ4=2, SNAPPY=3)  │
│  │ created_at:       8 bytes  (unix nanos)                         │
│  │ table_id:         8 bytes  (uint64)                              │
│  │ partition_seq:    8 bytes  (uint64 — monotonic partition ID)    │
│  │ lsn_min:          8 bytes  (minimum WAL LSN in this file)       │
│  │ lsn_max:          8 bytes  (maximum WAL LSN in this file)       │
│  │ time_min:         8 bytes  (min timestamp in partition, nanos)  │
│  │ time_max:         8 bytes  (max timestamp in partition, nanos)  │
│  │ file_checksum:    16 bytes (XXH3_128 of entire data section)    │
│  │ column_dir_offset:8 bytes  (uint64 — offset to column dir)     │
│  │ column_dir_size:  4 bytes  (uint32)                              │
│  │ footer_offset:    8 bytes  (uint64 — offset to footer)          │
│  │ reserved:         397 bytes (zero, future use)                   │
│  └──────────────────────────────────────────────────────────┘       │
├─────────────────────────────────────────────────────────────────────┤
│  COLUMN DIRECTORY  (variable, ~64 bytes per column)                 │
│  Array of ColumnDescriptor:                                         │
│  ┌──────────────────────────────────────────────────────────┐       │
│  │ column_id:        4 bytes  (uint32)                              │
│  │ column_name_hash: 8 bytes  (XXH3 of column name)                │
│  │ data_type:        1 byte   (INT64=1, FLOAT64=2, BOOL=3,        │
│  │                            BYTES=4, STRING=5, TIMESTAMP=6,      │
│  │                            INT32=7, FLOAT32=8, UUID=9)          │
│  │ encoding:         1 byte   (PLAIN=0, DELTA=1, RLE=2,           │
│  │                            DICT=3, BITPACK=4)                   │
│  │ data_offset:      8 bytes  (uint64 — start of column data)     │
│  │ data_size:        8 bytes  (uint64 — compressed size)           │
│  │ uncompressed_size:8 bytes  (uint64)                              │
│  │ null_count:       8 bytes  (uint64)                              │
│  │ zone_map_offset:  8 bytes  (uint64 — in footer section)        │
│  │ bloom_offset:     8 bytes  (uint64 — in footer, 0 if none)     │
│  │ bloom_size:       4 bytes  (uint32)                              │
│  │ pgm_offset:       8 bytes  (uint64 — in footer, 0 if none)     │
│  │ pgm_size:         4 bytes  (uint32)                              │
│  │ min_value:        16 bytes (type-specific, fixed slot)          │
│  │ max_value:        16 bytes (type-specific, fixed slot)          │
│  │ distinct_count:   8 bytes  (uint64 — HyperLogLog estimate)     │
│  │ is_sorted:        1 byte   (0=no, 1=yes — cluster key col)     │
│  │ reserved:         9 bytes                                        │
│  └──────────────────────────────────────────────────────────┘       │
├─────────────────────────────────────────────────────────────────────┤
│  COLUMN DATA SECTIONS  (one per column, contiguous)                 │
│                                                                      │
│  Column 0 Data:                                                      │
│  ┌──────────────────────────────────────────────────────────┐       │
│  │  DATA BLOCKS  (64KB each, independently compressible)           │
│  │  Each block:                                                     │
│  │    block_header: 16 bytes (row_start, row_count,                │
│  │                           compressed_size, flags)               │
│  │    null_bitmap:  ceil(row_count/8) bytes                        │
│  │    values:       tightly packed per encoding scheme              │
│  └──────────────────────────────────────────────────────────┘       │
│  Column 1 Data: [same structure]                                    │
│  ...                                                                 │
│  Column N Data: [same structure]                                    │
├─────────────────────────────────────────────────────────────────────┤
│  FOOTER SECTION  (variable)                                         │
│                                                                      │
│  Per-Column Zone Maps:                                               │
│  ┌──────────────────────────────────────────────────────────┐       │
│  │  Array of (block_offset, block_size, min_val, max_val,          │
│  │           null_count, row_count) — 64 bytes per zone            │
│  │  One zone per 64KB data block per column                        │
│  └──────────────────────────────────────────────────────────┘       │
│                                                                      │
│  Per-Column Bloom Filters (optional):                                │
│  ┌──────────────────────────────────────────────────────────┐       │
│  │  Blocked Cuckoo Filter — 0.3% FPR, 8-byte fingerprints         │
│  │  One per column (only for non-cluster-key columns)              │
│  │  Can be memory-mapped independently                             │
│  └──────────────────────────────────────────────────────────┘       │
│                                                                      │
│  Per-Column PGM Indexes (optional, for sorted columns only):        │
│  ┌──────────────────────────────────────────────────────────┐       │
│  │  Piecewise Geometric Model: key → page offset                   │
│  │  Tiny footprint (~2KB per 10M rows)                             │
│  │  Enables O(1) page lookup on sorted columns                     │
│  └──────────────────────────────────────────────────────────┘       │
│                                                                      │
│  Per-Column Roaring Bitmaps (optional, for indexed columns):        │
│  ┌──────────────────────────────────────────────────────────┐       │
│  │  Compressed Roaring bitmap: value → row_id set                  │
│  │  For high-cardinality equality predicates                       │
│  │  Enables sub-ms posting list intersections                      │
│  └──────────────────────────────────────────────────────────┘       │
│                                                                      │
│  Column Name Table:                                                  │
│  ┌──────────────────────────────────────────────────────────┐       │
│  │  Array of (column_id, name_length, name_bytes)                  │
│  │  Maps column_id → human-readable name                           │
│  └──────────────────────────────────────────────────────────┘       │
├─────────────────────────────────────────────────────────────────────┤
│  FILE FOOTER  (64 bytes)                                             │
│  • column_count:       2 bytes                                       │
│  • total_zone_maps:    4 bytes                                       │
│  • total_blooms:       4 bytes                                       │
│  • total_pgm_indexes:  4 bytes                                       │
│  • footer_section_size:8 bytes                                       │
│  • footer_checksum:    16 bytes (XXH3_128)                           │
│  • footer_magic:       8 bytes  (0x454E445F41524B33 = 'END_ARK3')   │
│  • footer_size:        4 bytes  (always 64)                          │
│  • reserved:           14 bytes                                      │
└─────────────────────────────────────────────────────────────────────┘
```

### 4.4 File Size Rationale — 128MB Default

The 128MB target is derived from hardware physics and S3 economics at exabyte scale.

| Interface        | Bandwidth    | Time to Read 128MB | 256MB Time | 128MB Rating                                 |
| :--------------- | :----------- | :----------------- | :--------- | :------------------------------------------- |
| NVMe Gen4        | 7,000 MB/s   | ~37ms              | ~18ms      | ★★★★★ Optimal                                |
| NVMe Gen3        | 3,500 MB/s   | ~73ms              | ~37ms      | ★★★★☆ Good                                   |
| S3 GET (US-East) | 100-200 MB/s | ~1.4s              | ~700ms     | ★★★★★ Optimal (amortizes first-byte latency) |
| 10GbE Network    | 1,250 MB/s   | ~205ms             | ~102ms     | ★★★★☆ Good                                   |
| 100GbE Network   | 12,500 MB/s  | ~20ms              | ~10ms      | ★★★★★ Optimal                                |

At exabyte scale, 128MB files produce ~3.9 billion L2 files (vs ~7.8 billion at 128MB). This halves the catalog metadata size, halves the S3 object count, and halves the manifest entries — all critical at exabyte scale.

The 128MB default is configurable down to 256MB via `partition.target_size_mb` for workloads where lower per-query latency matters more than S3 amortization.

### 4.5 Column-Skip via Byte-Range GET

Because the column directory records exact byte offsets and sizes for each column's data section, a query needing only 2 of 200 columns can issue a single S3 GET with multiple byte ranges (or 2-3 targeted GETs) to fetch only those columns. This eliminates the read amplification problem without requiring separate files per column.

```
// Query: SELECT name, age FROM users WHERE age > 30
//
// Step 1: Fetch file header + column directory (first 512 + ~12,800 bytes)
//         Single S3 GET with Range: bytes=0-13312
//
// Step 2: From column directory, find:
//         age column:  data_offset=1048576, data_size=2097152
//         name column: data_offset=5242880, data_size=4194304
//
// Step 3: Fetch only those column sections:
//         S3 GET with Range: bytes=1048576-3145727  (age)
//         S3 GET with Range: bytes=5242880-9437183  (name)
//
// Step 4: Fetch zone map + bloom for age from footer (if not cached)
//
// Total: 3-4 S3 GETs instead of downloading entire 128MB   file
// Data transferred: ~6MB instead of 128MB   (97% reduction)
```

---

## 5. Shared Distributed WAL Architecture

### 5.1 V2 vs V3 WAL Model

In v2, each ingest node maintained its own isolated WAL — writes went to node-local disk, then async to S3. This created WAL silos: query nodes could not see recent writes until S3 compaction. In v3, the WAL is shared across all ingest nodes AND readable by all query nodes. This collapses write-to-read latency from compaction-cycle latency (~1 hour) to WAL propagation latency (<100ms).

### 5.2 Raft-Backed WAL Coordinator

The WAL coordinator uses a Raft consensus cluster (3 or 5 nodes) for leader election and LSN sequencing. This combines Claude's detailed WAL implementation with ChatGPT's Raft-based sequencer for proven distributed consensus.

```
// WAL Coordinator: Raft cluster assigns globally unique, monotonically
// increasing LSNs. Leader handles all writes; followers replicate.
//
// On leader failure: Raft elects new leader in <5s. Ingest nodes
// retry against new leader. No LSN gaps — Raft log is the authority.

type WalCoordinator struct {
    raft          *raft.Raft           // hashicorp/raft
    lsnCounter    atomic.Uint64        // Global LSN — atomically incremented by leader
    activeSegment *WalSegment
    segmentMu     sync.RWMutex
    s3client      *S3Client
    replicaSet    []string             // Raft peer addresses
}

// Append is called by any ingest node via gRPC to the Raft leader.
func (c *WalCoordinator) Append(tableID uint64, batch *RowBatch) (uint64, error) {
    // 1. Atomically acquire LSN (leader only)
    lsn := c.lsnCounter.Add(1)

    // 2. Encode batch into WAL entry
    entry := encodeWALEntry(lsn, tableID, batch)

    // 3. Apply through Raft (replicates to majority before returning)
    future := c.raft.Apply(entry, 5*time.Second)
    if err := future.Error(); err != nil {
        return 0, fmt.Errorf("raft apply: %w", err)
    }

    // 4. Write to local WAL segment (post-Raft-commit)
    c.segmentMu.RLock()
    seg := c.activeSegment
    c.segmentMu.RUnlock()
    seg.AppendAtomic(entry)

    // 5. Group fsync (batches concurrent fsyncs)
    if err := seg.GroupFsync(lsn); err != nil {
        return 0, err
    }

    // 6. Async: upload to S3 + notify read nodes
    go c.replicateToS3(entry, lsn)
    go c.notifyReadNodes(tableID, lsn)

    return lsn, nil // Ack to client after Raft commit + fsync — <5ms target
}
```

### 5.3 WAL Segment Format

```
// Shared WAL lives at: tables/{table}/wal/seg_{start_lsn:020d}.arkwal
//
// WAL SEGMENT HEADER (128 bytes):
//   magic:          8 bytes  (0x41524B57414C5633 = 'ARKWALV3')
//   segment_id:     8 bytes  (uint64, monotonically assigned)
//   start_lsn:      8 bytes  (uint64, first LSN in this segment)
//   max_lsn:        8 bytes  (uint64, updated atomically)
//   writer_node_id: 4 bytes  (uint32, Raft leader node ID)
//   created_at:     8 bytes  (unix nanos)
//   segment_size:   8 bytes  (uint64, max bytes before roll = 64MB)
//   checksum:       8 bytes  (rolling XXH3)
//   reserved:       68 bytes
//
// WAL ENTRY (variable):
//   entry_magic:    4 bytes  (0xE1A1B1C1)
//   entry_len:      4 bytes  (uint32, total bytes including header)
//   lsn:            8 bytes  (uint64, globally unique)
//   table_id:       8 bytes  (uint64)
//   timestamp_ns:   8 bytes  (unix nanos)
//   row_count:      4 bytes  (uint32, rows in this batch)
//   schema_hash:    8 bytes  (xxhash of column schema)
//   payload_type:   1 byte   (INSERT=1, UPDATE=2, DELETE=3)
//   payload:        variable (columnar mini-batch)
//   checksum:       4 bytes  (CRC32C of all above bytes)
```

### 5.4 Group Fsync — Critical for Throughput

```
// GroupFsync batches concurrent fsync calls into a single syscall.
// At 5M rows/sec, individual fsyncs would saturate NVMe IOPS.
// Group commit reduces fsync calls by 100-1000x.

func (s *WalSegment) GroupFsync(lsn uint64) error {
    s.pendingMu.Lock()
    s.pendingLSNs = append(s.pendingLSNs, lsn)
    waitCh := s.flushedCh
    s.pendingMu.Unlock()

    select {
    case <-waitCh:  // Another goroutine already fsynced — piggyback
        return nil
    case s.fsyncTrigger <- struct{}{}: // We trigger the fsync
        return s.file.Sync()
    }
}
```

### 5.5 Read Nodes Subscribing to WAL

Query nodes subscribe to WAL events via the router's pub/sub channel. This allows them to serve freshly written rows (not yet compacted to column files) while maintaining consistency guarantees:

```
type QueryNodeWALBuffer struct {
    mu      sync.RWMutex
    entries map[uint64][]*WALEntry   // tableID -> recent entries
    maxLSN  atomic.Uint64
    maxAge  time.Duration             // evict after 2h (beyond compaction window)
}

// OnWALEvent is called when a new WAL LSN is published
func (b *QueryNodeWALBuffer) OnWALEvent(event WALEvent) {
    b.mu.Lock()
    defer b.mu.Unlock()
    b.entries[event.TableID] = append(b.entries[event.TableID], event.Entry)
    b.maxLSN.Store(event.LSN)
}

// During query execution, planner merges WAL buffer with column file results.
// This ensures writes < 1 hour old are visible without waiting for compaction.
```

### 5.6 WAL Durability & Retention

| Property          | Value                                  | Rationale                                        |
| :---------------- | :------------------------------------- | :----------------------------------------------- |
| Segment Size      | 64MB                                   | Rolls to new segment every ~100ms at 5M rows/sec |
| fsync Policy      | Group commit, <5ms                     | Batches concurrent writes into single fsync call |
| Replication       | Raft (majority quorum)                 | 2/3 or 3/5 nodes ack before leader confirms      |
| S3 Upload         | Async, <100ms after Raft commit        | S3 provides 11×9s durability for segment archive |
| Retention on Disk | 2 hours post-compaction                | Cover compaction lag + retry window              |
| Retention on S3   | 72 hours                               | Recovery window if compaction engine fails       |
| LSN Gap Detection | Continuous — Raft log is authoritative | Raft guarantees no gaps in committed log         |
| Leader Failover   | <5s (Raft election timeout)            | Ingest nodes retry against new leader            |

---

## 6. Hybrid Sort Strategy — The Core Innovation

### 6.1 Why Not Sort Every Column

Claude's proposal sorts every column independently. The math is beautiful on the read side (57 binary search steps for an exabyte), but the write side is catastrophic:

- At 5M rows/sec with 200 columns: 3.6 trillion column-value sorts per hour
- Per column: 18 billion values. At 8 bytes/value = 144GB per column
- External merge sort with 32GB RAM: ~4 passes × 200 columns = 800 sort passes per hour
- Row reconstruction requires row_id pointers in every column file, adding storage bloat

Gemini correctly identified this as a "mathematical paradox." The write amplification destroys the cost advantage.

### 6.2 The Hybrid Approach (ChatGPT's Strategy, Refined)

Data is sorted by a single primary cluster key during compaction. For all other columns, zone maps provide coarse pruning. When the query statistics aggregator detects a column is queried frequently (hot column), the compaction engine generates independent sorted runs for that column only.

```
// Compaction produces two types of output:
//
// 1. PRIMARY MICRO-PARTITIONS (always):
//    - All columns bundled into single ArkFormat file
//    - Sorted by primary cluster key (e.g., timestamp, tenant_id)
//    - Zone maps computed for EVERY column
//    - Bloom filters for non-cluster-key columns
//    - PGM index for the cluster key column
//    - Stored at: tables/{table}/partitions/L1/{partition_seq}.ark
//
// 2. HOT-COLUMN SORTED RUNS (on demand):
//    - Single-column ArkFormat files, sorted by that column's values
//    - Generated only for columns exceeding query frequency threshold
//    - Include (value, row_id) pairs for row reconstruction
//    - PGM index embedded for O(1) page lookup
//    - Stored at: tables/{table}/indexes/{col_name}/L1/{run_seq}.ark
//
// This means:
//   - Cluster key queries: O(log F) binary search on primary partitions
//   - Hot column queries: O(log F) binary search on sorted runs
//   - Cold column queries: zone map pruning + bloom filter + scan
//   - Multi-predicate: Roaring bitmap intersection across results
```

### 6.3 Hot Column Detection — Automated Policy

```
type HotColumnPolicy struct {
    stats           *QueryStats
    catalog         *CatalogClient
    createThreshold int64          // Queries/hour to trigger sorted run creation (default: 200)
    dropThreshold   int64          // Queries/hour below which to drop sorted runs (default: 10)
    checkInterval   time.Duration  // Evaluation interval (default: 5m)
    maxHotColumns   int            // Max concurrent hot columns per table (default: 10)
}

// Evaluate checks query statistics and returns actions
func (p *HotColumnPolicy) Evaluate(ctx context.Context, tableID uint64) ([]HotColumnAction, error) {
    topPredicates := p.stats.GetTopPredicates(tableID, 24*time.Hour)

    var actions []HotColumnAction
    existingHot := p.catalog.GetHotColumns(tableID)

    for _, pred := range topPredicates {
        if pred.Frequency > p.createThreshold && !existingHot.Contains(pred.Column) {
            if len(existingHot) < p.maxHotColumns {
                actions = append(actions, HotColumnAction{
                    Type:   CreateSortedRun,
                    Column: pred.Column,
                })
            }
        }
    }

    for _, col := range existingHot {
        freq := p.stats.GetColumnFrequency(tableID, col, 24*time.Hour)
        if freq < p.dropThreshold {
            actions = append(actions, HotColumnAction{
                Type:   DropSortedRun,
                Column: col,
            })
        }
    }

    return actions, nil
}
```

### 6.4 Sort Complexity Analysis

| Strategy                                   | Sort Work per Hour (200 cols, 5M rows/sec) | Read Complexity (point lookup)                 | Write Amplification                      |
| :----------------------------------------- | :----------------------------------------- | :--------------------------------------------- | :--------------------------------------- |
| Sort every column (Claude)                 | 200 × 18B values = 3.6T sorts              | O(log F) on any column                         | 200× (every column sorted independently) |
| Sort nothing (append-only)                 | 0                                          | O(N) full scan                                 | 1× (no sort overhead)                    |
| Hybrid: 1 cluster key + 5 hot columns (V3) | 6 × 18B values = 108B sorts                | O(log F) on 6 columns, zone-map-pruned on rest | 6× (only sorted columns)                 |

The hybrid approach delivers 97% of the read performance benefit at 3% of the write cost.

---

## 7. Compaction Engine — WAL to Columnar Micro-Partitions

### 7.1 Compaction Philosophy

V2 compacted continuously in the background. V3 compacts hourly, in batch. This is a deliberate economic choice: S3 PUTs cost $0.005 per 1,000 requests. Continuous compaction at 5M rows/sec generates millions of S3 PUTs per hour. Hourly batching amortizes this cost across a full hour of writes.

**Cost Analysis:** At 5M rows/sec with 200 columns, continuous compaction generates ~360,000 S3 PUTs/hour at $0.005/1K = $1.80/hour. Hourly batch compaction consolidates this to ~3,600 S3 PUTs/hour (100× reduction) = $0.018/hour. Note: because we use multi-column files (not per-column files), the PUT count is already 200× lower than Claude's per-column model.

### 7.2 Hourly Compaction Cycle

```
type CompactionEngine struct {
    walReader     *WALReader
    s3client      *S3Client
    catalog       *CatalogClient
    sorter        *ExternalMergeSorter
    hotPolicy     *HotColumnPolicy
    workers       int   // parallel compaction workers
}

// RunHourlyCycle is invoked by cron at :00 of each hour
func (e *CompactionEngine) RunHourlyCycle(ctx context.Context) error {
    // 1. Determine WAL range to compact
    lastCheckpoint, _ := e.catalog.GetLastCompactionCheckpoint()
    latestLSN := e.walReader.GetMaxLSN()

    // 2. Read all WAL entries since last checkpoint
    entries, _ := e.walReader.ReadRange(lastCheckpoint, latestLSN)

    // 3. Group by table_id
    byTable := groupByTable(entries)

    // 4. For each table, produce primary micro-partitions
    var wg sync.WaitGroup
    semaphore := make(chan struct{}, e.workers)
    for tableID, tableEntries := range byTable {
        wg.Add(1)
        semaphore <- struct{}{}
        go func(tID uint64, entries []*WALEntry) {
            defer func() { <-semaphore; wg.Done() }()

            // 4a. Sort all rows by primary cluster key
            clusterKey := e.catalog.GetClusterKey(tID)
            sorted := e.sorter.SortByColumn(entries, clusterKey)

            // 4b. Write sorted rows into 128MB   ArkFormat micro-partitions
            e.writePrimaryPartitions(ctx, tID, sorted)

            // 4c. Generate hot-column sorted runs (if any)
            hotCols := e.catalog.GetHotColumns(tID)
            for _, col := range hotCols {
                e.writeHotColumnRun(ctx, tID, col, entries)
            }
        }(tableID, tableEntries)
    }
    wg.Wait()

    // 5. Evaluate hot column policy (may trigger new sorted runs)
    for tableID := range byTable {
        actions, _ := e.hotPolicy.Evaluate(ctx, tableID)
        for _, action := range actions {
            if action.Type == CreateSortedRun {
                e.catalog.MarkColumnHot(tableID, action.Column)
            } else if action.Type == DropSortedRun {
                e.catalog.MarkColumnCold(tableID, action.Column)
            }
        }
    }

    // 6. Update compaction checkpoint atomically
    e.catalog.SetCompactionCheckpoint(latestLSN)
    return nil
}
```

### 7.3 Primary Partition Writer

```
func (e *CompactionEngine) writePrimaryPartitions(ctx context.Context, tableID uint64, sorted []Row) {
    writer := NewArkWriter(tableID, CompressionZSTD, DefaultPartitionSize)

    for _, row := range sorted {
        if writer.EstimatedSize() >= TargetPartitionSize {
            // Finalize: compute zone maps, bloom filters, PGM index for cluster key
            writer.ComputeZoneMaps()
            writer.ComputeBloomFilters()  // For non-cluster-key columns
            writer.ComputePGMIndex()       // For cluster key column only
            writer.Flush(ctx, e.s3client)  // Upload to S3
            e.catalog.RegisterPartition(writer.Metadata())

            writer = NewArkWriter(tableID, CompressionZSTD, DefaultPartitionSize)
        }
        writer.AppendRow(row)
    }
    // Flush final partition
    writer.ComputeZoneMaps()
    writer.ComputeBloomFilters()
    writer.ComputePGMIndex()
    writer.Flush(ctx, e.s3client)
    e.catalog.RegisterPartition(writer.Metadata())
}
```

### 7.4 Hot Column Sorted Run Writer

```
func (e *CompactionEngine) writeHotColumnRun(ctx context.Context, tableID uint64, colName string, entries []*WALEntry) {
    // Extract (value, row_id) pairs for this column
    pairs := extractColumnPairs(entries, colName)

    // Sort by (value, row_id)
    slices.SortFunc(pairs, func(a, b ColPair) int {
        if cmp := compareValue(a.Value, b.Value); cmp != 0 { return cmp }
        return cmp(a.RowID, b.RowID)
    })

    // Write into single-column ArkFormat files with PGM index
    writer := NewSortedRunWriter(tableID, colName, CompressionZSTD)
    for _, pair := range pairs {
        if writer.EstimatedSize() >= TargetPartitionSize {
            writer.ComputePGMIndex()
            writer.Flush(ctx, e.s3client)
            e.catalog.RegisterSortedRun(writer.Metadata())
            writer = NewSortedRunWriter(tableID, colName, CompressionZSTD)
        }
        writer.Append(pair.Value, pair.RowID)
    }
    writer.ComputePGMIndex()
    writer.Flush(ctx, e.s3client)
    e.catalog.RegisterSortedRun(writer.Metadata())
}
```

### 7.5 LSM-Like Level Hierarchy

Each table maintains a 3-level hierarchy for primary partitions. Hot-column sorted runs have their own independent 2-level hierarchy.

**Primary Partitions:**

| Level       | File Size     | Sort Property                                                | Source                     | Target Count                   |
| :---------- | :------------ | :----------------------------------------------------------- | :------------------------- | :----------------------------- |
| L0 (Fresh)  | 64-128MB      | Sorted by cluster key within file, may overlap between files | Hourly compaction from WAL | ≤16 per table                  |
| L1 (Merged) | 128MB (fixed) | Globally sorted by cluster key, no time-range overlap        | L0 merge                   | ≤128 per table shard           |
| L2 (Major)  | 128MB (fixed) | Globally sorted by cluster key, no time-range overlap        | L1 merge                   | Unlimited (by namespace shard) |

**Hot-Column Sorted Runs:**

| Level | File Size     | Sort Property                           | Source            |
| :---- | :------------ | :-------------------------------------- | :---------------- |
| L0    | 64-128MB      | Sorted by column value within file      | Hourly compaction |
| L1    | 128MB (fixed) | Globally sorted, no value-range overlap | L0 merge          |

Key Property: Within L1 and L2, all primary partition files have non-overlapping time ranges (cluster key ranges). File F1 covers [min1, max1], F2 covers [min2, max2], and max1 < min2 always holds. This enables O(log F) binary search to find the correct file.

### 7.6 External Merge Sort

```
type ExternalMergeSorter struct {
    ramBudget    int64    // Per-table RAM budget (e.g., 2GB)
    tmpDir       string   // NVMe temp directory for sort runs
    workers      int      // Parallel merge workers
}

// SortByColumn performs external merge sort on rows by a specific column
// Time: O(N log N), Memory: O(ramBudget)
func (s *ExternalMergeSorter) SortByColumn(entries []*WALEntry, column string) []Row {
    rows := flattenToRows(entries)

    if estimateSize(rows) <= s.ramBudget {
        // Fast path: fits in RAM
        slices.SortFunc(rows, makeColumnComparator(column))
        return rows
    }

    // External merge sort for data exceeding RAM
    chunkSize := int(s.ramBudget) / estimateRowSize(rows[0])
    var runFiles []string
    for i := 0; i < len(rows); i += chunkSize {
        chunk := rows[i:min(i+chunkSize, len(rows))]
        slices.SortFunc(chunk, makeColumnComparator(column))
        runFile := s.writeSortRun(chunk)
        runFiles = append(runFiles, runFile)
    }

    return s.kWayMerge(runFiles)
}
```

---

## 8. Search Engine — 6-Layer Pruning Stack at Exabyte Scale

### 8.1 The Fundamental Problem

At exabyte scale (10^18 bytes), a sequential scan at 10GB/s NVMe bandwidth takes 100 million seconds — over 3 years. The only viable approach is a multi-layer pruning stack where each layer eliminates candidates more cheaply than the layer below.

### 8.2 The 6-Layer Pruning Stack

This is the key architectural innovation of V3, synthesized from all three proposals. Each layer prunes more cheaply than the layer below:

```
┌─────────────────────────────────────────────────────────────────┐
│  LAYER 1: MANIFEST PRUNING (O(1) per shard)                    │
│  Sharded manifest returns candidate partitions given table,     │
│  time predicate, and optional index partitions.                 │
│  Cost: <1ms (in-memory catalog lookup)                          │
│  Prunes: 90-99% of partitions via time-range + shard selection  │
├─────────────────────────────────────────────────────────────────┤
│  LAYER 2: SECONDARY INDEX PARTITIONS (O(log N) lookup)          │
│  index partitions mapping column_value → list of      │
│  partition IDs. Built on demand by automated policy.            │
│  Cost: <10ms (cached index lookup)                              │
│  Prunes: 95-99% of remaining partitions for indexed columns     │
├─────────────────────────────────────────────────────────────────┤
│  LAYER 3: ZONE MAP PRUNING (O(1) per partition)                 │
│  Per-column min/max values stored in partition footer + cached   │
│  in catalog. Excludes partitions where predicate is impossible. │
│  Cost: <100ns per partition (in-memory min/max comparison)       │
│  Prunes: 80-99% of remaining partitions for range predicates    │
├─────────────────────────────────────────────────────────────────┤
│  LAYER 4: BLOOM FILTER PRUNING (O(1) per partition)             │
│  Per-column Blocked Cuckoo Filters in partition footer.         │
│  Proves value ABSENT in partition with 0.3% FPR.               │
│  Cost: <1μs per partition (single cache-line read)              │
│  Prunes: 95-99% of remaining partitions for equality predicates │
├─────────────────────────────────────────────────────────────────┤
│  LAYER 5: PGM INDEX + BINARY SEARCH (O(1) page lookup)         │
│  For cluster-key column and hot-column sorted runs:             │
│  PGM learned index maps key → page offset directly.            │
│  For non-sorted columns: binary search on zone map within file. │
│  Cost: <10μs (PGM lookup) or <100μs (zone map binary search)   │
│  Prunes: identifies exact 64KB block(s) containing target value │
├─────────────────────────────────────────────────────────────────┤
│  LAYER 6: ROARING BITMAP INTERSECTION (O(min(|A|,|B|)))        │
│  For multi-predicate queries: intersect row_id sets from each   │
│  predicate's results using compressed Roaring bitmaps.          │
│  Cost: <1ms for million-element intersections                   │
│  Prunes: produces exact row_id set matching all predicates      │
└─────────────────────────────────────────────────────────────────┘
```

### 8.3 PGM Learned Index — ChatGPT's Innovation

The Piecewise Geometric Model (PGM) index is a learned index structure that maps a key to its approximate position in a sorted array. It consists of a hierarchy of linear models, each covering a segment of the key space. The total size is typically 2-4KB for 10 million keys — orders of magnitude smaller than a B-tree.

```
// PGM index for sorted columns (cluster key + hot columns)
//
// Structure: hierarchy of linear segments
//   Level 0: covers entire key range with N segments
//   Level 1: index over Level 0 segments
//   ...
//   Top level: single segment (root)
//
// Lookup: traverse from top level down, each level narrows to a segment
// Result: approximate position ± epsilon (configurable error bound)
// Then: scan epsilon values to find exact match
//
// Memory: ~2KB per 10M keys (vs ~80MB for B-tree)
// Lookup: O(log log N) — faster than O(log N) binary search

type PGMIndex struct {
    levels    [][]Segment    // Hierarchy of linear segments
    epsilon   int            // Error bound (default: 64 — scan ±64 values)
    minKey    Value
    maxKey    Value
    size      int            // Total keys indexed
}

type Segment struct {
    Key       Value   // Starting key of this segment
    Slope     float64 // Linear model: position = slope * (key - Key) + intercept
    Intercept float64
}

// Search returns the approximate position of key in the sorted array
// Actual position is within [pos-epsilon, pos+epsilon]
func (p *PGMIndex) Search(key Value) (approxPos int, lo int, hi int) {
    // Traverse from top level down
    pos := 0
    for level := len(p.levels) - 1; level >= 0; level-- {
        seg := p.levels[level][pos]
        predicted := int(seg.Slope*float64(keyToInt(key)-keyToInt(seg.Key)) + seg.Intercept)
        pos = clamp(predicted, 0, len(p.levels[level])-1)
    }
    return pos, max(0, pos-p.epsilon), min(p.size-1, pos+p.epsilon)
}
```

### 8.4 Roaring Bitmap Intersections — Multi-Predicate Queries

For queries with multiple predicates (e.g., `WHERE age > 30 AND city = 'NYC' AND status = 'active'`), each predicate produces a set of matching row_ids. Roaring bitmaps enable sub-millisecond intersection of these sets.

```
// Multi-predicate query execution:
//
// 1. For each predicate, collect matching row_ids into a Roaring bitmap
// 2. Intersect all bitmaps: result = bitmap_age AND bitmap_city AND bitmap_status
// 3. Use result bitmap to fetch only matching rows from column data
//
// Roaring bitmaps compress integer sets using a hybrid of:
//   - Array containers (for sparse sets: <4096 elements per 64K chunk)
//   - Bitmap containers (for dense sets: ≥4096 elements per 64K chunk)
//   - Run containers (for consecutive ranges)
//
// Intersection of two Roaring bitmaps with 1M elements each: <500μs
// This is 1000x faster than hash-set intersection

type PredicateResult struct {
    Column  string
    Bitmap  *roaring.Bitmap  // github.com/RoaringBitmap/roaring
}

func (e *QueryExecutor) ExecuteMultiPredicate(ctx context.Context, predicates []Predicate) (*roaring.Bitmap, error) {
    results := make([]*roaring.Bitmap, len(predicates))

    // Execute each predicate in parallel
    var wg sync.WaitGroup
    for i, pred := range predicates {
        wg.Add(1)
        go func(idx int, p Predicate) {
            defer wg.Done()
            rowIDs := e.evaluatePredicate(ctx, p)
            results[idx] = roaring.BitmapOf(rowIDs...)
        }(i, pred)
    }
    wg.Wait()

    // Intersect all bitmaps (smallest first for efficiency)
    sort.Slice(results, func(i, j int) bool {
        return results[i].GetCardinality() < results[j].GetCardinality()
    })

    final := results[0]
    for i := 1; i < len(results); i++ {
        final = roaring.And(final, results[i])
    }
    return final, nil
}
```

### 8.5 Blocked Cuckoo Filter — Cache-Line Optimized Bloom Alternative

```
// Classic Bloom filters have poor cache behavior — random bit array access
// causes L3 cache misses on every probe. Blocked Cuckoo Filters segment
// the filter into 64-byte blocks (one CPU cache line). All hash probes
// for a single lookup fall within one cache line.
//
// Performance: 5-10x faster than classic Bloom at same FPR

const (
    BlockSize       = 64     // One CPU cache line
    FingerprintBits = 8      // 8-bit fingerprints
    BucketSize      = 4      // 4 slots per bucket
    TargetFPR       = 0.003  // 0.3% false positive rate
)

type BlockedCuckooFilter struct {
    blocks    []byte    // Main storage — 64-byte aligned blocks
    numBlocks uint32
    numItems  uint64
}

// MayContain: O(1) — single cache-line read
func (f *BlockedCuckooFilter) MayContain(item []byte) bool {
    h := xxhash.Sum64(item)
    blockIdx := uint32(h>>32) % f.numBlocks
    fp := uint8(h&0xFF) | 1 // Ensure fingerprint != 0

    block := f.blocks[blockIdx*BlockSize : (blockIdx+1)*BlockSize]
    for bucket := 0; bucket < 4; bucket++ {
        for slot := 0; slot < 4; slot++ {
            if block[bucket*BucketSize+slot] == fp {
                return true
            }
        }
    }
    return false
}
```

### 8.6 Search Complexity — Mathematical Analysis

For a point lookup on the cluster key column at exabyte scale:

| Scale | Total Rows (est.) | Partitions (256MB) | Catalog Binary Search | PGM Lookup | Zone Map Search | Total Steps | Wall Time                |
| :---- | :---------------- | :----------------- | :-------------------- | :--------- | :-------------- | :---------- | :----------------------- |
| 1GB   | ~10M              | 4                  | 2                     | O(1)       | 3               | ~5          | <100ns                   |
| 1TB   | ~10B              | 4,096              | 12                    | O(1)       | 6               | ~18         | <200ns                   |
| 1PB   | ~10T              | 4.2M               | 22                    | O(1)       | 9               | ~31         | <400ns                   |
| 1EB   | ~10Q              | 4.3B               | 32                    | O(1)       | 12              | ~44         | <600ns (+ network: <2ms) |

For a point lookup on a non-cluster-key, non-hot column (worst case):

| Scale | Partitions After Zone Map Prune | Bloom Filter Checks | Partitions to Scan | Wall Time                        |
| :---- | :------------------------------ | :------------------ | :----------------- | :------------------------------- |
| 1TB   | ~40 (1% of 4,096)               | 40                  | ~1-2               | <50ms (NVMe)                     |
| 1PB   | ~4,200 (0.1% of 4.2M)           | 4,200               | ~10-20             | <200ms (distributed)             |
| 1EB   | ~43,000 (0.001% of 4.3B)        | 43,000              | ~100-200           | <500ms (distributed, 1000 nodes) |

This is why hot-column sorted runs matter: promoting a cold column to hot drops its lookup from O(zone-map-pruned scan) to O(log F + PGM).

---

## 9. Vectorized Query Engine — SQL on Columnar Storage

### 9.1 SQL Interface Philosophy

Arkilian 3.0 is not a relational database. There are no foreign keys, no joins across tables (at the physical layer), and no ACID transactions spanning tables. However, it exposes standard ANSI SQL:2016 as its query interface. The SQL layer translates SQL predicates into column file operations. This is the same architectural choice as Snowflake: columnar storage, SQL interface.

### 9.2 Vectorized Execution (Gemini's Contribution)

Because ArkFormat returns column arrays (not rows), the query engine must process data in vectorized batches using SIMD instructions. Row-by-row iteration wastes 90%+ of modern CPU capability on columnar data.

```
// Vectorized execution processes columns in batches of 1024-4096 values.
// Each batch fits in L1/L2 cache. SIMD instructions process 4-8 values
// per CPU cycle (AVX2: 4×int64, AVX-512: 8×int64).
//
// Example: SUM(amount) WHERE region = 'EMEA'
//
// Row-by-row (v2):
//   for each row:
//     if row.region == 'EMEA': sum += row.amount
//   → 1 value per cycle, branch misprediction penalty
//
// Vectorized (v3):
//   for each batch of 1024 values:
//     mask = SIMD_compare(region_batch, 'EMEA')  // 4-8 comparisons per cycle
//     sum += SIMD_masked_sum(amount_batch, mask)  // 4-8 additions per cycle
//   → 4-8 values per cycle, no branch misprediction

const VectorBatchSize = 1024

type ColumnBatch struct {
    Values   []byte     // Raw column values (type-specific)
    Nulls    []byte     // Null bitmap
    RowCount int
    DataType DataType
}

type VectorizedExecutor struct {
    batchSize int
}

// ExecuteScan processes a column scan with predicate pushdown
func (e *VectorizedExecutor) ExecuteScan(
    ctx context.Context,
    column *ColumnReader,
    predicate Predicate,
) (*roaring.Bitmap, error) {
    result := roaring.New()
    rowOffset := uint32(0)

    for column.HasNext() {
        batch := column.NextBatch(e.batchSize)

        // Vectorized predicate evaluation
        mask := e.evaluatePredicate(batch, predicate)

        // Collect matching row IDs
        for i := 0; i < batch.RowCount; i++ {
            if mask[i] {
                result.Add(rowOffset + uint32(i))
            }
        }
        rowOffset += uint32(batch.RowCount)
    }
    return result, nil
}
```

### 9.3 Query Planning — 4-Phase Pipeline

```
// Phase 1: SQL Parse → AST
//   Input:  'SELECT name, age FROM users WHERE age > 30 AND city = "NYC"'
//   Output: AST with predicates: [(age, GT, 30), (city, EQ, 'NYC')]
//
// Phase 2: Column Pruning
//   Only fetch column sections for: age, city, name
//   Skip all other 197 columns (byte-range GET on ArkFormat)
//
// Phase 3: 6-Layer Predicate Pushdown + File Pruning
//   Layer 1: Manifest pruning (time range)
//   Layer 2: Secondary index lookup (if indexed)
//   Layer 3: Zone map pruning (age max <= 30? skip file)
//   Layer 4: Bloom filter (city = 'NYC' absent? skip file)
//   Layer 5: PGM index (if cluster key or hot column)
//   Layer 6: Roaring bitmap intersection (age_results AND city_results)
//
// Phase 4: Vectorized Execution + Row Materialization
//   Fetch matching column batches via byte-range GET
//   Vectorized predicate evaluation on column arrays
//   Intersect row_id bitmaps across predicates
//   Fetch projection columns (name) for matching row_ids
//   Return result set

type QueryPlanner struct {
    catalog     *CatalogClient
    indexLookup *IndexLookup
    walBuf      *QueryNodeWALBuffer
}

func (p *QueryPlanner) Plan(ctx context.Context, sql string) (*ExecutionPlan, error) {
    ast, _ := sqlparser.Parse(sql)

    // Phase 2: Column pruning
    neededCols := p.extractProjection(ast)
    predicateCols := p.extractPredicates(ast)

    // Phase 3: Multi-layer pruning
    candidatePartitions := p.catalog.GetPartitions(ast.TableID)

    // Layer 1: Manifest pruning (time range)
    candidatePartitions = p.pruneByTimeRange(candidatePartitions, ast.TimePredicates)

    // Layer 2: Secondary index (if available)
    for _, pred := range predicateCols {
        if indexed, partIDs := p.indexLookup.FindPartitions(ctx, pred); indexed {
            candidatePartitions = filterByIDs(candidatePartitions, partIDs)
        }
    }

    // Layer 3: Zone map pruning
    candidatePartitions = p.pruneByZoneMaps(candidatePartitions, predicateCols)

    // Layer 4: Bloom filter pruning
    candidatePartitions = p.pruneByBloomFilters(candidatePartitions, predicateCols)

    return &ExecutionPlan{
        Partitions:   candidatePartitions,
        Predicates:   predicateCols,
        ProjectCols:  neededCols,
        UsePGM:       p.hasPGMIndex(predicateCols),
        UseRoaring:   len(predicateCols) > 1,
        Vectorized:   true,
    }, nil
}
```

### 9.4 Distributed Query Execution at Exabyte Scale

At exabyte scale, a single query may need to scan millions of partitions. The query planner generates a distributed execution plan that fans out across N query nodes:

```
// Distributed query execution:
//
// 1. Planner divides partition list into N shards (one per query node)
// 2. Each node executes its shard independently:
//    - 6-layer pruning on its partition subset
//    - Vectorized scan on surviving partitions
//    - Produces partial Roaring bitmap of matching row_ids
// 3. Coordinator node unions/intersects partial results
// 4. Coordinator fetches projection columns for final row_ids
// 5. Final result assembled and returned
//
// Fan-out for a 1EB query across 1000 query nodes:
//   Partitions per node: 4.3B / 1000 = ~4.3M partitions
//   Catalog metadata per node: 4.3M × 200 bytes = 860MB (fits in RAM)
//   Zone map pruning: 4.3M × 100ns = 430ms per node
//   After pruning (99% eliminated): ~43K partitions to bloom-check
//   Bloom checks: 43K × 1μs = 43ms per node
//   Surviving partitions: ~100-200 per node
//   Vectorized scan: 200 × 128MB   / 7GB/s NVMe = ~7.3s per node
//   Total: <10s for a cold exabyte-scale query

type DistributedQueryCoordinator struct {
    nodes   []QueryNodeClient
    catalog *CatalogClient
}

func (c *DistributedQueryCoordinator) Execute(ctx context.Context, plan *ExecutionPlan) (*ResultSet, error) {
    shards := shardPartitionList(plan.Partitions, len(c.nodes))

    partialResults := make(chan *PartialResult, len(c.nodes))
    for i, node := range c.nodes {
        go func(n QueryNodeClient, shard PartitionShard) {
            result, _ := n.ExecuteShard(ctx, shard, plan.Predicates)
            partialResults <- result
        }(node, shards[i])
    }

    // Collect and merge partial results
    merged := collectAndMerge(partialResults, len(c.nodes))

    // Fetch projection columns for final row_ids
    return c.fetchProjection(ctx, plan.ProjectCols, merged)
}
```

---

## 10. Catalog & Metadata Architecture

### 10.1 V3 Catalog: Hierarchical In-Memory Metadata Service

```
// Catalog hierarchy for exabyte scale:
//
// GlobalCatalog
//   → TableCatalog (per table)
//       → PartitionIndex (sorted by cluster key range)
//           → [PartitionMeta, PartitionMeta, ...] (sorted by time_min)
//       → HotColumnIndex (per hot column)
//           → [SortedRunMeta, ...] (sorted by value range)
//       → SecondaryIndexes (per indexed column)
//           → [IndexPartitionMeta, ...] (hash-bucketed)

type PartitionMeta struct {
    PartitionSeq  uint64     // Unique file identifier
    S3Path        string     // Full S3 URI
    TimeMin       int64      // Min timestamp (cluster key range)
    TimeMax       int64      // Max timestamp
    RowCount      uint64
    FileSize      uint64
    LSNMin        uint64
    LSNMax        uint64
    Level         uint8      // 0=L0, 1=L1, 2=L2
    ColumnCount   uint16
    // Per-column zone map summaries (min/max only — full zone maps in file)
    ZoneMaps      []ZoneMapSummary
    CreatedAt     int64
    Checksum      [16]byte
}

type ZoneMapSummary struct {
    ColumnID  uint32
    MinValue  [16]byte
    MaxValue  [16]byte
    NullCount uint64
}

// At 1EB with 128MB   partitions: ~3.9 billion partitions
// PartitionMeta size: ~300 bytes + ~32 bytes per column zone map summary
// With 200 columns: 300 + 200*32 = ~6,700 bytes per partition
// Total catalog: 3.9B × 6,700 = ~26TB
// This CANNOT fit in a single node — requires catalog sharding.
//
// Catalog sharding: namespace by table_id hash
// 2,000 catalog shard nodes × ~13GB each = ~26TB total
// Each shard node: r6i.2xlarge (8 vCPU, 64GB RAM) — fits comfortably
```

### 10.2 Catalog Persistence & Recovery

| Property               | Value                                   | Notes                                      |
| :--------------------- | :-------------------------------------- | :----------------------------------------- |
| Catalog Storage Format | ArkFormat (.arkmeta)                    | Same format as data files                  |
| In-Memory Index        | Sorted []PartitionMeta per table        | Binary search for O(log F) lookup          |
| Persistence            | S3 append-only log of catalog mutations | Recovery: replay log from last snapshot    |
| Snapshot Frequency     | Hourly (post-compaction)                | Aligned with compaction cycle              |
| Shard Count (1EB)      | 2,000 catalog shards                    | Each shard: ~13GB in RAM                   |
| Consistency Model      | Eventual (within compaction cycle)      | Writes visible after WAL + next compaction |
| Catalog Lookup SLA     | <1ms (P99) in-memory                    | No disk I/O on query hot path              |

---

## 11. Consistency, Transactions, SQL Semantics

- LSN-based MVCC: queries run against snapshot LSN S. Writes with LSN ≤ S are visible.
- Simple single-statement transactions with snapshot guarantees.
- For multi-row atomicity: ingest node groups row-writes into single LSN-batch atomic append; compaction respects batch LSNs.
- Provide explicit `FLUSH`/`FORCE_COMMIT` for clients that need synchronous durability beyond WAL fsync.
- Schema evolution is additive only. Dropping a column creates a tombstone in the catalog — existing files are not rewritten. Column data is absent (NULL) for rows predating the column addition.

---

## 12. Exabyte Scale Architecture

### 12.1 Namespace Sharding Strategy

Exabyte scale cannot be achieved through vertical scaling. Arkilian 3.0 achieves exabyte scale through namespace sharding: the table namespace is partitioned across N WAL coordinators, N compaction engines, and N catalog shards, where N scales horizontally without bound.

```
// Namespace shard assignment:
//   shard_id = xxhash(table_id) % num_shards
//
// All operations for a table go to the same shard:
//   - WAL coordinator shard: handles writes for this table
//   - Compaction engine shard: compacts WAL for this table
//   - Catalog shard: serves metadata for this table
//   - S3 prefix: derived from shard_id for prefix entropy
//
// S3 prefix entropy (prevents prefix rate-limiting at 3,500 req/sec):
//   s3://bucket/{entropy_prefix}/{table_id}/partitions/{level}/{partition}.ark
//   entropy_prefix = hex(xxhash(table_id) & 0xFFFF)  // 65536 prefixes

func S3Path(tableID uint64, level int, partSeq uint64) string {
    entropy := xxhash.Sum64(binary.BigEndian.AppendUint64(nil, tableID))
    prefix := fmt.Sprintf("%04x", entropy&0xFFFF)
    return fmt.Sprintf("s3://arkilian/%s/t%d/partitions/L%d/%020d.ark",
        prefix, tableID, level, partSeq)
}
```

### 12.2 Exabyte Scale Configuration

| Component              | Count (1EB)                  | Spec per Node                     | Total Capacity                   |
| :--------------------- | :--------------------------- | :-------------------------------- | :------------------------------- |
| WAL Coordinator (Raft) | 300 clusters × 3 nodes = 900 | c6i.4xlarge (16 vCPU, 32GB)       | 5B rows/sec aggregate            |
| Ingest Node            | 10,000                       | c6i.2xlarge (8 vCPU, 16GB)        | 50M rows/sec aggregate           |
| Compaction Engine      | 1,000                        | c6i.8xlarge (32 vCPU, 64GB, NVMe) | 1,000 hourly sorts in parallel   |
| Query Node             | 5,000                        | r6i.4xlarge (16 vCPU, 128GB RAM)  | 50,000 concurrent queries        |
| Catalog Shard          | 2,000                        | r6i.2xlarge (8 vCPU, 64GB RAM)    | 3.9B PartitionMeta entries total |
| Router                 | 100                          | c6i.xlarge (4 vCPU, 8GB)          | High-availability routing        |

### 12.3 Storage Configuration

| Parameter                   | Value                                  | Rationale                                          |
| :-------------------------- | :------------------------------------- | :------------------------------------------------- |
| Partition File Size (L1/L2) | 128MB (default, configurable to 128MB) | Optimal for S3 GET amortization at exabyte scale   |
| Partition File Size (L0)    | 64-128MB                               | Fresh compaction — smaller for faster L0→L1 merge  |
| WAL Segment Size            | 64MB                                   | ~100ms segment at 5M rows/sec                      |
| Hot-Column Run Size         | 128MB (L1)                             | Aligned with primary partitions                    |
| Compression                 | ZSTD level 3 (default)                 | 3-5× compression ratio, decompression at 1.5GB/s   |
| S3 Storage Class (hot)      | S3 Standard                            | Last 7 days — fast access                          |
| S3 Storage Class (warm)     | S3 Intelligent-Tiering                 | 7-90 days — auto-tier                              |
| S3 Storage Class (cold)     | S3 Glacier Instant Retrieval           | 90+ days — sub-second retrieval, low cost          |
| Target Storage Cost         | <$0.06/GB/month                        | Achieved via ZSTD + hourly batch + Glacier tiering |

---

## 13. Performance Targets & Validation

### 13.1 Write Path Targets

| Metric                      | Target       | Validation Method                   | Failure Mode                   |
| :-------------------------- | :----------- | :---------------------------------- | :----------------------------- |
| Write Ack Latency (P50)     | <2ms         | WAL Raft commit + group fsync       | Raft election > 5s → alert     |
| Write Ack Latency (P99)     | <5ms         | WAL group commit + NVMe sync        | fsync > 10ms → degrade         |
| Write Throughput (per node) | 5M rows/sec  | Batch columnar encode to WAL        | Backpressure if WAL full       |
| WAL Visibility (P95)        | <50ms        | Pub/sub notification to query nodes | Router lag > 100ms → alert     |
| Compaction Cycle Time       | <50 min/hour | Must complete before next hour      | Backlog triggers extra workers |
| S3 PUT Latency (per file)   | <500ms (P95) | 128MB single PUT, US-East-1         | Retry with exponential backoff |
| Raft Leader Failover        | <5s          | Raft election timeout               | Ingest nodes buffer and retry  |

### 13.2 Read Path Targets

| Query Type                         | Target Latency | Key Mechanism                            | Scale Assumption          |
| :--------------------------------- | :------------- | :--------------------------------------- | :------------------------ |
| Point Lookup (hot, in WAL buffer)  | <5ms (P95)     | In-memory WAL buffer scan                | Single node, single value |
| Point Lookup (cluster key, cached) | <20ms (P95)    | PGM index + NVMe cache hit               | Partition on NVMe         |
| Point Lookup (hot column, cached)  | <20ms (P95)    | Sorted run PGM index + NVMe              | Sorted run on NVMe        |
| Point Lookup (cold column, S3)     | <300ms (P95)   | Zone map + bloom prune → S3 range GET    | Cold start, no cache      |
| Range Scan (10GB, cluster key)     | <500ms (P95)   | Sequential read of sorted partitions     | NVMe cached, 10 workers   |
| Range Scan (1TB)                   | <30s (P95)     | Distributed: 1000 query nodes × parallel | Distributed execution     |
| Full Column Scan (1TB)             | <2min (P95)    | Vectorized sequential read, distributed  | 1000 nodes, NVMe          |
| Aggregation (COUNT, SUM, 1TB)      | <5s            | Vectorized SIMD aggregation, distributed | NVMe + distributed        |
| Multi-predicate (3 predicates)     | <50ms (P95)    | Roaring bitmap intersection              | Cached partitions         |

### 13.3 Search Complexity Summary

| Column Type                | Point Lookup                  | Range Scan            | Mechanism                                 |
| :------------------------- | :---------------------------- | :-------------------- | :---------------------------------------- |
| Cluster key                | O(log F + 1)                  | O(log F + sequential) | Binary search on catalog + PGM index      |
| Hot column (sorted run)    | O(log F + 1)                  | O(log F + sequential) | Binary search on sorted run catalog + PGM |
| Indexed column (secondary) | O(1) index lookup             | N/A                   | Secondary index partition → partition IDs |
| Cold column                | O(F × zone_map_check + bloom) | O(F × zone_map_check) | Zone map prune + bloom filter + scan      |

---

## 14. Implementation Roadmap — 24 Weeks

### Phase 0: SQLite Removal & Foundation (Weeks 1-2)

| Task                                   | Deliverable                                                      | Success Gate                               |
| :------------------------------------- | :--------------------------------------------------------------- | :----------------------------------------- |
| Remove all SQLite dependencies         | Zero SQLite imports in codebase                                  | `grep -r 'sqlite' internal/` returns empty |
| Define ArkFormat read/write interfaces | `internal/format/ark_writer.go`, `internal/format/ark_reader.go` | Write 10M INT64 values to .ark in <200ms   |
| Define RowID system                    | `type RowID uint64 = (partition_seq << 32) \| row_offset`        | RowID encode/decode round-trips correctly  |
| Define column type system              | `internal/format/types.go`                                       | All 9 data types supported                 |

### Phase 1: ArkFormat Engine (Weeks 3-6)

| Task                            | Deliverable                                                          | Success Gate                                   |
| :------------------------------ | :------------------------------------------------------------------- | :--------------------------------------------- |
| ArkFormat writer (multi-column) | Complete .ark file writer with column directory, data blocks, footer | Write 200-column partition in <500ms           |
| ArkFormat reader (column-skip)  | Reader that fetches specific columns via byte-range offset           | Read 2 of 200 columns in <50ms from local file |
| ZSTD compression integration    | Per-block ZSTD encode/decode                                         | 3-5× compression ratio verified                |
| Zone map computation            | Per-column, per-block min/max/null_count                             | Zone maps correct for all data types           |
| Bloom filter (Blocked Cuckoo)   | Per-column bloom filter in footer                                    | FPR <0.5% on 10M distinct values               |
| PGM index builder               | PGM index for sorted columns                                         | Lookup within ±64 positions of target          |
| XXH3 checksums                  | File-level and section-level checksums                               | Corruption detected on single bit flip         |

### Phase 2: Shared WAL (Weeks 5-8)

| Task                          | Deliverable                                       | Success Gate                                |
| :---------------------------- | :------------------------------------------------ | :------------------------------------------ |
| WAL coordinator (Raft-backed) | `internal/wal/coordinator.go` with Raft consensus | Leader election in <5s, LSN assignment <1ms |
| Multi-writer atomic append    | Concurrent ingest nodes write to shared WAL       | 5M rows/sec sustained, zero LSN gaps        |
| Group fsync                   | Batch concurrent fsyncs into single syscall       | fsync <5ms P99, 100× reduction in syscalls  |
| WAL segment management        | 64MB segment rotation, 3x replication             | Segments rotate cleanly, no data loss       |
| S3 async upload               | Background upload of committed segments           | Segments in S3 within 100ms of commit       |
| Query node subscription       | Pub/sub WAL event notification                    | Query nodes see writes within 50ms          |
| WAL recovery                  | Replay uncommitted entries on startup             | Zero data loss after coordinator crash      |

### Phase 3: Compaction Engine (Weeks 7-10)

| Task                         | Deliverable                                        | Success Gate                                       |
| :--------------------------- | :------------------------------------------------- | :------------------------------------------------- |
| Hourly compaction cycle      | `internal/compaction/engine.go`                    | Cycle completes in <50 min at 5M rows/sec          |
| External merge sort          | Sort by cluster key with bounded RAM               | Correct sort of 100GB+ data                        |
| Primary partition writer     | WAL → sorted ArkFormat micro-partitions            | 128MB files, cluster-key sorted, zone maps correct |
| L0→L1 merge                  | Merge overlapping L0 files into non-overlapping L1 | L1 files: zero time-range overlap                  |
| L1→L2 major compaction       | Merge L1 files for long-term storage               | L2 files: globally sorted, non-overlapping         |
| Hot-column sorted run writer | Generate sorted runs for hot columns               | Sorted runs with PGM index, non-overlapping values |
| Compaction checkpoint        | Atomic checkpoint after S3 upload confirmed        | No data loss on compaction crash                   |

### Phase 4: Search & Indexing (Weeks 9-14)

| Task                                  | Deliverable                         | Success Gate                                |
| :------------------------------------ | :---------------------------------- | :------------------------------------------ |
| 6-layer pruning stack                 | `internal/search/pruner.go`         | Each layer measurably reduces candidate set |
| Manifest pruning (Layer 1)            | Time-range + shard selection        | <1ms catalog lookup                         |
| Secondary index builder (Layer 2)     | `internal/index/builder.go`         | Index build <5 min for 1TB                  |
| Secondary index lookup                | `internal/index/lookup.go`          | Lookup <10ms cached                         |
| Automated index policy                | `internal/index/policy.go`          | Auto-creates indexes for hot predicates     |
| Zone map pruning (Layer 3)            | In-memory min/max comparison        | <100ns per partition                        |
| Bloom filter pruning (Layer 4)        | Blocked Cuckoo filter check         | <1μs per partition, FPR <0.5%               |
| PGM index lookup (Layer 5)            | Learned index on sorted columns     | O(1) page lookup, ±64 error bound           |
| Roaring bitmap intersection (Layer 6) | Multi-predicate row_id intersection | <1ms for 1M-element intersection            |

### Phase 5: SQL Query Engine (Weeks 11-17)

| Task                          | Deliverable                                    | Success Gate                                       |
| :---------------------------- | :--------------------------------------------- | :------------------------------------------------- |
| SQL parser (extend v2)        | Support ANSI SQL:2016 subset                   | SELECT/WHERE/GROUP BY/ORDER BY/LIMIT/HAVING        |
| Column pruner                 | Only fetch needed columns via byte-range GET   | 97% data reduction on 200-column table             |
| Predicate pushdown            | Map SQL predicates to 6-layer pruning          | Correct predicate translation for all operators    |
| Vectorized execution engine   | SIMD batch processing of column arrays         | 4-8× throughput vs row-by-row                      |
| Distributed query coordinator | Fan-out across N query nodes                   | 1PB scan in <5 min across 1000 nodes               |
| Row materialization           | Reconstruct rows from column data + row_ids    | Correct row assembly for all data types            |
| WAL buffer merge              | Merge recent WAL data with column file results | Recent writes visible in query results             |
| Query statistics aggregator   | `internal/observability/query_stats.go`        | Track predicate frequency, drive hot-column policy |

### Phase 6: Catalog Service (Weeks 15-18)

| Task                           | Deliverable                      | Success Gate                      |
| :----------------------------- | :------------------------------- | :-------------------------------- |
| In-memory hierarchical catalog | `internal/catalog/service.go`    | <1ms P99 lookup                   |
| S3 persistence                 | Catalog snapshot + mutation log  | Recovery from S3 in <60s          |
| Catalog sharding               | Namespace by table_id hash       | 2,000 shards at 1EB scale         |
| Hot-column metadata            | Track sorted runs in catalog     | Sorted run lookup <1ms            |
| Partition registration         | Atomic register after compaction | No orphaned or missing partitions |

### Phase 7: Scale Validation & Hardening (Weeks 19-24)

| Task                          | Deliverable                                    | Success Gate                               |
| :---------------------------- | :--------------------------------------------- | :----------------------------------------- |
| 1PB load test                 | Sustained ingest + query at 1PB                | All P95 targets met                        |
| Distributed query test        | 500-node query fan-out                         | 1PB scan <5 min                            |
| Chaos: WAL coordinator kill   | Kill Raft leader mid-write                     | Zero data loss, recovery <10s              |
| Chaos: compaction engine kill | Kill mid-sort                                  | Compaction resumes from checkpoint         |
| Chaos: S3 outage 30s          | Simulate S3 unavailability                     | Zero data loss, ingest resumes within 10s  |
| Chaos: catalog shard kill     | Kill catalog node                              | Shard recovers from S3 in <60s             |
| Cost analysis                 | 30-day cost model at 1PB                       | <$0.06/GB/month confirmed                  |
| Exabyte simulation            | Object-store emulator + realistic S3 latencies | Catalog handles 3.9B partition metadata    |
| Property-based testing        | Random value distributions, edge cases         | Zero false negatives in search             |
| TPC-H benchmark               | Q1-Q6 on columnar data                         | All queries pass correctness + performance |

---

## 15. Engineering Directives

### 15.1 Absolute Mandates

**⚠ DIRECTIVE:** SQLite is prohibited in all packages. Use `grep -r 'sqlite' internal/` to enforce. Any PR introducing SQLite is rejected without review.

**⚠ DIRECTIVE:** Primary partitions at L1/L2 MUST have non-overlapping cluster-key ranges. Any compaction producing overlapping L1 files is a correctness bug — treat as P0 production incident.

**⚠ DIRECTIVE:** The WAL is the source of truth. Data acknowledged to the client MUST be in the WAL (Raft-committed). Data not in the WAL is not data. Never ack before Raft commit + fsync.

**⚠ DIRECTIVE:** File sizes MUST be 128MB (±5%) for L1/L2. Variable-size files break the catalog's assumptions. The compactor must enforce this strictly.

**⚠ DIRECTIVE:** Multi-column ArkFormat files MUST include a complete column directory. A file without a valid column directory is corrupt and must be rejected.

**⚠ DIRECTIVE:** Hot-column sorted runs are supplementary indexes, not replacements for primary partitions. Deleting a sorted run must never cause data loss — the data exists in primary partitions.

**⚠ DIRECTIVE:** Binary search and PGM index correctness must be validated with property-based tests (Go's `rapid` or `gopter`) generating random value distributions. Test empty files, single-element files, duplicate values, and all 9 data types.

### 15.2 Performance Directives

1. Hot path allocations: zero. Use `sync.Pool` for `ColumnBatch`, `[]PartitionMeta`, `*roaring.Bitmap`, and sort buffers. Profile with `go tool pprof` before any PR touching the search or vectorized execution path.

2. ArkFormat reads: use `mmap(2)` for zone maps, bloom filters, and PGM indexes. Zero-copy reads. Do NOT use `io.ReadAll` on hot-path file reads.

3. Vectorized execution: column batches MUST be processed in batches of 1024+ values. Row-by-row iteration on column data is prohibited in the query engine.

4. Sort comparisons: INT64 and FLOAT64 column sorts must use SIMD-aware Go sort (`slices.SortFunc` in Go 1.21+). Validate with benchmark showing ≥1B comparisons/sec on target hardware.

5. S3 parallel GET: implement a minimum of 8 concurrent S3 GETs per query node. Never serialize S3 fetches for independent partitions.

6. Compaction parallelism: one goroutine per table, bounded by `runtime.NumCPU() × 2` semaphore. Tables are independent — there is no reason to serialize them.

7. Roaring bitmap operations: use `roaring.And()` for intersection, not manual iteration. The library uses SIMD-optimized bitwise AND internally.

8. PGM index construction: build during compaction (offline), not during query (online). PGM build is O(N) — acceptable during hourly compaction, not during sub-20ms lookups.

### 15.3 Correctness Directives

9. Every ArkFormat file written must be verified with XXH3 checksum on read. Corrupted files trigger automatic re-download from S3 replica — never serve corrupt data.

10. LSN gaps in the WAL are impossible under Raft consensus. If detected, this indicates a Raft implementation bug — halt the cluster and alert immediately.

11. Search must return zero false negatives. A value present in the data must always be found by the pruning stack. False positives (extra results filtered post-fetch) are acceptable; false negatives are data loss.

12. The compaction checkpoint (last compacted LSN) must be atomically updated after all files are durably written to S3. Never update the checkpoint before S3 confirms all files uploaded.

13. Schema evolution (adding a new column) must be additive only. Dropping a column creates a tombstone in the catalog — existing files are not rewritten.

14. Roaring bitmap intersection must be commutative and associative. The order of predicate evaluation must not affect query results. Validate with property tests.

15. Zone map min/max values must be exact (not approximate). A zone map claiming max=100 for a block that contains value 101 is a correctness bug.

### 15.4 Operational Directives

16. Benchmark on live S3, not simulated latency. If a target fails on real S3 (US-East-1, standard tier), it fails. No exceptions.

17. Chaos tests are mandatory before each phase gate. Kill WAL Raft leader mid-segment. Kill compaction engine mid-sort. Simulate S3 outage for 30 seconds. Data must be 100% recoverable in all cases.

18. Every component exposes Prometheus metrics. Minimum required:
    - `wal_raft_commit_duration_seconds`
    - `wal_fsync_duration_seconds`
    - `compaction_cycle_duration_seconds`
    - `compaction_sort_duration_seconds`
    - `pruning_layer_eliminated_partitions` (per layer)
    - `pgm_lookup_duration_seconds`
    - `roaring_intersection_duration_seconds`
    - `s3_get_latency_seconds`
    - `s3_put_latency_seconds`
    - `catalog_lookup_duration_seconds`
    - `vectorized_batch_throughput_rows_per_sec`
    - `hot_column_sorted_run_count` (per table)

19. The codebase uses a single Go module. All components share `internal/` packages. No microservice-style separate repos — compile-time interface contracts enforced.

20. Hot-column policy decisions must be logged with full context: column name, query frequency, threshold, action taken. Operators must be able to audit why a sorted run was created or dropped.

---

## 16. New Package Layout

```
internal/
  format/                     # ArkFormat file format
    types.go                  # Column types, RowID, Value
    ark_writer.go             # Multi-column ArkFormat writer
    ark_reader.go             # Column-skip reader with byte-range support
    zone_map.go               # Zone map computation and lookup
    bloom.go                  # Blocked Cuckoo Filter
    pgm.go                    # PGM learned index
    roaring.go                # Roaring bitmap helpers
    checksum.go               # XXH3 checksum utilities
    encoding.go               # PLAIN, DELTA, RLE, DICT, BITPACK encoders
    compression.go            # ZSTD, LZ4, Snappy wrappers
    format_test.go
  wal/                        # Shared distributed WAL
    coordinator.go            # Raft-backed WAL coordinator
    segment.go                # WAL segment read/write
    group_fsync.go            # Group commit fsync batching
    recovery.go               # WAL replay on startup
    subscriber.go             # Query node WAL subscription
    wal_test.go
  compaction/                 # Hourly compaction engine (extends v2)
    engine.go                 # Hourly cycle orchestrator
    sorter.go                 # External merge sort
    partition_writer.go       # Primary partition ArkFormat writer
    sorted_run_writer.go      # Hot-column sorted run writer
    level_merger.go           # L0→L1, L1→L2 merge
    hot_column_policy.go      # Automated hot-column detection
    compaction_test.go
  search/                     # 6-layer pruning stack
    pruner.go                 # Orchestrates all 6 layers
    manifest_pruner.go        # Layer 1: manifest/time-range pruning
    zone_map_pruner.go        # Layer 3: zone map min/max pruning
    bloom_pruner.go           # Layer 4: bloom filter pruning
    pgm_searcher.go           # Layer 5: PGM index lookup
    roaring_intersect.go      # Layer 6: Roaring bitmap intersection
    search_test.go
  index/                      # Secondary index partitions (Layer 2)
    builder.go                # Index partition builder
    lookup.go                 # Index lookup (value → partition_ids)
    policy.go                 # Automated index create/drop policy
    schema.go                 # Index partition schema
    index_test.go
  query/                      # SQL query engine (extends v2)
    parser/                   # SQL parser (extend existing)
    planner/                  # Query planner with 6-layer integration
      planner.go              # 4-phase query planning
      distributed.go          # Distributed query coordinator
    executor/                 # Vectorized execution engine
      vectorized.go           # SIMD batch column processing
      executor.go             # Query execution orchestrator
    aggregator/               # Result aggregation (extend existing)
  catalog/                    # In-memory hierarchical catalog
    service.go                # Catalog service with sharding
    shard.go                  # Per-shard catalog management
    persistence.go            # S3 snapshot + mutation log
    catalog_test.go
  cache/                      # Tiered caching (extend v2)
    nvme.go                   # NVMe cache tier
    graph.go                  # Co-access graph (from v2)
    tiered.go                 # L1 (RAM) → L2 (NVMe) → L3 (S3)
  router/                     # Write notification bus (extend v2)
    notifier.go               # Pub/sub for WAL events
    subscriber.go             # Query node subscription
  observability/              # Query statistics
    query_stats.go            # Predicate frequency tracking
    metrics.go                # Prometheus metrics export

cmd/
  arkilian/                   # Unified binary (extend existing)
  arkilian-ingest/            # Ingest node (extend existing)
  arkilian-query/             # Query node (extend existing)
  arkilian-compact/           # Compaction engine (extend existing)
  arkilian-wal/               # NEW: WAL coordinator service
    main.go
  arkilian-catalog/           # NEW: Catalog service
    main.go
```

---

## 17. General Availability Validation Checklist

Before declaring General Availability for 1PB workloads (Phase 7 gate):

| #   | Validation Criterion                                                            | Method                                                               | Owner Team       |
| :-- | :------------------------------------------------------------------------------ | :------------------------------------------------------------------- | :--------------- |
| 1   | Zero SQLite dependencies in entire codebase                                     | CI: `grep -r 'sqlite' internal/` returns empty                       | Platform         |
| 2   | ArkFormat: write + read 10B rows across 200 columns with correct XXH3 checksum  | Integration test, automated                                          | Storage          |
| 3   | ArkFormat: column-skip read fetches only requested columns (verify byte ranges) | Integration test with S3 access log analysis                         | Storage          |
| 4   | WAL Raft commit <5ms P99 at 5M rows/sec sustained for 1 hour                    | Load test: custom ingest harness                                     | Ingest           |
| 5   | WAL group commit reduces fsync syscalls by >100× vs naive per-entry fsync       | `strace` + `perf stat` during load test                              | Ingest           |
| 6   | Raft leader failover: zero data loss, new leader elected in <5s                 | Chaos test: kill leader, verify LSN continuity                       | Ingest           |
| 7   | L1 primary partitions: zero cluster-key-range overlap after compaction          | Property test: scan all PartitionMeta, assert sorted non-overlapping | Compaction       |
| 8   | Hot-column sorted runs: zero value-range overlap at L1                          | Property test: scan all SortedRunMeta                                | Compaction       |
| 9   | Hourly compaction cycle completes in <50 min at 5M rows/sec ingest              | 1-week sustained ingest test                                         | Compaction       |
| 10  | 6-layer pruning: each layer measurably reduces candidate set (logged)           | Integration test with Prometheus metrics                             | Search           |
| 11  | PGM index: lookup within ±64 positions on 10B sorted values                     | Microbenchmark + property test                                       | Search           |
| 12  | Bloom filter: FPR <0.5% on 1B distinct values per column                        | Statistical test: 10M probes with known-absent values                | Search           |
| 13  | Roaring bitmap: intersection of 2× 10M-element bitmaps in <1ms                  | Microbenchmark                                                       | Search           |
| 14  | Point lookup <20ms P95 on NVMe-cached cluster-key column                        | Benchmark: 1M random lookups                                         | Search           |
| 15  | Point lookup: zero false negatives on 10B inserted values                       | Property test: insert → verify found                                 | Search           |
| 16  | Vectorized execution: 4× throughput vs row-by-row on SUM/COUNT                  | Microbenchmark: vectorized vs scalar                                 | Query            |
| 17  | SQL SELECT/WHERE/GROUP BY/ORDER BY/LIMIT pass TPC-H Q1-Q6                       | TPC-H benchmark harness                                              | Query            |
| 18  | Distributed query: 1PB scan completes in <5 min across 1,000 nodes              | Live S3 test, US-East-1                                              | Query            |
| 19  | Catalog lookup <1ms P99 for 3.9B PartitionMeta in-memory                        | Microbenchmark: catalog shard with simulated entries                 | Catalog          |
| 20  | Catalog recovery from S3 snapshot in <60s                                       | Chaos test: kill catalog node, measure recovery                      | Catalog          |
| 21  | Hot-column policy: auto-creates sorted run when threshold exceeded              | Integration test: generate query load, verify sorted run created     | Compaction       |
| 22  | WAL node kill: zero data loss, recovery in <10s                                 | Chaos test: kill Raft follower and leader                            | Platform         |
| 23  | S3 outage 30s: zero data loss, ingest resumes within 10s of recovery            | Chaos test: S3 proxy deny, then allow                                | Platform         |
| 24  | Storage cost <$0.06/GB/month at 1PB scale                                       | Cost model: S3 + compute + network, 30-day run                       | Finance/Platform |
| 25  | Multi-predicate query correctness: bitmap intersection matches brute-force      | Property test: random predicates, compare results                    | Query            |

---

## 18. Migration Strategy: V2 → V3

### 18.2 Migration Phases

| Phase                       | Duration   | What Happens                                                                                           |
| :-------------------------- | :--------- | :----------------------------------------------------------------------------------------------------- |
| Phase A: Dual-Write         | Weeks 1-4  | New writes go to V3 WAL. V2 ingest path disabled. V3 compaction produces ArkFormat.                    |
| Phase B: Background Convert | Weeks 5-12 | Background job converts historical V2 SQLite partitions to ArkFormat. Query engine reads both formats. |
| Phase C: V2 Sunset          | Week 13+   | All data in ArkFormat. SQLite legacy adapter removed. V2 code paths deleted.                           |

## 18. Risk Register

| Risk                                                          | Probability | Impact                        | Mitigation                                                                                                                 |
| :------------------------------------------------------------ | :---------- | :---------------------------- | :------------------------------------------------------------------------------------------------------------------------- |
| Raft consensus adds latency to write path                     | Medium      | Write ack >5ms                | Benchmark Raft commit latency early (Week 5). Use NVMe for Raft log. Consider single-node WAL for low-latency deployments. |
| Hourly compaction cannot keep up with 5M rows/sec             | Medium      | Compaction backlog grows      | Scale compaction workers horizontally. Reduce compaction window to 30 min. Add backpressure to ingest.                     |
| PGM index error bound too large for some data distributions   | Low         | Extra scan work during lookup | Configurable epsilon. Fall back to binary search if PGM error exceeds threshold.                                           |
| Hot-column detection creates too many sorted runs             | Medium      | S3 storage cost increase      | Cap at 10 hot columns per table. Sorted runs are supplementary — deletion is safe.                                         |
| 128MB file size too large for latency-sensitive workloads     | Low         | Point lookup latency increase | Configurable down to 128MB. Column-skip via byte-range GET mitigates (only fetch needed columns).                          |
| Catalog sharding at 2,000 nodes is operationally complex      | Medium      | Operational burden            | Start with fewer shards at sub-exabyte scale. Auto-shard when partition count exceeds threshold.                           |
| Vectorized execution engine is complex to implement correctly | Medium      | Query correctness bugs        | Extensive property-based testing. Scalar fallback for correctness validation.                                              |
| ArkFormat format changes during development                   | High        | Backward compatibility breaks | Version field in header. Reader supports all format versions. Never delete format support.                                 |
| Roaring bitmap memory usage on large result sets              | Low         | OOM on query nodes            | Stream results instead of materializing full bitmap. Cap bitmap size with early termination.                               |

---

## 20. Closing Statement

Arkilian 3.0 is a fundamental rearchitecture that synthesizes the strongest ideas from three independent design proposals while discarding their weaknesses.

From Claude: the structural backbone, the WAL design rigor, the engineering directives, and the mathematical analysis of search complexity. From Gemini: the critical correction that per-column files are an S3 cost disaster, and the insistence on vectorized execution. From ChatGPT: the hybrid sort strategy that delivers 97% of read performance at 3% of write cost, the PGM learned indexes that achieve O(1) page lookup, and the Roaring bitmap intersections that make multi-predicate queries sub-millisecond.

The result is a database where:

- Every write is durable in <5ms via Raft-backed shared WAL
- Every query passes through a 6-layer pruning stack that eliminates 99%+ of I/O
- Hot columns automatically get sorted runs with learned indexes — no manual tuning
- Columns are bundled into single files — no S3 object explosion
- Vectorized execution processes column arrays at SIMD speed
- Exabyte scale is achieved through namespace sharding, not vertical scaling
- Storage costs stay below $0.06/GB/month via hourly batch compaction and Glacier tiering

**_Build it. Break it. Make it unbreakable._**
