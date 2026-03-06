<br/>
<h1 align="center">Arkilian</h1>  
<p align="center">
  <a href="https://github.com/CodeDynasty-dev/birth-of-Arkilian">
    <img src="https://avatars.githubusercontent.com/u/261335565?s=88&v=4" alt="Arkilian Database"   
    >
  </a>
</p>

<!-- [![PRs Welcome](https://img.shields.io/badge/PRs-welcome-brightgreen.svg)](https://github.com/CodeDynasty-dev/birth-of-Arkilian/blob/next/contributing.md) -->

![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)
![Arkilian](https://img.shields.io/github/v/release/CodeDynasty-dev/birth-of-Arkilian)
[![Stargazers](https://img.shields.io/github/stars/CodeDynasty-dev/birth-of-Arkilian?style=social)](https://github.com/CodeDynasty-dev/birth-of-Arkilian)

### An immutable database that follows Snowflake architecture, designed for scalable, replayable systems beyond analytics.

Arkilian treats SQLite files as immutable 8-128MB micro-partitions stored in object storage (S3/GCS), with a stateless query federation layer providing Snowflake-like compute/storage separation.

## Why Arkilian?

Unlike monolithic SQLite distribution attempts (rqlite, LiteFS), Arkilian embraces SQLite's write single-node nature and partitions at the dataset level—no VFS complexity, no WAL coordination issues, no storage duplication.

**Target workloads:**

- Multi-tenant SaaS event stores
- Time-series analytics
- Edge-to-cloud sync with strong eventual consistency

**What you get:**

- SQLite's embedded simplicity + Snowflake's elasticity
- No forks of SQLite core
- No distributed transactions
- No consensus overhead on writes
- 1.0× replication factor (vs 3-5× in LiteFS)

## Architecture

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                              Client Applications                              │
│                         (HTTP/gRPC API Consumers)                            │
└─────────────────────────────────┬────────────────────────────────────────────┘
                                  │
        ┌─────────────────────────┼─────────────────────────┐
        │                         │                         │
        ▼                         ▼                         ▼
┌───────────────┐         ┌───────────────┐         ┌───────────────┐
│   arkilian-   │         │   arkilian-   │         │   arkilian-   │
│    ingest     │         │    query      │         │   compact     │
│   (Writers)   │         │  (Readers)    │         │  (Background) │
└───────┬───────┘         └───────┬───────┘         └───────┬───────┘
        │                         │                         │
        │                         │                         │
        ▼                         ▼                         │
┌───────────────┐         ┌───────────────┐                 │
│      WAL      │         │   Co-Access   │                 │
│  (fsync <10ms)│         │     Graph     │                 │
└───────┬───────┘         └───────┬───────┘                 │
        │                         │                         │
        └─────────────────────────┼─────────────────────────┘
                                  │
                                  ▼
┌──────────────────────────────────────────────────────────────────┐
│                        Manifest Catalog                          │
│                        (manifest.db)                             │
└──────────────────────────────────┬───────────────────────────────┘
                                   │
                                   ▼
┌──────────────────────────────────────────────────────────────────┐
│                     Object Storage (S3/GCS)                      │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐   │
│  │ events_20260205 │  │ events_20260205 │  │ events_20260206 │   │
│  │ _001.sqlite     │  │ _001.meta.json  │  │ _001.sqlite     │   │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘   │
│  ┌─────────────────┐  ┌─────────────────┐                        │
│  │ indexes/        │  │ cache/          │                        │
│  │ user_id/        │  │ nvme/           │                        │
│  │ 0.sqlite        │  │                 │                        │
│  └─────────────────┘  └─────────────────┘                        │
└──────────────────────────────────────────────────────────────────┘
```

### Features

| Feature                 | Description                                                 | Benefit                                       |
| ----------------------- | ----------------------------------------------------------- | --------------------------------------------- |
| **WAL**                 | Write-Ahead Log with local fsync before S3 upload           | <10ms write acknowledgment                    |
| **Secondary Indexes**   | Hash-partitioned index maps for point lookups on any column | <200ms queries on high-cardinality columns    |
| **NVMe Cache**          | Tiered L1 (RAM) → L2 (NVMe) → L3 (S3) cache                 | <1ms cache hits vs 50-200ms S3 GET            |
| **Co-Access Graph**     | Markov-like graph tracking partition access sequences       | Predictive prefetch for common query patterns |
| **Write Notifications** | In-process pub/sub for partition flush events               | <100ms write visibility                       |

All v2 features are enabled by default. The v1 synchronous ingest path has been removed.

## Core Principles

1. **Immutability**: Once written, SQLite files never change. Compaction produces new files.
2. **Partition Autonomy**: Each SQLite file is self-contained with its own indexes.
3. **Metadata-Driven Pruning**: Bloom filters + min/max stats eliminate >99% of I/O.
4. **Write Isolation**: Writers operate on disjoint partition keys (no distributed locking).

### Building

- Prerequisites
- Go 1.21+ in

![Go](https://img.shields.io/badge/Go-00ADD8?style=flat&logo=go&logoColor=white)

```bash
go build ./...
```

- Run benchmark

```bash
go test -v -bench=BenchmarkProd -run=TestProd -benchtime=2x -timeout=300s ./test/benchmark/
```

### Run Binary

```bash
# Run all services with a single command
./arkilian --data-dir /data/arkilian

# Or run specific services
./arkilian --mode ingest --data-dir /data/arkilian
./arkilian --mode query --data-dir /data/arkilian
./arkilian --mode compact --data-dir /data/arkilian
```

### Local-Only Mode (No Ports)

For local development and simple use cases, use `arkilian-local` which runs all services in a single process without network ports:

```bash
# Run local mode (all services, no network ports)
./arkilian-local --data-dir /data/arkilian-local

# Or with a config file
./arkilian-local --config config-local.yaml
```

**Key differences from the main binary:**

- No HTTP ports (8080, 8081, 8082)
- No gRPC server
- All services run in-process
- Simpler configuration
- Perfect for local development and testing

**When to use `arkilian-local`:**

- Local development and testing
- Single-node deployments
- CI/CD environments
- Learning and experimentation
- When you want a simpler setup without managing multiple services

**When to use the main `arkilian` binary:**

- Production deployments
- Multi-node clusters
- When you need HTTP/gRPC APIs
- When you need to run services separately
- When you need to expose the database to other services

## API

### Using the Local Binary

When using `arkilian-local`, you access the database directly through the data directory. The database files are stored in:

- **Data directory**: `--data-dir` (default: `./data/arkilian`)
- **Manifest**: `{data-dir}/manifest.db` (or sharded in `{data-dir}/manifest_*.db`)
- **Storage**: `{data-dir}/storage/` (partition files)
- **Downloads**: `{data-dir}/downloads/` (cached partitions for queries)
- **Compaction**: `{data-dir}/compaction/` (work files)

### Ingest Data (HTTP API)

For the main `arkilian` binary with HTTP API:

```bash
curl -X POST http://localhost:8080/v1/ingest \
  -H "Content-Type: application/json" \
  -d '{
    "partition_key": "20260206",
    "rows": [
      {
        "tenant_id": "acme",
        "user_id": 12345,
        "event_time": 1707235200,
        "event_type": "page_view",
        "payload": {"page": "/home"}
      }
    ]
  }'
```

**Response:**

```json
{
  "lsn": 42,
  "row_count": 1,
  "request_id": "req-xyz789",
  "status": "accepted"
}
```

The response includes an LSN (Log Sequence Number) for tracking. The partition_id becomes available after the background flusher processes the WAL entry (typically <1s).

### Query Data (HTTP API)

For the main `arkilian` binary with HTTP API:

```bash
curl -X POST http://localhost:8081/v1/query \
  -H "Content-Type: application/json" \
  -d '{
    "sql": "SELECT user_id, COUNT(*) FROM events WHERE tenant_id = '\''acme'\'' GROUP BY user_id LIMIT 10"
  }'
```

Response:

```json
{
  "columns": ["user_id", "count"],
  "rows": [
    [12345, 42],
    [67890, 37]
  ],
  "stats": {
    "partitions_scanned": 5,
    "partitions_pruned": 115,
    "execution_time_ms": 234
  },
  "request_id": "req-abc123"
}
```

## Partition Key Strategies

| Workload Type    | Partition Key | Target Size | Use Case            |
| ---------------- | ------------- | ----------- | ------------------- |
| Time-series      | `YYYYMMDD`    | 16-64 MB    | Event logs, metrics |
| Multi-tenant     | `tenant_id`   | 8-32 MB     | SaaS applications   |
| High-cardinality | `user_id % N` | 4-16 MB     | User analytics      |

## Query Optimization

Arkilian uses 2-phase partition pruning:

1. **Phase 1 (Min/Max)**: Query manifest catalog using column statistics
2. **Phase 2 (Bloom Filter)**: Apply bloom filters to eliminate false positives

Example: For a query on 100K partitions with `tenant_id = 'acme'`:

- Phase 1: 100K → 500 candidates (min/max pruning)
- Phase 2: 500 → 5 partitions (bloom filter, 99% reduction)

## Performance Targets

| Metric                  | Target            |
| ----------------------- | ----------------- |
| Write ack latency (P99) | <10ms             |
| Write visibility        | <100ms            |
| Indexed query latency   | <200ms            |
| Cold query latency      | <5s               |
| Cache hit latency       | <1ms              |
| Ingest throughput       | 50K rows/sec/node |
| Partition pruning ratio | 99.5%             |

## Testing

```bash
# Run all tests
go test ./...

# Run benchmarks
go test -bench=. ./test/benchmark/...

# Run integration tests
go test ./test/integration/...
```

## Configuration

### Unified Binary Configuration

The unified `arkilian` binary supports configuration via command-line flags, environment variables, and config files.

**Command-line flags:**

| Flag             | Default           | Description                               |
| ---------------- | ----------------- | ----------------------------------------- |
| `--config`       | -                 | Path to YAML/JSON config file             |
| `--data-dir`     | `./data/arkilian` | Base directory for all data files         |
| `--mode`         | `all`             | Service mode: all, ingest, query, compact |
| `--http-ingest`  | `:8080`           | HTTP address for ingest service           |
| `--http-query`   | `:8081`           | HTTP address for query service            |
| `--http-compact` | `:8082`           | HTTP address for compaction service       |
| `--grpc-addr`    | `:9090`           | gRPC server address                       |

**Environment variables:**

| Environment Variable         | Default              | Description              |
| ---------------------------- | -------------------- | ------------------------ |
| `ARKILIAN_MODE`              | `all`                | Service mode             |
| `ARKILIAN_DATA_DIR`          | `./data/arkilian`    | Base data directory      |
| `ARKILIAN_HTTP_INGEST_ADDR`  | `:8080`              | Ingest HTTP address      |
| `ARKILIAN_HTTP_QUERY_ADDR`   | `:8081`              | Query HTTP address       |
| `ARKILIAN_HTTP_COMPACT_ADDR` | `:8082`              | Compact HTTP address     |
| `ARKILIAN_GRPC_ADDR`         | `:9090`              | gRPC server address      |
| `ARKILIAN_STORAGE_TYPE`      | `local`              | Storage type (local, s3) |
| `ARKILIAN_STORAGE_PATH`      | `{data-dir}/storage` | Local storage path       |
| `ARKILIAN_S3_BUCKET`         | -                    | S3 bucket for partitions |
| `ARKILIAN_S3_REGION`         | `us-east-1`          | AWS region               |

**Example config file (config.yaml):**

```yaml
mode: all
data_dir: /data/arkilian

http:
  ingest_addr: ":8080"
  query_addr: ":8081"
  compact_addr: ":8082"

grpc:
  addr: ":9090"
  enabled: true

query:
  concurrency: 10
  pool_size: 100

compaction:
  check_interval: 5m
  min_partition_size: 8388608 # 8MB
  ttl_days: 7

storage:
  type: local
  path: /data/arkilian/storage
```

### Service Configuration

| Environment Variable     | Default             | Description              |
| ------------------------ | ------------------- | ------------------------ |
| `ARKILIAN_STORAGE_PATH`  | `/data/arkilian`    | Local storage path       |
| `ARKILIAN_MANIFEST_PATH` | `/data/manifest.db` | Manifest database path   |
| `ARKILIAN_S3_BUCKET`     | -                   | S3 bucket for partitions |
| `ARKILIAN_S3_REGION`     | `us-east-1`         | AWS region               |
| `ARKILIAN_HTTP_PORT`     | `8080`              | HTTP server port         |
| `ARKILIAN_GRPC_PORT`     | `9090`              | gRPC server port         |

| Environment Variable              | Default                | Description                     |
| --------------------------------- | ---------------------- | ------------------------------- |
| `ARKILIAN_WAL_DIR`                | `./data/arkilian/wal`  | WAL segment directory           |
| `ARKILIAN_WAL_FLUSH_INTERVAL`     | `500ms`                | How often to flush WAL to S3    |
| `ARKILIAN_WAL_MAX_SEGMENT_SIZE`   | `67108864`             | Max WAL segment size (bytes)    |
| `ARKILIAN_INDEX_BUCKET_COUNT`     | `64`                   | Hash buckets per index column   |
| `ARKILIAN_INDEX_CREATE_THRESHOLD` | `100`                  | Queries/hour to trigger index   |
| `ARKILIAN_INDEX_DROP_THRESHOLD`   | `5`                    | Queries/hour to drop index      |
| `ARKILIAN_CACHE_NVME_DIR`         | `./data/arkilian/nvme` | NVMe cache directory            |
| `ARKILIAN_CACHE_NVME_MAX_BYTES`   | `536870912000`         | Maximum NVMe cache size (500GB) |
| `ARKILIAN_CACHE_PREFETCH_ENABLED` | `true`                 | Enable predictive prefetch      |
| `ARKILIAN_ROUTER_BUFFER_SIZE`     | `1000`                 | Subscriber channel buffer size  |

**Example config:**

```yaml
wal:
  dir: /data/arkilian/wal
  flush_interval: 500ms
  max_segment_size: 67108864 # 64MB

index:
  bucket_count: 64
  create_threshold: 100
  drop_threshold: 5

cache:
  nvme_dir: /data/arkilian/nvme
  nvme_max_bytes: 536870912000 # 500GB
  prefetch_enabled: true

router:
  buffer_size: 1000
```

## Limitations

- No distributed ACID transactions across partitions
- Eventual consistency (not strong consistency)

## Documentation

- [Usage Guide](docs/usage-guide.md) - Complete usage documentation with real-world scenarios
- [Blueprint](blue-print.md) - Original RFC and architecture design

## License

MIT

## Acknowledgments

Inspired by:

- [Snowflake](https://dl.acm.org/doi/10.1145/2882903.2903741) - Micro-partition design
- [DuckDB](https://duckdb.org) - Embeddable analytical database
- [Litestream](https://litestream.io) - WAL replication
- [Apache Iceberg](https://iceberg.apache.org) - Metadata layer for immutable files

## Deployment

### Docker

Arkilian includes a multi-stage `Dockerfile` that builds minimal, secure images for all services.

\`\`\`bash

# Build the image

docker build -t arkilian:latest .

# Run Ingest Service (Default)

docker run -p 8080:8080 arkilian:latest

# Run Query Service

docker run -p 8081:8081 --entrypoint /usr/local/bin/arkilian-query arkilian:latest

# Run Compaction Service

docker run --entrypoint /usr/local/bin/arkilian-compact arkilian:latest
\`\`\`

### CI/CD

The project uses GitHub Actions for Continuous Integration. The workflow at \`.github/workflows/ci.yaml\` automatically:

1.  Verifies dependencies.
2.  Runs static analysis (\`go vet\`).
3.  Executes the full test suite with race detection.
4.  Verifies the Docker build.
