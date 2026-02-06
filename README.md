# Arkilian

A distributed analytical database that treats SQLite files as immutable 8-16MB micro-partitions stored in object storage (S3/GCS), with a stateless query federation layer providing Snowflake-like compute/storage separation.

## Why Arkilian?

Unlike monolithic SQLite distribution attempts (rqlite, LiteFS), Arkilian embraces SQLite's single-node nature and partitions at the dataset level—no VFS complexity, no WAL coordination hazards, no storage duplication.

**Target workloads:**

- Time-series analytics
- Multi-tenant SaaS event stores
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
└──────────────────────────────────────────────────────────────────┘
```

## Core Principles

1. **Immutability**: Once written, SQLite files never change. Compaction produces new files.
2. **Partition Autonomy**: Each SQLite file is self-contained with its own indexes.
3. **Metadata-Driven Pruning**: Bloom filters + min/max stats eliminate >99% of I/O.
4. **Write Isolation**: Writers operate on disjoint partition keys (no distributed locking).

## Quick Start

### Prerequisites

- Go 1.21+
- SQLite3

### Build

```bash
go build ./...
```

### Run Services

```bash
# Start the ingestion service
./arkilian-ingest --storage-path /data/arkilian --manifest-path /data/manifest.db

# Start the query service
./arkilian-query --storage-path /data/arkilian --manifest-path /data/manifest.db

# Start the compaction daemon
./arkilian-compact --storage-path /data/arkilian --manifest-path /data/manifest.db
```

## API

### Ingest Data

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

Response:

```json
{
  "partition_id": "events:20260206:abc123",
  "row_count": 1,
  "size_bytes": 4096,
  "request_id": "req-xyz789"
}
```

### Query Data

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

| Metric                  | Target                 |
| ----------------------- | ---------------------- |
| Ingest throughput       | 50K rows/sec/node      |
| P95 query latency       | <500ms (point queries) |
| Partition pruning ratio | 99.5%                  |
| Storage overhead        | <5%                    |
| Recovery time           | <10s                   |

## Project Structure

```
arkilian/
├── cmd/
│   ├── arkilian-ingest/     # Ingestion service
│   ├── arkilian-query/      # Query service
│   └── arkilian-compact/    # Compaction daemon
├── internal/
│   ├── bloom/               # Bloom filter implementation
│   ├── compaction/          # Compaction logic
│   ├── manifest/            # Manifest catalog
│   ├── partition/           # Partition builder
│   ├── query/               # Query parser, planner, executor
│   ├── storage/             # Object storage abstraction
│   └── api/                 # HTTP/gRPC handlers
├── pkg/types/               # Shared types (Row, Schema, ULID)
└── test/                    # Integration tests
```

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

| Environment Variable     | Default             | Description              |
| ------------------------ | ------------------- | ------------------------ |
| `ARKILIAN_STORAGE_PATH`  | `/data/arkilian`    | Local storage path       |
| `ARKILIAN_MANIFEST_PATH` | `/data/manifest.db` | Manifest database path   |
| `ARKILIAN_S3_BUCKET`     | -                   | S3 bucket for partitions |
| `ARKILIAN_S3_REGION`     | `us-east-1`         | AWS region               |
| `ARKILIAN_HTTP_PORT`     | `8080`              | HTTP server port         |
| `ARKILIAN_GRPC_PORT`     | `9090`              | gRPC server port         |

## Limitations

- No distributed ACID transactions across partitions
- Eventual consistency (not strong consistency)
- Schema must be declared per partition key
- Not suitable for real-time OLTP (<10ms latency requirements)

## When NOT to Use Arkilian

- You need distributed transactions → Use PostgreSQL or Spanner
- You need strong consistency → Use rqlite
- You have <100GB data in single region → Use LiteFS
- You need full ANSI SQL compliance → Use TimescaleDB

## License

MIT

## Acknowledgments

Inspired by:

- [Snowflake](https://dl.acm.org/doi/10.1145/2882903.2903741) - Micro-partition design
- [DuckDB](https://duckdb.org) - Embeddable analytical database
- [Litestream](https://litestream.io) - WAL replication
- [Apache Iceberg](https://iceberg.apache.org) - Metadata layer for immutable files
