# Arkilian Usage Guide

Complete documentation for building real-world applications with Arkilian.

## Table of Contents

- [Getting Started](#getting-started)
- [Data Ingestion](#data-ingestion)
- [Querying Data](#querying-data)
- [Partition Strategies](#partition-strategies)
- [Schema Management](#schema-management)
- [Real-World Scenarios](#real-world-scenarios)
- [Performance Tuning](#performance-tuning)
- [Monitoring & Observability](#monitoring--observability)
- [Troubleshooting](#troubleshooting)

---

## Getting Started

### Installation

```bash
# Clone the repository
git clone https://github.com/arkilian/arkilian.git
cd arkilian

# Build all binaries
go build ./...

# Verify installation
./arkilian --version
```

### Minimal Setup (Local Development)

The simplest way to run Arkilian is with the unified binary:

```bash
# Run all services with a single command
./arkilian --data-dir /tmp/arkilian

# This starts:
# - Ingest service on :8080
# - Query service on :8081
# - Compaction service on :8082
# - gRPC service on :9090
```

### Running Specific Services

You can run individual services using the `--mode` flag:

```bash
# Run only the ingest service
./arkilian --mode ingest --data-dir /tmp/arkilian

# Run only the query service
./arkilian --mode query --data-dir /tmp/arkilian

# Run only the compaction service
./arkilian --mode compact --data-dir /tmp/arkilian
```

### Using a Configuration File

Create a `config.yaml` file:

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
  ttl_days: 7

storage:
  type: local
```

Then run:

```bash
./arkilian --config config.yaml
```
### Production Setup (AWS S3)

Using the unified binary with S3:

```bash
# Set environment variables
export ARKILIAN_STORAGE_TYPE=s3
export ARKILIAN_S3_BUCKET=my-arkilian-bucket
export ARKILIAN_S3_REGION=us-east-1
export AWS_ACCESS_KEY_ID=your-access-key
export AWS_SECRET_ACCESS_KEY=your-secret-key

# Start with S3 backend
./arkilian --data-dir /data/arkilian
```

Or using a config file:

```yaml
# config-prod.yaml
mode: all
data_dir: /data/arkilian

storage:
  type: s3
  s3:
    bucket: my-arkilian-bucket
    region: us-east-1
```

```bash
./arkilian --config config-prod.yaml
```

---

## Data Ingestion

### Basic Ingestion

Insert a single batch of events:

```bash
curl -X POST http://localhost:8080/v1/ingest \
  -H "Content-Type: application/json" \
  -d '{
    "partition_key": "20260206",
    "rows": [
      {
        "tenant_id": "acme-corp",
        "user_id": 12345,
        "event_time": 1707235200,
        "event_type": "page_view",
        "payload": {"url": "/dashboard", "duration_ms": 1500}
      },
      {
        "tenant_id": "acme-corp",
        "user_id": 12345,
        "event_time": 1707235260,
        "event_type": "button_click",
        "payload": {"button_id": "submit-form", "page": "/dashboard"}
      }
    ]
  }'
```

### Batch Ingestion with Idempotency

For reliable ingestion with retry support:

```bash
curl -X POST http://localhost:8080/v1/ingest \
  -H "Content-Type: application/json" \
  -d '{
    "partition_key": "20260206",
    "idempotency_key": "batch-2026020601-001",
    "rows": [...]
  }'
```

If the same `idempotency_key` is sent again, Arkilian returns the original response without re-processing.

### High-Throughput Ingestion (gRPC)

For maximum throughput, use the gRPC API:

```go
package main

import (
    "context"
    pb "github.com/arkilian/arkilian/api/proto"
    "google.golang.org/grpc"
)

func main() {
    conn, _ := grpc.Dial("localhost:9090", grpc.WithInsecure())
    client := pb.NewIngestServiceClient(conn)

    resp, _ := client.BatchIngest(context.Background(), &pb.IngestRequest{
        PartitionKey: "20260206",
        Rows: []*pb.Row{
            {TenantId: "acme", UserId: 12345, EventTime: 1707235200, ...},
        },
    })
    fmt.Printf("Ingested %d rows\n", resp.RowCount)
}
```

### Basic Queries

```bash
# Simple SELECT
curl -X POST http://localhost:8081/v1/query \
  -d '{"sql": "SELECT * FROM events LIMIT 10"}'

# Filter by tenant
curl -X POST http://localhost:8081/v1/query \
  -d '{"sql": "SELECT * FROM events WHERE tenant_id = '\''acme-corp'\'' LIMIT 100"}'

# Time range query
curl -X POST http://localhost:8081/v1/query \
  -d '{"sql": "SELECT * FROM events WHERE event_time BETWEEN 1707235200 AND 1707321600"}'
```

### Aggregations

Arkilian supports `COUNT`, `SUM`, `AVG`, `MIN`, and `MAX` aggregates. Simple
aggregates without `GROUP BY` are computed correctly across all partitions.

```sql
-- Total events for a tenant
SELECT COUNT(*) as total
FROM events
WHERE tenant_id = 'acme-corp';

-- Event type distribution
SELECT event_type, COUNT(*) as count
FROM events
WHERE tenant_id = 'acme-corp'
GROUP BY event_type;

-- Count events per user
SELECT user_id, COUNT(*) as event_count
FROM events
WHERE tenant_id = 'acme-corp'
GROUP BY user_id
ORDER BY event_count DESC
LIMIT 10;
```

> **Important — GROUP BY correctness:** `GROUP BY` queries are executed
> independently on each partition and the per-partition results are concatenated.
> If the same group key appears in multiple partitions, you will see duplicate
> groups in the output. To get correct grouped results, either (a) ensure your
> partition strategy places all rows for a given group key in the same partition,
> or (b) re-aggregate the results in your application.
>
> `COUNT(DISTINCT ...)` is parsed but not correctly deduplicated across
> partitions — each partition counts its own distinct values independently.
> Use application-side deduplication for accurate distinct counts.

### Complex Queries

```sql
-- Session analysis: users with many events in a time window
SELECT
  user_id,
  MIN(event_time) as session_start,
  MAX(event_time) as session_end,
  COUNT(*) as events_in_session
FROM events
WHERE tenant_id = 'acme-corp'
  AND event_time BETWEEN 1707235200 AND 1707321600
GROUP BY user_id
HAVING COUNT(*) > 5;

-- Multi-predicate filtering with bloom + min/max pruning
SELECT user_id, event_type, event_time
FROM events
WHERE tenant_id = 'acme-corp'
  AND user_id = 12345
  AND event_time >= 1707235200
  AND event_time < 1707321600
ORDER BY event_time DESC
LIMIT 50;
```

> **Note:** Arkilian's query parser supports single-table queries only. JOINs, CTEs
> (`WITH ... AS`), subqueries, `CASE WHEN`, and `CAST` are not supported. For
> cross-event analysis (e.g., funnel queries), perform separate queries and join
> results in your application layer.

### Supported SQL Reference

Arkilian uses a handwritten recursive descent parser. The following SQL features
are supported:

```
SELECT [DISTINCT] columns
FROM table [AS alias]
[WHERE conditions]
[GROUP BY expressions]
[HAVING conditions]
[ORDER BY expressions [ASC|DESC]]
[LIMIT n]
[OFFSET n]
```

Supported in expressions:
- Comparison: `=`, `<>`, `!=`, `<`, `>`, `<=`, `>=`
- Logical: `AND`, `OR`, `NOT`
- Predicates: `IN (...)`, `BETWEEN ... AND`, `LIKE`, `IS [NOT] NULL`
- Arithmetic: `+`, `-`, `*`, `/`
- Aggregates: `COUNT(*)`, `COUNT(col)`, `SUM`, `AVG`, `MIN`, `MAX`
- Functions: `json_extract()`, `strftime()`, and other SQLite functions work
  in WHERE clauses (they are passed through to SQLite per-partition)

Not supported:
- `JOIN` (any kind), `UNION`, `INTERSECT`, `EXCEPT`
- `WITH ... AS` (CTEs), subqueries
- `CASE WHEN ... THEN ... ELSE ... END`
- `CAST(... AS ...)` — fails because `AS` is a reserved keyword inside expressions
- Window functions (`OVER`, `PARTITION BY`)
- `GROUP_CONCAT` — not a recognized aggregate;

Cross-partition caveats:
- `GROUP BY` results are concatenated from each partition, not merged. If the
  same group key exists in multiple partitions, you get duplicate groups.
  Re-aggregate in your application or use partition strategies that co-locate
  group keys.
- `COUNT(DISTINCT ...)` is parsed but each partition counts independently.
  Cross-partition distinct counts will be overcounted.
- Simple aggregates without `GROUP BY` (e.g., `SELECT COUNT(*) FROM events
  WHERE ...`) are correctly computed across all partitions.
- `ORDER BY` and `LIMIT`/`OFFSET` are correctly applied across partitions.

### Query Response Format

```json
{
  "columns": ["user_id", "event_count"],
  "rows": [
    [12345, 142],
    [67890, 98],
    [11111, 76]
  ],
  "stats": {
    "partitions_scanned": 5,
    "partitions_pruned": 115,
    "execution_time_ms": 234
  },
  "request_id": "req-abc123"
}
```

### Query Optimization Tips

1. **Always filter by indexed columns first**: `tenant_id`, `user_id`, `event_time`
2. **Use time ranges**: Arkilian prunes partitions by time automatically
3. **Leverage bloom filters**: Equality predicates on `tenant_id` and `user_id` trigger bloom filter pruning
4. **Use LIMIT**: Early termination saves resources

```sql
-- Good: Uses all pruning mechanisms
SELECT * FROM events
WHERE tenant_id = 'acme'           -- Bloom filter pruning
  AND event_time >= 1707235200     -- Min/max pruning
  AND event_time < 1707321600
LIMIT 100;

-- Bad: Full scan required
SELECT * FROM events
WHERE payload LIKE '%error%';
```

---

## Partition Strategies

### Time-Based Partitioning (Default)

Best for: Event logs, metrics, time-series data

```bash
# Partition by day
curl -X POST http://localhost:8080/v1/ingest \
  -d '{"partition_key": "20260206", "rows": [...]}'

# Partition by hour (high-volume)
curl -X POST http://localhost:8080/v1/ingest \
  -d '{"partition_key": "2026020614", "rows": [...]}'
```

### Tenant-Based Partitioning

Best for: Multi-tenant SaaS, data isolation requirements

```bash
# Each tenant gets its own partitions
curl -X POST http://localhost:8080/v1/ingest \
  -d '{"partition_key": "tenant:acme-corp", "rows": [...]}'

curl -X POST http://localhost:8080/v1/ingest \
  -d '{"partition_key": "tenant:beta-inc", "rows": [...]}'
```

### Hybrid Partitioning

Best for: Large tenants with time-based queries

```bash
# Combine tenant + date
curl -X POST http://localhost:8080/v1/ingest \
  -d '{"partition_key": "acme-corp:20260206", "rows": [...]}'
```

### Hash-Based Partitioning

Best for: High-cardinality data, even distribution

```go
// Client-side routing
func getPartitionKey(userID int64, numBuckets int) string {
    bucket := userID % int64(numBuckets)
    return fmt.Sprintf("user_bucket_%d", bucket)
}

// Route user 12345 to bucket
key := getPartitionKey(12345, 1024)  // "user_bucket_57"
```

---

## Schema Management

### Default Schema

Arkilian uses a default events schema:

```sql
CREATE TABLE events (
  event_id BLOB PRIMARY KEY,      -- ULID (auto-generated if not provided)
  tenant_id TEXT NOT NULL,
  user_id INTEGER NOT NULL,
  event_time INTEGER NOT NULL,    -- Unix timestamp
  event_type TEXT NOT NULL,
  payload BLOB NOT NULL           -- Snappy-compressed JSON
) WITHOUT ROWID;
```

### Schema Evolution

Add new nullable columns without rewriting existing partitions:

```bash
# Register new schema version
curl -X POST http://localhost:8080/v1/schema \
  -d '{
    "version": 2,
    "columns": [
      {"name": "event_id", "type": "BLOB", "primary_key": true},
      {"name": "tenant_id", "type": "TEXT", "nullable": false},
      {"name": "user_id", "type": "INTEGER", "nullable": false},
      {"name": "event_time", "type": "INTEGER", "nullable": false},
      {"name": "event_type", "type": "TEXT", "nullable": false},
      {"name": "payload", "type": "BLOB", "nullable": false},
      {"name": "session_id", "type": "TEXT", "nullable": true}
    ]
  }'
```

Queries automatically handle schema differences:

- New columns return `NULL` for old partitions
- Old columns remain accessible in new partitions

---

## Real-World Scenarios

### Scenario 1: E-Commerce Analytics Platform

Track user behavior across an online store.

**Data Model:**

```json
{
  "tenant_id": "shop-123",
  "user_id": 98765,
  "event_time": 1707235200,
  "event_type": "product_view",
  "payload": {
    "product_id": "SKU-001",
    "category": "electronics",
    "price": 299.99,
    "source": "search"
  }
}
```

**Ingestion Pattern:**

```python
# Partition by shop + date for multi-tenant isolation
partition_key = f"{shop_id}:{date.today().strftime('%Y%m%d')}"

events = [
    {"tenant_id": shop_id, "user_id": user_id, "event_type": "product_view", ...},
    {"tenant_id": shop_id, "user_id": user_id, "event_type": "add_to_cart", ...},
    {"tenant_id": shop_id, "user_id": user_id, "event_type": "purchase", ...},
]

requests.post(f"{ARKILIAN_URL}/v1/ingest", json={
    "partition_key": partition_key,
    "rows": events
})
```

**Analytics Queries:**

```sql
-- Count product views for a tenant
SELECT COUNT(*) as view_count
FROM events
WHERE tenant_id = 'shop-123'
  AND event_type = 'product_view'
  AND event_time >= 1707235200
  AND event_time < 1707321600;

-- Count purchases for a tenant
SELECT COUNT(*) as purchase_count
FROM events
WHERE tenant_id = 'shop-123'
  AND event_type = 'purchase'
  AND event_time >= 1707235200
  AND event_time < 1707321600;

-- Event breakdown by type
SELECT event_type, COUNT(*) as count
FROM events
WHERE tenant_id = 'shop-123'
  AND event_time >= 1707235200
  AND event_time < 1707321600
GROUP BY event_type
ORDER BY count DESC;
```

> **Tip:** For conversion funnel analysis (view → cart → purchase), run separate
> queries per event type and combine results in your application. Arkilian does
> not support `CASE WHEN`, `GROUP_CONCAT`, or JOINs.

### Scenario 2: IoT Sensor Data Platform

Collect and analyze data from thousands of IoT devices.

**Data Model:**

```json
{
  "tenant_id": "factory-a",
  "user_id": 1001,
  "event_time": 1707235200,
  "event_type": "sensor_reading",
  "payload": {
    "device_id": "sensor-temp-001",
    "metric": "temperature",
    "value": 72.5,
    "unit": "fahrenheit",
    "location": "building-1-floor-2"
  }
}
```

**Ingestion Pattern:**

```python
# High-frequency ingestion with batching
class SensorBatcher:
    def __init__(self):
        self.buffer = []
        self.batch_size = 50000

    def record(self, device_id, metric, value):
        self.buffer.append({
            "tenant_id": FACTORY_ID,
            "user_id": hash(device_id) % 1000000,  # Device ID as user_id
            "event_time": int(time.time()),
            "event_type": "sensor_reading",
            "payload": {"device_id": device_id, "metric": metric, "value": value}
        })

        if len(self.buffer) >= self.batch_size:
            self.flush()

    def flush(self):
        partition_key = datetime.now().strftime("%Y%m%d%H")  # Hourly partitions
        requests.post(f"{ARKILIAN_URL}/v1/ingest", json={
            "partition_key": partition_key,
            "rows": self.buffer
        })
        self.buffer = []
```

**Analytics Queries:**

```sql
-- Sensor readings for a specific device (last 24 hours)
SELECT event_time, payload
FROM events
WHERE tenant_id = 'factory-a'
  AND event_type = 'sensor_reading'
  AND event_time >= 1707148800
ORDER BY event_time DESC
LIMIT 1000;

-- Anomaly detection: all sensor readings in a time window
-- (filter by value thresholds in your application after retrieval)
SELECT user_id, event_time, payload
FROM events
WHERE tenant_id = 'factory-a'
  AND event_type = 'sensor_reading'
  AND event_time >= 1707148800
ORDER BY event_time DESC
LIMIT 100;

-- Count readings per device (using user_id as device identifier)
SELECT user_id, COUNT(*) as reading_count, MAX(event_time) as last_seen
FROM events
WHERE tenant_id = 'factory-a'
  AND event_time >= 1707231600
GROUP BY user_id
ORDER BY reading_count;
```

> **Note:** `json_extract()` works in WHERE clauses for filtering (it parses as a
> function call and is executed by SQLite within each partition). However, using
> it in GROUP BY or SELECT for aggregation faces the same cross-partition merging
> limitation as any GROUP BY query — results from each partition are concatenated,
> not merged. Retrieve raw rows and aggregate in your application for accurate results.

### Scenario 3: SaaS Application Event Tracking

Track user actions across a multi-tenant SaaS platform.

**Data Model:**

```json
{
  "tenant_id": "company-abc",
  "user_id": 5001,
  "event_time": 1707235200,
  "event_type": "feature_used",
  "payload": {
    "feature": "report_builder",
    "action": "create_report",
    "plan": "enterprise",
    "duration_ms": 2500
  }
}
```

**Ingestion Pattern:**

```javascript
// Node.js SDK example
const Arkilian = require("arkilian-client");

const client = new Arkilian({
  endpoint: "http://arkilian.internal:8080",
  batchSize: 5000,
  flushInterval: 10000,
});

// Track events from your application
app.post("/api/reports", async (req, res) => {
  const report = await createReport(req.body);

  // Track the event
  client.track({
    tenantId: req.user.tenantId,
    userId: req.user.id,
    eventType: "feature_used",
    payload: {
      feature: "report_builder",
      action: "create_report",
      reportId: report.id,
    },
  });

  res.json(report);
});
```

**Analytics Queries:**

```sql
-- Events for a specific feature
SELECT tenant_id, user_id, event_time, payload
FROM events
WHERE event_type = 'feature_used'
  AND event_time >= 1707235200
ORDER BY event_time DESC
LIMIT 100;

-- Total feature usage per tenant
SELECT tenant_id, COUNT(*) as total_uses
FROM events
WHERE event_type = 'feature_used'
  AND event_time >= 1707235200
GROUP BY tenant_id
ORDER BY total_uses DESC;

-- User activity for a specific tenant
SELECT user_id, COUNT(*) as total_events
FROM events
WHERE tenant_id = 'company-abc'
  AND event_time >= 1704643200
GROUP BY user_id
ORDER BY total_events DESC
LIMIT 100;
```

> **Note:** CTEs (`WITH ... AS`) and `CASE WHEN` are not supported by the parser.
> `json_extract()` in SELECT columns and `COUNT(DISTINCT ...)` parse correctly
> but face cross-partition merging limitations. For feature adoption analysis
> or churn detection, retrieve raw events and compute metrics in your application.

### Scenario 4: Gaming Analytics

Track player behavior and game events.

**Data Model:**

```json
{
  "tenant_id": "game-rpg-online",
  "user_id": 777888,
  "event_time": 1707235200,
  "event_type": "player_action",
  "payload": {
    "action": "level_up",
    "character_id": "char-001",
    "level": 25,
    "class": "warrior",
    "playtime_minutes": 1250,
    "server": "us-west-2"
  }
}
```

**Analytics Queries:**

```sql
-- Recent player actions
SELECT user_id, event_time, payload
FROM events
WHERE tenant_id = 'game-rpg-online'
  AND event_type = 'player_action'
  AND event_time >= 1707148800
ORDER BY event_time DESC
LIMIT 100;

-- Level-up events for analysis
SELECT user_id, event_time, payload
FROM events
WHERE tenant_id = 'game-rpg-online'
  AND event_type = 'player_action'
  AND event_time >= 1706630400
ORDER BY event_time
LIMIT 10000;

-- Character creation events
SELECT user_id, event_time, payload
FROM events
WHERE tenant_id = 'game-rpg-online'
  AND event_type = 'player_action'
  AND event_time >= 1706630400
ORDER BY event_time DESC
LIMIT 5000;

-- Session end events for duration analysis
SELECT user_id, event_time, payload
FROM events
WHERE tenant_id = 'game-rpg-online'
  AND event_type = 'session_end'
  AND event_time >= 1706630400
LIMIT 10000;
```

> **Note:** `CASE WHEN` and `CAST(... AS ...)` are not supported by the parser
> and will cause parse errors. `json_extract()` in GROUP BY and `COUNT(DISTINCT ...)`
> parse correctly but face cross-partition merging limitations (duplicate groups,
> overcounted distinct values). Retrieve raw events and compute player progression
> funnels, class distributions, and session bucketing in your application.

### Scenario 5: Financial Transaction Monitoring

Track and analyze financial transactions for fraud detection.

**Data Model:**

```json
{
  "tenant_id": "bank-xyz",
  "user_id": 100200300,
  "event_time": 1707235200,
  "event_type": "transaction",
  "payload": {
    "transaction_id": "txn-abc123",
    "type": "purchase",
    "amount": 150.0,
    "currency": "USD",
    "merchant": "Amazon",
    "category": "retail",
    "location": { "country": "US", "city": "Seattle" },
    "device_fingerprint": "fp-xyz789"
  }
}
```

**Analytics Queries:**

```sql
-- Recent transactions for a user
SELECT user_id, event_time, payload
FROM events
WHERE tenant_id = 'bank-xyz'
  AND event_type = 'transaction'
  AND user_id = 100200300
  AND event_time >= 1707148800
ORDER BY event_time DESC
LIMIT 100;

-- Suspicious activity: users with many transactions in the last hour
SELECT user_id, COUNT(*) as txn_count, MIN(event_time) as first_txn, MAX(event_time) as last_txn
FROM events
WHERE tenant_id = 'bank-xyz'
  AND event_type = 'transaction'
  AND event_time >= 1707231600
GROUP BY user_id
HAVING COUNT(*) > 10;

-- All transactions in a time window for analysis
SELECT user_id, event_time, payload
FROM events
WHERE tenant_id = 'bank-xyz'
  AND event_type = 'transaction'
  AND event_time >= 1707148800
  AND event_time < 1707235200
ORDER BY event_time
LIMIT 10000;
```

> **Note:** The original queries for this scenario used `GROUP_CONCAT` (not
> supported by the parser) and `GROUP BY` on `json_extract()` expressions
> (which faces the cross-partition merging limitation — duplicate groups from
> different partitions are not merged). Retrieve raw transaction events and
> compute aggregations in your application for accurate results.

### Scenario 6: Log Analytics Platform

Centralized logging for distributed systems.

**Data Model:**

```json
{
  "tenant_id": "service-api-gateway",
  "user_id": 0,
  "event_time": 1707235200,
  "event_type": "log",
  "payload": {
    "level": "ERROR",
    "message": "Connection timeout to database",
    "service": "user-service",
    "instance": "i-abc123",
    "trace_id": "trace-xyz789",
    "stack_trace": "...",
    "tags": ["database", "timeout", "critical"]
  }
}
```

**Ingestion Pattern:**

```python
# Structured logging integration
import logging
import requests
from datetime import datetime

class ArkilianHandler(logging.Handler):
    def __init__(self, endpoint, service_name, batch_size=1000):
        super().__init__()
        self.endpoint = endpoint
        self.service_name = service_name
        self.batch_size = batch_size
        self.buffer = []

    def emit(self, record):
        self.buffer.append({
            "tenant_id": self.service_name,
            "user_id": 0,
            "event_time": int(record.created),
            "event_type": "log",
            "payload": {
                "level": record.levelname,
                "message": record.getMessage(),
                "logger": record.name,
                "filename": record.filename,
                "lineno": record.lineno,
                "trace_id": getattr(record, 'trace_id', None)
            }
        })

        if len(self.buffer) >= self.batch_size:
            self.flush()

    def flush(self):
        if self.buffer:
            partition_key = datetime.now().strftime("%Y%m%d%H")
            requests.post(f"{self.endpoint}/v1/ingest", json={
                "partition_key": f"logs:{partition_key}",
                "rows": self.buffer
            })
            self.buffer = []
```

**Analytics Queries:**

```sql
-- Error logs for a service (last hour)
SELECT event_time, payload
FROM events
WHERE tenant_id = 'service-api-gateway'
  AND event_type = 'log'
  AND event_time >= 1707231600
ORDER BY event_time DESC
LIMIT 500;

-- Total log count per service
SELECT tenant_id, COUNT(*) as total
FROM events
WHERE event_type = 'log'
  AND event_time >= 1707231600
GROUP BY tenant_id
ORDER BY total DESC;

-- Trace analysis: all logs for a specific trace
SELECT event_time, tenant_id, payload
FROM events
WHERE event_type = 'log'
ORDER BY event_time
LIMIT 1000;
```

> **Note:** The original queries for this scenario used `CASE WHEN` expressions
> (not supported by the parser) and `json_extract()` in GROUP BY (which faces
> the cross-partition merging limitation). Functions like `ROUND()` and
> `strftime()` parse correctly and work per-partition, but `CASE WHEN` blocks
> the entire query from parsing. Retrieve raw log events and compute error rates
> and log level breakdowns in your application.

---

## Performance Tuning

### Partition Sizing

| Partition Size | Use Case                             | Trade-offs                                |
| -------------- | ------------------------------------ | ----------------------------------------- |
| 4-8 MB         | High-cardinality, many small queries | More partitions, higher metadata overhead |
| 8-16 MB        | General purpose (recommended)        | Balanced performance                      |
| 16-32 MB       | Large scans, fewer queries           | Fewer partitions, longer query times      |

### Bloom Filter Tuning

Default configuration targets 1% false positive rate. Adjust for your workload:

```bash
# Lower FPR (0.1%) - more memory, better pruning
./arkilian-ingest --bloom-fpr 0.001

# Higher FPR (5%) - less memory, acceptable for low-cardinality columns
./arkilian-ingest --bloom-fpr 0.05
```

### Connection Pooling

```bash
# Increase connection pool for high-concurrency workloads
./arkilian-query --connection-pool-size 200

# Reduce for memory-constrained environments
./arkilian-query --connection-pool-size 50
```

### Query Parallelism

```bash
# Increase parallel partition scans
./arkilian-query --max-parallel-partitions 20

# Limit for resource-constrained environments
./arkilian-query --max-parallel-partitions 5
```

---

## Monitoring & Observability

### Prometheus Metrics

Arkilian exposes metrics at `/metrics`:

```
# Ingestion metrics
arkilian_ingest_partitions_created_total
arkilian_ingest_rows_total
arkilian_ingest_bytes_total
arkilian_ingest_latency_seconds

# Query metrics
arkilian_query_requests_total
arkilian_query_latency_seconds
arkilian_query_partitions_scanned_total
arkilian_query_partitions_pruned_total

# Compaction metrics
arkilian_compact_operations_total
arkilian_compact_partitions_merged_total
arkilian_compact_bytes_reclaimed_total
```

### Grafana Dashboard

Example queries for monitoring:

```promql
# Ingestion rate (rows/sec)
rate(arkilian_ingest_rows_total[5m])

# Query latency P95
histogram_quantile(0.95, rate(arkilian_query_latency_seconds_bucket[5m]))

# Pruning effectiveness
1 - (rate(arkilian_query_partitions_scanned_total[5m]) /
     (rate(arkilian_query_partitions_scanned_total[5m]) +
      rate(arkilian_query_partitions_pruned_total[5m])))

# Error rate
rate(arkilian_query_errors_total[5m]) / rate(arkilian_query_requests_total[5m])
```

### Health Checks

```bash
# Liveness probe
curl http://localhost:8080/health/live

# Readiness probe (checks manifest DB and storage connectivity)
curl http://localhost:8080/health/ready
```

### Structured Logging

All services emit JSON logs with correlation IDs:

```json
{
  "timestamp": "2026-02-06T14:30:00Z",
  "level": "INFO",
  "message": "Query executed",
  "request_id": "req-abc123",
  "partitions_scanned": 5,
  "partitions_pruned": 115,
  "execution_time_ms": 234
}
```

---

## Troubleshooting

### Common Issues

**Issue: Slow queries**

```sql
-- Check partition pruning effectiveness
SELECT
  partitions_scanned,
  partitions_pruned,
  execution_time_ms
FROM arkilian_query_stats
WHERE request_id = 'req-abc123';
```

Solutions:

1. Add time range predicates to enable min/max pruning
2. Use equality predicates on `tenant_id` or `user_id` for bloom filter pruning
3. Check if partitions need compaction

**Issue: High memory usage on query nodes**

```bash
# Check bloom filter memory
curl http://localhost:8081/debug/bloom-stats
```

Solutions:

1. Reduce bloom filter FPR (trades memory for accuracy)
2. Enable tiered bloom filters (hot in RAM, cold on SSD)
3. Increase query node count

**Issue: Ingestion backpressure**

Solutions:

1. Increase batch size (reduce partition count)
2. Add more ingest nodes
3. Use gRPC instead of HTTP for higher throughput

**Issue: Partition skew**

```sql
-- Check partition size distribution
SELECT
  partition_key,
  COUNT(*) as partition_count,
  SUM(size_bytes) / 1024 / 1024 as total_mb,
  AVG(size_bytes) / 1024 / 1024 as avg_mb
FROM partitions
WHERE compacted_into IS NULL
GROUP BY partition_key
ORDER BY total_mb DESC;
```

Solutions:

1. Use finer-grained partition keys (hourly instead of daily)
2. Implement hash-based sub-partitioning for hot keys
3. Trigger manual compaction for oversized partitions

**Issue: S3 costs too high**

```bash
# Check S3 GET operations
aws cloudwatch get-metric-statistics \
  --namespace AWS/S3 \
  --metric-name GetRequests \
  --dimensions Name=BucketName,Value=arkilian-prod
```

Solutions:

1. Improve bloom filter accuracy (reduce false positives)
2. Enable partition caching on query nodes
3. Coalesce small partitions via compaction

### Debug Mode

Enable verbose logging:

```bash
./arkilian-query --log-level debug --log-format json
```

### Recovery Procedures

**Manifest corruption:**

```bash
# Restore from Litestream backup
litestream restore -o /data/manifest.db s3://arkilian-backups/manifest.db
```

**Orphaned partitions:**

```bash
# Run reconciliation
./arkilian-compact --reconcile --dry-run
./arkilian-compact --reconcile
```

**Failed compaction:**

```bash
# Check compaction status
curl http://localhost:8082/debug/compaction-status

# Retry failed compaction
curl -X POST http://localhost:8082/v1/compact/retry?partition_key=20260206
```

---

## Client Libraries

### Go

```go
import "github.com/arkilian/arkilian-go"

client := arkilian.NewClient("http://localhost:8080")
client.Ingest(ctx, "20260206", rows)
result, _ := client.Query(ctx, "SELECT * FROM events LIMIT 10")
```

### Python

```python
from arkilian import Client

client = Client("http://localhost:8080")
client.ingest("20260206", rows)
result = client.query("SELECT * FROM events LIMIT 10")
```

### JavaScript/TypeScript

```typescript
import { Arkilian } from "arkilian-js";

const client = new Arkilian("http://localhost:8080");
await client.ingest("20260206", rows);
const result = await client.query("SELECT * FROM events LIMIT 10");
```

---

## Next Steps

- [API Reference](./api-reference.md) - Complete API documentation
- [Architecture Deep Dive](./architecture.md) - Internal design details
- [Operations Guide](./operations.md) - Production deployment and maintenance
- [Migration Guide](./migration.md) - Migrating from other databases
````
