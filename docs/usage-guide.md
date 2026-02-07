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
./arkilian-ingest --version
./arkilian-query --version
./arkilian-compact --version
```

### Minimal Setup (Local Development)

```bash
# Create data directories
mkdir -p /tmp/arkilian/storage /tmp/arkilian/manifests
```

# Start services (in separate terminals)

./arkilian-ingest --storage-path /tmp/arkilian/storage --manifest-path /tmp/arkilian/manifests/manifest.db
./arkilian-query --storage-path /tmp/arkilian/storage --manifest-path /tmp/arkilian/manifests/manifest.db
./arkilian-compact --storage-path /tmp/arkilian/storage --manifest-path /tmp/arkilian/manifests/manifest.db

````

### Production Setup (AWS S3)

```bash
# Set environment variables
export ARKILIAN_S3_BUCKET=my-arkilian-bucket
export ARKILIAN_S3_REGION=us-east-1
export AWS_ACCESS_KEY_ID=your-access-key
export AWS_SECRET_ACCESS_KEY=your-secret-key

# Start with S3 backend
./arkilian-ingest --storage-type s3 --s3-bucket $ARKILIAN_S3_BUCKET
````

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

### Streaming Ingestion Pattern

For continuous data streams, batch events client-side:

```python
import requests
import time
from collections import defaultdict

class ArkilianBatcher:
    def __init__(self, endpoint, batch_size=10000, flush_interval=5):
        self.endpoint = endpoint
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self.buffers = defaultdict(list)
        self.last_flush = time.time()

    def add(self, partition_key, event):
        self.buffers[partition_key].append(event)
        if len(self.buffers[partition_key]) >= self.batch_size:
            self.flush(partition_key)
        elif time.time() - self.last_flush > self.flush_interval:
            self.flush_all()

    def flush(self, partition_key):
        if not self.buffers[partition_key]:
            return
        requests.post(f"{self.endpoint}/v1/ingest", json={
            "partition_key": partition_key,
            "rows": self.buffers[partition_key]
        })
        self.buffers[partition_key] = []
        self.last_flush = time.time()

    def flush_all(self):
        for key in list(self.buffers.keys()):
            self.flush(key)
```

---

## Querying Data

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

```sql
-- Count events per user
SELECT user_id, COUNT(*) as event_count
FROM events
WHERE tenant_id = 'acme-corp'
GROUP BY user_id
ORDER BY event_count DESC
LIMIT 10;

-- Daily active users
SELECT
  (event_time / 86400) * 86400 as day,
  COUNT(DISTINCT user_id) as dau
FROM events
WHERE tenant_id = 'acme-corp'
  AND event_time >= 1707235200
GROUP BY day
ORDER BY day;

-- Event type distribution
SELECT event_type, COUNT(*) as count
FROM events
WHERE tenant_id = 'acme-corp'
GROUP BY event_type;
```

### Complex Queries

```sql
-- Funnel analysis: users who viewed then purchased
SELECT
  v.user_id,
  v.event_time as view_time,
  p.event_time as purchase_time
FROM events v
JOIN events p ON v.user_id = p.user_id
WHERE v.event_type = 'product_view'
  AND p.event_type = 'purchase'
  AND p.event_time > v.event_time
  AND p.event_time < v.event_time + 3600
LIMIT 100;

-- Session analysis
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
```

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
-- Conversion funnel
SELECT
  COUNT(DISTINCT CASE WHEN event_type = 'product_view' THEN user_id END) as viewers,
  COUNT(DISTINCT CASE WHEN event_type = 'add_to_cart' THEN user_id END) as cart_adds,
  COUNT(DISTINCT CASE WHEN event_type = 'purchase' THEN user_id END) as purchasers
FROM events
WHERE tenant_id = 'shop-123'
  AND event_time >= 1707235200
  AND event_time < 1707321600;

-- Top products by revenue
SELECT
  json_extract(payload, '$.product_id') as product_id,
  SUM(json_extract(payload, '$.price')) as revenue,
  COUNT(*) as orders
FROM events
WHERE tenant_id = 'shop-123'
  AND event_type = 'purchase'
GROUP BY product_id
ORDER BY revenue DESC
LIMIT 20;

-- User journey analysis
SELECT
  user_id,
  GROUP_CONCAT(event_type, ' -> ') as journey
FROM events
WHERE tenant_id = 'shop-123'
  AND user_id = 98765
ORDER BY event_time;
```

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
-- Average temperature by location (last 24 hours)
SELECT
  json_extract(payload, '$.location') as location,
  AVG(json_extract(payload, '$.value')) as avg_temp,
  MIN(json_extract(payload, '$.value')) as min_temp,
  MAX(json_extract(payload, '$.value')) as max_temp
FROM events
WHERE tenant_id = 'factory-a'
  AND event_type = 'sensor_reading'
  AND json_extract(payload, '$.metric') = 'temperature'
  AND event_time >= strftime('%s', 'now') - 86400
GROUP BY location;

-- Anomaly detection: readings outside normal range
SELECT
  json_extract(payload, '$.device_id') as device,
  json_extract(payload, '$.value') as value,
  event_time
FROM events
WHERE tenant_id = 'factory-a'
  AND event_type = 'sensor_reading'
  AND (json_extract(payload, '$.value') > 100 OR json_extract(payload, '$.value') < 32)
ORDER BY event_time DESC
LIMIT 100;

-- Device health: readings per device
SELECT
  json_extract(payload, '$.device_id') as device,
  COUNT(*) as reading_count,
  MAX(event_time) as last_seen
FROM events
WHERE tenant_id = 'factory-a'
  AND event_time >= strftime('%s', 'now') - 3600
GROUP BY device
HAVING reading_count < 60;  -- Devices with fewer than expected readings
```

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
-- Feature adoption by tenant
SELECT
  tenant_id,
  json_extract(payload, '$.feature') as feature,
  COUNT(DISTINCT user_id) as unique_users,
  COUNT(*) as total_uses
FROM events
WHERE event_type = 'feature_used'
  AND event_time >= 1707235200
GROUP BY tenant_id, feature
ORDER BY unique_users DESC;

-- User engagement score
SELECT
  tenant_id,
  user_id,
  COUNT(*) as total_events,
  COUNT(DISTINCT json_extract(payload, '$.feature')) as features_used,
  COUNT(DISTINCT event_time / 86400) as active_days
FROM events
WHERE event_time >= strftime('%s', 'now') - 2592000  -- Last 30 days
GROUP BY tenant_id, user_id
ORDER BY total_events DESC
LIMIT 100;

-- Churn risk: users with declining activity
WITH weekly_activity AS (
  SELECT
    tenant_id,
    user_id,
    (event_time / 604800) as week,
    COUNT(*) as events
  FROM events
  WHERE event_time >= strftime('%s', 'now') - 2592000
  GROUP BY tenant_id, user_id, week
)
SELECT
  tenant_id,
  user_id,
  MAX(CASE WHEN week = (strftime('%s', 'now') / 604800) - 3 THEN events END) as week_3,
  MAX(CASE WHEN week = (strftime('%s', 'now') / 604800) - 2 THEN events END) as week_2,
  MAX(CASE WHEN week = (strftime('%s', 'now') / 604800) - 1 THEN events END) as week_1,
  MAX(CASE WHEN week = (strftime('%s', 'now') / 604800) THEN events END) as this_week
FROM weekly_activity
GROUP BY tenant_id, user_id
HAVING this_week < week_1 AND week_1 < week_2;
```

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
-- Player progression funnel
SELECT
  json_extract(payload, '$.level') as level,
  COUNT(DISTINCT user_id) as players_reached
FROM events
WHERE tenant_id = 'game-rpg-online'
  AND event_type = 'player_action'
  AND json_extract(payload, '$.action') = 'level_up'
GROUP BY level
ORDER BY CAST(level AS INTEGER);

-- Most popular character classes
SELECT
  json_extract(payload, '$.class') as class,
  COUNT(DISTINCT user_id) as players
FROM events
WHERE tenant_id = 'game-rpg-online'
  AND event_type = 'player_action'
  AND json_extract(payload, '$.action') = 'character_create'
GROUP BY class
ORDER BY players DESC;

-- Daily active players by server
SELECT
  (event_time / 86400) * 86400 as day,
  json_extract(payload, '$.server') as server,
  COUNT(DISTINCT user_id) as dap
FROM events
WHERE tenant_id = 'game-rpg-online'
  AND event_time >= strftime('%s', 'now') - 604800
GROUP BY day, server
ORDER BY day, server;

-- Session length distribution
SELECT
  CASE
    WHEN json_extract(payload, '$.session_minutes') < 15 THEN '0-15 min'
    WHEN json_extract(payload, '$.session_minutes') < 30 THEN '15-30 min'
    WHEN json_extract(payload, '$.session_minutes') < 60 THEN '30-60 min'
    ELSE '60+ min'
  END as session_bucket,
  COUNT(*) as sessions
FROM events
WHERE tenant_id = 'game-rpg-online'
  AND event_type = 'session_end'
GROUP BY session_bucket;
```

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
-- Transaction volume by hour
SELECT
  (event_time / 3600) * 3600 as hour,
  COUNT(*) as transaction_count,
  SUM(json_extract(payload, '$.amount')) as total_volume
FROM events
WHERE tenant_id = 'bank-xyz'
  AND event_type = 'transaction'
  AND event_time >= strftime('%s', 'now') - 86400
GROUP BY hour
ORDER BY hour;

-- Suspicious activity: multiple transactions in short time
SELECT
  user_id,
  COUNT(*) as txn_count,
  SUM(json_extract(payload, '$.amount')) as total_amount,
  MIN(event_time) as first_txn,
  MAX(event_time) as last_txn
FROM events
WHERE tenant_id = 'bank-xyz'
  AND event_type = 'transaction'
  AND event_time >= strftime('%s', 'now') - 3600
GROUP BY user_id
HAVING txn_count > 10 OR total_amount > 10000;

-- Geographic anomalies: transactions from multiple countries
SELECT
  user_id,
  GROUP_CONCAT(DISTINCT json_extract(payload, '$.location.country')) as countries,
  COUNT(DISTINCT json_extract(payload, '$.location.country')) as country_count
FROM events
WHERE tenant_id = 'bank-xyz'
  AND event_type = 'transaction'
  AND event_time >= strftime('%s', 'now') - 86400
GROUP BY user_id
HAVING country_count > 2;

-- Merchant category analysis
SELECT
  json_extract(payload, '$.category') as category,
  COUNT(*) as transactions,
  SUM(json_extract(payload, '$.amount')) as volume,
  AVG(json_extract(payload, '$.amount')) as avg_amount
FROM events
WHERE tenant_id = 'bank-xyz'
  AND event_type = 'transaction'
GROUP BY category
ORDER BY volume DESC;
```

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
-- Error rate by service (last hour)
SELECT
  tenant_id as service,
  SUM(CASE WHEN json_extract(payload, '$.level') = 'ERROR' THEN 1 ELSE 0 END) as errors,
  COUNT(*) as total,
  ROUND(100.0 * SUM(CASE WHEN json_extract(payload, '$.level') = 'ERROR' THEN 1 ELSE 0 END) / COUNT(*), 2) as error_rate
FROM events
WHERE event_type = 'log'
  AND event_time >= strftime('%s', 'now') - 3600
GROUP BY service
ORDER BY error_rate DESC;

-- Most common errors
SELECT
  json_extract(payload, '$.message') as error_message,
  COUNT(*) as occurrences,
  MIN(event_time) as first_seen,
  MAX(event_time) as last_seen
FROM events
WHERE event_type = 'log'
  AND json_extract(payload, '$.level') = 'ERROR'
  AND event_time >= strftime('%s', 'now') - 86400
GROUP BY error_message
ORDER BY occurrences DESC
LIMIT 20;

-- Trace analysis
SELECT
  event_time,
  tenant_id as service,
  json_extract(payload, '$.level') as level,
  json_extract(payload, '$.message') as message
FROM events
WHERE event_type = 'log'
  AND json_extract(payload, '$.trace_id') = 'trace-xyz789'
ORDER BY event_time;
```

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
