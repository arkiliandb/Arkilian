# Requirements Document

## Introduction

Project Arkilian is a distributed analytical database that treats SQLite files as immutable 8-16MB micro-partitions stored in object storage (S3/GCS). The system provides Snowflake-like compute/storage separation with a stateless query federation layer. Target workloads include time-series analytics, multi-tenant SaaS event stores, and edge-to-cloud sync with strong eventual consistency.

The architecture embraces SQLite's single-node nature by partitioning at the dataset level, avoiding VFS complexity, WAL coordination hazards, and storage duplication. The implementation is in Go with no SQLite core modifications.

## Glossary

- **Micro_Partition**: An immutable SQLite file (8-16MB) containing a subset of data, stored in object storage
- **Manifest_Catalog**: A SQLite file (`manifest.db`) serving as the source of truth for partition metadata
- **Partition_Key**: A logical key (time-based, tenant-based, or hash-based) used to route data to specific partitions
- **Bloom_Filter**: A probabilistic data structure used for efficient membership testing with configurable false positive rate
- **Ingestion_Service**: The `arkilian-ingest` component responsible for accepting writes and creating partitions
- **Query_Federation_Layer**: The `arkilian-query` component responsible for parsing SQL, pruning partitions, and executing queries
- **Compaction_Daemon**: The `arkilian-compact` component responsible for merging small partitions and garbage collection
- **Metadata_Sidecar**: A JSON file (`.meta.json`) containing bloom filters and statistics for a partition
- **Partition_Pruning**: The process of eliminating irrelevant partitions before query execution using min/max stats and bloom filters
- **Object_Storage**: Cloud storage services (S3/GCS) where immutable partitions are stored
- **Schema_Version**: An integer tracking schema evolution for backward compatibility
- **Litestream_Pattern**: A backup strategy using WAL replication to object storage

## Requirements

### Requirement 1: Ingestion Service Core Functionality

**User Story:** As a data engineer, I want to ingest batches of rows into the system via HTTP/gRPC API, so that I can efficiently load analytical data into the distributed database.

#### Acceptance Criteria

1. WHEN a client sends a batch write request to the Ingestion_Service, THE Ingestion_Service SHALL validate the rows against the declared schema for the specified Partition_Key
2. WHEN the Ingestion_Service receives valid rows, THE Ingestion_Service SHALL generate a SQLite Micro_Partition file with optimized indexes
3. WHEN a Micro_Partition is generated, THE Ingestion_Service SHALL compute min/max statistics for indexed columns
4. WHEN a Micro_Partition is generated, THE Ingestion_Service SHALL build Bloom_Filters for high-cardinality columns (tenant_id, user_id)
5. WHEN a Micro_Partition is ready, THE Ingestion_Service SHALL upload the `.sqlite` and `.meta.json` files to Object_Storage using atomic multipart upload
6. WHEN the Object_Storage upload succeeds, THE Ingestion_Service SHALL update the Manifest_Catalog with partition metadata
7. THE Ingestion_Service SHALL achieve P99 latency of less than 100ms for batch write acknowledgment
8. THE Ingestion_Service SHALL support throughput of at least 50,000 rows per second per node

### Requirement 2: Micro-Partition Generation

**User Story:** As a system architect, I want partitions to be self-contained SQLite files with consistent structure, so that queries can execute independently on each partition.

#### Acceptance Criteria

1. THE Ingestion_Service SHALL generate Micro_Partitions with size between 8MB and 16MB
2. WHEN generating a Micro_Partition, THE Ingestion_Service SHALL create indexes optimized for common query patterns (tenant_id + event_time, user_id + event_time)
3. WHEN generating a Micro_Partition, THE Ingestion_Service SHALL use ULID (time-ordered UUID) as the primary key for events
4. WHEN generating a Micro_Partition, THE Ingestion_Service SHALL compress payload data using Snappy compression
5. THE Ingestion_Service SHALL create partitions using SQLite WITHOUT ROWID optimization for storage efficiency
6. WHEN a Micro_Partition is created, THE Ingestion_Service SHALL generate a corresponding Metadata_Sidecar file containing statistics and Bloom_Filters

### Requirement 3: Metadata Sidecar Generation

**User Story:** As a query optimizer, I want each partition to have associated metadata with bloom filters and statistics, so that I can efficiently prune partitions during query planning.

#### Acceptance Criteria

1. WHEN generating a Metadata_Sidecar, THE Ingestion_Service SHALL include partition_id, schema_version, and creation timestamp
2. WHEN generating a Metadata_Sidecar, THE Ingestion_Service SHALL include row_count, size_bytes, and min/max values for indexed columns
3. WHEN generating a Metadata_Sidecar, THE Ingestion_Service SHALL build Bloom_Filters using murmur3_128 hash algorithm
4. WHEN generating a Metadata_Sidecar, THE Ingestion_Service SHALL configure Bloom_Filters with a false positive rate of 1% or less
5. WHEN serializing a Metadata_Sidecar, THE Ingestion_Service SHALL encode Bloom_Filter data as base64
6. THE Metadata_Sidecar SHALL be serialized as JSON and stored alongside the partition file

### Requirement 4: Manifest Catalog Management

**User Story:** As a system operator, I want a centralized catalog tracking all partition metadata, so that the query layer can efficiently discover and prune partitions.

#### Acceptance Criteria

1. THE Manifest_Catalog SHALL store partition metadata including partition_id, partition_key, object_path, min/max statistics, row_count, size_bytes, and schema_version
2. THE Manifest_Catalog SHALL maintain indexes on min/max columns for efficient range-based pruning
3. WHEN a partition is compacted, THE Manifest_Catalog SHALL update the compacted_into field to reference the new partition
4. THE Manifest_Catalog SHALL exclude compacted partitions from active queries via filtered indexes
5. THE Manifest_Catalog SHALL be backed up to Object_Storage every 5 seconds using the Litestream_Pattern
6. THE Manifest_Catalog SHALL enforce single-writer semantics to avoid distributed consensus overhead
7. THE Manifest_Catalog SHALL support recovery from backup within 10 seconds

### Requirement 5: Query Federation Layer - SQL Parsing

**User Story:** As a data analyst, I want to execute SQL queries against the distributed database, so that I can analyze data across multiple partitions.

#### Acceptance Criteria

1. THE Query_Federation_Layer SHALL parse SQL queries using a recursive descent parser
2. WHEN parsing a query, THE Query_Federation_Layer SHALL extract predicates for partition pruning (equality, range, IN clauses)
3. WHEN parsing a query, THE Query_Federation_Layer SHALL identify columns referenced in WHERE, GROUP BY, and ORDER BY clauses
4. THE Query_Federation_Layer SHALL cache parsed ASTs for repeated queries
5. IF a query contains unsupported SQL syntax, THEN THE Query_Federation_Layer SHALL return a descriptive error message
6. THE Query_Federation_Layer SHALL support SELECT, WHERE, GROUP BY, ORDER BY, LIMIT, and aggregate functions (COUNT, SUM, AVG, MIN, MAX)

### Requirement 6: Query Federation Layer - Partition Pruning

**User Story:** As a query optimizer, I want to eliminate irrelevant partitions before execution, so that queries scan minimal data and achieve low latency.

#### Acceptance Criteria

1. WHEN executing a query, THE Query_Federation_Layer SHALL perform 2-phase partition pruning
2. WHEN performing phase 1 pruning, THE Query_Federation_Layer SHALL query the Manifest_Catalog using min/max statistics
3. WHEN performing phase 2 pruning, THE Query_Federation_Layer SHALL apply Bloom_Filters to eliminate false positives from phase 1
4. THE Query_Federation_Layer SHALL achieve a partition pruning ratio of at least 99.5% for point queries on indexed columns
5. THE Query_Federation_Layer SHALL load Bloom_Filters into memory on startup for hot partitions
6. WHEN Bloom_Filter memory exceeds threshold, THE Query_Federation_Layer SHALL use tiered storage (RAM for hot, SSD cache for cold)

### Requirement 7: Query Federation Layer - Parallel Execution

**User Story:** As a data analyst, I want queries to execute in parallel across partitions, so that I can achieve low latency on large datasets.

#### Acceptance Criteria

1. WHEN executing a query, THE Query_Federation_Layer SHALL dispatch subqueries to pruned partitions in parallel
2. THE Query_Federation_Layer SHALL maintain a connection pool of at least 100 connections per node
3. WHEN executing subqueries, THE Query_Federation_Layer SHALL download partition files from Object_Storage on demand
4. THE Query_Federation_Layer SHALL merge partial results using stream-oriented UNION ALL
5. WHEN a query includes LIMIT, THE Query_Federation_Layer SHALL support early termination
6. THE Query_Federation_Layer SHALL achieve P95 query latency of less than 500ms for point queries

### Requirement 8: Query Federation Layer - Result Aggregation

**User Story:** As a data analyst, I want aggregate queries to combine results from multiple partitions correctly, so that I can compute accurate statistics across the dataset.

#### Acceptance Criteria

1. WHEN executing aggregate queries, THE Query_Federation_Layer SHALL compute partial aggregates per partition
2. WHEN merging results, THE Query_Federation_Layer SHALL combine partial aggregates correctly (SUM of SUMs, COUNT of COUNTs, weighted AVG)
3. WHEN executing GROUP BY queries, THE Query_Federation_Layer SHALL merge groups across partitions
4. WHEN executing ORDER BY queries, THE Query_Federation_Layer SHALL perform merge sort on partial results
5. WHEN executing LIMIT queries, THE Query_Federation_Layer SHALL apply limit after final merge

### Requirement 9: Compaction Daemon Core Functionality

**User Story:** As a system operator, I want small partitions to be automatically merged, so that query performance remains optimal and storage is efficient.

#### Acceptance Criteria

1. WHEN a partition is smaller than 8MB, THE Compaction_Daemon SHALL mark it as a candidate for compaction
2. WHEN more than 100 partitions exist for the same Partition_Key per day, THE Compaction_Daemon SHALL trigger compaction
3. WHEN compacting partitions, THE Compaction_Daemon SHALL merge data sorted by primary key to minimize B-tree fragmentation
4. WHEN compaction completes, THE Compaction_Daemon SHALL upload the new partition to Object_Storage before updating the Manifest_Catalog
5. WHEN updating the Manifest_Catalog, THE Compaction_Daemon SHALL atomically set compacted_into for source partitions
6. THE Compaction_Daemon SHALL retain source partitions for 7 days before garbage collection

### Requirement 10: Compaction Daemon Safety

**User Story:** As a system operator, I want compaction to be safe and recoverable, so that data is never lost during the compaction process.

#### Acceptance Criteria

1. THE Compaction_Daemon SHALL never delete source partitions until the new partition is queryable and validated
2. WHEN validation fails, THE Compaction_Daemon SHALL halt compaction and emit an alert
3. THE Compaction_Daemon SHALL validate compacted partitions using checksum comparison
4. THE Compaction_Daemon SHALL support idempotent compaction operations for retry safety
5. IF compaction fails mid-process, THEN THE Compaction_Daemon SHALL leave source partitions unchanged

### Requirement 11: Object Storage Integration

**User Story:** As a system architect, I want reliable integration with cloud object storage, so that partitions are durably stored with high availability.

#### Acceptance Criteria

1. THE System SHALL support S3-compatible object storage (AWS S3, GCS, MinIO)
2. WHEN uploading partitions, THE Ingestion_Service SHALL use multipart upload with ETag validation
3. WHEN upload fails, THE Ingestion_Service SHALL abort the multipart upload and retry
4. THE System SHALL use S3 conditional PUT (If-Match) before updating the Manifest_Catalog
5. THE System SHALL support cross-region replication for disaster recovery
6. THE System SHALL achieve 99.99% durability for stored partitions

### Requirement 12: Partition Key Strategies

**User Story:** As a data engineer, I want flexible partition key strategies, so that I can optimize data layout for different workload patterns.

#### Acceptance Criteria

1. THE System SHALL support time-based partition keys (YYYYMMDD format)
2. THE System SHALL support tenant-based partition keys for multi-tenant isolation
3. THE System SHALL support hash-based partition keys (modulo N) for high-cardinality data
4. WHEN routing data, THE Ingestion_Service SHALL determine the target partition based on the Partition_Key strategy
5. THE System SHALL support configurable partition size targets per Partition_Key strategy

### Requirement 13: Schema Evolution

**User Story:** As a data engineer, I want to evolve the schema over time, so that I can add new columns without breaking existing queries.

#### Acceptance Criteria

1. THE System SHALL track Schema_Version per partition in the Manifest_Catalog
2. WHEN a schema change is declared, THE Ingestion_Service SHALL increment the Schema_Version for new partitions
3. WHEN querying partitions with different Schema_Versions, THE Query_Federation_Layer SHALL rewrite queries for compatibility
4. THE System SHALL support adding nullable columns without requiring partition rewrite
5. IF a query references a column not present in an older Schema_Version, THEN THE Query_Federation_Layer SHALL return NULL for that column

### Requirement 14: Error Handling and Recovery

**User Story:** As a system operator, I want comprehensive error handling and recovery mechanisms, so that the system remains available and data is protected.

#### Acceptance Criteria

1. WHEN an Ingestion_Service node crashes mid-batch, THE System SHALL support client retry with idempotency keys
2. WHEN the Manifest_Catalog becomes corrupted, THE System SHALL restore from Litestream backup within 10 seconds
3. WHEN a Query_Federation_Layer node runs out of memory, THE System SHALL restart and reload Bloom_Filters from Object_Storage
4. THE System SHALL run daily reconciliation to detect and repair dangling Manifest_Catalog entries
5. IF Object_Storage returns errors at rate greater than 1%, THEN THE System SHALL failover to secondary region

### Requirement 15: Observability

**User Story:** As a system operator, I want comprehensive metrics and logging, so that I can monitor system health and diagnose issues.

#### Acceptance Criteria

1. THE Ingestion_Service SHALL emit metrics for partition_created events including size_bytes
2. THE Query_Federation_Layer SHALL emit metrics for query latency, partitions_scanned, and pruning_ratio
3. THE Compaction_Daemon SHALL emit metrics for compaction_completed events including source_count and result_size
4. THE System SHALL emit structured logs with correlation IDs for request tracing
5. THE System SHALL expose health check endpoints for load balancer integration
6. THE System SHALL emit alerts when error rates exceed configured thresholds

### Requirement 16: Performance Targets

**User Story:** As a system architect, I want the system to meet defined performance targets, so that it can handle production workloads efficiently.

#### Acceptance Criteria

1. THE Ingestion_Service SHALL achieve throughput of at least 50,000 rows per second per node
2. THE Query_Federation_Layer SHALL achieve P95 query latency of less than 500ms for point queries
3. THE System SHALL achieve partition pruning ratio of at least 99.5% using Bloom_Filters
4. THE System SHALL achieve storage overhead of less than 5% for indexes and metadata
5. THE System SHALL achieve recovery time of less than 10 seconds for failed primary
6. THE System SHALL achieve 1.0x replication factor (no unnecessary data duplication)

### Requirement 17: API Design

**User Story:** As a developer, I want well-designed HTTP and gRPC APIs, so that I can integrate with the system programmatically.

#### Acceptance Criteria

1. THE Ingestion_Service SHALL expose HTTP POST endpoint at `/v1/ingest` accepting JSON rows with partition_key parameter
2. THE Ingestion_Service SHALL expose gRPC service for high-throughput batch ingestion
3. THE Query_Federation_Layer SHALL expose HTTP POST endpoint at `/v1/query` accepting SQL queries
4. THE System SHALL return appropriate HTTP status codes (200 for success, 400 for client errors, 500 for server errors)
5. THE System SHALL include request_id in all API responses for tracing
6. THE System SHALL support API versioning via URL path prefix

### Requirement 18: Concurrency and Thread Safety

**User Story:** As a system architect, I want the system to handle concurrent operations safely, so that data integrity is maintained under load.

#### Acceptance Criteria

1. THE Ingestion_Service SHALL support concurrent batch writes to different Partition_Keys
2. THE Query_Federation_Layer SHALL support concurrent query execution
3. THE Manifest_Catalog SHALL enforce single-writer semantics using file locking
4. WHEN multiple writers attempt concurrent updates, THE Manifest_Catalog SHALL serialize writes
5. THE System SHALL use connection pooling for Object_Storage operations
6. THE System SHALL implement graceful shutdown with in-flight request completion
