Property 1:
WHEN a client sends a batch write request to the Ingestion_Service, THE Ingestion_Service SHALL validate the rows against the declared schema for the specified Partition_Key
This is about schema validation across all possible rows and schemas. We can generate random rows and schemas, then verify that valid rows pass validation and invalid rows are rejected.
Requirements 1.1
Property 2:
WHEN the Ingestion_Service receives valid rows, THE Ingestion_Service SHALL generate a SQLite Micro_Partition file with optimized indexes
This is about partition generation. We can verify that for any set of valid rows, a SQLite file is created with the expected indexes.
Requirements 1.2
Property 3:
WHEN a Micro_Partition is generated, THE Ingestion_Service SHALL compute min/max statistics for indexed columns
This is about statistics computation. For any set of rows, the computed min/max should match the actual min/max values in the data.
Requirements 1.3
Property 4:
WHEN a Micro_Partition is generated, THE Ingestion_Service SHALL build Bloom_Filters for high-cardinality columns (tenant_id, user_id)
This is about bloom filter construction. We can verify that all values added to the filter return true for Contains().
Requirements 1.4
Property 5:
THE Ingestion_Service SHALL generate Micro_Partitions with size between 8MB and 16MB
This is about partition sizing. For any set of rows that would exceed 16MB, the system should split into multiple partitions.
Requirements 2.1
Property 6:
WHEN generating a Micro_Partition, THE Ingestion_Service SHALL use ULID as the primary key for events
This is about ULID generation. ULIDs should be time-ordered and unique.
Requirements 2.3
Property 7:
WHEN generating a Micro_Partition, THE Ingestion_Service SHALL compress payload data using Snappy compression
This is about compression. We can verify round-trip: compress then decompress should equal original.
Requirements 2.4
Property 8:
WHEN a Micro_Partition is created, THE Ingestion_Service SHALL generate a corresponding Metadata_Sidecar file
This is about sidecar generation. For any partition, a valid sidecar should be created.
Requirements 2.6
Property 9:
WHEN generating a Metadata_Sidecar, THE Ingestion_Service SHALL include partition_id, schema_version, and creation timestamp
This is about sidecar content. We can verify required fields are present.
Requirements 3.1
Property 10:
WHEN generating a Metadata_Sidecar, THE Ingestion_Service SHALL include row_count, size_bytes, and min/max values
This is about statistics accuracy. The sidecar stats should match the actual partition data.
Requirements 3.2
Property 11:
WHEN generating a Metadata_Sidecar, THE Ingestion_Service SHALL configure Bloom_Filters with a false positive rate of 1% or less
This is about FPR configuration. We can test that the actual FPR is within bounds.
Requirements 3.4
Property 12:
WHEN serializing a Metadata_Sidecar, THE Ingestion_Service SHALL encode Bloom_Filter data as base64
This is about serialization. Round-trip: serialize then deserialize should produce equivalent filter.
Requirements 3.5
Property 13:
THE Metadata_Sidecar SHALL be serialized as JSON
This is about format. We can verify valid JSON is produced.
Requirements 3.6
Property 14:
THE Manifest_Catalog SHALL store partition metadata including partition_id, partition_key, object_path, min/max statistics, row_count, size_bytes, and schema_version
This is about schema completeness. We can verify all fields are stored and retrievable.
Requirements 4.1
Property 15:
WHEN a partition is compacted, THE Manifest_Catalog SHALL update the compacted_into field
This is about compaction tracking. After compaction, source partitions should reference the target.
Requirements 4.3
Property 16:
THE Manifest_Catalog SHALL exclude compacted partitions from active queries via filtered indexes
This is about query behavior. Compacted partitions should not appear in query results.
Requirements 4.4
Property 17:
THE Manifest_Catalog SHALL enforce single-writer semantics
This is about concurrency control. We can test that concurrent writes are serialized.
Requirements 4.6
Property 18:
THE Query_Federation_Layer SHALL parse SQL queries using a recursive descent parser
This is about parsing. We can test round-trip: parse then print should produce equivalent SQL.
Requirements 5.1
Property 19:
WHEN parsing a query, THE Query_Federation_Layer SHALL extract predicates for partition pruning
This is about predicate extraction. For any query with predicates, they should be correctly extracted.
Requirements 5.2
Property 20:
WHEN parsing a query, THE Query_Federation_Layer SHALL identify columns referenced in WHERE, GROUP BY, and ORDER BY clauses
This is about column identification. We can verify all referenced columns are found.
Requirements 5.3
Property 21:
IF a query contains unsupported SQL syntax, THEN THE Query_Federation_Layer SHALL return a descriptive error message
This is about error handling. We can test that invalid SQL produces errors.
Requirements 5.5
Property 22:
THE Query_Federation_Layer SHALL support SELECT, WHERE, GROUP BY, ORDER BY, LIMIT, and aggregate functions
This is about feature support. We can test each feature works correctly.
Requirements 5.6
Property 23:
WHEN performing phase 1 pruning, THE Query_Federation_Layer SHALL query the Manifest_Catalog using min/max statistics
This is about min/max pruning. Partitions outside the query range should be excluded.
Requirements 6.2
Property 24:
WHEN performing phase 2 pruning, THE Query_Federation_Layer SHALL apply Bloom_Filters to eliminate false positives
This is about bloom filter pruning. Partitions not containing the queried value should be excluded.
Requirements 6.3
Property 25:
WHEN executing a query, THE Query_Federation_Layer SHALL dispatch subqueries to pruned partitions in parallel
This is about parallel execution. We can verify results are correct regardless of parallelism.
Requirements 7.1
Property 26:
THE Query_Federation_Layer SHALL merge partial results using stream-oriented UNION ALL
This is about result merging. The merged result should contain all rows from all partitions.
Requirements 7.4
Property 27:
WHEN a query includes LIMIT, THE Query_Federation_Layer SHALL support early termination
This is about LIMIT behavior. Results should be limited correctly.
Requirements 7.5
Property 28:
WHEN executing aggregate queries, THE Query_Federation_Layer SHALL compute partial aggregates per partition
This is about aggregate computation. Partial aggregates should be computed correctly.
Requirements 8.1
Property 29:
WHEN merging results, THE Query_Federation_Layer SHALL combine partial aggregates correctly
This is about aggregate merging. SUM of SUMs, COUNT of COUNTs, etc. should be correct.
Requirements 8.2
Property 30:
WHEN executing GROUP BY queries, THE Query_Federation_Layer SHALL merge groups across partitions
This is about GROUP BY merging. Groups with same key should be combined.
Requirements 8.3
Property 31:
WHEN executing ORDER BY queries, THE Query_Federation_Layer SHALL perform merge sort on partial results
This is about sorting. Final results should be correctly ordered.
Requirements 8.4
Property 32:
WHEN executing LIMIT queries, THE Query_Federation_Layer SHALL apply limit after final merge
This is about LIMIT application. Limit should be applied to merged results.
Requirements 8.5
Property 33:
WHEN a partition is smaller than 8MB, THE Compaction_Daemon SHALL mark it as a candidate for compaction
This is about candidate selection. Small partitions should be identified.
Requirements 9.1
Property 34:
WHEN more than 100 partitions exist for the same Partition_Key per day, THE Compaction_Daemon SHALL trigger compaction
This is about trigger conditions. We can verify compaction is triggered.
Requirements 9.2
Property 35:
WHEN compacting partitions, THE Compaction_Daemon SHALL merge data sorted by primary key
This is about merge order. Compacted data should be sorted.
Requirements 9.3
Property 36:
WHEN updating the Manifest_Catalog, THE Compaction_Daemon SHALL atomically set compacted_into for source partitions
This is about atomicity. All sources should be updated together.
Requirements 9.5
Property 37:
THE Compaction_Daemon SHALL retain source partitions for 7 days before garbage collection
This is about TTL. Partitions should not be deleted before TTL.
Requirements 9.6
Property 38:
THE Compaction_Daemon SHALL never delete source partitions until the new partition is queryable and validated
This is about safety. Source partitions should remain until target is ready.
Requirements 10.1
Property 39:
THE Compaction_Daemon SHALL validate compacted partitions using checksum comparison
This is about validation. Checksums should match expected values.
Requirements 10.3
Property 40:
THE Compaction_Daemon SHALL support idempotent compaction operations
This is about idempotency. Running compaction twice should produce same result.
Requirements 10.4
Property 41:
IF compaction fails mid-process, THEN THE Compaction_Daemon SHALL leave source partitions unchanged
This is about failure handling. Sources should be preserved on failure.
Requirements 10.5
Property 42:
THE System SHALL support time-based partition keys (YYYYMMDD format)
This is about key format. We can verify time-based keys are parsed correctly.
Requirements 12.1
Property 43:
THE System SHALL support tenant-based partition keys
This is about key format. We can verify tenant keys work correctly.
Requirements 12.2
Property 44:
THE System SHALL support hash-based partition keys (modulo N)
This is about key format. We can verify hash keys distribute correctly.
Requirements 12.3
Property 45:
WHEN routing data, THE Ingestion_Service SHALL determine the target partition based on the Partition_Key strategy
This is about routing. Data should go to the correct partition.
Requirements 12.4
Property 46:
THE System SHALL track Schema_Version per partition in the Manifest_Catalog
This is about version tracking. Versions should be stored and retrievable.
Requirements 13.1
Property 47:
WHEN a schema change is declared, THE Ingestion_Service SHALL increment the Schema_Version
This is about version incrementing. New schema should have higher version.
Requirements 13.2
Property 48:
WHEN querying partitions with different Schema_Versions, THE Query_Federation_Layer SHALL rewrite queries for compatibility
This is about query rewriting. Queries should work across schema versions.
Requirements 13.3
Property 49:
THE System SHALL support adding nullable columns without requiring partition rewrite
This is about schema evolution. New columns should be addable.
Requirements 13.4
Property 50:
IF a query references a column not present in an older Schema_Version, THEN THE Query_Federation_Layer SHALL return NULL
This is about NULL handling. Missing columns should return NULL.
Requirements 13.5
Property 51:
WHEN an Ingestion_Service node crashes mid-batch, THE System SHALL support client retry with idempotency keys
This is about idempotency. Retries with same key should not duplicate data.
Requirements 14.1
Property 52:
THE System SHALL run daily reconciliation to detect and repair dangling Manifest_Catalog entries
This is about reconciliation. We can verify dangling entries are detected.
Requirements 14.4
Property 53:
THE System SHALL emit structured logs with correlation IDs for request tracing
This is about logging. We can verify logs contain correlation IDs.
Requirements 15.4
Property 54:
THE System SHALL return appropriate HTTP status codes
This is about HTTP semantics. We can verify correct status codes.
Requirements 17.4
Property 55:
THE System SHALL include request_id in all API responses
This is about response format. We can verify request_id is present.
Requirements 17.5
Property 56:
THE Ingestion_Service SHALL support concurrent batch writes to different Partition_Keys
This is about concurrency. Concurrent writes should not interfere.
Requirements 18.1
Property 57:
THE Query_Federation_Layer SHALL support concurrent query execution
This is about concurrency. Concurrent queries should produce correct results.
Requirements 18.2
Property 58:
THE Manifest_Catalog SHALL enforce single-writer semantics using file locking
This is about locking. Only one writer should succeed at a time.
Requirements 18.3
Property 59:
WHEN multiple writers attempt concurrent updates, THE Manifest_Catalog SHALL serialize writes
This is about serialization. Writes should be ordered.
Requirements 18.4
Property 60:
THE System SHALL implement graceful shutdown with in-flight request completion
This is about shutdown behavior. In-flight requests should complete.
Requirements 18.6
