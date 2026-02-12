// Package manifest provides the manifest catalog for tracking partition metadata.
package manifest

// Schema contains the SQL schema definitions for the manifest catalog (manifest.db).
// The manifest catalog is a SQLite database that serves as the source of truth
// for all partition metadata in the system.

// CreatePartitionsTableSQL creates the core partitions table.
// This table stores metadata for all partitions including min/max statistics
// for efficient pruning during query execution.
const CreatePartitionsTableSQL = `
CREATE TABLE IF NOT EXISTS partitions (
    partition_id TEXT PRIMARY KEY,
    partition_key TEXT NOT NULL,
    object_path TEXT NOT NULL,
    min_user_id INTEGER,
    max_user_id INTEGER,
    min_event_time INTEGER,
    max_event_time INTEGER,
    min_tenant_id TEXT,
    max_tenant_id TEXT,
    row_count INTEGER NOT NULL,
    size_bytes INTEGER NOT NULL,
    schema_version INTEGER NOT NULL DEFAULT 1,
    created_at INTEGER NOT NULL,
    compacted_into TEXT,
    FOREIGN KEY (compacted_into) REFERENCES partitions(partition_id)
)`

// CreatePartitionsIndexesSQL creates indexes for efficient partition pruning.
// All indexes use filtered conditions (WHERE compacted_into IS NULL) to exclude
// compacted partitions from active queries.
var CreatePartitionsIndexesSQL = []string{
	// Index for time-based range queries
	`CREATE INDEX IF NOT EXISTS idx_partitions_time ON partitions(min_event_time, max_event_time)
		WHERE compacted_into IS NULL`,

	// Index for user_id range queries
	`CREATE INDEX IF NOT EXISTS idx_partitions_user ON partitions(min_user_id, max_user_id)
		WHERE compacted_into IS NULL`,

	// Index for tenant_id range queries
	`CREATE INDEX IF NOT EXISTS idx_partitions_tenant ON partitions(min_tenant_id, max_tenant_id)
		WHERE compacted_into IS NULL`,

	// Index for partition key lookups
	`CREATE INDEX IF NOT EXISTS idx_partitions_key ON partitions(partition_key)
		WHERE compacted_into IS NULL`,

	// Index for finding small partitions (compaction candidates)
	`CREATE INDEX IF NOT EXISTS idx_partitions_size ON partitions(size_bytes)
		WHERE compacted_into IS NULL`,

	// Index for TTL-based garbage collection
	`CREATE INDEX IF NOT EXISTS idx_partitions_created ON partitions(created_at)`,

	// Composite covering index for the most common prune query pattern (time + user range)
	`CREATE INDEX IF NOT EXISTS idx_partitions_prune ON partitions(compacted_into, min_event_time, max_event_time, min_user_id, max_user_id)`,

	// Composite covering index for tenant-based pruning
	`CREATE INDEX IF NOT EXISTS idx_partitions_tenant_prune ON partitions(compacted_into, min_tenant_id, max_tenant_id)`,
}

// CreateSchemaVersionsTableSQL creates the schema versions table.
// This table tracks schema evolution for backward compatibility.
const CreateSchemaVersionsTableSQL = `
CREATE TABLE IF NOT EXISTS schema_versions (
    version INTEGER PRIMARY KEY,
    schema_json TEXT NOT NULL,
    created_at INTEGER NOT NULL
)`

// CreateIdempotencyKeysTableSQL creates the idempotency keys table.
// This table tracks idempotency keys to support client retry with deduplication.
const CreateIdempotencyKeysTableSQL = `
CREATE TABLE IF NOT EXISTS idempotency_keys (
    key TEXT PRIMARY KEY,
    partition_id TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    FOREIGN KEY (partition_id) REFERENCES partitions(partition_id)
)`

// CreateCompactionIntentsTableSQL creates the compaction intents table.
// This table tracks in-progress compaction operations for crash recovery.
// Phase 1 writes an intent; Phase 2 completes the compaction and deletes the intent.
const CreateCompactionIntentsTableSQL = `
CREATE TABLE IF NOT EXISTS compaction_intents (
    target_partition_id TEXT PRIMARY KEY,
    source_partition_ids TEXT NOT NULL,
    target_object_path TEXT NOT NULL,
    target_meta_path TEXT NOT NULL,
    created_at INTEGER NOT NULL
)`

// CreateZoneMapsTableSQL creates the zone maps table.
// Zone maps store per-partition-key aggregate bloom filters in the manifest itself,
// enabling bloom filter pruning without S3 GETs for metadata sidecars.
// Each row holds a merged bloom filter for all active partitions sharing a partition_key.
const CreateZoneMapsTableSQL = `
CREATE TABLE IF NOT EXISTS zone_maps (
    partition_key TEXT NOT NULL,
    column_name TEXT NOT NULL,
    bloom_data BLOB NOT NULL,
    num_bits INTEGER NOT NULL,
    num_hashes INTEGER NOT NULL,
    item_count INTEGER NOT NULL,
    distinct_count INTEGER NOT NULL DEFAULT 0,
    updated_at INTEGER NOT NULL,
    PRIMARY KEY (partition_key, column_name)
)`

// CreateIdempotencyKeysIndexSQL creates an index for TTL-based cleanup of idempotency keys.
const CreateIdempotencyKeysIndexSQL = `
CREATE INDEX IF NOT EXISTS idx_idempotency_created ON idempotency_keys(created_at)`

// AnalyzeSQL runs ANALYZE to keep the SQLite query planner informed about index statistics.
const AnalyzeSQL = `ANALYZE`

// AllSchemaSQL returns all SQL statements needed to initialize the manifest catalog.
func AllSchemaSQL() []string {
	statements := []string{
		CreatePartitionsTableSQL,
		CreateSchemaVersionsTableSQL,
		CreateIdempotencyKeysTableSQL,
		CreateIdempotencyKeysIndexSQL,
		CreateCompactionIntentsTableSQL,
		CreateZoneMapsTableSQL,
	}
	statements = append(statements, CreatePartitionsIndexesSQL...)
	return statements
}
