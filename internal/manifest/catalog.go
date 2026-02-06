package manifest

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	"github.com/arkilian/arkilian/internal/partition"
	_ "github.com/mattn/go-sqlite3"
)

// Catalog manages partition metadata in manifest.db.
type Catalog interface {
	// RegisterPartition adds a new partition to the catalog.
	RegisterPartition(ctx context.Context, info *partition.PartitionInfo, objectPath string) error

	// RegisterPartitionWithIdempotencyKey adds a new partition with idempotency key support.
	// If the idempotency key already exists, returns the existing partition ID without error.
	RegisterPartitionWithIdempotencyKey(ctx context.Context, info *partition.PartitionInfo, objectPath, idempotencyKey string) (string, error)

	// FindPartitions returns partitions matching the given predicates.
	FindPartitions(ctx context.Context, predicates []Predicate) ([]*PartitionRecord, error)

	// GetPartition retrieves a single partition by ID.
	GetPartition(ctx context.Context, partitionID string) (*PartitionRecord, error)

	// MarkCompacted marks source partitions as compacted into target.
	MarkCompacted(ctx context.Context, sourceIDs []string, targetID string) error

	// GetCompactionCandidates returns partitions eligible for compaction.
	GetCompactionCandidates(ctx context.Context, key string, maxSize int64) ([]*PartitionRecord, error)

	// DeleteExpired removes partitions past TTL.
	DeleteExpired(ctx context.Context, ttl time.Duration) ([]string, error)

	// Close closes the catalog database connection.
	Close() error
}

// PartitionRecord represents a partition in the manifest.
type PartitionRecord struct {
	PartitionID   string
	PartitionKey  string
	ObjectPath    string
	MinUserID     *int64
	MaxUserID     *int64
	MinEventTime  *int64
	MaxEventTime  *int64
	MinTenantID   *string
	MaxTenantID   *string
	RowCount      int64
	SizeBytes     int64
	SchemaVersion int
	CreatedAt     time.Time
	CompactedInto *string
}

// Predicate represents a query predicate for pruning.
type Predicate struct {
	Column   string
	Operator string // "=", "<", ">", "<=", ">=", "BETWEEN", "IN"
	Value    interface{}
	Values   []interface{} // For IN and BETWEEN
}

// SQLiteCatalog implements Catalog using SQLite.
type SQLiteCatalog struct {
	db     *sql.DB
	dbPath string
	mu     sync.Mutex // Single-writer lock
}

// NewCatalog creates a new SQLite-based catalog.
func NewCatalog(dbPath string) (*SQLiteCatalog, error) {
	db, err := sql.Open("sqlite3", dbPath+"?_journal_mode=WAL&_busy_timeout=5000")
	if err != nil {
		return nil, fmt.Errorf("manifest: failed to open database: %w", err)
	}

	// Set connection pool settings
	db.SetMaxOpenConns(1) // Single writer
	db.SetMaxIdleConns(1)

	catalog := &SQLiteCatalog{
		db:     db,
		dbPath: dbPath,
	}

	// Initialize schema
	if err := catalog.initSchema(); err != nil {
		db.Close()
		return nil, fmt.Errorf("manifest: failed to initialize schema: %w", err)
	}

	return catalog, nil
}

// initSchema creates all required tables and indexes.
func (c *SQLiteCatalog) initSchema() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, stmt := range AllSchemaSQL() {
		if _, err := c.db.Exec(stmt); err != nil {
			return fmt.Errorf("failed to execute schema statement: %w", err)
		}
	}
	return nil
}

// RegisterPartition adds a new partition to the catalog.
func (c *SQLiteCatalog) RegisterPartition(ctx context.Context, info *partition.PartitionInfo, objectPath string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.insertPartition(ctx, info, objectPath)
}

// insertPartition inserts a partition record (must be called with lock held).
func (c *SQLiteCatalog) insertPartition(ctx context.Context, info *partition.PartitionInfo, objectPath string) error {
	// Extract min/max stats
	var minUserID, maxUserID, minEventTime, maxEventTime *int64
	var minTenantID, maxTenantID *string

	if stat, ok := info.MinMaxStats["user_id"]; ok {
		if v, ok := stat.Min.(int64); ok {
			minUserID = &v
		}
		if v, ok := stat.Max.(int64); ok {
			maxUserID = &v
		}
	}

	if stat, ok := info.MinMaxStats["event_time"]; ok {
		if v, ok := stat.Min.(int64); ok {
			minEventTime = &v
		}
		if v, ok := stat.Max.(int64); ok {
			maxEventTime = &v
		}
	}

	if stat, ok := info.MinMaxStats["tenant_id"]; ok {
		if v, ok := stat.Min.(string); ok {
			minTenantID = &v
		}
		if v, ok := stat.Max.(string); ok {
			maxTenantID = &v
		}
	}

	query := `
		INSERT INTO partitions (
			partition_id, partition_key, object_path,
			min_user_id, max_user_id,
			min_event_time, max_event_time,
			min_tenant_id, max_tenant_id,
			row_count, size_bytes, schema_version, created_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

	_, err := c.db.ExecContext(ctx, query,
		info.PartitionID, info.PartitionKey, objectPath,
		minUserID, maxUserID,
		minEventTime, maxEventTime,
		minTenantID, maxTenantID,
		info.RowCount, info.SizeBytes, info.SchemaVersion, info.CreatedAt.Unix(),
	)
	if err != nil {
		return fmt.Errorf("manifest: failed to insert partition: %w", err)
	}

	return nil
}


// RegisterPartitionWithIdempotencyKey adds a new partition with idempotency key support.
func (c *SQLiteCatalog) RegisterPartitionWithIdempotencyKey(ctx context.Context, info *partition.PartitionInfo, objectPath, idempotencyKey string) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Check if idempotency key already exists
	var existingPartitionID string
	err := c.db.QueryRowContext(ctx,
		"SELECT partition_id FROM idempotency_keys WHERE key = ?",
		idempotencyKey,
	).Scan(&existingPartitionID)

	if err == nil {
		// Idempotency key exists, return existing partition ID
		return existingPartitionID, nil
	}
	if err != sql.ErrNoRows {
		return "", fmt.Errorf("manifest: failed to check idempotency key: %w", err)
	}

	// Start transaction for atomic insert
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return "", fmt.Errorf("manifest: failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Insert partition
	if err := c.insertPartitionTx(ctx, tx, info, objectPath); err != nil {
		return "", err
	}

	// Insert idempotency key
	_, err = tx.ExecContext(ctx,
		"INSERT INTO idempotency_keys (key, partition_id, created_at) VALUES (?, ?, ?)",
		idempotencyKey, info.PartitionID, time.Now().Unix(),
	)
	if err != nil {
		return "", fmt.Errorf("manifest: failed to insert idempotency key: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return "", fmt.Errorf("manifest: failed to commit transaction: %w", err)
	}

	return info.PartitionID, nil
}

// insertPartitionTx inserts a partition record within a transaction.
func (c *SQLiteCatalog) insertPartitionTx(ctx context.Context, tx *sql.Tx, info *partition.PartitionInfo, objectPath string) error {
	var minUserID, maxUserID, minEventTime, maxEventTime *int64
	var minTenantID, maxTenantID *string

	if stat, ok := info.MinMaxStats["user_id"]; ok {
		if v, ok := stat.Min.(int64); ok {
			minUserID = &v
		}
		if v, ok := stat.Max.(int64); ok {
			maxUserID = &v
		}
	}

	if stat, ok := info.MinMaxStats["event_time"]; ok {
		if v, ok := stat.Min.(int64); ok {
			minEventTime = &v
		}
		if v, ok := stat.Max.(int64); ok {
			maxEventTime = &v
		}
	}

	if stat, ok := info.MinMaxStats["tenant_id"]; ok {
		if v, ok := stat.Min.(string); ok {
			minTenantID = &v
		}
		if v, ok := stat.Max.(string); ok {
			maxTenantID = &v
		}
	}

	query := `
		INSERT INTO partitions (
			partition_id, partition_key, object_path,
			min_user_id, max_user_id,
			min_event_time, max_event_time,
			min_tenant_id, max_tenant_id,
			row_count, size_bytes, schema_version, created_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

	_, err := tx.ExecContext(ctx, query,
		info.PartitionID, info.PartitionKey, objectPath,
		minUserID, maxUserID,
		minEventTime, maxEventTime,
		minTenantID, maxTenantID,
		info.RowCount, info.SizeBytes, info.SchemaVersion, info.CreatedAt.Unix(),
	)
	if err != nil {
		return fmt.Errorf("manifest: failed to insert partition: %w", err)
	}

	return nil
}

// GetPartition retrieves a single partition by ID.
func (c *SQLiteCatalog) GetPartition(ctx context.Context, partitionID string) (*PartitionRecord, error) {
	query := `
		SELECT partition_id, partition_key, object_path,
			min_user_id, max_user_id,
			min_event_time, max_event_time,
			min_tenant_id, max_tenant_id,
			row_count, size_bytes, schema_version, created_at, compacted_into
		FROM partitions
		WHERE partition_id = ?`

	row := c.db.QueryRowContext(ctx, query, partitionID)
	return c.scanPartitionRecord(row)
}

// scanPartitionRecord scans a row into a PartitionRecord.
func (c *SQLiteCatalog) scanPartitionRecord(row *sql.Row) (*PartitionRecord, error) {
	var record PartitionRecord
	var createdAtUnix int64

	err := row.Scan(
		&record.PartitionID, &record.PartitionKey, &record.ObjectPath,
		&record.MinUserID, &record.MaxUserID,
		&record.MinEventTime, &record.MaxEventTime,
		&record.MinTenantID, &record.MaxTenantID,
		&record.RowCount, &record.SizeBytes, &record.SchemaVersion, &createdAtUnix, &record.CompactedInto,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("manifest: partition not found")
		}
		return nil, fmt.Errorf("manifest: failed to scan partition: %w", err)
	}

	record.CreatedAt = time.Unix(createdAtUnix, 0)
	return &record, nil
}

// FindPartitions returns partitions matching the given predicates.
func (c *SQLiteCatalog) FindPartitions(ctx context.Context, predicates []Predicate) ([]*PartitionRecord, error) {
	query, args := c.buildFindQuery(predicates)

	rows, err := c.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("manifest: failed to query partitions: %w", err)
	}
	defer rows.Close()

	var records []*PartitionRecord
	for rows.Next() {
		record, err := c.scanPartitionRows(rows)
		if err != nil {
			return nil, err
		}
		records = append(records, record)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("manifest: error iterating partitions: %w", err)
	}

	return records, nil
}

// scanPartitionRows scans rows into a PartitionRecord.
func (c *SQLiteCatalog) scanPartitionRows(rows *sql.Rows) (*PartitionRecord, error) {
	var record PartitionRecord
	var createdAtUnix int64

	err := rows.Scan(
		&record.PartitionID, &record.PartitionKey, &record.ObjectPath,
		&record.MinUserID, &record.MaxUserID,
		&record.MinEventTime, &record.MaxEventTime,
		&record.MinTenantID, &record.MaxTenantID,
		&record.RowCount, &record.SizeBytes, &record.SchemaVersion, &createdAtUnix, &record.CompactedInto,
	)
	if err != nil {
		return nil, fmt.Errorf("manifest: failed to scan partition: %w", err)
	}

	record.CreatedAt = time.Unix(createdAtUnix, 0)
	return &record, nil
}

// buildFindQuery builds a SQL query from predicates.
func (c *SQLiteCatalog) buildFindQuery(predicates []Predicate) (string, []interface{}) {
	baseQuery := `
		SELECT partition_id, partition_key, object_path,
			min_user_id, max_user_id,
			min_event_time, max_event_time,
			min_tenant_id, max_tenant_id,
			row_count, size_bytes, schema_version, created_at, compacted_into
		FROM partitions
		WHERE compacted_into IS NULL`

	var args []interface{}

	for _, pred := range predicates {
		clause, predArgs := c.buildPredicateClause(pred)
		if clause != "" {
			baseQuery += " AND " + clause
			args = append(args, predArgs...)
		}
	}

	return baseQuery, args
}

// buildPredicateClause builds a SQL clause from a predicate.
func (c *SQLiteCatalog) buildPredicateClause(pred Predicate) (string, []interface{}) {
	// Map column names to min/max columns for range-based pruning
	minCol, maxCol := c.getMinMaxColumns(pred.Column)

	switch pred.Operator {
	case "=":
		// For equality, check if value is within min/max range
		return fmt.Sprintf("(%s IS NULL OR %s <= ?) AND (%s IS NULL OR %s >= ?)",
			minCol, minCol, maxCol, maxCol), []interface{}{pred.Value, pred.Value}

	case "<":
		// Partition min must be less than value
		return fmt.Sprintf("(%s IS NULL OR %s < ?)", minCol, minCol), []interface{}{pred.Value}

	case "<=":
		return fmt.Sprintf("(%s IS NULL OR %s <= ?)", minCol, minCol), []interface{}{pred.Value}

	case ">":
		// Partition max must be greater than value
		return fmt.Sprintf("(%s IS NULL OR %s > ?)", maxCol, maxCol), []interface{}{pred.Value}

	case ">=":
		return fmt.Sprintf("(%s IS NULL OR %s >= ?)", maxCol, maxCol), []interface{}{pred.Value}

	case "BETWEEN":
		if len(pred.Values) >= 2 {
			// Partition range must overlap with query range
			return fmt.Sprintf("(%s IS NULL OR %s <= ?) AND (%s IS NULL OR %s >= ?)",
				minCol, minCol, maxCol, maxCol), []interface{}{pred.Values[1], pred.Values[0]}
		}

	case "IN":
		// For IN, we need to check if any value could be in the partition
		// This is a simplified approach - check if partition range overlaps with any value
		if len(pred.Values) > 0 {
			// Find min and max of IN values
			minVal, maxVal := pred.Values[0], pred.Values[0]
			for _, v := range pred.Values[1:] {
				if c.compareValues(v, minVal) < 0 {
					minVal = v
				}
				if c.compareValues(v, maxVal) > 0 {
					maxVal = v
				}
			}
			return fmt.Sprintf("(%s IS NULL OR %s <= ?) AND (%s IS NULL OR %s >= ?)",
				minCol, minCol, maxCol, maxCol), []interface{}{maxVal, minVal}
		}
	}

	return "", nil
}

// getMinMaxColumns returns the min and max column names for a given column.
func (c *SQLiteCatalog) getMinMaxColumns(column string) (string, string) {
	switch column {
	case "user_id":
		return "min_user_id", "max_user_id"
	case "event_time":
		return "min_event_time", "max_event_time"
	case "tenant_id":
		return "min_tenant_id", "max_tenant_id"
	default:
		// For unknown columns, return the column name itself
		return column, column
	}
}

// compareValues compares two interface values.
func (c *SQLiteCatalog) compareValues(a, b interface{}) int {
	switch va := a.(type) {
	case int64:
		if vb, ok := b.(int64); ok {
			if va < vb {
				return -1
			} else if va > vb {
				return 1
			}
			return 0
		}
	case string:
		if vb, ok := b.(string); ok {
			if va < vb {
				return -1
			} else if va > vb {
				return 1
			}
			return 0
		}
	}
	return 0
}

// MarkCompacted marks source partitions as compacted into target.
func (c *SQLiteCatalog) MarkCompacted(ctx context.Context, sourceIDs []string, targetID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("manifest: failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Verify target partition exists
	var exists int
	err = tx.QueryRowContext(ctx, "SELECT 1 FROM partitions WHERE partition_id = ?", targetID).Scan(&exists)
	if err != nil {
		return fmt.Errorf("manifest: target partition %s not found: %w", targetID, err)
	}

	// Update source partitions
	for _, sourceID := range sourceIDs {
		result, err := tx.ExecContext(ctx,
			"UPDATE partitions SET compacted_into = ? WHERE partition_id = ? AND compacted_into IS NULL",
			targetID, sourceID,
		)
		if err != nil {
			return fmt.Errorf("manifest: failed to mark partition %s as compacted: %w", sourceID, err)
		}

		rowsAffected, _ := result.RowsAffected()
		if rowsAffected == 0 {
			return fmt.Errorf("manifest: partition %s not found or already compacted", sourceID)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("manifest: failed to commit compaction: %w", err)
	}

	return nil
}

// GetCompactionCandidates returns partitions eligible for compaction.
func (c *SQLiteCatalog) GetCompactionCandidates(ctx context.Context, key string, maxSize int64) ([]*PartitionRecord, error) {
	query := `
		SELECT partition_id, partition_key, object_path,
			min_user_id, max_user_id,
			min_event_time, max_event_time,
			min_tenant_id, max_tenant_id,
			row_count, size_bytes, schema_version, created_at, compacted_into
		FROM partitions
		WHERE compacted_into IS NULL
			AND partition_key = ?
			AND size_bytes < ?
		ORDER BY created_at ASC`

	rows, err := c.db.QueryContext(ctx, query, key, maxSize)
	if err != nil {
		return nil, fmt.Errorf("manifest: failed to query compaction candidates: %w", err)
	}
	defer rows.Close()

	var records []*PartitionRecord
	for rows.Next() {
		record, err := c.scanPartitionRows(rows)
		if err != nil {
			return nil, err
		}
		records = append(records, record)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("manifest: error iterating compaction candidates: %w", err)
	}

	return records, nil
}

// DeleteExpired removes partitions past TTL.
func (c *SQLiteCatalog) DeleteExpired(ctx context.Context, ttl time.Duration) ([]string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	cutoff := time.Now().Add(-ttl).Unix()

	// Find expired partitions that have been compacted
	rows, err := c.db.QueryContext(ctx,
		`SELECT partition_id FROM partitions 
		 WHERE compacted_into IS NOT NULL AND created_at < ?`,
		cutoff,
	)
	if err != nil {
		return nil, fmt.Errorf("manifest: failed to query expired partitions: %w", err)
	}
	defer rows.Close()

	var expiredIDs []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("manifest: failed to scan partition ID: %w", err)
		}
		expiredIDs = append(expiredIDs, id)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("manifest: error iterating expired partitions: %w", err)
	}

	// Delete expired partitions
	for _, id := range expiredIDs {
		// First delete any idempotency keys referencing this partition
		if _, err := c.db.ExecContext(ctx, "DELETE FROM idempotency_keys WHERE partition_id = ?", id); err != nil {
			return nil, fmt.Errorf("manifest: failed to delete idempotency keys for partition %s: %w", id, err)
		}

		// Then delete the partition
		if _, err := c.db.ExecContext(ctx, "DELETE FROM partitions WHERE partition_id = ?", id); err != nil {
			return nil, fmt.Errorf("manifest: failed to delete partition %s: %w", id, err)
		}
	}

	return expiredIDs, nil
}

// Close closes the catalog database connection.
func (c *SQLiteCatalog) Close() error {
	return c.db.Close()
}

// GetPartitionCount returns the total number of active (non-compacted) partitions.
func (c *SQLiteCatalog) GetPartitionCount(ctx context.Context) (int64, error) {
	var count int64
	err := c.db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM partitions WHERE compacted_into IS NULL",
	).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("manifest: failed to count partitions: %w", err)
	}
	return count, nil
}

// GetPartitionCountByKey returns the number of active partitions for a given partition key.
func (c *SQLiteCatalog) GetPartitionCountByKey(ctx context.Context, key string) (int64, error) {
	var count int64
	err := c.db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM partitions WHERE compacted_into IS NULL AND partition_key = ?",
		key,
	).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("manifest: failed to count partitions by key: %w", err)
	}
	return count, nil
}
