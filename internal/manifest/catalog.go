package manifest

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/arkilian/arkilian/internal/index"
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

	// FindHighestIdempotencyLSN finds the highest LSN from idempotency keys matching the given prefix.
	// This is used by WAL recovery to determine which entries have already been flushed.
	// The prefix should be "wal-lsn-" to find WAL-flushed entries.
	FindHighestIdempotencyLSN(ctx context.Context, prefix string) (uint64, error)

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
	db     *sql.DB // Write connection (single writer)
	readDB *sql.DB // Read connection pool (concurrent readers)
	dbPath string
	mu     sync.Mutex // Write-only lock (reads don't need this)

	// Prepared statement cache (for read connection)
	insertPartitionStmt *sql.Stmt
	findStmtCache       map[string]*sql.Stmt
	findStmtMu          sync.RWMutex
}

// NewCatalog creates a new SQLite-based catalog.
func NewCatalog(dbPath string) (*SQLiteCatalog, error) {
	// Write connection: single writer with WAL mode
	db, err := sql.Open("sqlite3", dbPath+"?_journal_mode=WAL&_busy_timeout=5000")
	if err != nil {
		return nil, fmt.Errorf("manifest: failed to open database: %w", err)
	}
	db.SetMaxOpenConns(1) // Single writer
	db.SetMaxIdleConns(1)

	// Read connection pool: concurrent readers via read-only mode
	readDB, err := sql.Open("sqlite3", dbPath+"?_journal_mode=WAL&_busy_timeout=5000&mode=ro")
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("manifest: failed to open read database: %w", err)
	}
	readDB.SetMaxOpenConns(4)
	readDB.SetMaxIdleConns(4)
	readDB.SetConnMaxLifetime(5 * time.Minute)

	// Enable read_uncommitted on read connections for snapshot isolation without blocking
	if _, err := readDB.Exec("PRAGMA read_uncommitted = true"); err != nil {
		readDB.Close()
		db.Close()
		return nil, fmt.Errorf("manifest: failed to set read_uncommitted pragma: %w", err)
	}

	catalog := &SQLiteCatalog{
		db:            db,
		readDB:        readDB,
		dbPath:        dbPath,
		findStmtCache: make(map[string]*sql.Stmt),
	}

	// Initialize schema (uses write connection)
	if err := catalog.initSchema(); err != nil {
		readDB.Close()
		db.Close()
		return nil, fmt.Errorf("manifest: failed to initialize schema: %w", err)
	}

	// Prepare cached insert statement on write connection
	insertStmt, err := db.Prepare(`
		INSERT INTO partitions (
			partition_id, partition_key, object_path,
			min_user_id, max_user_id,
			min_event_time, max_event_time,
			min_tenant_id, max_tenant_id,
			row_count, size_bytes, schema_version, created_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		readDB.Close()
		db.Close()
		return nil, fmt.Errorf("manifest: failed to prepare insert statement: %w", err)
	}
	catalog.insertPartitionStmt = insertStmt

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

	if err := c.insertPartition(ctx, info, objectPath); err != nil {
		return err
	}

	// Check partition count thresholds and warn operators
	c.logPartitionCountThreshold(ctx)

	return nil
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

	_, err := c.insertPartitionStmt.ExecContext(ctx,
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

	row := c.readDB.QueryRowContext(ctx, query, partitionID)
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

	// Use cached prepared statement for this query pattern
	stmt, err := c.getOrPrepareStmt(query)
	if err != nil {
		return nil, fmt.Errorf("manifest: failed to prepare find query: %w", err)
	}

	rows, err := stmt.QueryContext(ctx, args...)
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

// getOrPrepareStmt returns a cached prepared statement or creates one.
func (c *SQLiteCatalog) getOrPrepareStmt(query string) (*sql.Stmt, error) {
	c.findStmtMu.RLock()
	if stmt, ok := c.findStmtCache[query]; ok {
		c.findStmtMu.RUnlock()
		return stmt, nil
	}
	c.findStmtMu.RUnlock()

	c.findStmtMu.Lock()
	defer c.findStmtMu.Unlock()

	// Double-check after acquiring write lock
	if stmt, ok := c.findStmtCache[query]; ok {
		return stmt, nil
	}

	stmt, err := c.readDB.Prepare(query)
	if err != nil {
		return nil, err
	}
	c.findStmtCache[query] = stmt
	return stmt, nil
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

	// Derive partition key prefix bounds from event_time predicates
	keyClause, keyArgs := c.derivePartitionKeyBounds(predicates)
	if keyClause != "" {
		baseQuery += " AND " + keyClause
		args = append(args, keyArgs...)
	}

	for _, pred := range predicates {
		clause, predArgs := c.buildPredicateClause(pred)
		if clause != "" {
			baseQuery += " AND " + clause
			args = append(args, predArgs...)
		}
	}

	return baseQuery, args
}

// derivePartitionKeyBounds extracts time bounds from event_time predicates
// and converts them to YYYYMMDD partition key bounds for prefix pruning.
func (c *SQLiteCatalog) derivePartitionKeyBounds(predicates []Predicate) (string, []interface{}) {
	var minTime, maxTime *int64

	for _, pred := range predicates {
		if pred.Column != "event_time" {
			continue
		}
		switch pred.Operator {
		case "=":
			if v, ok := pred.Value.(int64); ok {
				minTime = &v
				maxTime = &v
			}
		case ">=", ">":
			if v, ok := pred.Value.(int64); ok {
				if minTime == nil || v < *minTime {
					minTime = &v
				}
			}
		case "<=", "<":
			if v, ok := pred.Value.(int64); ok {
				if maxTime == nil || v > *maxTime {
					maxTime = &v
				}
			}
		case "BETWEEN":
			if len(pred.Values) >= 2 {
				if v, ok := pred.Values[0].(int64); ok {
					if minTime == nil || v < *minTime {
						minTime = &v
					}
				}
				if v, ok := pred.Values[1].(int64); ok {
					if maxTime == nil || v > *maxTime {
						maxTime = &v
					}
				}
			}
		}
	}

	if minTime == nil && maxTime == nil {
		return "", nil
	}

	// Use wide bounds for open-ended ranges
	if minTime == nil {
		minTime = maxTime
	}
	if maxTime == nil {
		maxTime = minTime
	}

	minKey := time.Unix(*minTime, 0).UTC().Format("20060102")
	maxKey := time.Unix(*maxTime, 0).UTC().Format("20060102")
	return "partition_key BETWEEN ? AND ?", []interface{}{minKey, maxKey}
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

	rows, err := c.readDB.QueryContext(ctx, query, key, maxSize)
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

// FindHighestIdempotencyLSN finds the highest LSN from idempotency keys matching the given prefix.
// This is used by WAL recovery to determine which entries have already been flushed.
// The prefix should be "wal-lsn-" to find WAL-flushed entries.
// It queries ALL keys matching prefix, parses each numerically, and returns the maximum.
// DO NOT use ORDER BY key DESC LIMIT 1 - lexicographic string ordering is wrong for numeric LSNs.
func (c *SQLiteCatalog) FindHighestIdempotencyLSN(ctx context.Context, prefix string) (uint64, error) {
	rows, err := c.readDB.QueryContext(ctx,
		"SELECT key FROM idempotency_keys WHERE key LIKE ?",
		prefix+"%",
	)
	if err != nil {
		return 0, fmt.Errorf("manifest: failed to query idempotency keys: %w", err)
	}
	defer rows.Close()

	var highestLSN uint64 = 0
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			return 0, fmt.Errorf("manifest: failed to scan idempotency key: %w", err)
		}

		// Parse the LSN from the key (format: "prefix{lsn}")
		lsn, err := parseIdempotencyKey(key)
		if err != nil {
			// Skip keys that don't match expected format
			continue
		}

		if lsn > highestLSN {
			highestLSN = lsn
		}
	}

	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("manifest: error iterating idempotency keys: %w", err)
	}

	return highestLSN, nil
}

// parseIdempotencyKey extracts the LSN from an idempotency key like "wal-lsn-42".
func parseIdempotencyKey(key string) (uint64, error) {
	// Key format: "wal-lsn-{lsn}"
	prefix := "wal-lsn-"
	if !strings.HasPrefix(key, prefix) {
		return 0, fmt.Errorf("invalid idempotency key format: %s", key)
	}

	lsnStr := strings.TrimPrefix(key, prefix)
	var lsn uint64
	_, err := fmt.Sscanf(lsnStr, "%d", &lsn)
	if err != nil {
		return 0, fmt.Errorf("failed to parse LSN from key %s: %w", key, err)
	}

	return lsn, nil
}

// RegisterIndexPartition registers a new index partition in the manifest.
func (c *SQLiteCatalog) RegisterIndexPartition(ctx context.Context, info *index.IndexPartitionInfo) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	_, err := c.db.ExecContext(ctx,
		`INSERT INTO index_partitions (
			index_id, collection, column_name, bucket_id,
			object_path, entry_count, size_bytes, min_time, max_time, created_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		info.IndexID, info.Collection, info.Column, info.BucketID,
		info.ObjectPath, info.EntryCount, info.SizeBytes,
		info.CoveredRange.Min, info.CoveredRange.Max, info.CreatedAt.Unix(),
	)
	if err != nil {
		return fmt.Errorf("manifest: failed to register index partition: %w", err)
	}

	return nil
}

// FindIndexPartition finds an index partition by collection, column, and bucket ID.
func (c *SQLiteCatalog) FindIndexPartition(ctx context.Context, collection, column string, bucketID int) (*index.IndexPartitionInfo, error) {
	query := `
		SELECT index_id, collection, column_name, bucket_id,
			object_path, entry_count, size_bytes, min_time, max_time, created_at
		FROM index_partitions
		WHERE collection = ? AND column_name = ? AND bucket_id = ?`

	row := c.readDB.QueryRowContext(ctx, query, collection, column, bucketID)

	var info index.IndexPartitionInfo
	var createdAtUnix int64

	err := row.Scan(
		&info.IndexID, &info.Collection, &info.Column, &info.BucketID,
		&info.ObjectPath, &info.EntryCount, &info.SizeBytes,
		&info.CoveredRange.Min, &info.CoveredRange.Max, &createdAtUnix,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("manifest: failed to scan index partition: %w", err)
	}

	info.CreatedAt = time.Unix(createdAtUnix, 0)
	return &info, nil
}

// ListIndexes lists all indexed columns for a given collection.
func (c *SQLiteCatalog) ListIndexes(ctx context.Context, collection string) ([]string, error) {
	query := `SELECT DISTINCT column_name FROM index_partitions WHERE collection = ?`

	rows, err := c.readDB.QueryContext(ctx, query, collection)
	if err != nil {
		return nil, fmt.Errorf("manifest: failed to query indexes: %w", err)
	}
	defer rows.Close()

	var columns []string
	for rows.Next() {
		var col string
		if err := rows.Scan(&col); err != nil {
			return nil, fmt.Errorf("manifest: failed to scan column: %w", err)
		}
		columns = append(columns, col)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("manifest: error iterating indexes: %w", err)
	}

	return columns, nil
}

// DeleteIndex deletes all index partitions for a given collection and column.
// Returns the object paths that were deleted (caller should delete from S3).
func (c *SQLiteCatalog) DeleteIndex(ctx context.Context, collection, column string) ([]string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// First, collect the object paths to return
	var objectPaths []string
	rows, err := c.db.QueryContext(ctx,
		`SELECT object_path FROM index_partitions WHERE collection = ? AND column_name = ?`,
		collection, column,
	)
	if err != nil {
		return nil, fmt.Errorf("manifest: failed to query index paths: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var path string
		if err := rows.Scan(&path); err != nil {
			return nil, fmt.Errorf("manifest: failed to scan index path: %w", err)
		}
		objectPaths = append(objectPaths, path)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("manifest: error iterating index paths: %w", err)
	}

	if len(objectPaths) == 0 {
		return nil, fmt.Errorf("manifest: no index found for collection=%s, column=%s", collection, column)
	}

	// Now delete from the database
	result, err := c.db.ExecContext(ctx,
		`DELETE FROM index_partitions WHERE collection = ? AND column_name = ?`,
		collection, column,
	)
	if err != nil {
		return nil, fmt.Errorf("manifest: failed to delete index: %w", err)
	}

	// Verify rows were affected
	rowsAffected, _ := result.RowsAffected()
	if rowsAffected == 0 {
		return nil, fmt.Errorf("manifest: no index found for collection=%s, column=%s", collection, column)
	}

	return objectPaths, nil
}

// Close closes the catalog database connections.
func (c *SQLiteCatalog) Close() error {
	// Close cached prepared statements (on write connection)
	if c.insertPartitionStmt != nil {
		c.insertPartitionStmt.Close()
	}
	// Close cached find statements (on read connection)
	c.findStmtMu.Lock()
	for _, stmt := range c.findStmtCache {
		stmt.Close()
	}
	c.findStmtCache = nil
	c.findStmtMu.Unlock()

	// Close read connection first, then write connection
	if err := c.readDB.Close(); err != nil {
		c.db.Close()
		return err
	}
	return c.db.Close()
}

// GetPartitionCount returns the total number of active (non-compacted) partitions.
func (c *SQLiteCatalog) GetPartitionCount(ctx context.Context) (int64, error) {
	var count int64
	err := c.readDB.QueryRowContext(ctx,
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
	err := c.readDB.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM partitions WHERE compacted_into IS NULL AND partition_key = ?",
		key,
	).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("manifest: failed to count partitions by key: %w", err)
	}
	return count, nil
}

// RunAnalyze runs ANALYZE to update SQLite query planner statistics.
// Should be called after bulk inserts to keep index statistics current.
func (c *SQLiteCatalog) RunAnalyze(ctx context.Context) error {
	_, err := c.db.ExecContext(ctx, AnalyzeSQL)
	if err != nil {
		return fmt.Errorf("manifest: failed to run ANALYZE: %w", err)
	}
	return nil
}

// FindCompactedPartitions returns all partitions that have a non-NULL compacted_into value.
// This is used by crash recovery to detect incomplete compactions.
func (c *SQLiteCatalog) FindCompactedPartitions(ctx context.Context) ([]*PartitionRecord, error) {
	query := `
		SELECT partition_id, partition_key, object_path,
			min_user_id, max_user_id,
			min_event_time, max_event_time,
			min_tenant_id, max_tenant_id,
			row_count, size_bytes, schema_version, created_at, compacted_into
		FROM partitions
		WHERE compacted_into IS NOT NULL`

	rows, err := c.readDB.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("manifest: failed to query compacted partitions: %w", err)
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
		return nil, fmt.Errorf("manifest: error iterating compacted partitions: %w", err)
	}
	return records, nil
}

// ResetCompactedInto sets compacted_into to NULL for the given partition IDs.
// This is used by crash recovery to rollback incomplete compactions.
func (c *SQLiteCatalog) ResetCompactedInto(ctx context.Context, partitionIDs []string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("manifest: failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	for _, id := range partitionIDs {
		if _, err := tx.ExecContext(ctx,
			"UPDATE partitions SET compacted_into = NULL WHERE partition_id = ?", id); err != nil {
			return fmt.Errorf("manifest: failed to reset compacted_into for %s: %w", id, err)
		}
	}

	return tx.Commit()
}


// CompactionIntent represents an in-progress compaction operation.
type CompactionIntent struct {
	TargetPartitionID string
	SourcePartitionIDs []string
	TargetObjectPath  string
	TargetMetaPath    string
	CreatedAt         time.Time
}

// WriteCompactionIntent records a compaction intent (Phase 1 of two-phase commit).
func (c *SQLiteCatalog) WriteCompactionIntent(ctx context.Context, intent *CompactionIntent) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	sourceIDsJSON, err := json.Marshal(intent.SourcePartitionIDs)
	if err != nil {
		return fmt.Errorf("manifest: failed to marshal source IDs: %w", err)
	}

	_, err = c.db.ExecContext(ctx,
		`INSERT OR REPLACE INTO compaction_intents (target_partition_id, source_partition_ids, target_object_path, target_meta_path, created_at)
		 VALUES (?, ?, ?, ?, ?)`,
		intent.TargetPartitionID, string(sourceIDsJSON), intent.TargetObjectPath, intent.TargetMetaPath, intent.CreatedAt.Unix())
	if err != nil {
		return fmt.Errorf("manifest: failed to write compaction intent: %w", err)
	}
	return nil
}

// CompleteCompaction executes Phase 2 of two-phase commit:
// registers the target partition, marks sources as compacted, and deletes the intent
// all within a single transaction.
func (c *SQLiteCatalog) CompleteCompaction(ctx context.Context, info *partition.PartitionInfo, objectPath string, sourceIDs []string, intentTargetID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("manifest: failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Register the target partition
	if err := c.insertPartitionTx(ctx, tx, info, objectPath); err != nil {
		return fmt.Errorf("manifest: failed to register compacted partition: %w", err)
	}

	// Mark source partitions as compacted
	for _, sourceID := range sourceIDs {
		result, err := tx.ExecContext(ctx,
			"UPDATE partitions SET compacted_into = ? WHERE partition_id = ? AND compacted_into IS NULL",
			info.PartitionID, sourceID)
		if err != nil {
			return fmt.Errorf("manifest: failed to mark partition %s as compacted: %w", sourceID, err)
		}
		rowsAffected, _ := result.RowsAffected()
		if rowsAffected == 0 {
			return fmt.Errorf("manifest: partition %s not found or already compacted", sourceID)
		}
	}

	// Delete the compaction intent
	if _, err := tx.ExecContext(ctx,
		"DELETE FROM compaction_intents WHERE target_partition_id = ?", intentTargetID); err != nil {
		return fmt.Errorf("manifest: failed to delete compaction intent: %w", err)
	}

	return tx.Commit()
}

// FindCompactionIntents returns all pending compaction intents.
func (c *SQLiteCatalog) FindCompactionIntents(ctx context.Context) ([]*CompactionIntent, error) {
	rows, err := c.readDB.QueryContext(ctx,
		"SELECT target_partition_id, source_partition_ids, target_object_path, target_meta_path, created_at FROM compaction_intents")
	if err != nil {
		return nil, fmt.Errorf("manifest: failed to query compaction intents: %w", err)
	}
	defer rows.Close()

	var intents []*CompactionIntent
	for rows.Next() {
		var intent CompactionIntent
		var sourceIDsJSON string
		var createdAtUnix int64
		if err := rows.Scan(&intent.TargetPartitionID, &sourceIDsJSON, &intent.TargetObjectPath, &intent.TargetMetaPath, &createdAtUnix); err != nil {
			return nil, fmt.Errorf("manifest: failed to scan compaction intent: %w", err)
		}
		if err := json.Unmarshal([]byte(sourceIDsJSON), &intent.SourcePartitionIDs); err != nil {
			return nil, fmt.Errorf("manifest: failed to unmarshal source IDs: %w", err)
		}
		intent.CreatedAt = time.Unix(createdAtUnix, 0)
		intents = append(intents, &intent)
	}
	return intents, rows.Err()
}

// DeleteCompactionIntent removes a compaction intent record.
func (c *SQLiteCatalog) DeleteCompactionIntent(ctx context.Context, targetPartitionID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	_, err := c.db.ExecContext(ctx,
		"DELETE FROM compaction_intents WHERE target_partition_id = ?", targetPartitionID)
	if err != nil {
		return fmt.Errorf("manifest: failed to delete compaction intent: %w", err)
	}
	return nil
}

// FindPartitionsByKeyPrefix returns active partitions whose partition_key starts with the given prefix.
// This enables scoping catalog queries to a shard boundary for future sharding support.
func (c *SQLiteCatalog) FindPartitionsByKeyPrefix(ctx context.Context, prefix string) ([]*PartitionRecord, error) {
	query := `
		SELECT partition_id, partition_key, object_path,
			min_user_id, max_user_id,
			min_event_time, max_event_time,
			min_tenant_id, max_tenant_id,
			row_count, size_bytes, schema_version, created_at, compacted_into
		FROM partitions
		WHERE compacted_into IS NULL
			AND partition_key >= ?
			AND partition_key < ?
		ORDER BY partition_key, created_at ASC`

	// Compute the exclusive upper bound by incrementing the last byte of the prefix.
	upperBound := prefix + "\xff"

	rows, err := c.readDB.QueryContext(ctx, query, prefix, upperBound)
	if err != nil {
		return nil, fmt.Errorf("manifest: failed to query partitions by key prefix: %w", err)
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
		return nil, fmt.Errorf("manifest: error iterating partitions by key prefix: %w", err)
	}
	return records, nil
}

// GetDistinctPartitionKeys returns all distinct partition key values for active (non-compacted) partitions.
// This is useful for shard discovery and enumerating partition key prefixes.
func (c *SQLiteCatalog) GetDistinctPartitionKeys(ctx context.Context) ([]string, error) {
	rows, err := c.readDB.QueryContext(ctx,
		"SELECT DISTINCT partition_key FROM partitions WHERE compacted_into IS NULL ORDER BY partition_key")
	if err != nil {
		return nil, fmt.Errorf("manifest: failed to query distinct partition keys: %w", err)
	}
	defer rows.Close()

	var keys []string
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			return nil, fmt.Errorf("manifest: failed to scan partition key: %w", err)
		}
		keys = append(keys, key)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("manifest: error iterating partition keys: %w", err)
	}
	return keys, nil
}


// GetPartitionCountByKeyPrefix returns the count of active partitions whose key starts with the given prefix.
func (c *SQLiteCatalog) GetPartitionCountByKeyPrefix(ctx context.Context, prefix string) (int64, error) {
	upperBound := prefix + "\xff"
	var count int64
	err := c.readDB.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM partitions WHERE compacted_into IS NULL AND partition_key >= ? AND partition_key < ?",
		prefix, upperBound,
	).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("manifest: failed to count partitions by key prefix: %w", err)
	}
	return count, nil
}

// GetTotalPartitionCount returns the total number of active partitions and logs a warning
// when the count exceeds 500K, indicating the manifest may need sharding.
func (c *SQLiteCatalog) GetTotalPartitionCount(ctx context.Context) (int64, error) {
	count, err := c.GetPartitionCount(ctx)
	if err != nil {
		return 0, err
	}
	if count > 500000 {
		log.Printf("[WARN] manifest: total active partition count (%d) exceeds 500K — consider sharding the manifest catalog", count)
	}
	return count, nil
}


// partitionCountThresholds defines the partition count levels at which warnings are emitted.
var partitionCountThresholds = []int64{1000000, 500000, 100000}

// logPartitionCountThreshold checks the total active partition count and logs a warning
// when it crosses 100K, 500K, or 1M thresholds. Called after each RegisterPartition.
func (c *SQLiteCatalog) logPartitionCountThreshold(ctx context.Context) {
	var count int64
	err := c.db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM partitions WHERE compacted_into IS NULL",
	).Scan(&count)
	if err != nil {
		return // best-effort; don't fail the write path
	}
	for _, threshold := range partitionCountThresholds {
		if count >= threshold {
			log.Printf("[WARN] manifest: active partition count (%d) has crossed %dK threshold — plan for manifest sharding", count, threshold/1000)
			return
		}
	}
}

