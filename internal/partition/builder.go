// Package partition provides functionality for creating and managing SQLite micro-partitions.
package partition

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/arkilian/arkilian/pkg/types"
	"github.com/golang/snappy"
	"github.com/google/uuid"
	_ "github.com/mattn/go-sqlite3"
)

// PartitionBuilder creates SQLite micro-partitions from rows.
type PartitionBuilder interface {
	// Build creates a partition from rows, returns partition info and file paths
	Build(ctx context.Context, rows []types.Row, key types.PartitionKey) (*PartitionInfo, error)

	// BuildWithSchema creates a partition with explicit schema
	BuildWithSchema(ctx context.Context, rows []types.Row, key types.PartitionKey, schema types.Schema) (*PartitionInfo, error)
}

// PartitionInfo contains metadata about a created partition.
type PartitionInfo struct {
	PartitionID   string
	PartitionKey  string
	SQLitePath    string
	MetadataPath  string
	RowCount      int64
	SizeBytes     int64
	MinMaxStats   map[string]MinMax
	SchemaVersion int
	CreatedAt     time.Time
}

// MinMax holds min/max values for a column.
type MinMax struct {
	Min interface{}
	Max interface{}
}

// Builder implements PartitionBuilder.
type Builder struct {
	outputDir     string
	ulidGenerator *types.ULIDGenerator
}

// NewBuilder creates a new partition builder.
func NewBuilder(outputDir string) *Builder {
	return &Builder{
		outputDir:     outputDir,
		ulidGenerator: types.NewULIDGenerator(),
	}
}

// Build creates a partition from rows using the default schema.
func (b *Builder) Build(ctx context.Context, rows []types.Row, key types.PartitionKey) (*PartitionInfo, error) {
	return b.BuildWithSchema(ctx, rows, key, DefaultSchema())
}


// BuildWithSchema creates a partition with explicit schema.
func (b *Builder) BuildWithSchema(ctx context.Context, rows []types.Row, key types.PartitionKey, schema types.Schema) (*PartitionInfo, error) {
	if len(rows) == 0 {
		return nil, fmt.Errorf("partition: cannot build partition with empty rows")
	}

	// Validate rows against schema
	validator := NewSchemaValidator(schema)
	if err := validator.Validate(rows); err != nil {
		return nil, fmt.Errorf("partition: validation failed: %w", err)
	}

	// Generate partition ID
	partitionID := fmt.Sprintf("events:%s:%s", key.Value, uuid.New().String()[:8])
	createdAt := time.Now()

	// Create output directory if needed
	if err := os.MkdirAll(b.outputDir, 0755); err != nil {
		return nil, fmt.Errorf("partition: failed to create output directory: %w", err)
	}

	// Create SQLite file path
	sqlitePath := filepath.Join(b.outputDir, fmt.Sprintf("%s.sqlite", partitionID))
	sqlitePath = filepath.Clean(sqlitePath)

	// Create SQLite database with WITHOUT ROWID optimization
	db, err := sql.Open("sqlite3", sqlitePath)
	if err != nil {
		return nil, fmt.Errorf("partition: failed to create SQLite database: %w", err)
	}
	defer db.Close()

	// Enable WAL mode for better write performance during build
	if _, err := db.ExecContext(ctx, "PRAGMA journal_mode=WAL"); err != nil {
		return nil, fmt.Errorf("partition: failed to set journal mode: %w", err)
	}

	// Create events table with WITHOUT ROWID optimization
	createTableSQL := `
		CREATE TABLE events (
			event_id BLOB PRIMARY KEY,
			tenant_id TEXT NOT NULL,
			user_id INTEGER NOT NULL,
			event_time INTEGER NOT NULL,
			event_type TEXT NOT NULL,
			payload BLOB NOT NULL
		) WITHOUT ROWID
	`
	if _, err := db.ExecContext(ctx, createTableSQL); err != nil {
		return nil, fmt.Errorf("partition: failed to create events table: %w", err)
	}

	// Create optimized indexes for common query patterns
	indexes := []string{
		"CREATE INDEX idx_events_tenant_time ON events(tenant_id, event_time)",
		"CREATE INDEX idx_events_user_time ON events(user_id, event_time)",
	}
	for _, idx := range indexes {
		if _, err := db.ExecContext(ctx, idx); err != nil {
			return nil, fmt.Errorf("partition: failed to create index: %w", err)
		}
	}

	// Prepare insert statement
	insertSQL := `INSERT INTO events (event_id, tenant_id, user_id, event_time, event_type, payload) VALUES (?, ?, ?, ?, ?, ?)`
	stmt, err := db.PrepareContext(ctx, insertSQL)
	if err != nil {
		return nil, fmt.Errorf("partition: failed to prepare insert statement: %w", err)
	}
	defer stmt.Close()

	// Initialize statistics tracker
	stats := NewStatsTracker()

	// Insert rows with Snappy compression for payloads
	for _, row := range rows {
		// Compress payload using Snappy
		payloadJSON, err := json.Marshal(row.Payload)
		if err != nil {
			return nil, fmt.Errorf("partition: failed to marshal payload: %w", err)
		}
		compressedPayload := snappy.Encode(nil, payloadJSON)

		// Use provided event_id or generate new ULID
		eventID := row.EventID
		if len(eventID) == 0 {
			ulid, err := b.ulidGenerator.Generate()
			if err != nil {
				return nil, fmt.Errorf("partition: failed to generate ULID: %w", err)
			}
			eventID = ulid.Bytes()
		}

		// Insert row
		if _, err := stmt.ExecContext(ctx, eventID, row.TenantID, row.UserID, row.EventTime, row.EventType, compressedPayload); err != nil {
			return nil, fmt.Errorf("partition: failed to insert row: %w", err)
		}

		// Update statistics
		stats.Update(row)
	}

	// Create internal statistics table
	statsTableSQL := `
		CREATE TABLE _arkilian_stats (
			table_name TEXT NOT NULL,
			column_name TEXT NOT NULL,
			null_count INTEGER,
			distinct_count INTEGER,
			min_value BLOB,
			max_value BLOB,
			PRIMARY KEY (table_name, column_name)
		) WITHOUT ROWID
	`
	if _, err := db.ExecContext(ctx, statsTableSQL); err != nil {
		return nil, fmt.Errorf("partition: failed to create stats table: %w", err)
	}

	// Checkpoint WAL and switch to DELETE mode for immutability
	if _, err := db.ExecContext(ctx, "PRAGMA wal_checkpoint(TRUNCATE)"); err != nil {
		return nil, fmt.Errorf("partition: failed to checkpoint WAL: %w", err)
	}
	if _, err := db.ExecContext(ctx, "PRAGMA journal_mode=DELETE"); err != nil {
		return nil, fmt.Errorf("partition: failed to set journal mode to DELETE: %w", err)
	}

	// Close database to finalize
	if err := db.Close(); err != nil {
		return nil, fmt.Errorf("partition: failed to close database: %w", err)
	}

	// Get file size
	fileInfo, err := os.Stat(sqlitePath)
	if err != nil {
		return nil, fmt.Errorf("partition: failed to stat SQLite file: %w", err)
	}

	// Build partition info
	info := &PartitionInfo{
		PartitionID:   partitionID,
		PartitionKey:  key.Value,
		SQLitePath:    sqlitePath,
		RowCount:      int64(len(rows)),
		SizeBytes:     fileInfo.Size(),
		MinMaxStats:   stats.GetMinMaxStats(),
		SchemaVersion: schema.Version,
		CreatedAt:     createdAt,
	}

	return info, nil
}

// DefaultSchema returns the default schema for events table.
func DefaultSchema() types.Schema {
	return types.Schema{
		Version: 1,
		Columns: []types.ColumnDef{
			{Name: "event_id", Type: "BLOB", Nullable: false, PrimaryKey: true},
			{Name: "tenant_id", Type: "TEXT", Nullable: false},
			{Name: "user_id", Type: "INTEGER", Nullable: false},
			{Name: "event_time", Type: "INTEGER", Nullable: false},
			{Name: "event_type", Type: "TEXT", Nullable: false},
			{Name: "payload", Type: "BLOB", Nullable: false},
		},
		Indexes: []types.IndexDef{
			{Name: "idx_events_tenant_time", Columns: []string{"tenant_id", "event_time"}},
			{Name: "idx_events_user_time", Columns: []string{"user_id", "event_time"}},
		},
	}
}
