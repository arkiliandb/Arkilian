package compaction

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/arkilian/arkilian/internal/bloom"
	"github.com/arkilian/arkilian/internal/manifest"
	"github.com/arkilian/arkilian/internal/partition"
	"github.com/arkilian/arkilian/internal/storage"
	"github.com/arkilian/arkilian/pkg/types"
	"github.com/golang/snappy"
	"github.com/google/uuid"
	_ "github.com/mattn/go-sqlite3"
)

// Merger handles merging multiple source partitions into a single compacted partition.
type Merger struct {
	storage  storage.ObjectStorage
	workDir  string
	targetFPR float64
}

// NewMerger creates a new partition merger.
func NewMerger(store storage.ObjectStorage, workDir string) *Merger {
	return &Merger{
		storage:  store,
		workDir:  workDir,
		targetFPR: 0.01,
	}
}

// MergeResult contains the output of a merge operation.
type MergeResult struct {
	PartitionInfo *partition.PartitionInfo
	Metadata      *partition.MetadataSidecar
	SQLitePath    string
	MetadataPath  string
	TotalRows     int64
	SourceIDs     []string
}

// Merge downloads source partitions, merges their data sorted by primary key,
// and produces a new compacted partition with updated statistics and bloom filters.
func (m *Merger) Merge(ctx context.Context, group *CandidateGroup) (*MergeResult, error) {
	if len(group.Partitions) < 2 {
		return nil, fmt.Errorf("compaction: need at least 2 partitions to merge, got %d", len(group.Partitions))
	}

	// Create work directory for this merge
	mergeDir := filepath.Join(m.workDir, fmt.Sprintf("merge_%s", uuid.New().String()[:8]))
	if err := os.MkdirAll(mergeDir, 0755); err != nil {
		return nil, fmt.Errorf("compaction: failed to create merge directory: %w", err)
	}

	// Download all source partitions
	localPaths, err := m.downloadSources(ctx, group.Partitions, mergeDir)
	if err != nil {
		return nil, fmt.Errorf("compaction: failed to download sources: %w", err)
	}

	// Read all rows from source partitions, sorted by primary key
	allRows, err := m.readAndMergeRows(ctx, localPaths)
	if err != nil {
		return nil, fmt.Errorf("compaction: failed to read source rows: %w", err)
	}

	// Generate new partition ID
	partitionID := fmt.Sprintf("events:%s:c_%s", group.PartitionKey, uuid.New().String()[:8])
	createdAt := time.Now()

	// Build the compacted SQLite partition
	sqlitePath := filepath.Join(mergeDir, fmt.Sprintf("%s.sqlite", partitionID))
	sizeBytes, err := m.buildCompactedPartition(ctx, sqlitePath, allRows)
	if err != nil {
		return nil, fmt.Errorf("compaction: failed to build compacted partition: %w", err)
	}

	// Compute statistics
	stats := partition.NewStatsTracker()
	for _, row := range allRows {
		stats.Update(row)
	}
	minMaxStats := stats.GetMinMaxStats()

	// Determine schema version (use max from sources)
	schemaVersion := 1
	for _, p := range group.Partitions {
		if p.SchemaVersion > schemaVersion {
			schemaVersion = p.SchemaVersion
		}
	}

	info := &partition.PartitionInfo{
		PartitionID:   partitionID,
		PartitionKey:  group.PartitionKey,
		SQLitePath:    sqlitePath,
		RowCount:      int64(len(allRows)),
		SizeBytes:     sizeBytes,
		MinMaxStats:   minMaxStats,
		SchemaVersion: schemaVersion,
		CreatedAt:     createdAt,
	}

	// Generate metadata sidecar with bloom filters
	metaGen := partition.NewMetadataGenerator()
	metadata, err := metaGen.Generate(info, allRows)
	if err != nil {
		return nil, fmt.Errorf("compaction: failed to generate metadata: %w", err)
	}

	metadataPath := filepath.Join(mergeDir, fmt.Sprintf("%s.meta.json", partitionID))
	if err := metadata.WriteToFile(metadataPath); err != nil {
		return nil, fmt.Errorf("compaction: failed to write metadata: %w", err)
	}
	info.MetadataPath = metadataPath

	// Collect source IDs
	sourceIDs := make([]string, len(group.Partitions))
	for i, p := range group.Partitions {
		sourceIDs[i] = p.PartitionID
	}

	return &MergeResult{
		PartitionInfo: info,
		Metadata:      metadata,
		SQLitePath:    sqlitePath,
		MetadataPath:  metadataPath,
		TotalRows:     int64(len(allRows)),
		SourceIDs:     sourceIDs,
	}, nil
}


// downloadSources downloads all source partition SQLite files to the local work directory.
func (m *Merger) downloadSources(ctx context.Context, partitions []*manifest.PartitionRecord, destDir string) ([]string, error) {
	paths := make([]string, len(partitions))
	for i, p := range partitions {
		localPath := filepath.Join(destDir, fmt.Sprintf("source_%d.sqlite", i))
		if err := m.storage.Download(ctx, p.ObjectPath, localPath); err != nil {
			return nil, fmt.Errorf("failed to download partition %s: %w", p.PartitionID, err)
		}
		paths[i] = localPath
	}
	return paths, nil
}

// mergedRow holds a row with its raw event_id for sorting.
type mergedRow struct {
	row     types.Row
	eventID []byte
}

// readAndMergeRows reads all rows from source partitions and returns them sorted by primary key (event_id).
func (m *Merger) readAndMergeRows(ctx context.Context, localPaths []string) ([]types.Row, error) {
	var allRows []mergedRow

	for _, path := range localPaths {
		rows, err := m.readPartitionRows(ctx, path)
		if err != nil {
			return nil, fmt.Errorf("failed to read rows from %s: %w", path, err)
		}
		allRows = append(allRows, rows...)
	}

	// Sort by primary key (event_id) for minimal B-tree fragmentation
	sort.Slice(allRows, func(i, j int) bool {
		return compareBytesLess(allRows[i].eventID, allRows[j].eventID)
	})

	result := make([]types.Row, len(allRows))
	for i, mr := range allRows {
		result[i] = mr.row
	}
	return result, nil
}

// readPartitionRows reads all rows from a single SQLite partition file.
func (m *Merger) readPartitionRows(ctx context.Context, sqlitePath string) ([]mergedRow, error) {
	db, err := sql.Open("sqlite3", sqlitePath+"?mode=ro")
	if err != nil {
		return nil, fmt.Errorf("failed to open partition: %w", err)
	}
	defer db.Close()

	rows, err := db.QueryContext(ctx,
		"SELECT event_id, tenant_id, user_id, event_time, event_type, payload FROM events ORDER BY event_id")
	if err != nil {
		return nil, fmt.Errorf("failed to query events: %w", err)
	}
	defer rows.Close()

	var result []mergedRow
	for rows.Next() {
		var eventID []byte
		var tenantID, eventType string
		var userID, eventTime int64
		var compressedPayload []byte

		if err := rows.Scan(&eventID, &tenantID, &userID, &eventTime, &eventType, &compressedPayload); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		// Decompress payload
		decompressed, err := snappy.Decode(nil, compressedPayload)
		if err != nil {
			return nil, fmt.Errorf("failed to decompress payload: %w", err)
		}

		var payload map[string]interface{}
		if err := json.Unmarshal(decompressed, &payload); err != nil {
			return nil, fmt.Errorf("failed to unmarshal payload: %w", err)
		}

		result = append(result, mergedRow{
			eventID: eventID,
			row: types.Row{
				EventID:   eventID,
				TenantID:  tenantID,
				UserID:    userID,
				EventTime: eventTime,
				EventType: eventType,
				Payload:   payload,
			},
		})
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}

	return result, nil
}

// buildCompactedPartition creates a new SQLite partition file from merged rows.
// Returns the file size in bytes.
func (m *Merger) buildCompactedPartition(ctx context.Context, sqlitePath string, rows []types.Row) (int64, error) {
	db, err := sql.Open("sqlite3", sqlitePath)
	if err != nil {
		return 0, fmt.Errorf("failed to create database: %w", err)
	}
	defer db.Close()

	if _, err := db.ExecContext(ctx, "PRAGMA journal_mode=WAL"); err != nil {
		return 0, fmt.Errorf("failed to set journal mode: %w", err)
	}

	// Create events table with WITHOUT ROWID
	createSQL := `
		CREATE TABLE events (
			event_id BLOB PRIMARY KEY,
			tenant_id TEXT NOT NULL,
			user_id INTEGER NOT NULL,
			event_time INTEGER NOT NULL,
			event_type TEXT NOT NULL,
			payload BLOB NOT NULL
		) WITHOUT ROWID`
	if _, err := db.ExecContext(ctx, createSQL); err != nil {
		return 0, fmt.Errorf("failed to create table: %w", err)
	}

	// Create indexes
	indexes := []string{
		"CREATE INDEX idx_events_tenant_time ON events(tenant_id, event_time)",
		"CREATE INDEX idx_events_user_time ON events(user_id, event_time)",
	}
	for _, idx := range indexes {
		if _, err := db.ExecContext(ctx, idx); err != nil {
			return 0, fmt.Errorf("failed to create index: %w", err)
		}
	}

	// Insert rows
	stmt, err := db.PrepareContext(ctx,
		"INSERT INTO events (event_id, tenant_id, user_id, event_time, event_type, payload) VALUES (?, ?, ?, ?, ?, ?)")
	if err != nil {
		return 0, fmt.Errorf("failed to prepare insert: %w", err)
	}
	defer stmt.Close()

	for _, row := range rows {
		payloadJSON, err := json.Marshal(row.Payload)
		if err != nil {
			return 0, fmt.Errorf("failed to marshal payload: %w", err)
		}
		compressed := snappy.Encode(nil, payloadJSON)

		if _, err := stmt.ExecContext(ctx, row.EventID, row.TenantID, row.UserID, row.EventTime, row.EventType, compressed); err != nil {
			return 0, fmt.Errorf("failed to insert row: %w", err)
		}
	}

	// Create stats table
	statsSQL := `
		CREATE TABLE _arkilian_stats (
			table_name TEXT NOT NULL,
			column_name TEXT NOT NULL,
			null_count INTEGER,
			distinct_count INTEGER,
			min_value BLOB,
			max_value BLOB,
			PRIMARY KEY (table_name, column_name)
		) WITHOUT ROWID`
	if _, err := db.ExecContext(ctx, statsSQL); err != nil {
		return 0, fmt.Errorf("failed to create stats table: %w", err)
	}

	// Finalize: checkpoint WAL and switch to DELETE mode for immutability
	if _, err := db.ExecContext(ctx, "PRAGMA wal_checkpoint(TRUNCATE)"); err != nil {
		return 0, fmt.Errorf("failed to checkpoint: %w", err)
	}
	if _, err := db.ExecContext(ctx, "PRAGMA journal_mode=DELETE"); err != nil {
		return 0, fmt.Errorf("failed to set journal mode: %w", err)
	}

	if err := db.Close(); err != nil {
		return 0, fmt.Errorf("failed to close database: %w", err)
	}

	fileInfo, err := os.Stat(sqlitePath)
	if err != nil {
		return 0, fmt.Errorf("failed to stat file: %w", err)
	}

	return fileInfo.Size(), nil
}

// compareBytesLess returns true if a < b lexicographically.
func compareBytesLess(a, b []byte) bool {
	minLen := len(a)
	if len(b) < minLen {
		minLen = len(b)
	}
	for i := 0; i < minLen; i++ {
		if a[i] < b[i] {
			return true
		}
		if a[i] > b[i] {
			return false
		}
	}
	return len(a) < len(b)
}

// Cleanup removes temporary merge files.
func (m *Merger) Cleanup(mergeDir string) error {
	return os.RemoveAll(mergeDir)
}

// Ensure bloom import is used (referenced in metadata generation)
var _ = bloom.New
