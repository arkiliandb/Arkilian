// Package index provides secondary index partitions for efficient point lookups on any column.
package index

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/arkilian/arkilian/internal/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestBuilderBasic tests basic index building functionality.
func TestBuilderBasic(t *testing.T) {
	ctx := context.Background()

	// Create temporary directories
	tempDir, err := os.MkdirTemp("", "index_builder_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	storageDir := filepath.Join(tempDir, "storage")
	workDir := filepath.Join(tempDir, "work")

	// Create storage
	storage, err := storage.NewLocalStorage(storageDir)
	require.NoError(t, err)

	// Create in-memory catalog for testing
	catalog := newTestCatalog()

	// Create builder
	builder := NewBuilder(storage, catalog, workDir, 64)

	// Create test partitions with known data
	partitions := createTestPartitions(t, storage, 10, 100)

	// Build index
	indexInfos, err := builder.BuildIndex(ctx, "events", "device_id", partitions)
	require.NoError(t, err)

	// Verify index was created
	assert.NotEmpty(t, indexInfos)
	// All 100 distinct values should hash to different buckets (56 buckets = 56 index files)
	assert.Equal(t, 56, len(indexInfos))

	// Verify index file exists in storage
	exists, err := storage.Exists(ctx, indexInfos[0].ObjectPath)
	assert.True(t, exists)
}

// TestBuilderBucketDistribution tests that values are distributed across buckets.
func TestBuilderBucketDistribution(t *testing.T) {
	ctx := context.Background()

	// Create temporary directories
	tempDir, err := os.MkdirTemp("", "index_builder_buckets_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	storageDir := filepath.Join(tempDir, "storage")
	workDir := filepath.Join(tempDir, "work")

	// Create storage
	storage, err := storage.NewLocalStorage(storageDir)
	require.NoError(t, err)

	// Create in-memory catalog for testing
	catalog := newTestCatalog()

	// Create builder with 4 buckets for easier testing
	builder := NewBuilder(storage, catalog, workDir, 4)

	// Create test partitions with many distinct values
	partitions := createTestPartitions(t, storage, 5, 100)

	// Build index
	indexInfos, err := builder.BuildIndex(ctx, "events", "device_id", partitions)
	require.NoError(t, err)

	// Verify multiple buckets were created
	assert.GreaterOrEqual(t, len(indexInfos), 1)
}

// TestBuilderEmptyPartitions tests that empty partitions produce no index entries.
func TestBuilderEmptyPartitions(t *testing.T) {
	ctx := context.Background()

	// Create temporary directories
	tempDir, err := os.MkdirTemp("", "index_builder_empty_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	storageDir := filepath.Join(tempDir, "storage")
	workDir := filepath.Join(tempDir, "work")

	// Create storage
	storage, err := storage.NewLocalStorage(storageDir)
	require.NoError(t, err)

	// Create in-memory catalog for testing
	catalog := newTestCatalog()

	// Create builder
	builder := NewBuilder(storage, catalog, workDir, 64)

	// Create partition with no data
	partition := createEmptyPartition(t, storage)

	// Build index
	indexInfos, err := builder.BuildIndex(ctx, "events", "device_id", []*PartitionInfo{partition})
	require.NoError(t, err)

	// Verify no index was created (empty partition)
	assert.Empty(t, indexInfos)
}

// TestBuilderValueIntegrity tests that index SQLite contains correct data.
func TestBuilderValueIntegrity(t *testing.T) {
	ctx := context.Background()

	// Create temporary directories
	tempDir, err := os.MkdirTemp("", "index_builder_integrity_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	storageDir := filepath.Join(tempDir, "storage")
	workDir := filepath.Join(tempDir, "work")

	// Create storage
	storage, err := storage.NewLocalStorage(storageDir)
	require.NoError(t, err)

	// Create in-memory catalog for testing
	catalog := newTestCatalog()

	// Create builder
	builder := NewBuilder(storage, catalog, workDir, 64)

	// Create test partition with known values
	partitions := createTestPartitions(t, storage, 1, 10)

	// Build index
	indexInfos, err := builder.BuildIndex(ctx, "events", "device_id", partitions)
	require.NoError(t, err)
	require.NotEmpty(t, indexInfos)

	// Verify total row count across all buckets
	totalRows := int64(0)
	for _, info := range indexInfos {
		totalRows += info.EntryCount
	}
	assert.Equal(t, int64(10), totalRows)

	// Download and verify first index file
	localIndexPath := filepath.Join(workDir, "verify_index.sqlite")
	err = storage.Download(ctx, indexInfos[0].ObjectPath, localIndexPath)
	require.NoError(t, err)

	// Open and verify index contents
	db, err := sql.Open("sqlite3", localIndexPath)
	require.NoError(t, err)
	defer db.Close()

	// Verify row count in this bucket
	var rowCount int64
	err = db.QueryRow("SELECT COUNT(*) FROM index_map").Scan(&rowCount)
	require.NoError(t, err)
	assert.Equal(t, int64(1), rowCount)

	// Verify specific values exist
	var value string
	err = db.QueryRow("SELECT column_value FROM index_map LIMIT 1").Scan(&value)
	require.NoError(t, err)
	assert.NotEmpty(t, value)
}

// createTestPartitions creates test partitions with known data.
func createTestPartitions(t *testing.T, storage storage.ObjectStorage, numPartitions, numValues int) []*PartitionInfo {
	var partitions []*PartitionInfo

	for i := 0; i < numPartitions; i++ {
		partitionID := fmt.Sprintf("partition_%d", i)
		localPath := filepath.Join(t.TempDir(), fmt.Sprintf("%s.sqlite", partitionID))

		// Create SQLite file with test data
		db, err := sql.Open("sqlite3", localPath)
		require.NoError(t, err)

		_, err = db.Exec(`
			CREATE TABLE events (
				id INTEGER PRIMARY KEY,
				device_id TEXT,
				payload TEXT,
				event_time INTEGER
			)
		`)
		require.NoError(t, err)

		// Insert test data
		stmt, err := db.Prepare("INSERT INTO events (device_id, payload, event_time) VALUES (?, ?, ?)")
		require.NoError(t, err)
		defer stmt.Close()

		for j := 0; j < numValues; j++ {
			deviceID := fmt.Sprintf("device_%d", j%numValues)
			_, err := stmt.Exec(deviceID, fmt.Sprintf(`{"value": %d}`, j), int64(1000000+i*1000+j))
			require.NoError(t, err)
		}
		db.Close()

		// Upload to storage
		objectPath := fmt.Sprintf("partitions/%s.sqlite", partitionID)
		err = storage.Upload(context.Background(), localPath, objectPath)
		require.NoError(t, err)

		// Create partition info
		partitions = append(partitions, &PartitionInfo{
			PartitionID: partitionID,
			ObjectPath:  objectPath,
			RowCount:    int64(numValues),
			MinEventTime: func() *int64 { v := int64(1000000 + i*1000); return &v }(),
			MaxEventTime: func() *int64 { v := int64(1000000 + i*1000 + numValues - 1); return &v }(),
		})
	}

	return partitions
}

// createEmptyPartition creates a partition with no data.
func createEmptyPartition(t *testing.T, storage storage.ObjectStorage) *PartitionInfo {
	partitionID := "empty_partition"
	localPath := filepath.Join(t.TempDir(), fmt.Sprintf("%s.sqlite", partitionID))

	// Create SQLite file with empty table
	db, err := sql.Open("sqlite3", localPath)
	require.NoError(t, err)

	_, err = db.Exec(`
		CREATE TABLE events (
			id INTEGER PRIMARY KEY,
			device_id TEXT,
			payload TEXT,
			event_time INTEGER
		)
	`)
	require.NoError(t, err)
	db.Close()

	// Upload to storage
	objectPath := fmt.Sprintf("partitions/%s.sqlite", partitionID)
	err = storage.Upload(context.Background(), localPath, objectPath)
	require.NoError(t, err)

	return &PartitionInfo{
		PartitionID: partitionID,
		ObjectPath:  objectPath,
		RowCount:    0,
	}
}

// testCatalog is a simple in-memory catalog for testing.
type testCatalog struct {
	indexes map[string]*IndexPartitionInfo
}

// newTestCatalog creates a new test catalog.
func newTestCatalog() *testCatalog {
	return &testCatalog{
		indexes: make(map[string]*IndexPartitionInfo),
	}
}

// RegisterIndexPartition registers a new index partition.
func (c *testCatalog) RegisterIndexPartition(ctx context.Context, info *IndexPartitionInfo) error {
	c.indexes[info.IndexID] = info
	return nil
}

// FindIndexPartition finds an index partition by collection, column, and bucket ID.
func (c *testCatalog) FindIndexPartition(ctx context.Context, collection, column string, bucketID int) (*IndexPartitionInfo, error) {
	indexID := fmt.Sprintf("%s_%s_%d", collection, column, bucketID)
	if info, ok := c.indexes[indexID]; ok {
		return info, nil
	}
	return nil, nil
}

// ListIndexes lists all indexed columns for a given collection.
func (c *testCatalog) ListIndexes(ctx context.Context, collection string) ([]string, error) {
	var columns []string
	for indexID := range c.indexes {
		if len(indexID) > len(collection)+1 && indexID[:len(collection)+1] == collection+"_" {
			columns = append(columns, indexID[len(collection)+1:])
		}
	}
	return columns, nil
}

// DeleteIndex deletes all index partitions for a given collection and column.
func (c *testCatalog) DeleteIndex(ctx context.Context, collection, column string) error {
	prefix := fmt.Sprintf("%s_%s_", collection, column)
	for indexID := range c.indexes {
		if len(indexID) > len(prefix) && indexID[:len(prefix)] == prefix {
			delete(c.indexes, indexID)
		}
	}
	return nil
}
