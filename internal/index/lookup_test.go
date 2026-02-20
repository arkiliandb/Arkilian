// Package index provides secondary index partitions for efficient point lookups on any column.
package index

import (
	"context"
	"database/sql"
	"fmt"
	"hash/fnv"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockStorage is a minimal in-memory mock of ObjectStorage for testing.
type mockStorage struct {
	mu    sync.RWMutex
	files map[string][]byte
}

func newMockStorage() *mockStorage {
	return &mockStorage{
		files: make(map[string][]byte),
	}
}

func (m *mockStorage) Upload(ctx context.Context, localPath, objectPath string) error {
	data, err := os.ReadFile(localPath)
	if err != nil {
		return err
	}
	m.mu.Lock()
	m.files[objectPath] = data
	m.mu.Unlock()
	return nil
}

func (m *mockStorage) UploadMultipart(ctx context.Context, localPath, objectPath string) (string, error) {
	return "", m.Upload(ctx, localPath, objectPath)
}

func (m *mockStorage) Download(ctx context.Context, objectPath, localPath string) error {
	m.mu.RLock()
	data, ok := m.files[objectPath]
	m.mu.RUnlock()

	if !ok {
		return fmt.Errorf("object not found: %s", objectPath)
	}

	if err := os.MkdirAll(filepath.Dir(localPath), 0755); err != nil {
		return err
	}

	return os.WriteFile(localPath, data, 0644)
}

func (m *mockStorage) Delete(ctx context.Context, objectPath string) error {
	m.mu.Lock()
	delete(m.files, objectPath)
	m.mu.Unlock()
	return nil
}

func (m *mockStorage) Exists(ctx context.Context, objectPath string) (bool, error) {
	m.mu.RLock()
	_, ok := m.files[objectPath]
	m.mu.RUnlock()
	return ok, nil
}

func (m *mockStorage) ConditionalPut(ctx context.Context, localPath, objectPath, etag string) error {
	return m.Upload(ctx, localPath, objectPath)
}

func (m *mockStorage) ListObjects(ctx context.Context, prefix string) ([]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var result []string
	for path := range m.files {
		result = append(result, path)
	}
	return result, nil
}

// mockCatalog is a minimal in-memory mock of IndexCatalog for testing.
type mockCatalog struct {
	mu       sync.RWMutex
	partitions map[string]*IndexPartitionInfo
}

func newMockCatalog() *mockCatalog {
	return &mockCatalog{
		partitions: make(map[string]*IndexPartitionInfo),
	}
}

func (m *mockCatalog) RegisterIndexPartition(ctx context.Context, info *IndexPartitionInfo) error {
	m.mu.Lock()
	m.partitions[info.IndexID] = info
	m.mu.Unlock()
	return nil
}

func (m *mockCatalog) FindIndexPartition(ctx context.Context, collection, column string, bucketID int) (*IndexPartitionInfo, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, info := range m.partitions {
		if info.Collection == collection && info.Column == column && info.BucketID == bucketID {
			return info, nil
		}
	}
	return nil, nil
}

func (m *mockCatalog) ListIndexes(ctx context.Context, collection string) ([]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	seen := make(map[string]bool)
	var result []string
	for _, info := range m.partitions {
		if info.Collection == collection && !seen[info.Column] {
			result = append(result, info.Column)
			seen[info.Column] = true
		}
	}
	return result, nil
}

func (m *mockCatalog) DeleteIndex(ctx context.Context, collection, column string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	for id, info := range m.partitions {
		if info.Collection == collection && info.Column == column {
			delete(m.partitions, id)
		}
	}
	return nil
}

// createTestIndexFile creates a test SQLite index file with sample data.
func createTestIndexFile(t *testing.T, path string, data map[string][]string) {
	db, err := sql.Open("sqlite3", path)
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec(IndexMapDDL)
	require.NoError(t, err)

	for value, partitionIDs := range data {
		for _, partID := range partitionIDs {
			_, err := db.Exec(
				"INSERT INTO index_map (column_value, partition_id, row_count, min_time, max_time) VALUES (?, ?, ?, ?, ?)",
				value, partID, 100, 1000, 2000,
			)
			require.NoError(t, err)
		}
	}
}

func TestLookupBasic(t *testing.T) {
	ctx := context.Background()
	tempDir := t.TempDir()
	cacheDir := filepath.Join(tempDir, "cache")
	require.NoError(t, os.Mkdir(cacheDir, 0755))

	// Create test index file
	indexPath := filepath.Join(tempDir, "index.sqlite")
	createTestIndexFile(t, indexPath, map[string][]string{
		"device-123": {"partition-001", "partition-002"},
	})

	// Setup mocks
	storage := newMockStorage()
	catalog := newMockCatalog()

	// Calculate correct bucket for "device-123"
	h := fnv.New32a()
	h.Write([]byte("device-123"))
	bucket := int(h.Sum32()) % 64

	// Register index partition
	indexInfo := &IndexPartitionInfo{
		IndexID:    "test_device",
		Collection: "events",
		Column:     "device_id",
		BucketID:   bucket,
		ObjectPath: "index.sqlite",
		EntryCount: 2,
		SizeBytes:  4096,
		CreatedAt:  time.Now(),
	}
	require.NoError(t, catalog.RegisterIndexPartition(ctx, indexInfo))

	// Upload index file to mock storage
	require.NoError(t, storage.Upload(ctx, indexPath, "index.sqlite"))

	// Create lookup and test
	lookup := NewLookup(storage, catalog, cacheDir, 64)
	partitionIDs, err := lookup.FindPartitions(ctx, "events", "device_id", "device-123")
	require.NoError(t, err)
	assert.Equal(t, []string{"partition-001", "partition-002"}, partitionIDs)
}

func TestLookupCacheHit(t *testing.T) {
	ctx := context.Background()
	tempDir := t.TempDir()
	cacheDir := filepath.Join(tempDir, "cache")
	require.NoError(t, os.Mkdir(cacheDir, 0755))

	// Create test index file
	indexPath := filepath.Join(tempDir, "index.sqlite")
	createTestIndexFile(t, indexPath, map[string][]string{
		"device-456": {"partition-003"},
	})

	// Setup mocks
	storage := newMockStorage()
	catalog := newMockCatalog()

	// Calculate correct bucket for "device-456"
	h := fnv.New32a()
	h.Write([]byte("device-456"))
	bucket := int(h.Sum32()) % 64

	// Register index partition
	indexInfo := &IndexPartitionInfo{
		IndexID:    "test_device_2",
		Collection: "events",
		Column:     "device_id",
		BucketID:   bucket,
		ObjectPath: "index.sqlite",
		EntryCount: 1,
		SizeBytes:  4096,
		CreatedAt:  time.Now(),
	}
	require.NoError(t, catalog.RegisterIndexPartition(ctx, indexInfo))

	// Upload index file to mock storage
	require.NoError(t, storage.Upload(ctx, indexPath, "index.sqlite"))

	// Create lookup
	lookup := NewLookup(storage, catalog, cacheDir, 64)

	// First lookup - should download and cache
	partitionIDs, err := lookup.FindPartitions(ctx, "events", "device_id", "device-456")
	require.NoError(t, err)
	assert.Equal(t, []string{"partition-003"}, partitionIDs)

	// Verify cache file exists
	cachePath := filepath.Join(cacheDir, fmt.Sprintf("idx_%s.sqlite", indexInfo.IndexID))
	_, err = os.Stat(cachePath)
	assert.NoError(t, err, "cache file should exist after first lookup")

	// Remove from storage to verify cache is used
	require.NoError(t, storage.Delete(ctx, "index.sqlite"))

	// Second lookup - should use cache
	partitionIDs, err = lookup.FindPartitions(ctx, "events", "device_id", "device-456")
	require.NoError(t, err)
	assert.Equal(t, []string{"partition-003"}, partitionIDs)
}

func TestLookupNoIndex(t *testing.T) {
	ctx := context.Background()
	tempDir := t.TempDir()
	cacheDir := filepath.Join(tempDir, "cache")
	require.NoError(t, os.Mkdir(cacheDir, 0755))

	// Setup mocks with no indexes
	storage := newMockStorage()
	catalog := newMockCatalog()

	// Create lookup
	lookup := NewLookup(storage, catalog, cacheDir, 64)

	// Lookup for non-indexed column should return nil, nil
	partitionIDs, err := lookup.FindPartitions(ctx, "events", "non_existent_column", "some-value")
	assert.NoError(t, err)
	assert.Nil(t, partitionIDs)
}
