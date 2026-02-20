// Package index provides secondary index partitions for efficient point lookups on any column.
package index

import (
	"context"
	"database/sql"
	"fmt"
	"hash/fnv"
	"os"
	"path/filepath"

	"github.com/arkilian/arkilian/internal/storage"
)

// Lookup provides index-based partition lookup for efficient point queries.
type Lookup struct {
	storage  storage.ObjectStorage
	catalog  IndexCatalog
	cacheDir string
	buckets  int
}

// NewLookup creates a new index lookup instance.
func NewLookup(storage storage.ObjectStorage, catalog IndexCatalog, cacheDir string, buckets int) *Lookup {
	if buckets == 0 {
		buckets = 64
	}
	return &Lookup{
		storage:  storage,
		catalog:  catalog,
		cacheDir: cacheDir,
		buckets:  buckets,
	}
}

// FindPartitions finds partition IDs for a given column value using secondary index.
// Returns nil, nil if no index exists for the column (caller should fall back to bloom pruning).
func (l *Lookup) FindPartitions(ctx context.Context, collection, column string, value interface{}) ([]string, error) {
	// Hash value to bucket
	h := fnv.New32a()
	h.Write([]byte(fmt.Sprint(value)))
	bucket := int(h.Sum32()) % l.buckets

	// Find index partition
	info, err := l.catalog.FindIndexPartition(ctx, collection, column, bucket)
	if err != nil {
		return nil, fmt.Errorf("index lookup: failed to find index partition: %w", err)
	}
	if info == nil {
		// No index exists for this column - caller should fall back to bloom pruning
		return nil, nil
	}

	// Check cache first
	cachePath := filepath.Join(l.cacheDir, fmt.Sprintf("idx_%s.sqlite", info.IndexID))
	if _, err := os.Stat(cachePath); err == nil {
		// Cache hit - use cached file
		return l.queryIndex(ctx, cachePath, value)
	}

	// Download index partition to temp file
	tempPath := filepath.Join(l.cacheDir, fmt.Sprintf("idx_%s_temp.sqlite", info.IndexID))
	if err := l.storage.Download(ctx, info.ObjectPath, tempPath); err != nil {
		return nil, fmt.Errorf("index lookup: failed to download index partition: %w", err)
	}

	// Move temp file to cache location
	if err := os.Rename(tempPath, cachePath); err != nil {
		os.Remove(tempPath)
		return nil, fmt.Errorf("index lookup: failed to cache index partition: %w", err)
	}

	return l.queryIndex(ctx, cachePath, value)
}

// queryIndex queries the index SQLite file for partition IDs.
func (l *Lookup) queryIndex(ctx context.Context, indexPath string, value interface{}) ([]string, error) {
	db, err := sql.Open("sqlite3", indexPath)
	if err != nil {
		return nil, fmt.Errorf("index lookup: failed to open index: %w", err)
	}
	defer db.Close()

	query := "SELECT partition_id FROM index_map WHERE column_value = ?"
	rows, err := db.QueryContext(ctx, query, fmt.Sprint(value))
	if err != nil {
		return nil, fmt.Errorf("index lookup: failed to query index: %w", err)
	}
	defer rows.Close()

	var partitionIDs []string
	for rows.Next() {
		var partitionID string
		if err := rows.Scan(&partitionID); err != nil {
			return nil, fmt.Errorf("index lookup: failed to scan partition ID: %w", err)
		}
		partitionIDs = append(partitionIDs, partitionID)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("index lookup: error iterating rows: %w", err)
	}

	return partitionIDs, nil
}
