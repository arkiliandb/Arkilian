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

	"github.com/arkilian/arkilian/internal/storage"
	_ "github.com/mattn/go-sqlite3"
)

// Builder builds secondary index partitions from data partitions.
type Builder struct {
	storage storage.ObjectStorage
	catalog IndexCatalog
	workDir string
	buckets int
}

// NewBuilder creates a new index builder.
func NewBuilder(storage storage.ObjectStorage, catalog IndexCatalog, workDir string, buckets int) *Builder {
	if buckets == 0 {
		buckets = 64
	}
	return &Builder{
		storage: storage,
		catalog: catalog,
		workDir: workDir,
		buckets: buckets,
	}
}

// PartitionInfo represents a data partition for index building.
// This is a minimal subset of manifest.PartitionRecord to avoid import cycles.
type PartitionInfo struct {
	PartitionID  string
	ObjectPath   string
	RowCount     int64
	MinEventTime *int64
	MaxEventTime *int64
}

// BuildIndex builds secondary index partitions for a given collection and column.
// It scans all data partitions, extracts distinct values, hashes them into buckets,
// and creates index SQLite files for each non-empty bucket.
func (b *Builder) BuildIndex(ctx context.Context, collection, column string, partitions []*PartitionInfo) ([]*IndexPartitionInfo, error) {
	// Create semaphore channel for bounded concurrency
	sem := make(chan struct{}, 32)

	// Collect all (value, partitionID) tuples from all partitions
	type scanResult struct {
		tuples []Tuple
		err    error
	}
	results := make(chan scanResult, len(partitions))

	// WaitGroup to track when all workers are done
	var wg sync.WaitGroup

	// Scan each partition concurrently
	for _, dp := range partitions {
		dp := dp
		wg.Add(1)
		go func() {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			result := scanResult{}

			// Download partition to temp file
			tempPath := filepath.Join(b.workDir, fmt.Sprintf("scan_%s.sqlite", dp.PartitionID))
			defer os.Remove(tempPath)

			if err := b.storage.Download(ctx, dp.ObjectPath, tempPath); err != nil {
				result.err = fmt.Errorf("failed to download partition %s: %w", dp.PartitionID, err)
				results <- result
				return
			}

			// Open SQLite and query distinct values
			db, err := sql.Open("sqlite3", tempPath)
			if err != nil {
				result.err = fmt.Errorf("failed to open partition %s: %w", dp.PartitionID, err)
				results <- result
				return
			}
			defer db.Close()

			query := fmt.Sprintf("SELECT DISTINCT %s FROM events", column)
			rows, err := db.QueryContext(ctx, query)
			if err != nil {
				result.err = fmt.Errorf("failed to query partition %s: %w", dp.PartitionID, err)
				results <- result
				return
			}
			defer rows.Close()

			// Collect (value, partitionID) tuples
			var tuples []Tuple
			for rows.Next() {
				var value interface{}
				if err := rows.Scan(&value); err != nil {
					result.err = fmt.Errorf("failed to scan value from partition %s: %w", dp.PartitionID, err)
					results <- result
					return
				}
				tuples = append(tuples, Tuple{
					Value:      fmt.Sprint(value),
					Partition:  dp.PartitionID,
					RowCount:   dp.RowCount,
					MinTime:    dp.MinEventTime,
					MaxTime:    dp.MaxEventTime,
				})
			}
			if err := rows.Err(); err != nil {
				result.err = fmt.Errorf("error iterating rows from partition %s: %w", dp.PartitionID, err)
				results <- result
				return
			}

			result.tuples = tuples
			results <- result
		}()
	}

	// Close results channel when all goroutines complete
	go func() {
		wg.Wait()
		close(results)
	}()

	// Aggregate all tuples by bucket
	bucketTuples := make(map[int][]Tuple)
	for result := range results {
		if result.err != nil {
			return nil, result.err
		}
		for _, t := range result.tuples {
			bucket := b.hashValue(t.Value)
			bucketTuples[bucket] = append(bucketTuples[bucket], t)
		}
	}

	// Build index partitions for each non-empty bucket
	var indexInfos []*IndexPartitionInfo
	for bucketID, tuples := range bucketTuples {
		if len(tuples) == 0 {
			continue
		}

		// Create index partition SQLite file
		indexPath := filepath.Join(b.workDir, fmt.Sprintf("index_%d.sqlite", bucketID))
		if err := b.createIndexSQLite(indexPath, tuples); err != nil {
			return nil, fmt.Errorf("failed to create index for bucket %d: %w", bucketID, err)
		}

		// Get file size
		info, err := os.Stat(indexPath)
		if err != nil {
			return nil, fmt.Errorf("failed to stat index file %s: %w", indexPath, err)
		}

		// Calculate time range
		var minTime, maxTime int64
		for i, t := range tuples {
			if i == 0 || (t.MinTime != nil && int64(*t.MinTime) < minTime) {
				if t.MinTime != nil {
					minTime = int64(*t.MinTime)
				}
			}
			if i == 0 || (t.MaxTime != nil && int64(*t.MaxTime) > maxTime) {
				if t.MaxTime != nil {
					maxTime = int64(*t.MaxTime)
				}
			}
		}

		// Upload to S3
		objectPath := fmt.Sprintf("indexes/%s/%s/%d.sqlite", collection, column, bucketID)
		if _, err := b.storage.UploadMultipart(ctx, indexPath, objectPath); err != nil {
			return nil, fmt.Errorf("failed to upload index for bucket %d: %w", bucketID, err)
		}

		// Register in manifest
		indexID := fmt.Sprintf("%s_%s_%d", collection, column, bucketID)
		infoPart := &IndexPartitionInfo{
			IndexID:    indexID,
			Collection: collection,
			Column:     column,
			BucketID:   bucketID,
			ObjectPath: objectPath,
			EntryCount: int64(len(tuples)),
			SizeBytes:  info.Size(),
			CreatedAt:  info.ModTime(),
			CoveredRange: TimeRange{
				Min: minTime,
				Max: maxTime,
			},
		}

		if err := b.catalog.RegisterIndexPartition(ctx, infoPart); err != nil {
			return nil, fmt.Errorf("failed to register index partition %s: %w", indexID, err)
		}

		indexInfos = append(indexInfos, infoPart)

		// Clean up local index file
		os.Remove(indexPath)
	}

	return indexInfos, nil
}

// createIndexSQLite creates an SQLite file with the index_map table and inserts all tuples.
func (b *Builder) createIndexSQLite(path string, tuples []Tuple) error {
	// Remove existing file if present
	os.Remove(path)

	db, err := sql.Open("sqlite3", path)
	if err != nil {
		return fmt.Errorf("failed to open database: %w", err)
	}
	defer db.Close()

	// Create index_map table
	if _, err := db.Exec(IndexMapDDL); err != nil {
		return fmt.Errorf("failed to create index_map table: %w", err)
	}

	// Insert tuples
	stmt, err := db.Prepare("INSERT INTO index_map (column_value, partition_id, row_count, min_time, max_time) VALUES (?, ?, ?, ?, ?)")
	if err != nil {
		return fmt.Errorf("failed to prepare insert: %w", err)
	}
	defer stmt.Close()

	for _, t := range tuples {
		_, err := stmt.Exec(t.Value, t.Partition, t.RowCount, t.MinTime, t.MaxTime)
		if err != nil {
			return fmt.Errorf("failed to insert tuple: %w", err)
		}
	}

	return nil
}

// hashValue hashes a value and returns the bucket ID.
func (b *Builder) hashValue(value string) int {
	h := fnv.New32a()
	h.Write([]byte(value))
	return int(h.Sum32()) % b.buckets
}

// Tuple represents a (value, partition_id) pair for the index.
type Tuple struct {
	Value      string
	Partition  string
	RowCount   int64
	MinTime    *int64
	MaxTime    *int64
}
