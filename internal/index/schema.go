// Package index provides secondary index partitions for efficient point lookups on any column.
package index

import (
	"context"
	"time"
)

// IndexPartitionInfo represents a secondary index partition in the manifest.
type IndexPartitionInfo struct {
	IndexID      string
	Collection   string
	Column       string
	BucketID     int
	ObjectPath   string
	EntryCount   int64
	SizeBytes    int64
	CreatedAt    time.Time
	CoveredRange TimeRange
}

// TimeRange represents the time range covered by an index partition.
type TimeRange struct {
	Min int64
	Max int64
}

// IndexMapDDL contains the CREATE TABLE and CREATE INDEX SQL for the index_map table.
const IndexMapDDL = `
CREATE TABLE IF NOT EXISTS index_map (
    column_value TEXT NOT NULL,
    partition_id TEXT NOT NULL,
    row_count    INTEGER,
    min_time     INTEGER,
    max_time     INTEGER,
    PRIMARY KEY (column_value, partition_id)
) WITHOUT ROWID;

CREATE INDEX IF NOT EXISTS idx_value ON index_map(column_value);
`

// IndexCatalog interface defines methods for managing secondary index partitions.
// This is a separate interface from manifest.Catalog to avoid breaking existing implementations.
// SQLiteCatalog and ShardedCatalog implement this interface separately.
type IndexCatalog interface {
	// RegisterIndexPartition registers a new index partition in the manifest.
	RegisterIndexPartition(ctx context.Context, info *IndexPartitionInfo) error

	// FindIndexPartition finds an index partition by collection, column, and bucket ID.
	FindIndexPartition(ctx context.Context, collection, column string, bucketID int) (*IndexPartitionInfo, error)

	// ListIndexes lists all indexed columns for a given collection.
	ListIndexes(ctx context.Context, collection string) ([]string, error)

	// DeleteIndex deletes all index partitions for a given collection and column.
	DeleteIndex(ctx context.Context, collection, column string) error
}
