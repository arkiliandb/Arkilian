package manifest

import "context"

// CatalogReader is the read-only interface used by the query planner and pruner.
// Both SQLiteCatalog and ShardedCatalog implement this interface.
type CatalogReader interface {
	// FindPartitions returns partitions matching the given predicates.
	FindPartitions(ctx context.Context, predicates []Predicate) ([]*PartitionRecord, error)

	// GetPartitionCount returns the total number of active (non-compacted) partitions.
	GetPartitionCount(ctx context.Context) (int64, error)

	// GetZoneMapsForKeys retrieves zone map entries for multiple partition keys and a single column.
	// Returns a map of partition_key → ZoneMapEntry. Missing keys are omitted.
	GetZoneMapsForKeys(ctx context.Context, partitionKeys []string, column string) (map[string]*ZoneMapEntry, error)
}
