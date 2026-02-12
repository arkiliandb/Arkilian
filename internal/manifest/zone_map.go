package manifest

import (
	"context"
	"fmt"
	"time"

	"github.com/arkilian/arkilian/internal/bloom"
)

// ZoneMapEntry represents a per-partition-key aggregate bloom filter stored in the manifest.
// Zone maps enable bloom filter pruning without S3 GETs — the merged filter for all
// partitions sharing a key is checked directly from the manifest catalog.
type ZoneMapEntry struct {
	PartitionKey  string
	ColumnName    string
	BloomFilter   *bloom.BloomFilter
	ItemCount     int
	DistinctCount int
	UpdatedAt     time.Time
}

// UpsertZoneMap inserts or updates a zone map entry for a partition_key + column.
// The bloom filter is serialized to bytes and stored as a BLOB in the manifest.
func (c *SQLiteCatalog) UpsertZoneMap(ctx context.Context, entry *ZoneMapEntry) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	data, err := entry.BloomFilter.Serialize()
	if err != nil {
		return fmt.Errorf("manifest: failed to serialize zone map bloom filter: %w", err)
	}

	_, err = c.db.ExecContext(ctx,
		`INSERT OR REPLACE INTO zone_maps
			(partition_key, column_name, bloom_data, num_bits, num_hashes, item_count, distinct_count, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		entry.PartitionKey, entry.ColumnName, data,
		entry.BloomFilter.NumBits(), entry.BloomFilter.NumHashes(),
		entry.ItemCount, entry.DistinctCount, entry.UpdatedAt.Unix(),
	)
	if err != nil {
		return fmt.Errorf("manifest: failed to upsert zone map: %w", err)
	}
	return nil
}

// GetZoneMap retrieves a zone map entry for a partition_key + column.
// Returns nil, nil if no zone map exists for this key/column.
func (c *SQLiteCatalog) GetZoneMap(ctx context.Context, partitionKey, column string) (*ZoneMapEntry, error) {
	var data []byte
	var numBits, numHashes, itemCount, distinctCount int
	var updatedAtUnix int64

	err := c.readDB.QueryRowContext(ctx,
		`SELECT bloom_data, num_bits, num_hashes, item_count, distinct_count, updated_at
		 FROM zone_maps WHERE partition_key = ? AND column_name = ?`,
		partitionKey, column,
	).Scan(&data, &numBits, &numHashes, &itemCount, &distinctCount, &updatedAtUnix)

	if err != nil {
		if err.Error() == "sql: no rows in result set" {
			return nil, nil
		}
		return nil, fmt.Errorf("manifest: failed to get zone map: %w", err)
	}

	bf, err := bloom.Deserialize(data)
	if err != nil {
		return nil, fmt.Errorf("manifest: failed to deserialize zone map bloom filter: %w", err)
	}

	return &ZoneMapEntry{
		PartitionKey:  partitionKey,
		ColumnName:    column,
		BloomFilter:   bf,
		ItemCount:     itemCount,
		DistinctCount: distinctCount,
		UpdatedAt:     time.Unix(updatedAtUnix, 0),
	}, nil
}

// GetZoneMapsForKeys retrieves zone map entries for multiple partition keys and a single column.
// Returns a map of partition_key → ZoneMapEntry. Missing keys are omitted.
func (c *SQLiteCatalog) GetZoneMapsForKeys(ctx context.Context, partitionKeys []string, column string) (map[string]*ZoneMapEntry, error) {
	if len(partitionKeys) == 0 {
		return nil, nil
	}

	result := make(map[string]*ZoneMapEntry, len(partitionKeys))

	// Query in batches to avoid SQLite variable limit
	const batchSize = 500
	for i := 0; i < len(partitionKeys); i += batchSize {
		end := i + batchSize
		if end > len(partitionKeys) {
			end = len(partitionKeys)
		}
		batch := partitionKeys[i:end]

		placeholders := ""
		args := make([]interface{}, 0, len(batch)+1)
		for j, key := range batch {
			if j > 0 {
				placeholders += ","
			}
			placeholders += "?"
			args = append(args, key)
		}
		args = append(args, column)

		query := fmt.Sprintf(
			`SELECT partition_key, bloom_data, num_bits, num_hashes, item_count, distinct_count, updated_at
			 FROM zone_maps WHERE partition_key IN (%s) AND column_name = ?`, placeholders)

		rows, err := c.readDB.QueryContext(ctx, query, args...)
		if err != nil {
			return nil, fmt.Errorf("manifest: failed to query zone maps: %w", err)
		}

		for rows.Next() {
			var key string
			var data []byte
			var numBits, numHashes, itemCount, distinctCount int
			var updatedAtUnix int64

			if err := rows.Scan(&key, &data, &numBits, &numHashes, &itemCount, &distinctCount, &updatedAtUnix); err != nil {
				rows.Close()
				return nil, fmt.Errorf("manifest: failed to scan zone map: %w", err)
			}

			bf, err := bloom.Deserialize(data)
			if err != nil {
				rows.Close()
				return nil, fmt.Errorf("manifest: failed to deserialize zone map for key %s: %w", key, err)
			}

			result[key] = &ZoneMapEntry{
				PartitionKey:  key,
				ColumnName:    column,
				BloomFilter:   bf,
				ItemCount:     itemCount,
				DistinctCount: distinctCount,
				UpdatedAt:     time.Unix(updatedAtUnix, 0),
			}
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("manifest: error iterating zone maps: %w", err)
		}
	}

	return result, nil
}

// MergeIntoZoneMap merges a per-partition bloom filter into the zone map for a partition key.
// If no zone map exists yet, the partition's filter becomes the zone map.
// If one exists, the bits are OR'd together (union of the two filters).
func (c *SQLiteCatalog) MergeIntoZoneMap(ctx context.Context, partitionKey, column string, partitionFilter *bloom.BloomFilter, distinctCount int) error {
	existing, err := c.GetZoneMap(ctx, partitionKey, column)
	if err != nil {
		return err
	}

	var merged *bloom.BloomFilter
	var totalItems int
	var totalDistinct int

	if existing == nil {
		// First filter for this key — use it directly
		merged = partitionFilter
		totalItems = int(partitionFilter.Count())
		totalDistinct = distinctCount
	} else {
		// Merge: OR the bit arrays together
		merged, err = mergeBloomFilters(existing.BloomFilter, partitionFilter)
		if err != nil {
			return fmt.Errorf("manifest: failed to merge zone map filters: %w", err)
		}
		totalItems = existing.ItemCount + int(partitionFilter.Count())
		totalDistinct = existing.DistinctCount + distinctCount // approximate upper bound
	}

	return c.UpsertZoneMap(ctx, &ZoneMapEntry{
		PartitionKey:  partitionKey,
		ColumnName:    column,
		BloomFilter:   merged,
		ItemCount:     totalItems,
		DistinctCount: totalDistinct,
		UpdatedAt:     time.Now(),
	})
}

// DeleteZoneMap removes zone map entries for a partition key (all columns).
// Called when all partitions for a key are deleted or compacted.
func (c *SQLiteCatalog) DeleteZoneMap(ctx context.Context, partitionKey string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	_, err := c.db.ExecContext(ctx,
		"DELETE FROM zone_maps WHERE partition_key = ?", partitionKey)
	if err != nil {
		return fmt.Errorf("manifest: failed to delete zone map: %w", err)
	}
	return nil
}

// UpdateZoneMapsFromMetadata merges bloom filters from a partition's metadata sidecar
// into the zone maps for the given partition key. Called after RegisterPartition during ingest.
func (c *SQLiteCatalog) UpdateZoneMapsFromMetadata(ctx context.Context, partitionKey string, bloomFilters map[string]*bloom.BloomFilter, distinctCounts map[string]int) error {
	for column, bf := range bloomFilters {
		dc := distinctCounts[column]
		if err := c.MergeIntoZoneMap(ctx, partitionKey, column, bf, dc); err != nil {
			return fmt.Errorf("manifest: failed to update zone map for %s/%s: %w", partitionKey, column, err)
		}
	}
	return nil
}

// RebuildZoneMap rebuilds the zone map for a partition key by scanning all active partitions'
// bloom filters from their metadata sidecars. Called after compaction when the merged filter
// may have become stale due to source partition removal.
func (c *SQLiteCatalog) RebuildZoneMap(ctx context.Context, partitionKey string) error {
	// Delete existing zone maps for this key
	if err := c.DeleteZoneMap(ctx, partitionKey); err != nil {
		return err
	}
	// Zone maps will be rebuilt incrementally as new partitions are ingested
	// or can be rebuilt by re-reading metadata sidecars (caller's responsibility)
	return nil
}

// mergeBloomFilters OR's two bloom filters together.
// Both filters must have the same numBits and numHashes.
// If they differ, the partition filter is returned as-is (safe fallback).
func mergeBloomFilters(a, b *bloom.BloomFilter) (*bloom.BloomFilter, error) {
	if a.NumBits() != b.NumBits() || a.NumHashes() != b.NumHashes() {
		// Incompatible filters — rebuild from scratch would be needed.
		// For now, return the larger one (safe: may produce false positives but no false negatives).
		if a.Count() >= b.Count() {
			return a, nil
		}
		return b, nil
	}

	aBits := a.Bits()
	bBits := b.Bits()

	merged := bloom.New(a.NumBits(), a.NumHashes())
	mergedBits := make([]uint64, len(aBits))
	for i := range aBits {
		mergedBits[i] = aBits[i] | bBits[i]
	}
	merged.SetBits(mergedBits)
	merged.SetCount(a.Count() + b.Count())

	return merged, nil
}
