package manifest

import (
	"context"
	"fmt"
	"log"

	"github.com/arkilian/arkilian/internal/partition"
)

// MigrateToSharded checks if a single-file catalog has crossed the partition
// count threshold and, if so, migrates all partition records into a new
// ShardedCatalog. The original single-file catalog is closed after migration.
//
// Returns the new ShardedCatalog if migration occurred, or nil if the threshold
// was not crossed (caller should continue using the single-file catalog).
//
// This is an online migration: it reads all active partitions from the source,
// re-registers them in the sharded catalog, then closes the source. The source
// manifest.db file is left on disk as a backup (not deleted).
func MigrateToSharded(source *SQLiteCatalog, baseDir string, shardCount int, threshold int64) (*ShardedCatalog, error) {
	if threshold <= 0 {
		return nil, nil // auto-migration disabled
	}

	ctx := context.Background()
	count, err := source.GetPartitionCount(ctx)
	if err != nil {
		return nil, fmt.Errorf("manifest: auto-shard check failed: %w", err)
	}

	if count < threshold {
		return nil, nil // below threshold, no migration needed
	}

	log.Printf("[WARN] manifest: partition count (%d) exceeds auto-shard threshold (%d) — migrating to sharded catalog with %d shards",
		count, threshold, shardCount)

	// Create the sharded catalog
	sharded, err := NewShardedCatalog(baseDir, shardCount)
	if err != nil {
		return nil, fmt.Errorf("manifest: failed to create sharded catalog for migration: %w", err)
	}

	// Read all active partitions from the single-file catalog
	allPartitions, err := source.FindPartitions(ctx, nil)
	if err != nil {
		sharded.Close()
		return nil, fmt.Errorf("manifest: failed to read partitions for migration: %w", err)
	}

	// Re-register each partition in the sharded catalog.
	migrated := 0
	for _, rec := range allPartitions {
		info := partitionRecordToInfo(rec)
		if err := sharded.RegisterPartition(ctx, info, rec.ObjectPath); err != nil {
			sharded.Close()
			return nil, fmt.Errorf("manifest: migration failed at partition %s: %w", rec.PartitionID, err)
		}
		migrated++

		if migrated%10000 == 0 {
			log.Printf("manifest: migration progress: %d/%d partitions", migrated, len(allPartitions))
		}
	}

	// Migrate zone maps
	if err := migrateZoneMaps(ctx, source, sharded); err != nil {
		log.Printf("[WARN] manifest: zone map migration failed (non-fatal, will rebuild): %v", err)
	}

	log.Printf("manifest: migration complete — %d partitions moved to %d shards", migrated, shardCount)

	// Close the source catalog (file remains on disk as backup)
	source.Close()

	return sharded, nil
}

// partitionRecordToInfo converts a PartitionRecord back to a partition.PartitionInfo
// for re-registration during migration.
func partitionRecordToInfo(rec *PartitionRecord) *partition.PartitionInfo {
	info := &partition.PartitionInfo{
		PartitionID:   rec.PartitionID,
		PartitionKey:  rec.PartitionKey,
		RowCount:      rec.RowCount,
		SizeBytes:     rec.SizeBytes,
		SchemaVersion: rec.SchemaVersion,
		CreatedAt:     rec.CreatedAt,
		MinMaxStats:   make(map[string]partition.MinMax),
	}

	if rec.MinEventTime != nil && rec.MaxEventTime != nil {
		info.MinMaxStats["event_time"] = partition.MinMax{Min: *rec.MinEventTime, Max: *rec.MaxEventTime}
	}
	if rec.MinUserID != nil && rec.MaxUserID != nil {
		info.MinMaxStats["user_id"] = partition.MinMax{Min: *rec.MinUserID, Max: *rec.MaxUserID}
	}
	if rec.MinTenantID != nil && rec.MaxTenantID != nil {
		info.MinMaxStats["tenant_id"] = partition.MinMax{Min: *rec.MinTenantID, Max: *rec.MaxTenantID}
	}

	return info
}

// migrateZoneMaps copies zone map entries from the source catalog to the sharded catalog.
func migrateZoneMaps(ctx context.Context, source *SQLiteCatalog, target *ShardedCatalog) error {
	keys, err := source.GetDistinctPartitionKeys(ctx)
	if err != nil {
		return err
	}

	columns := []string{"tenant_id", "user_id", "event_type"}
	for _, key := range keys {
		for _, col := range columns {
			entry, err := source.GetZoneMap(ctx, key, col)
			if err != nil || entry == nil {
				continue
			}
			if err := target.UpsertZoneMap(ctx, entry); err != nil {
				return fmt.Errorf("zone map migration failed for key=%s col=%s: %w", key, col, err)
			}
		}
	}
	return nil
}
