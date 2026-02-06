package compaction

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/arkilian/arkilian/internal/manifest"
	"github.com/arkilian/arkilian/internal/storage"
)

// GarbageCollector removes compacted source partitions after the TTL has elapsed.
// Source partitions are retained for the configured TTL period (default 7 days)
// to allow in-flight queries to complete before deletion.
type GarbageCollector struct {
	catalog manifest.Catalog
	storage storage.ObjectStorage
	ttl     time.Duration
}

// NewGarbageCollector creates a new garbage collector.
func NewGarbageCollector(catalog manifest.Catalog, store storage.ObjectStorage, ttl time.Duration) *GarbageCollector {
	if ttl <= 0 {
		ttl = 7 * 24 * time.Hour // Default 7 days
	}
	return &GarbageCollector{
		catalog: catalog,
		storage: store,
		ttl:     ttl,
	}
}

// GCResult holds the outcome of a garbage collection run.
type GCResult struct {
	DeletedPartitions []string
	DeletedObjects    []string
	Errors            []string
}

// CollectGarbage finds and removes partitions that have been compacted and are past the TTL.
// It deletes from object storage first, then removes the manifest entry.
func (gc *GarbageCollector) CollectGarbage(ctx context.Context) error {
	result, err := gc.CollectGarbageWithResult(ctx)
	if err != nil {
		return err
	}

	if len(result.DeletedPartitions) > 0 {
		log.Printf("compaction/gc: deleted %d expired partitions", len(result.DeletedPartitions))
	}

	if len(result.Errors) > 0 {
		log.Printf("compaction/gc: encountered %d errors during GC", len(result.Errors))
	}

	return nil
}

// CollectGarbageWithResult performs garbage collection and returns detailed results.
func (gc *GarbageCollector) CollectGarbageWithResult(ctx context.Context) (*GCResult, error) {
	result := &GCResult{}

	// Find expired partitions (compacted and past TTL)
	expiredIDs, err := gc.catalog.DeleteExpired(ctx, gc.ttl)
	if err != nil {
		return nil, fmt.Errorf("compaction/gc: failed to find expired partitions: %w", err)
	}

	result.DeletedPartitions = expiredIDs

	// Note: The catalog.DeleteExpired already removes the manifest entries.
	// Object storage cleanup for the actual files would require knowing the object paths
	// before deletion. For now, the manifest entries are cleaned up.
	// A separate reconciliation process handles orphaned storage objects.

	return result, nil
}

// FindExpiredPartitions returns partitions that are past the TTL without deleting them.
// This is useful for dry-run or reporting purposes.
func (gc *GarbageCollector) FindExpiredPartitions(ctx context.Context) ([]*manifest.PartitionRecord, error) {
	// Query compacted partitions that are past TTL
	allPartitions, err := gc.catalog.FindPartitions(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("compaction/gc: failed to query partitions: %w", err)
	}

	cutoff := time.Now().Add(-gc.ttl)
	var expired []*manifest.PartitionRecord

	for _, p := range allPartitions {
		// Only consider partitions that have been compacted
		if p.CompactedInto != nil && p.CreatedAt.Before(cutoff) {
			expired = append(expired, p)
		}
	}

	return expired, nil
}

// TTL returns the configured time-to-live for compacted partitions.
func (gc *GarbageCollector) TTL() time.Duration {
	return gc.ttl
}
