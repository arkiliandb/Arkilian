// Package compaction provides the compaction daemon for merging small partitions
// and garbage collection of expired data.
package compaction

import (
	"context"
	"fmt"
	"time"

	"github.com/arkilian/arkilian/internal/manifest"
)

const (
	// DefaultMaxPartitionSize is the threshold below which partitions are candidates for compaction (32MB).
	DefaultMaxPartitionSize int64 = 32 * 1024 * 1024

	// DefaultMaxPartitionsPerKey is the threshold above which compaction is triggered for a partition key.
	DefaultMaxPartitionsPerKey int64 = 50
)

// CandidateFinder identifies partitions eligible for compaction.
type CandidateFinder struct {
	catalog             manifest.Catalog
	maxPartitionSize    int64
	maxPartitionsPerKey int64
	adaptiveSizer       AdaptiveSizer
}

// AdaptiveSizer provides per-key partition size targets. When nil or when
// TargetSizeBytes returns 0, the static maxPartitionSize is used.
type AdaptiveSizer interface {
	TargetSizeBytes(ctx context.Context, partitionKey string) int64
}

// NewCandidateFinder creates a new compaction candidate finder.
func NewCandidateFinder(catalog manifest.Catalog, maxPartitionSize int64, maxPartitionsPerKey int64, sizer AdaptiveSizer) *CandidateFinder {
	if maxPartitionSize <= 0 {
		maxPartitionSize = DefaultMaxPartitionSize
	}
	if maxPartitionsPerKey <= 0 {
		maxPartitionsPerKey = DefaultMaxPartitionsPerKey
	}
	return &CandidateFinder{
		catalog:             catalog,
		maxPartitionSize:    maxPartitionSize,
		maxPartitionsPerKey: maxPartitionsPerKey,
		adaptiveSizer:       sizer,
	}
}

// thresholdForKey returns the compaction size threshold for a partition key.
// If adaptive sizing is enabled, it uses the per-key target; otherwise falls
// back to the static maxPartitionSize.
func (f *CandidateFinder) thresholdForKey(ctx context.Context, partitionKey string) int64 {
	if f.adaptiveSizer != nil {
		if target := f.adaptiveSizer.TargetSizeBytes(ctx, partitionKey); target > 0 {
			return target
		}
	}
	return f.maxPartitionSize
}

// CandidateGroup represents a group of partitions that should be compacted together.
type CandidateGroup struct {
	PartitionKey string
	Partitions   []*manifest.PartitionRecord
	Reason       CompactionReason
}

// CompactionReason describes why partitions were selected for compaction.
type CompactionReason string

const (
	// ReasonSmallPartitions indicates partitions are below the size threshold.
	ReasonSmallPartitions CompactionReason = "small_partitions"

	// ReasonTooManyPartitions indicates too many partitions exist for the same key.
	ReasonTooManyPartitions CompactionReason = "too_many_partitions"
)

// FindCandidates returns groups of partitions eligible for compaction.
func (f *CandidateFinder) FindCandidates(ctx context.Context) ([]*CandidateGroup, error) {
	var groups []*CandidateGroup

	// Find small partitions (< 8MB) grouped by partition key
	smallGroups, err := f.findSmallPartitions(ctx)
	if err != nil {
		return nil, fmt.Errorf("compaction: failed to find small partitions: %w", err)
	}
	groups = append(groups, smallGroups...)

	// Find partition keys with too many partitions
	overflowGroups, err := f.findOverflowPartitions(ctx)
	if err != nil {
		return nil, fmt.Errorf("compaction: failed to find overflow partitions: %w", err)
	}

	// Merge overflow groups, avoiding duplicates with small partition groups
	existingKeys := make(map[string]bool)
	for _, g := range groups {
		existingKeys[g.PartitionKey] = true
	}
	for _, g := range overflowGroups {
		if !existingKeys[g.PartitionKey] {
			groups = append(groups, g)
		}
	}

	return groups, nil
}

// FindSmallPartitions returns partitions smaller than the configured threshold,
// grouped by partition key. Each group must have at least 2 partitions to be worth compacting.
func (f *CandidateFinder) FindSmallPartitions(ctx context.Context) ([]*CandidateGroup, error) {
	return f.findSmallPartitions(ctx)
}

// findSmallPartitions finds partitions below the size threshold grouped by key.
// When adaptive sizing is enabled, each key gets its own threshold based on
// total data volume, so high-volume keys use larger target sizes.
func (f *CandidateFinder) findSmallPartitions(ctx context.Context) ([]*CandidateGroup, error) {
	// Query all active partition keys to check each for small partitions
	allPartitions, err := f.catalog.FindPartitions(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to query partitions: %w", err)
	}

	// Group all partitions by key first
	byKey := make(map[string][]*manifest.PartitionRecord)
	for _, p := range allPartitions {
		byKey[p.PartitionKey] = append(byKey[p.PartitionKey], p)
	}

	// For each key, determine the threshold and find small partitions
	var groups []*CandidateGroup
	for key, partitions := range byKey {
		threshold := f.thresholdForKey(ctx, key)
		var small []*manifest.PartitionRecord
		for _, p := range partitions {
			if p.SizeBytes < threshold {
				small = append(small, p)
			}
		}
		if len(small) >= 2 {
			groups = append(groups, &CandidateGroup{
				PartitionKey: key,
				Partitions:   small,
				Reason:       ReasonSmallPartitions,
			})
		}
	}

	return groups, nil
}

// FindOverflowPartitions returns partition keys that have more than the configured
// maximum number of partitions, along with their partitions.
func (f *CandidateFinder) FindOverflowPartitions(ctx context.Context) ([]*CandidateGroup, error) {
	return f.findOverflowPartitions(ctx)
}

// findOverflowPartitions finds partition keys with too many partitions.
func (f *CandidateFinder) findOverflowPartitions(ctx context.Context) ([]*CandidateGroup, error) {
	allPartitions, err := f.catalog.FindPartitions(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to query partitions: %w", err)
	}

	// Group all partitions by key and count per day
	type dayKey struct {
		partitionKey string
		day          string // YYYYMMDD
	}
	dayCounts := make(map[dayKey][]*manifest.PartitionRecord)

	for _, p := range allPartitions {
		dk := dayKey{
			partitionKey: p.PartitionKey,
			day:          p.CreatedAt.Format("20060102"),
		}
		dayCounts[dk] = append(dayCounts[dk], p)
	}

	var groups []*CandidateGroup
	seen := make(map[string]bool)

	for dk, partitions := range dayCounts {
		if int64(len(partitions)) > f.maxPartitionsPerKey && !seen[dk.partitionKey] {
			seen[dk.partitionKey] = true
			groups = append(groups, &CandidateGroup{
				PartitionKey: dk.partitionKey,
				Partitions:   partitions,
				Reason:       ReasonTooManyPartitions,
			})
		}
	}

	return groups, nil
}

// IsCompactionCandidate checks if a single partition is a candidate for compaction
// based on its size. Uses the static threshold; for per-key adaptive thresholds,
// use FindCandidates instead.
func (f *CandidateFinder) IsCompactionCandidate(p *manifest.PartitionRecord) bool {
	return p.SizeBytes < f.maxPartitionSize && p.CompactedInto == nil
}

// PartitionKeyExceedsThreshold checks if a partition key has more partitions than allowed
// for the given day.
func (f *CandidateFinder) PartitionKeyExceedsThreshold(ctx context.Context, partitionKey string, day time.Time) (bool, error) {
	allPartitions, err := f.catalog.FindPartitions(ctx, []manifest.Predicate{
		{Column: "partition_key", Operator: "=", Value: partitionKey},
	})
	if err != nil {
		return false, err
	}

	dayStr := day.Format("20060102")
	count := int64(0)
	for _, p := range allPartitions {
		if p.CreatedAt.Format("20060102") == dayStr {
			count++
		}
	}

	return count > f.maxPartitionsPerKey, nil
}
