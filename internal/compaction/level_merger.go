// Package compaction provides the hourly compaction engine for Arkilian V3.
package compaction

import (
	"context"
	"fmt"

	"github.com/arkilian/arkilian/internal/storage"
)

// LevelMerger merges partitions across compaction levels.
type LevelMerger struct {
	s3client *storage.S3Storage
}

// NewLevelMerger creates a new level merger.
func NewLevelMerger(s3 *storage.S3Storage) *LevelMerger {
	return &LevelMerger{
		s3client: s3,
	}
}

// MergeL0ToL1 merges overlapping L0 files into non-overlapping L1 files.
// Key invariant: L1 files have NON-OVERLAPPING cluster-key ranges.
func (m *LevelMerger) MergeL0ToL1(ctx context.Context, tableID uint64) error {
	// TODO: Implement L0→L1 merge
	// 1. Find all L0 partitions for the table
	// 2. Merge overlapping partitions
	// 3. Write non-overlapping L1 partitions
	// 4. Update catalog

	return fmt.Errorf("MergeL0ToL1 not yet implemented")
}

// MergeL1ToL2 merges L1 files for long-term storage.
// Key invariant: L2 files have NON-OVERLAPPING cluster-key ranges.
func (m *LevelMerger) MergeL1ToL2(ctx context.Context, tableID uint64) error {
	// TODO: Implement L1→L2 merge
	// 1. Find all L1 partitions for the table
	// 2. Merge overlapping partitions
	// 3. Write non-overlapping L2 partitions
	// 4. Update catalog

	return fmt.Errorf("MergeL1ToL2 not yet implemented")
}
