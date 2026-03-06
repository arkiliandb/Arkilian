// Package compaction provides the hourly compaction engine for Arkilian V3.
package compaction

import (
	"runtime"
	"sort"

	"github.com/arkilian/arkilian/internal/wal"
)

// ExternalMergeSorter sorts WAL entries by a specific column using external merge sort.
type ExternalMergeSorter struct {
	ramBudget int64    // Per-table RAM budget (e.g., 2GB)
	tmpDir    string   // NVMe temp directory
	workers   int
}

// NewExternalMergeSorter creates a new external merge sorter.
func NewExternalMergeSorter(ramBudget int64, tmpDir string, workers int) *ExternalMergeSorter {
	if workers == 0 {
		workers = runtime.NumCPU() * 2
	}
	return &ExternalMergeSorter{
		ramBudget: ramBudget,
		tmpDir:    tmpDir,
		workers:   workers,
	}
}

// SortByColumn sorts WAL entries by a specific column.
// Fast path: if data fits in RAM, use slices.SortFunc.
// Slow path: external merge sort — chunk into RAM-sized runs → sort each → k-way merge.
func (s *ExternalMergeSorter) SortByColumn(entries []*wal.WalEntry, column string) ([]*wal.WalEntry, error) {
	if len(entries) == 0 {
		return entries, nil
	}

	// Fast path: if data fits in RAM, use in-memory sort
	if s.fitsInRAM(entries) {
		return s.sortInMemory(entries, column), nil
	}

	// Slow path: external merge sort
	return s.externalMergeSort(entries, column)
}

// fitsInRAM checks if the data fits in the RAM budget.
func (s *ExternalMergeSorter) fitsInRAM(entries []*wal.WalEntry) bool {
	// Estimate memory usage (rough approximation)
	// Each entry is roughly 100 bytes + payload
	var totalSize int64
	for _, entry := range entries {
		totalSize += 100 + int64(len(entry.Payload))
	}
	return totalSize <= s.ramBudget
}

// sortInMemory sorts entries in memory using slices.SortFunc.
func (s *ExternalMergeSorter) sortInMemory(entries []*wal.WalEntry, column string) []*wal.WalEntry {
	switch column {
	case "lsn":
		sort.Slice(entries, func(i, j int) bool {
			return entries[i].LSN < entries[j].LSN
		})
	case "table_id":
		sort.Slice(entries, func(i, j int) bool {
			return entries[i].TableID < entries[j].TableID
		})
	default:
		// Default to sorting by LSN
		sort.Slice(entries, func(i, j int) bool {
			return entries[i].LSN < entries[j].LSN
		})
	}
	return entries
}

// externalMergeSort performs external merge sort.
func (s *ExternalMergeSorter) externalMergeSort(entries []*wal.WalEntry, column string) ([]*wal.WalEntry, error) {
	// TODO: Implement external merge sort
	// 1. Split entries into chunks that fit in RAM
	// 2. Sort each chunk and write to temp files
	// 3. Merge sorted chunks using k-way merge

	// For now, use in-memory sort
	return s.sortInMemory(entries, column), nil
}

// SortEntriesByLSN sorts entries by LSN (simple wrapper for testing).
func SortEntriesByLSN(entries []*wal.WalEntry) []*wal.WalEntry {
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].LSN < entries[j].LSN
	})
	return entries
}

// SortEntriesByTableID sorts entries by table ID (simple wrapper for testing).
func SortEntriesByTableID(entries []*wal.WalEntry) []*wal.WalEntry {
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].TableID < entries[j].TableID
	})
	return entries
}
