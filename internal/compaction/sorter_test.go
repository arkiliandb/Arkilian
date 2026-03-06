package compaction

import (
	"testing"

	"github.com/arkilian/arkilian/internal/wal"
	"github.com/stretchr/testify/assert"
)

func TestExternalMergeSorter_SortByLSN(t *testing.T) {
	// Create sorter
	sorter := NewExternalMergeSorter(2*1024*1024*1024, "/tmp", 0)

	// Create entries with unsorted LSNs
	entries := []*wal.WalEntry{
		{LSN: 5},
		{LSN: 2},
		{LSN: 8},
		{LSN: 1},
		{LSN: 9},
	}

	// Sort
	sorted, err := sorter.SortByColumn(entries, "lsn")
	assert.NoError(t, err)

	// Verify sorted by LSN
	assert.Equal(t, uint64(1), sorted[0].LSN)
	assert.Equal(t, uint64(2), sorted[1].LSN)
	assert.Equal(t, uint64(5), sorted[2].LSN)
	assert.Equal(t, uint64(8), sorted[3].LSN)
	assert.Equal(t, uint64(9), sorted[4].LSN)
}

func TestExternalMergeSorter_SortByTableID(t *testing.T) {
	// Create sorter
	sorter := NewExternalMergeSorter(2*1024*1024*1024, "/tmp", 0)

	// Create entries with unsorted table IDs
	entries := []*wal.WalEntry{
		{TableID: 5},
		{TableID: 2},
		{TableID: 8},
		{TableID: 1},
		{TableID: 9},
	}

	// Sort
	sorted, err := sorter.SortByColumn(entries, "table_id")
	assert.NoError(t, err)

	// Verify sorted by table ID
	assert.Equal(t, uint64(1), sorted[0].TableID)
	assert.Equal(t, uint64(2), sorted[1].TableID)
	assert.Equal(t, uint64(5), sorted[2].TableID)
	assert.Equal(t, uint64(8), sorted[3].TableID)
	assert.Equal(t, uint64(9), sorted[4].TableID)
}

func TestExternalMergeSorter_EmptyEntries(t *testing.T) {
	// Create sorter
	sorter := NewExternalMergeSorter(2*1024*1024*1024, "/tmp", 0)

	// Sort empty entries
	sorted, err := sorter.SortByColumn([]*wal.WalEntry{}, "lsn")
	assert.NoError(t, err)

	// Verify empty
	assert.Empty(t, sorted)
}

func TestExternalMergeSorter_SingleEntry(t *testing.T) {
	// Create sorter
	sorter := NewExternalMergeSorter(2*1024*1024*1024, "/tmp", 0)

	// Create single entry
	entries := []*wal.WalEntry{
		{LSN: 5},
	}

	// Sort
	sorted, err := sorter.SortByColumn(entries, "lsn")
	assert.NoError(t, err)

	// Verify single entry
	assert.Equal(t, 1, len(sorted))
	assert.Equal(t, uint64(5), sorted[0].LSN)
}

func TestExternalMergeSorter_LargeEntries(t *testing.T) {
	// Create sorter with small RAM budget to force external sort
	sorter := NewExternalMergeSorter(1000, "/tmp", 0)

	// Create many entries
	var entries []*wal.WalEntry
	for i := uint64(100); i > 0; i-- {
		entries = append(entries, &wal.WalEntry{
			LSN:     i,
			Payload: make([]byte, 10), // Small payload
		})
	}

	// Sort
	sorted, err := sorter.SortByColumn(entries, "lsn")
	assert.NoError(t, err)

	// Verify sorted by LSN (ascending)
	for i, entry := range sorted {
		expectedLSN := uint64(i + 1)
		assert.Equal(t, expectedLSN, entry.LSN)
	}
}

func TestSortEntriesByLSN(t *testing.T) {
	// Create entries
	entries := []*wal.WalEntry{
		{LSN: 5},
		{LSN: 2},
		{LSN: 8},
		{LSN: 1},
	}

	// Sort
	sorted := SortEntriesByLSN(entries)

	// Verify sorted
	assert.Equal(t, uint64(1), sorted[0].LSN)
	assert.Equal(t, uint64(2), sorted[1].LSN)
	assert.Equal(t, uint64(5), sorted[2].LSN)
	assert.Equal(t, uint64(8), sorted[3].LSN)
}

func TestSortEntriesByTableID(t *testing.T) {
	// Create entries
	entries := []*wal.WalEntry{
		{TableID: 5},
		{TableID: 2},
		{TableID: 8},
		{TableID: 1},
	}

	// Sort
	sorted := SortEntriesByTableID(entries)

	// Verify sorted
	assert.Equal(t, uint64(1), sorted[0].TableID)
	assert.Equal(t, uint64(2), sorted[1].TableID)
	assert.Equal(t, uint64(5), sorted[2].TableID)
	assert.Equal(t, uint64(8), sorted[3].TableID)
}
