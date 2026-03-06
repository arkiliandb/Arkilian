package search

import (
	"testing"

	"github.com/arkilian/arkilian/internal/format"
	"github.com/stretchr/testify/assert"
)

func TestPGMIndexSearcher_Create(t *testing.T) {
	// Create PGM index searcher
	searcher := NewPGMIndexSearcher()

	// Verify searcher
	assert.NotNil(t, searcher)
}

func TestPGMIndexSearcher_AddIndex(t *testing.T) {
	// Create PGM index searcher
	searcher := NewPGMIndexSearcher()

	// Add index
	index := format.BuildPGMIndex(nil, 64)
	searcher.AddIndex(1, 1, index)

	// Verify index was added
	assert.NotNil(t, searcher.indexes[1])
	assert.NotNil(t, searcher.indexes[1][1])
}

func TestPGMIndexSearcher_Search(t *testing.T) {
	// Create PGM index searcher
	searcher := NewPGMIndexSearcher()

	// Test unimplemented
	pos, lo, hi, err := searcher.Search(nil, 1, 1, nil)
	assert.NoError(t, err)
	assert.Equal(t, 0, pos)
	assert.Equal(t, 0, lo)
	assert.Equal(t, 0, hi)
}

func TestPGMIndexSearcher_SearchWithBounds(t *testing.T) {
	// Create PGM index searcher
	searcher := NewPGMIndexSearcher()

	// Test with unknown index
	pos, lo, hi := searcher.SearchWithBounds(1, 1, nil)

	// Verify unknown index returns default bounds
	assert.Equal(t, 0, pos)
	assert.Equal(t, 0, lo)
	assert.Equal(t, 0, hi)
}

func TestPGMIndexSearcher_GetPruneStats(t *testing.T) {
	// Create PGM index searcher
	searcher := NewPGMIndexSearcher()

	// Get stats
	stats := searcher.GetPruneStats()
	assert.NotNil(t, stats)
}
