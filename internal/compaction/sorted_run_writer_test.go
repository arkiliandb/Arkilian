package compaction

import (
	"testing"

	"github.com/arkilian/arkilian/internal/wal"
	"github.com/stretchr/testify/assert"
)

func TestSortedRunWriter_Create(t *testing.T) {
	// Create sorted run writer
	writer := NewSortedRunWriter(nil, 128*1024*1024, "zstd")

	// Verify writer
	assert.NotNil(t, writer)
	assert.Equal(t, int64(128*1024*1024), writer.targetSize)
}

func TestSortedRunWriter_EmptyEntries(t *testing.T) {
	// Create sorted run writer
	writer := NewSortedRunWriter(nil, 128*1024*1024, "zstd")

	// Write empty entries
	err := writer.WriteSortedRun(nil, 1, "col1", []*wal.WalEntry{})
	assert.NoError(t, err)
}

func TestSortedRunWriter_CompressionTypes(t *testing.T) {
	// Test different compression types
	for _, comp := range []string{"zstd", "lz4", "snappy"} {
		writer := NewSortedRunWriter(nil, 128*1024*1024, comp)
		assert.NotNil(t, writer)
	}
}

func TestSortedRunWriter_WriteMergedSortedRun(t *testing.T) {
	// Create sorted run writer
	writer := NewSortedRunWriter(nil, 128*1024*1024, "zstd")

	// Write merged sorted run
	err := writer.WriteMergedSortedRun(nil, 1, "col1", []*wal.WalEntry{})
	assert.NoError(t, err)
}
