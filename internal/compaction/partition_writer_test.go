package compaction

import (
	"testing"

	"github.com/arkilian/arkilian/internal/wal"
	"github.com/stretchr/testify/assert"
)

func TestPartitionWriter_Create(t *testing.T) {
	// Create partition writer
	writer := NewPartitionWriter(nil, 128*1024*1024, "zstd")

	// Verify writer
	assert.NotNil(t, writer)
	assert.Equal(t, int64(128*1024*1024), writer.targetSize)
}

func TestPartitionWriter_EmptyEntries(t *testing.T) {
	// Create partition writer
	writer := NewPartitionWriter(nil, 128*1024*1024, "zstd")

	// Write empty entries
	err := writer.WritePrimaryPartition(nil, 1, []*wal.WalEntry{})
	assert.NoError(t, err)
}

func TestPartitionWriter_CompressionTypes(t *testing.T) {
	// Test different compression types
	for _, comp := range []string{"zstd", "lz4", "snappy"} {
		writer := NewPartitionWriter(nil, 128*1024*1024, comp)
		assert.NotNil(t, writer)
	}
}
