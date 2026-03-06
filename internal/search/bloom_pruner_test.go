package search

import (
	"testing"

	"github.com/arkilian/arkilian/internal/format"
	"github.com/stretchr/testify/assert"
)

func TestBloomPruner_Create(t *testing.T) {
	// Create bloom pruner
	pruner := NewBloomPruner()

	// Verify pruner
	assert.NotNil(t, pruner)
}

func TestBloomPruner_AddFilter(t *testing.T) {
	// Create bloom pruner
	pruner := NewBloomPruner()

	// Add filter
	filter := format.NewBlockedCuckooFilter(1000)
	pruner.AddFilter(1, 1, filter)

	// Verify filter was added
	assert.NotNil(t, pruner.filters[1])
	assert.NotNil(t, pruner.filters[1][1])
}

func TestBloomPruner_MayContain(t *testing.T) {
	// Create bloom pruner
	pruner := NewBloomPruner()

	// Add filter
	filter := format.NewBlockedCuckooFilter(1000)
	filter.Insert([]byte("test"))
	pruner.AddFilter(1, 1, filter)

	// Check membership
	result := pruner.MayContain(1, 1, []byte("test"))

	// Verify membership check
	assert.True(t, result)
}

func TestBloomPruner_MayContainUnknown(t *testing.T) {
	// Create bloom pruner
	pruner := NewBloomPruner()

	// Check membership for unknown filter
	result := pruner.MayContain(1, 1, []byte("test"))

	// Verify unknown filter returns true
	assert.True(t, result)
}

func TestBloomPruner_Prune(t *testing.T) {
	// Create bloom pruner
	pruner := NewBloomPruner()

	// Test unimplemented
	partitions, err := pruner.Prune(nil, 1, nil)
	assert.NoError(t, err)
	assert.NotNil(t, partitions)
}

func TestBloomPruner_GetPruneStats(t *testing.T) {
	// Create bloom pruner
	pruner := NewBloomPruner()

	// Get stats
	stats := pruner.GetPruneStats()
	assert.NotNil(t, stats)
}
