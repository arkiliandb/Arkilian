package compaction

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLevelMerger_Create(t *testing.T) {
	// Create level merger
	merger := NewLevelMerger(nil)

	// Verify merger
	assert.NotNil(t, merger)
}

func TestLevelMerger_MergeL0ToL1(t *testing.T) {
	// Create level merger
	merger := NewLevelMerger(nil)

	// Test unimplemented
	err := merger.MergeL0ToL1(nil, 1)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not yet implemented")
}

func TestLevelMerger_MergeL1ToL2(t *testing.T) {
	// Create level merger
	merger := NewLevelMerger(nil)

	// Test unimplemented
	err := merger.MergeL1ToL2(nil, 1)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not yet implemented")
}
