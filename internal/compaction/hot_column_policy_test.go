package compaction

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestHotColumnPolicy_Create(t *testing.T) {
	// Create policy with defaults
	policy := NewHotColumnPolicy(nil, nil, 0, 0, 0, 0)

	// Verify defaults
	assert.Equal(t, int64(200), policy.createThreshold)
	assert.Equal(t, int64(10), policy.dropThreshold)
	assert.Equal(t, 5*time.Minute, policy.checkInterval)
	assert.Equal(t, 10, policy.maxHotColumns)
}

func TestHotColumnPolicy_CreateWithCustomValues(t *testing.T) {
	// Create policy with custom values
	policy := NewHotColumnPolicy(nil, nil, 100, 5, 10*time.Minute, 20)

	// Verify custom values
	assert.Equal(t, int64(100), policy.createThreshold)
	assert.Equal(t, int64(5), policy.dropThreshold)
	assert.Equal(t, 10*time.Minute, policy.checkInterval)
	assert.Equal(t, 20, policy.maxHotColumns)
}

func TestHotColumnPolicy_IsHotColumn(t *testing.T) {
	// Create policy
	policy := NewHotColumnPolicy(nil, nil, 200, 10, 0, 0)

	// Test hot column
	assert.True(t, policy.IsHotColumn("col1", 200))
	assert.True(t, policy.IsHotColumn("col1", 250))
	assert.False(t, policy.IsHotColumn("col1", 199))
}

func TestHotColumnPolicy_ShouldDropColumn(t *testing.T) {
	// Create policy
	policy := NewHotColumnPolicy(nil, nil, 200, 10, 0, 0)

	// Test drop column
	assert.True(t, policy.ShouldDropColumn("col1", 10))
	assert.True(t, policy.ShouldDropColumn("col1", 5))
	assert.False(t, policy.ShouldDropColumn("col1", 11))
}

func TestHotColumnPolicy_GetCheckInterval(t *testing.T) {
	// Create policy
	policy := NewHotColumnPolicy(nil, nil, 0, 0, 15*time.Minute, 0)

	// Verify check interval
	assert.Equal(t, 15*time.Minute, policy.GetCheckInterval())
}

func TestHotColumnPolicy_GetMaxHotColumns(t *testing.T) {
	// Create policy
	policy := NewHotColumnPolicy(nil, nil, 0, 0, 0, 15)

	// Verify max hot columns
	assert.Equal(t, 15, policy.GetMaxHotColumns())
}

func TestHotColumnPolicy_Evaluate(t *testing.T) {
	// Create policy
	policy := NewHotColumnPolicy(nil, nil, 200, 10, 0, 0)

	// Evaluate (returns empty for now)
	actions, err := policy.Evaluate(nil, 1)
	assert.NoError(t, err)
	assert.Empty(t, actions)
}

func TestHotColumnPolicy_LogDecision(t *testing.T) {
	// Create policy
	policy := NewHotColumnPolicy(nil, nil, 200, 10, 0, 0)

	// Log decision (no-op for now)
	policy.LogDecision("col1", 250, 200, HotColumnActionCreateSortedRun)
}
