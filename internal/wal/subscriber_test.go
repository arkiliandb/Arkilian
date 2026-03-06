package wal

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestQueryNodeWALBuffer_OnWALEvent(t *testing.T) {
	// Create buffer with 2h max age
	buffer := NewQueryNodeWALBuffer(2 * time.Hour)

	// Create event
	event := WALEvent{
		LSN:       2,
		TableID:   123,
		Timestamp: time.Now(),
		Entries: []*WalEntry{
			{LSN: 1, TableID: 123},
			{LSN: 2, TableID: 123},
		},
	}

	// Publish event
	buffer.OnWALEvent(event)

	// Verify max LSN
	assert.Equal(t, uint64(2), buffer.MaxLSN())
}

func TestQueryNodeWALBuffer_GetRecentEntries(t *testing.T) {
	// Create buffer
	buffer := NewQueryNodeWALBuffer(2 * time.Hour)

	// Publish some entries
	for i := uint64(1); i <= 10; i++ {
		event := WALEvent{
			LSN:       i,
			TableID:   123,
			Timestamp: time.Now(),
			Entries: []*WalEntry{
				{LSN: i, TableID: 123},
			},
		}
		buffer.OnWALEvent(event)
	}

	// Get entries since LSN 5
	entries := buffer.GetRecentEntries(123, 5)

	// Should return entries 6-10
	assert.Equal(t, 5, len(entries))
	for i, entry := range entries {
		assert.Equal(t, uint64(6+i), entry.LSN)
	}
}

func TestQueryNodeWALBuffer_MultipleTables(t *testing.T) {
	// Create buffer
	buffer := NewQueryNodeWALBuffer(2 * time.Hour)

	// Publish entries for table 1
	for i := uint64(1); i <= 5; i++ {
		event := WALEvent{
			LSN:       i,
			TableID:   1,
			Timestamp: time.Now(),
			Entries: []*WalEntry{
				{LSN: i, TableID: 1},
			},
		}
		buffer.OnWALEvent(event)
	}

	// Publish entries for table 2
	for i := uint64(1); i <= 3; i++ {
		event := WALEvent{
			LSN:       i,
			TableID:   2,
			Timestamp: time.Now(),
			Entries: []*WalEntry{
				{LSN: i, TableID: 2},
			},
		}
		buffer.OnWALEvent(event)
	}

	// Get entries for table 1
	entries1 := buffer.GetRecentEntries(1, 0)
	assert.Equal(t, 5, len(entries1))

	// Get entries for table 2
	entries2 := buffer.GetRecentEntries(2, 0)
	assert.Equal(t, 3, len(entries2))

	// Get entries for non-existent table
	entries3 := buffer.GetRecentEntries(999, 0)
	assert.Empty(t, entries3)
}

func TestQueryNodeWALBuffer_Clear(t *testing.T) {
	// Create buffer
	buffer := NewQueryNodeWALBuffer(2 * time.Hour)

	// Publish some entries
	event := WALEvent{
		LSN:       1,
		TableID:   123,
		Timestamp: time.Now(),
		Entries: []*WalEntry{
			{LSN: 1, TableID: 123},
		},
	}
	buffer.OnWALEvent(event)

	// Verify entries exist
	assert.Equal(t, uint64(1), buffer.MaxLSN())
	entries := buffer.GetRecentEntries(123, 0)
	assert.Equal(t, 1, len(entries))

	// Clear buffer
	buffer.Clear()

	// Verify buffer is empty
	assert.Equal(t, uint64(0), buffer.MaxLSN())
	entries = buffer.GetRecentEntries(123, 0)
	assert.Empty(t, entries)
}

func TestQueryNodeWALBuffer_Concurrent(t *testing.T) {
	// Create buffer
	buffer := NewQueryNodeWALBuffer(2 * time.Hour)

	// Run concurrent operations
	done := make(chan struct{})

	// Writer goroutines
	for i := 0; i < 10; i++ {
		go func(tableID uint64) {
			for j := uint64(1); j <= 100; j++ {
				event := WALEvent{
					LSN:       j,
					TableID:   tableID,
					Timestamp: time.Now(),
					Entries: []*WalEntry{
						{LSN: j, TableID: tableID},
					},
				}
				buffer.OnWALEvent(event)
			}
			done <- struct{}{}
		}(uint64(i + 1))
	}

	// Reader goroutines
	for i := 0; i < 5; i++ {
		go func(tableID uint64) {
			for j := 0; j < 100; j++ {
				_ = buffer.GetRecentEntries(tableID, 0)
				_ = buffer.MaxLSN()
			}
			done <- struct{}{}
		}(uint64(i + 1))
	}

	// Wait for all goroutines
	for i := 0; i < 15; i++ {
		<-done
	}

	// Verify max LSN
	assert.GreaterOrEqual(t, buffer.MaxLSN(), uint64(100))
}
