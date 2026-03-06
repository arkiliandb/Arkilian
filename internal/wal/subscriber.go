// Package wal provides a Raft-backed shared distributed write-ahead log for Arkilian V3.
package wal

import (
	"sync"
	"sync/atomic"
	"time"
)

// WALEvent represents a WAL event published to subscribers.
type WALEvent struct {
	LSN       uint64
	TableID   uint64
	Timestamp time.Time
	Entries   []*WalEntry
}

// QueryNodeWALBuffer is the WAL buffer for query nodes.
// It stores recent WAL entries for query-time WAL merge.
type QueryNodeWALBuffer struct {
	mu      sync.RWMutex
	entries map[uint64][]*WalEntry // tableID -> recent entries
	maxLSN  atomic.Uint64
	maxAge  time.Duration // evict after 2h
}

// NewQueryNodeWALBuffer creates a new query node WAL buffer.
func NewQueryNodeWALBuffer(maxAge time.Duration) *QueryNodeWALBuffer {
	return &QueryNodeWALBuffer{
		entries: make(map[uint64][]*WalEntry),
		maxAge:  maxAge,
	}
}

// OnWALEvent is called when a new WAL event is published.
func (b *QueryNodeWALBuffer) OnWALEvent(event WALEvent) {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Update max LSN
	if event.LSN > b.maxLSN.Load() {
		b.maxLSN.Store(event.LSN)
	}

	// Add entries to buffer
	for _, entry := range event.Entries {
		tableEntries := b.entries[entry.TableID]
		tableEntries = append(tableEntries, entry)
		b.entries[entry.TableID] = tableEntries
	}

	// Evict old entries
	b.evictOldEntries()
}

// GetRecentEntries returns entries for a table since a given LSN.
func (b *QueryNodeWALBuffer) GetRecentEntries(tableID uint64, sinceLSN uint64) []*WalEntry {
	b.mu.RLock()
	defer b.mu.RUnlock()

	var entries []*WalEntry
	tableEntries, ok := b.entries[tableID]
	if !ok {
		return entries
	}

	// Filter entries after sinceLSN
	for _, entry := range tableEntries {
		if entry.LSN > sinceLSN {
			entries = append(entries, entry)
		}
	}

	return entries
}

// MaxLSN returns the maximum LSN seen by this buffer.
func (b *QueryNodeWALBuffer) MaxLSN() uint64 {
	return b.maxLSN.Load()
}

// evictOldEntries removes entries older than maxAge.
func (b *QueryNodeWALBuffer) evictOldEntries() {
	// TODO: Implement proper eviction based on timestamps
	// For now, we just keep all entries since we don't have timestamps
}

// Clear removes all entries from the buffer.
func (b *QueryNodeWALBuffer) Clear() {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.entries = make(map[uint64][]*WalEntry)
	b.maxLSN.Store(0)
}
