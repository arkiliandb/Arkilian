// Package observability provides query statistics tracking for automated index creation and performance monitoring.
package observability

import (
	"sort"
	"sync"
	"time"
)

// QueryStats tracks predicate and JSON path frequency for automated index creation.
type QueryStats struct {
	mu            sync.RWMutex
	predicateFreq map[string]*ColumnStats
	jsonPathFreq  map[string]*ColumnStats
	window        time.Duration
}

// ColumnStats holds statistics for a column or JSON path.
type ColumnStats struct {
	Column    string
	Frequency int64
	LastSeen  time.Time
	Operators map[string]int // operator → count (e.g., "=" → 5, "IN" → 2)
}

// NewQueryStats creates a new query statistics tracker.
// window: time duration for pruning old entries (e.g., 1 hour)
func NewQueryStats(window time.Duration) *QueryStats {
	return &QueryStats{
		predicateFreq: make(map[string]*ColumnStats),
		jsonPathFreq:  make(map[string]*ColumnStats),
		window:        window,
	}
}

// RecordPredicate records a predicate access for a column.
// column: the column name (e.g., "user_id")
// operator: the comparison operator (e.g., "=", "IN", ">")
// This method is O(1) and thread-safe.
func (q *QueryStats) RecordPredicate(column, operator string) {
	q.mu.Lock()
	defer q.mu.Unlock()

	stats, exists := q.predicateFreq[column]
	if !exists {
		stats = &ColumnStats{
			Column:    column,
			Operators: make(map[string]int),
		}
		q.predicateFreq[column] = stats
	}

	stats.Frequency++
	stats.LastSeen = time.Now()
	stats.Operators[operator]++
}

// RecordJSONPath records a JSON path access.
// path: the JSON path (e.g., "$.user_agent")
// This method is O(1) and thread-safe.
func (q *QueryStats) RecordJSONPath(path string) {
	q.mu.Lock()
	defer q.mu.Unlock()

	stats, exists := q.jsonPathFreq[path]
	if !exists {
		stats = &ColumnStats{
			Column:    path,
			Operators: make(map[string]int),
		}
		q.jsonPathFreq[path] = stats
	}

	stats.Frequency++
	stats.LastSeen = time.Now()
}

// GetTopPredicates returns the top N predicates by frequency.
// Returns a copy of the stats sorted by frequency (descending).
func (q *QueryStats) GetTopPredicates(n int) []ColumnStats {
	q.mu.RLock()
	defer q.mu.RUnlock()

	if n <= 0 || len(q.predicateFreq) == 0 {
		return []ColumnStats{}
	}

	// Convert map to slice
	stats := make([]ColumnStats, 0, len(q.predicateFreq))
	for _, s := range q.predicateFreq {
		// Deep copy the ColumnStats to prevent external modification
		statsCopy := ColumnStats{
			Column:    s.Column,
			Frequency: s.Frequency,
			LastSeen:  s.LastSeen,
			Operators: make(map[string]int),
		}
		// Copy the operators map
		for op, count := range s.Operators {
			statsCopy.Operators[op] = count
		}
		stats = append(stats, statsCopy)
	}

	// Sort by frequency descending
	sort.Slice(stats, func(i, j int) bool {
		return stats[i].Frequency > stats[j].Frequency
	})

	// Return top N
	if n > len(stats) {
		n = len(stats)
	}
	return stats[:n]
}

// GetTopJSONPaths returns the top N JSON paths by frequency.
// Returns a copy of the stats sorted by frequency (descending).
func (q *QueryStats) GetTopJSONPaths(n int) []ColumnStats {
	q.mu.RLock()
	defer q.mu.RUnlock()

	if n <= 0 || len(q.jsonPathFreq) == 0 {
		return []ColumnStats{}
	}

	// Convert map to slice
	stats := make([]ColumnStats, 0, len(q.jsonPathFreq))
	for _, s := range q.jsonPathFreq {
		// Deep copy the ColumnStats to prevent external modification
		statsCopy := ColumnStats{
			Column:    s.Column,
			Frequency: s.Frequency,
			LastSeen:  s.LastSeen,
			Operators: make(map[string]int),
		}
		// Copy the operators map
		for op, count := range s.Operators {
			statsCopy.Operators[op] = count
		}
		stats = append(stats, statsCopy)
	}

	// Sort by frequency descending
	sort.Slice(stats, func(i, j int) bool {
		return stats[i].Frequency > stats[j].Frequency
	})

	// Return top N
	if n > len(stats) {
		n = len(stats)
	}
	return stats[:n]
}

// Prune removes entries where time.Since(LastSeen) > window.
// This should be called periodically (e.g., every 5 minutes).
func (q *QueryStats) Prune() {
	q.mu.Lock()
	defer q.mu.Unlock()

	now := time.Now()
	threshold := now.Add(-q.window)

	// Prune predicates
	for col, stats := range q.predicateFreq {
		if stats.LastSeen.Before(threshold) {
			delete(q.predicateFreq, col)
		}
	}

	// Prune JSON paths
	for path, stats := range q.jsonPathFreq {
		if stats.LastSeen.Before(threshold) {
			delete(q.jsonPathFreq, path)
		}
	}
}
