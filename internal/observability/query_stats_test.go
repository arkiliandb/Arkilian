// Package observability provides query statistics tracking for automated index creation and performance monitoring.
package observability

import (
	"sync"
	"testing"
	"time"
)

// TestRecordPredicateConcurrent tests concurrent RecordPredicate calls for race conditions.
func TestRecordPredicateConcurrent(t *testing.T) {
	qs := NewQueryStats(1 * time.Hour)
	var wg sync.WaitGroup
	numGoroutines := 10
	recordsPerGoroutine := 100

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < recordsPerGoroutine; j++ {
				qs.RecordPredicate("user_id", "=")
				qs.RecordPredicate("tenant_id", "IN")
				qs.RecordPredicate("event_time", ">")
			}
		}(i)
	}

	wg.Wait()

	// Verify counts
	top := qs.GetTopPredicates(10)
	if len(top) != 3 {
		t.Errorf("expected 3 predicates, got %d", len(top))
	}

	// Each predicate should have been recorded numGoroutines * recordsPerGoroutine times
	expectedFreq := int64(numGoroutines * recordsPerGoroutine)
	for _, stat := range top {
		if stat.Frequency != expectedFreq {
			t.Errorf("expected frequency %d for %s, got %d", expectedFreq, stat.Column, stat.Frequency)
		}
	}
}

// TestGetTopPredicatesOrdering tests that GetTopPredicates returns results sorted by frequency.
func TestGetTopPredicatesOrdering(t *testing.T) {
	qs := NewQueryStats(1 * time.Hour)

	// Record predicates with different frequencies
	for i := 0; i < 10; i++ {
		qs.RecordPredicate("user_id", "=")
	}
	for i := 0; i < 5; i++ {
		qs.RecordPredicate("tenant_id", "IN")
	}
	for i := 0; i < 20; i++ {
		qs.RecordPredicate("event_time", ">")
	}

	top := qs.GetTopPredicates(3)
	if len(top) != 3 {
		t.Errorf("expected 3 predicates, got %d", len(top))
	}

	// Should be ordered: event_time (20), user_id (10), tenant_id (5)
	if top[0].Column != "event_time" || top[0].Frequency != 20 {
		t.Errorf("expected event_time with frequency 20, got %s with %d", top[0].Column, top[0].Frequency)
	}
	if top[1].Column != "user_id" || top[1].Frequency != 10 {
		t.Errorf("expected user_id with frequency 10, got %s with %d", top[1].Column, top[1].Frequency)
	}
	if top[2].Column != "tenant_id" || top[2].Frequency != 5 {
		t.Errorf("expected tenant_id with frequency 5, got %s with %d", top[2].Column, top[2].Frequency)
	}
}

// TestPruneRemovesOldEntries tests that Prune removes entries older than the window.
func TestPruneRemovesOldEntries(t *testing.T) {
	window := 100 * time.Millisecond
	qs := NewQueryStats(window)

	// Record a predicate
	qs.RecordPredicate("user_id", "=")

	// Verify it exists
	top := qs.GetTopPredicates(10)
	if len(top) != 1 {
		t.Errorf("expected 1 predicate before prune, got %d", len(top))
	}

	// Wait for the window to expire
	time.Sleep(window + 50*time.Millisecond)

	// Prune
	qs.Prune()

	// Verify it's gone
	top = qs.GetTopPredicates(10)
	if len(top) != 0 {
		t.Errorf("expected 0 predicates after prune, got %d", len(top))
	}
}

// TestRecordPredicateTrackingOperators tests that RecordPredicate tracks operator distribution.
func TestRecordPredicateTrackingOperators(t *testing.T) {
	qs := NewQueryStats(1 * time.Hour)

	// Record predicates with different operators
	for i := 0; i < 5; i++ {
		qs.RecordPredicate("user_id", "=")
	}
	for i := 0; i < 3; i++ {
		qs.RecordPredicate("user_id", "IN")
	}
	for i := 0; i < 2; i++ {
		qs.RecordPredicate("user_id", ">")
	}

	top := qs.GetTopPredicates(1)
	if len(top) != 1 {
		t.Errorf("expected 1 predicate, got %d", len(top))
	}

	stat := top[0]
	if stat.Frequency != 10 {
		t.Errorf("expected frequency 10, got %d", stat.Frequency)
	}

	// Check operator distribution
	if stat.Operators["="] != 5 {
		t.Errorf("expected 5 '=' operators, got %d", stat.Operators["="])
	}
	if stat.Operators["IN"] != 3 {
		t.Errorf("expected 3 'IN' operators, got %d", stat.Operators["IN"])
	}
	if stat.Operators[">"] != 2 {
		t.Errorf("expected 2 '>' operators, got %d", stat.Operators[">"])
	}
}

// TestRecordJSONPathFrequency tests that RecordJSONPath tracks JSON path frequency.
func TestRecordJSONPathFrequency(t *testing.T) {
	qs := NewQueryStats(1 * time.Hour)

	// Record JSON paths with different frequencies
	for i := 0; i < 15; i++ {
		qs.RecordJSONPath("$.user_agent")
	}
	for i := 0; i < 8; i++ {
		qs.RecordJSONPath("$.device_id")
	}
	for i := 0; i < 3; i++ {
		qs.RecordJSONPath("$.ip_address")
	}

	top := qs.GetTopJSONPaths(3)
	if len(top) != 3 {
		t.Errorf("expected 3 JSON paths, got %d", len(top))
	}

	// Should be ordered: $.user_agent (15), $.device_id (8), $.ip_address (3)
	if top[0].Column != "$.user_agent" || top[0].Frequency != 15 {
		t.Errorf("expected $.user_agent with frequency 15, got %s with %d", top[0].Column, top[0].Frequency)
	}
	if top[1].Column != "$.device_id" || top[1].Frequency != 8 {
		t.Errorf("expected $.device_id with frequency 8, got %s with %d", top[1].Column, top[1].Frequency)
	}
	if top[2].Column != "$.ip_address" || top[2].Frequency != 3 {
		t.Errorf("expected $.ip_address with frequency 3, got %s with %d", top[2].Column, top[2].Frequency)
	}
}

// TestGetTopPredicatesEmpty tests GetTopPredicates with no data.
func TestGetTopPredicatesEmpty(t *testing.T) {
	qs := NewQueryStats(1 * time.Hour)
	top := qs.GetTopPredicates(10)
	if len(top) != 0 {
		t.Errorf("expected 0 predicates, got %d", len(top))
	}
}

// TestGetTopJSONPathsEmpty tests GetTopJSONPaths with no data.
func TestGetTopJSONPathsEmpty(t *testing.T) {
	qs := NewQueryStats(1 * time.Hour)
	top := qs.GetTopJSONPaths(10)
	if len(top) != 0 {
		t.Errorf("expected 0 JSON paths, got %d", len(top))
	}
}

// TestGetTopPredicatesLimitExceedsData tests GetTopPredicates when n exceeds available data.
func TestGetTopPredicatesLimitExceedsData(t *testing.T) {
	qs := NewQueryStats(1 * time.Hour)
	qs.RecordPredicate("user_id", "=")
	qs.RecordPredicate("tenant_id", "IN")

	top := qs.GetTopPredicates(100)
	if len(top) != 2 {
		t.Errorf("expected 2 predicates, got %d", len(top))
	}
}
