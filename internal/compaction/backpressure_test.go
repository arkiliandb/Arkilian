package compaction

import (
	"testing"
	"time"
)

func TestBackpressureController_InitialState(t *testing.T) {
	bp := NewBackpressureController(DefaultBackpressureConfig())

	if bp.Concurrency() != 8 {
		t.Fatalf("expected initial concurrency 8, got %d", bp.Concurrency())
	}
	if bp.FailureRate() != 0 {
		t.Fatalf("expected initial failure rate 0, got %f", bp.FailureRate())
	}
	if bp.ShouldPause(0) {
		t.Fatal("should not pause with zero backlog")
	}
}

func TestBackpressureController_FailureRateTracking(t *testing.T) {
	bp := NewBackpressureController(BackpressureConfig{
		MaxConcurrency:   4,
		MinConcurrency:   1,
		FailureThreshold: 0.10,
		WindowDuration:   1 * time.Minute,
	})

	// Record 8 successes and 2 failures = 20% failure rate
	for i := 0; i < 8; i++ {
		bp.RecordSuccess()
	}
	bp.RecordFailure()
	bp.RecordFailure()

	rate := bp.FailureRate()
	if rate < 0.19 || rate > 0.21 {
		t.Fatalf("expected ~20%% failure rate, got %.2f%%", rate*100)
	}
}

func TestBackpressureController_BackoffOnHighFailureRate(t *testing.T) {
	bp := NewBackpressureController(BackpressureConfig{
		MaxConcurrency:   8,
		MinConcurrency:   1,
		FailureThreshold: 0.10,
		WindowDuration:   1 * time.Minute,
	})

	// Record 50% failure rate
	for i := 0; i < 5; i++ {
		bp.RecordSuccess()
		bp.RecordFailure()
	}

	bp.AdjustConcurrency()

	// Should have halved from 8 → 4
	if bp.Concurrency() != 4 {
		t.Fatalf("expected concurrency 4 after backoff, got %d", bp.Concurrency())
	}

	// Another adjustment with same failure rate should halve again → 2
	bp.AdjustConcurrency()
	if bp.Concurrency() != 2 {
		t.Fatalf("expected concurrency 2 after second backoff, got %d", bp.Concurrency())
	}
}

func TestBackpressureController_RampUpOnLowFailureRate(t *testing.T) {
	bp := NewBackpressureController(BackpressureConfig{
		MaxConcurrency:   8,
		MinConcurrency:   1,
		FailureThreshold: 0.20,
		WindowDuration:   1 * time.Minute,
	})

	// Force concurrency down first
	bp.currentConcurrency.Store(2)

	// Record all successes (0% failure rate, well below threshold/2 = 10%)
	for i := 0; i < 10; i++ {
		bp.RecordSuccess()
	}

	bp.AdjustConcurrency()

	// Should ramp up: 2 + max(2/4, 1) = 2 + 1 = 3
	if bp.Concurrency() != 3 {
		t.Fatalf("expected concurrency 3 after ramp-up, got %d", bp.Concurrency())
	}
}

func TestBackpressureController_MinConcurrencyFloor(t *testing.T) {
	bp := NewBackpressureController(BackpressureConfig{
		MaxConcurrency:   4,
		MinConcurrency:   2,
		FailureThreshold: 0.05,
		WindowDuration:   1 * time.Minute,
	})

	// Record 100% failure rate
	for i := 0; i < 10; i++ {
		bp.RecordFailure()
	}

	// Multiple backoffs should not go below min
	for i := 0; i < 10; i++ {
		bp.AdjustConcurrency()
	}

	if bp.Concurrency() < 2 {
		t.Fatalf("concurrency %d dropped below min 2", bp.Concurrency())
	}
}

func TestBackpressureController_MaxConcurrencyCeiling(t *testing.T) {
	bp := NewBackpressureController(BackpressureConfig{
		MaxConcurrency:   4,
		MinConcurrency:   1,
		FailureThreshold: 0.50,
		WindowDuration:   1 * time.Minute,
	})

	// Record all successes
	for i := 0; i < 20; i++ {
		bp.RecordSuccess()
	}

	// Multiple ramp-ups should not exceed max
	for i := 0; i < 20; i++ {
		bp.AdjustConcurrency()
	}

	if bp.Concurrency() > 4 {
		t.Fatalf("concurrency %d exceeded max 4", bp.Concurrency())
	}
}

func TestBackpressureController_ShouldPause(t *testing.T) {
	bp := NewBackpressureController(BackpressureConfig{
		MaxConcurrency:   4,
		MinConcurrency:   1,
		FailureThreshold: 0.10,
		WindowDuration:   1 * time.Minute,
	})

	// No backlog → never pause
	bp.RecordFailure()
	if bp.ShouldPause(0) {
		t.Fatal("should not pause with zero backlog")
	}

	// High failure rate + backlog → pause
	for i := 0; i < 10; i++ {
		bp.RecordFailure()
	}
	if !bp.ShouldPause(5) {
		t.Fatal("should pause with high failure rate and backlog")
	}

	// Low failure rate + backlog → don't pause
	bp2 := NewBackpressureController(BackpressureConfig{
		MaxConcurrency:   4,
		MinConcurrency:   1,
		FailureThreshold: 0.50,
		WindowDuration:   1 * time.Minute,
	})
	for i := 0; i < 10; i++ {
		bp2.RecordSuccess()
	}
	if bp2.ShouldPause(5) {
		t.Fatal("should not pause with low failure rate")
	}
}

func TestBackpressureController_WindowExpiry(t *testing.T) {
	bp := NewBackpressureController(BackpressureConfig{
		MaxConcurrency:   4,
		MinConcurrency:   1,
		FailureThreshold: 0.10,
		WindowDuration:   50 * time.Millisecond, // very short window for testing
	})

	// Record failures
	for i := 0; i < 10; i++ {
		bp.RecordFailure()
	}

	if bp.FailureRate() != 1.0 {
		t.Fatalf("expected 100%% failure rate, got %.2f%%", bp.FailureRate()*100)
	}

	// Wait for window to expire
	time.Sleep(100 * time.Millisecond)

	if bp.FailureRate() != 0 {
		t.Fatalf("expected 0%% failure rate after window expiry, got %.2f%%", bp.FailureRate()*100)
	}
}

func TestBackpressureController_Stats(t *testing.T) {
	bp := NewBackpressureController(BackpressureConfig{
		MaxConcurrency:   4,
		MinConcurrency:   1,
		FailureThreshold: 0.10,
		WindowDuration:   1 * time.Minute,
	})

	bp.RecordSuccess()
	bp.RecordSuccess()
	bp.RecordFailure()

	stats := bp.Stats()
	if stats.AttemptsInWindow != 3 {
		t.Fatalf("expected 3 attempts, got %d", stats.AttemptsInWindow)
	}
	if stats.FailuresInWindow != 1 {
		t.Fatalf("expected 1 failure, got %d", stats.FailuresInWindow)
	}
	if stats.CurrentConcurrency != 4 {
		t.Fatalf("expected concurrency 4, got %d", stats.CurrentConcurrency)
	}
}
