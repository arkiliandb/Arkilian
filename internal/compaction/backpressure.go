package compaction

import (
	"sync"
	"sync/atomic"
	"time"
)

// BackpressureController tracks recent compaction failures and dynamically adjusts
// concurrency to prevent backlog growth under degraded storage conditions.
//
// When the failure rate exceeds the threshold, concurrency is halved (backoff).
// When the failure rate drops below half the threshold, concurrency ramps up by 20%.
// If a backlog exists AND the failure rate is high, compaction pauses entirely
// until the system stabilizes.
type BackpressureController struct {
	maxConcurrency int32
	minConcurrency int32
	threshold      float64 // failure rate threshold (e.g., 0.05 = 5%)

	currentConcurrency atomic.Int32

	mu       sync.Mutex
	attempts []attemptRecord
	window   time.Duration
}

type attemptRecord struct {
	at      time.Time
	success bool
}

// BackpressureConfig holds configuration for the backpressure controller.
type BackpressureConfig struct {
	// MaxConcurrency is the upper bound for concurrent compaction goroutines (default: 4).
	MaxConcurrency int `json:"max_concurrency" yaml:"max_concurrency"`

	// MinConcurrency is the lower bound (default: 1).
	MinConcurrency int `json:"min_concurrency" yaml:"min_concurrency"`

	// FailureThreshold is the failure rate above which backoff triggers (default: 0.10 = 10%).
	FailureThreshold float64 `json:"failure_threshold" yaml:"failure_threshold"`

	// WindowDuration is the sliding window for tracking failures (default: 5m).
	WindowDuration time.Duration `json:"window_duration" yaml:"window_duration"`
}

// DefaultBackpressureConfig returns sensible defaults.
func DefaultBackpressureConfig() BackpressureConfig {
	return BackpressureConfig{
		MaxConcurrency:   8,
		MinConcurrency:   1,
		FailureThreshold: 0.05,
		WindowDuration:   10 * time.Minute,
	}
}

// NewBackpressureController creates a new controller with the given config.
func NewBackpressureController(cfg BackpressureConfig) *BackpressureController {
	if cfg.MaxConcurrency <= 0 {
		cfg.MaxConcurrency = 8
	}
	if cfg.MinConcurrency <= 0 {
		cfg.MinConcurrency = 1
	}
	if cfg.FailureThreshold <= 0 {
		cfg.FailureThreshold = 0.05
	}
	if cfg.WindowDuration <= 0 {
		cfg.WindowDuration = 10 * time.Minute
	}

	bp := &BackpressureController{
		maxConcurrency: int32(cfg.MaxConcurrency),
		minConcurrency: int32(cfg.MinConcurrency),
		threshold:      cfg.FailureThreshold,
		window:         cfg.WindowDuration,
	}
	bp.currentConcurrency.Store(int32(cfg.MaxConcurrency))
	return bp
}

// RecordSuccess records a successful compaction attempt.
func (bp *BackpressureController) RecordSuccess() {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	bp.attempts = append(bp.attempts, attemptRecord{at: time.Now(), success: true})
}

// RecordFailure records a failed compaction attempt.
func (bp *BackpressureController) RecordFailure() {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	bp.attempts = append(bp.attempts, attemptRecord{at: time.Now(), success: false})
}

// FailureRate returns the failure rate within the sliding window.
func (bp *BackpressureController) FailureRate() float64 {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	return bp.failureRateLocked()
}

// failureRateLocked computes the failure rate. Caller must hold bp.mu.
func (bp *BackpressureController) failureRateLocked() float64 {
	bp.pruneWindowLocked()

	if len(bp.attempts) == 0 {
		return 0
	}

	failures := 0
	for _, a := range bp.attempts {
		if !a.success {
			failures++
		}
	}
	return float64(failures) / float64(len(bp.attempts))
}

// pruneWindowLocked removes records older than the sliding window. Caller must hold bp.mu.
func (bp *BackpressureController) pruneWindowLocked() {
	cutoff := time.Now().Add(-bp.window)
	i := 0
	for i < len(bp.attempts) && bp.attempts[i].at.Before(cutoff) {
		i++
	}
	if i > 0 {
		bp.attempts = bp.attempts[i:]
	}
}

// AdjustConcurrency recalculates the concurrency level based on recent failure rate.
// Call this at the start of each compaction cycle.
// AdjustConcurrency recalculates the concurrency level based on recent failure rate.
// Call this at the start of each compaction cycle.
//
// Recovery strategy:
//   - When failure rate > threshold: halve concurrency (aggressive backoff)
//   - When failure rate < threshold/2: exponential ramp-up (double, capped at max)
//   - When failure rate between threshold/2 and threshold: linear ramp-up (+1)
//   - Otherwise: hold steady
//
// The exponential ramp-up ensures recovery from min→max takes O(log N) cycles
// instead of O(N) cycles with the previous 25% linear approach.
func (bp *BackpressureController) AdjustConcurrency() {
	bp.mu.Lock()
	rate := bp.failureRateLocked()
	bp.mu.Unlock()

	current := bp.currentConcurrency.Load()

	if rate > bp.threshold {
		// Backoff: halve concurrency
		next := current / 2
		if next < bp.minConcurrency {
			next = bp.minConcurrency
		}
		bp.currentConcurrency.Store(next)
	} else if rate == 0 && len(bp.attempts) > 0 {
		// Zero failures with recent history: aggressive ramp-up (double)
		next := current * 2
		if next > bp.maxConcurrency {
			next = bp.maxConcurrency
		}
		bp.currentConcurrency.Store(next)
	} else if rate < bp.threshold/2 {
		// Low failure rate: moderate ramp-up (50%, at least +1)
		delta := current / 2
		if delta < 1 {
			delta = 1
		}
		next := current + delta
		if next > bp.maxConcurrency {
			next = bp.maxConcurrency
		}
		bp.currentConcurrency.Store(next)
	} else if rate <= bp.threshold {
		// Between threshold/2 and threshold: cautious linear ramp-up (+1)
		next := current + 1
		if next > bp.maxConcurrency {
			next = bp.maxConcurrency
		}
		bp.currentConcurrency.Store(next)
	}
	// rate exactly at threshold: hold steady (handled by no else clause)
}

// ShouldPause returns true if compaction should pause due to high failure rate
// combined with a non-zero backlog. This prevents cascading failures.
// ShouldPause returns true if compaction should pause due to high failure rate
// combined with a large backlog. Small backlogs (≤ maxConcurrency) are always
// processed to prevent starvation — the system needs to attempt compactions
// to generate the success records that drive recovery.
func (bp *BackpressureController) ShouldPause(backlogSize int) bool {
	if backlogSize == 0 {
		return false
	}
	// Never pause when backlog is small enough to process in one cycle —
	// this ensures the system always makes progress and generates success
	// records that lower the failure rate.
	if int32(backlogSize) <= bp.maxConcurrency {
		return false
	}
	return bp.FailureRate() > bp.threshold
}

// Concurrency returns the current allowed concurrency level.
func (bp *BackpressureController) Concurrency() int {
	return int(bp.currentConcurrency.Load())
}

// Stats returns a snapshot of the controller's state.
type BackpressureStats struct {
	CurrentConcurrency int
	FailureRate        float64
	AttemptsInWindow   int
	FailuresInWindow   int
}

// Stats returns current backpressure statistics.
func (bp *BackpressureController) Stats() BackpressureStats {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	bp.pruneWindowLocked()

	failures := 0
	for _, a := range bp.attempts {
		if !a.success {
			failures++
		}
	}

	return BackpressureStats{
		CurrentConcurrency: int(bp.currentConcurrency.Load()),
		FailureRate:        bp.failureRateLocked(),
		AttemptsInWindow:   len(bp.attempts),
		FailuresInWindow:   failures,
	}
}
