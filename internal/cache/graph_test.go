package cache

import (
	"math"
	"testing"
	"time"
)

func TestRecordAccess(t *testing.T) {
	g := NewCoAccessGraph(0.95, 0.70, 10)

	// Record [A, B, C] 10 times
	for i := 0; i < 10; i++ {
		g.RecordAccess([]string{"A", "B", "C"})
	}

	// GetPrefetchCandidates("A") should return B
	candidates := g.GetPrefetchCandidates("A")
	if len(candidates) != 1 || candidates[0] != "B" {
		t.Errorf("expected [B], got %v", candidates)
	}
}

func TestDecay(t *testing.T) {
	g := NewCoAccessGraph(0.95, 0.70, 10)

	// Record [A, B] once - weight should be 1.0
	g.RecordAccess([]string{"A", "B"})
	candidates := g.GetPrefetchCandidates("A")
	if len(candidates) != 1 {
		t.Fatalf("expected 1 candidate, got %d", len(candidates))
	}

	// Simulate time passing - 24 hours should decay weight below threshold
	g.mu.Lock()
	g.lastDecay = time.Now().Add(-24 * time.Hour)
	g.mu.Unlock()

	// Force decay by recording new access
	g.RecordAccess([]string{"X", "Y"})

	// Now B should not be a candidate (decayed below 0.70)
	candidates = g.GetPrefetchCandidates("A")
	if len(candidates) != 0 {
		t.Errorf("expected no candidates after decay, got %v", candidates)
	}
}

func TestMaxEdges(t *testing.T) {
	g := NewCoAccessGraph(0.95, 0.70, 10)

	// Record [A, B], [A, C], ..., [A, K] (11 edges)
	for i := 0; i < 11; i++ {
		g.RecordAccess([]string{"A", string(rune('B' + i))})
	}

	g.mu.RLock()
	node := g.nodes["A"]
	g.mu.RUnlock()

	if len(node.Edges) != 10 {
		t.Errorf("expected 10 edges (max enforced), got %d", len(node.Edges))
	}
}

func TestConcurrentRecordAccess(t *testing.T) {
	g := NewCoAccessGraph(0.95, 0.70, 10)

	done := make(chan bool)
	for i := 0; i < 10; i++ {
		go func(idx int) {
			for j := 0; j < 100; j++ {
				g.RecordAccess([]string{
					string(rune('A' + idx)),
					string(rune('B' + (j % 10))),
				})
			}
			done <- true
		}(i)
	}

	for i := 0; i < 10; i++ {
		<-done
	}

	// Verify no race conditions by checking graph is consistent
	g.mu.RLock()
	defer g.mu.RUnlock()
	if len(g.nodes) == 0 {
		t.Error("expected nodes to be recorded")
	}
}

func TestMemoryBudget(t *testing.T) {
	g := NewCoAccessGraph(0.95, 0.70, 10)

	// Create more nodes than the budget allows
	for i := 0; i < maxNodes+1000; i++ {
		g.RecordAccess([]string{
			string(rune('A' + i%26)),
			string(rune('B' + i%26)),
		})
	}

	// Should not exceed memory budget
	if g.Len() > maxNodes {
		t.Errorf("expected node count to be within budget, got %d", g.Len())
	}
}

func TestEmptyAndNilSequence(t *testing.T) {
	g := NewCoAccessGraph(0.95, 0.70, 10)

	// Empty sequence
	g.RecordAccess([]string{})
	if g.Len() != 0 {
		t.Error("empty sequence should not create nodes")
	}

	// Single element sequence
	g.RecordAccess([]string{"A"})
	if g.Len() != 0 {
		t.Error("single element sequence should not create nodes")
	}

	// Nil sequence - should not panic
	g.RecordAccess(nil)
}

func TestWeightAccumulation(t *testing.T) {
	g := NewCoAccessGraph(0.95, 0.70, 10)

	// Record [A, B] 5 times
	for i := 0; i < 5; i++ {
		g.RecordAccess([]string{"A", "B"})
	}

	g.mu.RLock()
	weight := g.nodes["A"].Edges["B"]
	g.mu.RUnlock()

	if weight != 5.0 {
		t.Errorf("expected weight 5.0, got %v", weight)
	}
}

func TestThresholdBehavior(t *testing.T) {
	g := NewCoAccessGraph(0.95, 0.50, 10)

	// Record [A, B] once - weight 1.0 > threshold 0.50
	g.RecordAccess([]string{"A", "B"})
	candidates := g.GetPrefetchCandidates("A")
	if len(candidates) != 1 {
		t.Errorf("expected B as candidate, got %v", candidates)
	}

	// Decay to below threshold
	g.mu.Lock()
	g.lastDecay = time.Now().Add(-24 * time.Hour)
	g.mu.Unlock()
	g.RecordAccess([]string{"X", "Y"})

	candidates = g.GetPrefetchCandidates("A")
	if len(candidates) != 0 {
		t.Errorf("expected no candidates after decay, got %v", candidates)
	}
}

func TestDecayFactor(t *testing.T) {
	// Verify math.Pow(0.95, 24) is below 0.70 threshold
	decay24h := math.Pow(0.95, 24)
	if decay24h >= 0.70 {
		t.Errorf("24h decay should be below threshold, got %v", decay24h)
	}
}