package cache

import (
	"math"
	"testing"
	"time"
)

func TestRecordAccess(t *testing.T) {
	g := NewCoAccessGraph(0.95, 0.70, 10)
	defer g.Close()

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
	defer g.Close()

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
	defer g.Close()

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
	defer g.Close()

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
	defer g.Close()

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
	defer g.Close()

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
	defer g.Close()

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
	defer g.Close()

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

func TestMetrics(t *testing.T) {
	g := NewCoAccessGraph(0.95, 0.70, 10)
	defer g.Close()

	// Record some accesses
	g.RecordAccess([]string{"A", "B"})
	g.RecordAccess([]string{"B", "C"})
	g.RecordAccess([]string{"C", "D"})

	metrics := g.Metrics()
	if metrics.Accesses != 3 {
		t.Errorf("expected 3 accesses, got %d", metrics.Accesses)
	}
	if metrics.EdgesAdded != 3 {
		t.Errorf("expected 3 edges added, got %d", metrics.EdgesAdded)
	}
	if metrics.Nodes != 3 {
		t.Errorf("expected 3 nodes, got %d", metrics.Nodes)
	}
}

func TestGetNode(t *testing.T) {
	g := NewCoAccessGraph(0.95, 0.70, 10)
	defer g.Close()

	g.RecordAccess([]string{"A", "B"})
	g.RecordAccess([]string{"A", "C"})

	edges, ok := g.GetNode("A")
	if !ok {
		t.Error("expected node A to exist")
	}
	if len(edges) != 2 {
		t.Errorf("expected 2 edges, got %d", len(edges))
	}
	if edges["B"] != 1.0 {
		t.Errorf("expected edge B weight 1.0, got %v", edges["B"])
	}

	// Non-existent node
	_, ok = g.GetNode("Z")
	if ok {
		t.Error("expected node Z to not exist")
	}
}

func TestClear(t *testing.T) {
	g := NewCoAccessGraph(0.95, 0.70, 10)
	defer g.Close()

	g.RecordAccess([]string{"A", "B"})
	g.RecordAccess([]string{"B", "C"})

	if g.Len() != 2 {
		t.Errorf("expected 2 nodes, got %d", g.Len())
	}

	g.Clear()

	if g.Len() != 0 {
		t.Errorf("expected 0 nodes after clear, got %d", g.Len())
	}
}

func TestClose(t *testing.T) {
	g := NewCoAccessGraph(0.95, 0.70, 10)

	// Close should not panic
	g.Close()

	// After close, operations should still work (no background workers)
	g.RecordAccess([]string{"A", "B"})
	if g.Len() != 1 {
		t.Errorf("expected 1 node, got %d", g.Len())
	}
}

func TestCapacity(t *testing.T) {
	g := NewCoAccessGraph(0.95, 0.70, 10)
	defer g.Close()

	if g.Capacity() != maxNodes {
		t.Errorf("expected capacity %d, got %d", maxNodes, g.Capacity())
	}
}

func TestUsage(t *testing.T) {
	g := NewCoAccessGraph(0.95, 0.70, 10)
	defer g.Close()

	if g.Usage() != 0 {
		t.Errorf("expected 0%% usage, got %f", g.Usage())
	}

	g.RecordAccess([]string{"A", "B"})

	if g.Usage() == 0 {
		t.Error("expected non-zero usage after adding node")
	}
}

func TestThresholdAccessor(t *testing.T) {
	g := NewCoAccessGraph(0.95, 0.70, 10)
	defer g.Close()

	if g.Threshold() != 0.70 {
		t.Errorf("expected threshold 0.70, got %f", g.Threshold())
	}

	g.SetThreshold(0.50)
	if g.Threshold() != 0.50 {
		t.Errorf("expected threshold 0.50, got %f", g.Threshold())
	}

	// Invalid threshold should be ignored
	g.SetThreshold(0)
	if g.Threshold() != 0.50 {
		t.Errorf("expected threshold 0.50, got %f", g.Threshold())
	}
}

func TestInvalidParameters(t *testing.T) {
	// Zero decay should use default
	g := NewCoAccessGraph(0, 0.70, 10)
	defer g.Close()
	if g.decay != defaultDecay {
		t.Errorf("expected default decay, got %f", g.decay)
	}

	// Zero threshold should use default
	g = NewCoAccessGraph(0.95, 0, 10)
	defer g.Close()
	if g.threshold != defaultThreshold {
		t.Errorf("expected default threshold, got %f", g.threshold)
	}

	// Zero maxEdges should use default
	g = NewCoAccessGraph(0.95, 0.70, 0)
	defer g.Close()
	if g.maxEdges != defaultMaxEdges {
		t.Errorf("expected default maxEdges, got %d", g.maxEdges)
	}

	// Negative values should use defaults
	g = NewCoAccessGraph(-1, -1, -1)
	defer g.Close()
	if g.decay != defaultDecay || g.threshold != defaultThreshold || g.maxEdges != defaultMaxEdges {
		t.Errorf("expected defaults for negative values")
	}
}

func TestBackgroundDecay(t *testing.T) {
	g := NewCoAccessGraph(0.95, 0.70, 10)
	defer g.Close()

	// Record [A, B] once
	g.RecordAccess([]string{"A", "B"})

	// Verify edge exists
	candidates := g.GetPrefetchCandidates("A")
	if len(candidates) != 1 {
		t.Fatalf("expected 1 candidate, got %d", len(candidates))
	}

	// Manually trigger background decay by advancing time
	g.mu.Lock()
	g.lastDecay = time.Now().Add(-25 * time.Hour)
	g.mu.Unlock()

	// Wait for background worker to run (it runs every hour, but we can trigger via RecordAccess)
	g.RecordAccess([]string{"X", "Y"})

	// Now B should not be a candidate (decayed below 0.70)
	candidates = g.GetPrefetchCandidates("A")
	if len(candidates) != 0 {
		t.Errorf("expected no candidates after background decay, got %v", candidates)
	}
}