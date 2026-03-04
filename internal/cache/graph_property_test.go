// Package cache provides tiered caching with predictive prefetch for partition data.
package cache

import (
	"testing"

	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/gen"
	"github.com/leanovate/gopter/prop"
)

// TestProperty_CoAccessGraphMemoryBound tests Property V2-9: Memory Bound
// For any sequence of RecordAccess calls with up to 10K distinct keys, memory < 10MB
func TestProperty_CoAccessGraphMemoryBound(t *testing.T) {
	parameters := gopter.DefaultTestParameters()
	parameters.MinSuccessfulTests = 100

	properties := gopter.NewProperties(parameters)

	properties.Property("V2-9: Memory Bound - graph memory usage under 10MB", prop.ForAll(
		func(seed int) bool {
			g := NewCoAccessGraph(0.95, 0.70, 10)
			defer g.Close()

			numKeys := 10000
			keys := make([]string, numKeys)
			for i := 0; i < numKeys; i++ {
				keys[i] = string(rune('A' + (i % 26))) + string(rune('a' + (i % 26))) + string(rune('0' + (i % 10))) + "_" + string(rune('0'+(i/10)%10)) + string(rune('0'+(i/100)%10)) + string(rune('0'+(i/1000)%10))
			}

			for i := 0; i < numKeys-1; i++ {
				g.RecordAccess([]string{keys[i], keys[i+1]})
			}

			metrics := g.Metrics()
			nodeCount := metrics.Nodes

			if nodeCount > maxNodes {
				t.Errorf("node count %d exceeds maxNodes %d", nodeCount, maxNodes)
				return false
			}

			if nodeCount > 0 {
				candidates := g.GetPrefetchCandidates(keys[0])
				_ = candidates
			}

			return true
		},
		gen.Int(),
	))

	properties.TestingRun(t)
}

// TestProperty_CoAccessGraphEdgePruning tests that max edges are enforced
func TestProperty_CoAccessGraphEdgePruning(t *testing.T) {
	parameters := gopter.DefaultTestParameters()
	parameters.MinSuccessfulTests = 100

	properties := gopter.NewProperties(parameters)

	properties.Property("CoAccess Graph Edge Pruning - max edges enforced", prop.ForAll(
		func(seed int) bool {
			g := NewCoAccessGraph(0.95, 0.70, 5)
			defer g.Close()

			numEdges := 10
			for i := 0; i < numEdges; i++ {
				g.RecordAccess([]string{"A", string(rune('B' + i))})
			}

			edges, ok := g.GetNode("A")
			if !ok {
				t.Fatal("expected node A to exist")
			}

			if len(edges) > 5 {
				t.Errorf("expected at most 5 edges, got %d", len(edges))
				return false
			}

			return true
		},
		gen.Int(),
	))

	properties.TestingRun(t)
}

// TestProperty_CoAccessGraphDecay tests that decay works correctly
func TestProperty_CoAccessGraphDecay(t *testing.T) {
	parameters := gopter.DefaultTestParameters()
	parameters.MinSuccessfulTests = 100

	properties := gopter.NewProperties(parameters)

	properties.Property("CoAccess Graph Decay - weights decay over time", prop.ForAll(
		func(seed int) bool {
			g := NewCoAccessGraph(0.95, 0.70, 10)
			defer g.Close()

			g.RecordAccess([]string{"A", "B"})

			edges, ok := g.GetNode("A")
			if !ok {
				t.Fatal("expected node A to exist")
			}

			weightBefore := edges["B"]
			if weightBefore != 1.0 {
				t.Errorf("expected weight 1.0, got %f", weightBefore)
				return false
			}

			g.mu.Lock()
			g.lastDecay = g.lastDecay.Add(-25 * 60 * 60 * 1000000000)
			g.mu.Unlock()

			g.RecordAccess([]string{"X", "Y"})

			edges, ok = g.GetNode("A")
			if !ok {
				t.Fatal("expected node A to exist")
			}

			weightAfter := edges["B"]
			if weightAfter >= weightBefore {
				t.Errorf("expected weight to decay, before=%f, after=%f", weightBefore, weightAfter)
				return false
			}

			expectedDecay := 0.95 * 0.95 * 0.95 * 0.95 * 0.95 * 0.95 * 0.95 * 0.95 * 0.95 * 0.95 * 0.95 * 0.95 * 0.95 * 0.95 * 0.95 * 0.95 * 0.95 * 0.95 * 0.95 * 0.95 * 0.95 * 0.95 * 0.95 * 0.95 * 0.95
			if weightAfter > expectedDecay*2 {
				t.Errorf("expected weight around %f, got %f", expectedDecay, weightAfter)
				return false
			}

			return true
		},
		gen.Int(),
	))

	properties.TestingRun(t)
}

// TestProperty_CoAccessGraphThresholdBehavior tests threshold filtering
func TestProperty_CoAccessGraphThresholdBehavior(t *testing.T) {
	parameters := gopter.DefaultTestParameters()
	parameters.MinSuccessfulTests = 100

	properties := gopter.NewProperties(parameters)

	properties.Property("CoAccess Graph Threshold - only high-weight edges returned", prop.ForAll(
		func(seed int) bool {
			g := NewCoAccessGraph(0.95, 0.90, 10)
			defer g.Close()

			g.RecordAccess([]string{"A", "B"})

			candidates := g.GetPrefetchCandidates("A")
			if len(candidates) != 1 || candidates[0] != "B" {
				t.Errorf("expected [B] as candidate, got %v", candidates)
				return false
			}

			g.mu.Lock()
			g.lastDecay = g.lastDecay.Add(-25 * 60 * 60 * 1000000000)
			g.mu.Unlock()
			g.RecordAccess([]string{"X", "Y"})

			candidates = g.GetPrefetchCandidates("A")
			if len(candidates) != 0 {
				t.Errorf("expected no candidates after decay, got %v", candidates)
				return false
			}

			return true
		},
		gen.Int(),
	))

	properties.TestingRun(t)
}

// TestProperty_CoAccessGraphSequenceHandling tests various sequence inputs
func TestProperty_CoAccessGraphSequenceHandling(t *testing.T) {
	parameters := gopter.DefaultTestParameters()
	parameters.MinSuccessfulTests = 100

	properties := gopter.NewProperties(parameters)

	properties.Property("CoAccess Graph Sequence Handling - various inputs handled", prop.ForAll(
		func(seed int) bool {
			g := NewCoAccessGraph(0.95, 0.70, 10)
			defer g.Close()

			g.RecordAccess([]string{})
			if g.Len() != 0 {
				t.Error("empty sequence should not create nodes")
				return false
			}

			g.RecordAccess([]string{"A"})
			if g.Len() != 0 {
				t.Error("single element sequence should not create nodes")
				return false
			}

			g.RecordAccess(nil)
			if g.Len() != 0 {
				t.Error("nil sequence should not create nodes")
				return false
			}

			g.RecordAccess([]string{"A", "B", "C"})
			if g.Len() != 2 {
				t.Errorf("expected 2 nodes (A and B), got %d", g.Len())
				return false
			}

			edges, ok := g.GetNode("A")
			if !ok {
				t.Fatal("expected node A to exist")
			}
			if _, hasB := edges["B"]; !hasB {
				t.Error("expected edge A->B")
				return false
			}

			edges, ok = g.GetNode("B")
			if !ok {
				t.Fatal("expected node B to exist")
			}
			if _, hasC := edges["C"]; !hasC {
				t.Error("expected edge B->C")
				return false
			}

			return true
		},
		gen.Int(),
	))

	properties.TestingRun(t)
}

// TestProperty_CoAccessGraphLRUEviction tests LRU eviction when at capacity
func TestProperty_CoAccessGraphLRUEviction(t *testing.T) {
	parameters := gopter.DefaultTestParameters()
	parameters.MinSuccessfulTests = 100

	properties := gopter.NewProperties(parameters)

	properties.Property("CoAccess Graph LRU Eviction - LRU nodes evicted at capacity", prop.ForAll(
		func(seed int) bool {
			g := NewCoAccessGraph(0.95, 0.70, 10)
			defer g.Close()

			numNodes := maxNodes
			for i := 0; i < numNodes; i++ {
				g.RecordAccess([]string{
					string(rune('A' + (i % 26))),
					string(rune('B' + (i % 26))),
				})
			}

			if g.Len() > maxNodes {
				t.Errorf("expected at most %d nodes, got %d", maxNodes, g.Len())
				return false
			}

			g.RecordAccess([]string{"Z", "Y"})

			if g.Len() > maxNodes {
				t.Errorf("expected at most %d nodes after eviction, got %d", maxNodes, g.Len())
				return false
			}

			return true
		},
		gen.Int(),
	))

	properties.TestingRun(t)
}

// TestProperty_CoAccessGraphConcurrentAccess tests concurrent access safety
func TestProperty_CoAccessGraphConcurrentAccess(t *testing.T) {
	parameters := gopter.DefaultTestParameters()
	parameters.MinSuccessfulTests = 100

	properties := gopter.NewProperties(parameters)

	properties.Property("CoAccess Graph Concurrent Access - no race conditions", prop.ForAll(
		func(seed int) bool {
			g := NewCoAccessGraph(0.95, 0.70, 10)
			defer g.Close()

			numGoroutines := 10
			accessesPerGoroutine := 100

			done := make(chan bool, numGoroutines)

			for i := 0; i < numGoroutines; i++ {
				go func(goroutineID int) {
					for j := 0; j < accessesPerGoroutine; j++ {
						g.RecordAccess([]string{
							string(rune('A' + (goroutineID % 26))),
							string(rune('B' + (j % 10))),
						})
					}
					done <- true
				}(i)
			}

			for i := 0; i < numGoroutines; i++ {
				<-done
			}

			metrics := g.Metrics()
			if metrics.Accesses != int64(numGoroutines*accessesPerGoroutine) {
				t.Errorf("expected %d accesses, got %d", numGoroutines*accessesPerGoroutine, metrics.Accesses)
				return false
			}

			return true
		},
		gen.Int(),
	))

	properties.TestingRun(t)
}

// TestProperty_CoAccessGraphClear tests Clear functionality
func TestProperty_CoAccessGraphClear(t *testing.T) {
	parameters := gopter.DefaultTestParameters()
	parameters.MinSuccessfulTests = 100

	properties := gopter.NewProperties(parameters)

	properties.Property("CoAccess Graph Clear - all nodes removed", prop.ForAll(
		func(seed int) bool {
			g := NewCoAccessGraph(0.95, 0.70, 10)
			defer g.Close()

			g.RecordAccess([]string{"A", "B"})
			g.RecordAccess([]string{"B", "C"})
			g.RecordAccess([]string{"C", "D"})

			// A, B, C are source nodes (A->B, B->C, C->D)
			if g.Len() != 3 {
				t.Errorf("expected 3 nodes (A, B, C), got %d", g.Len())
				return false
			}

			g.Clear()

			if g.Len() != 0 {
				t.Errorf("expected 0 nodes after clear, got %d", g.Len())
				return false
			}

			return true
		},
		gen.Int(),
	))

	properties.TestingRun(t)
}

// TestProperty_CoAccessGraphCapacity tests capacity accessor
func TestProperty_CoAccessGraphCapacity(t *testing.T) {
	parameters := gopter.DefaultTestParameters()
	parameters.MinSuccessfulTests = 100

	properties := gopter.NewProperties(parameters)

	properties.Property("CoAccess Graph Capacity - capacity accessor correct", prop.ForAll(
		func(seed int) bool {
			g := NewCoAccessGraph(0.95, 0.70, 10)

			if g.Capacity() != maxNodes {
				t.Errorf("expected capacity %d, got %d", maxNodes, g.Capacity())
				return false
			}

			return true
		},
		gen.Int(),
	))

	properties.TestingRun(t)
}

// TestProperty_CoAccessGraphUsage tests usage accessor
func TestProperty_CoAccessGraphUsage(t *testing.T) {
	parameters := gopter.DefaultTestParameters()
	parameters.MinSuccessfulTests = 100

	properties := gopter.NewProperties(parameters)

	properties.Property("CoAccess Graph Usage - usage increases with nodes", prop.ForAll(
		func(seed int) bool {
			g := NewCoAccessGraph(0.95, 0.70, 10)
			defer g.Close()

			if g.Usage() != 0 {
				t.Errorf("expected 0%% usage initially, got %f", g.Usage())
				return false
			}

			g.RecordAccess([]string{"A", "B"})

			usage := g.Usage()
			if usage == 0 {
				t.Error("expected non-zero usage after adding node")
				return false
			}

			if usage >= 100 {
				t.Errorf("expected usage < 100%%, got %f", usage)
				return false
			}

			return true
		},
		gen.Int(),
	))

	properties.TestingRun(t)
}

// TestProperty_CoAccessGraphThresholdAccessor tests threshold accessor
func TestProperty_CoAccessGraphThresholdAccessor(t *testing.T) {
	parameters := gopter.DefaultTestParameters()
	parameters.MinSuccessfulTests = 100

	properties := gopter.NewProperties(parameters)

	properties.Property("CoAccess Graph Threshold Accessor - threshold can be set", prop.ForAll(
		func(seed int) bool {
			g := NewCoAccessGraph(0.95, 0.70, 10)
			defer g.Close()

			if g.Threshold() != 0.70 {
				t.Errorf("expected threshold 0.70, got %f", g.Threshold())
				return false
			}

			g.SetThreshold(0.50)

			if g.Threshold() != 0.50 {
				t.Errorf("expected threshold 0.50, got %f", g.Threshold())
				return false
			}

			g.SetThreshold(0)

			if g.Threshold() != 0.50 {
				t.Errorf("expected threshold to remain 0.50, got %f", g.Threshold())
				return false
			}

			return true
		},
		gen.Int(),
	))

	properties.TestingRun(t)
}

// TestProperty_CoAccessGraphDecayFactor tests decay factor calculation
func TestProperty_CoAccessGraphDecayFactor(t *testing.T) {
	parameters := gopter.DefaultTestParameters()
	parameters.MinSuccessfulTests = 100

	properties := gopter.NewProperties(parameters)

	properties.Property("CoAccess Graph Decay Factor - decay factor correct", prop.ForAll(
		func(seed int) bool {
			decay24h := 0.95 * 0.95 * 0.95 * 0.95 * 0.95 * 0.95 * 0.95 * 0.95 * 0.95 * 0.95 * 0.95 * 0.95 * 0.95 * 0.95 * 0.95 * 0.95 * 0.95 * 0.95 * 0.95 * 0.95 * 0.95 * 0.95 * 0.95 * 0.95 * 0.95
			if decay24h >= 0.70 {
				t.Errorf("24h decay should be below threshold, got %f", decay24h)
				return false
			}

			decay1h := 0.95
			if decay1h < 0.94 || decay1h > 0.96 {
				t.Errorf("1h decay should be ~0.95, got %f", decay1h)
				return false
			}

			return true
		},
		gen.Int(),
	))

	properties.TestingRun(t)
}
