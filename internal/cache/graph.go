// Package cache provides tiered caching with predictive prefetch for partition data.
package cache

import (
	"log"
	"math"
	"sync"
	"time"
)

// Memory budget: ~10MB for 100K partitions with 10 edges each
const (
	defaultDecay     = 0.95
	defaultThreshold = 0.70
	defaultMaxEdges  = 10
	maxNodes         = 100000
)

// Metrics holds graph statistics for observability.
type GraphMetrics struct {
	Accesses    int64
	EdgesAdded  int64
	EdgesPruned int64
	NodesEvicted int64
	DecayCycles int64
	Nodes       int64
}

// CoAccessGraph tracks partition access patterns to predict prefetch candidates.
type CoAccessGraph struct {
	mu        sync.RWMutex
	nodes     map[string]*Node
	decay     float64
	threshold float64
	maxEdges  int
	lastDecay time.Time
	metrics   GraphMetrics

	// Background worker for periodic decay
	stopChan chan struct{}
	wg       sync.WaitGroup
}

// Node represents a partition key and its access relationships.
type Node struct {
	Edges      map[string]float64
	LastAccess time.Time
}

// NewCoAccessGraph creates a new co-access graph with the specified parameters.
func NewCoAccessGraph(decay, threshold float64, maxEdges int) *CoAccessGraph {
	if decay <= 0 || decay >= 1 {
		decay = defaultDecay
	}
	if threshold <= 0 {
		threshold = defaultThreshold
	}
	if maxEdges <= 0 {
		maxEdges = defaultMaxEdges
	}

	g := &CoAccessGraph{
		nodes:     make(map[string]*Node),
		decay:     decay,
		threshold: threshold,
		maxEdges:  maxEdges,
		lastDecay: time.Now(),
		stopChan:  make(chan struct{}),
	}

	// Start background decay worker
	g.wg.Add(1)
	go g.decayWorker()

	return g
}

// Close shuts down the graph and waits for background workers.
func (g *CoAccessGraph) Close() {
	close(g.stopChan)
	g.wg.Wait()
}

// decayWorker periodically applies decay to all edges.
func (g *CoAccessGraph) decayWorker() {
	defer g.wg.Done()

	ticker := time.NewTicker(1 * time.Hour)
	defer ticker.Stop()

	for {
		select {
		case <-g.stopChan:
			return
		case <-ticker.C:
			g.applyDecay()
		}
	}
}

// applyDecay applies time-based decay to all edges.
func (g *CoAccessGraph) applyDecay() {
	g.mu.Lock()
	defer g.mu.Unlock()

	elapsed := time.Since(g.lastDecay).Hours()
	if elapsed < 1.0 {
		return
	}

	decayFactor := math.Pow(g.decay, elapsed)
	if decayFactor > 0.99 {  // No meaningful decay
		g.lastDecay = time.Now()
		return
	}

	pruned := 0
	for _, node := range g.nodes {
		for k, w := range node.Edges {
			newWeight := w * decayFactor
			if newWeight < 0.01 {
				delete(node.Edges, k)
				pruned++
			} else {
				node.Edges[k] = newWeight
			}
		}
	}

	g.lastDecay = time.Now()
	g.metrics.DecayCycles++
	g.metrics.EdgesPruned += int64(pruned)
	log.Printf("graph: decay applied (factor=%.4f, pruned=%d)", decayFactor, pruned)
}

// Metrics returns current graph metrics.
func (g *CoAccessGraph) Metrics() GraphMetrics {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return GraphMetrics{
		Accesses:    g.metrics.Accesses,
		EdgesAdded:  g.metrics.EdgesAdded,
		EdgesPruned: g.metrics.EdgesPruned,
		NodesEvicted: g.metrics.NodesEvicted,
		DecayCycles: g.metrics.DecayCycles,
		Nodes:       int64(len(g.nodes)),
	}
}

// RecordAccess records a sequence of partition key accesses and updates edge weights.
func (g *CoAccessGraph) RecordAccess(sequence []string) {
	if len(sequence) < 2 {
		return
	}

	g.mu.Lock()
	defer g.mu.Unlock()

	g.metrics.Accesses++

	// Apply time-based decay to all nodes
	elapsed := time.Since(g.lastDecay).Hours()
	if elapsed >= 1.0 {
		decayFactor := math.Pow(g.decay, elapsed)
		for _, node := range g.nodes {
			for k, w := range node.Edges {
				node.Edges[k] = w * decayFactor
			}
		}
		g.lastDecay = time.Now()
	}

	// Record new access edges
	for i := 0; i < len(sequence)-1; i++ {
		src, dst := sequence[i], sequence[i+1]
		if g.nodes[src] == nil {
			// Enforce memory budget: evict LRU node if at capacity
			if len(g.nodes) >= maxNodes {
				g.evictLRU()
			}
			g.nodes[src] = &Node{Edges: make(map[string]float64)}
		}
		g.nodes[src].Edges[dst] += 1.0
		g.nodes[src].LastAccess = time.Now()
		g.metrics.EdgesAdded++
		if len(g.nodes[src].Edges) > g.maxEdges {
			g.pruneWeakest(g.nodes[src])
		}
	}
}

// GetPrefetchCandidates returns partition keys that are likely to be accessed next.
func (g *CoAccessGraph) GetPrefetchCandidates(current string) []string {
	g.mu.RLock()
	defer g.mu.RUnlock()

	node := g.nodes[current]
	if node == nil {
		return nil
	}

	var candidates []string
	for target, weight := range node.Edges {
		if weight > g.threshold {
			candidates = append(candidates, target)
		}
	}
	return candidates
}

// GetNode returns the node for a given partition key (for debugging/monitoring).
func (g *CoAccessGraph) GetNode(key string) (map[string]float64, bool) {
	g.mu.RLock()
	defer g.mu.RUnlock()

	node, ok := g.nodes[key]
	if !ok {
		return nil, false
	}
	return node.Edges, true
}

// pruneWeakest removes the edge with the lowest weight from a node.
func (g *CoAccessGraph) pruneWeakest(node *Node) {
	var weakestTarget string
	var weakestWeight float64 = math.MaxFloat64

	for target, weight := range node.Edges {
		if weight < weakestWeight {
			weakestWeight = weight
			weakestTarget = target
		}
	}

	if weakestTarget != "" {
		delete(node.Edges, weakestTarget)
		g.metrics.EdgesPruned++
	}
}

// evictLRU removes the least recently accessed node to enforce memory budget.
func (g *CoAccessGraph) evictLRU() {
	var lruKey string
	var lruTime time.Time

	for key, node := range g.nodes {
		if lruKey == "" || node.LastAccess.Before(lruTime) {
			lruKey = key
			lruTime = node.LastAccess
		}
	}

	if lruKey != "" {
		delete(g.nodes, lruKey)
		g.metrics.NodesEvicted++
		log.Printf("graph: evicted LRU node %s (memory budget enforcement)", lruKey)
	}
}

// Clear removes all nodes from the graph.
func (g *CoAccessGraph) Clear() {
	g.mu.Lock()
	defer g.mu.Unlock()

	g.nodes = make(map[string]*Node)
	g.lastDecay = time.Now()
	log.Printf("graph: cleared all nodes")
}

// Len returns the number of nodes in the graph (for testing/monitoring).
func (g *CoAccessGraph) Len() int {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return len(g.nodes)
}

// Capacity returns the maximum number of nodes.
func (g *CoAccessGraph) Capacity() int {
	return maxNodes
}

// Usage returns the node usage as a percentage.
func (g *CoAccessGraph) Usage() float64 {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return float64(len(g.nodes)) / float64(maxNodes) * 100
}

// Threshold returns the current prefetch threshold.
func (g *CoAccessGraph) Threshold() float64 {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.threshold
}

// SetThreshold updates the prefetch threshold.
func (g *CoAccessGraph) SetThreshold(threshold float64) {
	if threshold <= 0 {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	g.threshold = threshold
}