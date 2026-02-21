// Package cache provides tiered caching with predictive prefetch for partition data.
package cache

import (
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

// CoAccessGraph tracks partition access patterns to predict prefetch candidates.
type CoAccessGraph struct {
	mu        sync.RWMutex
	nodes     map[string]*Node
	decay     float64
	threshold float64
	maxEdges  int
	lastDecay time.Time
}

// Node represents a partition key and its access relationships.
type Node struct {
	Edges      map[string]float64
	LastAccess time.Time
}

// NewCoAccessGraph creates a new co-access graph with the specified parameters.
func NewCoAccessGraph(decay, threshold float64, maxEdges int) *CoAccessGraph {
	if decay == 0 {
		decay = defaultDecay
	}
	if threshold == 0 {
		threshold = defaultThreshold
	}
	if maxEdges == 0 {
		maxEdges = defaultMaxEdges
	}
	return &CoAccessGraph{
		nodes:     make(map[string]*Node),
		decay:     decay,
		threshold: threshold,
		maxEdges:  maxEdges,
		lastDecay: time.Now(),
	}
}

// RecordAccess records a sequence of partition key accesses and updates edge weights.
func (g *CoAccessGraph) RecordAccess(sequence []string) {
	if len(sequence) < 2 {
		return
	}

	g.mu.Lock()
	defer g.mu.Unlock()

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
	}
}

// Len returns the number of nodes in the graph (for testing/monitoring).
func (g *CoAccessGraph) Len() int {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return len(g.nodes)
}