// Package planner provides query planning and partition pruning for the query federation layer.
package planner

import (
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/arkilian/arkilian/internal/index"
	"github.com/arkilian/arkilian/internal/manifest"
	"github.com/arkilian/arkilian/internal/query/parser"
	"github.com/arkilian/arkilian/internal/router"
)

// recentPartitionBuffer is a thread-safe bounded ring buffer for recently flushed partitions.
type recentPartitionBuffer struct {
	mu         sync.RWMutex
	partitions []*manifest.PartitionRecord
	maxSize    int
}

// newRecentPartitionBuffer creates a new recent partition buffer.
func newRecentPartitionBuffer(maxSize int) *recentPartitionBuffer {
	if maxSize <= 0 {
		maxSize = 1000
	}
	return &recentPartitionBuffer{
		partitions: make([]*manifest.PartitionRecord, 0, maxSize),
		maxSize:    maxSize,
	}
}

// Add adds a partition to the buffer, dropping the oldest if at capacity.
func (b *recentPartitionBuffer) Add(p *manifest.PartitionRecord) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.partitions) >= b.maxSize {
		b.partitions = b.partitions[1:]
	}
	b.partitions = append(b.partitions, p)
}

// GetAll returns a copy of all partitions in the buffer.
func (b *recentPartitionBuffer) GetAll() []*manifest.PartitionRecord {
	b.mu.RLock()
	defer b.mu.RUnlock()
	result := make([]*manifest.PartitionRecord, len(b.partitions))
	copy(result, b.partitions)
	return result
}

// QueryPlan represents a plan for executing a query across partitions.
type QueryPlan struct {
	// Statement is the parsed SQL statement.
	Statement *parser.SelectStatement

	// Partitions is the list of partitions to scan after pruning.
	Partitions []*manifest.PartitionRecord

	// Predicates extracted from the WHERE clause.
	Predicates []parser.Predicate

	// PruningStats contains statistics about the pruning process.
	PruningStats PruningStats
}

// PruningStats contains statistics about partition pruning.
type PruningStats struct {
	// TotalPartitions is the total number of active partitions.
	TotalPartitions int

	// Phase1Candidates is the number of partitions after min/max pruning.
	Phase1Candidates int

	// Phase2Candidates is the number of partitions after bloom filter pruning.
	Phase2Candidates int

	// PrunedCount is the total number of partitions pruned.
	PrunedCount int

	// PruningRatio is the ratio of pruned partitions (0.0 to 1.0).
	PruningRatio float64

	// IndexUsed indicates whether a secondary index was used for pruning.
	IndexUsed bool

	// IndexColumn is the column name used for index-based pruning.
	IndexColumn string
}

// Planner generates query plans from parsed SQL statements.
type Planner struct {
	catalog          manifest.CatalogReader
	pruner           *Pruner
	indexLookup      *index.Lookup
	notifier         *router.Notifier
	recentPartitions *recentPartitionBuffer
	notificationCh   chan router.Notification
}

// NewPlanner creates a new query planner.
func NewPlanner(catalog manifest.CatalogReader) *Planner {
	return &Planner{
		catalog:          catalog,
		pruner:           NewPruner(catalog, nil),
		indexLookup:      nil,
		recentPartitions: nil,
	}
}

// NewPlannerWithPruner creates a new query planner with a custom pruner.
func NewPlannerWithPruner(catalog manifest.CatalogReader, pruner *Pruner) *Planner {
	return &Planner{
		catalog:          catalog,
		pruner:           pruner,
		indexLookup:      nil,
		recentPartitions: nil,
	}
}

// NewPlannerWithIndex creates a new query planner with index lookup support.
func NewPlannerWithIndex(catalog manifest.CatalogReader, pruner *Pruner, indexLookup *index.Lookup) *Planner {
	return &Planner{
		catalog:          catalog,
		pruner:           pruner,
		indexLookup:      indexLookup,
		recentPartitions: nil,
	}
}

// NewPlannerWithNotifier creates a new query planner with notification support for write visibility.
func NewPlannerWithNotifier(catalog manifest.CatalogReader, notifier *router.Notifier) *Planner {
	p := &Planner{
		catalog:          catalog,
		pruner:           NewPruner(catalog, nil),
		indexLookup:      nil,
		notifier:         notifier,
		recentPartitions: newRecentPartitionBuffer(1000),
	}

	// Subscribe to notifications if notifier is provided
	if notifier != nil {
		p.notificationCh = notifier.SubscribeAutoID()
		go p.notificationHandler()
	}

	return p
}

// notificationHandler processes notifications from the notifier.
func (p *Planner) notificationHandler() {
	if p.notificationCh == nil {
		return
	}

	for notif := range p.notificationCh {
		// Fetch the specific partition from the catalog using GetPartition (O(1) lookup)
		part, err := p.catalog.GetPartition(context.Background(), notif.PartitionID)
		if err != nil {
			log.Printf("planner: failed to fetch partition %s for notification: %v", notif.PartitionID, err)
			continue
		}
		if part != nil {
			p.recentPartitions.Add(part)
		}
	}
}

// Plan generates a query plan for the given SELECT statement.
func (p *Planner) Plan(ctx context.Context, stmt *parser.SelectStatement) (*QueryPlan, error) {
	if stmt == nil {
		return nil, fmt.Errorf("planner: nil statement")
	}

	// Extract predicates from WHERE clause
	predicates := parser.ExtractPredicates(stmt)

	// Derive collection name from the FROM clause
	collection := ""
	if stmt.From != nil {
		collection = stmt.From.Name
	}

	// Try index lookup if enabled and collection is specified
	if p.indexLookup != nil && collection != "" {
		for _, pred := range predicates {
			if pred.Operator == "=" {
				partitionIDs, err := p.indexLookup.FindPartitions(ctx, collection, pred.Column, pred.Value)
				if err != nil {
					// Index lookup failed — log and fall through to bloom pruning
					log.Printf("planner: index lookup failed for %s=%v: %v", pred.Column, pred.Value, err)
					break
				}
				if len(partitionIDs) > 0 {
					// Get full partition records for the index-derived IDs
					allPartitions, err := p.catalog.FindPartitions(ctx, nil)
					if err != nil {
						return nil, fmt.Errorf("planner: failed to get partitions: %w", err)
					}
					filtered := filterByIDs(allPartitions, partitionIDs)
					return &QueryPlan{
						Statement:  stmt,
						Partitions: filtered,
						Predicates: predicates,
						PruningStats: PruningStats{
							TotalPartitions:  len(allPartitions),
							Phase2Candidates: len(filtered),
							PrunedCount:      len(allPartitions) - len(filtered),
							IndexUsed:        true,
							IndexColumn:      pred.Column,
						},
					}, nil
				}
				// Empty result from index — fall through to bloom pruning
			}
		}
	}

	// Convert parser predicates to manifest predicates for pruning
	manifestPredicates := convertToManifestPredicates(predicates)

	// Perform 2-phase pruning
	pruneResult, err := p.pruner.Prune(ctx, manifestPredicates, predicates)
	if err != nil {
		return nil, fmt.Errorf("planner: pruning failed: %w", err)
	}

	plan := &QueryPlan{
		Statement:  stmt,
		Partitions: pruneResult.Partitions,
		Predicates: predicates,
		PruningStats: PruningStats{
			TotalPartitions:  pruneResult.TotalPartitions,
			Phase1Candidates: pruneResult.Phase1Candidates,
			Phase2Candidates: pruneResult.Phase2Candidates,
			PrunedCount:      pruneResult.TotalPartitions - pruneResult.Phase2Candidates,
			PruningRatio:     pruneResult.PruningRatio,
		},
	}

	// Merge recent partitions for <100ms write visibility
	if p.recentPartitions != nil {
		recent := p.recentPartitions.GetAll()
		if len(recent) > 0 {
			plan.Partitions = mergePartitions(plan.Partitions, recent)
		}
	}

	return plan, nil
}

// PlanWithoutPruning generates a query plan without partition pruning.
// This is useful for queries that need to scan all partitions.
func (p *Planner) PlanWithoutPruning(ctx context.Context, stmt *parser.SelectStatement) (*QueryPlan, error) {
	if stmt == nil {
		return nil, fmt.Errorf("planner: nil statement")
	}

	// Get all active partitions
	partitions, err := p.catalog.FindPartitions(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("planner: failed to get partitions: %w", err)
	}

	predicates := parser.ExtractPredicates(stmt)

	plan := &QueryPlan{
		Statement:  stmt,
		Partitions: partitions,
		Predicates: predicates,
		PruningStats: PruningStats{
			TotalPartitions:  len(partitions),
			Phase1Candidates: len(partitions),
			Phase2Candidates: len(partitions),
			PrunedCount:      0,
			PruningRatio:     0,
		},
	}

	return plan, nil
}

// convertToManifestPredicates converts parser predicates to manifest predicates.
func convertToManifestPredicates(predicates []parser.Predicate) []manifest.Predicate {
	var result []manifest.Predicate

	for _, p := range predicates {
		// Only convert predicates that can be used for min/max pruning
		if !parser.CanUseMinMaxPruning(p) {
			continue
		}

		mp := manifest.Predicate{
			Column:   p.Column,
			Operator: p.Operator,
			Value:    p.Value,
		}

		// Handle BETWEEN predicates
		if p.Type == parser.PredicateBetween {
			mp.Operator = "BETWEEN"
			mp.Values = []interface{}{p.Low, p.High}
		}

		// Handle IN predicates
		if p.Type == parser.PredicateIn && !p.Not {
			mp.Operator = "IN"
			mp.Values = p.Values
		}

		result = append(result, mp)
	}

	return result
}

// IdentifyPartitionsToScan returns the list of partition IDs that need to be scanned.
func (p *QueryPlan) IdentifyPartitionsToScan() []string {
	ids := make([]string, len(p.Partitions))
	for i, part := range p.Partitions {
		ids[i] = part.PartitionID
	}
	return ids
}

// GetObjectPaths returns the object storage paths for all partitions in the plan.
func (p *QueryPlan) GetObjectPaths() []string {
	paths := make([]string, len(p.Partitions))
	for i, part := range p.Partitions {
		paths[i] = part.ObjectPath
	}
	return paths
}

// EstimatedRowCount returns an estimate of the total rows to scan.
func (p *QueryPlan) EstimatedRowCount() int64 {
	var total int64
	for _, part := range p.Partitions {
		total += part.RowCount
	}
	return total
}

// EstimatedSizeBytes returns an estimate of the total data size to scan.
func (p *QueryPlan) EstimatedSizeBytes() int64 {
	var total int64
	for _, part := range p.Partitions {
		total += part.SizeBytes
	}
	return total
}

// HasAggregates returns true if the query contains aggregate functions.
func (p *QueryPlan) HasAggregates() bool {
	for _, col := range p.Statement.Columns {
		if hasAggregate(col.Expr) {
			return true
		}
	}
	return false
}

// HasGroupBy returns true if the query has a GROUP BY clause.
func (p *QueryPlan) HasGroupBy() bool {
	return len(p.Statement.GroupBy) > 0
}

// HasOrderBy returns true if the query has an ORDER BY clause.
func (p *QueryPlan) HasOrderBy() bool {
	return len(p.Statement.OrderBy) > 0
}

// HasLimit returns true if the query has a LIMIT clause.
func (p *QueryPlan) HasLimit() bool {
	return p.Statement.Limit != nil
}

// hasAggregate checks if an expression contains an aggregate function.
func hasAggregate(expr parser.Expression) bool {
	switch e := expr.(type) {
	case *parser.AggregateExpr:
		return true
	case *parser.BinaryExpr:
		return hasAggregate(e.Left) || hasAggregate(e.Right)
	case *parser.UnaryExpr:
		return hasAggregate(e.Operand)
	case *parser.ParenExpr:
		return hasAggregate(e.Expr)
	case *parser.FunctionCall:
		for _, arg := range e.Args {
			if hasAggregate(arg) {
				return true
			}
		}
	}
	return false
}

// filterByIDs filters partitions to only those whose PartitionID is in the ids set.
func filterByIDs(partitions []*manifest.PartitionRecord, ids []string) []*manifest.PartitionRecord {
	// Create a set for O(1) lookup
	idSet := make(map[string]struct{})
	for _, id := range ids {
		idSet[id] = struct{}{}
	}

	// Filter partitions
	var result []*manifest.PartitionRecord
	for _, part := range partitions {
		if _, exists := idSet[part.PartitionID]; exists {
			result = append(result, part)
		}
	}
	return result
}

// mergePartitions merges two partition lists and deduplicates by PartitionID.
func mergePartitions(a, b []*manifest.PartitionRecord) []*manifest.PartitionRecord {
	// Create a set of existing PartitionIDs
	existing := make(map[string]struct{})
	for _, part := range a {
		existing[part.PartitionID] = struct{}{}
	}

	// Start with existing partitions
	result := make([]*manifest.PartitionRecord, len(a))
	copy(result, a)

	// Add new partitions from b that aren't already present
	for _, part := range b {
		if _, exists := existing[part.PartitionID]; !exists {
			result = append(result, part)
			existing[part.PartitionID] = struct{}{}
		}
	}

	return result
}
