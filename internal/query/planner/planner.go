// Package planner provides query planning and partition pruning for the query federation layer.
package planner

import (
	"context"
	"fmt"

	"github.com/arkilian/arkilian/internal/manifest"
	"github.com/arkilian/arkilian/internal/query/parser"
)

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
}

// Planner generates query plans from parsed SQL statements.
type Planner struct {
	catalog *manifest.SQLiteCatalog
	pruner  *Pruner
}

// NewPlanner creates a new query planner.
func NewPlanner(catalog *manifest.SQLiteCatalog) *Planner {
	return &Planner{
		catalog: catalog,
		pruner:  NewPruner(catalog, nil), // No storage for basic planner
	}
}

// NewPlannerWithPruner creates a new query planner with a custom pruner.
func NewPlannerWithPruner(catalog *manifest.SQLiteCatalog, pruner *Pruner) *Planner {
	return &Planner{
		catalog: catalog,
		pruner:  pruner,
	}
}

// Plan generates a query plan for the given SELECT statement.
func (p *Planner) Plan(ctx context.Context, stmt *parser.SelectStatement) (*QueryPlan, error) {
	if stmt == nil {
		return nil, fmt.Errorf("planner: nil statement")
	}

	// Extract predicates from WHERE clause
	predicates := parser.ExtractPredicates(stmt)

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
