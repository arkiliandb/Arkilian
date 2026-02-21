package planner

import (
	"context"
	"fmt"
	"strings"

	"github.com/arkilian/arkilian/internal/manifest"
	"github.com/arkilian/arkilian/internal/partition"
	"github.com/arkilian/arkilian/internal/query/parser"
	"github.com/arkilian/arkilian/pkg/types"
)

// SchemaProvider retrieves schema definitions by version number.
type SchemaProvider interface {
	GetSchemaVersion(ctx context.Context, version int) (*SchemaRecord, error)
}

// SchemaRecord holds a schema definition for a specific version.
type SchemaRecord struct {
	Version int
	Schema  types.Schema
}

// Rewriter detects schema version differences across partitions and rewrites
// queries for compatibility. When a query references a column that doesn't exist
// in an older schema version, the rewriter substitutes NULL for that column.
// (Requirements 13.3, 13.4, 13.5)
type Rewriter struct {
	provider SchemaProvider
}

// NewRewriter creates a new query rewriter.
func NewRewriter(provider SchemaProvider) *Rewriter {
	return &Rewriter{provider: provider}
}

// RewrittenQuery holds a rewritten SELECT statement for a specific partition,
// along with the schema version it was rewritten for.
type RewrittenQuery struct {
	// Statement is the rewritten SELECT statement.
	Statement *parser.SelectStatement

	// Partition is the target partition.
	Partition *manifest.PartitionRecord

	// SchemaVersion is the schema version of the partition.
	SchemaVersion int

	// MissingColumns lists columns referenced in the query but absent in this partition's schema.
	MissingColumns []string

	// MaterializedColumns lists materialized columns available in this partition.
	MaterializedColumns []partition.MaterializedColumn
}

// RewriteForPartitions takes a query plan and returns per-partition rewritten queries.
// Partitions whose schema matches the latest version get the original statement.
// Partitions with older schemas get a rewritten statement where missing columns
// are replaced with NULL literals.
func (r *Rewriter) RewriteForPartitions(ctx context.Context, plan *QueryPlan) ([]*RewrittenQuery, error) {
	if plan == nil || len(plan.Partitions) == 0 {
		return nil, nil
	}

	// Collect distinct schema versions from partitions
	versionSet := make(map[int]bool)
	for _, p := range plan.Partitions {
		versionSet[p.SchemaVersion] = true
	}

	// Find the max (latest) schema version
	maxVersion := 0
	for v := range versionSet {
		if v > maxVersion {
			maxVersion = v
		}
	}

	// If all partitions share the same version, no rewriting needed
	if len(versionSet) == 1 {
		results := make([]*RewrittenQuery, len(plan.Partitions))
		for i, p := range plan.Partitions {
			results[i] = &RewrittenQuery{
				Statement:     plan.Statement,
				Partition:     p,
				SchemaVersion: p.SchemaVersion,
			}
		}
		return results, nil
	}

	// Load schemas for all versions
	schemas := make(map[int]*types.Schema)
	for v := range versionSet {
		record, err := r.provider.GetSchemaVersion(ctx, v)
		if err != nil {
			return nil, fmt.Errorf("rewriter: failed to load schema version %d: %w", v, err)
		}
		schemas[v] = &record.Schema
	}

	// Get column names from the latest schema
	latestSchema := schemas[maxVersion]
	latestCols := columnSet(latestSchema)

	// Extract columns referenced in the query
	referencedCols := extractReferencedColumns(plan.Statement)

	// Build rewritten queries per partition
	results := make([]*RewrittenQuery, len(plan.Partitions))
	for i, p := range plan.Partitions {
		partSchema := schemas[p.SchemaVersion]
		partCols := columnSet(partSchema)

		// Find columns referenced in the query but missing from this partition's schema
		var missing []string
		for _, col := range referencedCols {
			if latestCols[col] && !partCols[col] {
				missing = append(missing, col)
			}
		}

		if len(missing) == 0 {
			// No missing columns — use original statement
			results[i] = &RewrittenQuery{
				Statement:          plan.Statement,
				Partition:          p,
				SchemaVersion:      p.SchemaVersion,
				MaterializedColumns: nil, // TODO: Load from metadata sidecar
			}
		} else {
			// Rewrite the statement to replace missing columns with NULL
			rewritten := rewriteStatement(plan.Statement, missing, nil)
			results[i] = &RewrittenQuery{
				Statement:          rewritten,
				Partition:          p,
				SchemaVersion:      p.SchemaVersion,
				MissingColumns:     missing,
				MaterializedColumns: nil, // TODO: Load from metadata sidecar
			}
		}
	}

	return results, nil
}

// DetectSchemaVersions returns the distinct schema versions present in the partitions.
func DetectSchemaVersions(partitions []*manifest.PartitionRecord) []int {
	seen := make(map[int]bool)
	var versions []int
	for _, p := range partitions {
		if !seen[p.SchemaVersion] {
			seen[p.SchemaVersion] = true
			versions = append(versions, p.SchemaVersion)
		}
	}
	return versions
}

// NeedsRewriting returns true if the partitions span multiple schema versions.
func NeedsRewriting(partitions []*manifest.PartitionRecord) bool {
	if len(partitions) <= 1 {
		return false
	}
	first := partitions[0].SchemaVersion
	for _, p := range partitions[1:] {
		if p.SchemaVersion != first {
			return true
		}
	}
	return false
}

// rewriteStatement creates a copy of the statement with missing columns replaced by NULL.
func rewriteStatement(stmt *parser.SelectStatement, missingColumns []string, materializedColumns []partition.MaterializedColumn) *parser.SelectStatement {
	missingSet := make(map[string]bool)
	for _, col := range missingColumns {
		missingSet[col] = true
	}

	// Build a set of materialized column names for quick lookup
	materializedSet := make(map[string]bool)
	for _, mc := range materializedColumns {
		materializedSet[mc.ColumnName] = true
	}

	// Deep copy and rewrite SELECT columns
	newColumns := make([]parser.SelectColumn, len(stmt.Columns))
	for i, col := range stmt.Columns {
		newColumns[i] = parser.SelectColumn{
			Expr:  rewriteExpr(col.Expr, missingSet, materializedSet),
			Alias: col.Alias,
		}
	}

	// Rewrite WHERE clause
	var newWhere parser.Expression
	if stmt.Where != nil {
		newWhere = rewriteExpr(stmt.Where, missingSet, materializedSet)
	}

	// Rewrite GROUP BY
	var newGroupBy []parser.Expression
	for _, g := range stmt.GroupBy {
		newGroupBy = append(newGroupBy, rewriteExpr(g, missingSet, materializedSet))
	}

	// Rewrite HAVING
	var newHaving parser.Expression
	if stmt.Having != nil {
		newHaving = rewriteExpr(stmt.Having, missingSet, materializedSet)
	}

	// Rewrite ORDER BY
	var newOrderBy []parser.OrderByClause
	for _, o := range stmt.OrderBy {
		newOrderBy = append(newOrderBy, parser.OrderByClause{
			Expr: rewriteExpr(o.Expr, missingSet, materializedSet),
			Desc: o.Desc,
		})
	}

	return &parser.SelectStatement{
		Distinct: stmt.Distinct,
		Columns:  newColumns,
		From:     stmt.From,
		Where:    newWhere,
		GroupBy:  newGroupBy,
		Having:   newHaving,
		OrderBy:  newOrderBy,
		Limit:    stmt.Limit,
		Offset:   stmt.Offset,
	}
}

// rewriteExpr replaces ColumnRef nodes for missing columns with NULL literals.
// It also rewrites json_extract(payload, '$.path') to materialized column references
// when the partition has the materialized column.
func rewriteExpr(expr parser.Expression, missingCols map[string]bool, materializedSet map[string]bool) parser.Expression {
	if expr == nil {
		return nil
	}

	switch e := expr.(type) {
	case *parser.ColumnRef:
		if missingCols[e.Column] {
			return &parser.Literal{Value: nil} // NULL
		}
		return e

	case *parser.BinaryExpr:
		return &parser.BinaryExpr{
			Left:     rewriteExpr(e.Left, missingCols, materializedSet),
			Operator: e.Operator,
			Right:    rewriteExpr(e.Right, missingCols, materializedSet),
		}

	case *parser.UnaryExpr:
		return &parser.UnaryExpr{
			Operator: e.Operator,
			Operand:  rewriteExpr(e.Operand, missingCols, materializedSet),
		}

	case *parser.AggregateExpr:
		return &parser.AggregateExpr{
			Function: e.Function,
			Arg:      rewriteExpr(e.Arg, missingCols, materializedSet),
			Distinct: e.Distinct,
		}

	case *parser.FunctionCall:
		// Check for json_extract(payload, '$.path') that can be rewritten to materialized column
		if strings.EqualFold(e.Name, "json_extract") && len(e.Args) == 2 {
			if colRef, ok := e.Args[0].(*parser.ColumnRef); ok {
				if colRef.Column == "payload" {
					if lit, ok := e.Args[1].(*parser.Literal); ok {
						if pathStr, ok := lit.Value.(string); ok {
							// Only rewrite if the materialized column exists in this partition
							expectedColName := jsonPathToColumnName(pathStr)
							if materializedSet[expectedColName] {
								return &parser.ColumnRef{Column: expectedColName}
							}
						}
					}
				}
			}
		}
		newArgs := make([]parser.Expression, len(e.Args))
		for i, arg := range e.Args {
			newArgs[i] = rewriteExpr(arg, missingCols, materializedSet)
		}
		return &parser.FunctionCall{
			Name: e.Name,
			Args: newArgs,
		}

	case *parser.InExpr:
		newValues := make([]parser.Expression, len(e.Values))
		for i, v := range e.Values {
			newValues[i] = rewriteExpr(v, missingCols, materializedSet)
		}
		return &parser.InExpr{
			Expr:   rewriteExpr(e.Expr, missingCols, materializedSet),
			Values: newValues,
			Not:    e.Not,
		}

	case *parser.BetweenExpr:
		return &parser.BetweenExpr{
			Expr: rewriteExpr(e.Expr, missingCols, materializedSet),
			Low:  rewriteExpr(e.Low, missingCols, materializedSet),
			High: rewriteExpr(e.High, missingCols, materializedSet),
			Not:  e.Not,
		}

	case *parser.IsNullExpr:
		return &parser.IsNullExpr{
			Expr: rewriteExpr(e.Expr, missingCols, materializedSet),
			Not:  e.Not,
		}

	case *parser.LikeExpr:
		return &parser.LikeExpr{
			Expr:    rewriteExpr(e.Expr, missingCols, materializedSet),
			Pattern: rewriteExpr(e.Pattern, missingCols, materializedSet),
			Not:     e.Not,
		}

	case *parser.ParenExpr:
		return &parser.ParenExpr{
			Expr: rewriteExpr(e.Expr, missingCols, materializedSet),
		}

	case *parser.StarExpr, *parser.Literal:
		return e

	default:
		return e
	}
}

// extractReferencedColumns walks the AST and collects all column names referenced.
func extractReferencedColumns(stmt *parser.SelectStatement) []string {
	seen := make(map[string]bool)
	var cols []string

	collect := func(name string) {
		if !seen[name] {
			seen[name] = true
			cols = append(cols, name)
		}
	}

	// SELECT columns
	for _, col := range stmt.Columns {
		collectColumnsFromExpr(col.Expr, collect)
	}

	// WHERE
	if stmt.Where != nil {
		collectColumnsFromExpr(stmt.Where, collect)
	}

	// GROUP BY
	for _, g := range stmt.GroupBy {
		collectColumnsFromExpr(g, collect)
	}

	// HAVING
	if stmt.Having != nil {
		collectColumnsFromExpr(stmt.Having, collect)
	}

	// ORDER BY
	for _, o := range stmt.OrderBy {
		collectColumnsFromExpr(o.Expr, collect)
	}

	return cols
}

// collectColumnsFromExpr recursively collects column names from an expression.
func collectColumnsFromExpr(expr parser.Expression, collect func(string)) {
	if expr == nil {
		return
	}

	switch e := expr.(type) {
	case *parser.ColumnRef:
		collect(e.Column)
	case *parser.BinaryExpr:
		collectColumnsFromExpr(e.Left, collect)
		collectColumnsFromExpr(e.Right, collect)
	case *parser.UnaryExpr:
		collectColumnsFromExpr(e.Operand, collect)
	case *parser.AggregateExpr:
		collectColumnsFromExpr(e.Arg, collect)
	case *parser.FunctionCall:
		for _, arg := range e.Args {
			collectColumnsFromExpr(arg, collect)
		}
	case *parser.InExpr:
		collectColumnsFromExpr(e.Expr, collect)
		for _, v := range e.Values {
			collectColumnsFromExpr(v, collect)
		}
	case *parser.BetweenExpr:
		collectColumnsFromExpr(e.Expr, collect)
		collectColumnsFromExpr(e.Low, collect)
		collectColumnsFromExpr(e.High, collect)
	case *parser.IsNullExpr:
		collectColumnsFromExpr(e.Expr, collect)
	case *parser.LikeExpr:
		collectColumnsFromExpr(e.Expr, collect)
		collectColumnsFromExpr(e.Pattern, collect)
	case *parser.ParenExpr:
		collectColumnsFromExpr(e.Expr, collect)
	}
}

// columnSet builds a set of column names from a schema.
func columnSet(schema *types.Schema) map[string]bool {
	cols := make(map[string]bool)
	for _, col := range schema.Columns {
		cols[col.Name] = true
	}
	return cols
}

// findMaterializedColumn finds a materialized column for a given JSON path.
func findMaterializedColumn(columns []partition.MaterializedColumn, jsonPath string) *partition.MaterializedColumn {
	for _, mc := range columns {
		if mc.JSONPath == jsonPath {
			return &mc
		}
	}
	return nil
}

// jsonPathToColumnName converts a JSON path to a materialized column name.
// This mirrors the logic in internal/schema/materializer.go
func jsonPathToColumnName(jsonPath string) string {
	result := strings.TrimPrefix(jsonPath, "$.")
	result = strings.ReplaceAll(result, ".", "_")
	return "payload_" + result
}
