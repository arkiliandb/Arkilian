// Package aggregator provides aggregate computation and merging for distributed
// query execution across partitions.
package aggregator

import (
	"fmt"
	"strings"

	"github.com/arkilian/arkilian/internal/query/parser"
)

// AggregateType represents the type of aggregate function.
type AggregateType int

const (
	AggCount AggregateType = iota
	AggSum
	AggMin
	AggMax
	AggAvg
)

// ParseAggregateType converts a function name string to AggregateType.
func ParseAggregateType(name string) (AggregateType, error) {
	switch strings.ToUpper(name) {
	case "COUNT":
		return AggCount, nil
	case "SUM":
		return AggSum, nil
	case "MIN":
		return AggMin, nil
	case "MAX":
		return AggMax, nil
	case "AVG":
		return AggAvg, nil
	default:
		return 0, fmt.Errorf("unknown aggregate function: %s", name)
	}
}

// PartialAggregate holds the partial result of an aggregate computation
// from a single partition. For AVG, both Sum and Count are tracked so
// that a correct weighted average can be computed during merge.
type PartialAggregate struct {
	Type  AggregateType
	Count int64       // row count (used by COUNT and AVG)
	Sum   float64     // running sum (used by SUM and AVG)
	Min   interface{} // current minimum (nil if no rows)
	Max   interface{} // current maximum (nil if no rows)
	IsSet bool        // true once at least one value has been accumulated
}

// NewPartialAggregate creates a new empty partial aggregate of the given type.
func NewPartialAggregate(aggType AggregateType) *PartialAggregate {
	return &PartialAggregate{Type: aggType}
}

// Accumulate adds a single value to the partial aggregate.
func (p *PartialAggregate) Accumulate(value interface{}) {
	if value == nil {
		return // NULL values are ignored by all aggregate functions
	}

	switch p.Type {
	case AggCount:
		p.Count++
		p.IsSet = true

	case AggSum:
		if f, ok := toFloat(value); ok {
			p.Sum += f
			p.Count++
			p.IsSet = true
		}

	case AggMin:
		if !p.IsSet || compareAggValues(value, p.Min) < 0 {
			p.Min = value
			p.IsSet = true
		}
		p.Count++

	case AggMax:
		if !p.IsSet || compareAggValues(value, p.Max) > 0 {
			p.Max = value
			p.IsSet = true
		}
		p.Count++

	case AggAvg:
		if f, ok := toFloat(value); ok {
			p.Sum += f
			p.Count++
			p.IsSet = true
		}
	}
}

// Result returns the final value of this partial aggregate.
func (p *PartialAggregate) Result() interface{} {
	if !p.IsSet {
		if p.Type == AggCount {
			return int64(0)
		}
		return nil
	}

	switch p.Type {
	case AggCount:
		return p.Count
	case AggSum:
		return p.Sum
	case AggMin:
		return p.Min
	case AggMax:
		return p.Max
	case AggAvg:
		if p.Count == 0 {
			return nil
		}
		return p.Sum / float64(p.Count)
	}
	return nil
}

// PartialAggregateSet holds partial aggregates for all aggregate columns
// in a single query, computed from one partition's data.
type PartialAggregateSet struct {
	Aggregates []*PartialAggregate
}

// NewPartialAggregateSet creates a set of partial aggregates matching the
// aggregate expressions in the SELECT statement.
func NewPartialAggregateSet(aggExprs []*parser.AggregateExpr) *PartialAggregateSet {
	aggs := make([]*PartialAggregate, len(aggExprs))
	for i, expr := range aggExprs {
		aggType, _ := ParseAggregateType(expr.Function)
		aggs[i] = NewPartialAggregate(aggType)
	}
	return &PartialAggregateSet{Aggregates: aggs}
}

// ComputePartialAggregates computes partial aggregates from a set of rows.
// aggExprs defines which aggregates to compute, and colIndices maps each
// aggregate's argument column to its index in the row slice.
func ComputePartialAggregates(
	rows [][]interface{},
	aggExprs []*parser.AggregateExpr,
	colIndices []int,
) *PartialAggregateSet {
	set := NewPartialAggregateSet(aggExprs)

	for _, row := range rows {
		for i, agg := range set.Aggregates {
			idx := colIndices[i]
			if idx < 0 {
				// COUNT(*) — count every row regardless of column
				agg.Accumulate(int64(1))
			} else if idx < len(row) {
				agg.Accumulate(row[idx])
			}
		}
	}

	return set
}

// Results returns the computed values for all aggregates in the set.
func (s *PartialAggregateSet) Results() []interface{} {
	results := make([]interface{}, len(s.Aggregates))
	for i, agg := range s.Aggregates {
		results[i] = agg.Result()
	}
	return results
}

// ExtractAggregateExprs finds all AggregateExpr nodes in a SELECT statement's columns.
func ExtractAggregateExprs(stmt *parser.SelectStatement) []*parser.AggregateExpr {
	var aggs []*parser.AggregateExpr
	for _, col := range stmt.Columns {
		if aggExpr, ok := col.Expr.(*parser.AggregateExpr); ok {
			aggs = append(aggs, aggExpr)
		}
	}
	return aggs
}

// ResolveAggregateColumnIndices maps each aggregate expression's argument
// to a column index in the row. Returns -1 for COUNT(*).
func ResolveAggregateColumnIndices(
	aggExprs []*parser.AggregateExpr,
	columns []string,
) []int {
	colMap := make(map[string]int)
	for i, c := range columns {
		colMap[strings.ToLower(c)] = i
	}

	indices := make([]int, len(aggExprs))
	for i, expr := range aggExprs {
		if expr.Arg == nil {
			// COUNT(*)
			indices[i] = -1
			continue
		}
		colName := strings.ToLower(expr.Arg.String())
		if idx, ok := colMap[colName]; ok {
			indices[i] = idx
		} else {
			indices[i] = -1
		}
	}
	return indices
}

// toFloat converts a value to float64 for numeric aggregation.
func toFloat(v interface{}) (float64, bool) {
	switch val := v.(type) {
	case float64:
		return val, true
	case float32:
		return float64(val), true
	case int64:
		return float64(val), true
	case int:
		return float64(val), true
	case int32:
		return float64(val), true
	case int16:
		return float64(val), true
	case int8:
		return float64(val), true
	case uint64:
		return float64(val), true
	case uint32:
		return float64(val), true
	case uint:
		return float64(val), true
	}
	return 0, false
}

// compareAggValues compares two values for MIN/MAX aggregation.
func compareAggValues(a, b interface{}) int {
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return -1
	}
	if b == nil {
		return 1
	}

	// Numeric comparison
	fa, aOk := toFloat(a)
	fb, bOk := toFloat(b)
	if aOk && bOk {
		if fa < fb {
			return -1
		} else if fa > fb {
			return 1
		}
		return 0
	}

	// String comparison
	sa, aStr := a.(string)
	sb, bStr := b.(string)
	if aStr && bStr {
		if sa < sb {
			return -1
		} else if sa > sb {
			return 1
		}
		return 0
	}

	// Fallback: compare as strings
	sa = fmt.Sprintf("%v", a)
	sb = fmt.Sprintf("%v", b)
	if sa < sb {
		return -1
	} else if sa > sb {
		return 1
	}
	return 0
}

// IsAggregateQuery returns true if the statement contains aggregate functions.
func IsAggregateQuery(stmt *parser.SelectStatement) bool {
	for _, col := range stmt.Columns {
		if _, ok := col.Expr.(*parser.AggregateExpr); ok {
			return true
		}
	}
	return false
}


