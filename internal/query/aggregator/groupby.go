package aggregator

import (
	"fmt"
	"strings"

	"github.com/arkilian/arkilian/internal/query/parser"
)

// GroupKey is a string representation of a GROUP BY key tuple,
// used as a map key for combining groups across partitions.
type GroupKey = string

// GroupedPartialResult holds partial aggregates for a single group.
type GroupedPartialResult struct {
	KeyValues  []interface{}       // the actual GROUP BY column values
	Aggregates []*PartialAggregate // one per aggregate expression
}

// GroupByMerger combines grouped partial results from multiple partitions.
type GroupByMerger struct {
	groupByExprs []parser.Expression
	aggExprs     []*parser.AggregateExpr
}

// NewGroupByMerger creates a merger for GROUP BY queries.
func NewGroupByMerger(
	groupByExprs []parser.Expression,
	aggExprs []*parser.AggregateExpr,
) *GroupByMerger {
	return &GroupByMerger{
		groupByExprs: groupByExprs,
		aggExprs:     aggExprs,
	}
}

// ComputeGroupedPartials computes grouped partial aggregates from rows
// belonging to a single partition.
//
// groupColIndices maps each GROUP BY expression to its column index in the row.
// aggColIndices maps each aggregate expression's argument to its column index
// (-1 for COUNT(*)).
func ComputeGroupedPartials(
	rows [][]interface{},
	aggExprs []*parser.AggregateExpr,
	groupColIndices []int,
	aggColIndices []int,
) map[GroupKey]*GroupedPartialResult {
	groups := make(map[GroupKey]*GroupedPartialResult)

	for _, row := range rows {
		// Build group key
		keyVals := make([]interface{}, len(groupColIndices))
		for i, idx := range groupColIndices {
			if idx >= 0 && idx < len(row) {
				keyVals[i] = row[idx]
			}
		}
		key := groupKeyString(keyVals)

		gpr, exists := groups[key]
		if !exists {
			// Initialize aggregates for this group
			aggs := make([]*PartialAggregate, len(aggExprs))
			for i, expr := range aggExprs {
				aggType, _ := ParseAggregateType(expr.Function)
				aggs[i] = NewPartialAggregate(aggType)
			}
			gpr = &GroupedPartialResult{
				KeyValues:  keyVals,
				Aggregates: aggs,
			}
			groups[key] = gpr
		}

		// Accumulate values
		for i, agg := range gpr.Aggregates {
			idx := aggColIndices[i]
			if idx < 0 {
				agg.Accumulate(int64(1))
			} else if idx < len(row) {
				agg.Accumulate(row[idx])
			}
		}
	}

	return groups
}

// MergeGroupedPartials merges grouped partial results from multiple partitions.
// Groups with the same key are combined by merging their partial aggregates.
func (m *GroupByMerger) MergeGroupedPartials(
	partitionResults []map[GroupKey]*GroupedPartialResult,
) map[GroupKey]*GroupedPartialResult {
	merged := make(map[GroupKey]*GroupedPartialResult)

	for _, partResult := range partitionResults {
		for key, gpr := range partResult {
			existing, exists := merged[key]
			if !exists {
				// First time seeing this group — clone it
				cloned := &GroupedPartialResult{
					KeyValues:  gpr.KeyValues,
					Aggregates: make([]*PartialAggregate, len(gpr.Aggregates)),
				}
				for i, agg := range gpr.Aggregates {
					cloned.Aggregates[i] = &PartialAggregate{
						Type:  agg.Type,
						Count: agg.Count,
						Sum:   agg.Sum,
						Min:   agg.Min,
						Max:   agg.Max,
						IsSet: agg.IsSet,
					}
				}
				merged[key] = cloned
				continue
			}

			// Merge into existing group
			for i, agg := range gpr.Aggregates {
				if i >= len(existing.Aggregates) {
					break
				}
				mergeInto(existing.Aggregates[i], agg)
			}
		}
	}

	return merged
}

// ToRows converts merged grouped results into row format.
// Each row contains the group key values followed by the aggregate results.
func (m *GroupByMerger) ToRows(groups map[GroupKey]*GroupedPartialResult) [][]interface{} {
	rows := make([][]interface{}, 0, len(groups))

	for _, gpr := range groups {
		row := make([]interface{}, 0, len(gpr.KeyValues)+len(gpr.Aggregates))
		row = append(row, gpr.KeyValues...)
		for _, agg := range gpr.Aggregates {
			row = append(row, agg.Result())
		}
		rows = append(rows, row)
	}

	return rows
}

// mergeInto merges source partial aggregate into dest.
func mergeInto(dest, src *PartialAggregate) {
	if !src.IsSet {
		return
	}

	switch dest.Type {
	case AggCount:
		dest.Count += src.Count
		dest.IsSet = true

	case AggSum:
		dest.Sum += src.Sum
		dest.Count += src.Count
		dest.IsSet = true

	case AggMin:
		if !dest.IsSet || compareAggValues(src.Min, dest.Min) < 0 {
			dest.Min = src.Min
		}
		dest.Count += src.Count
		dest.IsSet = true

	case AggMax:
		if !dest.IsSet || compareAggValues(src.Max, dest.Max) > 0 {
			dest.Max = src.Max
		}
		dest.Count += src.Count
		dest.IsSet = true

	case AggAvg:
		dest.Sum += src.Sum
		dest.Count += src.Count
		dest.IsSet = true
	}
}

// groupKeyString produces a deterministic string key from a slice of values.
func groupKeyString(vals []interface{}) string {
	parts := make([]string, len(vals))
	for i, v := range vals {
		if v == nil {
			parts[i] = "<NULL>"
		} else {
			parts[i] = fmt.Sprintf("%v", v)
		}
	}
	return strings.Join(parts, "|")
}

// ResolveGroupByColumnIndices maps GROUP BY expressions to column indices.
func ResolveGroupByColumnIndices(
	groupByExprs []parser.Expression,
	columns []string,
) []int {
	colMap := make(map[string]int)
	for i, c := range columns {
		colMap[strings.ToLower(c)] = i
	}

	indices := make([]int, len(groupByExprs))
	for i, expr := range groupByExprs {
		colName := strings.ToLower(expr.String())
		if idx, ok := colMap[colName]; ok {
			indices[i] = idx
		} else {
			indices[i] = -1
		}
	}
	return indices
}
