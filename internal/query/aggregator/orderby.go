package aggregator

import (
	"fmt"
	"sort"
	"strings"

	"github.com/arkilian/arkilian/internal/query/parser"
)

// OrderBySorter sorts rows according to ORDER BY clauses.
// It supports multiple columns and ASC/DESC directions.
type OrderBySorter struct {
	clauses []parser.OrderByClause
	columns []string
}

// NewOrderBySorter creates a new sorter for the given ORDER BY clauses.
func NewOrderBySorter(clauses []parser.OrderByClause, columns []string) *OrderBySorter {
	return &OrderBySorter{
		clauses: clauses,
		columns: columns,
	}
}

// Sort sorts the rows in place according to the ORDER BY clauses.
func (s *OrderBySorter) Sort(rows [][]interface{}) error {
	if len(s.clauses) == 0 || len(rows) <= 1 {
		return nil
	}

	// Resolve column indices once
	colMap := make(map[string]int)
	for i, c := range s.columns {
		colMap[strings.ToLower(c)] = i
	}

	indices := make([]int, len(s.clauses))
	for i, clause := range s.clauses {
		colName := strings.ToLower(clause.Expr.String())
		idx, ok := colMap[colName]
		if !ok {
			return fmt.Errorf("orderby: column %q not found in result columns", clause.Expr.String())
		}
		indices[i] = idx
	}

	// Stable sort preserves insertion order for equal elements
	sort.SliceStable(rows, func(i, j int) bool {
		for k, clause := range s.clauses {
			idx := indices[k]
			var a, b interface{}
			if idx < len(rows[i]) {
				a = rows[i][idx]
			}
			if idx < len(rows[j]) {
				b = rows[j][idx]
			}

			cmp := compareAggValues(a, b)
			if cmp == 0 {
				continue
			}
			if clause.Desc {
				return cmp > 0
			}
			return cmp < 0
		}
		return false
	})

	return nil
}

// SortAndLimit sorts rows and applies LIMIT/OFFSET.
func (s *OrderBySorter) SortAndLimit(rows [][]interface{}, limit, offset *int64) ([][]interface{}, error) {
	if err := s.Sort(rows); err != nil {
		return nil, err
	}

	// Apply OFFSET
	if offset != nil && *offset > 0 {
		off := int(*offset)
		if off >= len(rows) {
			return [][]interface{}{}, nil
		}
		rows = rows[off:]
	}

	// Apply LIMIT
	if limit != nil {
		lim := int(*limit)
		if lim < len(rows) {
			rows = rows[:lim]
		}
	}

	return rows, nil
}

// MergeSortedStreams performs a k-way merge sort on pre-sorted row streams.
// Each stream must already be sorted according to the same ORDER BY clauses.
func (s *OrderBySorter) MergeSortedStreams(streams [][]interface{}, streamBoundaries []int) [][]interface{} {
	// For simplicity, concatenate and re-sort.
	// A true k-way merge with a heap would be more efficient for large datasets,
	// but the existing StreamMerger in executor/merger.go handles that case.
	_ = s.Sort(streams)
	return streams
}
