package executor

import (
	"fmt"
	"sort"
	"strings"

	"github.com/arkilian/arkilian/internal/query/parser"
)

// ResultMerger merges partial results from multiple partitions.
type ResultMerger struct {
	// columns is the list of column names in the result
	columns []string

	// orderBy specifies the ORDER BY clauses for sorting
	orderBy []parser.OrderByClause

	// limit is the maximum number of rows to return (nil for no limit)
	limit *int64

	// offset is the number of rows to skip (nil for no offset)
	offset *int64
}

// NewResultMerger creates a new result merger.
func NewResultMerger(columns []string, orderBy []parser.OrderByClause, limit, offset *int64) *ResultMerger {
	return &ResultMerger{
		columns: columns,
		orderBy: orderBy,
		limit:   limit,
		offset:  offset,
	}
}

// Merge combines partial results using stream-oriented UNION ALL semantics.
// It supports early termination for LIMIT queries.
func (m *ResultMerger) Merge(partialResults []*PartialResult) (*QueryResult, error) {
	// Collect all rows from successful partial results
	var allRows [][]interface{}
	var columns []string
	var totalRowsScanned int64

	for _, pr := range partialResults {
		if pr == nil || pr.Error != nil {
			continue
		}

		// Use columns from first successful result
		if columns == nil && len(pr.Columns) > 0 {
			columns = pr.Columns
		}

		allRows = append(allRows, pr.Rows...)
		totalRowsScanned += pr.RowCount
	}

	// Use provided columns if none found
	if columns == nil {
		columns = m.columns
	}

	// Apply ORDER BY if specified
	if len(m.orderBy) > 0 && len(allRows) > 0 {
		if err := m.sortRows(allRows, columns); err != nil {
			return nil, fmt.Errorf("merger: failed to sort rows: %w", err)
		}
	}

	// Apply OFFSET if specified
	if m.offset != nil && *m.offset > 0 {
		offset := int(*m.offset)
		if offset >= len(allRows) {
			allRows = [][]interface{}{}
		} else {
			allRows = allRows[offset:]
		}
	}

	// Apply LIMIT if specified (early termination)
	if m.limit != nil {
		limit := int(*m.limit)
		if len(allRows) > limit {
			allRows = allRows[:limit]
		}
	}

	return &QueryResult{
		Columns: columns,
		Rows:    allRows,
		Stats: ExecutionStats{
			PartitionsScanned: len(partialResults),
			RowsScanned:       totalRowsScanned,
		},
	}, nil
}

// MergeWithEarlyTermination merges results with early termination support.
// This is useful for LIMIT queries where we can stop once we have enough rows.
func (m *ResultMerger) MergeWithEarlyTermination(
	resultChan <-chan *PartialResult,
	done chan<- struct{},
) (*QueryResult, error) {
	var allRows [][]interface{}
	var columns []string
	var totalRowsScanned int64
	var partitionsScanned int

	targetRows := -1
	if m.limit != nil {
		targetRows = int(*m.limit)
		if m.offset != nil {
			targetRows += int(*m.offset)
		}
	}

	for pr := range resultChan {
		if pr == nil || pr.Error != nil {
			partitionsScanned++
			continue
		}

		if columns == nil && len(pr.Columns) > 0 {
			columns = pr.Columns
		}

		allRows = append(allRows, pr.Rows...)
		totalRowsScanned += pr.RowCount
		partitionsScanned++

		// Check for early termination
		// Only terminate early if we have no ORDER BY (otherwise we need all rows to sort)
		if targetRows > 0 && len(m.orderBy) == 0 && len(allRows) >= targetRows {
			close(done) // Signal to stop fetching more results
			break
		}
	}

	// Drain remaining results if we terminated early
	for range resultChan {
		partitionsScanned++
	}

	if columns == nil {
		columns = m.columns
	}

	// Apply ORDER BY if specified
	if len(m.orderBy) > 0 && len(allRows) > 0 {
		if err := m.sortRows(allRows, columns); err != nil {
			return nil, fmt.Errorf("merger: failed to sort rows: %w", err)
		}
	}

	// Apply OFFSET
	if m.offset != nil && *m.offset > 0 {
		offset := int(*m.offset)
		if offset >= len(allRows) {
			allRows = [][]interface{}{}
		} else {
			allRows = allRows[offset:]
		}
	}

	// Apply LIMIT
	if m.limit != nil {
		limit := int(*m.limit)
		if len(allRows) > limit {
			allRows = allRows[:limit]
		}
	}

	return &QueryResult{
		Columns: columns,
		Rows:    allRows,
		Stats: ExecutionStats{
			PartitionsScanned: partitionsScanned,
			RowsScanned:       totalRowsScanned,
		},
	}, nil
}

// sortRows sorts rows according to ORDER BY clauses.
func (m *ResultMerger) sortRows(rows [][]interface{}, columns []string) error {
	// Build column index map
	colIndex := make(map[string]int)
	for i, col := range columns {
		colIndex[col] = i
		// Also map lowercase version
		colIndex[strings.ToLower(col)] = i
	}

	// Resolve ORDER BY column indices
	orderIndices := make([]int, len(m.orderBy))
	for i, ob := range m.orderBy {
		colName := ob.Expr.String()
		idx, ok := colIndex[colName]
		if !ok {
			idx, ok = colIndex[strings.ToLower(colName)]
		}
		if !ok {
			return fmt.Errorf("column %s not found in result", colName)
		}
		orderIndices[i] = idx
	}

	// Sort rows
	sort.SliceStable(rows, func(i, j int) bool {
		for k, ob := range m.orderBy {
			idx := orderIndices[k]
			cmp := compareValues(rows[i][idx], rows[j][idx])

			if cmp == 0 {
				continue
			}

			if ob.Desc {
				return cmp > 0
			}
			return cmp < 0
		}
		return false
	})

	return nil
}

// compareValues compares two values for sorting.
func compareValues(a, b interface{}) int {
	// Handle nil values
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return -1 // NULL sorts first
	}
	if b == nil {
		return 1
	}

	// Compare based on type
	switch va := a.(type) {
	case int64:
		if vb, ok := b.(int64); ok {
			if va < vb {
				return -1
			} else if va > vb {
				return 1
			}
			return 0
		}
		// Try to convert b to int64
		if vb, ok := toInt64(b); ok {
			if va < vb {
				return -1
			} else if va > vb {
				return 1
			}
			return 0
		}

	case int:
		return compareValues(int64(va), b)

	case float64:
		if vb, ok := b.(float64); ok {
			if va < vb {
				return -1
			} else if va > vb {
				return 1
			}
			return 0
		}
		// Try to convert b to float64
		if vb, ok := toFloat64(b); ok {
			if va < vb {
				return -1
			} else if va > vb {
				return 1
			}
			return 0
		}

	case string:
		if vb, ok := b.(string); ok {
			if va < vb {
				return -1
			} else if va > vb {
				return 1
			}
			return 0
		}

	case []byte:
		if vb, ok := b.([]byte); ok {
			return compareBytes(va, vb)
		}
		if vb, ok := b.(string); ok {
			return compareBytes(va, []byte(vb))
		}

	case bool:
		if vb, ok := b.(bool); ok {
			if !va && vb {
				return -1
			} else if va && !vb {
				return 1
			}
			return 0
		}
	}

	// Fallback: compare string representations
	sa := fmt.Sprintf("%v", a)
	sb := fmt.Sprintf("%v", b)
	if sa < sb {
		return -1
	} else if sa > sb {
		return 1
	}
	return 0
}

// toInt64 attempts to convert a value to int64.
func toInt64(v interface{}) (int64, bool) {
	switch val := v.(type) {
	case int64:
		return val, true
	case int:
		return int64(val), true
	case int32:
		return int64(val), true
	case float64:
		return int64(val), true
	case float32:
		return int64(val), true
	}
	return 0, false
}

// toFloat64 attempts to convert a value to float64.
func toFloat64(v interface{}) (float64, bool) {
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
	}
	return 0, false
}

// compareBytes compares two byte slices.
func compareBytes(a, b []byte) int {
	minLen := len(a)
	if len(b) < minLen {
		minLen = len(b)
	}

	for i := 0; i < minLen; i++ {
		if a[i] < b[i] {
			return -1
		} else if a[i] > b[i] {
			return 1
		}
	}

	if len(a) < len(b) {
		return -1
	} else if len(a) > len(b) {
		return 1
	}
	return 0
}

// StreamMerger provides streaming merge capabilities for large result sets.
type StreamMerger struct {
	columns []string
	orderBy []parser.OrderByClause
	limit   *int64
	offset  *int64
}

// NewStreamMerger creates a new streaming merger.
func NewStreamMerger(columns []string, orderBy []parser.OrderByClause, limit, offset *int64) *StreamMerger {
	return &StreamMerger{
		columns: columns,
		orderBy: orderBy,
		limit:   limit,
		offset:  offset,
	}
}

// heapItem represents an item in the merge heap.
type heapItem struct {
	row       []interface{}
	streamIdx int
}

// MergeStreams performs a merge sort on multiple sorted streams.
// This is useful when each partition's results are already sorted.
func (m *StreamMerger) MergeStreams(streams []<-chan []interface{}) <-chan []interface{} {
	out := make(chan []interface{})

	go func() {
		defer close(out)

		// Initialize heap with first element from each stream
		heap := make([]heapItem, 0, len(streams))
		activeStreams := make([]<-chan []interface{}, len(streams))
		copy(activeStreams, streams)

		// Get first row from each stream
		for i, stream := range streams {
			if row, ok := <-stream; ok {
				heap = append(heap, heapItem{row: row, streamIdx: i})
			}
		}

		// Build initial heap
		m.buildHeap(heap)

		rowCount := 0
		offset := 0
		if m.offset != nil {
			offset = int(*m.offset)
		}
		limit := -1
		if m.limit != nil {
			limit = int(*m.limit)
		}

		for len(heap) > 0 {
			// Pop minimum element
			min := heap[0]

			// Replace with next element from same stream
			if row, ok := <-activeStreams[min.streamIdx]; ok {
				heap[0] = heapItem{row: row, streamIdx: min.streamIdx}
				m.heapifyDown(heap, 0)
			} else {
				// Stream exhausted, remove from heap
				heap[0] = heap[len(heap)-1]
				heap = heap[:len(heap)-1]
				if len(heap) > 0 {
					m.heapifyDown(heap, 0)
				}
			}

			// Apply offset
			if rowCount < offset {
				rowCount++
				continue
			}

			// Check limit
			if limit >= 0 && rowCount-offset >= limit {
				break
			}

			out <- min.row
			rowCount++
		}
	}()

	return out
}

// buildHeap builds a min-heap from the items.
func (m *StreamMerger) buildHeap(heap []heapItem) {
	for i := len(heap)/2 - 1; i >= 0; i-- {
		m.heapifyDown(heap, i)
	}
}

// heapifyDown maintains heap property by moving element down.
func (m *StreamMerger) heapifyDown(heap []heapItem, i int) {
	for {
		smallest := i
		left := 2*i + 1
		right := 2*i + 2

		if left < len(heap) && m.compareRows(heap[left].row, heap[smallest].row) < 0 {
			smallest = left
		}
		if right < len(heap) && m.compareRows(heap[right].row, heap[smallest].row) < 0 {
			smallest = right
		}

		if smallest == i {
			break
		}

		heap[i], heap[smallest] = heap[smallest], heap[i]
		i = smallest
	}
}

// compareRows compares two rows according to ORDER BY clauses.
func (m *StreamMerger) compareRows(a, b []interface{}) int {
	if len(m.orderBy) == 0 {
		return 0
	}

	// Build column index map
	colIndex := make(map[string]int)
	for i, col := range m.columns {
		colIndex[col] = i
		colIndex[strings.ToLower(col)] = i
	}

	for _, ob := range m.orderBy {
		colName := ob.Expr.String()
		idx, ok := colIndex[colName]
		if !ok {
			idx, ok = colIndex[strings.ToLower(colName)]
		}
		if !ok || idx >= len(a) || idx >= len(b) {
			continue
		}

		cmp := compareValues(a[idx], b[idx])
		if cmp == 0 {
			continue
		}

		if ob.Desc {
			return -cmp
		}
		return cmp
	}

	return 0
}

// UnionAllMerger performs simple UNION ALL merge without sorting.
type UnionAllMerger struct {
	limit  *int64
	offset *int64
}

// NewUnionAllMerger creates a new UNION ALL merger.
func NewUnionAllMerger(limit, offset *int64) *UnionAllMerger {
	return &UnionAllMerger{
		limit:  limit,
		offset: offset,
	}
}

// Merge combines all partial results using UNION ALL semantics.
func (m *UnionAllMerger) Merge(partialResults []*PartialResult) (*QueryResult, error) {
	var allRows [][]interface{}
	var columns []string
	var totalRowsScanned int64

	for _, pr := range partialResults {
		if pr == nil || pr.Error != nil {
			continue
		}

		if columns == nil && len(pr.Columns) > 0 {
			columns = pr.Columns
		}

		allRows = append(allRows, pr.Rows...)
		totalRowsScanned += pr.RowCount
	}

	// Apply OFFSET
	if m.offset != nil && *m.offset > 0 {
		offset := int(*m.offset)
		if offset >= len(allRows) {
			allRows = [][]interface{}{}
		} else {
			allRows = allRows[offset:]
		}
	}

	// Apply LIMIT
	if m.limit != nil {
		limit := int(*m.limit)
		if len(allRows) > limit {
			allRows = allRows[:limit]
		}
	}

	return &QueryResult{
		Columns: columns,
		Rows:    allRows,
		Stats: ExecutionStats{
			PartitionsScanned: len(partialResults),
			RowsScanned:       totalRowsScanned,
		},
	}, nil
}
