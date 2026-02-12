package executor

import (
	"testing"

	"github.com/arkilian/arkilian/internal/query/parser"
)

func TestStreamingCollector_BasicCollection(t *testing.T) {
	collector := NewStreamingCollector(
		[]string{"id", "name"},
		nil, nil, nil,
		256*1024*1024,
	)

	rowChan := make(chan streamedRow, 10)
	done := make(chan struct{})

	// Send some rows
	go func() {
		for i := 0; i < 5; i++ {
			rowChan <- streamedRow{
				Columns: []string{"id", "name"},
				Row:     []interface{}{int64(i), "test"},
			}
		}
		close(rowChan)
	}()

	result, err := collector.CollectFromChannel(rowChan, done)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(result.Rows) != 5 {
		t.Fatalf("expected 5 rows, got %d", len(result.Rows))
	}
}

func TestStreamingCollector_LimitEarlyTermination(t *testing.T) {
	limit := int64(3)
	collector := NewStreamingCollector(
		[]string{"id"},
		nil, // no ORDER BY — enables early termination
		&limit, nil,
		256*1024*1024,
	)

	rowChan := make(chan streamedRow, 100)
	done := make(chan struct{})

	// Send more rows than the limit
	go func() {
		for i := 0; i < 100; i++ {
			select {
			case rowChan <- streamedRow{
				Columns: []string{"id"},
				Row:     []interface{}{int64(i)},
			}:
			case <-done:
				close(rowChan)
				return
			}
		}
		close(rowChan)
	}()

	result, err := collector.CollectFromChannel(rowChan, done)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(result.Rows) != 3 {
		t.Fatalf("expected 3 rows after LIMIT, got %d", len(result.Rows))
	}
}

func TestStreamingCollector_MemoryBound(t *testing.T) {
	// Set a very small memory limit to trigger the bound
	collector := NewStreamingCollector(
		[]string{"data"},
		nil, nil, nil,
		200, // 200 bytes — very small
	)

	rowChan := make(chan streamedRow, 100)
	done := make(chan struct{})

	go func() {
		for i := 0; i < 50; i++ {
			select {
			case rowChan <- streamedRow{
				Columns: []string{"data"},
				Row:     []interface{}{"a long string value that takes up memory space in the collector"},
			}:
			case <-done:
				close(rowChan)
				return
			}
		}
		close(rowChan)
	}()

	result, err := collector.CollectFromChannel(rowChan, done)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should have collected fewer than 50 rows due to memory bound
	if len(result.Rows) >= 50 {
		t.Fatalf("expected fewer than 50 rows due to memory bound, got %d", len(result.Rows))
	}
}

func TestStreamingCollector_WithOrderBy(t *testing.T) {
	limit := int64(3)
	collector := NewStreamingCollector(
		[]string{"id"},
		[]parser.OrderByClause{{Expr: &parser.ColumnRef{Column: "id"}, Desc: true}},
		&limit, nil,
		256*1024*1024,
	)

	rowChan := make(chan streamedRow, 10)
	done := make(chan struct{})

	go func() {
		for i := 0; i < 10; i++ {
			rowChan <- streamedRow{
				Columns: []string{"id"},
				Row:     []interface{}{int64(i)},
			}
		}
		close(rowChan)
	}()

	result, err := collector.CollectFromChannel(rowChan, done)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(result.Rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(result.Rows))
	}

	// Should be sorted descending: 9, 8, 7
	for i, expected := range []int64{9, 8, 7} {
		if val, ok := result.Rows[i][0].(int64); !ok || val != expected {
			t.Fatalf("row %d: expected %d, got %v", i, expected, result.Rows[i][0])
		}
	}
}

func TestEstimateRowSize(t *testing.T) {
	row := []interface{}{int64(42), "hello", []byte{1, 2, 3}}
	size := estimateRowSize(row)
	if size <= 0 {
		t.Fatalf("expected positive size, got %d", size)
	}

	// A row with a map should be larger
	rowWithMap := []interface{}{map[string]interface{}{"key": "value", "num": 42}}
	sizeWithMap := estimateRowSize(rowWithMap)
	if sizeWithMap <= 0 {
		t.Fatalf("expected positive size for map row, got %d", sizeWithMap)
	}
}

func TestResultMerger_KWayMergeWithOrderBy(t *testing.T) {
	orderBy := []parser.OrderByClause{
		{Expr: &parser.ColumnRef{Column: "id"}},
	}
	merger := NewResultMerger([]string{"id"}, orderBy, nil, nil)

	// Two partitions with pre-sorted results
	results := []*PartialResult{
		{
			Columns:  []string{"id"},
			Rows:     [][]interface{}{{int64(1)}, {int64(3)}, {int64(5)}},
			RowCount: 3,
		},
		{
			Columns:  []string{"id"},
			Rows:     [][]interface{}{{int64(2)}, {int64(4)}, {int64(6)}},
			RowCount: 3,
		},
	}

	result, err := merger.Merge(results)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(result.Rows) != 6 {
		t.Fatalf("expected 6 rows, got %d", len(result.Rows))
	}

	// Should be merged in sorted order: 1, 2, 3, 4, 5, 6
	for i, expected := range []int64{1, 2, 3, 4, 5, 6} {
		if val, ok := result.Rows[i][0].(int64); !ok || val != expected {
			t.Fatalf("row %d: expected %d, got %v", i, expected, result.Rows[i][0])
		}
	}
}

func TestResultMerger_KWayMergeWithLimit(t *testing.T) {
	orderBy := []parser.OrderByClause{
		{Expr: &parser.ColumnRef{Column: "id"}},
	}
	limit := int64(3)
	merger := NewResultMerger([]string{"id"}, orderBy, &limit, nil)

	results := []*PartialResult{
		{
			Columns:  []string{"id"},
			Rows:     [][]interface{}{{int64(1)}, {int64(4)}, {int64(7)}},
			RowCount: 3,
		},
		{
			Columns:  []string{"id"},
			Rows:     [][]interface{}{{int64(2)}, {int64(5)}, {int64(8)}},
			RowCount: 3,
		},
		{
			Columns:  []string{"id"},
			Rows:     [][]interface{}{{int64(3)}, {int64(6)}, {int64(9)}},
			RowCount: 3,
		},
	}

	result, err := merger.Merge(results)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(result.Rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(result.Rows))
	}

	for i, expected := range []int64{1, 2, 3} {
		if val, ok := result.Rows[i][0].(int64); !ok || val != expected {
			t.Fatalf("row %d: expected %d, got %v", i, expected, result.Rows[i][0])
		}
	}
}
