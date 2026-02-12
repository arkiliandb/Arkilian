package aggregator

import (
	"os"
	"testing"

	"github.com/arkilian/arkilian/internal/query/parser"
)

func ascClause(col string) parser.OrderByClause {
	return parser.OrderByClause{Expr: &parser.ColumnRef{Column: col}}
}

func descClause(col string) parser.OrderByClause {
	return parser.OrderByClause{Expr: &parser.ColumnRef{Column: col}, Desc: true}
}

func TestSort_Basic(t *testing.T) {
	s := NewOrderBySorter(
		[]parser.OrderByClause{ascClause("id")},
		[]string{"id", "name"},
	)
	rows := [][]interface{}{
		{int64(3), "c"},
		{int64(1), "a"},
		{int64(2), "b"},
	}
	if err := s.Sort(rows); err != nil {
		t.Fatal(err)
	}
	for i, expected := range []int64{1, 2, 3} {
		if rows[i][0] != expected {
			t.Fatalf("row %d: expected %d, got %v", i, expected, rows[i][0])
		}
	}
}

func TestTopN_Basic(t *testing.T) {
	s := NewOrderBySorter(
		[]parser.OrderByClause{ascClause("id")},
		[]string{"id"},
	)
	rows := make([][]interface{}, 1000)
	for i := range rows {
		rows[i] = []interface{}{int64(1000 - i)}
	}

	result, err := s.TopN(rows, 5)
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 5 {
		t.Fatalf("expected 5 rows, got %d", len(result))
	}
	for i, expected := range []int64{1, 2, 3, 4, 5} {
		if result[i][0] != expected {
			t.Fatalf("row %d: expected %d, got %v", i, expected, result[i][0])
		}
	}
}

func TestTopN_Desc(t *testing.T) {
	s := NewOrderBySorter(
		[]parser.OrderByClause{descClause("val")},
		[]string{"val"},
	)
	rows := make([][]interface{}, 100)
	for i := range rows {
		rows[i] = []interface{}{int64(i)}
	}

	result, err := s.TopN(rows, 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(result))
	}
	// DESC: 99, 98, 97
	for i, expected := range []int64{99, 98, 97} {
		if result[i][0] != expected {
			t.Fatalf("row %d: expected %d, got %v", i, expected, result[i][0])
		}
	}
}

func TestTopN_FewerRowsThanN(t *testing.T) {
	s := NewOrderBySorter(
		[]parser.OrderByClause{ascClause("id")},
		[]string{"id"},
	)
	rows := [][]interface{}{{int64(3)}, {int64(1)}, {int64(2)}}

	result, err := s.TopN(rows, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(result))
	}
}

func TestSortExternal_FallbackToInMemory(t *testing.T) {
	s := NewOrderBySorter(
		[]parser.OrderByClause{ascClause("id")},
		[]string{"id"},
	)
	s.SetExternalSortThreshold(100)

	rows := [][]interface{}{
		{int64(3)}, {int64(1)}, {int64(2)},
	}
	result, err := s.SortExternal(rows, os.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	for i, expected := range []int64{1, 2, 3} {
		if result[i][0] != expected {
			t.Fatalf("row %d: expected %d, got %v", i, expected, result[i][0])
		}
	}
}

func TestSortExternal_LargeDataset(t *testing.T) {
	s := NewOrderBySorter(
		[]parser.OrderByClause{ascClause("id")},
		[]string{"id", "name"},
	)
	// Set a low threshold to force external sort
	s.SetExternalSortThreshold(50)

	n := 200
	rows := make([][]interface{}, n)
	for i := range rows {
		rows[i] = []interface{}{float64(n - i), "row"}
	}

	tmpDir := t.TempDir()
	result, err := s.SortExternal(rows, tmpDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != n {
		t.Fatalf("expected %d rows, got %d", n, len(result))
	}
	// Verify sorted ascending
	for i := 1; i < len(result); i++ {
		prev := result[i-1][0].(float64)
		curr := result[i][0].(float64)
		if prev > curr {
			t.Fatalf("not sorted at index %d: %v > %v", i, prev, curr)
		}
	}
}
