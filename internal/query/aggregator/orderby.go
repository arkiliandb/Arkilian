package aggregator

import (
	"container/heap"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	_ "github.com/mattn/go-sqlite3"

	"github.com/arkilian/arkilian/internal/query/parser"
)

// DefaultExternalSortThreshold is the row count above which SortExternal is used.
const DefaultExternalSortThreshold = 100_000

// OrderBySorter sorts rows according to ORDER BY clauses.
// It supports multiple columns and ASC/DESC directions.
type OrderBySorter struct {
	clauses   []parser.OrderByClause
	columns   []string
	threshold int // row count threshold for external sort
}

// NewOrderBySorter creates a new sorter for the given ORDER BY clauses.
func NewOrderBySorter(clauses []parser.OrderByClause, columns []string) *OrderBySorter {
	return &OrderBySorter{
		clauses:   clauses,
		columns:   columns,
		threshold: DefaultExternalSortThreshold,
	}
}

// SetExternalSortThreshold sets the row count threshold above which
// SortExternal is used instead of in-memory sort.
func (s *OrderBySorter) SetExternalSortThreshold(n int) {
	if n > 0 {
		s.threshold = n
	}
}

// resolveIndices resolves ORDER BY column names to indices in the columns slice.
func (s *OrderBySorter) resolveIndices() ([]int, error) {
	colMap := make(map[string]int)
	for i, c := range s.columns {
		colMap[strings.ToLower(c)] = i
	}

	indices := make([]int, len(s.clauses))
	for i, clause := range s.clauses {
		colName := strings.ToLower(clause.Expr.String())
		idx, ok := colMap[colName]
		if !ok {
			return nil, fmt.Errorf("orderby: column %q not found in result columns", clause.Expr.String())
		}
		indices[i] = idx
	}
	return indices, nil
}

// Sort sorts the rows in place according to the ORDER BY clauses.
func (s *OrderBySorter) Sort(rows [][]interface{}) error {
	if len(s.clauses) == 0 || len(rows) <= 1 {
		return nil
	}

	indices, err := s.resolveIndices()
	if err != nil {
		return err
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

// SortExternal performs an external merge sort when the row count exceeds the
// in-memory threshold. It splits rows into sorted chunks, writes each chunk to
// a temporary SQLite table, then performs a k-way merge using a heap.
// If the row count is below the threshold, it falls back to in-memory sort.
func (s *OrderBySorter) SortExternal(rows [][]interface{}, tempDir string) ([][]interface{}, error) {
	if len(s.clauses) == 0 || len(rows) <= 1 {
		return rows, nil
	}

	// Fall back to in-memory sort for small datasets
	if len(rows) <= s.threshold {
		if err := s.Sort(rows); err != nil {
			return nil, err
		}
		return rows, nil
	}

	indices, err := s.resolveIndices()
	if err != nil {
		return nil, err
	}

	chunkSize := s.threshold
	if chunkSize <= 0 {
		chunkSize = DefaultExternalSortThreshold
	}

	// Split into sorted chunks and write each to a temp SQLite file
	var chunkDBs []*sql.DB
	var chunkPaths []string
	defer func() {
		for _, db := range chunkDBs {
			db.Close()
		}
		for _, p := range chunkPaths {
			os.Remove(p)
		}
	}()

	numCols := 0
	if len(rows) > 0 {
		numCols = len(rows[0])
	}

	for start := 0; start < len(rows); start += chunkSize {
		end := start + chunkSize
		if end > len(rows) {
			end = len(rows)
		}
		chunk := rows[start:end]

		// Sort this chunk in memory
		sort.SliceStable(chunk, func(i, j int) bool {
			for k, clause := range s.clauses {
				idx := indices[k]
				var a, b interface{}
				if idx < len(chunk[i]) {
					a = chunk[i][idx]
				}
				if idx < len(chunk[j]) {
					b = chunk[j][idx]
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

		// Write chunk to temp SQLite
		dbPath := filepath.Join(tempDir, fmt.Sprintf("sort_chunk_%d.sqlite", len(chunkDBs)))
		db, err := sql.Open("sqlite3", dbPath+"?_journal_mode=OFF&_synchronous=OFF")
		if err != nil {
			return nil, fmt.Errorf("orderby: failed to create temp db: %w", err)
		}

		// Create table with a single JSON column for simplicity
		if _, err := db.Exec("CREATE TABLE chunk (row_idx INTEGER PRIMARY KEY, data TEXT NOT NULL)"); err != nil {
			db.Close()
			os.Remove(dbPath)
			return nil, fmt.Errorf("orderby: failed to create chunk table: %w", err)
		}

		tx, err := db.Begin()
		if err != nil {
			db.Close()
			os.Remove(dbPath)
			return nil, fmt.Errorf("orderby: failed to begin tx: %w", err)
		}

		insertStmt, err := tx.Prepare("INSERT INTO chunk (row_idx, data) VALUES (?, ?)")
		if err != nil {
			tx.Rollback()
			db.Close()
			os.Remove(dbPath)
			return nil, fmt.Errorf("orderby: failed to prepare insert: %w", err)
		}

		for i, row := range chunk {
			data, err := json.Marshal(row)
			if err != nil {
				insertStmt.Close()
				tx.Rollback()
				db.Close()
				os.Remove(dbPath)
				return nil, fmt.Errorf("orderby: failed to marshal row: %w", err)
			}
			if _, err := insertStmt.Exec(i, string(data)); err != nil {
				insertStmt.Close()
				tx.Rollback()
				db.Close()
				os.Remove(dbPath)
				return nil, fmt.Errorf("orderby: failed to insert row: %w", err)
			}
		}
		insertStmt.Close()

		if err := tx.Commit(); err != nil {
			db.Close()
			os.Remove(dbPath)
			return nil, fmt.Errorf("orderby: failed to commit chunk: %w", err)
		}

		chunkDBs = append(chunkDBs, db)
		chunkPaths = append(chunkPaths, dbPath)
	}

	// K-way merge using a heap
	return s.kWayMerge(chunkDBs, numCols, indices)
}

// chunkReader reads sorted rows from a chunk SQLite database.
type chunkReader struct {
	rows    *sql.Rows
	numCols int
}

func newChunkReader(db *sql.DB, numCols int) (*chunkReader, error) {
	rows, err := db.Query("SELECT data FROM chunk ORDER BY row_idx")
	if err != nil {
		return nil, err
	}
	return &chunkReader{rows: rows, numCols: numCols}, nil
}

func (r *chunkReader) Next() ([]interface{}, bool) {
	if !r.rows.Next() {
		return nil, false
	}
	var data string
	if err := r.rows.Scan(&data); err != nil {
		return nil, false
	}
	var row []interface{}
	if err := json.Unmarshal([]byte(data), &row); err != nil {
		return nil, false
	}
	return row, true
}

func (r *chunkReader) Close() {
	r.rows.Close()
}

// mergeHeapItem is an item in the k-way merge heap.
type mergeHeapItem struct {
	row      []interface{}
	chunkIdx int
}

// mergeHeap implements heap.Interface for k-way merge.
type mergeHeap struct {
	items   []mergeHeapItem
	clauses []parser.OrderByClause
	indices []int
}

func (h *mergeHeap) Len() int { return len(h.items) }

func (h *mergeHeap) Less(i, j int) bool {
	for k, clause := range h.clauses {
		idx := h.indices[k]
		var a, b interface{}
		if idx < len(h.items[i].row) {
			a = h.items[i].row[idx]
		}
		if idx < len(h.items[j].row) {
			b = h.items[j].row[idx]
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
}

func (h *mergeHeap) Swap(i, j int) { h.items[i], h.items[j] = h.items[j], h.items[i] }

func (h *mergeHeap) Push(x interface{}) {
	h.items = append(h.items, x.(mergeHeapItem))
}

func (h *mergeHeap) Pop() interface{} {
	old := h.items
	n := len(old)
	item := old[n-1]
	h.items = old[:n-1]
	return item
}

// kWayMerge performs a k-way merge of sorted chunks using a min-heap.
func (s *OrderBySorter) kWayMerge(chunkDBs []*sql.DB, numCols int, colIndices []int) ([][]interface{}, error) {
	readers := make([]*chunkReader, len(chunkDBs))
	for i, db := range chunkDBs {
		r, err := newChunkReader(db, numCols)
		if err != nil {
			// Close already-opened readers
			for j := 0; j < i; j++ {
				readers[j].Close()
			}
			return nil, fmt.Errorf("orderby: failed to open chunk reader: %w", err)
		}
		readers[i] = r
	}
	defer func() {
		for _, r := range readers {
			r.Close()
		}
	}()

	h := &mergeHeap{
		clauses: s.clauses,
		indices: colIndices,
	}

	// Seed the heap with the first row from each chunk
	for i, r := range readers {
		if row, ok := r.Next(); ok {
			heap.Push(h, mergeHeapItem{row: row, chunkIdx: i})
		}
	}
	heap.Init(h)

	var result [][]interface{}
	for h.Len() > 0 {
		item := heap.Pop(h).(mergeHeapItem)
		result = append(result, item.row)

		// Get next row from the same chunk
		if row, ok := readers[item.chunkIdx].Next(); ok {
			heap.Push(h, mergeHeapItem{row: row, chunkIdx: item.chunkIdx})
		}
	}

	return result, nil
}

// TopN returns the top n rows according to the ORDER BY clauses without
// performing a full sort. It uses a bounded heap of size n, reducing memory
// from O(N) to O(n) where n = LIMIT + OFFSET. This is ideal for queries
// like "SELECT ... ORDER BY x LIMIT 100" over millions of rows.
func (s *OrderBySorter) TopN(rows [][]interface{}, n int) ([][]interface{}, error) {
	if len(s.clauses) == 0 || n <= 0 {
		return [][]interface{}{}, nil
	}
	if len(rows) <= n {
		// Fewer rows than requested — just sort them all
		if err := s.Sort(rows); err != nil {
			return nil, err
		}
		return rows, nil
	}

	indices, err := s.resolveIndices()
	if err != nil {
		return nil, err
	}

	// Use a max-heap of size n. The heap keeps the "worst" element at the top
	// so we can efficiently evict it when a better candidate arrives.
	h := &topNHeap{
		clauses: s.clauses,
		indices: indices,
	}

	// Seed the heap with the first n rows
	for i := 0; i < n && i < len(rows); i++ {
		heap.Push(h, rows[i])
	}

	// Process remaining rows — push if better than current worst, then pop
	for i := n; i < len(rows); i++ {
		if h.Len() > 0 && s.lessThan(rows[i], h.items[0], indices) {
			h.items[0] = rows[i]
			heap.Fix(h, 0)
		}
	}

	// Extract results from heap and sort them
	result := make([][]interface{}, h.Len())
	for i := h.Len() - 1; i >= 0; i-- {
		result[i] = heap.Pop(h).([]interface{})
	}

	return result, nil
}

// lessThan returns true if row a should come before row b in the sort order.
func (s *OrderBySorter) lessThan(a, b []interface{}, indices []int) bool {
	for k, clause := range s.clauses {
		idx := indices[k]
		var va, vb interface{}
		if idx < len(a) {
			va = a[idx]
		}
		if idx < len(b) {
			vb = b[idx]
		}
		cmp := compareAggValues(va, vb)
		if cmp == 0 {
			continue
		}
		if clause.Desc {
			return cmp > 0
		}
		return cmp < 0
	}
	return false
}

// topNHeap is a max-heap used by TopN. It keeps the "worst" element at the
// root so it can be efficiently replaced when a better candidate arrives.
// "Worst" means the element that would sort last in the desired order.
type topNHeap struct {
	items   [][]interface{}
	clauses []parser.OrderByClause
	indices []int
}

func (h *topNHeap) Len() int { return len(h.items) }

// Less is inverted (max-heap): returns true if items[i] should sort AFTER items[j]
// in the desired order, so the "worst" element bubbles to the top.
func (h *topNHeap) Less(i, j int) bool {
	for k, clause := range h.clauses {
		idx := h.indices[k]
		var a, b interface{}
		if idx < len(h.items[i]) {
			a = h.items[i][idx]
		}
		if idx < len(h.items[j]) {
			b = h.items[j][idx]
		}
		cmp := compareAggValues(a, b)
		if cmp == 0 {
			continue
		}
		// Inverted: for ASC order, the larger value is "less" in the max-heap
		if clause.Desc {
			return cmp < 0
		}
		return cmp > 0
	}
	return false
}

func (h *topNHeap) Swap(i, j int) { h.items[i], h.items[j] = h.items[j], h.items[i] }

func (h *topNHeap) Push(x interface{}) {
	h.items = append(h.items, x.([]interface{}))
}

func (h *topNHeap) Pop() interface{} {
	old := h.items
	n := len(old)
	item := old[n-1]
	old[n-1] = nil // avoid memory leak
	h.items = old[:n-1]
	return item
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
