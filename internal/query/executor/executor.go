package executor

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/arkilian/arkilian/internal/manifest"
	"github.com/arkilian/arkilian/internal/query/aggregator"
	"github.com/arkilian/arkilian/internal/query/parser"
	"github.com/arkilian/arkilian/internal/query/planner"
	"github.com/arkilian/arkilian/internal/storage"
	"github.com/golang/snappy"
)

// snappyDecodeBufPool provides reusable destination buffers for Snappy decoding.
var snappyDecodeBufPool = sync.Pool{
	New: func() interface{} {
		b := make([]byte, 0, 4096)
		return &b
	},
}

// rowSlicePool provides reusable []interface{} slices for row processing.
// Each goroutine gets its own slice from the pool, copies values into it,
// then sends it on the channel. The receiver owns the slice after that.
// Pool entries are keyed by capacity to avoid mismatched lengths.
var rowSlicePool = sync.Pool{
	New: func() interface{} {
		s := make([]interface{}, 0, 16)
		return &s
	},
}

// getRowSlice returns a []interface{} of the given length from the pool.
func getRowSlice(n int) []interface{} {
	sp := rowSlicePool.Get().(*[]interface{})
	s := *sp
	if cap(s) >= n {
		s = s[:n]
	} else {
		s = make([]interface{}, n)
	}
	return s
}

// QueryExecutor executes queries across partitions in parallel.
type QueryExecutor interface {
	// Execute runs a query and returns results
	Execute(ctx context.Context, stmt *parser.SelectStatement) (*QueryResult, error)

	// ExecutePlan executes a pre-planned query
	ExecutePlan(ctx context.Context, plan *planner.QueryPlan) (*QueryResult, error)

	// Close releases all resources
	Close() error
}

// QueryResult holds query execution results.
type QueryResult struct {
	Columns []string
	Rows    [][]interface{}
	Stats   ExecutionStats
}

// ExecutionStats contains query execution metrics.
type ExecutionStats struct {
	PartitionsScanned   int
	PartitionsPruned    int
	RowsScanned         int64
	ExecutionTimeMs     int64
	Phase1PruningTimeMs int64
	Phase2PruningTimeMs int64
	DownloadTimeMs      int64
	QueryTimeMs         int64
}

// PartialResult holds results from a single partition.
type PartialResult struct {
	PartitionID string
	Columns     []string
	Rows        [][]interface{}
	RowCount    int64
	Error       error
}

// ParallelExecutor implements QueryExecutor with parallel partition execution.
type ParallelExecutor struct {
	planner        *planner.Planner
	storage        storage.ObjectStorage
	pool           *ConnectionPool
	downloadDir    string
	concurrency    int
	downloadCache  *DownloadCache
	maxMemoryBytes int64
	mu             sync.Mutex
}

// ExecutorConfig holds configuration for the executor.
type ExecutorConfig struct {
	// Concurrency is the number of parallel partition queries (default: 10)
	Concurrency int

	// DownloadDir is the directory for downloaded partitions
	DownloadDir string

	// PoolConfig is the connection pool configuration
	PoolConfig PoolConfig

	// MaxCacheBytes is the maximum total size of cached partition files (default: 10GB).
	// Set to 0 to disable caching.
	MaxCacheBytes int64

	// MaxMemoryBytes is the maximum memory for result collection before spilling to disk (default: 256MB).
	MaxMemoryBytes int64
}

// DefaultExecutorConfig returns the default executor configuration.
// DefaultExecutorConfig returns the default executor configuration.
func DefaultExecutorConfig() ExecutorConfig {
	return ExecutorConfig{
		Concurrency:    32,
		DownloadDir:    filepath.Join(os.TempDir(), "arkilian-partitions"),
		PoolConfig:     DefaultPoolConfig(),
		MaxCacheBytes:  20 * 1024 * 1024 * 1024, // 20 GB
		MaxMemoryBytes: 512 * 1024 * 1024,        // 512 MB
	}
}

// NewParallelExecutor creates a new parallel query executor.
// NewParallelExecutor creates a new parallel query executor.
func NewParallelExecutor(
	planner *planner.Planner,
	storage storage.ObjectStorage,
	config ExecutorConfig,
) (*ParallelExecutor, error) {
	if config.Concurrency <= 0 {
		config.Concurrency = 10
	}
	if config.MaxMemoryBytes <= 0 {
		config.MaxMemoryBytes = 256 * 1024 * 1024
	}

	// Create download directory
	if err := os.MkdirAll(config.DownloadDir, 0755); err != nil {
		return nil, fmt.Errorf("executor: failed to create download directory: %w", err)
	}

	var cache *DownloadCache
	if config.MaxCacheBytes > 0 {
		cache = NewDownloadCache(config.MaxCacheBytes)
	}

	return &ParallelExecutor{
		planner:        planner,
		storage:        storage,
		pool:           NewConnectionPool(config.PoolConfig),
		downloadDir:    config.DownloadDir,
		concurrency:    config.Concurrency,
		downloadCache:  cache,
		maxMemoryBytes: config.MaxMemoryBytes,
	}, nil
}

// Execute runs a query and returns results.
func (e *ParallelExecutor) Execute(ctx context.Context, stmt *parser.SelectStatement) (*QueryResult, error) {
	startTime := time.Now()

	// Generate query plan
	plan, err := e.planner.Plan(ctx, stmt)
	if err != nil {
		return nil, fmt.Errorf("executor: planning failed: %w", err)
	}

	result, err := e.ExecutePlan(ctx, plan)
	if err != nil {
		return nil, err
	}

	result.Stats.ExecutionTimeMs = time.Since(startTime).Milliseconds()
	return result, nil
}

// streamedRow carries a single row plus its column names from a partition goroutine.
type streamedRow struct {
	Columns []string
	Row     []interface{}
}

// ExecutePlan executes a pre-planned query using streaming row iteration.
// Partition goroutines send rows to a shared channel instead of collecting into slices,
// and the merger consumes from the channel with early LIMIT termination.
func (e *ParallelExecutor) ExecutePlan(ctx context.Context, plan *planner.QueryPlan) (*QueryResult, error) {
	startTime := time.Now()

	if len(plan.Partitions) == 0 {
		columns := e.extractColumnNames(plan.Statement)
		return &QueryResult{
			Columns: columns,
			Rows:    [][]interface{}{},
			Stats: ExecutionStats{
				PartitionsScanned: 0,
				PartitionsPruned:  plan.PruningStats.PrunedCount,
			},
		}, nil
	}

	// Create a channel for streaming rows from partition goroutines.
	rowChan := make(chan streamedRow, e.concurrency*64)
	// done channel signals early termination (e.g. LIMIT reached).
	done := make(chan struct{})

	// Launch partition goroutines that stream rows into the channel.
	var wg sync.WaitGroup
	sem := make(chan struct{}, e.concurrency)
	var totalRowsScanned int64
	var rowCountMu sync.Mutex

	for _, partition := range plan.Partitions {
		wg.Add(1)
		go func(part *manifest.PartitionRecord) {
			defer wg.Done()

			select {
			case sem <- struct{}{}:
				defer func() { <-sem }()
			case <-ctx.Done():
				return
			case <-done:
				return
			}

			count := e.streamPartitionRows(ctx, plan, part, rowChan, done)
			rowCountMu.Lock()
			totalRowsScanned += count
			rowCountMu.Unlock()
		}(partition)
	}

	// Close rowChan once all goroutines finish.
	go func() {
		wg.Wait()
		close(rowChan)
	}()

	// Consume rows from the channel with memory-bounded collection.
	collector := NewStreamingCollector(
		e.extractColumnNames(plan.Statement),
		plan.Statement.OrderBy,
		plan.Statement.Limit,
		plan.Statement.Offset,
		e.maxMemoryBytes,
	)

	result, err := collector.CollectFromChannel(rowChan, done)
	if err != nil {
		return nil, fmt.Errorf("executor: merge failed: %w", err)
	}

	rowCountMu.Lock()
	scanned := totalRowsScanned
	rowCountMu.Unlock()

	result.Stats = ExecutionStats{
		PartitionsScanned: len(plan.Partitions),
		PartitionsPruned:  plan.PruningStats.PrunedCount,
		RowsScanned:       scanned,
		ExecutionTimeMs:   time.Since(startTime).Milliseconds(),
	}

	return result, nil
}

// streamPartitionRows downloads a partition, executes the query, and streams
// each row into rowChan. For aggregate queries without GROUP BY, partial
// aggregates are computed inline during the scan without materializing rows,
// and only the final aggregate row is sent. Returns the number of rows scanned.
func (e *ParallelExecutor) streamPartitionRows(
	ctx context.Context,
	plan *planner.QueryPlan,
	partition *manifest.PartitionRecord,
	rowChan chan<- streamedRow,
	done <-chan struct{},
) int64 {
	localPath, err := e.ensurePartitionDownloaded(ctx, partition)
	if err != nil {
		return 0
	}

	db, err := e.pool.Get(ctx, localPath)
	if err != nil {
		return 0
	}
	defer e.pool.Release(localPath)

	sqlQuery := e.buildPartitionQuery(plan.Statement)
	rows, err := db.QueryContext(ctx, sqlQuery)
	if err != nil {
		return 0
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return 0
	}

	// Detect if this is a simple aggregate query (aggregates without GROUP BY).
	// If so, compute partial aggregates inline without materializing rows.
	aggExprs := aggregator.ExtractAggregateExprs(plan.Statement)
	isInlineAggregate := len(aggExprs) > 0 && len(plan.Statement.GroupBy) == 0

	var aggSet *aggregator.PartialAggregateSet
	var aggColIndices []int
	if isInlineAggregate {
		aggColIndices = aggregator.ResolveAggregateColumnIndices(aggExprs, columns)
		aggSet = aggregator.NewPartialAggregateSet(aggExprs)
	}

	values := make([]interface{}, len(columns))
	valuePtrs := make([]interface{}, len(columns))
	for i := range values {
		valuePtrs[i] = &values[i]
	}

	decodeBufPtr := snappyDecodeBufPool.Get().(*[]byte)
	defer snappyDecodeBufPool.Put(decodeBufPtr)
	decodeBuf := *decodeBufPtr

	var count int64
	for rows.Next() {
		if err := rows.Scan(valuePtrs...); err != nil {
			return count
		}

		processed := getRowSlice(len(values))
		for i, col := range columns {
			val := values[i]
			if col == "payload" {
				if compressed, ok := val.([]byte); ok && len(compressed) > 0 {
					var decErr error
					decodeBuf, decErr = snappy.Decode(decodeBuf[:cap(decodeBuf)], compressed)
					if decErr == nil {
						var payload map[string]interface{}
						if json.Unmarshal(decodeBuf, &payload) == nil {
							val = payload
						}
					}
				}
			}
			processed[i] = val
		}

		count++

		if isInlineAggregate {
			// Accumulate into partial aggregates without materializing the row.
			for i, agg := range aggSet.Aggregates {
				idx := aggColIndices[i]
				if idx < 0 {
					agg.Accumulate(int64(1)) // COUNT(*)
				} else if idx < len(processed) {
					agg.Accumulate(processed[idx])
				}
			}
		} else {
			// Stream the row to the collector.
			select {
			case rowChan <- streamedRow{Columns: columns, Row: processed}:
			case <-done:
				return count
			case <-ctx.Done():
				return count
			}
		}

		for i := range values {
			values[i] = nil
		}
	}

	// For inline aggregates, send the single result row.
	if isInlineAggregate && aggSet != nil {
		resultRow := aggSet.Results()
		// Build column names from aggregate expressions.
		aggColumns := make([]string, len(aggExprs))
		for i, expr := range aggExprs {
			col := plan.Statement.Columns[i]
			if col.Alias != "" {
				aggColumns[i] = col.Alias
			} else {
				aggColumns[i] = expr.String()
			}
		}
		select {
		case rowChan <- streamedRow{Columns: aggColumns, Row: resultRow}:
		case <-done:
		case <-ctx.Done():
		}
	}

	return count
}

// ensurePartitionDownloaded downloads a partition if not already present locally.
func (e *ParallelExecutor) ensurePartitionDownloaded(
	ctx context.Context,
	partition *manifest.PartitionRecord,
) (string, error) {
	// Check LRU cache first
	if e.downloadCache != nil {
		if cached := e.downloadCache.Get(partition.ObjectPath); cached != "" {
			return cached, nil
		}
	}

	localPath := filepath.Join(e.downloadDir, filepath.Base(partition.ObjectPath))

	// Check if already on disk (e.g. from a previous run without cache)
	if info, err := os.Stat(localPath); err == nil && info.Size() > 0 {
		if e.downloadCache != nil {
			e.downloadCache.Put(partition.ObjectPath, localPath)
		}
		return localPath, nil
	}

	// Download from storage
	if err := e.storage.Download(ctx, partition.ObjectPath, localPath); err != nil {
		return "", err
	}

	// Record in cache
	if e.downloadCache != nil {
		e.downloadCache.Put(partition.ObjectPath, localPath)
	}

	return localPath, nil
}

// buildPartitionQuery builds the SQL query for a single partition.
func (e *ParallelExecutor) buildPartitionQuery(stmt *parser.SelectStatement) string {
	// For now, use the statement's String() method
	// In a full implementation, we might need to modify the query
	// to handle partition-specific optimizations
	return stmt.String()
}

// processRowValues processes row values, decompressing payload if needed.
func (e *ParallelExecutor) processRowValues(columns []string, values []interface{}) []interface{} {
	result := make([]interface{}, len(values))
	for i, col := range columns {
		val := values[i]

		// Decompress payload column
		if col == "payload" {
			if compressed, ok := val.([]byte); ok && len(compressed) > 0 {
				decompressed, err := snappy.Decode(nil, compressed)
				if err == nil {
					var payload map[string]interface{}
					if json.Unmarshal(decompressed, &payload) == nil {
						val = payload
					}
				}
			}
		}

		result[i] = val
	}
	return result
}

// extractColumnNames extracts column names from a SELECT statement.
func (e *ParallelExecutor) extractColumnNames(stmt *parser.SelectStatement) []string {
	var columns []string
	for _, col := range stmt.Columns {
		if col.Alias != "" {
			columns = append(columns, col.Alias)
		} else {
			columns = append(columns, col.Expr.String())
		}
	}
	return columns
}

// Close releases all resources.
func (e *ParallelExecutor) Close() error {
	if e.downloadCache != nil {
		e.downloadCache.Clear()
	}
	return e.pool.Close()
}

// ExecuteSQL is a convenience method that parses and executes SQL.
func (e *ParallelExecutor) ExecuteSQL(ctx context.Context, sql string) (*QueryResult, error) {
	stmt, err := parser.Parse(sql)
	if err != nil {
		return nil, fmt.Errorf("executor: parse error: %w", err)
	}

	selectStmt, ok := stmt.(*parser.SelectStatement)
	if !ok {
		return nil, fmt.Errorf("executor: only SELECT statements are supported")
	}

	return e.Execute(ctx, selectStmt)
}

// SimpleExecutor is a simpler executor for testing without storage.
type SimpleExecutor struct {
	pool        *ConnectionPool
	concurrency int
}

// NewSimpleExecutor creates a simple executor that works with local partition files.
func NewSimpleExecutor(config ExecutorConfig) *SimpleExecutor {
	if config.Concurrency <= 0 {
		config.Concurrency = 10
	}

	return &SimpleExecutor{
		pool:        NewConnectionPool(config.PoolConfig),
		concurrency: config.Concurrency,
	}
}

// ExecuteOnPartitions executes a query on the given local partition paths.
func (e *SimpleExecutor) ExecuteOnPartitions(
	ctx context.Context,
	stmt *parser.SelectStatement,
	partitionPaths []string,
) (*QueryResult, error) {
	startTime := time.Now()

	if len(partitionPaths) == 0 {
		columns := e.extractColumnNames(stmt)
		return &QueryResult{
			Columns: columns,
			Rows:    [][]interface{}{},
			Stats:   ExecutionStats{},
		}, nil
	}

	// Execute in parallel
	results := make([]*PartialResult, len(partitionPaths))
	sem := make(chan struct{}, e.concurrency)
	var wg sync.WaitGroup

	for i, path := range partitionPaths {
		wg.Add(1)
		go func(idx int, partPath string) {
			defer wg.Done()

			select {
			case sem <- struct{}{}:
				defer func() { <-sem }()
			case <-ctx.Done():
				results[idx] = &PartialResult{Error: ctx.Err()}
				return
			}

			results[idx] = e.executeOnPath(ctx, stmt, partPath)
		}(i, path)
	}

	wg.Wait()

	// Merge results
	var allRows [][]interface{}
	var columns []string
	var totalRowsScanned int64

	for _, pr := range results {
		if pr.Error != nil {
			continue
		}
		if columns == nil && len(pr.Columns) > 0 {
			columns = pr.Columns
		}
		allRows = append(allRows, pr.Rows...)
		totalRowsScanned += pr.RowCount
	}

	if columns == nil {
		columns = e.extractColumnNames(stmt)
	}

	// Apply LIMIT
	if stmt.Limit != nil {
		limit := int(*stmt.Limit)
		if len(allRows) > limit {
			allRows = allRows[:limit]
		}
	}

	return &QueryResult{
		Columns: columns,
		Rows:    allRows,
		Stats: ExecutionStats{
			PartitionsScanned: len(partitionPaths),
			RowsScanned:       totalRowsScanned,
			ExecutionTimeMs:   time.Since(startTime).Milliseconds(),
		},
	}, nil
}

// executeOnPath executes a query on a single partition file.
func (e *SimpleExecutor) executeOnPath(
	ctx context.Context,
	stmt *parser.SelectStatement,
	partitionPath string,
) *PartialResult {
	result := &PartialResult{
		PartitionID: filepath.Base(partitionPath),
	}

	db, err := e.pool.Get(ctx, partitionPath)
	if err != nil {
		result.Error = err
		return result
	}
	defer e.pool.Release(partitionPath)

	sqlQuery := stmt.String()
	rows, err := db.QueryContext(ctx, sqlQuery)
	if err != nil {
		result.Error = err
		return result
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		result.Error = err
		return result
	}
	result.Columns = columns

	// Pre-allocate scan buffers once outside the loop
	values := make([]interface{}, len(columns))
	valuePtrs := make([]interface{}, len(columns))
	for i := range values {
		valuePtrs[i] = &values[i]
	}

	for rows.Next() {
		if err := rows.Scan(valuePtrs...); err != nil {
			result.Error = err
			return result
		}

		// Copy values for this row (values slice is reused)
		rowCopy := make([]interface{}, len(values))
		copy(rowCopy, values)
		result.Rows = append(result.Rows, rowCopy)
		result.RowCount++

		// Reset scan targets for next iteration
		for i := range values {
			values[i] = nil
		}
	}

	if err := rows.Err(); err != nil {
		result.Error = err
	}

	return result
}

// extractColumnNames extracts column names from a SELECT statement.
func (e *SimpleExecutor) extractColumnNames(stmt *parser.SelectStatement) []string {
	var columns []string
	for _, col := range stmt.Columns {
		if col.Alias != "" {
			columns = append(columns, col.Alias)
		} else {
			columns = append(columns, col.Expr.String())
		}
	}
	return columns
}

// Close releases all resources.
func (e *SimpleExecutor) Close() error {
	return e.pool.Close()
}
