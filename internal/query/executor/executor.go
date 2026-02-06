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
	"github.com/arkilian/arkilian/internal/query/parser"
	"github.com/arkilian/arkilian/internal/query/planner"
	"github.com/arkilian/arkilian/internal/storage"
	"github.com/golang/snappy"
)

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
	planner     *planner.Planner
	storage     storage.ObjectStorage
	pool        *ConnectionPool
	downloadDir string
	concurrency int
	mu          sync.Mutex
}

// ExecutorConfig holds configuration for the executor.
type ExecutorConfig struct {
	// Concurrency is the number of parallel partition queries (default: 10)
	Concurrency int

	// DownloadDir is the directory for downloaded partitions
	DownloadDir string

	// PoolConfig is the connection pool configuration
	PoolConfig PoolConfig
}

// DefaultExecutorConfig returns the default executor configuration.
func DefaultExecutorConfig() ExecutorConfig {
	return ExecutorConfig{
		Concurrency: 10,
		DownloadDir: filepath.Join(os.TempDir(), "arkilian-partitions"),
		PoolConfig:  DefaultPoolConfig(),
	}
}

// NewParallelExecutor creates a new parallel query executor.
func NewParallelExecutor(
	planner *planner.Planner,
	storage storage.ObjectStorage,
	config ExecutorConfig,
) (*ParallelExecutor, error) {
	if config.Concurrency <= 0 {
		config.Concurrency = 10
	}

	// Create download directory
	if err := os.MkdirAll(config.DownloadDir, 0755); err != nil {
		return nil, fmt.Errorf("executor: failed to create download directory: %w", err)
	}

	return &ParallelExecutor{
		planner:     planner,
		storage:     storage,
		pool:        NewConnectionPool(config.PoolConfig),
		downloadDir: config.DownloadDir,
		concurrency: config.Concurrency,
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

// ExecutePlan executes a pre-planned query.
func (e *ParallelExecutor) ExecutePlan(ctx context.Context, plan *planner.QueryPlan) (*QueryResult, error) {
	startTime := time.Now()

	if len(plan.Partitions) == 0 {
		// No partitions to scan, return empty result
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

	// Execute queries in parallel across partitions
	partialResults := e.executeParallel(ctx, plan)

	// Collect results
	var allRows [][]interface{}
	var columns []string
	var totalRowsScanned int64
	var downloadTimeMs, queryTimeMs int64

	for _, pr := range partialResults {
		if pr.Error != nil {
			// Log error but continue with other partitions
			continue
		}
		if columns == nil && len(pr.Columns) > 0 {
			columns = pr.Columns
		}
		allRows = append(allRows, pr.Rows...)
		totalRowsScanned += pr.RowCount
	}

	// If no columns were found, extract from statement
	if columns == nil {
		columns = e.extractColumnNames(plan.Statement)
	}

	// Apply LIMIT if specified
	if plan.Statement.Limit != nil {
		limit := int(*plan.Statement.Limit)
		if len(allRows) > limit {
			allRows = allRows[:limit]
		}
	}

	return &QueryResult{
		Columns: columns,
		Rows:    allRows,
		Stats: ExecutionStats{
			PartitionsScanned: len(plan.Partitions),
			PartitionsPruned:  plan.PruningStats.PrunedCount,
			RowsScanned:       totalRowsScanned,
			ExecutionTimeMs:   time.Since(startTime).Milliseconds(),
			DownloadTimeMs:    downloadTimeMs,
			QueryTimeMs:       queryTimeMs,
		},
	}, nil
}

// executeParallel executes queries across partitions in parallel.
func (e *ParallelExecutor) executeParallel(ctx context.Context, plan *planner.QueryPlan) []*PartialResult {
	results := make([]*PartialResult, len(plan.Partitions))

	// Create a semaphore to limit concurrency
	sem := make(chan struct{}, e.concurrency)
	var wg sync.WaitGroup

	for i, partition := range plan.Partitions {
		wg.Add(1)
		go func(idx int, part *manifest.PartitionRecord) {
			defer wg.Done()

			// Acquire semaphore
			select {
			case sem <- struct{}{}:
				defer func() { <-sem }()
			case <-ctx.Done():
				results[idx] = &PartialResult{
					PartitionID: part.PartitionID,
					Error:       ctx.Err(),
				}
				return
			}

			// Execute query on this partition
			result := e.executeOnPartition(ctx, plan, part)
			results[idx] = result
		}(i, partition)
	}

	wg.Wait()
	return results
}

// executeOnPartition executes a query on a single partition.
func (e *ParallelExecutor) executeOnPartition(
	ctx context.Context,
	plan *planner.QueryPlan,
	partition *manifest.PartitionRecord,
) *PartialResult {
	result := &PartialResult{
		PartitionID: partition.PartitionID,
	}

	// Download partition if needed
	localPath, err := e.ensurePartitionDownloaded(ctx, partition)
	if err != nil {
		result.Error = fmt.Errorf("failed to download partition: %w", err)
		return result
	}

	// Get connection from pool
	db, err := e.pool.Get(ctx, localPath)
	if err != nil {
		result.Error = fmt.Errorf("failed to get connection: %w", err)
		return result
	}
	defer e.pool.Release(localPath)

	// Build and execute SQL query
	sqlQuery := e.buildPartitionQuery(plan.Statement)
	rows, err := db.QueryContext(ctx, sqlQuery)
	if err != nil {
		result.Error = fmt.Errorf("failed to execute query: %w", err)
		return result
	}
	defer rows.Close()

	// Get column names
	columns, err := rows.Columns()
	if err != nil {
		result.Error = fmt.Errorf("failed to get columns: %w", err)
		return result
	}
	result.Columns = columns

	// Scan rows
	for rows.Next() {
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			result.Error = fmt.Errorf("failed to scan row: %w", err)
			return result
		}

		// Process values (decompress payload if needed)
		processedValues := e.processRowValues(columns, values)
		result.Rows = append(result.Rows, processedValues)
		result.RowCount++
	}

	if err := rows.Err(); err != nil {
		result.Error = fmt.Errorf("error iterating rows: %w", err)
	}

	return result
}

// ensurePartitionDownloaded downloads a partition if not already present locally.
func (e *ParallelExecutor) ensurePartitionDownloaded(
	ctx context.Context,
	partition *manifest.PartitionRecord,
) (string, error) {
	localPath := filepath.Join(e.downloadDir, filepath.Base(partition.ObjectPath))

	// Check if already downloaded
	if _, err := os.Stat(localPath); err == nil {
		return localPath, nil
	}

	// Download from storage
	if err := e.storage.Download(ctx, partition.ObjectPath, localPath); err != nil {
		return "", err
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

	for rows.Next() {
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			result.Error = err
			return result
		}

		result.Rows = append(result.Rows, values)
		result.RowCount++
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
