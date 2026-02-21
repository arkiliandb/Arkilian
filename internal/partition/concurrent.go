// Package partition provides functionality for creating and managing SQLite micro-partitions.
package partition

import (
	"context"
	"fmt"
	"sync"

	"github.com/arkilian/arkilian/pkg/types"
)

// ConcurrentWriter provides concurrent write handling for partition building.
// It ensures isolation between concurrent operations on different partition keys
// while serializing writes to the same partition key.
type ConcurrentWriter struct {
	builder   PartitionBuilder
	keyLocks  map[string]*sync.Mutex
	globalMu  sync.RWMutex
	inFlight  sync.WaitGroup
	closed    bool
	closedMu  sync.RWMutex
}

// NewConcurrentWriter creates a new concurrent writer wrapping the given builder.
func NewConcurrentWriter(builder PartitionBuilder) *ConcurrentWriter {
	return &ConcurrentWriter{
		builder:  builder,
		keyLocks: make(map[string]*sync.Mutex),
	}
}

// Build creates a partition from rows with concurrent write support.
// Writes to different partition keys can proceed in parallel.
// Writes to the same partition key are serialized.
func (cw *ConcurrentWriter) Build(ctx context.Context, rows []types.Row, key types.PartitionKey) (*PartitionInfo, error) {
	if err := cw.checkClosed(); err != nil {
		return nil, err
	}

	// Track in-flight operation
	cw.inFlight.Add(1)
	defer cw.inFlight.Done()

	// Check again after adding to in-flight (race condition protection)
	if err := cw.checkClosed(); err != nil {
		return nil, err
	}

	// Get or create lock for this partition key
	keyLock := cw.getKeyLock(key.Value)

	// Acquire lock for this partition key
	keyLock.Lock()
	defer keyLock.Unlock()

	// Check context cancellation
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	// Delegate to underlying builder
	return cw.builder.Build(ctx, rows, key)
}

// BuildWithSchema creates a partition with explicit schema and concurrent write support.
func (cw *ConcurrentWriter) BuildWithSchema(ctx context.Context, rows []types.Row, key types.PartitionKey, schema types.Schema) (*PartitionInfo, error) {
	if err := cw.checkClosed(); err != nil {
		return nil, err
	}

	// Track in-flight operation
	cw.inFlight.Add(1)
	defer cw.inFlight.Done()

	// Check again after adding to in-flight
	if err := cw.checkClosed(); err != nil {
		return nil, err
	}

	// Get or create lock for this partition key
	keyLock := cw.getKeyLock(key.Value)

	// Acquire lock for this partition key
	keyLock.Lock()
	defer keyLock.Unlock()

	// Check context cancellation
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	// Delegate to underlying builder
	return cw.builder.BuildWithSchema(ctx, rows, key, schema, nil)
}

// getKeyLock returns the lock for a partition key, creating one if needed.
func (cw *ConcurrentWriter) getKeyLock(keyValue string) *sync.Mutex {
	// Try read lock first for existing key
	cw.globalMu.RLock()
	if lock, exists := cw.keyLocks[keyValue]; exists {
		cw.globalMu.RUnlock()
		return lock
	}
	cw.globalMu.RUnlock()

	// Need to create new lock - acquire write lock
	cw.globalMu.Lock()
	defer cw.globalMu.Unlock()

	// Double-check after acquiring write lock
	if lock, exists := cw.keyLocks[keyValue]; exists {
		return lock
	}

	// Create new lock for this key
	lock := &sync.Mutex{}
	cw.keyLocks[keyValue] = lock
	return lock
}

// checkClosed returns an error if the writer is closed.
func (cw *ConcurrentWriter) checkClosed() error {
	cw.closedMu.RLock()
	defer cw.closedMu.RUnlock()
	if cw.closed {
		return ErrWriterClosed
	}
	return nil
}

// Close marks the writer as closed and waits for in-flight operations to complete.
// After Close returns, no new operations will be accepted.
func (cw *ConcurrentWriter) Close() error {
	cw.closedMu.Lock()
	cw.closed = true
	cw.closedMu.Unlock()

	// Wait for all in-flight operations to complete
	cw.inFlight.Wait()

	return nil
}

// InFlightCount returns the number of in-flight operations.
// This is useful for monitoring and graceful shutdown.
func (cw *ConcurrentWriter) InFlightCount() int {
	// Note: This is an approximation since WaitGroup doesn't expose count
	// For accurate tracking, we'd need a separate counter
	return 0 // Placeholder - actual implementation would need atomic counter
}

// WriteResult represents the result of a concurrent write operation.
type WriteResult struct {
	Info  *PartitionInfo
	Error error
}

// BatchWrite executes multiple write operations concurrently.
// Each operation targets a different partition key for maximum parallelism.
func (cw *ConcurrentWriter) BatchWrite(ctx context.Context, operations []WriteOperation) []WriteResult {
	results := make([]WriteResult, len(operations))
	var wg sync.WaitGroup

	for i, op := range operations {
		wg.Add(1)
		go func(idx int, operation WriteOperation) {
			defer wg.Done()

			var info *PartitionInfo
			var err error

			if operation.Schema != nil {
				info, err = cw.BuildWithSchema(ctx, operation.Rows, operation.Key, *operation.Schema)
			} else {
				info, err = cw.Build(ctx, operation.Rows, operation.Key)
			}

			results[idx] = WriteResult{Info: info, Error: err}
		}(i, op)
	}

	wg.Wait()
	return results
}

// WriteOperation represents a single write operation for batch processing.
type WriteOperation struct {
	Rows   []types.Row
	Key    types.PartitionKey
	Schema *types.Schema // Optional - if nil, uses default schema
}

// ErrWriterClosed is returned when attempting to write to a closed writer.
var ErrWriterClosed = fmt.Errorf("partition: writer is closed")

// ConcurrentWriterStats holds statistics about concurrent write operations.
type ConcurrentWriterStats struct {
	ActiveKeys     int   // Number of partition keys with active locks
	TotalWrites    int64 // Total number of write operations completed
	FailedWrites   int64 // Number of failed write operations
	InFlightWrites int64 // Current number of in-flight operations
}

// TrackedConcurrentWriter extends ConcurrentWriter with operation tracking.
type TrackedConcurrentWriter struct {
	*ConcurrentWriter
	mu             sync.Mutex
	totalWrites    int64
	failedWrites   int64
	inFlightWrites int64
}

// NewTrackedConcurrentWriter creates a new tracked concurrent writer.
func NewTrackedConcurrentWriter(builder PartitionBuilder) *TrackedConcurrentWriter {
	return &TrackedConcurrentWriter{
		ConcurrentWriter: NewConcurrentWriter(builder),
	}
}

// Build creates a partition with tracking.
func (tw *TrackedConcurrentWriter) Build(ctx context.Context, rows []types.Row, key types.PartitionKey) (*PartitionInfo, error) {
	tw.mu.Lock()
	tw.inFlightWrites++
	tw.mu.Unlock()

	info, err := tw.ConcurrentWriter.Build(ctx, rows, key)

	tw.mu.Lock()
	tw.inFlightWrites--
	tw.totalWrites++
	if err != nil {
		tw.failedWrites++
	}
	tw.mu.Unlock()

	return info, err
}

// BuildWithSchema creates a partition with explicit schema and tracking.
func (tw *TrackedConcurrentWriter) BuildWithSchema(ctx context.Context, rows []types.Row, key types.PartitionKey, schema types.Schema) (*PartitionInfo, error) {
	tw.mu.Lock()
	tw.inFlightWrites++
	tw.mu.Unlock()

	info, err := tw.ConcurrentWriter.BuildWithSchema(ctx, rows, key, schema)

	tw.mu.Lock()
	tw.inFlightWrites--
	tw.totalWrites++
	if err != nil {
		tw.failedWrites++
	}
	tw.mu.Unlock()

	return info, err
}

// Stats returns current statistics.
func (tw *TrackedConcurrentWriter) Stats() ConcurrentWriterStats {
	tw.mu.Lock()
	defer tw.mu.Unlock()

	tw.ConcurrentWriter.globalMu.RLock()
	activeKeys := len(tw.ConcurrentWriter.keyLocks)
	tw.ConcurrentWriter.globalMu.RUnlock()

	return ConcurrentWriterStats{
		ActiveKeys:     activeKeys,
		TotalWrites:    tw.totalWrites,
		FailedWrites:   tw.failedWrites,
		InFlightWrites: tw.inFlightWrites,
	}
}
