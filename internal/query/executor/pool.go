// Package executor provides query execution across partitions.
package executor

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// ConnectionPool manages SQLite connections to downloaded partitions.
// It supports concurrent access with configurable connection limits.
type ConnectionPool struct {
	mu sync.RWMutex

	// connections maps partition paths to their connection entries
	connections map[string]*connectionEntry

	// maxConnections is the maximum number of connections per partition
	maxConnections int

	// maxTotalConnections is the maximum total connections across all partitions
	maxTotalConnections int

	// totalConnections tracks the current total connection count
	totalConnections int

	// idleTimeout is how long a connection can be idle before being closed
	idleTimeout time.Duration

	// closed indicates if the pool has been closed
	closed bool
}

// connectionEntry holds a connection and its metadata.
type connectionEntry struct {
	db         *sql.DB
	path       string
	refCount   int
	lastUsed   time.Time
	createTime time.Time
}

// PoolConfig holds configuration for the connection pool.
type PoolConfig struct {
	// MaxConnections is the maximum connections per partition (default: 10)
	MaxConnections int

	// MaxTotalConnections is the maximum total connections (default: 100)
	MaxTotalConnections int

	// IdleTimeout is how long a connection can be idle (default: 5 minutes)
	IdleTimeout time.Duration
}

// DefaultPoolConfig returns the default pool configuration.
func DefaultPoolConfig() PoolConfig {
	return PoolConfig{
		MaxConnections:      10,
		MaxTotalConnections: 100,
		IdleTimeout:         5 * time.Minute,
	}
}

// NewConnectionPool creates a new connection pool with the given configuration.
func NewConnectionPool(config PoolConfig) *ConnectionPool {
	if config.MaxConnections <= 0 {
		config.MaxConnections = 10
	}
	if config.MaxTotalConnections <= 0 {
		config.MaxTotalConnections = 100
	}
	if config.IdleTimeout <= 0 {
		config.IdleTimeout = 5 * time.Minute
	}

	pool := &ConnectionPool{
		connections:         make(map[string]*connectionEntry),
		maxConnections:      config.MaxConnections,
		maxTotalConnections: config.MaxTotalConnections,
		idleTimeout:         config.IdleTimeout,
	}

	// Start background cleanup goroutine
	go pool.cleanupLoop()

	return pool
}

// Get retrieves or creates a connection for the given partition path.
// The caller must call Release when done with the connection.
func (p *ConnectionPool) Get(ctx context.Context, partitionPath string) (*sql.DB, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return nil, fmt.Errorf("pool: connection pool is closed")
	}

	// Check if we already have a connection for this partition
	if entry, ok := p.connections[partitionPath]; ok {
		entry.refCount++
		entry.lastUsed = time.Now()
		return entry.db, nil
	}

	// Check if we've hit the total connection limit
	if p.totalConnections >= p.maxTotalConnections {
		// Try to evict an idle connection
		if !p.evictIdleConnection() {
			return nil, fmt.Errorf("pool: maximum connections reached (%d)", p.maxTotalConnections)
		}
	}

	// Create a new connection
	db, err := p.openConnection(ctx, partitionPath)
	if err != nil {
		return nil, err
	}

	entry := &connectionEntry{
		db:         db,
		path:       partitionPath,
		refCount:   1,
		lastUsed:   time.Now(),
		createTime: time.Now(),
	}

	p.connections[partitionPath] = entry
	p.totalConnections++

	return db, nil
}

// Release decrements the reference count for a connection.
func (p *ConnectionPool) Release(partitionPath string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if entry, ok := p.connections[partitionPath]; ok {
		entry.refCount--
		entry.lastUsed = time.Now()
	}
}

// openConnection opens a new SQLite connection with read-only mode.
func (p *ConnectionPool) openConnection(ctx context.Context, partitionPath string) (*sql.DB, error) {
	// Open in read-only mode with query-only pragma
	dsn := fmt.Sprintf("file:%s?mode=ro&_query_only=true", partitionPath)
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return nil, fmt.Errorf("pool: failed to open connection: %w", err)
	}

	// Configure connection pool settings for the SQLite connection
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(p.idleTimeout)

	// Verify connection is valid
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("pool: failed to ping connection: %w", err)
	}

	return db, nil
}

// evictIdleConnection evicts the oldest idle connection.
// Must be called with lock held. Returns true if a connection was evicted.
func (p *ConnectionPool) evictIdleConnection() bool {
	var oldestPath string
	var oldestTime time.Time

	for path, entry := range p.connections {
		if entry.refCount == 0 {
			if oldestPath == "" || entry.lastUsed.Before(oldestTime) {
				oldestPath = path
				oldestTime = entry.lastUsed
			}
		}
	}

	if oldestPath != "" {
		entry := p.connections[oldestPath]
		entry.db.Close()
		delete(p.connections, oldestPath)
		p.totalConnections--
		return true
	}

	return false
}

// cleanupLoop periodically cleans up idle connections.
func (p *ConnectionPool) cleanupLoop() {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		p.mu.Lock()
		if p.closed {
			p.mu.Unlock()
			return
		}
		p.cleanupIdleConnections()
		p.mu.Unlock()
	}
}

// cleanupIdleConnections closes connections that have been idle too long.
// Must be called with lock held.
func (p *ConnectionPool) cleanupIdleConnections() {
	now := time.Now()
	var toDelete []string

	for path, entry := range p.connections {
		if entry.refCount == 0 && now.Sub(entry.lastUsed) > p.idleTimeout {
			toDelete = append(toDelete, path)
		}
	}

	for _, path := range toDelete {
		entry := p.connections[path]
		entry.db.Close()
		delete(p.connections, path)
		p.totalConnections--
	}
}

// Close closes all connections in the pool.
func (p *ConnectionPool) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return nil
	}

	p.closed = true

	var lastErr error
	for path, entry := range p.connections {
		if err := entry.db.Close(); err != nil {
			lastErr = err
		}
		delete(p.connections, path)
	}

	p.totalConnections = 0
	return lastErr
}

// Stats returns statistics about the connection pool.
type PoolStats struct {
	TotalConnections  int
	ActiveConnections int
	IdleConnections   int
	PartitionCount    int
}

// Stats returns current pool statistics.
func (p *ConnectionPool) Stats() PoolStats {
	p.mu.RLock()
	defer p.mu.RUnlock()

	stats := PoolStats{
		TotalConnections: p.totalConnections,
		PartitionCount:   len(p.connections),
	}

	for _, entry := range p.connections {
		if entry.refCount > 0 {
			stats.ActiveConnections++
		} else {
			stats.IdleConnections++
		}
	}

	return stats
}

// HasConnection checks if a connection exists for the given partition.
func (p *ConnectionPool) HasConnection(partitionPath string) bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	_, ok := p.connections[partitionPath]
	return ok
}

// Evict removes a specific connection from the pool.
func (p *ConnectionPool) Evict(partitionPath string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	entry, ok := p.connections[partitionPath]
	if !ok {
		return nil
	}

	if entry.refCount > 0 {
		return fmt.Errorf("pool: cannot evict connection with active references")
	}

	if err := entry.db.Close(); err != nil {
		return err
	}

	delete(p.connections, partitionPath)
	p.totalConnections--
	return nil
}
