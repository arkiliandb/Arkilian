// Package schema provides JSON column materialization for frequently queried JSON paths.
package schema

import (
	"log"
	"strings"
	"sync"
	"time"

	"github.com/arkilian/arkilian/internal/observability"
)

// MaterializedColumn represents a JSON path that should be materialized as a generated column.
type MaterializedColumn struct {
	JSONPath   string
	ColumnName string
	SQLiteType string
}

// MaterializerMetrics holds materializer statistics.
type MaterializerMetrics struct {
	Calls           int64
	ColumnsReturned int64
	CacheHits       int64
}

// Materializer determines which JSON paths should be materialized based on query statistics.
type Materializer struct {
	stats       *observability.QueryStats
	threshold   int64
	maxColumns  int
	metrics     MaterializerMetrics
	mu          sync.RWMutex
	cache       []MaterializedColumn
	cacheUntil  time.Time
	cacheTTL    time.Duration
}

// NewMaterializer creates a new materializer.
// threshold: minimum query frequency (queries/hour) to trigger materialization.
// maxColumns: maximum number of columns to materialize (0 = unlimited).
func NewMaterializer(stats *observability.QueryStats, threshold int64, maxColumns int) *Materializer {
	if threshold <= 0 {
		threshold = 50 // Default threshold from requirements
	}
	if maxColumns <= 0 {
		maxColumns = 20 // Reasonable default
	}

	return &Materializer{
		stats:      stats,
		threshold:  threshold,
		maxColumns: maxColumns,
		cacheTTL:   5 * time.Minute, // Cache for 5 minutes
	}
}

// Close cleans up resources (placeholder for future use).
func (m *Materializer) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.cache = nil
}

// Metrics returns current materializer metrics.
func (m *Materializer) Metrics() MaterializerMetrics {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.metrics
}

// GetMaterializedColumns returns columns that should be materialized based on query statistics.
// Results are cached for cacheTTL to avoid frequent stats queries.
func (m *Materializer) GetMaterializedColumns(collection string) []MaterializedColumn {
	m.mu.Lock()
	m.metrics.Calls++

	// Check cache
	if m.cache != nil && time.Now().Before(m.cacheUntil) {
		m.metrics.CacheHits++
		result := m.cache
		m.mu.Unlock()
		return result
	}
	m.mu.Unlock()

	// Get columns from stats
	var columns []MaterializedColumn
	if m.stats != nil {
		topPaths := m.stats.GetTopJSONPaths(m.maxColumns * 2) // Get extra to account for filtering

		for _, pathStats := range topPaths {
			if int64(len(columns)) >= int64(m.maxColumns) {
				break
			}
			if pathStats.Frequency >= m.threshold {
				columnName := jsonPathToColumnName(pathStats.Column)
				sqliteType := inferSQLiteType(pathStats.Column)
				columns = append(columns, MaterializedColumn{
					JSONPath:   pathStats.Column,
					ColumnName: columnName,
					SQLiteType: sqliteType,
				})
			}
		}
	}

	m.mu.Lock()
	m.cache = columns
	m.cacheUntil = time.Now().Add(m.cacheTTL)
	m.metrics.ColumnsReturned += int64(len(columns))
	m.mu.Unlock()

	log.Printf("schema: materialized %d columns (threshold=%d)", len(columns), m.threshold)

	return columns
}

// InvalidateCache clears the cached result.
func (m *Materializer) InvalidateCache() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.cache = nil
}

// SetThreshold updates the materialization threshold.
func (m *Materializer) SetThreshold(threshold int64) {
	if threshold <= 0 {
		return
	}
	m.mu.Lock()
	m.threshold = threshold
	m.cache = nil // Invalidate cache on threshold change
	m.mu.Unlock()
}

// Threshold returns the current materialization threshold.
func (m *Materializer) Threshold() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.threshold
}

// jsonPathToColumnName converts a JSON path to a column name.
// "$.user_agent" -> "payload_user_agent"
// "$.device.id" -> "payload_device_id"
func jsonPathToColumnName(jsonPath string) string {
	// Remove leading "$." prefix
	result := strings.TrimPrefix(jsonPath, "$.")
	// Replace remaining "." with "_"
	result = strings.ReplaceAll(result, ".", "_")
	// Add "payload_" prefix
	return "payload_" + result
}

// inferSQLiteType attempts to infer the SQLite type based on the JSON path.
// For now, defaults to TEXT. In a more sophisticated implementation,
// this could sample actual values to determine if they're numeric.
func inferSQLiteType(jsonPath string) string {
	// Remove leading "$." prefix for analysis
	path := strings.TrimPrefix(jsonPath, "$.")
	lowerPath := strings.ToLower(path)

	// Heuristic: paths ending with common numeric field names suggest INTEGER
	if strings.HasSuffix(lowerPath, "_id") ||
		strings.HasSuffix(lowerPath, "_count") ||
		strings.HasSuffix(lowerPath, "_timestamp") ||
		strings.HasSuffix(lowerPath, "_time") ||
		strings.HasSuffix(lowerPath, "_year") ||
		strings.HasSuffix(lowerPath, "_month") ||
		strings.HasSuffix(lowerPath, "_day") ||
		strings.HasSuffix(lowerPath, "_hour") ||
		strings.HasSuffix(lowerPath, "_at") {
		return "INTEGER"
	}

	// Paths suggesting real numbers
	if strings.HasSuffix(lowerPath, "_lat") ||
		strings.HasSuffix(lowerPath, "_lon") ||
		strings.HasSuffix(lowerPath, "_latency") ||
		strings.HasSuffix(lowerPath, "_duration") ||
		strings.HasSuffix(lowerPath, "_price") ||
		strings.HasSuffix(lowerPath, "_rate") ||
		strings.HasSuffix(lowerPath, "_ratio") ||
		strings.HasSuffix(lowerPath, "_percent") {
		return "REAL"
	}

	// Default to TEXT for strings
	return "TEXT"
}

// ValidateColumnName checks if a column name is valid for SQLite.
func ValidateColumnName(name string) bool {
	if len(name) == 0 || len(name) > 100 {
		return false
	}
	// First character must be a letter or underscore
	first := name[0]
	if (first < 'a' || first > 'z') && (first < 'A' || first > 'Z') && first != '_' {
		return false
	}
	// Subsequent characters can be letters, digits, or underscores
	for i := 1; i < len(name); i++ {
		c := name[i]
		if (c < 'a' || c > 'z') && (c < 'A' || c > 'Z') && (c < '0' || c > '9') && c != '_' {
			return false
		}
	}
	return true
}