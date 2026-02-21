// Package schema provides JSON column materialization for frequently queried JSON paths.
package schema

import (
	"strings"

	"github.com/arkilian/arkilian/internal/observability"
)

// MaterializedColumn represents a JSON path that should be materialized as a generated column.
type MaterializedColumn struct {
	JSONPath   string
	ColumnName string
	SQLiteType string
}

// Materializer determines which JSON paths should be materialized based on query statistics.
type Materializer struct {
	stats     *observability.QueryStats
	threshold int64
}

// NewMaterializer creates a new materializer.
// threshold: minimum query frequency (queries/hour) to trigger materialization.
func NewMaterializer(stats *observability.QueryStats, threshold int64) *Materializer {
	return &Materializer{
		stats:     stats,
		threshold: threshold,
	}
}

// GetMaterializedColumns returns columns that should be materialized based on query statistics.
// It consults the query stats and returns columns above the threshold.
func (m *Materializer) GetMaterializedColumns(collection string) []MaterializedColumn {
	if m.stats == nil {
		return nil
	}

	// Get top JSON paths by frequency
	topPaths := m.stats.GetTopJSONPaths(100) // Get up to 100 paths

	var columns []MaterializedColumn
	for _, pathStats := range topPaths {
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

	return columns
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
	// Default to TEXT for now
	// TODO: Add logic to sample values and determine if they're numeric
	return "TEXT"
}