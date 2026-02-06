package manifest

import (
	"context"
	"fmt"
	"strings"
)

// Pruner provides partition pruning functionality using min/max statistics.
// This implements phase 1 of the 2-phase pruning strategy described in the design.
type Pruner struct {
	catalog *SQLiteCatalog
}

// NewPruner creates a new partition pruner.
func NewPruner(catalog *SQLiteCatalog) *Pruner {
	return &Pruner{catalog: catalog}
}

// PruneByTimeRange returns partitions that may contain data within the given time range.
// Partitions whose event_time range does not overlap with [minTime, maxTime] are excluded.
func (p *Pruner) PruneByTimeRange(ctx context.Context, minTime, maxTime int64) ([]*PartitionRecord, error) {
	query := `
		SELECT partition_id, partition_key, object_path,
			min_user_id, max_user_id,
			min_event_time, max_event_time,
			min_tenant_id, max_tenant_id,
			row_count, size_bytes, schema_version, created_at, compacted_into
		FROM partitions
		WHERE compacted_into IS NULL
			AND (min_event_time IS NULL OR min_event_time <= ?)
			AND (max_event_time IS NULL OR max_event_time >= ?)`

	return p.executeQuery(ctx, query, maxTime, minTime)
}

// PruneByUserID returns partitions that may contain data for the given user ID.
// Partitions whose user_id range does not include the target user are excluded.
func (p *Pruner) PruneByUserID(ctx context.Context, userID int64) ([]*PartitionRecord, error) {
	query := `
		SELECT partition_id, partition_key, object_path,
			min_user_id, max_user_id,
			min_event_time, max_event_time,
			min_tenant_id, max_tenant_id,
			row_count, size_bytes, schema_version, created_at, compacted_into
		FROM partitions
		WHERE compacted_into IS NULL
			AND (min_user_id IS NULL OR min_user_id <= ?)
			AND (max_user_id IS NULL OR max_user_id >= ?)`

	return p.executeQuery(ctx, query, userID, userID)
}

// PruneByUserIDRange returns partitions that may contain data within the given user ID range.
func (p *Pruner) PruneByUserIDRange(ctx context.Context, minUserID, maxUserID int64) ([]*PartitionRecord, error) {
	query := `
		SELECT partition_id, partition_key, object_path,
			min_user_id, max_user_id,
			min_event_time, max_event_time,
			min_tenant_id, max_tenant_id,
			row_count, size_bytes, schema_version, created_at, compacted_into
		FROM partitions
		WHERE compacted_into IS NULL
			AND (min_user_id IS NULL OR min_user_id <= ?)
			AND (max_user_id IS NULL OR max_user_id >= ?)`

	return p.executeQuery(ctx, query, maxUserID, minUserID)
}

// PruneByTenantID returns partitions that may contain data for the given tenant ID.
// Partitions whose tenant_id range does not include the target tenant are excluded.
func (p *Pruner) PruneByTenantID(ctx context.Context, tenantID string) ([]*PartitionRecord, error) {
	query := `
		SELECT partition_id, partition_key, object_path,
			min_user_id, max_user_id,
			min_event_time, max_event_time,
			min_tenant_id, max_tenant_id,
			row_count, size_bytes, schema_version, created_at, compacted_into
		FROM partitions
		WHERE compacted_into IS NULL
			AND (min_tenant_id IS NULL OR min_tenant_id <= ?)
			AND (max_tenant_id IS NULL OR max_tenant_id >= ?)`

	return p.executeQuery(ctx, query, tenantID, tenantID)
}

// PruneByTenantIDRange returns partitions that may contain data within the given tenant ID range.
func (p *Pruner) PruneByTenantIDRange(ctx context.Context, minTenantID, maxTenantID string) ([]*PartitionRecord, error) {
	query := `
		SELECT partition_id, partition_key, object_path,
			min_user_id, max_user_id,
			min_event_time, max_event_time,
			min_tenant_id, max_tenant_id,
			row_count, size_bytes, schema_version, created_at, compacted_into
		FROM partitions
		WHERE compacted_into IS NULL
			AND (min_tenant_id IS NULL OR min_tenant_id <= ?)
			AND (max_tenant_id IS NULL OR max_tenant_id >= ?)`

	return p.executeQuery(ctx, query, maxTenantID, minTenantID)
}

// PruneByPartitionKey returns all active partitions for a given partition key.
func (p *Pruner) PruneByPartitionKey(ctx context.Context, partitionKey string) ([]*PartitionRecord, error) {
	query := `
		SELECT partition_id, partition_key, object_path,
			min_user_id, max_user_id,
			min_event_time, max_event_time,
			min_tenant_id, max_tenant_id,
			row_count, size_bytes, schema_version, created_at, compacted_into
		FROM partitions
		WHERE compacted_into IS NULL
			AND partition_key = ?`

	return p.executeQuery(ctx, query, partitionKey)
}

// PruneResult contains the result of a pruning operation.
type PruneResult struct {
	Partitions     []*PartitionRecord
	TotalScanned   int
	TotalPruned    int
	PruningRatio   float64
}

// PruneWithPredicates performs pruning using multiple predicates and returns detailed results.
func (p *Pruner) PruneWithPredicates(ctx context.Context, predicates []Predicate) (*PruneResult, error) {
	// Get total partition count for statistics
	totalCount, err := p.catalog.GetPartitionCount(ctx)
	if err != nil {
		return nil, fmt.Errorf("pruning: failed to get partition count: %w", err)
	}

	// Find matching partitions
	partitions, err := p.catalog.FindPartitions(ctx, predicates)
	if err != nil {
		return nil, fmt.Errorf("pruning: failed to find partitions: %w", err)
	}

	matchedCount := len(partitions)
	prunedCount := int(totalCount) - matchedCount

	var pruningRatio float64
	if totalCount > 0 {
		pruningRatio = float64(prunedCount) / float64(totalCount)
	}

	return &PruneResult{
		Partitions:   partitions,
		TotalScanned: int(totalCount),
		TotalPruned:  prunedCount,
		PruningRatio: pruningRatio,
	}, nil
}

// PruneComplex performs complex pruning with multiple conditions combined.
type PruneCondition struct {
	TimeRange   *TimeRange
	UserIDRange *Int64Range
	TenantID    *string
	PartitionKey *string
}

// TimeRange represents a time range for pruning.
type TimeRange struct {
	Min int64
	Max int64
}

// Int64Range represents an int64 range for pruning.
type Int64Range struct {
	Min int64
	Max int64
}

// PruneComplex performs pruning with multiple conditions.
func (p *Pruner) PruneComplex(ctx context.Context, cond PruneCondition) ([]*PartitionRecord, error) {
	var conditions []string
	var args []interface{}

	// Base condition: exclude compacted partitions
	conditions = append(conditions, "compacted_into IS NULL")

	// Add time range condition
	if cond.TimeRange != nil {
		conditions = append(conditions, "(min_event_time IS NULL OR min_event_time <= ?)")
		args = append(args, cond.TimeRange.Max)
		conditions = append(conditions, "(max_event_time IS NULL OR max_event_time >= ?)")
		args = append(args, cond.TimeRange.Min)
	}

	// Add user ID range condition
	if cond.UserIDRange != nil {
		conditions = append(conditions, "(min_user_id IS NULL OR min_user_id <= ?)")
		args = append(args, cond.UserIDRange.Max)
		conditions = append(conditions, "(max_user_id IS NULL OR max_user_id >= ?)")
		args = append(args, cond.UserIDRange.Min)
	}

	// Add tenant ID condition
	if cond.TenantID != nil {
		conditions = append(conditions, "(min_tenant_id IS NULL OR min_tenant_id <= ?)")
		args = append(args, *cond.TenantID)
		conditions = append(conditions, "(max_tenant_id IS NULL OR max_tenant_id >= ?)")
		args = append(args, *cond.TenantID)
	}

	// Add partition key condition
	if cond.PartitionKey != nil {
		conditions = append(conditions, "partition_key = ?")
		args = append(args, *cond.PartitionKey)
	}

	query := fmt.Sprintf(`
		SELECT partition_id, partition_key, object_path,
			min_user_id, max_user_id,
			min_event_time, max_event_time,
			min_tenant_id, max_tenant_id,
			row_count, size_bytes, schema_version, created_at, compacted_into
		FROM partitions
		WHERE %s`, strings.Join(conditions, " AND "))

	return p.executeQuery(ctx, query, args...)
}

// GetAllActivePartitions returns all partitions that have not been compacted.
func (p *Pruner) GetAllActivePartitions(ctx context.Context) ([]*PartitionRecord, error) {
	query := `
		SELECT partition_id, partition_key, object_path,
			min_user_id, max_user_id,
			min_event_time, max_event_time,
			min_tenant_id, max_tenant_id,
			row_count, size_bytes, schema_version, created_at, compacted_into
		FROM partitions
		WHERE compacted_into IS NULL`

	return p.executeQuery(ctx, query)
}

// executeQuery executes a query and returns partition records.
func (p *Pruner) executeQuery(ctx context.Context, query string, args ...interface{}) ([]*PartitionRecord, error) {
	rows, err := p.catalog.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("pruning: failed to execute query: %w", err)
	}
	defer rows.Close()

	var records []*PartitionRecord
	for rows.Next() {
		record, err := p.catalog.scanPartitionRows(rows)
		if err != nil {
			return nil, err
		}
		records = append(records, record)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("pruning: error iterating results: %w", err)
	}

	return records, nil
}

// OverlapCheck provides utilities for checking range overlaps.
type OverlapCheck struct{}

// TimeRangeOverlaps checks if two time ranges overlap.
func (OverlapCheck) TimeRangeOverlaps(partMin, partMax, queryMin, queryMax int64) bool {
	return partMin <= queryMax && partMax >= queryMin
}

// Int64RangeOverlaps checks if two int64 ranges overlap.
func (OverlapCheck) Int64RangeOverlaps(partMin, partMax, queryMin, queryMax int64) bool {
	return partMin <= queryMax && partMax >= queryMin
}

// StringRangeOverlaps checks if two string ranges overlap (lexicographic).
func (OverlapCheck) StringRangeOverlaps(partMin, partMax, queryMin, queryMax string) bool {
	return partMin <= queryMax && partMax >= queryMin
}

// PointInRange checks if a point is within a range.
func (OverlapCheck) PointInRange(point, min, max int64) bool {
	return point >= min && point <= max
}

// StringPointInRange checks if a string point is within a range.
func (OverlapCheck) StringPointInRange(point, min, max string) bool {
	return point >= min && point <= max
}
