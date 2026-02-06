package partition

import (
	"github.com/arkilian/arkilian/pkg/types"
)

// StatsTracker tracks min/max statistics for indexed columns during partition build.
type StatsTracker struct {
	rowCount int64

	// Min/max for user_id (INTEGER)
	minUserID *int64
	maxUserID *int64

	// Min/max for event_time (INTEGER)
	minEventTime *int64
	maxEventTime *int64

	// Min/max for tenant_id (TEXT)
	minTenantID *string
	maxTenantID *string
}

// NewStatsTracker creates a new statistics tracker.
func NewStatsTracker() *StatsTracker {
	return &StatsTracker{}
}

// Update updates statistics with a new row.
func (s *StatsTracker) Update(row types.Row) {
	s.rowCount++

	// Update user_id stats
	if s.minUserID == nil || row.UserID < *s.minUserID {
		userID := row.UserID
		s.minUserID = &userID
	}
	if s.maxUserID == nil || row.UserID > *s.maxUserID {
		userID := row.UserID
		s.maxUserID = &userID
	}

	// Update event_time stats
	if s.minEventTime == nil || row.EventTime < *s.minEventTime {
		eventTime := row.EventTime
		s.minEventTime = &eventTime
	}
	if s.maxEventTime == nil || row.EventTime > *s.maxEventTime {
		eventTime := row.EventTime
		s.maxEventTime = &eventTime
	}

	// Update tenant_id stats (lexicographic comparison)
	if s.minTenantID == nil || row.TenantID < *s.minTenantID {
		tenantID := row.TenantID
		s.minTenantID = &tenantID
	}
	if s.maxTenantID == nil || row.TenantID > *s.maxTenantID {
		tenantID := row.TenantID
		s.maxTenantID = &tenantID
	}
}

// GetMinMaxStats returns the computed min/max statistics.
func (s *StatsTracker) GetMinMaxStats() map[string]MinMax {
	stats := make(map[string]MinMax)

	if s.minUserID != nil && s.maxUserID != nil {
		stats["user_id"] = MinMax{Min: *s.minUserID, Max: *s.maxUserID}
	}

	if s.minEventTime != nil && s.maxEventTime != nil {
		stats["event_time"] = MinMax{Min: *s.minEventTime, Max: *s.maxEventTime}
	}

	if s.minTenantID != nil && s.maxTenantID != nil {
		stats["tenant_id"] = MinMax{Min: *s.minTenantID, Max: *s.maxTenantID}
	}

	return stats
}

// RowCount returns the number of rows tracked.
func (s *StatsTracker) RowCount() int64 {
	return s.rowCount
}

// MinUserID returns the minimum user_id value.
func (s *StatsTracker) MinUserID() *int64 {
	return s.minUserID
}

// MaxUserID returns the maximum user_id value.
func (s *StatsTracker) MaxUserID() *int64 {
	return s.maxUserID
}

// MinEventTime returns the minimum event_time value.
func (s *StatsTracker) MinEventTime() *int64 {
	return s.minEventTime
}

// MaxEventTime returns the maximum event_time value.
func (s *StatsTracker) MaxEventTime() *int64 {
	return s.maxEventTime
}

// MinTenantID returns the minimum tenant_id value.
func (s *StatsTracker) MinTenantID() *string {
	return s.minTenantID
}

// MaxTenantID returns the maximum tenant_id value.
func (s *StatsTracker) MaxTenantID() *string {
	return s.maxTenantID
}
