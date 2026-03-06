// Package compaction provides the hourly compaction engine for Arkilian V3.
package compaction

import (
	"context"
	"time"

	"github.com/arkilian/arkilian/internal/manifest"
	"github.com/arkilian/arkilian/internal/observability"
)

// HotColumnActionType represents the type of hot-column action.
type HotColumnActionType int

const (
	HotColumnActionNone HotColumnActionType = iota
	HotColumnActionCreateSortedRun
	HotColumnActionDropSortedRun
)

// HotColumnAction represents an action to take on a hot column.
type HotColumnAction struct {
	Type   HotColumnActionType
	Column string
}

// HotColumnPolicy evaluates query statistics and determines hot-column actions.
type HotColumnPolicy struct {
	stats           *observability.QueryStats
	catalog         manifest.Catalog
	createThreshold int64
	dropThreshold   int64
	checkInterval   time.Duration
	maxHotColumns   int
}

// NewHotColumnPolicy creates a new hot-column policy.
func NewHotColumnPolicy(stats *observability.QueryStats, cat manifest.Catalog, createThreshold, dropThreshold int64, checkInterval time.Duration, maxHotColumns int) *HotColumnPolicy {
	if createThreshold == 0 {
		createThreshold = 200 // default: 200 queries/hour
	}
	if dropThreshold == 0 {
		dropThreshold = 10 // default: 10 queries/hour
	}
	if checkInterval == 0 {
		checkInterval = 5 * time.Minute
	}
	if maxHotColumns == 0 {
		maxHotColumns = 10 // default: 10
	}

	return &HotColumnPolicy{
		stats:           stats,
		catalog:         cat,
		createThreshold: createThreshold,
		dropThreshold:   dropThreshold,
		checkInterval:   checkInterval,
		maxHotColumns:   maxHotColumns,
	}
}

// Evaluate evaluates query statistics and returns hot-column actions.
func (p *HotColumnPolicy) Evaluate(ctx context.Context, tableID uint64) ([]HotColumnAction, error) {
	// TODO: Implement hot-column evaluation
	// 1. Get query statistics for the table
	// 2. Check which columns exceed create_threshold
	// 3. Check which columns are below drop_threshold
	// 4. Return create/drop actions

	// For now, return empty actions
	return []HotColumnAction{}, nil
}

// GetCheckInterval returns the check interval.
func (p *HotColumnPolicy) GetCheckInterval() time.Duration {
	return p.checkInterval
}

// GetMaxHotColumns returns the max hot columns.
func (p *HotColumnPolicy) GetMaxHotColumns() int {
	return p.maxHotColumns
}

// LogDecision logs a hot-column decision with full context.
func (p *HotColumnPolicy) LogDecision(column string, frequency int64, threshold int64, action HotColumnActionType) {
	// TODO: Implement logging with full context
	// log.Printf("hot-column: column=%s, frequency=%d, threshold=%d, action=%v", column, frequency, threshold, action)
}

// IsHotColumn checks if a column is currently hot.
func (p *HotColumnPolicy) IsHotColumn(column string, frequency int64) bool {
	return frequency >= p.createThreshold
}

// ShouldDropColumn checks if a column should be dropped.
func (p *HotColumnPolicy) ShouldDropColumn(column string, frequency int64) bool {
	return frequency <= p.dropThreshold
}
