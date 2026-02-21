// Package index provides secondary index partitions for efficient point lookups on any column.
package index

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/arkilian/arkilian/internal/config"
	"github.com/arkilian/arkilian/internal/observability"
	"github.com/arkilian/arkilian/internal/storage"
)

// PartitionProvider is an interface for getting partition information for index building.
// This is defined in the index package to avoid import cycles with the manifest package.
type PartitionProvider interface {
	// GetPartitions returns all partitions for index building.
	GetPartitions(ctx context.Context) ([]*PartitionInfo, error)
}

// ActionType represents the type of index action to perform.
type ActionType string

const (
	ActionCreate ActionType = "CREATE"
	ActionDrop   ActionType = "DROP"
)

// IndexAction represents an action to create or drop an index.
type IndexAction struct {
	Type   ActionType
	Column string
}

// Policy manages automated index creation and deletion based on query statistics.
type Policy struct {
	stats           *observability.QueryStats
	builder         *Builder
	indexCatalog    IndexCatalog
	dataCatalog     PartitionProvider
	storage         storage.ObjectStorage // For deleting S3 objects on index drop
	createThreshold int64
	dropThreshold   int64
	checkInterval   time.Duration
	maxIndexes      int
	collection      string // For MVP, "events" is the only collection
	mu              sync.Mutex
}

// NewPolicy creates a new index policy manager.
func NewPolicy(
	stats *observability.QueryStats,
	builder *Builder,
	indexCatalog IndexCatalog,
	dataCatalog PartitionProvider,
	storage storage.ObjectStorage,
	cfg config.IndexConfig,
) *Policy {
	return &Policy{
		stats:           stats,
		builder:         builder,
		indexCatalog:    indexCatalog,
		dataCatalog:     dataCatalog,
		storage:         storage,
		createThreshold: cfg.CreateThreshold,
		dropThreshold:   cfg.DropThreshold,
		checkInterval:   cfg.CheckInterval,
		maxIndexes:      cfg.MaxIndexes,
		collection:      "events", // MVP: only "events" collection
	}
}

// Run starts the background policy evaluation loop.
// It runs until the context is cancelled.
func (p *Policy) Run(ctx context.Context) {
	if p.checkInterval <= 0 {
		p.checkInterval = 5 * time.Minute // Default to 5 minutes
	}

	ticker := time.NewTicker(p.checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			actions, err := p.evaluate(ctx)
			if err != nil {
				log.Printf("index policy: evaluate failed: %v", err)
				continue
			}

			for _, action := range actions {
				if err := p.executeAction(ctx, action); err != nil {
					log.Printf("index policy: failed to execute %s action for column %s: %v",
						action.Type, action.Column, err)
				}
			}
		}
	}
}

// evaluate determines which index actions should be taken based on query statistics.
func (p *Policy) evaluate(ctx context.Context) ([]IndexAction, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	var actions []IndexAction

	// Step 1: Get top predicates from query stats
	topPredicates := p.stats.GetTopPredicates(p.maxIndexes + 10) // Get extra for safety

	// Step 2: Get existing indexes from index catalog
	existingIndexes, err := p.indexCatalog.ListIndexes(ctx, p.collection)
	if err != nil {
		return nil, fmt.Errorf("failed to list existing indexes: %w", err)
	}

	// Build a set of existing indexed columns for O(1) lookup
	existingSet := make(map[string]bool)
	for _, col := range existingIndexes {
		existingSet[col] = true
	}

	// Step 3: Identify columns that need new indexes
	for _, stats := range topPredicates {
		if stats.Frequency >= p.createThreshold && !existingSet[stats.Column] {
			if len(existingIndexes) < p.maxIndexes {
				actions = append(actions, IndexAction{
					Type:   ActionCreate,
					Column: stats.Column,
				})
				existingIndexes = append(existingIndexes, stats.Column)
				existingSet[stats.Column] = true
			}
		}
	}

	// Step 4: Identify indexes that should be dropped
	for _, col := range existingIndexes {
		// Find the frequency for this column
		colFrequency := int64(0)
		for _, stats := range topPredicates {
			if stats.Column == col {
				colFrequency = stats.Frequency
				break
			}
		}

		// If column is not in top predicates (frequency = 0) or below threshold, drop it
		if colFrequency < p.dropThreshold {
			actions = append(actions, IndexAction{
				Type:   ActionDrop,
				Column: col,
			})
		}
	}

	return actions, nil
}

// executeAction performs the specified index action.
func (p *Policy) executeAction(ctx context.Context, action IndexAction) error {
	switch action.Type {
	case ActionCreate:
		return p.executeCreate(ctx, action.Column)
	case ActionDrop:
		return p.executeDrop(ctx, action.Column)
	default:
		return fmt.Errorf("unknown action type: %s", action.Type)
	}
}

// executeCreate builds a new index for the specified column.
func (p *Policy) executeCreate(ctx context.Context, column string) error {
	log.Printf("index policy: creating index for column %s", column)

	// Get recent partitions from the data catalog
	partitions, err := p.dataCatalog.GetPartitions(ctx)
	if err != nil {
		return fmt.Errorf("failed to get partitions for index build: %w", err)
	}

	if len(partitions) == 0 {
		log.Printf("index policy: no partitions found for column %s, skipping index creation", column)
		return nil
	}

	// Build the index
	indexInfos, err := p.builder.BuildIndex(ctx, p.collection, column, partitions)
	if err != nil {
		return fmt.Errorf("failed to build index for column %s: %w", column, err)
	}

	log.Printf("index policy: successfully created %d index partitions for column %s", len(indexInfos), column)
	return nil
}

// executeDrop removes the index for the specified column.
func (p *Policy) executeDrop(ctx context.Context, column string) error {
	log.Printf("index policy: dropping index for column %s", column)

	// Delete from catalog and get object paths for S3 cleanup
	objectPaths, err := p.indexCatalog.DeleteIndex(ctx, p.collection, column)
	if err != nil {
		return fmt.Errorf("failed to delete index for column %s: %w", column, err)
	}

	// Delete S3 objects (best-effort, log errors but don't fail)
	if p.storage != nil {
		for _, path := range objectPaths {
			if err := p.storage.Delete(ctx, path); err != nil {
				log.Printf("index policy: failed to delete S3 object %s: %v", path, err)
			}
		}
	}

	log.Printf("index policy: successfully dropped index for column %s (%d S3 objects deleted)",
		column, len(objectPaths))
	return nil
}