package planner

import (
	"context"
	"encoding/binary"
	"fmt"
	"path/filepath"
	"strings"
	"sync"

	"github.com/arkilian/arkilian/internal/bloom"
	"github.com/arkilian/arkilian/internal/manifest"
	"github.com/arkilian/arkilian/internal/partition"
	"github.com/arkilian/arkilian/internal/query/parser"
	"github.com/arkilian/arkilian/internal/storage"
)

// PruneResult contains the result of a 2-phase pruning operation.
type PruneResult struct {
	// Partitions is the final list of partitions after all pruning phases.
	Partitions []*manifest.PartitionRecord

	// TotalPartitions is the total number of active partitions before pruning.
	TotalPartitions int

	// Phase1Candidates is the number of partitions after min/max pruning.
	Phase1Candidates int

	// Phase2Candidates is the number of partitions after bloom filter pruning.
	Phase2Candidates int

	// PruningRatio is the ratio of pruned partitions (0.0 to 1.0).
	PruningRatio float64
}

// Pruner implements 2-phase partition pruning.
// Phase 1: Query manifest with min/max predicates
// Phase 2: Apply bloom filters to candidate partitions
type Pruner struct {
	catalog *manifest.SQLiteCatalog
	storage storage.ObjectStorage

	// bloomCache caches loaded bloom filters by partition ID and column.
	bloomCache   map[string]map[string]*bloom.BloomFilter
	bloomCacheMu sync.RWMutex

	// metadataCache caches loaded metadata sidecars by partition ID.
	metadataCache   map[string]*partition.MetadataSidecar
	metadataCacheMu sync.RWMutex
}

// NewPruner creates a new 2-phase pruner.
func NewPruner(catalog *manifest.SQLiteCatalog, storage storage.ObjectStorage) *Pruner {
	return &Pruner{
		catalog:       catalog,
		storage:       storage,
		bloomCache:    make(map[string]map[string]*bloom.BloomFilter),
		metadataCache: make(map[string]*partition.MetadataSidecar),
	}
}

// Prune performs 2-phase partition pruning.
// Phase 1: Query manifest with min/max predicates to get candidate partitions.
// Phase 2: Apply bloom filters to eliminate false positives from phase 1.
func (p *Pruner) Prune(ctx context.Context, manifestPredicates []manifest.Predicate, parserPredicates []parser.Predicate) (*PruneResult, error) {
	// Get total partition count
	totalCount, err := p.catalog.GetPartitionCount(ctx)
	if err != nil {
		return nil, fmt.Errorf("pruner: failed to get partition count: %w", err)
	}

	// Phase 1: Min/max pruning via manifest catalog
	phase1Candidates, err := p.phase1Prune(ctx, manifestPredicates)
	if err != nil {
		return nil, fmt.Errorf("pruner: phase 1 failed: %w", err)
	}

	// Phase 2: Bloom filter pruning
	phase2Candidates, err := p.phase2Prune(ctx, phase1Candidates, parserPredicates)
	if err != nil {
		return nil, fmt.Errorf("pruner: phase 2 failed: %w", err)
	}

	// Calculate pruning ratio
	var pruningRatio float64
	if totalCount > 0 {
		pruningRatio = float64(int(totalCount)-len(phase2Candidates)) / float64(totalCount)
	}

	return &PruneResult{
		Partitions:       phase2Candidates,
		TotalPartitions:  int(totalCount),
		Phase1Candidates: len(phase1Candidates),
		Phase2Candidates: len(phase2Candidates),
		PruningRatio:     pruningRatio,
	}, nil
}

// phase1Prune performs min/max based pruning using the manifest catalog.
func (p *Pruner) phase1Prune(ctx context.Context, predicates []manifest.Predicate) ([]*manifest.PartitionRecord, error) {
	if len(predicates) == 0 {
		// No predicates, return all active partitions
		return p.catalog.FindPartitions(ctx, nil)
	}

	return p.catalog.FindPartitions(ctx, predicates)
}

// phase2Prune applies bloom filters to eliminate false positives.
func (p *Pruner) phase2Prune(ctx context.Context, candidates []*manifest.PartitionRecord, predicates []parser.Predicate) ([]*manifest.PartitionRecord, error) {
	// Get equality predicates that can use bloom filters
	bloomPredicates := getBloomFilterPredicates(predicates)

	if len(bloomPredicates) == 0 {
		// No bloom filter predicates, return all candidates
		return candidates, nil
	}

	var result []*manifest.PartitionRecord

	for _, candidate := range candidates {
		// Check if partition passes all bloom filter predicates
		passes, err := p.checkBloomFilters(ctx, candidate, bloomPredicates)
		if err != nil {
			// On error, include the partition to avoid false negatives
			result = append(result, candidate)
			continue
		}

		if passes {
			result = append(result, candidate)
		}
	}

	return result, nil
}

// getBloomFilterPredicates extracts predicates that can use bloom filter pruning.
func getBloomFilterPredicates(predicates []parser.Predicate) []parser.Predicate {
	var result []parser.Predicate

	for _, p := range predicates {
		if parser.CanUseBloomFilter(p) {
			// Only include predicates on columns that have bloom filters
			if isBloomFilterColumn(p.Column) {
				result = append(result, p)
			}
		}
	}

	return result
}

// isBloomFilterColumn returns true if the column has a bloom filter.
func isBloomFilterColumn(column string) bool {
	// Bloom filters are built for tenant_id and user_id columns
	return column == "tenant_id" || column == "user_id"
}

// checkBloomFilters checks if a partition passes all bloom filter predicates.
func (p *Pruner) checkBloomFilters(ctx context.Context, partition *manifest.PartitionRecord, predicates []parser.Predicate) (bool, error) {
	for _, pred := range predicates {
		passes, err := p.checkBloomFilter(ctx, partition, pred)
		if err != nil {
			return true, err // On error, assume it passes to avoid false negatives
		}
		if !passes {
			return false, nil
		}
	}
	return true, nil
}

// checkBloomFilter checks if a partition passes a single bloom filter predicate.
func (p *Pruner) checkBloomFilter(ctx context.Context, partitionRec *manifest.PartitionRecord, pred parser.Predicate) (bool, error) {
	// Get bloom filter for this partition and column
	filter, err := p.getBloomFilter(ctx, partitionRec, pred.Column)
	if err != nil {
		return true, err // On error, assume it passes
	}
	if filter == nil {
		return true, nil // No bloom filter, assume it passes
	}

	// Check based on predicate type
	switch pred.Type {
	case parser.PredicateEquality:
		if pred.Operator == "=" {
			return p.checkBloomFilterValue(filter, pred.Column, pred.Value), nil
		}
	case parser.PredicateIn:
		if !pred.Not {
			// For IN predicates, check if any value might be in the partition
			for _, val := range pred.Values {
				if p.checkBloomFilterValue(filter, pred.Column, val) {
					return true, nil
				}
			}
			return false, nil
		}
	}

	return true, nil
}

// checkBloomFilterValue checks if a value might be in the bloom filter.
func (p *Pruner) checkBloomFilterValue(filter *bloom.BloomFilter, column string, value interface{}) bool {
	var bytes []byte

	switch v := value.(type) {
	case string:
		bytes = []byte(v)
	case int64:
		bytes = int64ToBytes(v)
	case int:
		bytes = int64ToBytes(int64(v))
	case float64:
		bytes = int64ToBytes(int64(v))
	default:
		// Unknown type, assume it might be present
		return true
	}

	return filter.Contains(bytes)
}

// int64ToBytes converts an int64 to a byte slice (big-endian).
func int64ToBytes(v int64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(v))
	return b
}

// getBloomFilter retrieves a bloom filter for a partition and column.
func (p *Pruner) getBloomFilter(ctx context.Context, partitionRec *manifest.PartitionRecord, column string) (*bloom.BloomFilter, error) {
	// Check cache first
	p.bloomCacheMu.RLock()
	if filters, ok := p.bloomCache[partitionRec.PartitionID]; ok {
		if filter, ok := filters[column]; ok {
			p.bloomCacheMu.RUnlock()
			return filter, nil
		}
	}
	p.bloomCacheMu.RUnlock()

	// Load metadata sidecar
	metadata, err := p.loadMetadata(ctx, partitionRec)
	if err != nil {
		return nil, err
	}
	if metadata == nil {
		return nil, nil
	}

	// Get bloom filter for column
	filterMeta, ok := metadata.BloomFilters[column]
	if !ok {
		return nil, nil
	}

	// Deserialize bloom filter
	filter, err := bloom.DeserializeFromBase64(filterMeta.Base64Data)
	if err != nil {
		return nil, fmt.Errorf("pruner: failed to deserialize bloom filter: %w", err)
	}

	// Cache the filter
	p.bloomCacheMu.Lock()
	if _, ok := p.bloomCache[partitionRec.PartitionID]; !ok {
		p.bloomCache[partitionRec.PartitionID] = make(map[string]*bloom.BloomFilter)
	}
	p.bloomCache[partitionRec.PartitionID][column] = filter
	p.bloomCacheMu.Unlock()

	return filter, nil
}

// loadMetadata loads the metadata sidecar for a partition.
func (p *Pruner) loadMetadata(ctx context.Context, partitionRec *manifest.PartitionRecord) (*partition.MetadataSidecar, error) {
	// Check cache first
	p.metadataCacheMu.RLock()
	if metadata, ok := p.metadataCache[partitionRec.PartitionID]; ok {
		p.metadataCacheMu.RUnlock()
		return metadata, nil
	}
	p.metadataCacheMu.RUnlock()

	// Generate metadata path from object path
	metadataPath := generateMetadataPath(partitionRec.ObjectPath)

	// If storage is available, download the metadata
	if p.storage != nil {
		localPath := filepath.Join("/tmp", filepath.Base(metadataPath))
		if err := p.storage.Download(ctx, metadataPath, localPath); err != nil {
			return nil, fmt.Errorf("pruner: failed to download metadata: %w", err)
		}

		metadata, err := partition.ReadMetadataFromFile(localPath)
		if err != nil {
			return nil, fmt.Errorf("pruner: failed to read metadata: %w", err)
		}

		// Cache the metadata
		p.metadataCacheMu.Lock()
		p.metadataCache[partitionRec.PartitionID] = metadata
		p.metadataCacheMu.Unlock()

		return metadata, nil
	}

	// Try to read from local path (for testing)
	metadata, err := partition.ReadMetadataFromFile(metadataPath)
	if err != nil {
		// Metadata not available, return nil without error
		return nil, nil
	}

	// Cache the metadata
	p.metadataCacheMu.Lock()
	p.metadataCache[partitionRec.PartitionID] = metadata
	p.metadataCacheMu.Unlock()

	return metadata, nil
}

// generateMetadataPath generates the metadata file path from a SQLite object path.
func generateMetadataPath(objectPath string) string {
	ext := filepath.Ext(objectPath)
	base := objectPath[:len(objectPath)-len(ext)]
	return base + ".meta.json"
}

// PreloadBloomFilters preloads bloom filters for the given partitions into cache.
func (p *Pruner) PreloadBloomFilters(ctx context.Context, partitions []*manifest.PartitionRecord, columns []string) error {
	for _, part := range partitions {
		for _, col := range columns {
			if _, err := p.getBloomFilter(ctx, part, col); err != nil {
				// Log error but continue loading other filters
				continue
			}
		}
	}
	return nil
}

// ClearCache clears all cached bloom filters and metadata.
func (p *Pruner) ClearCache() {
	p.bloomCacheMu.Lock()
	p.bloomCache = make(map[string]map[string]*bloom.BloomFilter)
	p.bloomCacheMu.Unlock()

	p.metadataCacheMu.Lock()
	p.metadataCache = make(map[string]*partition.MetadataSidecar)
	p.metadataCacheMu.Unlock()
}

// CacheStats returns statistics about the bloom filter cache.
type CacheStats struct {
	PartitionsWithFilters int
	TotalFilters          int
	MetadataCached        int
}

// GetCacheStats returns statistics about the cache.
func (p *Pruner) GetCacheStats() CacheStats {
	p.bloomCacheMu.RLock()
	defer p.bloomCacheMu.RUnlock()

	p.metadataCacheMu.RLock()
	defer p.metadataCacheMu.RUnlock()

	var totalFilters int
	for _, filters := range p.bloomCache {
		totalFilters += len(filters)
	}

	return CacheStats{
		PartitionsWithFilters: len(p.bloomCache),
		TotalFilters:          totalFilters,
		MetadataCached:        len(p.metadataCache),
	}
}

// LoadBloomFiltersFromMetadata loads bloom filters directly from a metadata sidecar.
// This is useful for testing or when metadata is already available.
func (p *Pruner) LoadBloomFiltersFromMetadata(partitionID string, metadata *partition.MetadataSidecar) error {
	if metadata == nil {
		return nil
	}

	p.metadataCacheMu.Lock()
	p.metadataCache[partitionID] = metadata
	p.metadataCacheMu.Unlock()

	p.bloomCacheMu.Lock()
	defer p.bloomCacheMu.Unlock()

	if _, ok := p.bloomCache[partitionID]; !ok {
		p.bloomCache[partitionID] = make(map[string]*bloom.BloomFilter)
	}

	for column, filterMeta := range metadata.BloomFilters {
		filter, err := bloom.DeserializeFromBase64(filterMeta.Base64Data)
		if err != nil {
			return fmt.Errorf("pruner: failed to deserialize bloom filter for %s: %w", column, err)
		}
		p.bloomCache[partitionID][column] = filter
	}

	return nil
}

// PruneWithBloomFiltersOnly performs only bloom filter pruning on the given partitions.
// This is useful when min/max pruning has already been done.
func (p *Pruner) PruneWithBloomFiltersOnly(ctx context.Context, partitions []*manifest.PartitionRecord, predicates []parser.Predicate) ([]*manifest.PartitionRecord, error) {
	return p.phase2Prune(ctx, partitions, predicates)
}

// PruneWithMinMaxOnly performs only min/max pruning using the manifest catalog.
// This is useful when bloom filters are not available.
func (p *Pruner) PruneWithMinMaxOnly(ctx context.Context, predicates []manifest.Predicate) ([]*manifest.PartitionRecord, error) {
	return p.phase1Prune(ctx, predicates)
}

// CanPruneColumn returns true if the column can be used for pruning.
func CanPruneColumn(column string) bool {
	// Columns that support min/max pruning
	minMaxColumns := map[string]bool{
		"user_id":    true,
		"event_time": true,
		"tenant_id":  true,
	}

	return minMaxColumns[column] || isBloomFilterColumn(column)
}

// GetPrunableColumns returns the list of columns that can be used for pruning.
func GetPrunableColumns() []string {
	return []string{"user_id", "event_time", "tenant_id"}
}

// GetBloomFilterColumns returns the list of columns that have bloom filters.
func GetBloomFilterColumns() []string {
	return []string{"tenant_id", "user_id"}
}

// ExtractPrunablePredicates extracts predicates that can be used for pruning.
func ExtractPrunablePredicates(predicates []parser.Predicate) (minMaxPredicates []manifest.Predicate, bloomPredicates []parser.Predicate) {
	for _, p := range predicates {
		if parser.CanUseMinMaxPruning(p) && CanPruneColumn(p.Column) {
			mp := manifest.Predicate{
				Column:   p.Column,
				Operator: p.Operator,
				Value:    p.Value,
			}
			if p.Type == parser.PredicateBetween {
				mp.Operator = "BETWEEN"
				mp.Values = []interface{}{p.Low, p.High}
			}
			if p.Type == parser.PredicateIn && !p.Not {
				mp.Operator = "IN"
				mp.Values = p.Values
			}
			minMaxPredicates = append(minMaxPredicates, mp)
		}

		if parser.CanUseBloomFilter(p) && isBloomFilterColumn(p.Column) {
			bloomPredicates = append(bloomPredicates, p)
		}
	}
	return
}

// PartitionContainsValue checks if a partition might contain a specific value
// based on its min/max statistics.
func PartitionContainsValue(partition *manifest.PartitionRecord, column string, value interface{}) bool {
	switch column {
	case "user_id":
		if partition.MinUserID == nil || partition.MaxUserID == nil {
			return true // Unknown range, assume it might contain the value
		}
		if v, ok := value.(int64); ok {
			return v >= *partition.MinUserID && v <= *partition.MaxUserID
		}
	case "event_time":
		if partition.MinEventTime == nil || partition.MaxEventTime == nil {
			return true
		}
		if v, ok := value.(int64); ok {
			return v >= *partition.MinEventTime && v <= *partition.MaxEventTime
		}
	case "tenant_id":
		if partition.MinTenantID == nil || partition.MaxTenantID == nil {
			return true
		}
		if v, ok := value.(string); ok {
			return v >= *partition.MinTenantID && v <= *partition.MaxTenantID
		}
	}
	return true // Unknown column, assume it might contain the value
}

// PartitionOverlapsRange checks if a partition's range overlaps with a query range.
func PartitionOverlapsRange(partition *manifest.PartitionRecord, column string, minVal, maxVal interface{}) bool {
	switch column {
	case "user_id":
		if partition.MinUserID == nil || partition.MaxUserID == nil {
			return true
		}
		minV, okMin := minVal.(int64)
		maxV, okMax := maxVal.(int64)
		if okMin && okMax {
			return *partition.MinUserID <= maxV && *partition.MaxUserID >= minV
		}
	case "event_time":
		if partition.MinEventTime == nil || partition.MaxEventTime == nil {
			return true
		}
		minV, okMin := minVal.(int64)
		maxV, okMax := maxVal.(int64)
		if okMin && okMax {
			return *partition.MinEventTime <= maxV && *partition.MaxEventTime >= minV
		}
	case "tenant_id":
		if partition.MinTenantID == nil || partition.MaxTenantID == nil {
			return true
		}
		minV, okMin := minVal.(string)
		maxV, okMax := maxVal.(string)
		if okMin && okMax {
			return strings.Compare(*partition.MinTenantID, maxV) <= 0 &&
				strings.Compare(*partition.MaxTenantID, minV) >= 0
		}
	}
	return true
}
