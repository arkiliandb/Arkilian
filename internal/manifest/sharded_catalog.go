// Package manifest provides the manifest catalog for tracking partition metadata.
//
// ShardedCatalog distributes partition metadata across multiple SQLite shard files
// to overcome single-file I/O limits at 500K+ partitions. Shards are selected by
// FNV-1a hash of the partition_key, so all partitions sharing a key land in the
// same shard — preserving transactional guarantees for compaction within a key.
package manifest

import (
	"context"
	"fmt"
	"hash/fnv"
	"log"
	"path/filepath"
	"sync"
	"time"

	"github.com/arkilian/arkilian/internal/bloom"
	"github.com/arkilian/arkilian/internal/index"
	"github.com/arkilian/arkilian/internal/partition"
)

// DefaultShardCount is the default number of manifest shards.
const DefaultShardCount = 16

// ShardedCatalog implements Catalog by distributing partitions across N SQLite shards.
// Shard selection: fnv32a(partition_key) % shardCount.
// Each shard is a full SQLiteCatalog with its own WAL, write mutex, and read pool.
type ShardedCatalog struct {
	shards     []*SQLiteCatalog
	shardCount uint32
	baseDir    string
}

// NewShardedCatalog creates a sharded catalog with the given number of shards.
// Each shard is stored as manifest_shard_NNNN.db in baseDir.
func NewShardedCatalog(baseDir string, shardCount int) (*ShardedCatalog, error) {
	if shardCount <= 0 {
		shardCount = DefaultShardCount
	}

	sc := &ShardedCatalog{
		shards:     make([]*SQLiteCatalog, shardCount),
		shardCount: uint32(shardCount),
		baseDir:    baseDir,
	}

	for i := 0; i < shardCount; i++ {
		dbPath := filepath.Join(baseDir, fmt.Sprintf("manifest_shard_%04d.db", i))
		catalog, err := NewCatalog(dbPath)
		if err != nil {
			// Close any already-opened shards
			for j := 0; j < i; j++ {
				sc.shards[j].Close()
			}
			return nil, fmt.Errorf("manifest: failed to open shard %d: %w", i, err)
		}
		sc.shards[i] = catalog
	}

	log.Printf("Sharded manifest catalog initialized: %d shards in %s", shardCount, baseDir)
	return sc, nil
}

// shardFor returns the shard index for a given partition key.
func (sc *ShardedCatalog) shardFor(partitionKey string) int {
	h := fnv.New32a()
	h.Write([]byte(partitionKey))
	return int(h.Sum32() % sc.shardCount)
}

// shardForID returns the shard that owns a partition by looking it up across all shards.
// This is the slow path — used only for GetPartition by ID where we don't know the key.
func (sc *ShardedCatalog) shardForID(ctx context.Context, partitionID string) (*SQLiteCatalog, error) {
	for _, shard := range sc.shards {
		record, err := shard.GetPartition(ctx, partitionID)
		if err == nil && record != nil {
			return shard, nil
		}
	}
	return nil, fmt.Errorf("manifest: partition %s not found in any shard", partitionID)
}

// Shard returns the underlying SQLiteCatalog for a given partition key.
// Exported for callers that need direct shard access (e.g., Pruner).
func (sc *ShardedCatalog) Shard(partitionKey string) *SQLiteCatalog {
	return sc.shards[sc.shardFor(partitionKey)]
}

// ShardCount returns the number of shards.
func (sc *ShardedCatalog) ShardCount() int {
	return int(sc.shardCount)
}

// AllShards returns all underlying shard catalogs.
func (sc *ShardedCatalog) AllShards() []*SQLiteCatalog {
	return sc.shards
}

// ---------------------------------------------------------------------------
// Catalog interface implementation
// ---------------------------------------------------------------------------

// RegisterPartition routes the partition to the correct shard and registers it.
func (sc *ShardedCatalog) RegisterPartition(ctx context.Context, info *partition.PartitionInfo, objectPath string) error {
	return sc.shards[sc.shardFor(info.PartitionKey)].RegisterPartition(ctx, info, objectPath)
}

// RegisterPartitionWithIdempotencyKey routes to the correct shard.
// Idempotency keys are scoped per-shard (same partition_key → same shard → same idempotency table).
func (sc *ShardedCatalog) RegisterPartitionWithIdempotencyKey(ctx context.Context, info *partition.PartitionInfo, objectPath, idempotencyKey string) (string, error) {
	return sc.shards[sc.shardFor(info.PartitionKey)].RegisterPartitionWithIdempotencyKey(ctx, info, objectPath, idempotencyKey)
}

// GetPartition searches all shards for the partition ID (we don't know the key from just an ID).
func (sc *ShardedCatalog) GetPartition(ctx context.Context, partitionID string) (*PartitionRecord, error) {
	type result struct {
		record *PartitionRecord
		err    error
	}
	ch := make(chan result, len(sc.shards))

	for _, shard := range sc.shards {
		go func(s *SQLiteCatalog) {
			r, err := s.GetPartition(ctx, partitionID)
			ch <- result{r, err}
		}(shard)
	}

	for range sc.shards {
		res := <-ch
		if res.err == nil && res.record != nil {
			return res.record, nil
		}
	}
	return nil, fmt.Errorf("manifest: partition %s not found", partitionID)
}

// FindPartitions queries all shards in parallel and merges results.
func (sc *ShardedCatalog) FindPartitions(ctx context.Context, predicates []Predicate) ([]*PartitionRecord, error) {
	type result struct {
		records []*PartitionRecord
		err     error
	}
	ch := make(chan result, len(sc.shards))

	for _, shard := range sc.shards {
		go func(s *SQLiteCatalog) {
			r, err := s.FindPartitions(ctx, predicates)
			ch <- result{r, err}
		}(shard)
	}

	var all []*PartitionRecord
	for range sc.shards {
		res := <-ch
		if res.err != nil {
			return nil, res.err
		}
		all = append(all, res.records...)
	}
	return all, nil
}

// MarkCompacted marks source partitions as compacted into target.
// All source partitions and the target must share the same partition key (same shard).
// We find the shard via the target partition ID.
func (sc *ShardedCatalog) MarkCompacted(ctx context.Context, sourceIDs []string, targetID string) error {
	shard, err := sc.shardForID(ctx, targetID)
	if err != nil {
		return err
	}
	return shard.MarkCompacted(ctx, sourceIDs, targetID)
}

// GetCompactionCandidates routes to the shard owning the partition key.
func (sc *ShardedCatalog) GetCompactionCandidates(ctx context.Context, key string, maxSize int64) ([]*PartitionRecord, error) {
	return sc.shards[sc.shardFor(key)].GetCompactionCandidates(ctx, key, maxSize)
}

// DeleteExpired runs TTL cleanup across all shards and aggregates deleted IDs.
func (sc *ShardedCatalog) DeleteExpired(ctx context.Context, ttl time.Duration) ([]string, error) {
	type result struct {
		ids []string
		err error
	}
	ch := make(chan result, len(sc.shards))

	for _, shard := range sc.shards {
		go func(s *SQLiteCatalog) {
			ids, err := s.DeleteExpired(ctx, ttl)
			ch <- result{ids, err}
		}(shard)
	}

	var all []string
	for range sc.shards {
		res := <-ch
		if res.err != nil {
			return nil, res.err
		}
		all = append(all, res.ids...)
	}
	return all, nil
}

// FindHighestIdempotencyLSN queries all shards and returns the maximum LSN found.
// This fans out to all shards, queries their idempotency_keys table, and returns the max.
func (sc *ShardedCatalog) FindHighestIdempotencyLSN(ctx context.Context, prefix string) (uint64, error) {
	type result struct {
		lsn uint64
		err error
	}
	ch := make(chan result, len(sc.shards))

	for _, shard := range sc.shards {
		go func(s *SQLiteCatalog) {
			lsn, err := s.FindHighestIdempotencyLSN(ctx, prefix)
			ch <- result{lsn, err}
		}(shard)
	}

	var highestLSN uint64 = 0
	for range sc.shards {
		res := <-ch
		if res.err != nil {
			return 0, res.err
		}
		if res.lsn > highestLSN {
			highestLSN = res.lsn
		}
	}
	return highestLSN, nil
}

// Close closes all shard databases.
func (sc *ShardedCatalog) Close() error {
	var firstErr error
	for i, shard := range sc.shards {
		if err := shard.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("manifest: failed to close shard %d: %w", i, err)
		}
	}
	return firstErr
}

// ---------------------------------------------------------------------------
// Extended methods (used by planner, pruner, compaction beyond the Catalog interface)
// ---------------------------------------------------------------------------

// GetPartitionCount returns the total active partition count across all shards.
func (sc *ShardedCatalog) GetPartitionCount(ctx context.Context) (int64, error) {
	var total int64
	var mu sync.Mutex
	errs := make(chan error, len(sc.shards))

	for _, shard := range sc.shards {
		go func(s *SQLiteCatalog) {
			count, err := s.GetPartitionCount(ctx)
			if err != nil {
				errs <- err
				return
			}
			mu.Lock()
			total += count
			mu.Unlock()
			errs <- nil
		}(shard)
	}

	for range sc.shards {
		if err := <-errs; err != nil {
			return 0, err
		}
	}
	return total, nil
}

// GetPartitionCountByKey returns the count for a specific partition key (single shard).
func (sc *ShardedCatalog) GetPartitionCountByKey(ctx context.Context, key string) (int64, error) {
	return sc.shards[sc.shardFor(key)].GetPartitionCountByKey(ctx, key)
}

// RunAnalyze runs ANALYZE on all shards in parallel.
func (sc *ShardedCatalog) RunAnalyze(ctx context.Context) error {
	errs := make(chan error, len(sc.shards))
	for _, shard := range sc.shards {
		go func(s *SQLiteCatalog) {
			errs <- s.RunAnalyze(ctx)
		}(shard)
	}
	for range sc.shards {
		if err := <-errs; err != nil {
			return err
		}
	}
	return nil
}

// FindCompactedPartitions returns compacted partitions across all shards.
func (sc *ShardedCatalog) FindCompactedPartitions(ctx context.Context) ([]*PartitionRecord, error) {
	type result struct {
		records []*PartitionRecord
		err     error
	}
	ch := make(chan result, len(sc.shards))

	for _, shard := range sc.shards {
		go func(s *SQLiteCatalog) {
			r, err := s.FindCompactedPartitions(ctx)
			ch <- result{r, err}
		}(shard)
	}

	var all []*PartitionRecord
	for range sc.shards {
		res := <-ch
		if res.err != nil {
			return nil, res.err
		}
		all = append(all, res.records...)
	}
	return all, nil
}

// ResetCompactedInto resets compacted_into across all shards (for crash recovery).
func (sc *ShardedCatalog) ResetCompactedInto(ctx context.Context, partitionIDs []string) error {
	// Group IDs by shard — we need to find which shard owns each ID.
	// For crash recovery this is a rare path, so the fan-out is acceptable.
	for _, shard := range sc.shards {
		if err := shard.ResetCompactedInto(ctx, partitionIDs); err != nil {
			return err
		}
	}
	return nil
}

// WriteCompactionIntent writes to the shard that owns the target partition key.
func (sc *ShardedCatalog) WriteCompactionIntent(ctx context.Context, intent *CompactionIntent) error {
	// The target partition ID encodes the partition key via the source partitions.
	// We need to find the right shard. Look up the first source partition to determine the key.
	if len(intent.SourcePartitionIDs) > 0 {
		record, err := sc.GetPartition(ctx, intent.SourcePartitionIDs[0])
		if err == nil {
			return sc.shards[sc.shardFor(record.PartitionKey)].WriteCompactionIntent(ctx, intent)
		}
	}
	// Fallback: write to all shards (only one will have the matching data)
	for _, shard := range sc.shards {
		if err := shard.WriteCompactionIntent(ctx, intent); err != nil {
			return err
		}
	}
	return nil
}

// CompleteCompaction completes a compaction in the shard owning the partition key.
func (sc *ShardedCatalog) CompleteCompaction(ctx context.Context, info *partition.PartitionInfo, objectPath string, sourceIDs []string, intentTargetID string) error {
	return sc.shards[sc.shardFor(info.PartitionKey)].CompleteCompaction(ctx, info, objectPath, sourceIDs, intentTargetID)
}

// FindCompactionIntents returns intents from all shards.
func (sc *ShardedCatalog) FindCompactionIntents(ctx context.Context) ([]*CompactionIntent, error) {
	type result struct {
		intents []*CompactionIntent
		err     error
	}
	ch := make(chan result, len(sc.shards))

	for _, shard := range sc.shards {
		go func(s *SQLiteCatalog) {
			r, err := s.FindCompactionIntents(ctx)
			ch <- result{r, err}
		}(shard)
	}

	var all []*CompactionIntent
	for range sc.shards {
		res := <-ch
		if res.err != nil {
			return nil, res.err
		}
		all = append(all, res.intents...)
	}
	return all, nil
}

// DeleteCompactionIntent deletes from all shards (only one will match).
func (sc *ShardedCatalog) DeleteCompactionIntent(ctx context.Context, targetPartitionID string) error {
	for _, shard := range sc.shards {
		if err := shard.DeleteCompactionIntent(ctx, targetPartitionID); err != nil {
			return err
		}
	}
	return nil
}

// FindPartitionsByKeyPrefix queries all shards in parallel.
func (sc *ShardedCatalog) FindPartitionsByKeyPrefix(ctx context.Context, prefix string) ([]*PartitionRecord, error) {
	type result struct {
		records []*PartitionRecord
		err     error
	}
	ch := make(chan result, len(sc.shards))

	for _, shard := range sc.shards {
		go func(s *SQLiteCatalog) {
			r, err := s.FindPartitionsByKeyPrefix(ctx, prefix)
			ch <- result{r, err}
		}(shard)
	}

	var all []*PartitionRecord
	for range sc.shards {
		res := <-ch
		if res.err != nil {
			return nil, res.err
		}
		all = append(all, res.records...)
	}
	return all, nil
}

// GetDistinctPartitionKeys returns distinct keys across all shards.
func (sc *ShardedCatalog) GetDistinctPartitionKeys(ctx context.Context) ([]string, error) {
	type result struct {
		keys []string
		err  error
	}
	ch := make(chan result, len(sc.shards))

	for _, shard := range sc.shards {
		go func(s *SQLiteCatalog) {
			k, err := s.GetDistinctPartitionKeys(ctx)
			ch <- result{k, err}
		}(shard)
	}

	seen := make(map[string]struct{})
	for range sc.shards {
		res := <-ch
		if res.err != nil {
			return nil, res.err
		}
		for _, k := range res.keys {
			seen[k] = struct{}{}
		}
	}

	keys := make([]string, 0, len(seen))
	for k := range seen {
		keys = append(keys, k)
	}
	return keys, nil
}

// GetPartitionCountByKeyPrefix aggregates counts across all shards.
func (sc *ShardedCatalog) GetPartitionCountByKeyPrefix(ctx context.Context, prefix string) (int64, error) {
	var total int64
	var mu sync.Mutex
	errs := make(chan error, len(sc.shards))

	for _, shard := range sc.shards {
		go func(s *SQLiteCatalog) {
			count, err := s.GetPartitionCountByKeyPrefix(ctx, prefix)
			if err != nil {
				errs <- err
				return
			}
			mu.Lock()
			total += count
			mu.Unlock()
			errs <- nil
		}(shard)
	}

	for range sc.shards {
		if err := <-errs; err != nil {
			return 0, err
		}
	}
	return total, nil
}

// GetTotalPartitionCount returns total count with threshold warnings.
func (sc *ShardedCatalog) GetTotalPartitionCount(ctx context.Context) (int64, error) {
	count, err := sc.GetPartitionCount(ctx)
	if err != nil {
		return 0, err
	}
	if count > 500000 {
		log.Printf("[WARN] manifest: total active partition count (%d) exceeds 500K across %d shards", count, sc.shardCount)
	}
	return count, nil
}

// ---------------------------------------------------------------------------
// Zone map methods (route to correct shard by partition key)
// ---------------------------------------------------------------------------

// UpsertZoneMap routes to the shard owning the partition key.
func (sc *ShardedCatalog) UpsertZoneMap(ctx context.Context, entry *ZoneMapEntry) error {
	return sc.shards[sc.shardFor(entry.PartitionKey)].UpsertZoneMap(ctx, entry)
}

// GetZoneMap routes to the shard owning the partition key.
func (sc *ShardedCatalog) GetZoneMap(ctx context.Context, partitionKey, column string) (*ZoneMapEntry, error) {
	return sc.shards[sc.shardFor(partitionKey)].GetZoneMap(ctx, partitionKey, column)
}

// GetZoneMapsForKeys queries zone maps across shards in parallel.
// Keys are grouped by shard, queried concurrently, and results merged.
func (sc *ShardedCatalog) GetZoneMapsForKeys(ctx context.Context, partitionKeys []string, column string) (map[string]*ZoneMapEntry, error) {
	if len(partitionKeys) == 0 {
		return nil, nil
	}

	// Group keys by shard
	shardKeys := make(map[int][]string)
	for _, key := range partitionKeys {
		idx := sc.shardFor(key)
		shardKeys[idx] = append(shardKeys[idx], key)
	}

	type result struct {
		entries map[string]*ZoneMapEntry
		err     error
	}
	ch := make(chan result, len(shardKeys))

	for idx, keys := range shardKeys {
		go func(s *SQLiteCatalog, k []string) {
			entries, err := s.GetZoneMapsForKeys(ctx, k, column)
			ch <- result{entries, err}
		}(sc.shards[idx], keys)
	}

	merged := make(map[string]*ZoneMapEntry, len(partitionKeys))
	for range shardKeys {
		res := <-ch
		if res.err != nil {
			return nil, res.err
		}
		for k, v := range res.entries {
			merged[k] = v
		}
	}
	return merged, nil
}

// MergeIntoZoneMap routes to the shard owning the partition key.
func (sc *ShardedCatalog) MergeIntoZoneMap(ctx context.Context, partitionKey, column string, partitionFilter *bloom.BloomFilter, distinctCount int) error {
	return sc.shards[sc.shardFor(partitionKey)].MergeIntoZoneMap(ctx, partitionKey, column, partitionFilter, distinctCount)
}

// DeleteZoneMap routes to the shard owning the partition key.
func (sc *ShardedCatalog) DeleteZoneMap(ctx context.Context, partitionKey string) error {
	return sc.shards[sc.shardFor(partitionKey)].DeleteZoneMap(ctx, partitionKey)
}

// UpdateZoneMapsFromMetadata routes to the shard owning the partition key.
func (sc *ShardedCatalog) UpdateZoneMapsFromMetadata(ctx context.Context, partitionKey string, bloomFilters map[string]*bloom.BloomFilter, distinctCounts map[string]int) error {
	return sc.shards[sc.shardFor(partitionKey)].UpdateZoneMapsFromMetadata(ctx, partitionKey, bloomFilters, distinctCounts)
}

// RebuildZoneMap routes to the shard owning the partition key.
func (sc *ShardedCatalog) RebuildZoneMap(ctx context.Context, partitionKey string) error {
	return sc.shards[sc.shardFor(partitionKey)].RebuildZoneMap(ctx, partitionKey)
}

// ---------------------------------------------------------------------------
// Index partition methods (route to correct shard by collection)
// ---------------------------------------------------------------------------

// RegisterIndexPartition routes to the shard that owns the collection.
func (sc *ShardedCatalog) RegisterIndexPartition(ctx context.Context, info *index.IndexPartitionInfo) error {
return sc.shards[sc.shardFor(info.Collection)].RegisterIndexPartition(ctx, info)
}

// FindIndexPartition routes to the shard that owns the collection.
func (sc *ShardedCatalog) FindIndexPartition(ctx context.Context, collection, column string, bucketID int) (*index.IndexPartitionInfo, error) {
return sc.shards[sc.shardFor(collection)].FindIndexPartition(ctx, collection, column, bucketID)
}

// ListIndexes queries all shards and merges results (deduplicates column names).
func (sc *ShardedCatalog) ListIndexes(ctx context.Context, collection string) ([]string, error) {
type result struct {
columns []string
err     error
}
ch := make(chan result, len(sc.shards))

for _, shard := range sc.shards {
go func(s *SQLiteCatalog) {
cols, err := s.ListIndexes(ctx, collection)
ch <- result{cols, err}
}(shard)
}

seen := make(map[string]struct{})
for range sc.shards {
res := <-ch
if res.err != nil {
return nil, res.err
}
for _, col := range res.columns {
seen[col] = struct{}{}
}
}

columns := make([]string, 0, len(seen))
for col := range seen {
columns = append(columns, col)
}
return columns, nil
}

// DeleteIndex fans out to all shards (only one will have the matching data).
// Returns all object paths deleted across all shards.
func (sc *ShardedCatalog) DeleteIndex(ctx context.Context, collection, column string) ([]string, error) {
	var allPaths []string
	var firstErr error

	for _, shard := range sc.shards {
		paths, err := shard.DeleteIndex(ctx, collection, column)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		allPaths = append(allPaths, paths...)
	}

	if firstErr != nil && len(allPaths) == 0 {
		return nil, firstErr
	}

	return allPaths, nil
}
