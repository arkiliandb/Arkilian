// Package config provides unified configuration for all Arkilian services.
package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	arkilianerrors "github.com/arkilian/arkilian/internal/errors"
	"github.com/joho/godotenv"
)

// Mode represents the service mode to run.
type Mode string

const (
	ModeAll     Mode = "all"
	ModeIngest  Mode = "ingest"
	ModeQuery   Mode = "query"
	ModeCompact Mode = "compact"
)

// Config holds the unified configuration for all Arkilian services.
type Config struct {
	// Mode specifies which services to run: all, ingest, query, compact
	Mode Mode `json:"mode" yaml:"mode"`

	// DataDir is the base directory for all data files
	DataDir string `json:"data_dir" yaml:"data_dir"`

	// HTTP configuration
	HTTP HTTPConfig `json:"http" yaml:"http"`

	// gRPC configuration
	GRPC GRPCConfig `json:"grpc" yaml:"grpc"`

	// Ingest service configuration
	Ingest IngestConfig `json:"ingest" yaml:"ingest"`

	// Query service configuration
	Query QueryConfig `json:"query" yaml:"query"`

	// Compaction service configuration
	Compaction CompactionConfig `json:"compaction" yaml:"compaction"`

	// Storage configuration
	Storage StorageConfig `json:"storage" yaml:"storage"`

	// Manifest configuration
	Manifest ManifestConfig `json:"manifest" yaml:"manifest"`

	// WAL configuration
	WAL WALConfig `json:"wal" yaml:"wal"`

	// Index configuration
	Index IndexConfig `json:"index" yaml:"index"`

	// Cache configuration
	Cache CacheConfig `json:"cache" yaml:"cache"`

	// Router configuration
	Router RouterConfig `json:"router" yaml:"router"`

	// V3 configuration (ArkFormat, shared WAL, etc.)
	V3 V3Config `json:"v3" yaml:"v3"`
}

// WALConfig holds write-ahead log configuration.
type WALConfig struct {
	Dir            string        `json:"dir" yaml:"dir"`
	MaxSegmentSize int64         `json:"max_segment_size" yaml:"max_segment_size"`
	FlushInterval  time.Duration `json:"flush_interval" yaml:"flush_interval"`
	FlushBatchSize int           `json:"flush_batch_size" yaml:"flush_batch_size"`
	RetentionTime  time.Duration `json:"retention_time" yaml:"retention_time"`
}

// IndexConfig holds secondary index configuration.
type IndexConfig struct {
	Collection      string        `json:"collection" yaml:"collection"`
	CreateThreshold int64         `json:"create_threshold" yaml:"create_threshold"`
	DropThreshold   int64         `json:"drop_threshold" yaml:"drop_threshold"`
	CheckInterval   time.Duration `json:"check_interval" yaml:"check_interval"`
	MaxIndexes      int           `json:"max_indexes" yaml:"max_indexes"`
	BucketCount     int           `json:"bucket_count" yaml:"bucket_count"`
}

// CacheConfig holds tiered cache configuration.
type CacheConfig struct {
	NVMeDir         string `json:"nvme_dir" yaml:"nvme_dir"`
	NVMeMaxBytes    int64  `json:"nvme_max_bytes" yaml:"nvme_max_bytes"`
	PrefetchEnabled bool   `json:"prefetch_enabled" yaml:"prefetch_enabled"`
}

// RouterConfig holds write notification router configuration.
type RouterConfig struct {
	BufferSize int `json:"buffer_size" yaml:"buffer_size"`
}

// ManifestConfig holds manifest catalog configuration.
type ManifestConfig struct {
	// Sharded enables sharded manifest mode (multiple SQLite files instead of one).
	// When false (default), a single manifest.db is used for backward compatibility.
	Sharded bool `json:"sharded" yaml:"sharded"`

	// ShardCount is the number of manifest shards (default: 16). Only used when Sharded=true.
	ShardCount int `json:"shard_count" yaml:"shard_count"`

	// AutoShardThreshold is the partition count at which a single-file catalog
	// automatically migrates to sharded mode. Set to 0 to disable auto-migration.
	// Default: 50000. When the threshold is crossed during startup, the app logs
	// a warning and performs an online migration to sharded mode.
	AutoShardThreshold int64 `json:"auto_shard_threshold" yaml:"auto_shard_threshold"`
}

// HTTPConfig holds HTTP server configuration.
type HTTPConfig struct {
	// IngestAddr is the HTTP address for the ingest service
	IngestAddr string `json:"ingest_addr" yaml:"ingest_addr"`

	// QueryAddr is the HTTP address for the query service
	QueryAddr string `json:"query_addr" yaml:"query_addr"`

	// CompactAddr is the HTTP address for the compaction service
	CompactAddr string `json:"compact_addr" yaml:"compact_addr"`

	// ReadTimeout is the HTTP read timeout
	ReadTimeout time.Duration `json:"read_timeout" yaml:"read_timeout"`

	// WriteTimeout is the HTTP write timeout
	WriteTimeout time.Duration `json:"write_timeout" yaml:"write_timeout"`

	// IdleTimeout is the HTTP idle timeout
	IdleTimeout time.Duration `json:"idle_timeout" yaml:"idle_timeout"`
}

// GRPCConfig holds gRPC server configuration.
type GRPCConfig struct {
	// Addr is the gRPC server address
	Addr string `json:"addr" yaml:"addr"`

	// Enabled controls whether gRPC is enabled
	Enabled bool `json:"enabled" yaml:"enabled"`
}

// IngestConfig holds ingest service configuration.
type IngestConfig struct {
	// PartitionDir is the directory for partition output
	PartitionDir string `json:"partition_dir" yaml:"partition_dir"`

	// TargetPartitionSizeMB is the target partition size in megabytes (8–256, default 16).
	// When AdaptiveSizing is enabled, this serves as the fallback for new partition keys.
	TargetPartitionSizeMB int `json:"target_partition_size_mb" yaml:"target_partition_size_mb"`

	// AdaptiveSizing configures automatic partition size scaling based on data volume.
	// When enabled, partition target sizes grow with the total data stored per key,
	// reducing S3 object count and LIST costs at scale.
	AdaptiveSizing AdaptiveSizingConfig `json:"adaptive_sizing" yaml:"adaptive_sizing"`
}

// AdaptiveSizingConfig controls how partition target sizes scale with data volume.
type AdaptiveSizingConfig struct {
	// Enabled turns adaptive sizing on/off. When false, TargetPartitionSizeMB is used.
	Enabled bool `json:"enabled" yaml:"enabled"`

	// MinSizeMB is the floor for partition size (default: 8).
	MinSizeMB int `json:"min_size_mb" yaml:"min_size_mb"`

	// MaxSizeMB is the ceiling for partition size (default: 128).
	MaxSizeMB int `json:"max_size_mb" yaml:"max_size_mb"`

	// Tiers maps cumulative data volume thresholds (in GB) to target partition sizes (in MB).
	Tiers []SizingTier `json:"tiers" yaml:"tiers"`
}

// SizingTier maps a cumulative volume threshold to a target partition size.
type SizingTier struct {
	// ThresholdGB is the minimum total data volume (in GB) for this tier to apply.
	ThresholdGB float64 `json:"threshold_gb" yaml:"threshold_gb"`

	// TargetSizeMB is the partition target size when this tier is active.
	TargetSizeMB int `json:"target_size_mb" yaml:"target_size_mb"`
}

// QueryConfig holds query service configuration.
type QueryConfig struct {
	// DownloadDir is the directory for downloaded partitions
	DownloadDir string `json:"download_dir" yaml:"download_dir"`

	// Concurrency is the number of parallel partition queries
	Concurrency int `json:"concurrency" yaml:"concurrency"`

	// PoolSize is the maximum number of SQLite connections
	PoolSize int `json:"pool_size" yaml:"pool_size"`

	// MaxPreloadPartitions is the max partitions to preload bloom filters for
	MaxPreloadPartitions int `json:"max_preload_partitions" yaml:"max_preload_partitions"`

	// BloomCacheSizeMB is the maximum memory for the bloom filter LRU cache in MB (default: 1024).
	BloomCacheSizeMB int `json:"bloom_cache_size_mb" yaml:"bloom_cache_size_mb"`
}

// CompactionConfig holds compaction service configuration.
type CompactionConfig struct {
	// WorkDir is the directory for compaction work files
	WorkDir string `json:"work_dir" yaml:"work_dir"`

	// CheckInterval is the interval between compaction checks
	CheckInterval time.Duration `json:"check_interval" yaml:"check_interval"`

	// MinPartitionSize is the minimum partition size before compaction (bytes)
	MinPartitionSize int64 `json:"min_partition_size" yaml:"min_partition_size"`

	// MaxPartitionsPerKey is the max partitions per key before compaction
	MaxPartitionsPerKey int64 `json:"max_partitions_per_key" yaml:"max_partitions_per_key"`

	// TTLDays is the days before compacted partitions are garbage collected
	TTLDays int `json:"ttl_days" yaml:"ttl_days"`

	// Backpressure controls dynamic concurrency adjustment based on failure rate.
	Backpressure BackpressureConfig `json:"backpressure" yaml:"backpressure"`
}

// BackpressureConfig holds backpressure configuration for the compaction daemon.
type BackpressureConfig struct {
	// MaxConcurrency is the upper bound for concurrent compaction goroutines (default: 4).
	MaxConcurrency int `json:"max_concurrency" yaml:"max_concurrency"`

	// MinConcurrency is the lower bound (default: 1).
	MinConcurrency int `json:"min_concurrency" yaml:"min_concurrency"`

	// FailureThreshold is the failure rate (0.0–1.0) above which backoff triggers (default: 0.10).
	FailureThreshold float64 `json:"failure_threshold" yaml:"failure_threshold"`
}

// StorageConfig holds storage configuration.
type StorageConfig struct {
	// Type is the storage type: local, s3
	Type string `json:"type" yaml:"type"`

	// Path is the local storage path (for local type)
	Path string `json:"path" yaml:"path"`

	// S3 configuration (for s3 type)
	S3 S3Config `json:"s3" yaml:"s3"`
}

// S3Config holds S3 storage configuration.
type S3Config struct {
	// Bucket is the S3 bucket name
	Bucket string `json:"bucket" yaml:"bucket"`

	// Region is the AWS region
	Region string `json:"region" yaml:"region"`

	// Endpoint is the S3 endpoint (for S3-compatible storage)
	Endpoint string `json:"endpoint" yaml:"endpoint"`
}

// V3Config holds Arkilian V3 configuration (ArkFormat, shared WAL, etc.)
type V3Config struct {
	ArkFormat  ArkFormatConfig    `json:"ark_format" yaml:"ark_format"`
	SharedWAL  SharedWALConfig    `json:"shared_wal" yaml:"shared_wal"`
	Compaction V3CompactionConfig `json:"v3_compaction" yaml:"v3_compaction"`
	Catalog    CatalogConfig      `json:"catalog" yaml:"catalog"`
	HotColumn  HotColumnConfig    `json:"hot_column" yaml:"hot_column"`
	Migration  MigrationConfig    `json:"migration" yaml:"migration"`
}

// ArkFormatConfig holds ArkFormat file format configuration.
type ArkFormatConfig struct {
	TargetFileSizeMB int     `json:"target_file_size_mb" yaml:"target_file_size_mb"` // default: 128
	Compression      string  `json:"compression" yaml:"compression"`                  // default: "zstd"
	BloomFPR         float64 `json:"bloom_fpr" yaml:"bloom_fpr"`                      // default: 0.003
	PGMEpsilon       int     `json:"pgm_epsilon" yaml:"pgm_epsilon"`                  // default: 64
}

// SharedWALConfig holds shared distributed WAL configuration.
type SharedWALConfig struct {
	Enabled       bool     `json:"enabled" yaml:"enabled"`                         // default: false
	RaftPeers     []string `json:"raft_peers" yaml:"raft_peers"`                   // required if enabled
	RaftDataDir   string   `json:"raft_data_dir" yaml:"raft_data_dir"`             // required if enabled
	SegmentSizeMB int      `json:"segment_size_mb" yaml:"segment_size_mb"`         // default: 64
	GRPCPort      int      `json:"grpc_port" yaml:"grpc_port"`                     // default: 9090
}

// V3CompactionConfig holds V3 compaction engine configuration.
type V3CompactionConfig struct {
	HourlyCron  string `json:"hourly_cron" yaml:"hourly_cron"`       // default: "0 * * * *"
	Workers     int    `json:"workers" yaml:"workers"`               // default: runtime.NumCPU() * 2
	RAMBudgetMB int    `json:"ram_budget_mb" yaml:"ram_budget_mb"`   // default: 2048
	TmpDir      string `json:"tmp_dir" yaml:"tmp_dir"`
}

// CatalogConfig holds catalog service configuration.
type CatalogConfig struct {
	ShardCount  int    `json:"shard_count" yaml:"shard_count"`     // default: 64
	SnapshotDir string `json:"snapshot_dir" yaml:"snapshot_dir"`
	GRPCPort    int    `json:"grpc_port" yaml:"grpc_port"`         // default: 9091
}

// HotColumnConfig holds hot-column detection and sorted run configuration.
type HotColumnConfig struct {
	CreateThreshold int64         `json:"create_threshold" yaml:"create_threshold"` // default: 200
	DropThreshold   int64         `json:"drop_threshold" yaml:"drop_threshold"`     // default: 10
	CheckInterval   time.Duration `json:"check_interval" yaml:"check_interval"`     // default: 5m
	MaxHotColumns   int           `json:"max_hot_columns" yaml:"max_hot_columns"`   // default: 10
}

// MigrationConfig holds V2 → V3 migration configuration.
type MigrationConfig struct {
	Phase     string `json:"phase" yaml:"phase"`           // "A", "B", "C", or empty
	Workers   int    `json:"workers" yaml:"workers"`       // default: 32
	DualWrite bool   `json:"dual_write" yaml:"dual_write"` // Phase A only
}

// DefaultConfig returns the default configuration for local development.
func DefaultConfig() *Config {
	return &Config{
		Mode:    ModeAll,
		DataDir: "./data/arkilian",
		HTTP: HTTPConfig{
			IngestAddr:   ":8080",
			QueryAddr:    ":8081",
			CompactAddr:  ":8082",
			ReadTimeout:  30 * time.Second,
			WriteTimeout: 120 * time.Second,
			IdleTimeout:  120 * time.Second,
		},
		GRPC: GRPCConfig{
			Addr:    ":9090",
			Enabled: true,
		},
		Ingest: IngestConfig{
			PartitionDir:          "",
			TargetPartitionSizeMB: 64,
			AdaptiveSizing: AdaptiveSizingConfig{
				Enabled:   true,
				MinSizeMB: 16,
				MaxSizeMB: 256,
				Tiers: []SizingTier{
					{ThresholdGB: 0, TargetSizeMB: 64},
					{ThresholdGB: 1, TargetSizeMB: 128},
					{ThresholdGB: 10, TargetSizeMB: 192},
					{ThresholdGB: 100, TargetSizeMB: 256},
				},
			},
		},
		Query: QueryConfig{
			DownloadDir:          "",
			Concurrency:          32,
			PoolSize:             512,
			MaxPreloadPartitions: 10000,
			BloomCacheSizeMB:     4096,
		},
		Compaction: CompactionConfig{
			WorkDir:             "",
			CheckInterval:       2 * time.Minute,
			MinPartitionSize:    64 * 1024 * 1024,
			MaxPartitionsPerKey: 100,
			TTLDays:             7,
			Backpressure: BackpressureConfig{
				MaxConcurrency:   8,
				MinConcurrency:   1,
				FailureThreshold: 0.05,
			},
		},
		Storage: StorageConfig{
			Type: "local",
			Path: "",
		},
		Manifest: ManifestConfig{
			Sharded:            true,
			ShardCount:         64,
			AutoShardThreshold: 50000,
		},
		WAL: WALConfig{
			Dir:            "./data/arkilian/wal",
			MaxSegmentSize: 67108864,
			FlushInterval:  500 * time.Millisecond,
			FlushBatchSize: 10000,
			RetentionTime:  1 * time.Hour,
		},
		Index: IndexConfig{
			Collection:      "events",
			CreateThreshold: 100,
			DropThreshold:   5,
			CheckInterval:   5 * time.Minute,
			MaxIndexes:      10,
			BucketCount:     64,
		},
		Cache: CacheConfig{
			NVMeDir:         "./data/arkilian/nvme",
			NVMeMaxBytes:    536870912000,
			PrefetchEnabled: true,
		},
		Router: RouterConfig{
			BufferSize: 1000,
		},
		V3: V3Config{
			ArkFormat: ArkFormatConfig{
				TargetFileSizeMB: 128,
				Compression:      "zstd",
				BloomFPR:         0.003,
				PGMEpsilon:       64,
			},
			SharedWAL: SharedWALConfig{
				Enabled:       false,
				RaftPeers:     nil,
				RaftDataDir:   "",
				SegmentSizeMB: 64,
				GRPCPort:      9090,
			},
			Compaction: V3CompactionConfig{
				HourlyCron:  "0 * * * *",
				Workers:     runtime.NumCPU() * 2,
				RAMBudgetMB: 2048,
				TmpDir:      "",
			},
			Catalog: CatalogConfig{
				ShardCount:  64,
				SnapshotDir: "",
				GRPCPort:    9091,
			},
			HotColumn: HotColumnConfig{
				CreateThreshold: 200,
				DropThreshold:   10,
				CheckInterval:   5 * time.Minute,
				MaxHotColumns:   10,
			},
			Migration: MigrationConfig{
				Phase:     "",
				Workers:   32,
				DualWrite: false,
			},
		},
	}
}

// Resolve resolves relative paths and sets defaults based on DataDir.
func (c *Config) Resolve() {
	if c.DataDir == "" {
		c.DataDir = "./data/arkilian"
	}

	// Resolve storage path
	if c.Storage.Path == "" {
		c.Storage.Path = filepath.Join(c.DataDir, "storage")
	}

	// Resolve ingest paths
	if c.Ingest.PartitionDir == "" {
		c.Ingest.PartitionDir = filepath.Join(c.DataDir, "partitions")
	}

	// Resolve query paths
	if c.Query.DownloadDir == "" {
		c.Query.DownloadDir = filepath.Join(c.DataDir, "downloads")
	}

	// Resolve compaction paths
	if c.Compaction.WorkDir == "" {
		c.Compaction.WorkDir = filepath.Join(c.DataDir, "compaction")
	}
}

// ManifestPath returns the path to the single manifest database (non-sharded mode).
func (c *Config) ManifestPath() string {
	return filepath.Join(c.DataDir, "manifest.db")
}

// ManifestDir returns the directory where manifest shard files are stored.
func (c *Config) ManifestDir() string {
	return c.DataDir
}

// Validate validates the configuration.
func (c *Config) Validate() error {
	switch c.Mode {
	case ModeAll, ModeIngest, ModeQuery, ModeCompact:
		// Valid modes
	default:
		return arkilianerrors.New(arkilianerrors.ErrCategoryValidation, "INVALID_MODE",
			fmt.Sprintf("invalid mode: %s (must be all, ingest, query, or compact)", c.Mode))
	}

	if c.DataDir == "" {
		return arkilianerrors.New(arkilianerrors.ErrCategoryValidation, "MISSING_DATA_DIR", "data_dir is required")
	}

	if c.Storage.Type != "local" && c.Storage.Type != "s3" {
		return arkilianerrors.New(arkilianerrors.ErrCategoryValidation, "INVALID_STORAGE_TYPE",
			fmt.Sprintf("invalid storage type: %s (must be local or s3)", c.Storage.Type))
	}

	if c.Storage.Type == "s3" && c.Storage.S3.Bucket == "" {
		return arkilianerrors.New(arkilianerrors.ErrCategoryValidation, "MISSING_S3_BUCKET", "s3.bucket is required when storage type is s3")
	}

	if c.Ingest.TargetPartitionSizeMB < 8 || c.Ingest.TargetPartitionSizeMB > 256 {
		return arkilianerrors.New(arkilianerrors.ErrCategoryValidation, "INVALID_PARTITION_SIZE",
			fmt.Sprintf("ingest.target_partition_size_mb must be between 8 and 256, got %d", c.Ingest.TargetPartitionSizeMB))
	}

	// Validate WAL config
	if c.WAL.Dir == "" {
		return arkilianerrors.New(arkilianerrors.ErrCategoryValidation, "INVALID_WAL_CONFIG", "wal.dir is required")
	}
	if c.WAL.FlushInterval < 100*time.Millisecond {
		return arkilianerrors.New(arkilianerrors.ErrCategoryValidation, "INVALID_WAL_CONFIG",
			fmt.Sprintf("wal.flush_interval must be >= 100ms, got %v", c.WAL.FlushInterval))
	}
	if c.WAL.MaxSegmentSize < 1024*1024 {
		return arkilianerrors.New(arkilianerrors.ErrCategoryValidation, "INVALID_WAL_CONFIG",
			fmt.Sprintf("wal.max_segment_size must be >= 1MB, got %d", c.WAL.MaxSegmentSize))
	}
	if c.WAL.FlushBatchSize < 1 {
		return arkilianerrors.New(arkilianerrors.ErrCategoryValidation, "INVALID_WAL_CONFIG",
			fmt.Sprintf("wal.flush_batch_size must be >= 1, got %d", c.WAL.FlushBatchSize))
	}
	if c.WAL.RetentionTime < 1*time.Minute {
		return arkilianerrors.New(arkilianerrors.ErrCategoryValidation, "INVALID_WAL_CONFIG",
			fmt.Sprintf("wal.retention_time must be >= 1m, got %v", c.WAL.RetentionTime))
	}

	// Validate Index config
	if c.Index.BucketCount <= 0 || c.Index.BucketCount > 65536 {
		return arkilianerrors.New(arkilianerrors.ErrCategoryValidation, "INVALID_INDEX_CONFIG",
			fmt.Sprintf("index.bucket_count must be > 0 and <= 65536, got %d", c.Index.BucketCount))
	}
	if c.Index.MaxIndexes < 1 {
		return arkilianerrors.New(arkilianerrors.ErrCategoryValidation, "INVALID_INDEX_CONFIG",
			fmt.Sprintf("index.max_indexes must be >= 1, got %d", c.Index.MaxIndexes))
	}
	if c.Index.CreateThreshold <= c.Index.DropThreshold {
		return arkilianerrors.New(arkilianerrors.ErrCategoryValidation, "INVALID_INDEX_CONFIG",
			fmt.Sprintf("index.create_threshold (%d) must be > drop_threshold (%d)", c.Index.CreateThreshold, c.Index.DropThreshold))
	}
	if c.Index.CheckInterval < 1*time.Second {
		return arkilianerrors.New(arkilianerrors.ErrCategoryValidation, "INVALID_INDEX_CONFIG",
			fmt.Sprintf("index.check_interval must be >= 1s, got %v", c.Index.CheckInterval))
	}

	// Validate Cache config
	if c.Cache.NVMeDir != "" && c.Cache.NVMeMaxBytes <= 0 {
		return arkilianerrors.New(arkilianerrors.ErrCategoryValidation, "INVALID_CACHE_CONFIG",
			fmt.Sprintf("cache.nvme_max_bytes must be > 0 when nvme_dir is set, got %d", c.Cache.NVMeMaxBytes))
	}

	// Validate Router config
	if c.Router.BufferSize <= 0 {
		return arkilianerrors.New(arkilianerrors.ErrCategoryValidation, "INVALID_ROUTER_CONFIG",
			fmt.Sprintf("router.buffer_size must be > 0, got %d", c.Router.BufferSize))
	}

	// Validate adaptive sizing config
	if err := c.validateAdaptiveSizing(); err != nil {
		return err
	}

	// Validate V3 config
	if err := c.validateV3Config(); err != nil {
		return err
	}

	return nil
}

// validateAdaptiveSizing checks the adaptive sizing configuration.
func (c *Config) validateAdaptiveSizing() error {
	as := c.Ingest.AdaptiveSizing
	if !as.Enabled {
		return nil
	}

	if as.MinSizeMB < 8 {
		return arkilianerrors.New(arkilianerrors.ErrCategoryValidation, "INVALID_ADAPTIVE_SIZING",
			fmt.Sprintf("ingest.adaptive_sizing.min_size_mb must be >= 8, got %d", as.MinSizeMB))
	}
	if as.MaxSizeMB > 256 {
		return arkilianerrors.New(arkilianerrors.ErrCategoryValidation, "INVALID_ADAPTIVE_SIZING",
			fmt.Sprintf("ingest.adaptive_sizing.max_size_mb must be <= 256, got %d", as.MaxSizeMB))
	}
	if as.MinSizeMB > as.MaxSizeMB {
		return arkilianerrors.New(arkilianerrors.ErrCategoryValidation, "INVALID_ADAPTIVE_SIZING",
			fmt.Sprintf("ingest.adaptive_sizing.min_size_mb (%d) must be <= max_size_mb (%d)", as.MinSizeMB, as.MaxSizeMB))
	}

	if len(as.Tiers) == 0 {
		return arkilianerrors.New(arkilianerrors.ErrCategoryValidation, "INVALID_ADAPTIVE_SIZING",
			"ingest.adaptive_sizing.tiers must have at least one entry")
	}

	prevThreshold := -1.0
	for i, tier := range as.Tiers {
		if tier.ThresholdGB < 0 {
			return arkilianerrors.New(arkilianerrors.ErrCategoryValidation, "INVALID_ADAPTIVE_SIZING",
				fmt.Sprintf("ingest.adaptive_sizing.tiers[%d].threshold_gb must be >= 0", i))
		}
		if tier.ThresholdGB <= prevThreshold && i > 0 {
			return arkilianerrors.New(arkilianerrors.ErrCategoryValidation, "INVALID_ADAPTIVE_SIZING",
				"ingest.adaptive_sizing.tiers must be sorted ascending by threshold_gb")
		}
		if tier.TargetSizeMB < as.MinSizeMB || tier.TargetSizeMB > as.MaxSizeMB {
			return arkilianerrors.New(arkilianerrors.ErrCategoryValidation, "INVALID_ADAPTIVE_SIZING",
				fmt.Sprintf("ingest.adaptive_sizing.tiers[%d].target_size_mb (%d) must be between %d and %d",
					i, tier.TargetSizeMB, as.MinSizeMB, as.MaxSizeMB))
		}
		prevThreshold = tier.ThresholdGB
	}

	return nil
}

// validateV3Config checks the V3 configuration.
func (c *Config) validateV3Config() error {
	v3 := c.V3
	
	// Only validate V3 config if V3 is being used (SharedWAL enabled or any V3 field set)
	// Check if any V3 field is non-zero (indicating user explicitly set it)
	v3InUse := v3.SharedWAL.Enabled ||
		v3.ArkFormat.TargetFileSizeMB != 0 ||
		v3.ArkFormat.Compression != "" ||
		v3.ArkFormat.BloomFPR != 0 ||
		v3.ArkFormat.PGMEpsilon != 0 ||
		len(v3.SharedWAL.RaftPeers) > 0 ||
		v3.SharedWAL.RaftDataDir != "" ||
		v3.SharedWAL.SegmentSizeMB != 0 ||
		v3.SharedWAL.GRPCPort != 0 ||
		v3.Compaction.HourlyCron != "" ||
		v3.Compaction.Workers != 0 ||
		v3.Compaction.RAMBudgetMB != 0 ||
		v3.Compaction.TmpDir != "" ||
		v3.Catalog.ShardCount != 0 ||
		v3.Catalog.SnapshotDir != "" ||
		v3.Catalog.GRPCPort != 0 ||
		v3.HotColumn.CreateThreshold != 0 ||
		v3.HotColumn.DropThreshold != 0 ||
		v3.HotColumn.CheckInterval != 0 ||
		v3.HotColumn.MaxHotColumns != 0 ||
		v3.Migration.Phase != "" ||
		v3.Migration.Workers != 0 ||
		v3.Migration.DualWrite
	
	if !v3InUse {
		// V3 not in use, skip validation
		return nil
	}
	
	// Validate SharedWAL config
	if v3.SharedWAL.Enabled {
		if len(v3.SharedWAL.RaftPeers) == 0 {
			return arkilianerrors.New(arkilianerrors.ErrCategoryValidation, "INVALID_V3_CONFIG",
				"v3.shared_wal.raft_peers must not be empty when shared_wal.enabled is true")
		}
		if v3.SharedWAL.RaftDataDir == "" {
			return arkilianerrors.New(arkilianerrors.ErrCategoryValidation, "INVALID_V3_CONFIG",
				"v3.shared_wal.raft_data_dir must not be empty when shared_wal.enabled is true")
		}
	}
	
	// Validate ArkFormat config (only if set)
	if v3.ArkFormat.TargetFileSizeMB != 0 {
		if v3.ArkFormat.TargetFileSizeMB != 64 && v3.ArkFormat.TargetFileSizeMB != 128 && v3.ArkFormat.TargetFileSizeMB != 256 {
			return arkilianerrors.New(arkilianerrors.ErrCategoryValidation, "INVALID_V3_CONFIG",
				fmt.Sprintf("v3.ark_format.target_file_size_mb must be 64, 128, or 256, got %d", v3.ArkFormat.TargetFileSizeMB))
		}
	}
	if v3.ArkFormat.BloomFPR != 0 {
		if v3.ArkFormat.BloomFPR <= 0 || v3.ArkFormat.BloomFPR >= 0.1 {
			return arkilianerrors.New(arkilianerrors.ErrCategoryValidation, "INVALID_V3_CONFIG",
				fmt.Sprintf("v3.ark_format.bloom_fpr must be > 0 and < 0.1, got %f", v3.ArkFormat.BloomFPR))
		}
	}
	if v3.ArkFormat.PGMEpsilon != 0 {
		if v3.ArkFormat.PGMEpsilon <= 0 {
			return arkilianerrors.New(arkilianerrors.ErrCategoryValidation, "INVALID_V3_CONFIG",
				fmt.Sprintf("v3.ark_format.pgm_epsilon must be > 0, got %d", v3.ArkFormat.PGMEpsilon))
		}
	}
	
	// Validate HotColumn config (only if set)
	if v3.HotColumn.CreateThreshold != 0 || v3.HotColumn.DropThreshold != 0 {
		if v3.HotColumn.CreateThreshold <= v3.HotColumn.DropThreshold {
			return arkilianerrors.New(arkilianerrors.ErrCategoryValidation, "INVALID_V3_CONFIG",
				fmt.Sprintf("v3.hot_column.create_threshold (%d) must be > drop_threshold (%d)", 
					v3.HotColumn.CreateThreshold, v3.HotColumn.DropThreshold))
		}
	}
	if v3.HotColumn.MaxHotColumns != 0 {
		if v3.HotColumn.MaxHotColumns <= 0 || v3.HotColumn.MaxHotColumns > 50 {
			return arkilianerrors.New(arkilianerrors.ErrCategoryValidation, "INVALID_V3_CONFIG",
				fmt.Sprintf("v3.hot_column.max_hot_columns must be > 0 and <= 50, got %d", v3.HotColumn.MaxHotColumns))
		}
	}
	
	// Validate Migration config
	if v3.Migration.Phase != "" && v3.Migration.Phase != "A" && v3.Migration.Phase != "B" && v3.Migration.Phase != "C" {
		return arkilianerrors.New(arkilianerrors.ErrCategoryValidation, "INVALID_V3_CONFIG",
			fmt.Sprintf("v3.migration.phase must be one of \"A\", \"B\", \"C\", or empty, got %s", v3.Migration.Phase))
	}
	
	return nil
}

// ShouldRunIngest returns true if the ingest service should run.
func (c *Config) ShouldRunIngest() bool {
	return c.Mode == ModeAll || c.Mode == ModeIngest
}

// ShouldRunQuery returns true if the query service should run.
func (c *Config) ShouldRunQuery() bool {
	return c.Mode == ModeAll || c.Mode == ModeQuery
}

// ShouldRunCompact returns true if the compaction service should run.
func (c *Config) ShouldRunCompact() bool {
	return c.Mode == ModeAll || c.Mode == ModeCompact
}

// LoadFromFile loads configuration from a YAML or JSON file.
func LoadFromFile(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	cfg := DefaultConfig()

	ext := strings.ToLower(filepath.Ext(path))
	switch ext {
	case ".yaml", ".yml":
		if err := yaml.Unmarshal(data, cfg); err != nil {
			return nil, fmt.Errorf("failed to parse YAML config: %w", err)
		}
	case ".json":
		if err := json.Unmarshal(data, cfg); err != nil {
			return nil, fmt.Errorf("failed to parse JSON config: %w", err)
		}
	default:
		return nil, fmt.Errorf("unsupported config file format: %s", ext)
	}

	return cfg, nil
}

// LoadFromEnv loads configuration from environment variables.
// Environment variables use the ARKILIAN_ prefix.
func LoadFromEnv(cfg *Config) {
	// Try loading .env file (ignore error if not present)
	_ = godotenv.Load()

	if v := os.Getenv("ARKILIAN_MODE"); v != "" {
		cfg.Mode = Mode(v)
	}
	if v := os.Getenv("ARKILIAN_DATA_DIR"); v != "" {
		cfg.DataDir = v
	}

	// HTTP configuration
	if v := os.Getenv("ARKILIAN_HTTP_INGEST_ADDR"); v != "" {
		cfg.HTTP.IngestAddr = v
	}
	if v := os.Getenv("ARKILIAN_HTTP_QUERY_ADDR"); v != "" {
		cfg.HTTP.QueryAddr = v
	}
	if v := os.Getenv("ARKILIAN_HTTP_COMPACT_ADDR"); v != "" {
		cfg.HTTP.CompactAddr = v
	}

	// gRPC configuration
	if v := os.Getenv("ARKILIAN_GRPC_ADDR"); v != "" {
		cfg.GRPC.Addr = v
	}
	if v := os.Getenv("ARKILIAN_GRPC_ENABLED"); v != "" {
		cfg.GRPC.Enabled = v == "true" || v == "1"
	}

	// Query configuration
	if v := os.Getenv("ARKILIAN_QUERY_CONCURRENCY"); v != "" {
		fmt.Sscanf(v, "%d", &cfg.Query.Concurrency)
	}
	if v := os.Getenv("ARKILIAN_QUERY_POOL_SIZE"); v != "" {
		fmt.Sscanf(v, "%d", &cfg.Query.PoolSize)
	}

	// Compaction configuration
	if v := os.Getenv("ARKILIAN_COMPACTION_CHECK_INTERVAL"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			cfg.Compaction.CheckInterval = d
		}
	}
	if v := os.Getenv("ARKILIAN_COMPACTION_TTL_DAYS"); v != "" {
		fmt.Sscanf(v, "%d", &cfg.Compaction.TTLDays)
	}

	// Storage configuration
	if v := os.Getenv("ARKILIAN_STORAGE_TYPE"); v != "" {
		cfg.Storage.Type = v
	}

	// Ingest configuration
	if v := os.Getenv("ARKILIAN_INGEST_TARGET_PARTITION_SIZE_MB"); v != "" {
		fmt.Sscanf(v, "%d", &cfg.Ingest.TargetPartitionSizeMB)
	}
	if v := os.Getenv("ARKILIAN_STORAGE_PATH"); v != "" {
		cfg.Storage.Path = v
	}
	if v := os.Getenv("ARKILIAN_S3_BUCKET"); v != "" {
		cfg.Storage.S3.Bucket = v
	}
	if v := os.Getenv("ARKILIAN_S3_REGION"); v != "" {
		cfg.Storage.S3.Region = v
	}
	if v := os.Getenv("ARKILIAN_S3_ENDPOINT"); v != "" {
		cfg.Storage.S3.Endpoint = v
	}

	// Manifest configuration
	if v := os.Getenv("ARKILIAN_MANIFEST_SHARDED"); v != "" {
		cfg.Manifest.Sharded = v == "true" || v == "1"
	}
	if v := os.Getenv("ARKILIAN_MANIFEST_SHARD_COUNT"); v != "" {
		fmt.Sscanf(v, "%d", &cfg.Manifest.ShardCount)
	}
	if v := os.Getenv("ARKILIAN_MANIFEST_AUTO_SHARD_THRESHOLD"); v != "" {
		fmt.Sscanf(v, "%d", &cfg.Manifest.AutoShardThreshold)
	}

	// Adaptive sizing configuration
	if v := os.Getenv("ARKILIAN_INGEST_ADAPTIVE_SIZING"); v != "" {
		cfg.Ingest.AdaptiveSizing.Enabled = v == "true" || v == "1"
	}
	if v := os.Getenv("ARKILIAN_INGEST_ADAPTIVE_MIN_SIZE_MB"); v != "" {
		fmt.Sscanf(v, "%d", &cfg.Ingest.AdaptiveSizing.MinSizeMB)
	}
	if v := os.Getenv("ARKILIAN_INGEST_ADAPTIVE_MAX_SIZE_MB"); v != "" {
		fmt.Sscanf(v, "%d", &cfg.Ingest.AdaptiveSizing.MaxSizeMB)
	}

	// Backpressure configuration
	if v := os.Getenv("ARKILIAN_COMPACTION_BP_MAX_CONCURRENCY"); v != "" {
		fmt.Sscanf(v, "%d", &cfg.Compaction.Backpressure.MaxConcurrency)
	}
	if v := os.Getenv("ARKILIAN_COMPACTION_BP_MIN_CONCURRENCY"); v != "" {
		fmt.Sscanf(v, "%d", &cfg.Compaction.Backpressure.MinConcurrency)
	}
	if v := os.Getenv("ARKILIAN_COMPACTION_BP_FAILURE_THRESHOLD"); v != "" {
		fmt.Sscanf(v, "%f", &cfg.Compaction.Backpressure.FailureThreshold)
	}

	// Bloom cache configuration
	if v := os.Getenv("ARKILIAN_QUERY_BLOOM_CACHE_SIZE_MB"); v != "" {
		fmt.Sscanf(v, "%d", &cfg.Query.BloomCacheSizeMB)
	}

	// WAL configuration
	if v := os.Getenv("ARKILIAN_WAL_DIR"); v != "" {
		cfg.WAL.Dir = v
	}
	if v := os.Getenv("ARKILIAN_WAL_FLUSH_INTERVAL"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			cfg.WAL.FlushInterval = d
		}
	}
	if v := os.Getenv("ARKILIAN_WAL_FLUSH_BATCH_SIZE"); v != "" {
		fmt.Sscanf(v, "%d", &cfg.WAL.FlushBatchSize)
	}
	if v := os.Getenv("ARKILIAN_WAL_MAX_SEGMENT_SIZE"); v != "" {
		fmt.Sscanf(v, "%d", &cfg.WAL.MaxSegmentSize)
	}
	if v := os.Getenv("ARKILIAN_WAL_RETENTION_TIME"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			cfg.WAL.RetentionTime = d
		}
	}

	// Index configuration
	if v := os.Getenv("ARKILIAN_INDEX_CREATE_THRESHOLD"); v != "" {
		fmt.Sscanf(v, "%d", &cfg.Index.CreateThreshold)
	}
	if v := os.Getenv("ARKILIAN_INDEX_DROP_THRESHOLD"); v != "" {
		fmt.Sscanf(v, "%d", &cfg.Index.DropThreshold)
	}
	if v := os.Getenv("ARKILIAN_INDEX_CHECK_INTERVAL"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			cfg.Index.CheckInterval = d
		}
	}
	if v := os.Getenv("ARKILIAN_INDEX_MAX_INDEXES"); v != "" {
		fmt.Sscanf(v, "%d", &cfg.Index.MaxIndexes)
	}
	if v := os.Getenv("ARKILIAN_INDEX_BUCKET_COUNT"); v != "" {
		fmt.Sscanf(v, "%d", &cfg.Index.BucketCount)
	}

	// Cache configuration
	if v := os.Getenv("ARKILIAN_CACHE_NVME_DIR"); v != "" {
		cfg.Cache.NVMeDir = v
	}
	if v := os.Getenv("ARKILIAN_CACHE_NVME_MAX_BYTES"); v != "" {
		fmt.Sscanf(v, "%d", &cfg.Cache.NVMeMaxBytes)
	}
	if v := os.Getenv("ARKILIAN_CACHE_PREFETCH_ENABLED"); v != "" {
		cfg.Cache.PrefetchEnabled = v == "true" || v == "1"
	}

	// Router configuration
	if v := os.Getenv("ARKILIAN_ROUTER_BUFFER_SIZE"); v != "" {
		fmt.Sscanf(v, "%d", &cfg.Router.BufferSize)
	}

	// V3 configuration
	if v := os.Getenv("ARKILIAN_V3_ENABLED"); v != "" {
		cfg.V3.ArkFormat.TargetFileSizeMB = 128 // Default when V3 enabled
		cfg.V3.SharedWAL.Enabled = v == "true" || v == "1"
	}
	
	// Shared WAL configuration
	if v := os.Getenv("ARKILIAN_V3_WAL_RAFT_PEERS"); v != "" {
		cfg.V3.SharedWAL.RaftPeers = strings.Split(v, ",")
	}
	if v := os.Getenv("ARKILIAN_V3_WAL_RAFT_DATA_DIR"); v != "" {
		cfg.V3.SharedWAL.RaftDataDir = v
	}
	if v := os.Getenv("ARKILIAN_V3_WAL_SEGMENT_SIZE_MB"); v != "" {
		fmt.Sscanf(v, "%d", &cfg.V3.SharedWAL.SegmentSizeMB)
	}
	if v := os.Getenv("ARKILIAN_V3_WAL_GRPC_PORT"); v != "" {
		fmt.Sscanf(v, "%d", &cfg.V3.SharedWAL.GRPCPort)
	}
	
	// ArkFormat configuration
	if v := os.Getenv("ARKILIAN_V3_ARKFORMAT_TARGET_FILE_SIZE_MB"); v != "" {
		fmt.Sscanf(v, "%d", &cfg.V3.ArkFormat.TargetFileSizeMB)
	}
	if v := os.Getenv("ARKILIAN_V3_ARKFORMAT_COMPRESSION"); v != "" {
		cfg.V3.ArkFormat.Compression = v
	}
	if v := os.Getenv("ARKILIAN_V3_ARKFORMAT_BLOOM_FPR"); v != "" {
		fmt.Sscanf(v, "%f", &cfg.V3.ArkFormat.BloomFPR)
	}
	if v := os.Getenv("ARKILIAN_V3_ARKFORMAT_PGM_EPSILON"); v != "" {
		fmt.Sscanf(v, "%d", &cfg.V3.ArkFormat.PGMEpsilon)
	}
	
	// Compaction configuration
	if v := os.Getenv("ARKILIAN_V3_COMPACTION_WORKERS"); v != "" {
		fmt.Sscanf(v, "%d", &cfg.V3.Compaction.Workers)
	}
	if v := os.Getenv("ARKILIAN_V3_COMPACTION_RAM_BUDGET_MB"); v != "" {
		fmt.Sscanf(v, "%d", &cfg.V3.Compaction.RAMBudgetMB)
	}
	if v := os.Getenv("ARKILIAN_V3_COMPACTION_TMP_DIR"); v != "" {
		cfg.V3.Compaction.TmpDir = v
	}
	if v := os.Getenv("ARKILIAN_V3_COMPACTION_HOURLY_CRON"); v != "" {
		cfg.V3.Compaction.HourlyCron = v
	}
	
	// Catalog configuration
	if v := os.Getenv("ARKILIAN_V3_CATALOG_SHARD_COUNT"); v != "" {
		fmt.Sscanf(v, "%d", &cfg.V3.Catalog.ShardCount)
	}
	if v := os.Getenv("ARKILIAN_V3_CATALOG_SNAPSHOT_DIR"); v != "" {
		cfg.V3.Catalog.SnapshotDir = v
	}
	if v := os.Getenv("ARKILIAN_V3_CATALOG_GRPC_PORT"); v != "" {
		fmt.Sscanf(v, "%d", &cfg.V3.Catalog.GRPCPort)
	}
	
	// Hot column configuration
	if v := os.Getenv("ARKILIAN_V3_HOT_COLUMN_CREATE_THRESHOLD"); v != "" {
		fmt.Sscanf(v, "%d", &cfg.V3.HotColumn.CreateThreshold)
	}
	if v := os.Getenv("ARKILIAN_V3_HOT_COLUMN_DROP_THRESHOLD"); v != "" {
		fmt.Sscanf(v, "%d", &cfg.V3.HotColumn.DropThreshold)
	}
	if v := os.Getenv("ARKILIAN_V3_HOT_COLUMN_CHECK_INTERVAL"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			cfg.V3.HotColumn.CheckInterval = d
		}
	}
	if v := os.Getenv("ARKILIAN_V3_HOT_COLUMN_MAX_COLUMNS"); v != "" {
		fmt.Sscanf(v, "%d", &cfg.V3.HotColumn.MaxHotColumns)
	}
	
	// Migration configuration
	if v := os.Getenv("ARKILIAN_V3_MIGRATION_PHASE"); v != "" {
		cfg.V3.Migration.Phase = v
	}
	if v := os.Getenv("ARKILIAN_V3_MIGRATION_WORKERS"); v != "" {
		fmt.Sscanf(v, "%d", &cfg.V3.Migration.Workers)
	}
	if v := os.Getenv("ARKILIAN_V3_MIGRATION_DUAL_WRITE"); v != "" {
		cfg.V3.Migration.DualWrite = v == "true" || v == "1"
	}

	// Map ARKILIAN_AWS_ credentials to standard AWS_ credentials for the SDK
	if v := os.Getenv("ARKILIAN_AWS_ACCESS_KEY_ID"); v != "" {
		os.Setenv("AWS_ACCESS_KEY_ID", v)
	}
	if v := os.Getenv("ARKILIAN_AWS_SECRET_ACCESS_KEY"); v != "" {
		os.Setenv("AWS_SECRET_ACCESS_KEY", v)
	}
}

// EnsureDirectories creates all required directories.
func (c *Config) EnsureDirectories() error {
	dirs := []string{
		c.DataDir,
		c.Storage.Path,
		c.Ingest.PartitionDir,
		c.Query.DownloadDir,
		c.Compaction.WorkDir,
	}

	for _, dir := range dirs {
		if dir == "" {
			continue
		}
		if err := os.MkdirAll(dir, 0755); err != nil {
			return fmt.Errorf("failed to create directory %s: %w", dir, err)
		}
	}

	return nil
}
