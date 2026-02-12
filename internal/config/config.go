// Package config provides unified configuration for all Arkilian services.
package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
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
}

// ManifestConfig holds manifest catalog configuration.
type ManifestConfig struct {
	// Sharded enables sharded manifest mode (multiple SQLite files instead of one).
	// When false (default), a single manifest.db is used for backward compatibility.
	Sharded bool `json:"sharded" yaml:"sharded"`

	// ShardCount is the number of manifest shards (default: 16). Only used when Sharded=true.
	ShardCount int `json:"shard_count" yaml:"shard_count"`
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
			WriteTimeout: 60 * time.Second,
			IdleTimeout:  120 * time.Second,
		},
		GRPC: GRPCConfig{
			Addr:    ":9090",
			Enabled: true,
		},
		Ingest: IngestConfig{
			PartitionDir:          "",
			TargetPartitionSizeMB: 32,
			AdaptiveSizing: AdaptiveSizingConfig{
				Enabled:   true,
				MinSizeMB: 16,
				MaxSizeMB: 256,
				Tiers: []SizingTier{
					{ThresholdGB: 0, TargetSizeMB: 32},
					{ThresholdGB: 1, TargetSizeMB: 64},
					{ThresholdGB: 10, TargetSizeMB: 128},
					{ThresholdGB: 100, TargetSizeMB: 256},
				},
			},
		},
		Query: QueryConfig{
			DownloadDir:          "",
			Concurrency:          16,
			PoolSize:             200,
			MaxPreloadPartitions: 2000,
			BloomCacheSizeMB:     1024,
		},
		Compaction: CompactionConfig{
			WorkDir:             "",
			CheckInterval:       5 * time.Minute,
			MinPartitionSize:    32 * 1024 * 1024,
			MaxPartitionsPerKey: 50,
			TTLDays:             7,
			Backpressure: BackpressureConfig{
				MaxConcurrency:   4,
				MinConcurrency:   1,
				FailureThreshold: 0.10,
			},
		},
		Storage: StorageConfig{
			Type: "local",
			Path: "",
		},
		Manifest: ManifestConfig{
			Sharded:    true,
			ShardCount: 32,
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
		return fmt.Errorf("invalid mode: %s (must be all, ingest, query, or compact)", c.Mode)
	}

	if c.DataDir == "" {
		return fmt.Errorf("data_dir is required")
	}

	if c.Storage.Type != "local" && c.Storage.Type != "s3" {
		return fmt.Errorf("invalid storage type: %s (must be local or s3)", c.Storage.Type)
	}

	if c.Storage.Type == "s3" && c.Storage.S3.Bucket == "" {
		return fmt.Errorf("s3.bucket is required when storage type is s3")
	}

	if c.Ingest.TargetPartitionSizeMB < 8 || c.Ingest.TargetPartitionSizeMB > 256 {
		return fmt.Errorf("ingest.target_partition_size_mb must be between 8 and 256, got %d", c.Ingest.TargetPartitionSizeMB)
	}

	// Validate adaptive sizing config
	if err := c.validateAdaptiveSizing(); err != nil {
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
		return fmt.Errorf("ingest.adaptive_sizing.min_size_mb must be >= 8, got %d", as.MinSizeMB)
	}
	if as.MaxSizeMB > 256 {
		return fmt.Errorf("ingest.adaptive_sizing.max_size_mb must be <= 256, got %d", as.MaxSizeMB)
	}
	if as.MinSizeMB > as.MaxSizeMB {
		return fmt.Errorf("ingest.adaptive_sizing.min_size_mb (%d) must be <= max_size_mb (%d)", as.MinSizeMB, as.MaxSizeMB)
	}

	if len(as.Tiers) == 0 {
		return fmt.Errorf("ingest.adaptive_sizing.tiers must have at least one entry")
	}

	prevThreshold := -1.0
	for i, tier := range as.Tiers {
		if tier.ThresholdGB < 0 {
			return fmt.Errorf("ingest.adaptive_sizing.tiers[%d].threshold_gb must be >= 0", i)
		}
		if tier.ThresholdGB <= prevThreshold && i > 0 {
			return fmt.Errorf("ingest.adaptive_sizing.tiers must be sorted ascending by threshold_gb")
		}
		if tier.TargetSizeMB < as.MinSizeMB || tier.TargetSizeMB > as.MaxSizeMB {
			return fmt.Errorf("ingest.adaptive_sizing.tiers[%d].target_size_mb (%d) must be between %d and %d",
				i, tier.TargetSizeMB, as.MinSizeMB, as.MaxSizeMB)
		}
		prevThreshold = tier.ThresholdGB
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
