package types

// PartitionKeyStrategy defines the strategy for routing data to partitions.
type PartitionKeyStrategy string

const (
	// StrategyTime routes data based on time (YYYYMMDD format)
	StrategyTime PartitionKeyStrategy = "time"

	// StrategyTenant routes data based on tenant ID for multi-tenant isolation
	StrategyTenant PartitionKeyStrategy = "tenant"

	// StrategyHash routes data using hash-based partitioning (modulo N)
	StrategyHash PartitionKeyStrategy = "hash"
)

// PartitionKey represents a partition key used to route data.
type PartitionKey struct {
	// Strategy is the partitioning strategy (time, tenant, or hash)
	Strategy PartitionKeyStrategy `json:"strategy"`

	// Value is the computed partition key value
	Value string `json:"value"`
}

// PartitionKeyConfig holds configuration for partition key generation.
type PartitionKeyConfig struct {
	// Strategy is the partitioning strategy to use
	Strategy PartitionKeyStrategy `json:"strategy"`

	// TargetSizeMB is the target partition size in megabytes (8-16MB)
	TargetSizeMB int `json:"target_size_mb"`

	// HashModulo is the modulo value for hash-based partitioning (only used with StrategyHash)
	HashModulo int `json:"hash_modulo,omitempty"`
}
