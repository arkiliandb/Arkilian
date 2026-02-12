package partition

import (
	"context"
	"log"
)

// VolumeQuerier provides the total data volume for a partition key.
// This interface breaks the dependency on the manifest package.
type VolumeQuerier interface {
	// TotalVolumeBytes returns the sum of SizeBytes for all active partitions
	// with the given partition key.
	TotalVolumeBytes(ctx context.Context, partitionKey string) (int64, error)
}

// AdaptiveSizer computes the optimal partition target size for a given partition
// key based on the total data volume already stored for that key. This reduces
// S3 object count and LIST costs at scale by producing fewer, larger partitions
// as data volume grows.
type AdaptiveSizer struct {
	enabled  bool
	minBytes int64
	maxBytes int64
	tiers    []sizingTier
	querier  VolumeQuerier
}

// sizingTier maps a cumulative volume threshold to a target partition size.
type sizingTier struct {
	thresholdBytes int64
	targetBytes    int64
}

// AdaptiveSizerOption configures an AdaptiveSizer.
type AdaptiveSizerOption func(*AdaptiveSizer)

// WithTierMB adds a sizing tier: when total volume for a key reaches thresholdGB,
// the target partition size becomes targetMB.
func WithTierMB(thresholdGB float64, targetMB int) AdaptiveSizerOption {
	return func(s *AdaptiveSizer) {
		s.tiers = append(s.tiers, sizingTier{
			thresholdBytes: int64(thresholdGB * 1024 * 1024 * 1024),
			targetBytes:    int64(targetMB) * 1024 * 1024,
		})
	}
}

// WithBoundsMB sets the min/max partition size in MB.
func WithBoundsMB(minMB, maxMB int) AdaptiveSizerOption {
	return func(s *AdaptiveSizer) {
		s.minBytes = int64(minMB) * 1024 * 1024
		s.maxBytes = int64(maxMB) * 1024 * 1024
	}
}

// NewAdaptiveSizer creates a new adaptive sizer. When enabled is false,
// TargetSizeBytes always returns the fallbackMB size.
func NewAdaptiveSizer(enabled bool, fallbackMB int, querier VolumeQuerier, opts ...AdaptiveSizerOption) *AdaptiveSizer {
	s := &AdaptiveSizer{
		enabled:  enabled,
		minBytes: 16 * 1024 * 1024,
		maxBytes: 256 * 1024 * 1024,
		querier:  querier,
	}

	for _, opt := range opts {
		opt(s)
	}

	// If no tiers were provided, use defaults optimized for scale.
	if len(s.tiers) == 0 {
		s.tiers = []sizingTier{
			{thresholdBytes: 0, targetBytes: int64(fallbackMB) * 1024 * 1024},
			{thresholdBytes: 1 * 1024 * 1024 * 1024, targetBytes: 64 * 1024 * 1024},
			{thresholdBytes: 10 * 1024 * 1024 * 1024, targetBytes: 128 * 1024 * 1024},
			{thresholdBytes: 100 * 1024 * 1024 * 1024, targetBytes: 256 * 1024 * 1024},
		}
	}

	return s
}

// TargetSizeBytes returns the recommended partition size in bytes for the given
// partition key. It queries the volume querier for the current volume and
// selects the appropriate tier. On error it falls back to the minimum size.
func (s *AdaptiveSizer) TargetSizeBytes(ctx context.Context, partitionKey string) int64 {
	if !s.enabled || s.querier == nil {
		return s.tiers[0].targetBytes
	}

	volumeBytes, err := s.querier.TotalVolumeBytes(ctx, partitionKey)
	if err != nil {
		log.Printf("adaptive: failed to query volume for key %s, using min size: %v", partitionKey, err)
		return s.minBytes
	}

	// Walk tiers — pick the highest tier whose threshold is met.
	target := s.tiers[0].targetBytes
	for _, tier := range s.tiers {
		if volumeBytes >= tier.thresholdBytes {
			target = tier.targetBytes
		}
	}

	// Clamp to bounds.
	if target < s.minBytes {
		target = s.minBytes
	}
	if target > s.maxBytes {
		target = s.maxBytes
	}

	return target
}
