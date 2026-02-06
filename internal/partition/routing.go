package partition

import (
	"fmt"
	"hash/fnv"
	"time"

	"github.com/arkilian/arkilian/pkg/types"
)

// Router determines the partition key for a row based on the configured strategy.
type Router struct {
	config types.PartitionKeyConfig
}

// NewRouter creates a new partition key router with the given configuration.
func NewRouter(config types.PartitionKeyConfig) (*Router, error) {
	if err := validateConfig(config); err != nil {
		return nil, err
	}
	return &Router{config: config}, nil
}

// RouteRow computes the partition key for a single row.
func (r *Router) RouteRow(row types.Row) (types.PartitionKey, error) {
	var value string
	var err error

	switch r.config.Strategy {
	case types.StrategyTime:
		value = routeByTime(row.EventTime)
	case types.StrategyTenant:
		value, err = routeByTenant(row.TenantID)
	case types.StrategyHash:
		value = routeByHash(row.UserID, r.config.HashModulo)
	default:
		return types.PartitionKey{}, fmt.Errorf("routing: unsupported strategy %q", r.config.Strategy)
	}

	if err != nil {
		return types.PartitionKey{}, err
	}

	return types.PartitionKey{
		Strategy: r.config.Strategy,
		Value:    value,
	}, nil
}

// RouteRows groups rows by their computed partition key.
func (r *Router) RouteRows(rows []types.Row) (map[string][]types.Row, error) {
	groups := make(map[string][]types.Row)
	for _, row := range rows {
		key, err := r.RouteRow(row)
		if err != nil {
			return nil, fmt.Errorf("routing: failed to route row: %w", err)
		}
		groups[key.Value] = append(groups[key.Value], row)
	}
	return groups, nil
}

// routeByTime produces a YYYYMMDD partition key from a Unix nanosecond timestamp.
func routeByTime(eventTimeNanos int64) string {
	t := time.Unix(0, eventTimeNanos).UTC()
	return t.Format("20060102")
}

// routeByTenant uses the tenant ID directly as the partition key.
func routeByTenant(tenantID string) (string, error) {
	if tenantID == "" {
		return "", fmt.Errorf("routing: tenant_id must not be empty for tenant-based routing")
	}
	return tenantID, nil
}

// routeByHash computes a hash-based partition key using FNV-1a on the user ID.
func routeByHash(userID int64, modulo int) string {
	h := fnv.New64a()
	// Write the int64 as 8 bytes (big-endian)
	b := [8]byte{
		byte(userID >> 56), byte(userID >> 48), byte(userID >> 40), byte(userID >> 32),
		byte(userID >> 24), byte(userID >> 16), byte(userID >> 8), byte(userID),
	}
	h.Write(b[:])
	bucket := int(h.Sum64() % uint64(modulo))
	return fmt.Sprintf("hash_%d", bucket)
}

// validateConfig checks that the routing configuration is valid.
func validateConfig(config types.PartitionKeyConfig) error {
	switch config.Strategy {
	case types.StrategyTime, types.StrategyTenant:
		// No extra config needed
	case types.StrategyHash:
		if config.HashModulo <= 0 {
			return fmt.Errorf("routing: hash_modulo must be > 0, got %d", config.HashModulo)
		}
	default:
		return fmt.Errorf("routing: unsupported strategy %q", config.Strategy)
	}
	return nil
}
