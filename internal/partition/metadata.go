package partition

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/arkilian/arkilian/internal/bloom"
	"github.com/arkilian/arkilian/pkg/types"
)

// MetadataSidecar represents the .meta.json file structure.
type MetadataSidecar struct {
	PartitionID   string                       `json:"partition_id"`
	SchemaVersion int                          `json:"schema_version"`
	Stats         PartitionStats               `json:"stats"`
	BloomFilters  map[string]*BloomFilterMeta  `json:"bloom_filters"`
	CreatedAt     int64                        `json:"created_at"`
}

// PartitionStats holds partition-level statistics.
type PartitionStats struct {
	RowCount     int64   `json:"row_count"`
	SizeBytes    int64   `json:"size_bytes"`
	MinEventTime *int64  `json:"min_event_time,omitempty"`
	MaxEventTime *int64  `json:"max_event_time,omitempty"`
	MinUserID    *int64  `json:"min_user_id,omitempty"`
	MaxUserID    *int64  `json:"max_user_id,omitempty"`
	MinTenantID  *string `json:"min_tenant_id,omitempty"`
	MaxTenantID  *string `json:"max_tenant_id,omitempty"`
}

// BloomFilterMeta holds bloom filter metadata and data.
type BloomFilterMeta struct {
	Algorithm  string `json:"algorithm"`
	NumBits    int    `json:"num_bits"`
	NumHashes  int    `json:"num_hashes"`
	Base64Data string `json:"base64_data"`
}

// MetadataGenerator generates metadata sidecars for partitions.
type MetadataGenerator struct {
	targetFPR float64 // Target false positive rate for bloom filters
}

// NewMetadataGenerator creates a new metadata generator.
func NewMetadataGenerator() *MetadataGenerator {
	return &MetadataGenerator{
		targetFPR: 0.01, // 1% false positive rate as per requirements
	}
}

// Generate creates a metadata sidecar for the given partition info and rows.
func (g *MetadataGenerator) Generate(info *PartitionInfo, rows []types.Row) (*MetadataSidecar, error) {
	// Build bloom filters for high-cardinality columns
	bloomFilters, err := g.buildBloomFilters(rows)
	if err != nil {
		return nil, fmt.Errorf("metadata: failed to build bloom filters: %w", err)
	}

	// Extract min/max stats
	var minEventTime, maxEventTime, minUserID, maxUserID *int64
	var minTenantID, maxTenantID *string

	if stat, ok := info.MinMaxStats["event_time"]; ok {
		if v, ok := stat.Min.(int64); ok {
			minEventTime = &v
		}
		if v, ok := stat.Max.(int64); ok {
			maxEventTime = &v
		}
	}

	if stat, ok := info.MinMaxStats["user_id"]; ok {
		if v, ok := stat.Min.(int64); ok {
			minUserID = &v
		}
		if v, ok := stat.Max.(int64); ok {
			maxUserID = &v
		}
	}

	if stat, ok := info.MinMaxStats["tenant_id"]; ok {
		if v, ok := stat.Min.(string); ok {
			minTenantID = &v
		}
		if v, ok := stat.Max.(string); ok {
			maxTenantID = &v
		}
	}

	sidecar := &MetadataSidecar{
		PartitionID:   info.PartitionID,
		SchemaVersion: info.SchemaVersion,
		Stats: PartitionStats{
			RowCount:     info.RowCount,
			SizeBytes:    info.SizeBytes,
			MinEventTime: minEventTime,
			MaxEventTime: maxEventTime,
			MinUserID:    minUserID,
			MaxUserID:    maxUserID,
			MinTenantID:  minTenantID,
			MaxTenantID:  maxTenantID,
		},
		BloomFilters: bloomFilters,
		CreatedAt:    info.CreatedAt.Unix(),
	}

	return sidecar, nil
}


// buildBloomFilters creates bloom filters for tenant_id and user_id columns.
func (g *MetadataGenerator) buildBloomFilters(rows []types.Row) (map[string]*BloomFilterMeta, error) {
	if len(rows) == 0 {
		return nil, nil
	}

	filters := make(map[string]*BloomFilterMeta)

	// Build bloom filter for tenant_id
	tenantFilter := bloom.NewWithEstimates(len(rows), g.targetFPR)
	for _, row := range rows {
		tenantFilter.Add([]byte(row.TenantID))
	}
	tenantMeta, err := g.serializeBloomFilter(tenantFilter)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize tenant_id bloom filter: %w", err)
	}
	filters["tenant_id"] = tenantMeta

	// Build bloom filter for user_id
	userFilter := bloom.NewWithEstimates(len(rows), g.targetFPR)
	for _, row := range rows {
		// Convert user_id to bytes for bloom filter
		userIDBytes := int64ToBytes(row.UserID)
		userFilter.Add(userIDBytes)
	}
	userMeta, err := g.serializeBloomFilter(userFilter)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize user_id bloom filter: %w", err)
	}
	filters["user_id"] = userMeta

	return filters, nil
}

// serializeBloomFilter converts a bloom filter to BloomFilterMeta.
func (g *MetadataGenerator) serializeBloomFilter(filter *bloom.BloomFilter) (*BloomFilterMeta, error) {
	base64Data, err := filter.SerializeToBase64()
	if err != nil {
		return nil, err
	}

	return &BloomFilterMeta{
		Algorithm:  "murmur3_128",
		NumBits:    filter.NumBits(),
		NumHashes:  filter.NumHashes(),
		Base64Data: base64Data,
	}, nil
}

// int64ToBytes converts an int64 to a byte slice (big-endian).
func int64ToBytes(v int64) []byte {
	b := make([]byte, 8)
	b[0] = byte(v >> 56)
	b[1] = byte(v >> 48)
	b[2] = byte(v >> 40)
	b[3] = byte(v >> 32)
	b[4] = byte(v >> 24)
	b[5] = byte(v >> 16)
	b[6] = byte(v >> 8)
	b[7] = byte(v)
	return b
}

// WriteToFile writes the metadata sidecar to a JSON file.
func (s *MetadataSidecar) WriteToFile(path string) error {
	data, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return fmt.Errorf("metadata: failed to marshal sidecar: %w", err)
	}

	if err := os.WriteFile(path, data, 0644); err != nil {
		return fmt.Errorf("metadata: failed to write sidecar file: %w", err)
	}

	return nil
}

// ReadFromFile reads a metadata sidecar from a JSON file.
func ReadMetadataFromFile(path string) (*MetadataSidecar, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("metadata: failed to read sidecar file: %w", err)
	}

	var sidecar MetadataSidecar
	if err := json.Unmarshal(data, &sidecar); err != nil {
		return nil, fmt.Errorf("metadata: failed to unmarshal sidecar: %w", err)
	}

	return &sidecar, nil
}

// GenerateMetadataPath returns the metadata file path for a given SQLite path.
func GenerateMetadataPath(sqlitePath string) string {
	dir := filepath.Dir(sqlitePath)
	base := filepath.Base(sqlitePath)
	ext := filepath.Ext(base)
	name := base[:len(base)-len(ext)]
	return filepath.Join(dir, name+".meta.json")
}

// GenerateAndWrite generates metadata and writes it to a file.
func (g *MetadataGenerator) GenerateAndWrite(info *PartitionInfo, rows []types.Row) (string, error) {
	sidecar, err := g.Generate(info, rows)
	if err != nil {
		return "", err
	}

	metadataPath := GenerateMetadataPath(info.SQLitePath)
	if err := sidecar.WriteToFile(metadataPath); err != nil {
		return "", err
	}

	return metadataPath, nil
}

// ToJSON serializes the metadata sidecar to JSON bytes.
func (s *MetadataSidecar) ToJSON() ([]byte, error) {
	return json.Marshal(s)
}

// FromJSON deserializes a metadata sidecar from JSON bytes.
func FromJSON(data []byte) (*MetadataSidecar, error) {
	var sidecar MetadataSidecar
	if err := json.Unmarshal(data, &sidecar); err != nil {
		return nil, fmt.Errorf("metadata: failed to unmarshal sidecar: %w", err)
	}
	return &sidecar, nil
}

// CreatedAtTime returns the creation time as time.Time.
func (s *MetadataSidecar) CreatedAtTime() time.Time {
	return time.Unix(s.CreatedAt, 0)
}
