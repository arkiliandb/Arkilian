// Package grpc provides gRPC API handlers for the Arkilian system.
package grpc

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/arkilian/arkilian/api/proto"
	"github.com/arkilian/arkilian/internal/bloom"
	"github.com/arkilian/arkilian/internal/manifest"
	"github.com/arkilian/arkilian/internal/partition"
	"github.com/arkilian/arkilian/internal/storage"
	"github.com/arkilian/arkilian/pkg/types"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// IngestServer implements the IngestService gRPC server.
type IngestServer struct {
	proto.UnimplementedIngestServiceServer
	builder partition.PartitionBuilder
	metaGen *partition.MetadataGenerator
	catalog manifest.Catalog
	storage storage.ObjectStorage
}

// NewIngestServer creates a new gRPC ingest server.
func NewIngestServer(
	builder partition.PartitionBuilder,
	metaGen *partition.MetadataGenerator,
	catalog manifest.Catalog,
	store storage.ObjectStorage,
) *IngestServer {
	return &IngestServer{
		builder: builder,
		metaGen: metaGen,
		catalog: catalog,
		storage: store,
	}
}

// BatchIngest handles batch ingestion via gRPC.
func (s *IngestServer) BatchIngest(ctx context.Context, req *proto.IngestRequest) (*proto.IngestResponse, error) {
	// Generate or extract request ID
	requestID := extractRequestID(ctx)

	// Validate partition_key
	if req.PartitionKey == "" {
		return nil, status.Error(codes.InvalidArgument, "partition_key is required")
	}

	// Validate rows
	if len(req.Rows) == 0 {
		return nil, status.Error(codes.InvalidArgument, "rows must not be empty")
	}

	// Convert proto rows to typed rows
	rows, err := convertProtoRows(req.Rows)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid row data: %v", err)
	}

	// Build partition
	key := types.PartitionKey{
		Strategy: types.StrategyTime,
		Value:    req.PartitionKey,
	}

	info, err := s.builder.Build(ctx, rows, key)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to build partition: %v", err)
	}

	// Generate metadata sidecar
	sidecar, err := s.metaGen.Generate(info, rows)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to generate metadata: %v", err)
	}
	metaPath := partition.GenerateMetadataPath(info.SQLitePath)
	if err := sidecar.WriteToFile(metaPath); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to write metadata: %v", err)
	}
	info.MetadataPath = metaPath

	// Upload to object storage
	objectPath := fmt.Sprintf("partitions/%s/%s.sqlite", req.PartitionKey, info.PartitionID)
	metaObjectPath := fmt.Sprintf("partitions/%s/%s.meta.json", req.PartitionKey, info.PartitionID)

	if err := s.uploadPartition(ctx, info, objectPath, metaObjectPath); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to upload partition: %v", err)
	}

	// Register in manifest catalog
	if err := s.registerPartition(ctx, info, objectPath, req.IdempotencyKey); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to register partition: %v", err)
	}

	// Update zone maps with bloom filters from this partition (best-effort)
	s.updateZoneMaps(ctx, req.PartitionKey, sidecar)

	return &proto.IngestResponse{
		PartitionId: info.PartitionID,
		RowCount:    info.RowCount,
		SizeBytes:   info.SizeBytes,
		RequestId:   requestID,
	}, nil
}

// uploadPartition uploads the SQLite file and metadata sidecar to object storage.
func (s *IngestServer) uploadPartition(ctx context.Context, info *partition.PartitionInfo, objectPath, metaObjectPath string) error {
	if _, err := s.storage.UploadMultipart(ctx, info.SQLitePath, objectPath); err != nil {
		return fmt.Errorf("failed to upload sqlite file: %w", err)
	}

	if err := s.storage.Upload(ctx, info.MetadataPath, metaObjectPath); err != nil {
		return fmt.Errorf("failed to upload metadata: %w", err)
	}

	return nil
}

// registerPartition registers the partition in the manifest catalog.
func (s *IngestServer) registerPartition(ctx context.Context, info *partition.PartitionInfo, objectPath, idempotencyKey string) error {
	if idempotencyKey != "" {
		_, err := s.catalog.RegisterPartitionWithIdempotencyKey(ctx, info, objectPath, idempotencyKey)
		return err
	}
	return s.catalog.RegisterPartition(ctx, info, objectPath)
}

// updateZoneMaps merges bloom filters from the metadata sidecar into zone maps.
// This is best-effort — zone map update failures don't fail the ingest request.
func (s *IngestServer) updateZoneMaps(ctx context.Context, partitionKey string, sidecar *partition.MetadataSidecar) {
	if sidecar == nil || len(sidecar.BloomFilters) == 0 {
		return
	}

	type zoneMapUpdater interface {
		UpdateZoneMapsFromMetadata(ctx context.Context, partitionKey string, bloomFilters map[string]*bloom.BloomFilter, distinctCounts map[string]int) error
	}

	updater, ok := s.catalog.(zoneMapUpdater)
	if !ok {
		return
	}

	filters := make(map[string]*bloom.BloomFilter, len(sidecar.BloomFilters))
	distinctCounts := make(map[string]int, len(sidecar.BloomFilters))
	for col, meta := range sidecar.BloomFilters {
		bf, err := bloom.DeserializeFromBase64(meta.Base64Data)
		if err != nil {
			log.Printf("grpc ingest: failed to deserialize bloom filter for zone map update (%s): %v", col, err)
			continue
		}
		filters[col] = bf
		distinctCounts[col] = meta.DistinctCount
	}

	if err := updater.UpdateZoneMapsFromMetadata(ctx, partitionKey, filters, distinctCounts); err != nil {
		log.Printf("grpc ingest: failed to update zone maps for key %s: %v", partitionKey, err)
	}
}

// convertProtoRows converts proto Row messages to typed Row structs.
func convertProtoRows(protoRows []*proto.Row) ([]types.Row, error) {
	rows := make([]types.Row, 0, len(protoRows))

	for i, pr := range protoRows {
		// Validate required fields
		if pr.TenantId == "" {
			return nil, fmt.Errorf("row %d: tenant_id is required", i)
		}
		if pr.EventType == "" {
			return nil, fmt.Errorf("row %d: event_type is required", i)
		}

		row := types.Row{
			EventID:   pr.EventId,
			TenantID:  pr.TenantId,
			UserID:    pr.UserId,
			EventTime: pr.EventTime,
			EventType: pr.EventType,
		}

		// Parse payload from JSON bytes
		if len(pr.Payload) > 0 {
			var payload map[string]interface{}
			if err := json.Unmarshal(pr.Payload, &payload); err != nil {
				return nil, fmt.Errorf("row %d: invalid payload JSON: %v", i, err)
			}
			row.Payload = payload
		} else {
			row.Payload = make(map[string]interface{})
		}

		rows = append(rows, row)
	}

	return rows, nil
}

// extractRequestID extracts or generates a request ID from the gRPC context.
func extractRequestID(ctx context.Context) string {
	// Try to extract from gRPC metadata
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if ids := md.Get("x-request-id"); len(ids) > 0 {
			return ids[0]
		}
	}
	// Generate a new request ID if not provided
	return uuid.New().String()
}
