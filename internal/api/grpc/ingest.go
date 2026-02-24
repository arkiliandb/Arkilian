// Package grpc provides gRPC API handlers for the Arkilian system.
package grpc

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/arkilian/arkilian/api/proto"
	"github.com/arkilian/arkilian/internal/partition"
	"github.com/arkilian/arkilian/internal/wal"
	"github.com/arkilian/arkilian/pkg/types"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// IngestServer implements the IngestService gRPC server.
type IngestServer struct {
	proto.UnimplementedIngestServiceServer
	wal *wal.WAL
}

// NewIngestServer creates a new gRPC ingest server.
func NewIngestServer(walInstance *wal.WAL) *IngestServer {
	return &IngestServer{wal: walInstance}
}

// BatchIngest handles batch ingestion via gRPC.
func (s *IngestServer) BatchIngest(ctx context.Context, req *proto.IngestRequest) (*proto.IngestResponse, error) {
	requestID := extractRequestID(ctx)

	if req.PartitionKey == "" {
		return nil, status.Error(codes.InvalidArgument, "partition_key is required")
	}

	if len(req.Rows) == 0 {
		return nil, status.Error(codes.InvalidArgument, "rows must not be empty")
	}

	rows, err := convertProtoRows(req.Rows)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid row data: %v", err)
	}

	entry := &wal.Entry{
		PartitionKey: req.PartitionKey,
		Rows:         rows,
		Schema:       partition.DefaultSchema(),
		Timestamp:    time.Now().UnixNano(),
	}
	lsn, err := s.wal.Append(entry)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "WAL write failed: %v", err)
	}

	return &proto.IngestResponse{
		PartitionId: "",
		RowCount:    int64(len(rows)),
		SizeBytes:   0,
		RequestId:   requestID,
		Lsn:         lsn,
	}, nil
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
