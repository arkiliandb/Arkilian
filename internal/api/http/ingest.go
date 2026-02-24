package http

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/arkilian/arkilian/internal/manifest"
	"github.com/arkilian/arkilian/internal/partition"
	"github.com/arkilian/arkilian/internal/schema"
	"github.com/arkilian/arkilian/internal/storage"
	"github.com/arkilian/arkilian/internal/wal"
	"github.com/arkilian/arkilian/pkg/types"
)

// IngestRequest represents a batch ingest request.
type IngestRequest struct {
	PartitionKey   string                   `json:"partition_key"`
	Rows           []map[string]interface{} `json:"rows"`
	IdempotencyKey string                   `json:"idempotency_key,omitempty"`
}

// IngestResponse represents the ingest response when WAL is enabled.
type IngestResponse struct {
	LSN       uint64 `json:"lsn"`
	RowCount  int64  `json:"row_count"`
	RequestID string `json:"request_id"`
	Status    string `json:"status"`
}

// IngestHandler handles POST /v1/ingest requests.
type IngestHandler struct {
	builder      partition.PartitionBuilder
	metaGen      *partition.MetadataGenerator
	catalog      manifest.Catalog
	storage      storage.ObjectStorage
	wal          *wal.WAL
	materializer *schema.Materializer
}

// NewIngestHandler creates a new ingest handler.
func NewIngestHandler(
	builder partition.PartitionBuilder,
	metaGen *partition.MetadataGenerator,
	catalog manifest.Catalog,
	store storage.ObjectStorage,
	walInstance *wal.WAL,
	materializer *schema.Materializer,
) *IngestHandler {
	return &IngestHandler{
		builder:      builder,
		metaGen:      metaGen,
		catalog:      catalog,
		storage:      store,
		wal:          walInstance,
		materializer: materializer,
	}
}

// ServeHTTP handles the ingest HTTP request.
func (h *IngestHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	requestID := GetRequestID(r.Context())

	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed", requestID)
		return
	}

	// Parse request body
	var req IngestRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid request body: %v", err), requestID)
		return
	}

	// Validate partition_key
	if req.PartitionKey == "" {
		writeError(w, http.StatusBadRequest, "partition_key is required", requestID)
		return
	}

	// Validate rows
	if len(req.Rows) == 0 {
		writeError(w, http.StatusBadRequest, "rows must not be empty", requestID)
		return
	}

	// Convert map rows to typed rows
	rows, err := convertRows(req.Rows)
	if err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid row data: %v", err), requestID)
		return
	}

	// Append to WAL and return LSN immediately
	entry := &wal.Entry{
		PartitionKey: req.PartitionKey,
		Rows:         rows,
		Schema:       partition.DefaultSchema(),
		Timestamp:    time.Now().UnixNano(),
	}
	lsn, err := h.wal.Append(entry)
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("WAL write failed: %v", err), requestID)
		return
	}
	resp := IngestResponse{
		LSN:       lsn,
		RowCount:  int64(len(rows)),
		RequestID: requestID,
		Status:    "accepted",
	}
	writeJSON(w, http.StatusOK, resp)
}

// convertRows converts raw map rows to typed Row structs.
func convertRows(rawRows []map[string]interface{}) ([]types.Row, error) {
	rows := make([]types.Row, 0, len(rawRows))

	for i, raw := range rawRows {
		row := types.Row{}

		// tenant_id (required)
		if v, ok := raw["tenant_id"]; ok {
			if s, ok := v.(string); ok {
				row.TenantID = s
			} else {
				return nil, fmt.Errorf("row %d: tenant_id must be a string", i)
			}
		} else {
			return nil, fmt.Errorf("row %d: tenant_id is required", i)
		}

		// user_id (required)
		if v, ok := raw["user_id"]; ok {
			switch n := v.(type) {
			case float64:
				row.UserID = int64(n)
			case json.Number:
				val, err := n.Int64()
				if err != nil {
					return nil, fmt.Errorf("row %d: invalid user_id: %v", i, err)
				}
				row.UserID = val
			default:
				return nil, fmt.Errorf("row %d: user_id must be a number", i)
			}
		} else {
			return nil, fmt.Errorf("row %d: user_id is required", i)
		}

		// event_time (required)
		if v, ok := raw["event_time"]; ok {
			switch n := v.(type) {
			case float64:
				row.EventTime = int64(n)
			case json.Number:
				val, err := n.Int64()
				if err != nil {
					return nil, fmt.Errorf("row %d: invalid event_time: %v", i, err)
				}
				row.EventTime = val
			default:
				return nil, fmt.Errorf("row %d: event_time must be a number", i)
			}
		} else {
			return nil, fmt.Errorf("row %d: event_time is required", i)
		}

		// event_type (required)
		if v, ok := raw["event_type"]; ok {
			if s, ok := v.(string); ok {
				row.EventType = s
			} else {
				return nil, fmt.Errorf("row %d: event_type must be a string", i)
			}
		} else {
			return nil, fmt.Errorf("row %d: event_type is required", i)
		}

		// payload (optional, defaults to empty map)
		if v, ok := raw["payload"]; ok {
			if m, ok := v.(map[string]interface{}); ok {
				row.Payload = m
			} else {
				return nil, fmt.Errorf("row %d: payload must be an object", i)
			}
		} else {
			row.Payload = make(map[string]interface{})
		}

		// event_id (optional, will be auto-generated if not provided)
		if v, ok := raw["event_id"]; ok {
			if s, ok := v.(string); ok {
				row.EventID = []byte(s)
			}
		}

		rows = append(rows, row)
	}

	return rows, nil
}
