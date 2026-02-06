package http

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/arkilian/arkilian/internal/query/executor"
	"github.com/arkilian/arkilian/internal/query/parser"
)

// QueryRequest represents a query request.
type QueryRequest struct {
	SQL string `json:"sql"`
}

// QueryResponse represents the query response.
type QueryResponse struct {
	Columns   []string        `json:"columns"`
	Rows      [][]interface{} `json:"rows"`
	Stats     QueryStats      `json:"stats"`
	RequestID string          `json:"request_id"`
}

// QueryStats contains execution statistics.
type QueryStats struct {
	PartitionsScanned int   `json:"partitions_scanned"`
	PartitionsPruned  int   `json:"partitions_pruned"`
	ExecutionTimeMs   int64 `json:"execution_time_ms"`
}

// QueryHandler handles POST /v1/query requests.
type QueryHandler struct {
	executor executor.QueryExecutor
}

// NewQueryHandler creates a new query handler.
func NewQueryHandler(exec executor.QueryExecutor) *QueryHandler {
	return &QueryHandler{
		executor: exec,
	}
}

// ServeHTTP handles the query HTTP request.
func (h *QueryHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	requestID := GetRequestID(r.Context())

	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed", requestID)
		return
	}

	// Parse request body
	var req QueryRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid request body: %v", err), requestID)
		return
	}

	// Validate SQL
	if req.SQL == "" {
		writeError(w, http.StatusBadRequest, "sql is required", requestID)
		return
	}

	// Parse SQL
	stmt, err := parser.Parse(req.SQL)
	if err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid SQL: %v", err), requestID)
		return
	}

	// Only SELECT statements are supported
	selectStmt, ok := stmt.(*parser.SelectStatement)
	if !ok {
		writeError(w, http.StatusBadRequest, "only SELECT statements are supported", requestID)
		return
	}

	// Execute query
	result, err := h.executor.Execute(r.Context(), selectStmt)
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("query execution failed: %v", err), requestID)
		return
	}

	// Build response
	resp := QueryResponse{
		Columns: result.Columns,
		Rows:    result.Rows,
		Stats: QueryStats{
			PartitionsScanned: result.Stats.PartitionsScanned,
			PartitionsPruned:  result.Stats.PartitionsPruned,
			ExecutionTimeMs:   result.Stats.ExecutionTimeMs,
		},
		RequestID: requestID,
	}

	// Ensure rows is not nil for JSON serialization
	if resp.Rows == nil {
		resp.Rows = [][]interface{}{}
	}
	if resp.Columns == nil {
		resp.Columns = []string{}
	}

	writeJSON(w, http.StatusOK, resp)
}
