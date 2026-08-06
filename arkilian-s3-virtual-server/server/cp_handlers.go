package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync/atomic"
	"time"
)

// ControlPlaneHandler handles key issuance, health, stats, and WAL CDC observation.
type ControlPlaneHandler struct {
	storage       *StorageEngine
	serverURL     string
	verifier      *SigV4Verifier
	totalWALBytes uint64
	totalWALRows  uint64
}

func NewControlPlaneHandler(storage *StorageEngine, serverURL string, verifier *SigV4Verifier) *ControlPlaneHandler {
	return &ControlPlaneHandler{
		storage:   storage,
		serverURL: serverURL,
		verifier:  verifier,
	}
}

// HandlePresignedURLRequest generates a signed upload URL for Arkilian clients calling /v1/upload/request.
func (cp *ControlPlaneHandler) HandlePresignedURLRequest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req PresignedURLRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		// Fallback for empty body or query string
		req.FileName = "backup.sqlite"
		req.DBID = "default"
	}

	if req.DBID == "" {
		req.DBID = "default"
	}
	if req.FileName == "" {
		req.FileName = "backup.sqlite"
	}

	s3Key := fmt.Sprintf("%s/backups/%s", req.DBID, req.FileName)
	bucket := "arkilian-backups"

	// Construct direct S3 upload URL or presigned URL
	amzDate := time.Now().UTC().Format("20060102T150405Z")
	uploadURL := fmt.Sprintf("%s/%s/%s?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=%s%%2F%s%%2F%s%%2Fs3%%2Faws4_request&X-Amz-Date=%s&X-Amz-Expires=3600&X-Amz-SignedHeaders=host&X-Amz-Signature=sigv4-presigned-token",
		cp.serverURL, bucket, s3Key, cp.verifier.AccessKey, time.Now().UTC().Format("20060102"), cp.verifier.Region, amzDate)

	resp := PresignedURLResponse{
		UploadURL: uploadURL,
		S3Key:     s3Key,
		ExpiresIn: 3600,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(resp)
}

// HandleWALPush handles streaming CDC updates sent by Arkilian flush threads to /v1/wal/push.
func (cp *ControlPlaneHandler) HandleWALPush(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var payload WALPushPayload
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, "Invalid WAL payload", http.StatusBadRequest)
		return
	}

	atomic.AddUint64(&cp.totalWALRows, 1)
	atomic.AddUint64(&cp.totalWALBytes, uint64(len(payload.SQL)))

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"success","message":"WAL payload received"}`))
}

// HandleHealth returns cluster liveness status.
func (cp *ControlPlaneHandler) HandleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"healthy","service":"arkilian-s3-virtual-server","timestamp":` + fmt.Sprintf("%d", time.Now().Unix()) + `}`))
}

// HandleStats returns observability metrics.
func (cp *ControlPlaneHandler) HandleStats(w http.ResponseWriter, r *http.Request) {
	stats := map[string]interface{}{
		"status":          "running",
		"service":         "arkilian-s3-virtual-server",
		"total_wal_rows":  atomic.LoadUint64(&cp.totalWALRows),
		"total_wal_bytes": atomic.LoadUint64(&cp.totalWALBytes),
		"uptime_seconds":  time.Now().Unix(),
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(stats)
}
