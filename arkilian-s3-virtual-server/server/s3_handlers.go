package server

import (
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// S3Handler orchestrates HTTP S3 request routing and operations.
type S3Handler struct {
	storage  *StorageEngine
	verifier *SigV4Verifier
	requireAuth bool
}

func NewS3Handler(storage *StorageEngine, verifier *SigV4Verifier, requireAuth bool) *S3Handler {
	return &S3Handler{
		storage:     storage,
		verifier:    verifier,
		requireAuth: requireAuth,
	}
}

// ServeHTTP routes incoming S3 requests based on HTTP Method and URL parameters.
func (h *S3Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Enable CORS for web apps & SDKs
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, PUT, POST, DELETE, HEAD, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "*")
	w.Header().Set("Access-Control-Expose-Headers", "ETag, Content-Length, Content-Type, Last-Modified, X-Amz-Request-Id")

	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}

	// Authenticate if required
	if h.requireAuth {
		if ok, reason := h.verifier.VerifyRequest(r); !ok {
			h.writeS3Error(w, r, "AccessDenied", fmt.Sprintf("Access Denied: %s", reason), http.StatusForbidden)
			return
		}
	}

	bucket, key := h.parseBucketAndKey(r)
	if bucket == "" {
		h.writeS3Error(w, r, "InvalidBucketName", "The specified bucket is not valid.", http.StatusBadRequest)
		return
	}

	switch r.Method {
	case http.MethodPut:
		h.handlePutObject(w, r, bucket, key)
	case http.MethodGet:
		if key == "" {
			h.handleListObjects(w, r, bucket)
		} else {
			h.handleGetObject(w, r, bucket, key)
		}
	case http.MethodHead:
		h.handleHeadObject(w, r, bucket, key)
	case http.MethodDelete:
		h.handleDeleteObject(w, r, bucket, key)
	default:
		h.writeS3Error(w, r, "MethodNotAllowed", "The specified method is not allowed against this resource.", http.StatusMethodNotAllowed)
	}
}

func (h *S3Handler) parseBucketAndKey(r *http.Request) (string, string) {
	path := strings.TrimPrefix(r.URL.Path, "/")
	if path == "" {
		return "", ""
	}

	parts := strings.SplitN(path, "/", 2)
	bucket := parts[0]
	key := ""
	if len(parts) > 1 {
		key = parts[1]
	}

	return bucket, key
}

func (h *S3Handler) handlePutObject(w http.ResponseWriter, r *http.Request, bucket, key string) {
	if key == "" {
		// Bucket creation fallback
		w.WriteHeader(http.StatusOK)
		return
	}

	contentType := r.Header.Get("Content-Type")
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	meta, err := h.storage.PutObject(bucket, key, contentType, r.Body, nil)
	if err != nil {
		h.writeS3Error(w, r, "InternalError", fmt.Sprintf("Failed to store object: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("ETag", meta.ETag)
	w.WriteHeader(http.StatusOK)
}

func (h *S3Handler) handleGetObject(w http.ResponseWriter, r *http.Request, bucket, key string) {
	file, meta, err := h.storage.GetObject(bucket, key)
	if err != nil {
		if err == ErrObjectNotFound {
			h.writeS3Error(w, r, "NoSuchKey", "The specified key does not exist.", http.StatusNotFound)
		} else {
			h.writeS3Error(w, r, "InternalError", fmt.Sprintf("Failed to retrieve object: %v", err), http.StatusInternalServerError)
		}
		return
	}
	defer file.Close()

	w.Header().Set("Content-Type", meta.ContentType)
	w.Header().Set("Content-Length", strconv.FormatInt(meta.Size, 10))
	w.Header().Set("ETag", meta.ETag)
	w.Header().Set("Last-Modified", meta.LastModified.Format(time.RFC1123))

	// Handle Range Requests
	rangeHeader := r.Header.Get("Range")
	if rangeHeader != "" && strings.HasPrefix(rangeHeader, "bytes=") {
		var start, end int64
		bytesRange := strings.TrimPrefix(rangeHeader, "bytes=")
		parts := strings.Split(bytesRange, "-")
		if len(parts) == 2 {
			start, _ = strconv.ParseInt(parts[0], 10, 64)
			if parts[1] != "" {
				end, _ = strconv.ParseInt(parts[1], 10, 64)
			} else {
				end = meta.Size - 1
			}
			if start >= 0 && end >= start && start < meta.Size {
				if end >= meta.Size {
					end = meta.Size - 1
				}
				contentLength := end - start + 1

				_, _ = file.Seek(start, io.SeekStart)
				w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, meta.Size))
				w.Header().Set("Content-Length", strconv.FormatInt(contentLength, 10))
				w.WriteHeader(http.StatusPartialContent)
				_, _ = io.CopyN(w, file, contentLength)
				return
			}
		}
	}

	w.WriteHeader(http.StatusOK)
	_, _ = io.Copy(w, file)
}

func (h *S3Handler) handleHeadObject(w http.ResponseWriter, r *http.Request, bucket, key string) {
	meta, err := h.storage.HeadObject(bucket, key)
	if err != nil {
		if err == ErrObjectNotFound {
			w.WriteHeader(http.StatusNotFound)
		} else {
			w.WriteHeader(http.StatusInternalServerError)
		}
		return
	}

	w.Header().Set("Content-Type", meta.ContentType)
	w.Header().Set("Content-Length", strconv.FormatInt(meta.Size, 10))
	w.Header().Set("ETag", meta.ETag)
	w.Header().Set("Last-Modified", meta.LastModified.Format(time.RFC1123))
	w.WriteHeader(http.StatusOK)
}

func (h *S3Handler) handleDeleteObject(w http.ResponseWriter, r *http.Request, bucket, key string) {
	if err := h.storage.DeleteObject(bucket, key); err != nil {
		h.writeS3Error(w, r, "InternalError", fmt.Sprintf("Failed to delete object: %v", err), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *S3Handler) handleListObjects(w http.ResponseWriter, r *http.Request, bucket string) {
	prefix := r.URL.Query().Get("prefix")
	marker := r.URL.Query().Get("marker")
	maxKeysStr := r.URL.Query().Get("max-keys")
	maxKeys := 1000
	if maxKeysStr != "" {
		if k, err := strconv.Atoi(maxKeysStr); err == nil {
			maxKeys = k
		}
	}

	result, err := h.storage.ListObjects(bucket, prefix, marker, maxKeys)
	if err != nil {
		h.writeS3Error(w, r, "InternalError", fmt.Sprintf("Failed to list objects: %v", err), http.StatusInternalServerError)
		return
	}

	xmlData, err := xml.MarshalIndent(result, "", "  ")
	if err != nil {
		h.writeS3Error(w, r, "InternalError", "Failed to format XML response", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(xml.Header))
	_, _ = w.Write(xmlData)
}

func (h *S3Handler) writeS3Error(w http.ResponseWriter, r *http.Request, code, message string, statusCode int) {
	s3Err := S3Error{
		Code:      code,
		Message:   message,
		Resource:  r.URL.Path,
		RequestId: fmt.Sprintf("%x", time.Now().UnixNano()),
		HostId:    "arkilian-s3-server",
	}

	xmlData, _ := xml.MarshalIndent(s3Err, "", "  ")
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(statusCode)
	_, _ = w.Write([]byte(xml.Header))
	_, _ = w.Write(xmlData)
}
