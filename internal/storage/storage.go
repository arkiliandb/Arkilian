// Package storage provides object storage abstractions for cloud storage operations.
package storage

import (
	"context"
	"errors"
)

// Common errors for storage operations.
var (
	ErrObjectNotFound     = errors.New("object not found")
	ErrPreconditionFailed = errors.New("precondition failed")
	ErrUploadFailed       = errors.New("upload failed")
	ErrDownloadFailed     = errors.New("download failed")
	ErrDeleteFailed       = errors.New("delete failed")
)

// ObjectStorage abstracts cloud object storage operations.
// Implementations include S3, GCS, and local filesystem for testing.
type ObjectStorage interface {
	// Upload uploads a file to object storage.
	// localPath is the path to the local file to upload.
	// objectPath is the destination path in object storage.
	Upload(ctx context.Context, localPath, objectPath string) error

	// UploadMultipart uploads using multipart for large files.
	// Returns the ETag of the uploaded object for validation.
	// localPath is the path to the local file to upload.
	// objectPath is the destination path in object storage.
	UploadMultipart(ctx context.Context, localPath, objectPath string) (string, error)

	// Download downloads a file from object storage.
	// objectPath is the source path in object storage.
	// localPath is the destination path on the local filesystem.
	Download(ctx context.Context, objectPath, localPath string) error

	// Delete removes an object from storage.
	// objectPath is the path of the object to delete.
	Delete(ctx context.Context, objectPath string) error

	// Exists checks if an object exists in storage.
	// Returns true if the object exists, false otherwise.
	Exists(ctx context.Context, objectPath string) (bool, error)

	// ConditionalPut uploads only if the precondition is met.
	// This is used for atomic operations where we want to ensure
	// the object hasn't been modified since we last read it.
	// etag is the expected ETag of the existing object (empty string for new objects).
	ConditionalPut(ctx context.Context, localPath, objectPath, etag string) error

	// ListObjects returns all object paths under the given prefix.
	// Used by reconciliation to detect orphaned objects.
	ListObjects(ctx context.Context, prefix string) ([]string, error)
}

// MultipartUploadConfig holds configuration for multipart uploads.
type MultipartUploadConfig struct {
	// PartSize is the size of each part in bytes (default: 5MB).
	PartSize int64
	// Concurrency is the number of concurrent part uploads (default: 5).
	Concurrency int
}

// DefaultMultipartConfig returns the default multipart upload configuration.
func DefaultMultipartConfig() MultipartUploadConfig {
	return MultipartUploadConfig{
		PartSize:    5 * 1024 * 1024, // 5MB
		Concurrency: 5,
	}
}
