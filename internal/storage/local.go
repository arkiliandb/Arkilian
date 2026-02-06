package storage

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// LocalStorage implements ObjectStorage using the local filesystem.
// This is primarily used for testing and development.
type LocalStorage struct {
	basePath string
	mu       sync.RWMutex
	etags    map[string]string // Track ETags for conditional operations
}

// NewLocalStorage creates a new local filesystem storage.
func NewLocalStorage(basePath string) (*LocalStorage, error) {
	if err := os.MkdirAll(basePath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create base directory: %w", err)
	}

	return &LocalStorage{
		basePath: basePath,
		etags:    make(map[string]string),
	}, nil
}

// Upload uploads a file to local storage.
func (l *LocalStorage) Upload(ctx context.Context, localPath, objectPath string) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	destPath := l.fullPath(objectPath)

	// Create parent directories
	if err := os.MkdirAll(filepath.Dir(destPath), 0755); err != nil {
		return fmt.Errorf("%w: %v", ErrUploadFailed, err)
	}

	// Open source file
	src, err := os.Open(localPath)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrUploadFailed, err)
	}
	defer src.Close()

	// Create destination file
	dst, err := os.Create(destPath)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrUploadFailed, err)
	}
	defer dst.Close()

	// Copy content and compute ETag
	hash := md5.New()
	writer := io.MultiWriter(dst, hash)

	if _, err := io.Copy(writer, src); err != nil {
		return fmt.Errorf("%w: %v", ErrUploadFailed, err)
	}

	// Store ETag
	etag := hex.EncodeToString(hash.Sum(nil))
	l.mu.Lock()
	l.etags[objectPath] = etag
	l.mu.Unlock()

	return nil
}

// UploadMultipart uploads a file using multipart upload simulation.
// For local storage, this behaves the same as Upload but returns an ETag.
func (l *LocalStorage) UploadMultipart(ctx context.Context, localPath, objectPath string) (string, error) {
	if err := l.Upload(ctx, localPath, objectPath); err != nil {
		return "", err
	}

	l.mu.RLock()
	etag := l.etags[objectPath]
	l.mu.RUnlock()

	return etag, nil
}

// Download downloads a file from local storage.
func (l *LocalStorage) Download(ctx context.Context, objectPath, localPath string) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	srcPath := l.fullPath(objectPath)

	// Check if source exists
	if _, err := os.Stat(srcPath); os.IsNotExist(err) {
		return ErrObjectNotFound
	}

	// Create parent directories for destination
	if err := os.MkdirAll(filepath.Dir(localPath), 0755); err != nil {
		return fmt.Errorf("%w: %v", ErrDownloadFailed, err)
	}

	// Open source file
	src, err := os.Open(srcPath)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrDownloadFailed, err)
	}
	defer src.Close()

	// Create destination file
	dst, err := os.Create(localPath)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrDownloadFailed, err)
	}
	defer dst.Close()

	if _, err := io.Copy(dst, src); err != nil {
		return fmt.Errorf("%w: %v", ErrDownloadFailed, err)
	}

	return nil
}

// Delete removes an object from local storage.
func (l *LocalStorage) Delete(ctx context.Context, objectPath string) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	fullPath := l.fullPath(objectPath)

	if err := os.Remove(fullPath); err != nil {
		if os.IsNotExist(err) {
			// S3 Delete is idempotent, so we don't return an error
			return nil
		}
		return fmt.Errorf("%w: %v", ErrDeleteFailed, err)
	}

	l.mu.Lock()
	delete(l.etags, objectPath)
	l.mu.Unlock()

	return nil
}

// Exists checks if an object exists in local storage.
func (l *LocalStorage) Exists(ctx context.Context, objectPath string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}

	fullPath := l.fullPath(objectPath)
	_, err := os.Stat(fullPath)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// ConditionalPut uploads only if the precondition is met.
func (l *LocalStorage) ConditionalPut(ctx context.Context, localPath, objectPath, etag string) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	l.mu.RLock()
	currentETag, exists := l.etags[objectPath]
	l.mu.RUnlock()

	// If etag is provided, check if it matches
	if etag != "" {
		if !exists {
			return ErrPreconditionFailed
		}
		if currentETag != etag {
			return ErrPreconditionFailed
		}
	}

	return l.Upload(ctx, localPath, objectPath)
}

// GetETag returns the ETag for an object.
func (l *LocalStorage) GetETag(objectPath string) (string, bool) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	etag, exists := l.etags[objectPath]
	return etag, exists
}

// fullPath returns the full filesystem path for an object.
func (l *LocalStorage) fullPath(objectPath string) string {
	return filepath.Join(l.basePath, objectPath)
}

// ListObjects returns all object paths under the given prefix.
func (l *LocalStorage) ListObjects(ctx context.Context, prefix string) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	searchDir := l.fullPath(prefix)
	var objects []string

	err := filepath.Walk(searchDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return nil // prefix doesn't exist, return empty list
			}
			return err
		}
		if !info.IsDir() {
			rel, err := filepath.Rel(l.basePath, path)
			if err != nil {
				return err
			}
			objects = append(objects, rel)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return objects, nil
}

// Clear removes all objects from local storage.
// This is useful for test cleanup.
func (l *LocalStorage) Clear() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if err := os.RemoveAll(l.basePath); err != nil {
		return err
	}

	if err := os.MkdirAll(l.basePath, 0755); err != nil {
		return err
	}

	l.etags = make(map[string]string)
	return nil
}
