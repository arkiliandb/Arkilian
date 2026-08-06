package server

import "encoding/json"

import (
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

var (
	ErrObjectNotFound = errors.New("NoSuchKey: The specified key does not exist")
	ErrBucketNotFound = errors.New("NoSuchBucket: The specified bucket does not exist")
	ErrInvalidPath    = errors.New("InvalidPath: Path traversal detected")
)

// ObjectMetadata stores metadata associated with an S3 object.
type ObjectMetadata struct {
	Key          string            `json:"key"`
	Bucket       string            `json:"bucket"`
	Size         int64             `json:"size"`
	ETag         string            `json:"etag"`
	ContentType  string            `json:"content_type"`
	LastModified time.Time         `json:"last_modified"`
	UserMeta     map[string]string `json:"user_meta,omitempty"`
}

// StorageEngine manages physical storage on disk for multi-tenant buckets.
type StorageEngine struct {
	rootDir string
	mu      sync.RWMutex
}

// NewStorageEngine initializes the root storage directory.
func NewStorageEngine(rootDir string) (*StorageEngine, error) {
	absDir, err := filepath.Abs(rootDir)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(absDir, 0755); err != nil {
		return nil, err
	}
	return &StorageEngine{rootDir: absDir}, nil
}

// sanitizePath secures bucket and key paths against traversal attacks.
func (s *StorageEngine) sanitizePath(bucket, key string) (string, error) {
	cleanBucket := filepath.Clean(bucket)
	cleanKey := filepath.Clean(key)

	if cleanBucket == "." || cleanBucket == ".." || strings.Contains(cleanBucket, "/") || strings.Contains(cleanBucket, "\\") {
		return "", ErrInvalidPath
	}

	fullPath := filepath.Join(s.rootDir, cleanBucket, cleanKey)
	rel, err := filepath.Rel(s.rootDir, fullPath)
	if err != nil || strings.HasPrefix(rel, "..") {
		return "", ErrInvalidPath
	}

	return fullPath, nil
}

// PutObject atomically stores an object and writes its metadata.
func (s *StorageEngine) PutObject(bucket, key, contentType string, r io.Reader, userMeta map[string]string) (*ObjectMetadata, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	targetPath, err := s.sanitizePath(bucket, key)
	if err != nil {
		return nil, err
	}

	dir := filepath.Dir(targetPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create directory: %w", err)
	}

	// Write to temporary file for atomic rename
	tmpFile, err := os.CreateTemp(dir, ".tmp-upload-*")
	if err != nil {
		return nil, fmt.Errorf("failed to create temp file: %w", err)
	}
	tmpName := tmpFile.Name()
	defer func() {
		_ = tmpFile.Close()
		_ = os.Remove(tmpName)
	}()

	hash := md5.New()
	multiWriter := io.MultiWriter(tmpFile, hash)

	size, err := io.Copy(multiWriter, r)
	if err != nil {
		return nil, fmt.Errorf("failed to write object data: %w", err)
	}

	if err := tmpFile.Close(); err != nil {
		return nil, fmt.Errorf("failed to close temp file: %w", err)
	}

	etag := fmt.Sprintf("\"%s\"", hex.EncodeToString(hash.Sum(nil)))
	meta := &ObjectMetadata{
		Key:          key,
		Bucket:       bucket,
		Size:         size,
		ETag:         etag,
		ContentType:  contentType,
		LastModified: time.Now().UTC(),
		UserMeta:     userMeta,
	}

	// Save metadata file alongside target
	metaPath := targetPath + ".meta.json"
	metaBytes, err := json.Marshal(meta)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal metadata: %w", err)
	}
	if err := os.WriteFile(metaPath, metaBytes, 0644); err != nil {
		return nil, fmt.Errorf("failed to write metadata file: %w", err)
	}

	// Atomic rename
	if err := os.Rename(tmpName, targetPath); err != nil {
		_ = os.Remove(metaPath)
		return nil, fmt.Errorf("failed to commit object: %w", err)
	}

	return meta, nil
}

// GetObject retrieves object data and metadata.
func (s *StorageEngine) GetObject(bucket, key string) (*os.File, *ObjectMetadata, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	targetPath, err := s.sanitizePath(bucket, key)
	if err != nil {
		return nil, nil, err
	}

	file, err := os.Open(targetPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil, ErrObjectNotFound
		}
		return nil, nil, err
	}

	meta, err := s.readMetadata(targetPath)
	if err != nil {
		// Fallback metadata if .meta.json missing
		fi, statErr := file.Stat()
		if statErr != nil {
			_ = file.Close()
			return nil, nil, statErr
		}
		meta = &ObjectMetadata{
			Key:          key,
			Bucket:       bucket,
			Size:         fi.Size(),
			ETag:         fmt.Sprintf("\"%x\"", fi.ModTime().UnixNano()),
			ContentType:  "application/octet-stream",
			LastModified: fi.ModTime().UTC(),
		}
	}

	return file, meta, nil
}

// HeadObject retrieves metadata without opening data file.
func (s *StorageEngine) HeadObject(bucket, key string) (*ObjectMetadata, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	targetPath, err := s.sanitizePath(bucket, key)
	if err != nil {
		return nil, err
	}

	fi, err := os.Stat(targetPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrObjectNotFound
		}
		return nil, err
	}

	meta, err := s.readMetadata(targetPath)
	if err != nil {
		meta = &ObjectMetadata{
			Key:          key,
			Bucket:       bucket,
			Size:         fi.Size(),
			ETag:         fmt.Sprintf("\"%x\"", fi.ModTime().UnixNano()),
			ContentType:  "application/octet-stream",
			LastModified: fi.ModTime().UTC(),
		}
	}

	return meta, nil
}

// DeleteObject deletes object and its metadata file.
func (s *StorageEngine) DeleteObject(bucket, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	targetPath, err := s.sanitizePath(bucket, key)
	if err != nil {
		return err
	}

	_ = os.Remove(targetPath + ".meta.json")
	err = os.Remove(targetPath)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// ListObjects lists objects in a bucket with prefix filtering.
func (s *StorageEngine) ListObjects(bucket, prefix, marker string, maxKeys int) (*ListBucketResult, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	bucketPath, err := s.sanitizePath(bucket, "")
	if err != nil {
		return nil, err
	}

	if maxKeys <= 0 || maxKeys > 1000 {
		maxKeys = 1000
	}

	var objects []Object
	err = filepath.Walk(bucketPath, func(path string, info os.FileInfo, walkErr error) error {
		if walkErr != nil {
			if os.IsNotExist(walkErr) {
				return nil
			}
			return walkErr
		}
		if info.IsDir() || strings.HasSuffix(path, ".meta.json") || strings.Contains(path, ".tmp-upload-") {
			return nil
		}

		relKey, relErr := filepath.Rel(bucketPath, path)
		if relErr != nil {
			return nil
		}
		relKey = filepath.ToSlash(relKey)

		if prefix != "" && !strings.HasPrefix(relKey, prefix) {
			return nil
		}
		if marker != "" && relKey <= marker {
			return nil
		}

		meta, metaErr := s.readMetadata(path)
		etag := fmt.Sprintf("\"%x\"", info.ModTime().UnixNano())
		if metaErr == nil && meta.ETag != "" {
			etag = meta.ETag
		}

		objects = append(objects, Object{
			Key:          relKey,
			LastModified: info.ModTime().UTC().Format(time.RFC3339),
			ETag:         etag,
			Size:         info.Size(),
			StorageClass: "STANDARD",
		})

		return nil
	})

	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}

	sort.Slice(objects, func(i, j int) bool {
		return objects[i].Key < objects[j].Key
	})

	isTruncated := false
	if len(objects) > maxKeys {
		isTruncated = true
		objects = objects[:maxKeys]
	}

	return &ListBucketResult{
		Name:        bucket,
		Prefix:      prefix,
		Marker:      marker,
		MaxKeys:     maxKeys,
		IsTruncated: isTruncated,
		Contents:    objects,
	}, nil
}

func (s *StorageEngine) readMetadata(targetPath string) (*ObjectMetadata, error) {
	data, err := os.ReadFile(targetPath + ".meta.json")
	if err != nil {
		return nil, err
	}
	var meta ObjectMetadata
	if err := json.Unmarshal(data, &meta); err != nil {
		return nil, err
	}
	return &meta, nil
}
