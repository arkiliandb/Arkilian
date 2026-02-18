package benchmark

import (
	"context"
	"fmt"
	"os"
	"path"
	"testing"
	"time"

	"github.com/arkilian/arkilian/internal/storage"
	"github.com/joho/godotenv"
)

// PrefixedStorage wraps an ObjectStorage and prepends a prefix to all object paths.
type PrefixedStorage struct {
	inner  storage.ObjectStorage
	prefix string
}

func (s *PrefixedStorage) Upload(ctx context.Context, localPath, objectPath string) error {
	return s.inner.Upload(ctx, localPath, s.prefix+"/"+objectPath)
}

func (s *PrefixedStorage) UploadMultipart(ctx context.Context, localPath, objectPath string) (string, error) {
	return s.inner.UploadMultipart(ctx, localPath, s.prefix+"/"+objectPath)
}

func (s *PrefixedStorage) Download(ctx context.Context, objectPath, localPath string) error {
	return s.inner.Download(ctx, s.prefix+"/"+objectPath, localPath)
}

func (s *PrefixedStorage) Delete(ctx context.Context, objectPath string) error {
	return s.inner.Delete(ctx, s.prefix+"/"+objectPath)
}

func (s *PrefixedStorage) Exists(ctx context.Context, objectPath string) (bool, error) {
	return s.inner.Exists(ctx, s.prefix+"/"+objectPath)
}

func (s *PrefixedStorage) ConditionalPut(ctx context.Context, localPath, objectPath, etag string) error {
	return s.inner.ConditionalPut(ctx, localPath, s.prefix+"/"+objectPath, etag)
}

func (s *PrefixedStorage) ListObjects(ctx context.Context, prefix string) ([]string, error) {
	// For ListObjects, we prepend the prefix to the query prefix.
	// We also need to strip the prefix from the returned results to be transparent.
	fullPrefix := s.prefix + "/" + prefix
	objects, err := s.inner.ListObjects(ctx, fullPrefix)
	if err != nil {
		return nil, err
	}
	
	stripped := make([]string, len(objects))
	for i, obj := range objects {
		// keys returned by ListObjects usually contain the full key.
		// simple string slicing assuming keys start with fullPrefix
		if len(obj) > len(s.prefix)+1 {
			stripped[i] = obj[len(s.prefix)+1:]
		} else {
			stripped[i] = obj
		}
	}
	return stripped, nil
}

// getBenchmarkStorage returns a storage interface and a root path/prefix.
// It respects ARKILIAN_STORAGE_TYPE=s3 from .env or environment.
// For S3: prefix is "bench/<benchName>/<timestamp>".
// For Local: prefix is "" (writes directly to temp dir).
func getBenchmarkStorage(b *testing.B, benchName string) (storage.ObjectStorage, string, func()) {
	// Try loading .env from project root (../../.env relative to test/benchmark)
	_ = godotenv.Load("../../.env")

	storageType := os.Getenv("ARKILIAN_STORAGE_TYPE")

	if storageType == "s3" {
		// Map credentials
		if v := os.Getenv("ARKILIAN_AWS_ACCESS_KEY_ID"); v != "" {
			os.Setenv("AWS_ACCESS_KEY_ID", v)
		}
		if v := os.Getenv("ARKILIAN_AWS_SECRET_ACCESS_KEY"); v != "" {
			os.Setenv("AWS_SECRET_ACCESS_KEY", v)
		}

		bucket := os.Getenv("ARKILIAN_S3_BUCKET")
		region := os.Getenv("ARKILIAN_S3_REGION")
		endpoint := os.Getenv("ARKILIAN_S3_ENDPOINT")

		if bucket == "" {
			b.Fatal("ARKILIAN_S3_BUCKET is required for s3 benchmark")
		}

		cfg := storage.DefaultS3Config()
		cfg.Region = region
		cfg.Endpoint = endpoint

		st, err := storage.NewS3Storage(context.Background(), bucket, cfg)
		if err != nil {
			b.Fatalf("Failed to initialize S3 storage: %v", err)
		}

		// Unique prefix for this run
		prefix := fmt.Sprintf("bench/%s/%d", benchName, time.Now().UnixNano())

		// Cleanup is manual/optional for S3 to avoid deleting large datasets if debugging
		cleanup := func() {
			// No-op for now
		}

		b.Logf("Running benchmark against S3 Bucket: %s Prefix: %s", bucket, prefix)
		
		// Wrap with prefix storage so caller doesn't need to conform
		prefixed := &PrefixedStorage{inner: st, prefix: prefix}
		return prefixed, "", cleanup
	}

	// Default to Local
	dir, err := os.MkdirTemp("", "arkilian-bench-"+benchName+"-*")
	if err != nil {
		b.Fatal(err)
	}
	storageDir := path.Join(dir, "storage")
	os.MkdirAll(storageDir, 0755)

	st, err := storage.NewLocalStorage(storageDir)
	if err != nil {
		b.Fatal(err)
	}

	cleanup := func() {
		os.RemoveAll(dir)
	}

	// Local storage uses absolute paths or relative to its base, but we initialize it with a base.
	// We return empty prefix because we don't prepend a "folder" in the object key for local tests usually,
	// or rather, we want local tests to write to the temp dir.
	return st, "", cleanup
}
