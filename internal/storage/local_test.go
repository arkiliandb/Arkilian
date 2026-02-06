package storage

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestLocalStorage_UploadDownload(t *testing.T) {
	// Create temp directories
	baseDir := t.TempDir()
	storage, err := NewLocalStorage(baseDir)
	if err != nil {
		t.Fatalf("failed to create local storage: %v", err)
	}

	// Create a test file
	srcDir := t.TempDir()
	srcPath := filepath.Join(srcDir, "test.txt")
	content := []byte("hello world")
	if err := os.WriteFile(srcPath, content, 0644); err != nil {
		t.Fatalf("failed to write test file: %v", err)
	}

	ctx := context.Background()

	// Test Upload
	objectPath := "test/object.txt"
	if err := storage.Upload(ctx, srcPath, objectPath); err != nil {
		t.Fatalf("Upload failed: %v", err)
	}

	// Test Exists
	exists, err := storage.Exists(ctx, objectPath)
	if err != nil {
		t.Fatalf("Exists failed: %v", err)
	}
	if !exists {
		t.Error("expected object to exist")
	}

	// Test Download
	dstPath := filepath.Join(srcDir, "downloaded.txt")
	if err := storage.Download(ctx, objectPath, dstPath); err != nil {
		t.Fatalf("Download failed: %v", err)
	}

	downloaded, err := os.ReadFile(dstPath)
	if err != nil {
		t.Fatalf("failed to read downloaded file: %v", err)
	}
	if string(downloaded) != string(content) {
		t.Errorf("content mismatch: got %q, want %q", downloaded, content)
	}

	// Test Delete
	if err := storage.Delete(ctx, objectPath); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	exists, err = storage.Exists(ctx, objectPath)
	if err != nil {
		t.Fatalf("Exists after delete failed: %v", err)
	}
	if exists {
		t.Error("expected object to not exist after delete")
	}
}

func TestLocalStorage_UploadMultipart(t *testing.T) {
	baseDir := t.TempDir()
	storage, err := NewLocalStorage(baseDir)
	if err != nil {
		t.Fatalf("failed to create local storage: %v", err)
	}

	srcDir := t.TempDir()
	srcPath := filepath.Join(srcDir, "test.txt")
	content := []byte("multipart test content")
	if err := os.WriteFile(srcPath, content, 0644); err != nil {
		t.Fatalf("failed to write test file: %v", err)
	}

	ctx := context.Background()
	objectPath := "multipart/object.txt"

	etag, err := storage.UploadMultipart(ctx, srcPath, objectPath)
	if err != nil {
		t.Fatalf("UploadMultipart failed: %v", err)
	}
	if etag == "" {
		t.Error("expected non-empty ETag")
	}

	// Verify ETag is stored
	storedETag, exists := storage.GetETag(objectPath)
	if !exists {
		t.Error("expected ETag to be stored")
	}
	if storedETag != etag {
		t.Errorf("ETag mismatch: got %q, want %q", storedETag, etag)
	}
}

func TestLocalStorage_ConditionalPut(t *testing.T) {
	baseDir := t.TempDir()
	storage, err := NewLocalStorage(baseDir)
	if err != nil {
		t.Fatalf("failed to create local storage: %v", err)
	}

	srcDir := t.TempDir()
	srcPath := filepath.Join(srcDir, "test.txt")
	content := []byte("conditional put test")
	if err := os.WriteFile(srcPath, content, 0644); err != nil {
		t.Fatalf("failed to write test file: %v", err)
	}

	ctx := context.Background()
	objectPath := "conditional/object.txt"

	// First upload
	etag, err := storage.UploadMultipart(ctx, srcPath, objectPath)
	if err != nil {
		t.Fatalf("initial upload failed: %v", err)
	}

	// Conditional put with correct ETag should succeed
	if err := storage.ConditionalPut(ctx, srcPath, objectPath, etag); err != nil {
		t.Fatalf("ConditionalPut with correct ETag failed: %v", err)
	}

	// Conditional put with wrong ETag should fail
	err = storage.ConditionalPut(ctx, srcPath, objectPath, "wrong-etag")
	if err != ErrPreconditionFailed {
		t.Errorf("expected ErrPreconditionFailed, got %v", err)
	}
}

func TestLocalStorage_DownloadNotFound(t *testing.T) {
	baseDir := t.TempDir()
	storage, err := NewLocalStorage(baseDir)
	if err != nil {
		t.Fatalf("failed to create local storage: %v", err)
	}

	ctx := context.Background()
	dstPath := filepath.Join(t.TempDir(), "downloaded.txt")

	err = storage.Download(ctx, "nonexistent/object.txt", dstPath)
	if err != ErrObjectNotFound {
		t.Errorf("expected ErrObjectNotFound, got %v", err)
	}
}

func TestLocalStorage_Clear(t *testing.T) {
	baseDir := t.TempDir()
	storage, err := NewLocalStorage(baseDir)
	if err != nil {
		t.Fatalf("failed to create local storage: %v", err)
	}

	srcDir := t.TempDir()
	srcPath := filepath.Join(srcDir, "test.txt")
	if err := os.WriteFile(srcPath, []byte("test"), 0644); err != nil {
		t.Fatalf("failed to write test file: %v", err)
	}

	ctx := context.Background()

	// Upload some objects
	if err := storage.Upload(ctx, srcPath, "obj1.txt"); err != nil {
		t.Fatalf("Upload failed: %v", err)
	}
	if err := storage.Upload(ctx, srcPath, "obj2.txt"); err != nil {
		t.Fatalf("Upload failed: %v", err)
	}

	// Clear storage
	if err := storage.Clear(); err != nil {
		t.Fatalf("Clear failed: %v", err)
	}

	// Verify objects are gone
	exists, _ := storage.Exists(ctx, "obj1.txt")
	if exists {
		t.Error("expected obj1.txt to not exist after clear")
	}
	exists, _ = storage.Exists(ctx, "obj2.txt")
	if exists {
		t.Error("expected obj2.txt to not exist after clear")
	}
}
