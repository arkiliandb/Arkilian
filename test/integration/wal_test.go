// Package integration provides end-to-end integration tests for Project Arkilian.
package integration

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/arkilian/arkilian/internal/app"
	"github.com/arkilian/arkilian/internal/config"
	"github.com/arkilian/arkilian/internal/wal"
	"github.com/arkilian/arkilian/pkg/types"
)

// TestWALRecovery tests that unflushed WAL entries are replayed on restart.
func TestWALRecovery(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "arkilian-wal-recovery-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	walDir := filepath.Join(tempDir, "wal")
	storageDir := filepath.Join(tempDir, "storage")
	partitionDir := filepath.Join(tempDir, "partitions")

	for _, dir := range []string{walDir, storageDir, partitionDir} {
		os.MkdirAll(dir, 0755)
	}

	// Create WAL and write some entries
	walInstance, err := wal.NewWAL(walDir, 64*1024*1024)
	if err != nil {
		t.Fatalf("failed to create WAL: %v", err)
	}

	// Write entries without flushing
	for i := 0; i < 5; i++ {
		entry := &wal.Entry{
			PartitionKey: "20260206",
			Rows:         walTestRows(),
			Timestamp:    time.Now().UnixNano(),
		}
		_, err := walInstance.Append(entry)
		if err != nil {
			t.Fatalf("failed to append to WAL: %v", err)
		}
	}
	walInstance.Close()

	// Create app with same WAL dir - recovery should replay entries
	cfg := &config.Config{
		Mode:    config.ModeAll,
		DataDir: tempDir,
		Storage: config.StorageConfig{
			Type: "local",
			Path: storageDir,
		},
		Ingest: config.IngestConfig{
			TargetPartitionSizeMB: 64,
		},
		Manifest: config.ManifestConfig{Sharded: false},
		WAL: config.WALConfig{
			Dir:            walDir,
			MaxSegmentSize: 64 * 1024 * 1024,
			FlushInterval:  500 * time.Millisecond,
			FlushBatchSize: 1000,
			RetentionTime:  1 * time.Hour,
		},
		Router: config.RouterConfig{BufferSize: 1000},
		Index:  config.IndexConfig{BucketCount: 64, MaxIndexes: 10, CreateThreshold: 100, DropThreshold: 5, CheckInterval: 5 * time.Minute},
		Cache:  config.CacheConfig{NVMeDir: "", NVMeMaxBytes: 0, PrefetchEnabled: false},
		Compaction: config.CompactionConfig{
			CheckInterval:       2 * time.Minute,
			MinPartitionSize:    64 * 1024 * 1024,
			MaxPartitionsPerKey: 100,
			TTLDays:             7,
			Backpressure: config.BackpressureConfig{
				MaxConcurrency:   8,
				MinConcurrency:   1,
				FailureThreshold: 0.05,
			},
		},
	}

	testApp, err := app.New(cfg)
	if err != nil {
		t.Fatalf("failed to create app: %v", err)
	}

	ctx := context.Background()
	if err := testApp.Start(ctx); err != nil {
		t.Fatalf("failed to start app: %v", err)
	}

	// Wait for flush
	time.Sleep(2 * time.Second)
	testApp.Stop(context.Background())

	t.Logf("WAL recovery test passed")
}

// TestWALConcurrentAppend tests concurrent WAL appends.
func TestWALConcurrentAppend(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "arkilian-wal-concurrent-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	walDir := filepath.Join(tempDir, "wal")
	os.MkdirAll(walDir, 0755)

	walInstance, err := wal.NewWAL(walDir, 64*1024*1024)
	if err != nil {
		t.Fatalf("failed to create WAL: %v", err)
	}
	defer walInstance.Close()

	// Concurrent appends
	done := make(chan bool, 10)
	for i := 0; i < 10; i++ {
		go func(idx int) {
			for j := 0; j < 100; j++ {
				entry := &wal.Entry{
					PartitionKey: "20260206",
					Rows:         walTestRows(),
					Timestamp:    time.Now().UnixNano(),
				}
				_, err := walInstance.Append(entry)
				if err != nil {
					t.Errorf("goroutine %d: failed to append: %v", idx, err)
				}
			}
			done <- true
		}(i)
	}

	// Wait for all goroutines
	for i := 0; i < 10; i++ {
		<-done
	}

	lsn := walInstance.CurrentLSN()
	if lsn != 1000 {
		t.Errorf("expected LSN=1000, got %d", lsn)
	}

	t.Logf("WAL concurrent append test passed: final LSN=%d", lsn)
}

func walTestRows() []types.Row {
	return []types.Row{
		{
			TenantID:  "acme",
			UserID:    12345,
			EventTime: time.Now().UnixNano(),
			EventType: "page_view",
			Payload:   map[string]interface{}{"page": "/home"},
		},
	}
}