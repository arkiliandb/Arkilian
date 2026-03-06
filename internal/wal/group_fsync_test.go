package wal

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestGroupFsync_Basic(t *testing.T) {
	// Create temporary file
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "test.fsync")
	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	assert.NoError(t, err)
	defer os.Remove(filePath)

	// Create group fsyncer
	g := NewGroupFsyncer(file)
	defer g.Close()

	// Test basic fsync
	err = g.Fsync(1)
	assert.NoError(t, err)
}

func TestGroupFsync_Concurrent(t *testing.T) {
	// Create temporary file
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "test_concurrent.fsync")
	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	assert.NoError(t, err)
	defer os.Remove(filePath)

	// Create group fsyncer
	g := NewGroupFsyncer(file)
	defer g.Close()

	// Run concurrent fsyncs
	var wg sync.WaitGroup
	numGoroutines := 100

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(lsn uint64) {
			defer wg.Done()
			err := g.Fsync(lsn)
			assert.NoError(t, err)
		}(uint64(i + 1))
	}

	// Wait for all fsyncs to complete
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Success
	case <-time.After(10 * time.Second):
		t.Fatal("fsync timeout")
	}
}

func TestGroupFsync_Batching(t *testing.T) {
	// Create temporary file
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "test_batch.fsync")
	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	assert.NoError(t, err)
	defer os.Remove(filePath)

	// Create group fsyncer
	g := NewGroupFsyncer(file)
	defer g.Close()

	// Write some data to the file
	_, err = file.WriteString("test data\n")
	assert.NoError(t, err)

	// Trigger multiple fsyncs in quick succession
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(lsn uint64) {
			defer wg.Done()
			_ = g.Fsync(lsn)
		}(uint64(i + 1))
	}

	// Wait for all fsyncs
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Success
	case <-time.After(5 * time.Second):
		t.Fatal("fsync timeout")
	}
}

func TestGroupFsync_SyncInterval(t *testing.T) {
	// Create temporary file
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "test_interval.fsync")
	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	assert.NoError(t, err)
	defer os.Remove(filePath)

	// Create group fsyncer with 1ms sync interval
	g := NewGroupFsyncer(file)
	defer g.Close()

	// Write data
	_, err = file.WriteString("test data\n")
	assert.NoError(t, err)

	// Wait for periodic sync
	time.Sleep(15 * time.Millisecond)

	// Trigger fsync
	err = g.Fsync(1)
	assert.NoError(t, err)
}
