package wal

import (
	"path/filepath"
	"testing"

	"github.com/arkilian/arkilian/internal/storage"
	"github.com/stretchr/testify/assert"
)

func TestWalCoordinator_Create(t *testing.T) {
	// Create temporary directory
	tmpDir := t.TempDir()
	dataDir := filepath.Join(tmpDir, "wal-coordinator-test")

	// Create S3 client (using local storage for testing)
	s3, err := storage.NewLocalStorage(filepath.Join(tmpDir, "s3"))
	assert.NoError(t, err)

	// Create coordinator with single node
	peers := []string{"node1"}
	coordinator, err := NewWalCoordinator(dataDir, peers, s3)
	assert.NoError(t, err)
	assert.NotNil(t, coordinator)

	// Verify coordinator state
	assert.Equal(t, uint64(0), coordinator.GetMaxLSN())

	// Close coordinator
	err = coordinator.Close()
	assert.NoError(t, err)

	// Verify directory structure
	assert.DirExists(t, dataDir)
	assert.DirExists(t, filepath.Join(dataDir, "wal"))
}

func TestWalCoordinator_MultiNode(t *testing.T) {
	// Create temporary directory
	tmpDir := t.TempDir()
	dataDir := filepath.Join(tmpDir, "wal-coordinator-multinode")

	// Create S3 client
	s3, err := storage.NewLocalStorage(filepath.Join(tmpDir, "s3"))
	assert.NoError(t, err)

	// Create coordinator with multiple nodes
	peers := []string{"node1", "node2", "node3"}
	coordinator, err := NewWalCoordinator(dataDir, peers, s3)
	assert.NoError(t, err)
	assert.NotNil(t, coordinator)

	// Close coordinator
	err = coordinator.Close()
	assert.NoError(t, err)
}

func TestWalCoordinator_EmptyPeers(t *testing.T) {
	// Create temporary directory
	tmpDir := t.TempDir()
	dataDir := filepath.Join(tmpDir, "wal-coordinator-empty")

	// Create S3 client
	s3, err := storage.NewLocalStorage(filepath.Join(tmpDir, "s3"))
	assert.NoError(t, err)

	// Create coordinator with empty peers - should fail
	peers := []string{}
	_, err = NewWalCoordinator(dataDir, peers, s3)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "at least one Raft peer is required")
}

func TestWalCoordinator_Append(t *testing.T) {
	// Create temporary directory
	tmpDir := t.TempDir()
	dataDir := filepath.Join(tmpDir, "wal-coordinator-append")

	// Create S3 client
	s3, err := storage.NewLocalStorage(filepath.Join(tmpDir, "s3"))
	assert.NoError(t, err)

	// Create coordinator
	peers := []string{"node1"}
	coordinator, err := NewWalCoordinator(dataDir, peers, s3)
	assert.NoError(t, err)
	defer coordinator.Close()

	// Append a batch of rows
	rows := []interface{}{
		map[string]interface{}{"key": "value1"},
		map[string]interface{}{"key": "value2"},
	}
	lsn, err := coordinator.Append(1, rows)
	assert.NoError(t, err)
	assert.Equal(t, uint64(2), lsn)

	// Verify LSN increased
	assert.Equal(t, uint64(2), coordinator.GetMaxLSN())

	// Append another batch
	rows2 := []interface{}{
		map[string]interface{}{"key": "value3"},
	}
	lsn2, err := coordinator.Append(1, rows2)
	assert.NoError(t, err)
	assert.Equal(t, uint64(3), lsn2)

	// Verify LSN increased again
	assert.Equal(t, uint64(3), coordinator.GetMaxLSN())
}

func TestWalCoordinator_SegmentRotation(t *testing.T) {
	// Create temporary directory
	tmpDir := t.TempDir()
	dataDir := filepath.Join(tmpDir, "wal-coordinator-rotation")

	// Create S3 client
	s3, err := storage.NewLocalStorage(filepath.Join(tmpDir, "s3"))
	assert.NoError(t, err)

	// Create coordinator
	peers := []string{"node1"}
	coordinator, err := NewWalCoordinator(dataDir, peers, s3)
	assert.NoError(t, err)
	defer coordinator.Close()

	// Append many rows
	for i := 0; i < 1000; i++ {
		rows := []interface{}{
			map[string]interface{}{"key": i},
		}
		_, err := coordinator.Append(1, rows)
		assert.NoError(t, err)
	}

	// Verify segments directory exists
	walDir := filepath.Join(dataDir, "wal")
	assert.DirExists(t, walDir)

	// The test verifies that the coordinator can handle many appends
	// Segment rotation only happens at 64MB threshold, which we don't reach in this test
}
