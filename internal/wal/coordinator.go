// Package wal provides a Raft-backed shared distributed write-ahead log for Arkilian V3.
package wal

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/arkilian/arkilian/internal/storage"
	"github.com/hashicorp/go-hclog"
)

// WalCoordinator is the Raft-backed WAL coordinator that handles distributed writes.
// This is a placeholder implementation that will be expanded with full Raft support.
type WalCoordinator struct {
	lsnCounter    atomic.Uint64
	activeSegment *WalSegment
	segmentMu     sync.RWMutex
	s3client      storage.ObjectStorage
	replicaSet    []string
	dataDir       string
	logger        hclog.Logger
	shutdownCh    chan struct{}
	shutdownOnce  sync.Once
}

// NewWalCoordinator creates a new WAL coordinator.
// This is a placeholder implementation that will be expanded with full Raft support.
func NewWalCoordinator(dataDir string, peers []string, s3 storage.ObjectStorage) (*WalCoordinator, error) {
	if len(peers) == 0 {
		return nil, fmt.Errorf("at least one Raft peer is required")
	}

	// Create data directory
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory: %w", err)
	}

	// Create WAL segment directory
	walDir := filepath.Join(dataDir, "wal")
	if err := os.MkdirAll(walDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create WAL directory: %w", err)
	}

	// Initialize logger
	logger := hclog.New(&hclog.LoggerOptions{
		Name:   "wal-coordinator",
		Level:  hclog.Info,
		Output: os.Stdout,
	})

	// Create coordinator
	coordinator := &WalCoordinator{
		dataDir:    dataDir,
		s3client:   s3,
		replicaSet: peers,
		logger:     logger,
		shutdownCh: make(chan struct{}),
	}

	// Open first WAL segment
	if err := coordinator.openFirstSegment(); err != nil {
		return nil, fmt.Errorf("failed to open first segment: %w", err)
	}

	return coordinator, nil
}

// openFirstSegment opens the first WAL segment or recovers from existing segments.
func (c *WalCoordinator) openFirstSegment() error {
	c.segmentMu.Lock()
	defer c.segmentMu.Unlock()

	// Find existing segments
	segments, err := c.findSegments()
	if err != nil {
		return err
	}

	if len(segments) > 0 {
		// Recover from last segment
		lastSegment := segments[len(segments)-1]
		c.activeSegment = lastSegment
		c.lsnCounter.Store(lastSegment.maxLSN.Load())
	} else {
		// Create new segment
		segment, err := NewWalSegment(c.dataDir, 0, 0, 1)
		if err != nil {
			return err
		}
		c.activeSegment = segment
	}

	return nil
}

// findSegments finds all existing WAL segments in the data directory.
func (c *WalCoordinator) findSegments() ([]*WalSegment, error) {
	walDir := filepath.Join(c.dataDir, "wal")
	files, err := os.ReadDir(walDir)
	if err != nil {
		return nil, fmt.Errorf("failed to read WAL directory: %w", err)
	}

	var segments []*WalSegment
	for _, file := range files {
		if file.IsDir() {
			continue
		}
		name := file.Name()
		if len(name) < 24 || name[:4] != "seg_" {
			continue
		}
		// Parse segment file: seg_{start_lsn:020d}.arkwal
		var startLSN uint64
		_, err := fmt.Sscanf(name[4:24], "%020d", &startLSN)
		if err != nil {
			continue
		}
		segmentPath := filepath.Join(walDir, name)
		segment, err := OpenWalSegment(segmentPath)
		if err != nil {
			c.logger.Warn("failed to open segment", "path", segmentPath, "error", err)
			continue
		}
		segments = append(segments, segment)
	}

	// Sort segments by start LSN
	// TODO: Sort segments

	return segments, nil
}

// Append adds a batch of rows to the WAL and returns the assigned LSN.
// This is the hot path for writes.
func (c *WalCoordinator) Append(tableID uint64, rows []interface{}) (uint64, error) {
	// 1. Atomically acquire LSN
	lsn := c.lsnCounter.Add(uint64(len(rows)))

	// 2. Create WAL entry
	entry := &WalEntry{
		EntryMagic:  MagicWALEntry,
		EntryLen:    0,
		LSN:         lsn,
		TableID:     tableID,
		TimestampNS: time.Now().UnixNano(),
		RowCount:    uint32(len(rows)),
		SchemaHash:  0, // TODO: Compute schema hash
		PayloadType: PayloadTypeInsert,
		Payload:     nil,
		CRC32C:      0,
	}

	// 3. Write to local WAL segment
	c.segmentMu.RLock()
	segment := c.activeSegment
	c.segmentMu.RUnlock()

	if err := segment.AppendAtomic(entry); err != nil {
		return 0, fmt.Errorf("failed to write to segment: %w", err)
	}

	// 4. Group fsync
	if err := segment.GroupFsync(lsn); err != nil {
		return 0, fmt.Errorf("failed to fsync: %w", err)
	}

	// 5. Async: upload to S3 + notify read nodes
	go c.asyncUploadToS3(lsn)

	return lsn, nil
}

// asyncUploadToS3 uploads committed segments to S3 asynchronously.
func (c *WalCoordinator) asyncUploadToS3(lsn uint64) {
	// TODO: Implement S3 upload
	c.logger.Info("async upload to S3", "lsn", lsn)
}

// GetMaxLSN returns the current maximum LSN.
func (c *WalCoordinator) GetMaxLSN() uint64 {
	return c.lsnCounter.Load()
}

// Close shuts down the WAL coordinator.
func (c *WalCoordinator) Close() error {
	var err error
	c.shutdownOnce.Do(func() {
		// Close active segment
		c.segmentMu.RLock()
		if c.activeSegment != nil {
			if closeErr := c.activeSegment.Close(); closeErr != nil {
				if err == nil {
					err = closeErr
				} else {
					err = fmt.Errorf("%w; failed to close segment: %v", err, closeErr)
				}
			}
		}
		c.segmentMu.RUnlock()

		// Close shutdown channel
		close(c.shutdownCh)
	})

	return err
}
