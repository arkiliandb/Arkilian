// Package wal provides a Raft-backed shared distributed write-ahead log for Arkilian V3.
package wal

import (
	"fmt"
	"os"
	"sync"
	"time"
)

// GroupFsyncer batches concurrent fsyncs into a single syscall.
// At 5M rows/sec, individual fsyncs would saturate NVMe IOPS. Group commit reduces by 100-1000×.
type GroupFsyncer struct {
	file        *os.File
	pendingMu   sync.Mutex
	pendingLSNs []uint64
	flushedCh   chan struct{}
	fsyncTrigger chan struct{}
	shutdownCh  chan struct{}
}

// NewGroupFsyncer creates a new group fsyncer.
func NewGroupFsyncer(file *os.File) *GroupFsyncer {
	g := &GroupFsyncer{
		file:         file,
		pendingLSNs:  make([]uint64, 0),
		flushedCh:    make(chan struct{}, 1),
		fsyncTrigger: make(chan struct{}, 1),
		shutdownCh:   make(chan struct{}),
	}

	// Start background fsync goroutine
	go g.backgroundFsync()

	return g
}

// Fsync batches concurrent fsyncs into a single syscall.
// Returns when the fsync for the given LSN is complete.
func (g *GroupFsyncer) Fsync(lsn uint64) error {
	g.pendingMu.Lock()
	g.pendingLSNs = append(g.pendingLSNs, lsn)
	g.pendingMu.Unlock()

	// Trigger fsync
	select {
	case g.fsyncTrigger <- struct{}{}:
	default:
		// Fsync already pending
	}

	// Wait for fsync to complete
	select {
	case <-g.flushedCh:
		return nil
	case <-time.After(5 * time.Second):
		return fmt.Errorf("fsync timeout")
	}
}

// backgroundFsync runs in a goroutine and batches fsyncs.
func (g *GroupFsyncer) backgroundFsync() {
	ticker := time.NewTicker(1 * time.Millisecond)
	defer ticker.Stop()

	var pending []uint64

	for {
		select {
		case <-g.fsyncTrigger:
			// Collect pending LSNs
			g.pendingMu.Lock()
			pending = append(pending, g.pendingLSNs...)
			g.pendingLSNs = g.pendingLSNs[:0]
			g.pendingMu.Unlock()

			// Fsync
			if err := g.file.Sync(); err != nil {
				// Log error but continue
				continue
			}

			// Signal completion
			select {
			case g.flushedCh <- struct{}{}:
			default:
			}

		case <-ticker.C:
			// Periodic fsync to ensure no entries are stuck
			g.pendingMu.Lock()
			if len(g.pendingLSNs) > 0 {
				pending = append(pending, g.pendingLSNs...)
				g.pendingLSNs = g.pendingLSNs[:0]
			}
			g.pendingMu.Unlock()

			if len(pending) > 0 {
				if err := g.file.Sync(); err != nil {
					continue
				}

				select {
				case g.flushedCh <- struct{}{}:
				default:
				}
			}

		case <-g.shutdownCh:
			return
		}
	}
}

// Close shuts down the group fsyncer.
func (g *GroupFsyncer) Close() error {
	close(g.shutdownCh)
	return nil
}
