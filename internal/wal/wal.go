// Package wal provides a write-ahead log for durable write acknowledgment before asynchronous S3 upload.
package wal

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/arkilian/arkilian/pkg/types"
)

// WAL provides a write-ahead log for durable write acknowledgment.
type WAL struct {
	dir        string
	segment    *os.File
	segmentID  uint64
	offset     int64
	maxSegSize int64
	currentLSN uint64
	mu         sync.Mutex
}

// Entry represents a single WAL entry.
type Entry struct {
	LSN          uint64 `json:"lsn"`
	PartitionKey string `json:"partition_key"`
	Rows         []types.Row `json:"rows"`
	Schema       types.Schema `json:"schema"`
	Checksum     uint32 `json:"checksum"`
	Timestamp    int64 `json:"timestamp"`
}

// NewWAL creates a new WAL instance, creating the directory if it doesn't exist.
func NewWAL(dir string, maxSegSize int64) (*WAL, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create WAL directory: %w", err)
	}

	w := &WAL{
		dir:        dir,
		maxSegSize: maxSegSize,
	}

	// Find existing segments to determine the highest segmentID
	if err := w.findLastSegment(); err != nil {
		return nil, err
	}

	// Open or create the first segment
	if err := w.openSegment(); err != nil {
		return nil, err
	}

	return w, nil
}

// findLastSegment finds the highest segmentID from existing WAL files.
func (w *WAL) findLastSegment() error {
	files, err := os.ReadDir(w.dir)
	if err != nil {
		return fmt.Errorf("failed to read WAL directory: %w", err)
	}

	var lastSegmentID uint64 = 0
	for _, file := range files {
		if file.IsDir() {
			continue
		}
		name := file.Name()
		if len(name) < 24 || name[:4] != "wal_" {
			continue
		}
		// Extract segmentID from filename: wal_{segmentID:016x}.log
		var segmentID uint64
		_, err := fmt.Sscanf(name[4:20], "%016x", &segmentID)
		if err == nil && segmentID >= lastSegmentID {
			lastSegmentID = segmentID
		}
	}

	w.segmentID = lastSegmentID

	// If we found segments, get the current offset and LSN from the last segment
	if lastSegmentID >= 0 {
		segmentPath := filepath.Join(w.dir, fmt.Sprintf("wal_%016x.log", lastSegmentID))
		
		// Check if file exists before opening
		if _, err := os.Stat(segmentPath); os.IsNotExist(err) {
			return nil
		}
		
		file, err := os.Open(segmentPath)
		if err != nil {
			return fmt.Errorf("failed to open last segment: %w", err)
		}
		defer file.Close()

		stat, err := file.Stat()
		if err != nil {
			return fmt.Errorf("failed to stat segment: %w", err)
		}
		w.offset = stat.Size()

		// Read the last LSN from the segment
		if w.offset > 0 {
			if lsn, err := w.readLastLSN(file); err == nil {
				w.currentLSN = lsn
			}
		}
	}

	return nil
}

// readLastLSN reads the LSN from the last entry in a segment.
func (w *WAL) readLastLSN(file *os.File) (uint64, error) {
	// Read all entries and return the last LSN
	_, err := file.Seek(0, io.SeekStart)
	if err != nil {
		return 0, err
	}

	var lastLSN uint64
	for {
		var length uint32
		if err := binary.Read(file, binary.LittleEndian, &length); err != nil {
			if err == io.EOF {
				break
			}
			return 0, err
		}

		var crc uint32
		if err := binary.Read(file, binary.LittleEndian, &crc); err != nil {
			return 0, err
		}

		payload := make([]byte, length)
		if _, err := io.ReadFull(file, payload); err != nil {
			if err == io.EOF {
				break
			}
			return 0, err
		}

		var entry Entry
		if err := json.Unmarshal(payload, &entry); err != nil {
			continue
		}

		lastLSN = entry.LSN
	}

	return lastLSN, nil
}

// openSegment opens the current segment file for writing.
func (w *WAL) openSegment() error {
	segmentPath := filepath.Join(w.dir, fmt.Sprintf("wal_%016x.log", w.segmentID))

	file, err := os.OpenFile(segmentPath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return fmt.Errorf("failed to open segment file: %w", err)
	}

	// Seek to end to get current offset
	offset, err := file.Seek(0, io.SeekEnd)
	if err != nil {
		file.Close()
		return fmt.Errorf("failed to seek segment: %w", err)
	}

	w.segment = file
	w.offset = offset

	return nil
}

// Append adds an entry to the WAL and returns its LSN.
func (w *WAL) Append(entry *Entry) (uint64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Assign LSN to entry before serialization
	w.currentLSN++
	entry.LSN = w.currentLSN

	// Serialize entry to JSON (including LSN)
	payload, err := json.Marshal(entry)
	if err != nil {
		return 0, fmt.Errorf("failed to serialize entry: %w", err)
	}

	// Compute CRC32 (IEEE polynomial)
	crc := computeCRC32(payload)

	// Write to segment: [length:4][crc32:4][payload:length]
	if err := w.writeEntry(uint32(len(payload)), crc, payload); err != nil {
		return 0, err
	}

	return w.currentLSN, nil
}

// writeEntry writes a single entry to the segment.
func (w *WAL) writeEntry(length uint32, crc uint32, payload []byte) error {
	// Write length (4 bytes LE)
	if err := binary.Write(w.segment, binary.LittleEndian, length); err != nil {
		return fmt.Errorf("failed to write length: %w", err)
	}

	// Write CRC (4 bytes LE)
	if err := binary.Write(w.segment, binary.LittleEndian, crc); err != nil {
		return fmt.Errorf("failed to write CRC: %w", err)
	}

	// Write payload
	if _, err := w.segment.Write(payload); err != nil {
		return fmt.Errorf("failed to write payload: %w", err)
	}

	// Fsync for durability
	if err := w.segment.Sync(); err != nil {
		return fmt.Errorf("failed to fsync: %w", err)
	}

	w.offset += int64(8 + len(payload))

	// Check if we need to rotate segment
	if w.offset >= w.maxSegSize {
		if err := w.RotateSegment(); err != nil {
			return err
		}
	}

	return nil
}

// RotateSegment closes the current segment and opens a new one.
func (w *WAL) RotateSegment() error {
	// Close current segment
	if w.segment != nil {
		if err := w.segment.Close(); err != nil {
			return fmt.Errorf("failed to close segment: %w", err)
		}
	}

	// Increment segment ID
	w.segmentID++

	// Open new segment
	if err := w.openSegment(); err != nil {
		return err
	}

	return nil
}

// CurrentLSN returns the current LSN.
func (w *WAL) CurrentLSN() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.currentLSN
}

// Close closes the WAL and fsyncs the current segment.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.segment != nil {
		if err := w.segment.Sync(); err != nil {
			return fmt.Errorf("failed to fsync on close: %w", err)
		}
		if err := w.segment.Close(); err != nil {
			return fmt.Errorf("failed to close segment: %w", err)
		}
		w.segment = nil
	}

	return nil
}

// computeCRC32 computes CRC32 using IEEE polynomial.
func computeCRC32(data []byte) uint32 {
	crc := uint32(0xFFFFFFFF)
	for _, b := range data {
		crc ^= uint32(b)
		for i := 0; i < 8; i++ {
			if crc&1 == 1 {
				crc = (crc >> 1) ^ 0xEDB88320
			} else {
				crc >>= 1
			}
		}
	}
	return crc ^ 0xFFFFFFFF
}

// ReadEntries reads all entries from a segment file.
func ReadEntries(segmentPath string) ([]*Entry, error) {
	file, err := os.Open(segmentPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open segment: %w", err)
	}
	defer file.Close()

	var entries []*Entry
	for {
		var length uint32
		if err := binary.Read(file, binary.LittleEndian, &length); err != nil {
			if err == io.EOF {
				break
			}
			return nil, fmt.Errorf("failed to read length: %w", err)
		}

		var crc uint32
		if err := binary.Read(file, binary.LittleEndian, &crc); err != nil {
			return nil, fmt.Errorf("failed to read CRC: %w", err)
		}

		payload := make([]byte, length)
		if _, err := io.ReadFull(file, payload); err != nil {
			// Truncated write - stop reading
			break
		}

		// Verify CRC
		if computed := computeCRC32(payload); computed != crc {
			// CRC mismatch - log warning and skip
			offset, _ := file.Seek(0, io.SeekCurrent)
			fmt.Printf("WAL: CRC mismatch at offset %d in %s, skipping entry\n", offset-int64(length+8), segmentPath)
			continue
		}

		var entry Entry
		if err := json.Unmarshal(payload, &entry); err != nil {
			continue
		}

		entries = append(entries, &entry)
	}

	return entries, nil
}
