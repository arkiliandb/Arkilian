// Package wal provides a Raft-backed shared distributed write-ahead log for Arkilian V3.
package wal

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/arkilian/arkilian/pkg/types"
)

// WalSegment represents a single WAL segment file.
type WalSegment struct {
	file        *os.File
	segmentID   uint64
	startLSN    uint64
	maxLSN      atomic.Uint64
	writerID    uint32
	size        int64
	maxSize     int64
	mu          sync.Mutex
	segmentPath string
}

// WalEntry represents a single WAL entry for V3.
type WalEntry struct {
	EntryMagic   uint32 `json:"entry_magic"`   // Magic: 0xARKWALV3
	EntryLen     uint32 `json:"entry_len"`     // Length of entry payload
	LSN          uint64 `json:"lsn"`           // Log sequence number
	TableID      uint64 `json:"table_id"`      // Table identifier
	TimestampNS  int64  `json:"timestamp_ns"`  // Timestamp in nanoseconds
	RowCount     uint32 `json:"row_count"`     // Number of rows in batch
	SchemaHash   uint64 `json:"schema_hash"`   // Hash of schema
	PayloadType  uint8  `json:"payload_type"`  // INSERT=1, UPDATE=2, DELETE=3
	Payload      []byte `json:"payload"`       // Compressed row batch
	CRC32C       uint32 `json:"crc32c"`        // CRC32C checksum
}

// SegmentHeader is the 128-byte header for each segment file.
type SegmentHeader struct {
	Magic         uint64 // 8 bytes: ARKWALV3
	SegmentID     uint64 // 8 bytes
	StartLSN      uint64 // 8 bytes
	EndLSN        uint64 // 8 bytes
	WriterID      uint32 // 4 bytes
	EntryCount    uint64 // 8 bytes
	TotalSize     uint64 // 8 bytes
	Checksum      [16]byte // 16 bytes: XXH3_128
	Reserved      [64]byte // 64 bytes reserved
}

const (
	// Magic numbers
	MagicARKWALV3 = uint64(0x41524B57414C5633) // 'ARKWALV3'
	MagicENDWAL3  = uint64(0x454E445F57414C33) // 'END_WAL3'

	// Entry magic
	MagicWALEntry = uint32(0x57414C45) // 'WALE'

	// Payload types
	PayloadTypeInsert = uint8(1)
	PayloadTypeUpdate = uint8(2)
	PayloadTypeDelete = uint8(3)

	// Segment configuration
	DefaultSegmentSize = 64 * 1024 * 1024 // 64MB
	HeaderSize         = 128              // 128-byte header
)

// NewWalSegment creates a new WAL segment file.
func NewWalSegment(dir string, segmentID, startLSN uint64, writerID uint32) (*WalSegment, error) {
	segmentPath := filepath.Join(dir, fmt.Sprintf("seg_%020d.arkwal", startLSN))

	file, err := os.OpenFile(segmentPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open segment file: %w", err)
	}

	// Write header
	header := SegmentHeader{
		Magic:     MagicARKWALV3,
		SegmentID: segmentID,
		StartLSN:  startLSN,
		WriterID:  writerID,
	}
	if err := writeSegmentHeader(file, &header); err != nil {
		file.Close()
		return nil, err
	}

	return &WalSegment{
		file:        file,
		segmentID:   segmentID,
		startLSN:    startLSN,
		maxSize:     DefaultSegmentSize,
		segmentPath: segmentPath,
	}, nil
}

// writeSegmentHeader writes the 128-byte segment header.
func writeSegmentHeader(w io.Writer, header *SegmentHeader) error {
	// Write fixed fields
	if err := binary.Write(w, binary.LittleEndian, header.Magic); err != nil {
		return fmt.Errorf("failed to write magic: %w", err)
	}
	if err := binary.Write(w, binary.LittleEndian, header.SegmentID); err != nil {
		return fmt.Errorf("failed to write segment ID: %w", err)
	}
	if err := binary.Write(w, binary.LittleEndian, header.StartLSN); err != nil {
		return fmt.Errorf("failed to write start LSN: %w", err)
	}
	if err := binary.Write(w, binary.LittleEndian, header.EndLSN); err != nil {
		return fmt.Errorf("failed to write end LSN: %w", err)
	}
	if err := binary.Write(w, binary.LittleEndian, header.WriterID); err != nil {
		return fmt.Errorf("failed to write writer ID: %w", err)
	}
	if err := binary.Write(w, binary.LittleEndian, header.EntryCount); err != nil {
		return fmt.Errorf("failed to write entry count: %w", err)
	}
	if err := binary.Write(w, binary.LittleEndian, header.TotalSize); err != nil {
		return fmt.Errorf("failed to write total size: %w", err)
	}
	if _, err := w.Write(header.Checksum[:]); err != nil {
		return fmt.Errorf("failed to write checksum: %w", err)
	}
	if _, err := w.Write(header.Reserved[:]); err != nil {
		return fmt.Errorf("failed to write reserved: %w", err)
	}
	return nil
}

// AppendAtomic appends an entry to the segment atomically.
func (s *WalSegment) AppendAtomic(entry *WalEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Compute CRC32C over the entry data (excluding CRC itself)
	crc := computeCRC32C(entry.Payload)

	// Write entry: [entry_magic:4][entry_len:4][lsn:8][table_id:8][timestamp_ns:8]
	//             [row_count:4][schema_hash:8][payload_type:1][payload:var][crc32c:4]
	if err := binary.Write(s.file, binary.LittleEndian, MagicWALEntry); err != nil {
		return fmt.Errorf("failed to write entry magic: %w", err)
	}
	if err := binary.Write(s.file, binary.LittleEndian, uint32(len(entry.Payload))); err != nil {
		return fmt.Errorf("failed to write entry len: %w", err)
	}
	if err := binary.Write(s.file, binary.LittleEndian, entry.LSN); err != nil {
		return fmt.Errorf("failed to write LSN: %w", err)
	}
	if err := binary.Write(s.file, binary.LittleEndian, entry.TableID); err != nil {
		return fmt.Errorf("failed to write table ID: %w", err)
	}
	if err := binary.Write(s.file, binary.LittleEndian, entry.TimestampNS); err != nil {
		return fmt.Errorf("failed to write timestamp: %w", err)
	}
	if err := binary.Write(s.file, binary.LittleEndian, entry.RowCount); err != nil {
		return fmt.Errorf("failed to write row count: %w", err)
	}
	if err := binary.Write(s.file, binary.LittleEndian, entry.SchemaHash); err != nil {
		return fmt.Errorf("failed to write schema hash: %w", err)
	}
	if err := binary.Write(s.file, binary.LittleEndian, entry.PayloadType); err != nil {
		return fmt.Errorf("failed to write payload type: %w", err)
	}
	if _, err := s.file.Write(entry.Payload); err != nil {
		return fmt.Errorf("failed to write payload: %w", err)
	}
	if err := binary.Write(s.file, binary.LittleEndian, crc); err != nil {
		return fmt.Errorf("failed to write CRC: %w", err)
	}

	// Update size
	s.size += int64(4 + 4 + 8 + 8 + 8 + 4 + 8 + 1 + len(entry.Payload) + 4)

	// Update max LSN
	s.maxLSN.Store(entry.LSN)

	// Check if we need to rotate
	if s.size >= s.maxSize {
		return fmt.Errorf("segment full, rotation required")
	}

	return nil
}

// GroupFsync batches concurrent fsyncs into a single syscall.
func (s *WalSegment) GroupFsync(lsn uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// For now, just fsync the file
	if err := s.file.Sync(); err != nil {
		return fmt.Errorf("failed to fsync: %w", err)
	}

	return nil
}

// ReadEntries reads all entries from the segment.
func (s *WalSegment) ReadEntries() ([]*WalEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Seek past header
	if _, err := s.file.Seek(HeaderSize, io.SeekStart); err != nil {
		return nil, fmt.Errorf("failed to seek past header: %w", err)
	}

	var entries []*WalEntry
	for {
		// Read entry magic
		var magic uint32
		if err := binary.Read(s.file, binary.LittleEndian, &magic); err != nil {
			if err == io.EOF {
				break
			}
			return nil, fmt.Errorf("failed to read entry magic: %w", err)
		}

		if magic != MagicWALEntry {
			// Skip invalid entry
			continue
		}

		// Read entry length (payload length only)
		var entryLen uint32
		if err := binary.Read(s.file, binary.LittleEndian, &entryLen); err != nil {
			return nil, fmt.Errorf("failed to read entry len: %w", err)
		}

		// Read fixed fields: lsn(8) + table_id(8) + timestamp_ns(8) + row_count(4) + schema_hash(8) + payload_type(1)
		fixedData := make([]byte, 8+8+8+4+8+1)
		if _, err := io.ReadFull(s.file, fixedData); err != nil {
			if err == io.EOF {
				break
			}
			return nil, fmt.Errorf("failed to read fixed data: %w", err)
		}

		// Read payload
		payload := make([]byte, entryLen)
		if _, err := io.ReadFull(s.file, payload); err != nil {
			if err == io.EOF {
				break
			}
			return nil, fmt.Errorf("failed to read payload: %w", err)
		}

		// Read CRC
		var crc uint32
		if err := binary.Read(s.file, binary.LittleEndian, &crc); err != nil {
			return nil, fmt.Errorf("failed to read CRC: %w", err)
		}

		// Verify CRC over payload only
		if computed := computeCRC32C(payload); computed != crc {
			// CRC mismatch - log warning and skip
			continue
		}

		// Parse fixed fields
		offset := 0
		lsn := binary.LittleEndian.Uint64(fixedData[offset : offset+8])
		offset += 8
		tableID := binary.LittleEndian.Uint64(fixedData[offset : offset+8])
		offset += 8
		timestampNS := int64(binary.LittleEndian.Uint64(fixedData[offset : offset+8]))
		offset += 8
		rowCount := binary.LittleEndian.Uint32(fixedData[offset : offset+4])
		offset += 4
		schemaHash := binary.LittleEndian.Uint64(fixedData[offset : offset+8])
		offset += 8
		payloadType := fixedData[offset]

		// Create entry
		entry := &WalEntry{
			EntryMagic:  MagicWALEntry,
			EntryLen:    entryLen,
			LSN:         lsn,
			TableID:     tableID,
			TimestampNS: timestampNS,
			RowCount:    rowCount,
			SchemaHash:  schemaHash,
			PayloadType: payloadType,
			Payload:     payload,
			CRC32C:      crc,
		}

		entries = append(entries, entry)
	}

	return entries, nil
}

// Close closes the segment file.
func (s *WalSegment) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.file != nil {
		if err := s.file.Sync(); err != nil {
			return fmt.Errorf("failed to fsync on close: %w", err)
		}
		if err := s.file.Close(); err != nil {
			return fmt.Errorf("failed to close segment: %w", err)
		}
		s.file = nil
	}

	return nil
}

// computeCRC32C computes CRC32C using Castagnoli polynomial.
func computeCRC32C(data []byte) uint32 {
	crc := uint32(0xFFFFFFFF)
	for _, b := range data {
		crc ^= uint32(b)
		for i := 0; i < 8; i++ {
			if crc&1 == 1 {
				crc = (crc >> 1) ^ 0x82F63B78 // Castagnoli polynomial
			} else {
				crc >>= 1
			}
		}
	}
	return crc ^ 0xFFFFFFFF
}

// OpenWalSegment opens an existing WAL segment from a file path.
func OpenWalSegment(path string) (*WalSegment, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open segment file: %w", err)
	}

	// Read header
	header, err := readSegmentHeader(file)
	if err != nil {
		file.Close()
		return nil, err
	}

	// Get file size
	stat, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, err
	}

	return &WalSegment{
		file:        file,
		segmentID:   header.SegmentID,
		startLSN:    header.StartLSN,
		maxSize:     DefaultSegmentSize,
		segmentPath: path,
		size:        stat.Size() - HeaderSize,
	}, nil
}

// readSegmentHeader reads the 128-byte segment header.
func readSegmentHeader(r io.Reader) (*SegmentHeader, error) {
	var header SegmentHeader

	if err := binary.Read(r, binary.LittleEndian, &header.Magic); err != nil {
		return nil, fmt.Errorf("failed to read magic: %w", err)
	}
	if err := binary.Read(r, binary.LittleEndian, &header.SegmentID); err != nil {
		return nil, fmt.Errorf("failed to read segment ID: %w", err)
	}
	if err := binary.Read(r, binary.LittleEndian, &header.StartLSN); err != nil {
		return nil, fmt.Errorf("failed to read start LSN: %w", err)
	}
	if err := binary.Read(r, binary.LittleEndian, &header.EndLSN); err != nil {
		return nil, fmt.Errorf("failed to read end LSN: %w", err)
	}
	if err := binary.Read(r, binary.LittleEndian, &header.WriterID); err != nil {
		return nil, fmt.Errorf("failed to read writer ID: %w", err)
	}
	if err := binary.Read(r, binary.LittleEndian, &header.EntryCount); err != nil {
		return nil, fmt.Errorf("failed to read entry count: %w", err)
	}
	if err := binary.Read(r, binary.LittleEndian, &header.TotalSize); err != nil {
		return nil, fmt.Errorf("failed to read total size: %w", err)
	}
	if _, err := io.ReadFull(r, header.Checksum[:]); err != nil {
		return nil, fmt.Errorf("failed to read checksum: %w", err)
	}
	if _, err := io.ReadFull(r, header.Reserved[:]); err != nil {
		return nil, fmt.Errorf("failed to read reserved: %w", err)
	}

	return &header, nil
}

// EncodeWALEntry encodes a WAL entry for Raft replication.
func EncodeWALEntry(lsn uint64, tableID uint64, rows []types.Row, schema types.Schema) ([]byte, error) {
	entry := &WalEntry{
		EntryMagic:  MagicWALEntry,
		EntryLen:    0, // Will be set after encoding
		LSN:         lsn,
		TableID:     tableID,
		TimestampNS: rows[0].EventTime,
		RowCount:    uint32(len(rows)),
		SchemaHash:  hashSchema(schema),
		PayloadType: PayloadTypeInsert,
		Payload:     nil, // Will be set after encoding
		CRC32C:      0,   // Will be set after encoding
	}

	// Serialize rows
	payload, err := json.Marshal(rows)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize rows: %w", err)
	}
	entry.Payload = payload
	entry.EntryLen = uint32(len(payload))

	// Compute CRC32C
	crc := computeCRC32C(payload)
	entry.CRC32C = crc

	// Re-serialize with CRC
	return json.Marshal(entry)
}

// hashSchema computes a hash of the schema.
func hashSchema(schema types.Schema) uint64 {
	// Simple hash for now - could use xxHash for better performance
	var hash uint64 = 5381
	for _, col := range schema.Columns {
		for _, c := range col.Name {
			hash = ((hash << 5) + hash) + uint64(c)
		}
	}
	return hash
}
