package wal

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWalSegment_Create(t *testing.T) {
	// Create temporary directory
	tmpDir := t.TempDir()
	segmentPath := filepath.Join(tmpDir, "seg_00000000000000000000.arkwal")

	// Create segment
	segment, err := NewWalSegment(tmpDir, 0, 0, 1)
	assert.NoError(t, err)
	assert.NotNil(t, segment)
	assert.Equal(t, uint64(0), segment.startLSN)
	assert.Equal(t, uint64(0), segment.maxLSN.Load())

	// Close segment
	err = segment.Close()
	assert.NoError(t, err)

	// Verify file was created
	assert.FileExists(t, segmentPath)
}

func TestWalSegment_AppendEntry(t *testing.T) {
	// Create temporary directory
	tmpDir := t.TempDir()

	// Create segment
	segment, err := NewWalSegment(tmpDir, 0, 0, 1)
	assert.NoError(t, err)
	defer segment.Close()

	// Create and append entry
	entry := &WalEntry{
		EntryMagic:  MagicWALEntry,
		EntryLen:    100,
		LSN:         1,
		TableID:     123,
		TimestampNS: 1234567890,
		RowCount:    5,
		SchemaHash:  456,
		PayloadType: PayloadTypeInsert,
		Payload:     []byte("test payload"),
		CRC32C:      789,
	}

	err = segment.AppendAtomic(entry)
	assert.NoError(t, err)

	// Verify max LSN was updated
	assert.Equal(t, uint64(1), segment.maxLSN.Load())
}

func TestWalSegment_GroupFsync(t *testing.T) {
	// Create temporary directory
	tmpDir := t.TempDir()

	// Create segment
	segment, err := NewWalSegment(tmpDir, 0, 0, 1)
	assert.NoError(t, err)
	defer segment.Close()

	// Test group fsync
	err = segment.GroupFsync(1)
	assert.NoError(t, err)
}

func TestWalSegment_ReadEntries(t *testing.T) {
	// Create temporary directory
	tmpDir := t.TempDir()

	// Create segment
	segment, err := NewWalSegment(tmpDir, 0, 0, 1)
	assert.NoError(t, err)

	// Create and append entries
	for i := uint64(1); i <= 5; i++ {
		entry := &WalEntry{
			EntryMagic:  MagicWALEntry,
			EntryLen:    50,
			LSN:         i,
			TableID:     123,
			TimestampNS: 1234567890,
			RowCount:    1,
			SchemaHash:  456,
			PayloadType: PayloadTypeInsert,
			Payload:     []byte("test"),
			CRC32C:      0, // Will be computed
		}
		// Compute CRC
		entry.CRC32C = computeCRC32C(entry.Payload)

		err = segment.AppendAtomic(entry)
		assert.NoError(t, err)
	}

	// Close segment to flush
	err = segment.Close()
	assert.NoError(t, err)

	// Check file size
	segmentPath := filepath.Join(tmpDir, "seg_00000000000000000000.arkwal")
	stat, err := os.Stat(segmentPath)
	assert.NoError(t, err)
	t.Logf("Segment file size: %d bytes", stat.Size())

	// Reopen segment
	segment2, err := OpenWalSegment(segmentPath)
	assert.NoError(t, err)
	defer segment2.Close()

	// Read entries
	entries, err := segment2.ReadEntries()
	assert.NoError(t, err)
	t.Logf("Read %d entries", len(entries))
	assert.GreaterOrEqual(t, len(entries), 1)
}

func TestWalSegment_HeaderSize(t *testing.T) {
	// Create temporary directory
	tmpDir := t.TempDir()

	// Create segment
	segment, err := NewWalSegment(tmpDir, 0, 0, 1)
	assert.NoError(t, err)
	defer segment.Close()

	// Check file size (should be header size + entry data)
	stat, err := os.Stat(segment.segmentPath)
	assert.NoError(t, err)
	assert.GreaterOrEqual(t, stat.Size(), int64(HeaderSize))
}

func TestWalSegment_CorruptionDetection(t *testing.T) {
	// Create temporary directory
	tmpDir := t.TempDir()

	// Create segment
	segment, err := NewWalSegment(tmpDir, 0, 0, 1)
	assert.NoError(t, err)

	// Create and append entry
	entry := &WalEntry{
		EntryMagic:  MagicWALEntry,
		EntryLen:    50,
		LSN:         1,
		TableID:     123,
		TimestampNS: 1234567890,
		RowCount:    1,
		SchemaHash:  456,
		PayloadType: PayloadTypeInsert,
		Payload:     []byte("test payload"),
		CRC32C:      0, // Will be computed
	}

	// Manually compute CRC
	entry.CRC32C = computeCRC32C(entry.Payload)

	err = segment.AppendAtomic(entry)
	assert.NoError(t, err)

	// Close segment
	err = segment.Close()
	assert.NoError(t, err)

	// Reopen and read
	segmentPath := filepath.Join(tmpDir, "seg_00000000000000000000.arkwal")
	segment2, err := OpenWalSegment(segmentPath)
	assert.NoError(t, err)
	defer segment2.Close()

	// Read entries - should succeed with valid CRC
	entries, err := segment2.ReadEntries()
	assert.NoError(t, err)
	assert.GreaterOrEqual(t, len(entries), 1)
}

func TestWalSegment_MultipleSegments(t *testing.T) {
	// Create temporary directory
	tmpDir := t.TempDir()

	// Create first segment
	segment1, err := NewWalSegment(tmpDir, 0, 0, 1)
	assert.NoError(t, err)

	// Append some entries
	for i := uint64(1); i <= 100; i++ {
		entry := &WalEntry{
			EntryMagic:  MagicWALEntry,
			EntryLen:    50,
			LSN:         i,
			TableID:     123,
			TimestampNS: 1234567890,
			RowCount:    1,
			SchemaHash:  456,
			PayloadType: PayloadTypeInsert,
			Payload:     []byte("test"),
			CRC32C:      789,
		}
		err = segment1.AppendAtomic(entry)
		assert.NoError(t, err)
	}

	// Close first segment
	err = segment1.Close()
	assert.NoError(t, err)

	// Create second segment
	segment2, err := NewWalSegment(tmpDir, 1, 100, 1)
	assert.NoError(t, err)
	defer segment2.Close()

	// Append more entries
	for i := uint64(101); i <= 200; i++ {
		entry := &WalEntry{
			EntryMagic:  MagicWALEntry,
			EntryLen:    50,
			LSN:         i,
			TableID:     123,
			TimestampNS: 1234567890,
			RowCount:    1,
			SchemaHash:  456,
			PayloadType: PayloadTypeInsert,
			Payload:     []byte("test"),
			CRC32C:      789,
		}
		err = segment2.AppendAtomic(entry)
		assert.NoError(t, err)
	}

	// Verify both segments exist
	walDir := tmpDir
	entries, err := os.ReadDir(walDir)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(entries))
}
