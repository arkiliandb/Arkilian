// Package wal provides a write-ahead log for durable write acknowledgment before asynchronous S3 upload.
package wal

import (
	"context"
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/arkilian/arkilian/internal/manifest"
	"github.com/arkilian/arkilian/internal/partition"
	"github.com/arkilian/arkilian/internal/storage"
	"github.com/arkilian/arkilian/pkg/types"
	"github.com/stretchr/testify/assert"
)

func TestRecovery_ReplaysUnflushedEntries(t *testing.T) {
	dir := t.TempDir()
	walDir := filepath.Join(dir, "wal")
	storageDir := filepath.Join(dir, "storage")
	catalogDir := filepath.Join(dir, "catalog")
	builderDir := filepath.Join(dir, "builder")

	// Create WAL
	wal, err := NewWAL(walDir, 64*1024*1024)
	assert.NoError(t, err)
	defer wal.Close()

	// Create storage
	storage, err := storage.NewLocalStorage(storageDir)
	assert.NoError(t, err)

	// Create catalog
	catalog, err := manifest.NewCatalog(catalogDir)
	assert.NoError(t, err)
	defer catalog.Close()

	// Create builder
	builder := partition.NewBuilder(builderDir, 16)

	// Create metadata generator
	metaGen := partition.NewMetadataGenerator()

	// Create flusher with immediate interval for testing
	flusher := NewFlusher(wal, builder, storage, catalog, metaGen, 10*time.Millisecond, 10000)

	// Append 10 entries to WAL with same partition key to create one partition
	for i := 0; i < 10; i++ {
		entry := &Entry{
			LSN:          0,
			PartitionKey: "20220101",
			Rows: []types.Row{
				{
					EventID:   []byte{},
					TenantID:  "tenant-1",
					UserID:    int64(i),
					EventTime: 1640000000000000000 + int64(i),
					EventType: "test",
					Payload:   map[string]any{"index": i},
				},
			},
			Schema:    types.Schema{Version: 1},
			Timestamp: time.Now().UnixNano(),
		}
		_, err := wal.Append(entry)
		assert.NoError(t, err)
	}

	// Manually flush entries 1-5 by calling flushGroup directly
	// This simulates what the flusher would do
	entries, err := ReadEntries(filepath.Join(walDir, "wal_0000000000000000.log"))
	assert.NoError(t, err)
	assert.Len(t, entries, 10)

	// Flush entries 1-5 (LSN 1-5)
	flushEntries := entries[:5]
	err = flusher.FlushGroup(context.Background(), "20220101", flushEntries)
	assert.NoError(t, err)

	// Verify partition was created
	partitions, err := catalog.FindPartitions(context.Background(), nil)
	assert.NoError(t, err)
	assert.Len(t, partitions, 1)

	// Close WAL to simulate crash
	err = wal.Close()
	assert.NoError(t, err)

	// Create new WAL instance (simulating restart)
	wal2, err := NewWAL(walDir, 64*1024*1024)
	assert.NoError(t, err)
	defer wal2.Close()

	// Create new flusher
	flusher2 := NewFlusher(wal2, builder, storage, catalog, metaGen, 10*time.Millisecond, 10000)

	// Create recovery and run
	recovery := NewRecovery(wal2, flusher2, catalog)
	recoveredCount, err := recovery.Recover(context.Background())
	assert.NoError(t, err)

	// Should recover 5 entries (6-10)
	assert.Equal(t, 5, recoveredCount)

	// Verify 2 partitions were created:
	// - Manual flush created partition with idempotency key wal-lsn-5
	// - Recovery created partition with idempotency key wal-lsn-10
	// These are different keys, so both partitions are registered
	partitions, err = catalog.FindPartitions(context.Background(), nil)
	assert.NoError(t, err)
	assert.Len(t, partitions, 2)
}

func TestRecovery_SkipsCorruptEntries(t *testing.T) {
	dir := t.TempDir()
	walDir := filepath.Join(dir, "wal")
	storageDir := filepath.Join(dir, "storage")
	catalogDir := filepath.Join(dir, "catalog")
	builderDir := filepath.Join(dir, "builder")

	// Create WAL
	wal, err := NewWAL(walDir, 64*1024*1024)
	assert.NoError(t, err)

	// Append some entries
	for i := 0; i < 5; i++ {
		entry := &Entry{
			LSN:          0,
			PartitionKey: "20220101",
			Rows: []types.Row{
				{
					EventID:   []byte{},
					TenantID:  "tenant-1",
					UserID:    int64(i),
					EventTime: 1640000000000000000 + int64(i),
					EventType: "test",
					Payload:   map[string]any{"index": i},
				},
			},
			Schema:    types.Schema{Version: 1},
			Timestamp: time.Now().UnixNano(),
		}
		_, err := wal.Append(entry)
		assert.NoError(t, err)
	}

	// Corrupt the last entry's CRC
	segmentPath := filepath.Join(walDir, "wal_0000000000000000.log")
	file, err := os.OpenFile(segmentPath, os.O_RDWR, 0644)
	assert.NoError(t, err)
	defer file.Close()

	// Read all entries and find the last one
	file.Seek(0, 0)
	var lastOffset int64
	for {
		var length uint32
		err := binary.Read(file, binary.LittleEndian, &length)
		if err == io.EOF {
			break
		}
		assert.NoError(t, err)

		var crc uint32
		err = binary.Read(file, binary.LittleEndian, &crc)
		assert.NoError(t, err)

		// Skip payload
		_, err = file.Seek(int64(length), io.SeekCurrent)
		assert.NoError(t, err)

		lastOffset, err = file.Seek(0, io.SeekCurrent)
		assert.NoError(t, err)
		lastOffset -= 8 + int64(length) // Back up to start of entry
	}

	// Corrupt the CRC of the last entry
	_, err = file.Seek(lastOffset+4, io.SeekStart) // +4 to skip length field
	assert.NoError(t, err)
	binary.Write(file, binary.LittleEndian, uint32(0xDEADBEEF))

	err = wal.Close()
	assert.NoError(t, err)

	// Create storage and catalog
	storage, err := storage.NewLocalStorage(storageDir)
	assert.NoError(t, err)

	catalog, err := manifest.NewCatalog(catalogDir)
	assert.NoError(t, err)
	defer catalog.Close()

	builder := partition.NewBuilder(builderDir, 16)
	metaGen := partition.NewMetadataGenerator()

	// Create WAL instance for recovery
	wal2, err := NewWAL(walDir, 64*1024*1024)
	assert.NoError(t, err)
	defer wal2.Close()

	flusher := NewFlusher(wal2, builder, storage, catalog, metaGen, 10*time.Millisecond, 10000)
	recovery := NewRecovery(wal2, flusher, catalog)

	// Should recover 4 entries (5th is corrupt and skipped)
	recoveredCount, err := recovery.Recover(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, 4, recoveredCount)

	// Verify only 1 partition was created (4 entries with same partition key)
	partitions, err := catalog.FindPartitions(context.Background(), nil)
	assert.NoError(t, err)
	assert.Len(t, partitions, 1)
}

func TestRecovery_IdempotencyPreventsDuplicate(t *testing.T) {
	dir := t.TempDir()
	walDir := filepath.Join(dir, "wal")
	storageDir := filepath.Join(dir, "storage")
	catalogDir := filepath.Join(dir, "catalog")
	builderDir := filepath.Join(dir, "builder")

	// Create WAL
	wal, err := NewWAL(walDir, 64*1024*1024)
	assert.NoError(t, err)
	defer wal.Close()

	// Create storage
	storage, err := storage.NewLocalStorage(storageDir)
	assert.NoError(t, err)

	// Create catalog
	catalog, err := manifest.NewCatalog(catalogDir)
	assert.NoError(t, err)
	defer catalog.Close()

	// Create builder
	builder := partition.NewBuilder(builderDir, 16)

	// Create metadata generator
	metaGen := partition.NewMetadataGenerator()

	// Create flusher
	flusher := NewFlusher(wal, builder, storage, catalog, metaGen, 10*time.Millisecond, 10000)

	// Append an entry
	entry := &Entry{
		LSN:          0,
		PartitionKey: "20220101",
		Rows: []types.Row{
			{
				EventID:   []byte{},
				TenantID:  "tenant-1",
				UserID:    123,
				EventTime: 1640000000000000000,
				EventType: "test",
				Payload:   map[string]any{"key": "value"},
			},
		},
		Schema:    types.Schema{Version: 1},
		Timestamp: time.Now().UnixNano(),
	}
	_, err = wal.Append(entry)
	assert.NoError(t, err)

	// Manually flush this entry (simulate flusher running)
	// Need to read the entry from WAL first
	entries, err := ReadEntries(filepath.Join(walDir, "wal_0000000000000000.log"))
	assert.NoError(t, err)
	assert.Len(t, entries, 1)
	
	// Call FlushGroup to register the idempotency key
	err = flusher.FlushGroup(context.Background(), "20220101", entries)
	assert.NoError(t, err)

	// Simulate crash: close WAL without updating flusher's in-memory state
	err = wal.Close()
	assert.NoError(t, err)

	// Create new WAL instance (simulating restart)
	wal2, err := NewWAL(walDir, 64*1024*1024)
	assert.NoError(t, err)
	defer wal2.Close()

	// Create new flusher
	flusher2 := NewFlusher(wal2, builder, storage, catalog, metaGen, 10*time.Millisecond, 10000)

	// Create recovery and run
	recovery := NewRecovery(wal2, flusher2, catalog)
	recoveredCount, err := recovery.Recover(context.Background())
	assert.NoError(t, err)

	// Should recover 0 entries because idempotency key prevents duplicate
	assert.Equal(t, 0, recoveredCount)

	// Verify only 1 partition exists (no duplicate)
	partitions, err := catalog.FindPartitions(context.Background(), nil)
	assert.NoError(t, err)
	assert.Len(t, partitions, 1)
}
