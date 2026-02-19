// Package wal provides a write-ahead log for durable write acknowledgment before asynchronous S3 upload.
package wal

import (
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/arkilian/arkilian/pkg/types"
	"github.com/stretchr/testify/assert"
)

func TestWAL_AppendSingleEntry(t *testing.T) {
	dir := t.TempDir()
	wal, err := NewWAL(dir, 64*1024*1024)
	assert.NoError(t, err)
	defer wal.Close()

	entry := &Entry{
		LSN:          0,
		PartitionKey: "test-key",
		Rows: []types.Row{
			{
				EventID:   []byte("01ARZ3NDEKTSV4RRFFQ69G5FAV"),
				TenantID:  "tenant-1",
				UserID:    123,
				EventTime: 1640000000000000000,
				EventType: "test",
				Payload:   map[string]interface{}{"key": "value"},
			},
		},
		Schema: types.Schema{
			Version: 1,
			Columns: []types.ColumnDef{
				{Name: "event_id", Type: "TEXT", Nullable: false, PrimaryKey: true},
				{Name: "tenant_id", Type: "TEXT", Nullable: false, PrimaryKey: false},
				{Name: "user_id", Type: "INTEGER", Nullable: false, PrimaryKey: false},
				{Name: "event_time", Type: "INTEGER", Nullable: false, PrimaryKey: false},
				{Name: "event_type", Type: "TEXT", Nullable: false, PrimaryKey: false},
				{Name: "payload", Type: "BLOB", Nullable: false, PrimaryKey: false},
			},
		},
		Timestamp: 1640000000000000000,
	}

	lsn, err := wal.Append(entry)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), lsn)

	// Read back the entry
	segmentPath := filepath.Join(dir, "wal_0000000000000000.log")
	readEntries, err := ReadEntries(segmentPath)
	assert.NoError(t, err)
	assert.Len(t, readEntries, 1)
	// LSN is assigned by WAL, not stored in entry payload
	assert.Equal(t, "test-key", readEntries[0].PartitionKey)
	assert.Len(t, readEntries[0].Rows, 1)
}

func TestWAL_AppendMultipleEntries(t *testing.T) {
	dir := t.TempDir()
	wal, err := NewWAL(dir, 64*1024*1024)
	assert.NoError(t, err)
	defer wal.Close()

	// Append 1000 entries
	for i := 0; i < 1000; i++ {
		entry := &Entry{
			LSN:          0,
			PartitionKey: "test-key",
			Rows: []types.Row{
				{
					EventID:   []byte("01ARZ3NDEKTSV4RRFFQ69G5FAV"),
					TenantID:  "tenant-1",
					UserID:    123,
					EventTime: 1640000000000000000,
					EventType: "test",
					Payload:   map[string]interface{}{"index": i},
				},
			},
			Schema:    types.Schema{Version: 1},
			Timestamp: 1640000000000000000,
		}
		_, err := wal.Append(entry)
		assert.NoError(t, err)
	}

	// Read back all entries
	segmentPath := filepath.Join(dir, "wal_0000000000000000.log")
	readEntries, err := ReadEntries(segmentPath)
	assert.NoError(t, err)
	assert.Len(t, readEntries, 1000)

	// Verify order and payload (JSON unmarshals integers as float64)
	for i, e := range readEntries {
		payloadIndex := e.Rows[0].Payload["index"]
		var index int
		switch v := payloadIndex.(type) {
		case float64:
			index = int(v)
		case int:
			index = v
		}
		assert.Equal(t, i, index)
	}
}

func TestWAL_SegmentRotation(t *testing.T) {
	dir := t.TempDir()
	// Use a small max segment size to trigger rotation
	wal, err := NewWAL(dir, 1024)
	assert.NoError(t, err)
	defer wal.Close()

	// Append entries until rotation occurs
	// Each entry is approximately 200 bytes, so 10 entries should trigger rotation
	for i := 0; i < 10; i++ {
		entry := &Entry{
			LSN:          0,
			PartitionKey: "test-key",
			Rows: []types.Row{
				{
					EventID:   []byte("01ARZ3NDEKTSV4RRFFQ69G5FAV"),
					TenantID:  "tenant-1",
					UserID:    123,
					EventTime: 1640000000000000000,
					EventType: "test",
					Payload:   map[string]interface{}{"index": i, "large_field": "x"},
				},
			},
			Schema:    types.Schema{Version: 1},
			Timestamp: 1640000000000000000,
		}
		_, err := wal.Append(entry)
		assert.NoError(t, err)
	}

	// Check that we have at least 2 segments
	entries, err := os.ReadDir(dir)
	assert.NoError(t, err)
	segmentCount := 0
	for _, e := range entries {
		if e.Name()[:4] == "wal_" {
			segmentCount++
		}
	}
	assert.GreaterOrEqual(t, segmentCount, 2)
}

func TestWAL_CRCValidation(t *testing.T) {
	dir := t.TempDir()
	wal, err := NewWAL(dir, 64*1024*1024)
	assert.NoError(t, err)
	defer wal.Close()

	// Append an entry
	entry := &Entry{
		LSN:          0,
		PartitionKey: "test-key",
		Rows: []types.Row{
			{
				EventID:   []byte("01ARZ3NDEKTSV4RRFFQ69G5FAV"),
				TenantID:  "tenant-1",
				UserID:    123,
				EventTime: 1640000000000000000,
				EventType: "test",
				Payload:   map[string]interface{}{"key": "value"},
			},
		},
		Schema:    types.Schema{Version: 1},
		Timestamp: 1640000000000000000,
	}
	_, err = wal.Append(entry)
	assert.NoError(t, err)

	// Corrupt the CRC in the segment file
	segmentPath := filepath.Join(dir, "wal_0000000000000000.log")
	file, err := os.OpenFile(segmentPath, os.O_RDWR, 0644)
	assert.NoError(t, err)
	defer file.Close()

	// Read the entry header
	var length uint32
	binary.Read(file, binary.LittleEndian, &length)
	var crc uint32
	binary.Read(file, binary.LittleEndian, &crc)

	// Corrupt the CRC
	_, err = file.Seek(8, io.SeekStart)
	assert.NoError(t, err)
	binary.Write(file, binary.LittleEndian, crc^0xFFFFFFFF)

	// Try to read entries - should skip corrupted entry
	readEntries, err := ReadEntries(segmentPath)
	assert.NoError(t, err)
	// Corrupted entry should be skipped
	assert.Len(t, readEntries, 0)
}

func TestWAL_ConcurrentAppend(t *testing.T) {
	dir := t.TempDir()
	wal, err := NewWAL(dir, 64*1024*1024)
	assert.NoError(t, err)
	defer wal.Close()

	var wg sync.WaitGroup
	numGoroutines := 10
	entriesPerGoroutine := 100

	// Start 10 goroutines appending entries
	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			for i := 0; i < entriesPerGoroutine; i++ {
				entry := &Entry{
					LSN:          0,
					PartitionKey: "test-key",
					Rows: []types.Row{
						{
							EventID:   []byte("01ARZ3NDEKTSV4RRFFQ69G5FAV"),
							TenantID:  "tenant-1",
							UserID:    123,
							EventTime: 1640000000000000000,
							EventType: "test",
							Payload:   map[string]interface{}{"goroutine": goroutineID, "index": i},
						},
					},
					Schema:    types.Schema{Version: 1},
					Timestamp: 1640000000000000000,
				}
				_, err := wal.Append(entry)
				assert.NoError(t, err)
			}
		}(g)
	}

	wg.Wait()

	// Verify all entries were written
	segmentPath := filepath.Join(dir, "wal_0000000000000000.log")
	readEntries, err := ReadEntries(segmentPath)
	assert.NoError(t, err)
	assert.Len(t, readEntries, numGoroutines*entriesPerGoroutine)

	// Verify LSNs are monotonic by checking CurrentLSN
	assert.Equal(t, uint64(numGoroutines*entriesPerGoroutine), wal.CurrentLSN())
}

func TestWAL_CloseAndReopen(t *testing.T) {
	dir := t.TempDir()

	// Create WAL, append entries, and close
	wal1, err := NewWAL(dir, 64*1024*1024)
	assert.NoError(t, err)

	for i := 0; i < 10; i++ {
		entry := &Entry{
			LSN:          0,
			PartitionKey: "test-key",
			Rows: []types.Row{
				{
					EventID:   []byte("01ARZ3NDEKTSV4RRFFQ69G5FAV"),
					TenantID:  "tenant-1",
					UserID:    123,
					EventTime: 1640000000000000000,
					EventType: "test",
					Payload:   map[string]interface{}{"index": i},
				},
			},
			Schema:    types.Schema{Version: 1},
			Timestamp: 1640000000000000000,
		}
		_, err := wal1.Append(entry)
		assert.NoError(t, err)
	}

	lsnBeforeClose := wal1.CurrentLSN()
	assert.Equal(t, uint64(10), lsnBeforeClose)

	err = wal1.Close()
	assert.NoError(t, err)

	// Reopen WAL and verify LSN continues
	wal2, err := NewWAL(dir, 64*1024*1024)
	assert.NoError(t, err)
	defer wal2.Close()

	// The LSN should be restored from the segment file
	assert.Equal(t, uint64(10), wal2.CurrentLSN())

	// Append new entry and verify LSN increments
	entry := &Entry{
		LSN:          0,
		PartitionKey: "test-key",
		Rows: []types.Row{
			{
				EventID:   []byte("01ARZ3NDEKTSV4RRFFQ69G5FAV"),
				TenantID:  "tenant-1",
				UserID:    123,
				EventTime: 1640000000000000000,
				EventType: "test",
				Payload:   map[string]interface{}{"index": 100},
			},
		},
		Schema:    types.Schema{Version: 1},
		Timestamp: 1640000000000000000,
	}
	lsnAfterReopen, err := wal2.Append(entry)
	assert.NoError(t, err)
	assert.Equal(t, uint64(11), lsnAfterReopen)
	assert.Equal(t, uint64(11), wal2.CurrentLSN())
}
