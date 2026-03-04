// Package wal provides a write-ahead log for durable write acknowledgment before asynchronous S3 upload.
package wal

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/arkilian/arkilian/pkg/types"
	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/gen"
	"github.com/leanovate/gopter/prop"
)

// TestProperty_WAL_Durability tests Property V2-1: WAL Durability
// For any sequence of Append calls, all entries are recoverable after close+reopen
func TestProperty_WAL_Durability(t *testing.T) {
	parameters := gopter.DefaultTestParameters()
	parameters.MinSuccessfulTests = 100

	properties := gopter.NewProperties(parameters)

	properties.Property("V2-1: WAL Durability - all entries recoverable after close+reopen", prop.ForAll(
		func(numEntries int) bool {
			dir := t.TempDir()
			wal, err := NewWAL(dir, 64*1024*1024)
			if err != nil {
				t.Fatalf("failed to create WAL: %v", err)
			}

			lsns := make([]uint64, numEntries)

			for i := 0; i < numEntries; i++ {
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

				lsn, err := wal.Append(entry)
				if err != nil {
					t.Fatalf("failed to append entry: %v", err)
				}
				lsns[i] = lsn
			}

			if err := wal.Close(); err != nil {
				t.Fatalf("failed to close WAL: %v", err)
			}

			wal2, err := NewWAL(dir, 64*1024*1024)
			if err != nil {
				t.Fatalf("failed to reopen WAL: %v", err)
			}
			defer wal2.Close()

			expectedLSN := uint64(numEntries)
			if wal2.CurrentLSN() != expectedLSN {
				t.Errorf("expected LSN %d, got %d", expectedLSN, wal2.CurrentLSN())
				return false
			}

			segmentPath := filepath.Join(dir, "wal_0000000000000000.log")
			entries, err := ReadEntries(segmentPath)
			if err != nil {
				t.Fatalf("failed to read entries: %v", err)
			}

			if len(entries) != numEntries {
				t.Errorf("expected %d entries, got %d", numEntries, len(entries))
				return false
			}

			for i := 0; i < numEntries; i++ {
				payloadIndex := entries[i].Rows[0].Payload["index"]
				var index int
				switch v := payloadIndex.(type) {
				case float64:
					index = int(v)
				case int:
					index = v
				default:
					t.Errorf("unexpected payload type: %T", payloadIndex)
					return false
				}
				if index != i {
					t.Errorf("expected entry %d, got %d", i, index)
					return false
				}
			}

			return true
		},
		gen.IntRange(1, 100),
	))

	properties.TestingRun(t)
}

// TestProperty_WAL_CRCIntegrity tests Property V2-3: CRC Integrity
// For any entry, CRC at write matches CRC at read
func TestProperty_WAL_CRCIntegrity(t *testing.T) {
	parameters := gopter.DefaultTestParameters()
	parameters.MinSuccessfulTests = 100

	properties := gopter.NewProperties(parameters)

	properties.Property("V2-3: CRC Integrity - CRC matches at read", prop.ForAll(
		func(seed int) bool {
			dir := t.TempDir()
			wal, err := NewWAL(dir, 64*1024*1024)
			if err != nil {
				t.Fatalf("failed to create WAL: %v", err)
			}

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
						Payload:   map[string]interface{}{"value": seed},
					},
				},
				Schema:    types.Schema{Version: 1},
				Timestamp: 1640000000000000000,
			}

			_, err = wal.Append(entry)
			if err != nil {
				t.Fatalf("failed to append entry: %v", err)
			}

			if err := wal.Close(); err != nil {
				t.Fatalf("failed to close WAL: %v", err)
			}

			segmentPath := filepath.Join(dir, "wal_0000000000000000.log")
			entries, err := ReadEntries(segmentPath)
			if err != nil {
				t.Fatalf("failed to read entries: %v", err)
			}

			if len(entries) != 1 {
				t.Errorf("expected 1 entry, got %d", len(entries))
				return false
			}

			payloadValue := entries[0].Rows[0].Payload["value"]
			var value int
			switch v := payloadValue.(type) {
			case float64:
				value = int(v)
			case int:
				value = v
			default:
				t.Errorf("unexpected payload type: %T", payloadValue)
				return false
			}

			return value == seed
		},
		gen.Int(),
	))

	properties.TestingRun(t)
}

// TestProperty_WAL_Ordering tests Property V2-4: WAL Ordering
// For any two entries, LSN ordering matches file ordering
func TestProperty_WAL_Ordering(t *testing.T) {
	parameters := gopter.DefaultTestParameters()
	parameters.MinSuccessfulTests = 100

	properties := gopter.NewProperties(parameters)

	properties.Property("V2-4: WAL Ordering - LSN ordering matches file ordering", prop.ForAll(
		func(numEntries int) bool {
			dir := t.TempDir()
			wal, err := NewWAL(dir, 64*1024*1024)
			if err != nil {
				t.Fatalf("failed to create WAL: %v", err)
			}

			lsns := make([]uint64, numEntries)

			for i := 0; i < numEntries; i++ {
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

				lsn, err := wal.Append(entry)
				if err != nil {
					t.Fatalf("failed to append entry: %v", err)
				}
				lsns[i] = lsn
			}

			if err := wal.Close(); err != nil {
				t.Fatalf("failed to close WAL: %v", err)
			}

			segmentPath := filepath.Join(dir, "wal_0000000000000000.log")
			entries, err := ReadEntries(segmentPath)
			if err != nil {
				t.Fatalf("failed to read entries: %v", err)
			}

			for i := 0; i < len(entries)-1; i++ {
				if entries[i].LSN >= entries[i+1].LSN {
					t.Errorf("LSN ordering violation: entry %d has LSN %d, entry %d has LSN %d",
						i, entries[i].LSN, i+1, entries[i+1].LSN)
					return false
				}
			}

			if len(entries) != numEntries {
				t.Errorf("expected %d entries, got %d", numEntries, len(entries))
				return false
			}

			for i := 0; i < numEntries; i++ {
				if entries[i].LSN != uint64(i+1) {
					t.Errorf("expected LSN %d, got %d", uint64(i+1), entries[i].LSN)
					return false
				}
			}

			return true
		},
		gen.IntRange(10, 50),
	))

	properties.TestingRun(t)
}

// TestWALProperty_AppendRecoveryRoundTrip tests a comprehensive round-trip property
func TestWALProperty_AppendRecoveryRoundTrip(t *testing.T) {
	parameters := gopter.DefaultTestParameters()
	parameters.MinSuccessfulTests = 100

	properties := gopter.NewProperties(parameters)

	properties.Property("WAL Append-Recovery RoundTrip", prop.ForAll(
		func(numEntries int) bool {
			dir := t.TempDir()

			wal, err := NewWAL(dir, 64*1024*1024)
			if err != nil {
				t.Fatalf("failed to create WAL: %v", err)
			}

			createdEntries := make([]*Entry, numEntries)

			for i := 0; i < numEntries; i++ {
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
							Payload:   map[string]interface{}{"index": i, "seed": numEntries},
						},
					},
					Schema:    types.Schema{Version: 1},
					Timestamp: 1640000000000000000,
				}

				_, err := wal.Append(entry)
				if err != nil {
					t.Fatalf("failed to append entry: %v", err)
				}
				createdEntries[i] = entry
			}

			if err := wal.Close(); err != nil {
				t.Fatalf("failed to close WAL: %v", err)
			}

			wal2, err := NewWAL(dir, 64*1024*1024)
			if err != nil {
				t.Fatalf("failed to reopen WAL: %v", err)
			}
			defer wal2.Close()

			segmentPath := filepath.Join(dir, "wal_0000000000000000.log")
			readEntries, err := ReadEntries(segmentPath)
			if err != nil {
				t.Fatalf("failed to read entries: %v", err)
			}

			if len(readEntries) != numEntries {
				t.Errorf("expected %d entries, got %d", numEntries, len(readEntries))
				return false
			}

			for i := 0; i < numEntries; i++ {
				if readEntries[i].PartitionKey != createdEntries[i].PartitionKey {
					t.Errorf("entry %d: partition key mismatch", i)
					return false
				}

				if len(readEntries[i].Rows) != len(createdEntries[i].Rows) {
					t.Errorf("entry %d: row count mismatch", i)
					return false
				}

				readPayload := readEntries[i].Rows[0].Payload
				createdPayload := createdEntries[i].Rows[0].Payload

				readIndex := readPayload["index"]
				createdIndex := createdPayload["index"]

				var readIdxVal, createdIdxVal int
				switch v := readIndex.(type) {
				case float64:
					readIdxVal = int(v)
				case int:
					readIdxVal = v
				}
				switch v := createdIndex.(type) {
				case float64:
					createdIdxVal = int(v)
				case int:
					createdIdxVal = v
				}

				if readIdxVal != createdIdxVal {
					t.Errorf("entry %d: index mismatch - read %d, created %d", i, readIdxVal, createdIdxVal)
					return false
				}
			}

			return true
		},
		gen.IntRange(1, 100),
	))

	properties.TestingRun(t)
}

// TestWALProperty_MultipleSegments tests ordering across segment boundaries
func TestWALProperty_MultipleSegments(t *testing.T) {
	parameters := gopter.DefaultTestParameters()
	parameters.MinSuccessfulTests = 100

	properties := gopter.NewProperties(parameters)

	properties.Property("WAL Multiple Segments - LSN ordering preserved", prop.ForAll(
		func(seed int) bool {
			dir := t.TempDir()
			wal, err := NewWAL(dir, 1024)
			if err != nil {
				t.Fatalf("failed to create WAL: %v", err)
			}

			numEntries := 50
			lsns := make([]uint64, numEntries)

			for i := 0; i < numEntries; i++ {
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
							Payload:   map[string]interface{}{"index": i, "large": "x"},
						},
					},
					Schema:    types.Schema{Version: 1},
					Timestamp: 1640000000000000000,
				}

				lsn, err := wal.Append(entry)
				if err != nil {
					t.Fatalf("failed to append entry: %v", err)
				}
				lsns[i] = lsn
			}

			if err := wal.Close(); err != nil {
				t.Fatalf("failed to close WAL: %v", err)
			}

			entries, err := os.ReadDir(dir)
			if err != nil {
				t.Fatalf("failed to read dir: %v", err)
			}

			segmentCount := 0
			for _, e := range entries {
				if e.Name()[:4] == "wal_" {
					segmentCount++
				}
			}

			if segmentCount < 2 {
				t.Errorf("expected multiple segments, got %d", segmentCount)
				return false
			}

			var allEntries []*Entry
			for i := 0; i < len(entries); i++ {
				if entries[i].Name()[:4] != "wal_" {
					continue
				}
				segmentPath := filepath.Join(dir, entries[i].Name())
				segEntries, err := ReadEntries(segmentPath)
				if err != nil {
					t.Logf("failed to read segment %s: %v", segmentPath, err)
					continue
				}
				allEntries = append(allEntries, segEntries...)
			}

			if len(allEntries) != numEntries {
				t.Errorf("expected %d entries, got %d", numEntries, len(allEntries))
				return false
			}

			for i := 0; i < len(allEntries)-1; i++ {
				if allEntries[i].LSN >= allEntries[i+1].LSN {
					t.Errorf("LSN ordering violation at entry %d: %d >= %d",
						i, allEntries[i].LSN, allEntries[i+1].LSN)
					return false
				}
			}

			return true
		},
		gen.Int(),
	))

	properties.TestingRun(t)
}
