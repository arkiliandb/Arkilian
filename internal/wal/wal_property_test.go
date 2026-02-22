// Package wal provides property-based tests for WAL durability and integrity.
package wal

import (
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/arkilian/arkilian/pkg/types"
	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/gen"
	"github.com/leanovate/gopter/prop"
)

// Validates: Requirements 16.1
func TestWAL_Properties(t *testing.T) {
	parameters := gopter.DefaultTestParameters()
	parameters.MinSuccessfulTests = 100

	properties := gopter.NewProperties(parameters)

	// Property V2-1: WAL Durability
	// For any sequence of Append calls, all entries are recoverable after close+reopen
	properties.Property("WAL Durability - all entries recoverable after close+reopen", prop.ForAll(
		func(entries []testEntry) bool {
			if len(entries) == 0 {
				return true
			}

			dir := t.TempDir()
			wal, err := NewWAL(dir, 64*1024*1024)
			if err != nil {
				return false
			}

			// Append all entries
			lsns := make([]uint64, len(entries))
			for i, e := range entries {
				entry := createTestEntry(e)
				lsn, err := wal.Append(entry)
				if err != nil {
					wal.Close()
					return false
				}
				lsns[i] = lsn
			}

			// Close WAL
			if err := wal.Close(); err != nil {
				return false
			}

			// Reopen WAL
			wal2, err := NewWAL(dir, 64*1024*1024)
			if err != nil {
				return false
			}
			defer wal2.Close()

			// Verify LSN continues from where it left off
			if wal2.CurrentLSN() != lsns[len(lsns)-1] {
				return false
			}

			// Read all entries from segment
			segmentPath := filepath.Join(dir, "wal_0000000000000000.log")
			readEntries, err := ReadEntries(segmentPath)
			if err != nil {
				return false
			}

			// Verify all entries are recoverable
			if len(readEntries) != len(entries) {
				return false
			}

			// Verify LSN ordering
			for i, entry := range readEntries {
				if entry.LSN != lsns[i] {
					return false
				}
			}

			return true
		},
		genEntries(),
	))

	// Property V2-3: CRC Integrity
	// For any entry, CRC at write matches CRC at read
	properties.Property("CRC Integrity - CRC at write matches CRC at read", prop.ForAll(
		func(entry testEntry) bool {
			dir := t.TempDir()
			wal, err := NewWAL(dir, 64*1024*1024)
			if err != nil {
				return false
			}
			defer wal.Close()

			testEntry := createTestEntry(entry)
			lsn, err := wal.Append(testEntry)
			if err != nil {
				return false
			}

			// Read entries back
			segmentPath := filepath.Join(dir, "wal_0000000000000000.log")
			readEntries, err := ReadEntries(segmentPath)
			if err != nil {
				return false
			}

			if len(readEntries) != 1 {
				return false
			}

			readEntry := readEntries[0]
			if readEntry.LSN != lsn {
				return false
			}

			// Verify partition key
			if readEntry.PartitionKey != testEntry.PartitionKey {
				return false
			}

			// Verify row count
			if len(readEntry.Rows) != len(testEntry.Rows) {
				return false
			}

			return true
		},
		genEntry(),
	))

	// Property V2-4: WAL Ordering
	// For any two entries, LSN ordering matches file ordering
	properties.Property("WAL Ordering - LSN ordering matches file ordering", prop.ForAll(
		func(entries []orderedEntry) bool {
			if len(entries) < 2 {
				return true
			}

			dir := t.TempDir()
			wal, err := NewWAL(dir, 64*1024*1024)
			if err != nil {
				return false
			}
			defer wal.Close()

			// Append entries in specific order
			expectedOrder := make([]string, len(entries))
			lsns := make([]uint64, len(entries))

			for i, e := range entries {
				entry := &Entry{
					PartitionKey: e.partitionKey,
					Rows: []types.Row{
						{
							EventID:   []byte(e.eventID),
							TenantID:  e.tenantID,
							UserID:    e.userID,
							EventTime: e.eventTime,
							EventType: e.eventType,
							Payload:   e.payload,
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
					Timestamp: time.Now().UnixNano(),
				}
				lsn, err := wal.Append(entry)
				if err != nil {
					return false
				}
				lsns[i] = lsn
				expectedOrder[i] = e.partitionKey
			}

			// Read entries back
			segmentPath := filepath.Join(dir, "wal_0000000000000000.log")
			readEntries, err := ReadEntries(segmentPath)
			if err != nil {
				return false
			}

			if len(readEntries) != len(entries) {
				return false
			}

			// Verify LSNs are monotonically increasing
			for i := 1; i < len(lsns); i++ {
				if lsns[i] <= lsns[i-1] {
					return false
				}
			}

			// Verify file order matches LSN order
			for i, readEntry := range readEntries {
				if readEntry.LSN != lsns[i] {
					return false
				}
				if readEntry.PartitionKey != expectedOrder[i] {
					return false
				}
			}

			return true
		},
		genOrderedEntries(),
	))

	properties.TestingRun(t)
}

// testEntry is a generator-friendly entry structure
type testEntry struct {
	partitionKey string
	eventID      string
	tenantID     string
	userID       int64
	eventTime    int64
	eventType    string
	payload      map[string]interface{}
}

// genEntry generates a single test entry
func genEntry() gopter.Gen {
	return gen.Struct(
		reflect.TypeOf(testEntry{}),
		map[string]gopter.Gen{
			"partitionKey": gen.AlphaString(),
			"eventID":      gen.AlphaString(),
			"tenantID":     gen.AlphaString(),
			"userID":       gen.Int64Range(1, 1000000),
			"eventTime":    gen.Int64Range(1, time.Now().UnixNano()),
			"eventType":    gen.AlphaString(),
			"payload":      gen.MapOf(gen.AlphaString(), gen.Int64Range(1, 1000)),
		},
	)
}

// genEntries generates a slice of test entries
func genEntries() gopter.Gen {
	return gen.SliceOf(
		genEntry(),
		reflect.TypeOf(testEntry{}),
	).SuchThat(func(v interface{}) bool {
		entries := v.([]testEntry)
		return len(entries) >= 1 && len(entries) <= 100
	})
}

// orderedEntry for ordering tests
type orderedEntry struct {
	partitionKey string
	eventID      string
	tenantID     string
	userID       int64
	eventTime    int64
	eventType    string
	payload      map[string]interface{}
}

// genOrderedEntries generates entries for ordering tests
func genOrderedEntries() gopter.Gen {
	return gen.SliceOf(
		gen.Struct(
			reflect.TypeOf(orderedEntry{}),
			map[string]gopter.Gen{
				"partitionKey": gen.AlphaString(),
				"eventID":      gen.AlphaString(),
				"tenantID":     gen.AlphaString(),
				"userID":       gen.Int64Range(1, 1000000),
				"eventTime":    gen.Int64Range(1, time.Now().UnixNano()),
				"eventType":    gen.AlphaString(),
				"payload":      gen.MapOf(gen.AlphaString(), gen.Int64Range(1, 1000)),
			},
		),
		reflect.TypeOf(orderedEntry{}),
	).SuchThat(func(v interface{}) bool {
		entries := v.([]orderedEntry)
		return len(entries) >= 2 && len(entries) <= 50
	})
}

// createTestEntry converts testEntry to Entry
func createTestEntry(e testEntry) *Entry {
	return &Entry{
		PartitionKey: e.partitionKey,
		Rows: []types.Row{
			{
				EventID:   []byte(e.eventID),
				TenantID:  e.tenantID,
				UserID:    e.userID,
				EventTime: e.eventTime,
				EventType: e.eventType,
				Payload:   e.payload,
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
		Timestamp: time.Now().UnixNano(),
	}
}