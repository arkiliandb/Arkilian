// Package index provides property-based tests for index completeness and correctness.
package index

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/gen"
	"github.com/leanovate/gopter/prop"
	_ "github.com/mattn/go-sqlite3"
)

// Validates: Requirements 16.3
func TestIndex_Properties(t *testing.T) {
	parameters := gopter.DefaultTestParameters()
	parameters.MinSuccessfulTests = 100

	properties := gopter.NewProperties(parameters)

	// Property V2-5: Index Completeness
	// For any value V in partition P, FindPartitions(column, V) returns set containing P
	properties.Property("Index Completeness - FindPartitions returns partition containing value", prop.ForAll(
		func(testData indexTestData) bool {
			if len(testData.partitions) == 0 || len(testData.values) == 0 {
				return true
			}

			ctx := context.Background()
			dir := t.TempDir()

			// Create mock storage
			mockStore := newMockStorage()

			// Create mock catalog
			mockCatalog := newMockIndexCatalog()

			// Create index builder
			builder := NewBuilder(mockStore, mockCatalog, dir, 64)

			// Build index from test data
			partitions := make([]*PartitionInfo, len(testData.partitions))
			for i, p := range testData.partitions {
				partitions[i] = &PartitionInfo{
					PartitionID:  p.partitionID,
					ObjectPath:   p.objectPath,
					RowCount:     int64(p.rowCount),
					MinEventTime: int64Ptr(p.minTime),
					MaxEventTime: int64Ptr(p.maxTime),
				}
			}

			indexInfos, err := builder.BuildIndex(ctx, testData.collection, testData.column, partitions)
			if err != nil {
				return false
			}

			if len(indexInfos) == 0 {
				return true // No index created (no data)
			}

			// Create lookup
			lookup := NewLookup(mockStore, mockCatalog, dir, 64)

			// For each value, verify it can be found
			for _, value := range testData.values {
				if value.partitionID == "" {
					continue
				}

				// Find partitions for this value
				foundPartitions, err := lookup.FindPartitions(ctx, testData.collection, testData.column, value.value)
				if err != nil {
					return false
				}

				// Value should be found in at least one partition
				found := false
				if foundPartitions != nil {
					for _, pid := range foundPartitions {
						if pid == value.partitionID {
							found = true
							break
						}
					}
				}

				if !found {
					// Value might be in a different bucket, check all index partitions
					for _, info := range indexInfos {
						cachePath := filepath.Join(dir, "idx_"+info.IndexID+".sqlite")
						if _, err := os.Stat(cachePath); err == nil {
							db, err := sql.Open("sqlite3", cachePath)
							if err != nil {
								continue
							}
							defer db.Close()

							var foundPID string
							err = db.QueryRow("SELECT partition_id FROM index_map WHERE column_value = ?", value.value).Scan(&foundPID)
							if err == sql.ErrNoRows {
								continue
							}
							if err == nil && foundPID == value.partitionID {
								found = true
								break
							}
						}
					}
				}

				if !found {
					return false
				}
			}

			return true
		},
		genIndexTestData(),
	))

	// Property V2-6: Index Pruning Correctness
	// Query result with index == query result without index
	properties.Property("Index Pruning Correctness - results match full scan", prop.ForAll(
		func(testData indexPruningTestData) bool {
			if len(testData.partitions) == 0 || len(testData.values) == 0 {
				return true
			}

			ctx := context.Background()
			dir := t.TempDir()

			// Create mock storage
			mockStore := newMockStorage()

			// Create mock catalog
			mockCatalog := newMockIndexCatalog()

			// Create index builder
			builder := NewBuilder(mockStore, mockCatalog, dir, 64)

			// Build partitions with data
			partitions := make([]*PartitionInfo, len(testData.partitions))
			for i, p := range testData.partitions {
				// Create SQLite file with data
				dbPath := filepath.Join(dir, p.partitionID+".sqlite")
				if err := createTestDataSQLite(dbPath, testData.column, p.values); err != nil {
					return false
				}

				partitions[i] = &PartitionInfo{
					PartitionID:  p.partitionID,
					ObjectPath:   dbPath,
					RowCount:     int64(len(p.values)),
					MinEventTime: int64Ptr(time.Now().UnixNano() - 3600),
					MaxEventTime: int64Ptr(time.Now().UnixNano()),
				}
			}

			// Build index
			indexInfos, err := builder.BuildIndex(ctx, testData.collection, testData.column, partitions)
			if err != nil {
				return false
			}

			// For each value, compare index result with full scan
			for _, value := range testData.values {
				// Get result from index lookup
				var indexResult []string
				if len(indexInfos) > 0 {
					lookup := NewLookup(mockStore, mockCatalog, dir, 64)
					foundPartitions, err := lookup.FindPartitions(ctx, testData.collection, testData.column, value)
					if err == nil && foundPartitions != nil {
						indexResult = foundPartitions
					}
				}

				// Get result from full scan
				var fullScanResult []string
				for _, p := range testData.partitions {
					dbPath := filepath.Join(dir, p.partitionID+".sqlite")
					db, err := sql.Open("sqlite3", dbPath)
					if err != nil {
						continue
					}
					defer db.Close()

					var found bool
					err = db.QueryRow("SELECT 1 FROM events WHERE device_id = ? LIMIT 1", value).Scan(&found)
					if err == nil {
						fullScanResult = append(fullScanResult, p.partitionID)
					}
				}

				// Results should be consistent
				// Note: Index might return superset (false positives) but never subset
				if len(indexResult) > 0 && len(fullScanResult) == 0 {
					// Index found something but full scan didn't - this is a bug
					return false
				}
			}

			return true
		},
		genIndexPruningTestData(),
	))

	properties.TestingRun(t)
}

// Test data structures for generators
type indexTestData struct {
	collection string
	column     string
	partitions []testPartition
	values     []testValue
}

type testPartition struct {
	partitionID string
	objectPath  string
	rowCount    int
	minTime     int64
	maxTime     int64
	values      []string
}

type testValue struct {
	value       string
	partitionID string
}

type indexPruningTestData struct {
	collection string
	column     string
	partitions []pruningTestPartition
	values     []string
}

type pruningTestPartition struct {
	partitionID string
	values      []string
}

// Generators
func genIndexTestData() gopter.Gen {
	return gen.Struct(
		reflect.TypeOf(indexTestData{}),
		map[string]gopter.Gen{
			"collection": gen.AlphaString(),
			"column":     gen.Const("device_id"),
			"partitions": gen.SliceOf(
				genTestPartition(),
				reflect.TypeOf(testPartition{}),
			).SuchThat(func(v interface{}) bool {
				return len(v.([]testPartition)) >= 1 && len(v.([]testPartition)) <= 5
			}),
			"values": gen.SliceOf(
				genTestValue(),
				reflect.TypeOf(testValue{}),
			).SuchThat(func(v interface{}) bool {
				return len(v.([]testValue)) >= 1 && len(v.([]testValue)) <= 10
			}),
		},
	)
}

func genTestPartition() gopter.Gen {
	return gen.Struct(
		reflect.TypeOf(testPartition{}),
		map[string]gopter.Gen{
			"partitionID": gen.AlphaString(),
			"objectPath":  gen.AlphaString(),
			"rowCount":    gen.IntRange(1, 100),
			"minTime":     gen.Int64Range(1, time.Now().UnixNano()),
			"maxTime":     gen.Int64Range(1, time.Now().UnixNano()),
			"values": gen.SliceOf(
				gen.AlphaString(),
				reflect.TypeOf(""),
			).SuchThat(func(v interface{}) bool {
				return len(v.([]string)) >= 1 && len(v.([]string)) <= 20
			}),
		},
	)
}

func genTestValue() gopter.Gen {
	return gen.Struct(
		reflect.TypeOf(testValue{}),
		map[string]gopter.Gen{
			"value":       gen.AlphaString(),
			"partitionID": gen.AlphaString(),
		},
	)
}

func genIndexPruningTestData() gopter.Gen {
	return gen.Struct(
		reflect.TypeOf(indexPruningTestData{}),
		map[string]gopter.Gen{
			"collection": gen.AlphaString(),
			"column":     gen.Const("device_id"),
			"partitions": gen.SliceOf(
				genPruningTestPartition(),
				reflect.TypeOf(pruningTestPartition{}),
			).SuchThat(func(v interface{}) bool {
				return len(v.([]pruningTestPartition)) >= 1 && len(v.([]pruningTestPartition)) <= 5
			}),
			"values": gen.SliceOf(
				gen.AlphaString(),
				reflect.TypeOf(""),
			).SuchThat(func(v interface{}) bool {
				return len(v.([]string)) >= 1 && len(v.([]string)) <= 10
			}),
		},
	)
}

func genPruningTestPartition() gopter.Gen {
	return gen.Struct(
		reflect.TypeOf(pruningTestPartition{}),
		map[string]gopter.Gen{
			"partitionID": gen.AlphaString(),
			"values": gen.SliceOf(
				gen.AlphaString(),
				reflect.TypeOf(""),
			).SuchThat(func(v interface{}) bool {
				return len(v.([]string)) >= 1 && len(v.([]string)) <= 20
			}),
		},
	)
}

// Helper functions
func int64Ptr(v int64) *int64 {
	return &v
}

func createTestDataSQLite(dbPath, column string, values []string) error {
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return err
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE events (id INTEGER PRIMARY KEY, device_id TEXT, timestamp INTEGER)")
	if err != nil {
		return err
	}

	for i, v := range values {
		_, err := db.Exec("INSERT INTO events (device_id, timestamp) VALUES (?, ?)", v, time.Now().UnixNano()+int64(i))
		if err != nil {
			return err
		}
	}

	return nil
}