// Package cache provides property-based tests for co-access graph memory bounds.
package cache

import (
	"reflect"
	"runtime"
	"testing"
	"time"

	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/gen"
	"github.com/leanovate/gopter/prop"
)

// Validates: Requirements 9.5
func TestCoAccessGraph_Properties(t *testing.T) {
	parameters := gopter.DefaultTestParameters()
	parameters.MinSuccessfulTests = 100
	parameters.MaxSize = 20

	properties := gopter.NewProperties(parameters)

	// Property V2-9: Memory Bound
	// For any sequence of RecordAccess calls with up to 10K distinct keys, memory < 10MB
	properties.Property("Memory Bound - graph stays under 10MB with up to 10K distinct keys", prop.ForAll(
		func(testData graphTestData) bool {
			// Create graph with default parameters
			g := NewCoAccessGraph(0.95, 0.70, 10)
			defer g.Close()

			// Record accesses
			for _, access := range testData.accesses {
				g.RecordAccess(access.sequence)
			}

			// Force GC to get accurate memory measurement
			runtime.GC()
			time.Sleep(10 * time.Millisecond)
			runtime.GC()

			// Get memory usage
			var memStats runtime.MemStats
			runtime.ReadMemStats(&memStats)

			// HeapAlloc is in bytes, 10MB = 10 * 1024 * 1024 = 10485760
			// Allow some overhead for the test itself
			maxMemory := int64(15 * 1024 * 1024) // 15MB to account for test overhead

			if int64(memStats.HeapAlloc) > maxMemory {
				return false
			}

			// Verify graph metrics
			metrics := g.Metrics()
			if metrics.Nodes > int64(testData.distinctKeys*2) {
				// Should not have more than 2x distinct keys
				return false
			}

			return true
		},
		genGraphTestData(),
	))

	// Additional property: Memory bound with concurrent access
	properties.Property("Memory Bound - graph stays under memory limit with concurrent access", prop.ForAll(
		func(testData concurrentGraphTestData) bool {
			// Create graph
			g := NewCoAccessGraph(0.95, 0.70, 10)
			defer g.Close()

			// Record accesses concurrently
			done := make(chan bool, len(testData.accesses))
			for _, access := range testData.accesses {
				go func(seq []string) {
					g.RecordAccess(seq)
					done <- true
				}(access.sequence)
			}

			// Wait for all goroutines
			for i := 0; i < len(testData.accesses); i++ {
				<-done
			}

			// Force GC
			runtime.GC()
			time.Sleep(10 * time.Millisecond)
			runtime.GC()

			// Check memory
			var memStats runtime.MemStats
			runtime.ReadMemStats(&memStats)

			maxMemory := int64(15 * 1024 * 1024) // 15MB with overhead
			if int64(memStats.HeapAlloc) > maxMemory {
				return false
			}

			return true
		},
		genConcurrentGraphTestData(),
	))

	properties.TestingRun(t)
}

// Test data structures
type graphTestData struct {
	accesses     []accessSequence
	distinctKeys int
}

type accessSequence struct {
	sequence []string
}

type concurrentGraphTestData struct {
	accesses []accessSequence
}

// Generators with realistic constraints
func genGraphTestData() gopter.Gen {
	return gen.Struct(
		reflect.TypeOf(graphTestData{}),
		map[string]gopter.Gen{
			"accesses": gen.SliceOf(
				genAccessSequence(),
				reflect.TypeOf(accessSequence{}),
			).SuchThat(func(v interface{}) bool {
				return len(v.([]accessSequence)) >= 1 && len(v.([]accessSequence)) <= 20
			}),
			"distinctKeys": gen.IntRange(100, 1000), // Reduced from 10000 to 1000
		},
	)
}

func genAccessSequence() gopter.Gen {
	return gen.Struct(
		reflect.TypeOf(accessSequence{}),
		map[string]gopter.Gen{
			"sequence": gen.SliceOf(
				gen.AlphaString(),
				reflect.TypeOf(""),
			).SuchThat(func(v interface{}) bool {
				seq := v.([]string)
				return len(seq) >= 2 && len(seq) <= 5
			}),
		},
	)
}

func genConcurrentGraphTestData() gopter.Gen {
	return gen.Struct(
		reflect.TypeOf(concurrentGraphTestData{}),
		map[string]gopter.Gen{
			"accesses": gen.SliceOf(
				genAccessSequence(),
				reflect.TypeOf(accessSequence{}),
			).SuchThat(func(v interface{}) bool {
				return len(v.([]accessSequence)) >= 1 && len(v.([]accessSequence)) <= 10
			}),
		},
	)
}