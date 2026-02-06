package types

import (
	"testing"
	"time"

	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/gen"
	"github.com/leanovate/gopter/prop"
)

// TestProperty_ULIDTimeOrdering validates Property 8: ULID Time Ordering
// For any sequence of generated ULIDs, if ULID A is generated before ULID B,
// then A < B in lexicographic ordering.
// **Validates: Requirements 2.3**
func TestProperty_ULIDTimeOrdering(t *testing.T) {
	parameters := gopter.DefaultTestParameters()
	parameters.MinSuccessfulTests = 100
	properties := gopter.NewProperties(parameters)

	// Property: ULIDs generated at different times maintain time ordering
	properties.Property("ULIDs generated at later times are lexicographically greater", prop.ForAll(
		func(t1Ms, t2Ms int64) bool {
			// Ensure t1 < t2
			if t1Ms >= t2Ms {
				t1Ms, t2Ms = t2Ms, t1Ms+1
			}

			gen := NewULIDGenerator()
			time1 := time.UnixMilli(t1Ms)
			time2 := time.UnixMilli(t2Ms)

			ulid1, err := gen.GenerateWithTime(time1)
			if err != nil {
				return false
			}

			ulid2, err := gen.GenerateWithTime(time2)
			if err != nil {
				return false
			}

			// ULID generated at earlier time should be less than ULID at later time
			return ulid1.Compare(ulid2) < 0
		},
		gen.Int64Range(1000000000000, 2000000000000), // Timestamps in reasonable range (2001-2033)
		gen.Int64Range(1000000000000, 2000000000000),
	))

	// Property: ULIDs generated in sequence within same millisecond are monotonically increasing
	properties.Property("ULIDs within same millisecond are monotonically increasing", prop.ForAll(
		func(timestampMs int64, count int) bool {
			if count < 2 {
				count = 2
			}
			if count > 1000 {
				count = 1000
			}

			gen := NewULIDGenerator()
			ts := time.UnixMilli(timestampMs)

			var prev ULID
			for i := 0; i < count; i++ {
				curr, err := gen.GenerateWithTime(ts)
				if err != nil {
					return false
				}

				if i > 0 {
					// Each ULID should be strictly greater than the previous
					if prev.Compare(curr) >= 0 {
						return false
					}
				}
				prev = curr
			}
			return true
		},
		gen.Int64Range(1000000000000, 2000000000000),
		gen.IntRange(2, 100),
	))

	// Property: ULID timestamp extraction is accurate
	properties.Property("ULID timestamp extraction matches generation time", prop.ForAll(
		func(timestampMs int64) bool {
			gen := NewULIDGenerator()
			ts := time.UnixMilli(timestampMs)

			ulid, err := gen.GenerateWithTime(ts)
			if err != nil {
				return false
			}

			// Extracted timestamp should match the input timestamp
			return ulid.Timestamp() == uint64(timestampMs)
		},
		gen.Int64Range(0, 281474976710655), // Max 48-bit value
	))

	properties.TestingRun(t)
}
