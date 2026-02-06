package types

import (
	"bytes"
	"testing"
	"time"
)

func TestULIDGenerator_Generate(t *testing.T) {
	gen := NewULIDGenerator()

	ulid1, err := gen.Generate()
	if err != nil {
		t.Fatalf("failed to generate ULID: %v", err)
	}

	ulid2, err := gen.Generate()
	if err != nil {
		t.Fatalf("failed to generate ULID: %v", err)
	}

	// ULIDs should be different
	if ulid1 == ulid2 {
		t.Error("expected different ULIDs")
	}

	// ULID2 should be >= ULID1 (lexicographic ordering)
	if bytes.Compare(ulid1[:], ulid2[:]) > 0 {
		t.Error("expected ULID2 >= ULID1 for lexicographic ordering")
	}
}

func TestULIDGenerator_TimeOrdering(t *testing.T) {
	gen := NewULIDGenerator()

	t1 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	t2 := time.Date(2026, 1, 1, 0, 0, 1, 0, time.UTC) // 1 second later

	ulid1, err := gen.GenerateWithTime(t1)
	if err != nil {
		t.Fatalf("failed to generate ULID: %v", err)
	}

	ulid2, err := gen.GenerateWithTime(t2)
	if err != nil {
		t.Fatalf("failed to generate ULID: %v", err)
	}

	// ULID generated at later time should be greater
	if ulid1.Compare(ulid2) >= 0 {
		t.Errorf("expected ULID at t1 < ULID at t2, got %s >= %s", ulid1.String(), ulid2.String())
	}
}

func TestULIDGenerator_MonotonicWithinMillisecond(t *testing.T) {
	gen := NewULIDGenerator()
	ts := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

	// Generate multiple ULIDs at the same millisecond
	var ulids []ULID
	for i := 0; i < 100; i++ {
		ulid, err := gen.GenerateWithTime(ts)
		if err != nil {
			t.Fatalf("failed to generate ULID: %v", err)
		}
		ulids = append(ulids, ulid)
	}

	// All ULIDs should be strictly increasing
	for i := 1; i < len(ulids); i++ {
		if ulids[i-1].Compare(ulids[i]) >= 0 {
			t.Errorf("expected ULID[%d] < ULID[%d], got %s >= %s",
				i-1, i, ulids[i-1].String(), ulids[i].String())
		}
	}
}

func TestULID_Timestamp(t *testing.T) {
	gen := NewULIDGenerator()
	ts := time.Date(2026, 2, 5, 10, 30, 0, 0, time.UTC)

	ulid, err := gen.GenerateWithTime(ts)
	if err != nil {
		t.Fatalf("failed to generate ULID: %v", err)
	}

	// Timestamp should match (within millisecond precision)
	expectedMs := uint64(ts.UnixMilli())
	if ulid.Timestamp() != expectedMs {
		t.Errorf("expected timestamp %d, got %d", expectedMs, ulid.Timestamp())
	}
}

func TestULID_StringRoundTrip(t *testing.T) {
	gen := NewULIDGenerator()

	ulid1, err := gen.Generate()
	if err != nil {
		t.Fatalf("failed to generate ULID: %v", err)
	}

	str := ulid1.String()
	if len(str) != 26 {
		t.Errorf("expected string length 26, got %d", len(str))
	}

	ulid2, err := ParseULID(str)
	if err != nil {
		t.Fatalf("failed to parse ULID: %v", err)
	}

	if ulid1 != ulid2 {
		t.Errorf("round-trip failed: %v != %v", ulid1, ulid2)
	}
}

func TestULID_BytesRoundTrip(t *testing.T) {
	gen := NewULIDGenerator()

	ulid1, err := gen.Generate()
	if err != nil {
		t.Fatalf("failed to generate ULID: %v", err)
	}

	b := ulid1.Bytes()
	if len(b) != 16 {
		t.Errorf("expected bytes length 16, got %d", len(b))
	}

	ulid2, err := ULIDFromBytes(b)
	if err != nil {
		t.Fatalf("failed to create ULID from bytes: %v", err)
	}

	if ulid1 != ulid2 {
		t.Errorf("round-trip failed: %v != %v", ulid1, ulid2)
	}
}

func TestParseULID_InvalidLength(t *testing.T) {
	_, err := ParseULID("short")
	if err != ErrInvalidULIDLength {
		t.Errorf("expected ErrInvalidULIDLength, got %v", err)
	}
}

func TestParseULID_InvalidCharacter(t *testing.T) {
	// 'I', 'L', 'O', 'U' are not valid in Crockford Base32
	_, err := ParseULID("01234567890123456789012I45")
	if err != ErrInvalidULIDCharacter {
		t.Errorf("expected ErrInvalidULIDCharacter, got %v", err)
	}
}
