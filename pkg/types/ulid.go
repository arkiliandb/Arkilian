package types

import (
	"crypto/rand"
	"encoding/binary"
	"sync"
	"time"
)

// ULID represents a Universally Unique Lexicographically Sortable Identifier.
// ULIDs are 128-bit identifiers that are time-ordered and lexicographically sortable.
// Format: 48-bit timestamp (milliseconds) + 80-bit random
type ULID [16]byte

// Crockford's Base32 alphabet (excludes I, L, O, U to avoid confusion)
const crockfordBase32 = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"

// ULIDGenerator generates time-ordered ULIDs with monotonic ordering within the same millisecond.
type ULIDGenerator struct {
	mu            sync.Mutex
	lastTimestamp uint64
	lastRandom    [10]byte
}

// NewULIDGenerator creates a new ULID generator.
func NewULIDGenerator() *ULIDGenerator {
	return &ULIDGenerator{}
}

// Generate creates a new ULID with the current timestamp.
// ULIDs generated within the same millisecond are monotonically increasing.
func (g *ULIDGenerator) Generate() (ULID, error) {
	return g.GenerateWithTime(time.Now())
}

// GenerateWithTime creates a new ULID with the specified timestamp.
// This is useful for testing and for generating ULIDs for historical events.
func (g *ULIDGenerator) GenerateWithTime(t time.Time) (ULID, error) {
	g.mu.Lock()
	defer g.mu.Unlock()

	timestamp := uint64(t.UnixMilli())

	var ulid ULID

	// Encode timestamp (48 bits = 6 bytes) in big-endian for lexicographic ordering
	ulid[0] = byte(timestamp >> 40)
	ulid[1] = byte(timestamp >> 32)
	ulid[2] = byte(timestamp >> 24)
	ulid[3] = byte(timestamp >> 16)
	ulid[4] = byte(timestamp >> 8)
	ulid[5] = byte(timestamp)

	// Handle random component (80 bits = 10 bytes)
	if timestamp == g.lastTimestamp {
		// Same millisecond: increment the random component for monotonic ordering
		g.incrementRandom()
		copy(ulid[6:], g.lastRandom[:])
	} else {
		// New millisecond: generate fresh random bytes
		if _, err := rand.Read(g.lastRandom[:]); err != nil {
			return ULID{}, err
		}
		copy(ulid[6:], g.lastRandom[:])
		g.lastTimestamp = timestamp
	}

	return ulid, nil
}


// incrementRandom increments the random component by 1 for monotonic ordering.
// This ensures ULIDs generated in the same millisecond are still ordered.
func (g *ULIDGenerator) incrementRandom() {
	// Increment as a big-endian 80-bit integer
	for i := 9; i >= 0; i-- {
		g.lastRandom[i]++
		if g.lastRandom[i] != 0 {
			break // No overflow, done
		}
		// Overflow, continue to next byte
	}
}

// Bytes returns the ULID as a byte slice.
func (u ULID) Bytes() []byte {
	return u[:]
}

// Timestamp returns the timestamp component of the ULID as Unix milliseconds.
func (u ULID) Timestamp() uint64 {
	return uint64(u[0])<<40 | uint64(u[1])<<32 | uint64(u[2])<<24 |
		uint64(u[3])<<16 | uint64(u[4])<<8 | uint64(u[5])
}

// Time returns the timestamp component as a time.Time.
func (u ULID) Time() time.Time {
	return time.UnixMilli(int64(u.Timestamp()))
}

// String returns the ULID as a 26-character Crockford Base32 string.
func (u ULID) String() string {
	// ULID string is 26 characters: 10 for timestamp + 16 for random
	var buf [26]byte

	// Encode timestamp (48 bits -> 10 characters)
	buf[0] = crockfordBase32[(u[0]&224)>>5]
	buf[1] = crockfordBase32[u[0]&31]
	buf[2] = crockfordBase32[(u[1]&248)>>3]
	buf[3] = crockfordBase32[((u[1]&7)<<2)|((u[2]&192)>>6)]
	buf[4] = crockfordBase32[(u[2]&62)>>1]
	buf[5] = crockfordBase32[((u[2]&1)<<4)|((u[3]&240)>>4)]
	buf[6] = crockfordBase32[((u[3]&15)<<1)|((u[4]&128)>>7)]
	buf[7] = crockfordBase32[(u[4]&124)>>2]
	buf[8] = crockfordBase32[((u[4]&3)<<3)|((u[5]&224)>>5)]
	buf[9] = crockfordBase32[u[5]&31]

	// Encode random (80 bits -> 16 characters)
	buf[10] = crockfordBase32[(u[6]&248)>>3]
	buf[11] = crockfordBase32[((u[6]&7)<<2)|((u[7]&192)>>6)]
	buf[12] = crockfordBase32[(u[7]&62)>>1]
	buf[13] = crockfordBase32[((u[7]&1)<<4)|((u[8]&240)>>4)]
	buf[14] = crockfordBase32[((u[8]&15)<<1)|((u[9]&128)>>7)]
	buf[15] = crockfordBase32[(u[9]&124)>>2]
	buf[16] = crockfordBase32[((u[9]&3)<<3)|((u[10]&224)>>5)]
	buf[17] = crockfordBase32[u[10]&31]
	buf[18] = crockfordBase32[(u[11]&248)>>3]
	buf[19] = crockfordBase32[((u[11]&7)<<2)|((u[12]&192)>>6)]
	buf[20] = crockfordBase32[(u[12]&62)>>1]
	buf[21] = crockfordBase32[((u[12]&1)<<4)|((u[13]&240)>>4)]
	buf[22] = crockfordBase32[((u[13]&15)<<1)|((u[14]&128)>>7)]
	buf[23] = crockfordBase32[(u[14]&124)>>2]
	buf[24] = crockfordBase32[((u[14]&3)<<3)|((u[15]&224)>>5)]
	buf[25] = crockfordBase32[u[15]&31]

	return string(buf[:])
}

// Compare compares two ULIDs lexicographically.
// Returns -1 if u < other, 0 if u == other, 1 if u > other.
func (u ULID) Compare(other ULID) int {
	for i := 0; i < 16; i++ {
		if u[i] < other[i] {
			return -1
		}
		if u[i] > other[i] {
			return 1
		}
	}
	return 0
}

// ParseULID parses a 26-character Crockford Base32 string into a ULID.
func ParseULID(s string) (ULID, error) {
	if len(s) != 26 {
		return ULID{}, ErrInvalidULIDLength
	}

	var ulid ULID
	var dec [26]byte

	// Decode each character
	for i, c := range s {
		idx := decodeBase32(byte(c))
		if idx == 0xFF {
			return ULID{}, ErrInvalidULIDCharacter
		}
		dec[i] = idx
	}

	// Decode timestamp (10 characters -> 48 bits)
	ulid[0] = (dec[0] << 5) | dec[1]
	ulid[1] = (dec[2] << 3) | (dec[3] >> 2)
	ulid[2] = (dec[3] << 6) | (dec[4] << 1) | (dec[5] >> 4)
	ulid[3] = (dec[5] << 4) | (dec[6] >> 1)
	ulid[4] = (dec[6] << 7) | (dec[7] << 2) | (dec[8] >> 3)
	ulid[5] = (dec[8] << 5) | dec[9]

	// Decode random (16 characters -> 80 bits)
	ulid[6] = (dec[10] << 3) | (dec[11] >> 2)
	ulid[7] = (dec[11] << 6) | (dec[12] << 1) | (dec[13] >> 4)
	ulid[8] = (dec[13] << 4) | (dec[14] >> 1)
	ulid[9] = (dec[14] << 7) | (dec[15] << 2) | (dec[16] >> 3)
	ulid[10] = (dec[16] << 5) | dec[17]
	ulid[11] = (dec[18] << 3) | (dec[19] >> 2)
	ulid[12] = (dec[19] << 6) | (dec[20] << 1) | (dec[21] >> 4)
	ulid[13] = (dec[21] << 4) | (dec[22] >> 1)
	ulid[14] = (dec[22] << 7) | (dec[23] << 2) | (dec[24] >> 3)
	ulid[15] = (dec[24] << 5) | dec[25]

	return ulid, nil
}

// decodeBase32 decodes a single Crockford Base32 character.
// Returns 0xFF for invalid characters.
func decodeBase32(c byte) byte {
	switch {
	case c >= '0' && c <= '9':
		return c - '0'
	case c >= 'A' && c <= 'H':
		return c - 'A' + 10
	case c >= 'J' && c <= 'K':
		return c - 'J' + 18
	case c >= 'M' && c <= 'N':
		return c - 'M' + 20
	case c >= 'P' && c <= 'T':
		return c - 'P' + 22
	case c >= 'V' && c <= 'Z':
		return c - 'V' + 27
	case c >= 'a' && c <= 'h':
		return c - 'a' + 10
	case c >= 'j' && c <= 'k':
		return c - 'j' + 18
	case c >= 'm' && c <= 'n':
		return c - 'm' + 20
	case c >= 'p' && c <= 't':
		return c - 'p' + 22
	case c >= 'v' && c <= 'z':
		return c - 'v' + 27
	default:
		return 0xFF
	}
}

// ULIDFromBytes creates a ULID from a byte slice.
func ULIDFromBytes(b []byte) (ULID, error) {
	if len(b) != 16 {
		return ULID{}, ErrInvalidULIDLength
	}
	var ulid ULID
	copy(ulid[:], b)
	return ulid, nil
}

// MustULIDFromBytes creates a ULID from bytes, panicking on error.
func MustULIDFromBytes(b []byte) ULID {
	ulid, err := ULIDFromBytes(b)
	if err != nil {
		panic(err)
	}
	return ulid
}

// NewULIDFromTimestamp creates a ULID with the given timestamp and random bytes.
func NewULIDFromTimestamp(timestamp uint64, random []byte) ULID {
	var ulid ULID
	binary.BigEndian.PutUint64(ulid[0:8], timestamp<<16)
	copy(ulid[6:], random[:10])
	return ulid
}
