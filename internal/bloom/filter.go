// Package bloom provides a probabilistic data structure for efficient membership testing.
package bloom

import (
	"math"
	"sync"

	"github.com/spaolacci/murmur3"
)

// BloomFilter provides probabilistic membership testing with configurable false positive rate.
// It guarantees no false negatives - if an item was added, Contains() will always return true.
type BloomFilter struct {
	mu        sync.RWMutex
	bits      []uint64
	numBits   uint64
	numHashes uint64
	count     uint64 // number of items added
}

// New creates a new BloomFilter with the specified number of bits and hash functions.
func New(numBits, numHashes int) *BloomFilter {
	if numBits <= 0 {
		numBits = 1024
	}
	if numHashes <= 0 {
		numHashes = 7
	}

	// Round up to nearest 64 bits for efficient storage
	numWords := (numBits + 63) / 64
	actualBits := uint64(numWords * 64)

	return &BloomFilter{
		bits:      make([]uint64, numWords),
		numBits:   actualBits,
		numHashes: uint64(numHashes),
	}
}

// NewWithEstimates creates a BloomFilter optimized for the expected number of items
// and target false positive rate.
func NewWithEstimates(expectedItems int, targetFPR float64) *BloomFilter {
	if expectedItems <= 0 {
		expectedItems = 1000
	}
	if targetFPR <= 0 || targetFPR >= 1 {
		targetFPR = 0.01
	}

	numBits, numHashes := OptimalParameters(expectedItems, targetFPR)
	return New(numBits, numHashes)
}

// OptimalParameters calculates the optimal number of bits and hash functions
// for a given expected number of items and target false positive rate.
//
// The formulas are:
//   - m = -n * ln(p) / (ln(2)^2)  where m = bits, n = items, p = FPR
//   - k = (m/n) * ln(2)           where k = hash functions
func OptimalParameters(expectedItems int, targetFPR float64) (numBits, numHashes int) {
	if expectedItems <= 0 {
		expectedItems = 1000
	}
	if targetFPR <= 0 || targetFPR >= 1 {
		targetFPR = 0.01
	}

	n := float64(expectedItems)
	p := targetFPR
	ln2 := math.Ln2
	ln2Sq := ln2 * ln2

	// m = -n * ln(p) / (ln(2)^2)
	m := -n * math.Log(p) / ln2Sq
	numBits = int(math.Ceil(m))

	// k = (m/n) * ln(2)
	k := (m / n) * ln2
	numHashes = int(math.Ceil(k))

	// Ensure minimum values
	if numBits < 64 {
		numBits = 64
	}
	if numHashes < 1 {
		numHashes = 1
	}

	return numBits, numHashes
}

// Add adds an item to the bloom filter.
func (bf *BloomFilter) Add(item []byte) {
	bf.mu.Lock()
	defer bf.mu.Unlock()

	h1, h2 := bf.hash128(item)

	for i := uint64(0); i < bf.numHashes; i++ {
		// Double hashing: h(i) = h1 + i*h2
		pos := (h1 + i*h2) % bf.numBits
		bf.setBit(pos)
	}
	bf.count++
}

// Contains tests if an item might be in the filter.
// Returns true if the item might be present (could be false positive).
// Returns false if the item is definitely not present (no false negatives).
func (bf *BloomFilter) Contains(item []byte) bool {
	bf.mu.RLock()
	defer bf.mu.RUnlock()

	h1, h2 := bf.hash128(item)

	for i := uint64(0); i < bf.numHashes; i++ {
		pos := (h1 + i*h2) % bf.numBits
		if !bf.getBit(pos) {
			return false
		}
	}
	return true
}

// hash128 computes murmur3 128-bit hash and returns two 64-bit values.
func (bf *BloomFilter) hash128(item []byte) (uint64, uint64) {
	h := murmur3.New128()
	h.Write(item)
	return h.Sum128()
}

// setBit sets the bit at position pos.
func (bf *BloomFilter) setBit(pos uint64) {
	wordIdx := pos / 64
	bitIdx := pos % 64
	bf.bits[wordIdx] |= (1 << bitIdx)
}

// getBit returns true if the bit at position pos is set.
func (bf *BloomFilter) getBit(pos uint64) bool {
	wordIdx := pos / 64
	bitIdx := pos % 64
	return (bf.bits[wordIdx] & (1 << bitIdx)) != 0
}

// NumBits returns the number of bits in the filter.
func (bf *BloomFilter) NumBits() int {
	return int(bf.numBits)
}

// NumHashes returns the number of hash functions used.
func (bf *BloomFilter) NumHashes() int {
	return int(bf.numHashes)
}

// Count returns the number of items added to the filter.
func (bf *BloomFilter) Count() uint64 {
	bf.mu.RLock()
	defer bf.mu.RUnlock()
	return bf.count
}

// FalsePositiveRate returns the estimated false positive rate based on
// the current fill ratio.
//
// Formula: (1 - e^(-k*n/m))^k
// where k = numHashes, n = count, m = numBits
func (bf *BloomFilter) FalsePositiveRate() float64 {
	bf.mu.RLock()
	defer bf.mu.RUnlock()

	if bf.count == 0 {
		return 0
	}

	k := float64(bf.numHashes)
	n := float64(bf.count)
	m := float64(bf.numBits)

	// (1 - e^(-k*n/m))^k
	return math.Pow(1-math.Exp(-k*n/m), k)
}

// Bits returns a copy of the underlying bit array for serialization.
func (bf *BloomFilter) Bits() []uint64 {
	bf.mu.RLock()
	defer bf.mu.RUnlock()

	result := make([]uint64, len(bf.bits))
	copy(result, bf.bits)
	return result
}

// SetBits sets the underlying bit array from serialized data.
func (bf *BloomFilter) SetBits(bits []uint64) {
	bf.mu.Lock()
	defer bf.mu.Unlock()

	bf.bits = make([]uint64, len(bits))
	copy(bf.bits, bits)
	bf.numBits = uint64(len(bits) * 64)
}

// SetCount sets the item count (used during deserialization).
func (bf *BloomFilter) SetCount(count uint64) {
	bf.mu.Lock()
	defer bf.mu.Unlock()
	bf.count = count
}
