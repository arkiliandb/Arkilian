package bloom

import (
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/golang/snappy"
)

// SerializedFilter represents a bloom filter in serialized form.
type SerializedFilter struct {
	Algorithm  string `json:"algorithm"`
	NumBits    int    `json:"num_bits"`
	NumHashes  int    `json:"num_hashes"`
	Count      uint64 `json:"count"`
	Base64Data string `json:"base64_data"`
}

// Serialize converts the bloom filter to a base64-encoded byte representation.
// The format is:
//   - 8 bytes: numBits (uint64, little-endian)
//   - 8 bytes: numHashes (uint64, little-endian)
//   - 8 bytes: count (uint64, little-endian)
//   - remaining: bit array ([]uint64, little-endian)
func (bf *BloomFilter) Serialize() ([]byte, error) {
	bf.mu.RLock()
	defer bf.mu.RUnlock()

	// Calculate total size: 3 uint64 header + bit array
	headerSize := 3 * 8
	dataSize := len(bf.bits) * 8
	totalSize := headerSize + dataSize

	buf := make([]byte, totalSize)

	// Write header
	binary.LittleEndian.PutUint64(buf[0:8], bf.numBits)
	binary.LittleEndian.PutUint64(buf[8:16], bf.numHashes)
	binary.LittleEndian.PutUint64(buf[16:24], bf.count)

	// Write bit array
	for i, word := range bf.bits {
		offset := headerSize + i*8
		binary.LittleEndian.PutUint64(buf[offset:offset+8], word)
	}

	return buf, nil
}

// SerializeToBase64 returns the bloom filter as a base64-encoded string.
func (bf *BloomFilter) SerializeToBase64() (string, error) {
	data, err := bf.Serialize()
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(data), nil
}

// ToSerializedFilter converts the bloom filter to a SerializedFilter struct
// suitable for JSON marshaling.
func (bf *BloomFilter) ToSerializedFilter() (*SerializedFilter, error) {
	base64Data, err := bf.SerializeToBase64()
	if err != nil {
		return nil, err
	}

	bf.mu.RLock()
	defer bf.mu.RUnlock()

	return &SerializedFilter{
		Algorithm:  "murmur3_128",
		NumBits:    int(bf.numBits),
		NumHashes:  int(bf.numHashes),
		Count:      bf.count,
		Base64Data: base64Data,
	}, nil
}

// Deserialize reconstructs a bloom filter from serialized bytes.
func Deserialize(data []byte) (*BloomFilter, error) {
	if len(data) < 24 {
		return nil, errors.New("bloom: serialized data too short")
	}

	// Read header
	numBits := binary.LittleEndian.Uint64(data[0:8])
	numHashes := binary.LittleEndian.Uint64(data[8:16])
	count := binary.LittleEndian.Uint64(data[16:24])

	// Validate
	if numBits == 0 {
		return nil, errors.New("bloom: numBits cannot be zero")
	}
	if numHashes == 0 {
		return nil, errors.New("bloom: numHashes cannot be zero")
	}

	// Calculate expected data size
	numWords := (numBits + 63) / 64
	expectedSize := 24 + int(numWords)*8

	if len(data) < expectedSize {
		return nil, fmt.Errorf("bloom: expected %d bytes, got %d", expectedSize, len(data))
	}

	// Read bit array
	bits := make([]uint64, numWords)
	for i := range bits {
		offset := 24 + i*8
		bits[i] = binary.LittleEndian.Uint64(data[offset : offset+8])
	}

	return &BloomFilter{
		bits:      bits,
		numBits:   numBits,
		numHashes: numHashes,
		count:     count,
	}, nil
}

// DeserializeFromBase64 reconstructs a bloom filter from a base64-encoded string.
func DeserializeFromBase64(base64Data string) (*BloomFilter, error) {
	data, err := base64.StdEncoding.DecodeString(base64Data)
	if err != nil {
		return nil, fmt.Errorf("bloom: invalid base64 data: %w", err)
	}
	return Deserialize(data)
}

// FromSerializedFilter reconstructs a bloom filter from a SerializedFilter struct.
func FromSerializedFilter(sf *SerializedFilter) (*BloomFilter, error) {
	if sf == nil {
		return nil, errors.New("bloom: nil SerializedFilter")
	}
	return DeserializeFromBase64(sf.Base64Data)
}

// CompressedBloomFilter holds a bloom filter in Snappy-compressed form.
// It decompresses lazily on Contains() calls, reducing in-memory footprint
// by ~60-80% for sparse filters.
type CompressedBloomFilter struct {
	compressedData []byte // Snappy-compressed serialized bloom filter bytes
	numBits        uint64
	numHashes      uint64
	count          uint64
}

// CompressFilter creates a CompressedBloomFilter from an existing BloomFilter.
func CompressFilter(bf *BloomFilter) (*CompressedBloomFilter, error) {
	raw, err := bf.Serialize()
	if err != nil {
		return nil, fmt.Errorf("bloom: failed to serialize for compression: %w", err)
	}

	compressed := snappy.Encode(nil, raw)

	bf.mu.RLock()
	defer bf.mu.RUnlock()

	return &CompressedBloomFilter{
		compressedData: compressed,
		numBits:        bf.numBits,
		numHashes:      bf.numHashes,
		count:          bf.count,
	}, nil
}

// Contains decompresses the filter and checks membership.
func (cf *CompressedBloomFilter) Contains(item []byte) bool {
	raw, err := snappy.Decode(nil, cf.compressedData)
	if err != nil {
		return true // On error, assume present to avoid false negatives
	}
	bf, err := Deserialize(raw)
	if err != nil {
		return true
	}
	return bf.Contains(item)
}

// Decompress fully decompresses and returns the underlying BloomFilter.
func (cf *CompressedBloomFilter) Decompress() (*BloomFilter, error) {
	raw, err := snappy.Decode(nil, cf.compressedData)
	if err != nil {
		return nil, fmt.Errorf("bloom: snappy decompress failed: %w", err)
	}
	return Deserialize(raw)
}

// CompressedSizeBytes returns the in-memory size of the compressed data.
func (cf *CompressedBloomFilter) CompressedSizeBytes() int {
	return len(cf.compressedData)
}

// NumBits returns the original number of bits in the filter.
func (cf *CompressedBloomFilter) NumBits() uint64 {
	return cf.numBits
}

// Count returns the number of items in the filter.
func (cf *CompressedBloomFilter) Count() uint64 {
	return cf.count
}

// SerializeCompressed serializes a bloom filter with Snappy compression.
// Format: 8 bytes numBits + 8 bytes numHashes + 8 bytes count + snappy(bit_array)
func SerializeCompressed(bf *BloomFilter) ([]byte, error) {
	bf.mu.RLock()
	defer bf.mu.RUnlock()

	// Serialize just the bit array
	bitData := make([]byte, len(bf.bits)*8)
	for i, word := range bf.bits {
		binary.LittleEndian.PutUint64(bitData[i*8:(i+1)*8], word)
	}

	compressed := snappy.Encode(nil, bitData)

	// Header (24 bytes) + compressed bit array
	buf := make([]byte, 24+len(compressed))
	binary.LittleEndian.PutUint64(buf[0:8], bf.numBits)
	binary.LittleEndian.PutUint64(buf[8:16], bf.numHashes)
	binary.LittleEndian.PutUint64(buf[16:24], bf.count)
	copy(buf[24:], compressed)

	return buf, nil
}

// DeserializeCompressed reconstructs a bloom filter from Snappy-compressed bytes.
func DeserializeCompressed(data []byte) (*BloomFilter, error) {
	if len(data) < 24 {
		return nil, errors.New("bloom: compressed data too short")
	}

	numBits := binary.LittleEndian.Uint64(data[0:8])
	numHashes := binary.LittleEndian.Uint64(data[8:16])
	count := binary.LittleEndian.Uint64(data[16:24])

	if numBits == 0 || numHashes == 0 {
		return nil, errors.New("bloom: invalid compressed filter parameters")
	}

	bitData, err := snappy.Decode(nil, data[24:])
	if err != nil {
		return nil, fmt.Errorf("bloom: snappy decompress failed: %w", err)
	}

	numWords := (numBits + 63) / 64
	if len(bitData) < int(numWords)*8 {
		return nil, fmt.Errorf("bloom: decompressed data too short: expected %d bytes, got %d", numWords*8, len(bitData))
	}

	bits := make([]uint64, numWords)
	for i := range bits {
		bits[i] = binary.LittleEndian.Uint64(bitData[i*8 : (i+1)*8])
	}

	return &BloomFilter{
		bits:      bits,
		numBits:   numBits,
		numHashes: numHashes,
		count:     count,
	}, nil
}
