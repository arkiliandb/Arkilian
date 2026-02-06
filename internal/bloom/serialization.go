package bloom

import (
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
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
