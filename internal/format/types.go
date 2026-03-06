// Package format provides the ArkFormat binary columnar micro-partition file format for Arkilian V3.
package format

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"time"
)

// DataType represents the data type of a column.
type DataType uint8

const (
	TypeINT64     DataType = 1
	TypeFLOAT64   DataType = 2
	TypeBOOL      DataType = 3
	TypeBYTES     DataType = 4
	TypeSTRING    DataType = 5
	TypeTIMESTAMP DataType = 6
	TypeINT32     DataType = 7
	TypeFLOAT32   DataType = 8
	TypeUUID      DataType = 9
)

// Encoding represents the encoding method for column data.
type Encoding uint8

const (
	EncodingPLAIN   Encoding = 0
	EncodingDELTA   Encoding = 1
	EncodingRLE     Encoding = 2
	EncodingDICT    Encoding = 3
	EncodingBITPACK Encoding = 4
)

// Compression represents the compression algorithm.
type Compression uint8

const (
	CompressionNONE   Compression = 0
	CompressionZSTD   Compression = 1
	CompressionLZ4    Compression = 2
	CompressionSNAPPY Compression = 3
)

// Value is a tagged union that can hold any of the 9 data types.
type Value struct {
	Type  DataType
	Data  interface{}
}

// MarshalJSON implements json.Marshaler for Value.
func (v Value) MarshalJSON() ([]byte, error) {
	switch v.Type {
	case TypeINT64:
		return json.Marshal(v.Data.(int64))
	case TypeFLOAT64:
		return json.Marshal(v.Data.(float64))
	case TypeBOOL:
		return json.Marshal(v.Data.(bool))
	case TypeBYTES:
		return json.Marshal(v.Data.([]byte))
	case TypeSTRING:
		return json.Marshal(v.Data.(string))
	case TypeTIMESTAMP:
		return json.Marshal(v.Data.(time.Time))
	case TypeINT32:
		return json.Marshal(v.Data.(int32))
	case TypeFLOAT32:
		return json.Marshal(v.Data.(float32))
	case TypeUUID:
		return json.Marshal(v.Data.([16]byte))
	default:
		return nil, fmt.Errorf("unknown data type: %d", v.Type)
	}
}

// UnmarshalJSON implements json.Unmarshaler for Value.
func (v *Value) UnmarshalJSON(data []byte) error {
	// Try to determine type from JSON value
	var num int64
	if err := json.Unmarshal(data, &num); err == nil {
		v.Type = TypeINT64
		v.Data = num
		return nil
	}

	var str string
	if err := json.Unmarshal(data, &str); err == nil {
		// Try to parse as timestamp first
		if t, err := time.Parse(time.RFC3339Nano, str); err == nil {
			v.Type = TypeTIMESTAMP
			v.Data = t
			return nil
		}
		v.Type = TypeSTRING
		v.Data = str
		return nil
	}

	var b bool
	if err := json.Unmarshal(data, &b); err == nil {
		v.Type = TypeBOOL
		v.Data = b
		return nil
	}

	var f float64
	if err := json.Unmarshal(data, &f); err == nil {
		v.Type = TypeFLOAT64
		v.Data = f
		return nil
	}

	return fmt.Errorf("cannot unmarshal value: %s", string(data))
}

// RowID encodes partition_seq and row_offset into a single uint64.
type RowID uint64

// NewRowID creates a new RowID from partition_seq and row_offset.
func NewRowID(partitionSeq uint32, rowOffset uint32) RowID {
	return RowID(uint64(partitionSeq)<<32 | uint64(rowOffset))
}

// PartitionSeq returns the partition sequence number.
func (r RowID) PartitionSeq() uint32 {
	return uint32(r >> 32)
}

// RowOffset returns the row offset within the partition.
func (r RowID) RowOffset() uint32 {
	return uint32(r & 0xFFFFFFFF)
}

// Encode serializes the RowID to a [16]byte for min/max storage.
func (r RowID) Encode() [16]byte {
	var buf [16]byte
	binary.LittleEndian.PutUint64(buf[:8], uint64(r))
	return buf
}

// Decode deserializes a [16]byte to RowID.
func DecodeRowID(buf [16]byte) RowID {
	return RowID(binary.LittleEndian.Uint64(buf[:8]))
}

// ValueSlotSize is the size of a Value slot for min/max storage.
const ValueSlotSize = 16

// EncodeValue encodes a Value to a [16]byte for min/max storage.
func EncodeValue(v Value) ([16]byte, error) {
	var buf [16]byte

	switch v.Type {
	case TypeINT64:
		binary.LittleEndian.PutUint64(buf[:8], uint64(v.Data.(int64)))
	case TypeFLOAT64:
		bits := math.Float64bits(v.Data.(float64))
		binary.LittleEndian.PutUint64(buf[:8], bits)
	case TypeBOOL:
		if v.Data.(bool) {
			buf[0] = 1
		}
	case TypeBYTES:
		data := v.Data.([]byte)
		copy(buf[:min(len(data), 16)], data)
	case TypeSTRING:
		data := []byte(v.Data.(string))
		copy(buf[:min(len(data), 16)], data)
	case TypeTIMESTAMP:
		binary.LittleEndian.PutUint64(buf[:8], uint64(v.Data.(time.Time).UnixNano()))
	case TypeINT32:
		binary.LittleEndian.PutUint32(buf[:4], uint32(v.Data.(int32)))
	case TypeFLOAT32:
		bits := math.Float32bits(v.Data.(float32))
		binary.LittleEndian.PutUint32(buf[:4], bits)
	case TypeUUID:
		uuid := v.Data.([16]byte)
		copy(buf[:], uuid[:])
	default:
		return buf, fmt.Errorf("unknown data type: %d", v.Type)
	}

	return buf, nil
}

// DecodeValue decodes a [16]byte to Value.
func DecodeValue(buf [16]byte, dataType DataType) (Value, error) {
	var v Value
	v.Type = dataType

	switch dataType {
	case TypeINT64:
		v.Data = int64(binary.LittleEndian.Uint64(buf[:8]))
	case TypeFLOAT64:
		bits := binary.LittleEndian.Uint64(buf[:8])
		v.Data = math.Float64frombits(bits)
	case TypeBOOL:
		v.Data = buf[0] != 0
	case TypeBYTES:
		v.Data = buf[:]
	case TypeSTRING:
		// Find the end of the string (first null byte)
		end := 16
		for i := 0; i < 16; i++ {
			if buf[i] == 0 {
				end = i
				break
			}
		}
		v.Data = string(buf[:end])
	case TypeTIMESTAMP:
		v.Data = time.Unix(0, int64(binary.LittleEndian.Uint64(buf[:8])))
	case TypeINT32:
		v.Data = int32(binary.LittleEndian.Uint32(buf[:4]))
	case TypeFLOAT32:
		v.Data = math.Float32frombits(binary.LittleEndian.Uint32(buf[:4]))
	case TypeUUID:
		var uuid [16]byte
		copy(uuid[:], buf[:])
		v.Data = uuid
	default:
		return v, fmt.Errorf("unknown data type: %d", dataType)
	}

	return v, nil
}

// CompareValues compares two Values and returns -1, 0, or 1.
func CompareValues(v1, v2 Value) (int, error) {
	if v1.Type != v2.Type {
		return 0, fmt.Errorf("cannot compare different types: %d vs %d", v1.Type, v2.Type)
	}

	switch v1.Type {
	case TypeINT64:
		n1, n2 := v1.Data.(int64), v2.Data.(int64)
		if n1 < n2 {
			return -1, nil
		} else if n1 > n2 {
			return 1, nil
		}
		return 0, nil
	case TypeFLOAT64:
		f1, f2 := v1.Data.(float64), v2.Data.(float64)
		if f1 < f2 {
			return -1, nil
		} else if f1 > f2 {
			return 1, nil
		}
		return 0, nil
	case TypeBOOL:
		b1, b2 := v1.Data.(bool), v2.Data.(bool)
		if b1 == b2 {
			return 0, nil
		}
		if !b1 && b2 {
			return -1, nil
		}
		return 1, nil
	case TypeBYTES:
		return compareBytes(v1.Data.([]byte), v2.Data.([]byte)), nil
	case TypeSTRING:
		return compareStrings(v1.Data.(string), v2.Data.(string)), nil
	case TypeTIMESTAMP:
		t1, t2 := v1.Data.(time.Time), v2.Data.(time.Time)
		if t1.Before(t2) {
			return -1, nil
		} else if t1.After(t2) {
			return 1, nil
		}
		return 0, nil
	case TypeINT32:
		n1, n2 := v1.Data.(int32), v2.Data.(int32)
		if n1 < n2 {
			return -1, nil
		} else if n1 > n2 {
			return 1, nil
		}
		return 0, nil
	case TypeFLOAT32:
		f1, f2 := v1.Data.(float32), v2.Data.(float32)
		if f1 < f2 {
			return -1, nil
		} else if f1 > f2 {
			return 1, nil
		}
		return 0, nil
	case TypeUUID:
		u1 := v1.Data.([16]byte)
		u2 := v2.Data.([16]byte)
		return compareBytes(u1[:], u2[:]), nil
	default:
		return 0, fmt.Errorf("unknown data type: %d", v1.Type)
	}
}

func compareBytes(b1, b2 []byte) int {
	minLen := len(b1)
	if len(b2) < minLen {
		minLen = len(b2)
	}
	for i := 0; i < minLen; i++ {
		if b1[i] < b2[i] {
			return -1
		} else if b1[i] > b2[i] {
			return 1
		}
	}
	if len(b1) < len(b2) {
		return -1
	} else if len(b1) > len(b2) {
		return 1
	}
	return 0
}

func compareStrings(s1, s2 string) int {
	minLen := len(s1)
	if len(s2) < minLen {
		minLen = len(s2)
	}
	for i := 0; i < minLen; i++ {
		if s1[i] < s2[i] {
			return -1
		} else if s1[i] > s2[i] {
			return 1
		}
	}
	if len(s1) < len(s2) {
		return -1
	} else if len(s1) > len(s2) {
		return 1
	}
	return 0
}

// IsNull checks if a Value is null.
func IsNull(v Value) bool {
	return v.Type == 0 && v.Data == nil
}

// IsNaN checks if a float value is NaN.
func IsNaN(v Value) bool {
	if v.Type != TypeFLOAT64 && v.Type != TypeFLOAT32 {
		return false
	}
	switch v.Type {
	case TypeFLOAT64:
		return math.IsNaN(v.Data.(float64))
	case TypeFLOAT32:
		return math.IsNaN(float64(v.Data.(float32)))
	}
	return false
}

// ZoneMapSummary is a simplified zone map for catalog storage.
type ZoneMapSummary struct {
	ColumnID  uint32
	MinVal    [16]byte
	MaxVal    [16]byte
	NullCount uint64
}

// ZoneMapEntry represents a single zone map entry for a block.
type ZoneMapEntry struct {
	BlockOffset  uint64 // 8 bytes
	BlockSize    int64  // 8 bytes
	MinVal       [16]byte // 16 bytes
	MaxVal       [16]byte // 16 bytes
	NullCount    uint64 // 8 bytes
	RowCount     uint64 // 8 bytes
	// Reserved: 16 bytes
}

// Serialize serializes a ZoneMapEntry to bytes.
func (zm ZoneMapEntry) Serialize() ([]byte, error) {
	buf := make([]byte, 64)

	binary.LittleEndian.PutUint64(buf[0:8], zm.BlockOffset)
	binary.LittleEndian.PutUint64(buf[8:16], uint64(zm.BlockSize))
	copy(buf[16:32], zm.MinVal[:])
	copy(buf[32:48], zm.MaxVal[:])
	binary.LittleEndian.PutUint64(buf[48:56], zm.NullCount)
	binary.LittleEndian.PutUint64(buf[56:64], zm.RowCount)

	return buf, nil
}

// BlockedCuckooFilter is a cache-line-optimized bloom filter.
type BlockedCuckooFilter struct {
	blocks    []byte
	numBlocks uint32
	numItems  uint64
}

const (
	CuckooBlockSize       = 64    // One CPU cache line
	CuckooFingerprintBits = 8
	CuckooBucketSize      = 4
	CuckooTargetFPR       = 0.003 // 0.3%
)

// NewBlockedCuckooFilter creates a new blocked cuckoo filter.
func NewBlockedCuckooFilter(expectedItems uint64) *BlockedCuckooFilter {
	numBlocks := (expectedItems * CuckooFingerprintBits) / (CuckooBucketSize * 8 * CuckooBlockSize)
	if numBlocks < 1 {
		numBlocks = 1
	}
	return &BlockedCuckooFilter{
		blocks:    make([]byte, numBlocks*CuckooBlockSize),
		numBlocks: uint32(numBlocks),
	}
}

// Insert adds an item to the filter.
func (f *BlockedCuckooFilter) Insert(item []byte) bool {
	// TODO: Implement blocked cuckoo filter insertion
	return true
}

// MayContain checks if an item may be in the filter.
func (f *BlockedCuckooFilter) MayContain(item []byte) bool {
	// TODO: Implement blocked cuckoo filter lookup
	return true
}

// Serialize serializes the filter to bytes.
func (f *BlockedCuckooFilter) Serialize() []byte {
	// TODO: Implement serialization
	return nil
}

// PGMIndex is a Piecewise Geometric Model learned index.
type PGMIndex struct {
	levels  [][]Segment
	epsilon int
	minKey  Value
	maxKey  Value
	size    int
}

// Segment is a linear segment in the PGM index.
type Segment struct {
	Key       Value
	Slope     float64
	Intercept float64
}

// BuildPGMIndex builds a PGM index from sorted keys.
func BuildPGMIndex(sortedKeys []Value, epsilon int) *PGMIndex {
	// TODO: Implement PGM index construction
	return &PGMIndex{
		levels:  make([][]Segment, 0),
		epsilon: epsilon,
	}
}

// Search finds the approximate position of a key in the index.
func (p *PGMIndex) Search(key Value) (approxPos int, lo int, hi int) {
	// TODO: Implement PGM index search
	return 0, 0, 0
}

// Serialize serializes the PGM index to bytes.
func (p *PGMIndex) Serialize() []byte {
	// TODO: Implement serialization
	return nil
}

// DeserializePGMIndex deserializes a PGM index from bytes.
func DeserializePGMIndex(data []byte) *PGMIndex {
	// TODO: Implement deserialization
	return &PGMIndex{
		levels:  make([][]Segment, 0),
		epsilon: 64,
	}
}

// DeserializeBlockedCuckooFilter deserializes a blocked cuckoo filter from bytes.
func DeserializeBlockedCuckooFilter(data []byte) *BlockedCuckooFilter {
	// TODO: Implement deserialization
	return &BlockedCuckooFilter{
		blocks:    data,
		numBlocks: uint32(len(data) / CuckooBlockSize),
	}
}

// PartitionMeta is the partition metadata for catalog registration.
type PartitionMeta struct {
	TableID       uint64
	PartitionSeq  uint64
	S3Path        string
	TimeMin       int64
	TimeMax       int64
	RowCount      uint64
	FileSize      uint64
	LSNMin        uint64
	LSNMax        uint64
	Level         uint8
	ColumnCount   uint16
	ZoneMaps      []ZoneMapSummary
	CreatedAt     int64
	Checksum      [16]byte
}
