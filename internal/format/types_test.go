package format

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestRowIDEncodeDecode(t *testing.T) {
	// Test round-trip for max values
	maxSeq := uint32(0xFFFFFFFF)
	maxOffset := uint32(0xFFFFFFFF)
	rid := NewRowID(maxSeq, maxOffset)
	assert.Equal(t, maxSeq, rid.PartitionSeq())
	assert.Equal(t, maxOffset, rid.RowOffset())

	// Test round-trip for zero values
	zeroRid := NewRowID(0, 0)
	assert.Equal(t, uint32(0), zeroRid.PartitionSeq())
	assert.Equal(t, uint32(0), zeroRid.RowOffset())

	// Test round-trip for random values
	testCases := []struct {
		seq     uint32
		offset  uint32
	}{
		{seq: 1, offset: 1},
		{seq: 100, offset: 200},
		{seq: 1000, offset: 5000},
		{seq: 12345, offset: 67890},
		{seq: 100000, offset: 200000},
	}

	for _, tc := range testCases {
		rid := NewRowID(tc.seq, tc.offset)
		assert.Equal(t, tc.seq, rid.PartitionSeq(), "PartitionSeq mismatch")
		assert.Equal(t, tc.offset, rid.RowOffset(), "RowOffset mismatch")
	}
}

func TestRowIDSlotEncoding(t *testing.T) {
	rid := NewRowID(12345, 67890)
	buf := rid.Encode()
	decoded := DecodeRowID(buf)
	assert.Equal(t, rid, decoded)
}

func TestValueSerialization(t *testing.T) {
	testCases := []struct {
		name string
		val  Value
	}{
		{"INT64", Value{Type: TypeINT64, Data: int64(12345)}},
		{"FLOAT64", Value{Type: TypeFLOAT64, Data: float64(3.14159)}},
		{"BOOL true", Value{Type: TypeBOOL, Data: true}},
		{"BOOL false", Value{Type: TypeBOOL, Data: false}},
		{"STRING", Value{Type: TypeSTRING, Data: "hello world"}},
		{"TIMESTAMP", Value{Type: TypeTIMESTAMP, Data: time.Unix(1234567890, 0)}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			jsonBytes, err := tc.val.MarshalJSON()
			assert.NoError(t, err)

			var decoded Value
			err = decoded.UnmarshalJSON(jsonBytes)
			assert.NoError(t, err)
			assert.Equal(t, tc.val.Type, decoded.Type)

			// Compare values based on type
			switch tc.val.Type {
			case TypeINT64:
				assert.Equal(t, tc.val.Data.(int64), decoded.Data.(int64))
			case TypeFLOAT64:
				assert.Equal(t, tc.val.Data.(float64), decoded.Data.(float64))
			case TypeBOOL:
				assert.Equal(t, tc.val.Data.(bool), decoded.Data.(bool))
			case TypeSTRING:
				assert.Equal(t, tc.val.Data.(string), decoded.Data.(string))
			case TypeTIMESTAMP:
				assert.Equal(t, tc.val.Data.(time.Time), decoded.Data.(time.Time))
			}
		})
	}
}

func TestValueSlotEncoding(t *testing.T) {
	testCases := []struct {
		name string
		val  Value
	}{
		{"INT64", Value{Type: TypeINT64, Data: int64(1234567890)}},
		{"FLOAT64", Value{Type: TypeFLOAT64, Data: float64(3.14159)}},
		{"BOOL", Value{Type: TypeBOOL, Data: true}},
		{"STRING", Value{Type: TypeSTRING, Data: "test"}},
		{"TIMESTAMP", Value{Type: TypeTIMESTAMP, Data: time.Unix(1234567890, 0)}},
		{"INT32", Value{Type: TypeINT32, Data: int32(12345)}},
		{"FLOAT32", Value{Type: TypeFLOAT32, Data: float32(3.14)}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			buf, err := EncodeValue(tc.val)
			assert.NoError(t, err)

			decoded, err := DecodeValue(buf, tc.val.Type)
			assert.NoError(t, err)
			assert.Equal(t, tc.val.Type, decoded.Type)

			// Compare values based on type
			switch tc.val.Type {
			case TypeINT64:
				assert.Equal(t, tc.val.Data.(int64), decoded.Data.(int64))
			case TypeFLOAT64:
				assert.Equal(t, tc.val.Data.(float64), decoded.Data.(float64))
			case TypeBOOL:
				assert.Equal(t, tc.val.Data.(bool), decoded.Data.(bool))
			case TypeSTRING:
				assert.Equal(t, tc.val.Data.(string), decoded.Data.(string))
			case TypeTIMESTAMP:
				assert.Equal(t, tc.val.Data.(time.Time), decoded.Data.(time.Time))
			case TypeINT32:
				assert.Equal(t, tc.val.Data.(int32), decoded.Data.(int32))
			case TypeFLOAT32:
				assert.Equal(t, tc.val.Data.(float32), decoded.Data.(float32))
			}
		})
	}
}

func TestValueComparison(t *testing.T) {
	testCases := []struct {
		name     string
		v1       Value
		v2       Value
		expected int
	}{
		{"INT64 equal", Value{Type: TypeINT64, Data: int64(100)}, Value{Type: TypeINT64, Data: int64(100)}, 0},
		{"INT64 less", Value{Type: TypeINT64, Data: int64(50)}, Value{Type: TypeINT64, Data: int64(100)}, -1},
		{"INT64 greater", Value{Type: TypeINT64, Data: int64(150)}, Value{Type: TypeINT64, Data: int64(100)}, 1},
		{"FLOAT64 equal", Value{Type: TypeFLOAT64, Data: float64(3.14)}, Value{Type: TypeFLOAT64, Data: float64(3.14)}, 0},
		{"FLOAT64 less", Value{Type: TypeFLOAT64, Data: float64(3.1)}, Value{Type: TypeFLOAT64, Data: float64(3.14)}, -1},
		{"FLOAT64 greater", Value{Type: TypeFLOAT64, Data: float64(3.2)}, Value{Type: TypeFLOAT64, Data: float64(3.14)}, 1},
		{"BOOL equal", Value{Type: TypeBOOL, Data: true}, Value{Type: TypeBOOL, Data: true}, 0},
		{"BOOL false < true", Value{Type: TypeBOOL, Data: false}, Value{Type: TypeBOOL, Data: true}, -1},
		{"STRING equal", Value{Type: TypeSTRING, Data: "test"}, Value{Type: TypeSTRING, Data: "test"}, 0},
		{"STRING less", Value{Type: TypeSTRING, Data: "apple"}, Value{Type: TypeSTRING, Data: "banana"}, -1},
		{"STRING greater", Value{Type: TypeSTRING, Data: "cherry"}, Value{Type: TypeSTRING, Data: "banana"}, 1},
		{"TIMESTAMP equal", Value{Type: TypeTIMESTAMP, Data: time.Unix(100, 0)}, Value{Type: TypeTIMESTAMP, Data: time.Unix(100, 0)}, 0},
		{"TIMESTAMP less", Value{Type: TypeTIMESTAMP, Data: time.Unix(50, 0)}, Value{Type: TypeTIMESTAMP, Data: time.Unix(100, 0)}, -1},
		{"TIMESTAMP greater", Value{Type: TypeTIMESTAMP, Data: time.Unix(150, 0)}, Value{Type: TypeTIMESTAMP, Data: time.Unix(100, 0)}, 1},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := CompareValues(tc.v1, tc.v2)
			assert.NoError(t, err)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestValueComparisonEdgeCases(t *testing.T) {
	// Test NaN comparison
	nan1 := Value{Type: TypeFLOAT64, Data: math.NaN()}
	nan2 := Value{Type: TypeFLOAT64, Data: math.NaN()}
	result, err := CompareValues(nan1, nan2)
	assert.NoError(t, err)
	// NaN == NaN should return 0 (equal) for our purposes
	assert.Equal(t, 0, result)

	// Test empty string
	empty1 := Value{Type: TypeSTRING, Data: ""}
	empty2 := Value{Type: TypeSTRING, Data: ""}
	result, err = CompareValues(empty1, empty2)
	assert.NoError(t, err)
	assert.Equal(t, 0, result)

	// Test nil bytes
	nilBytes1 := Value{Type: TypeBYTES, Data: []byte{}}
	nilBytes2 := Value{Type: TypeBYTES, Data: []byte{}}
	result, err = CompareValues(nilBytes1, nilBytes2)
	assert.NoError(t, err)
	assert.Equal(t, 0, result)
}

func TestValueComparisonDifferentTypes(t *testing.T) {
	v1 := Value{Type: TypeINT64, Data: int64(100)}
	v2 := Value{Type: TypeSTRING, Data: "100"}
	result, err := CompareValues(v1, v2)
	assert.Error(t, err)
	assert.Equal(t, 0, result)
}

func TestIsNull(t *testing.T) {
	// Null value
	nullVal := Value{}
	assert.True(t, IsNull(nullVal))

	// Non-null value
	nonNullVal := Value{Type: TypeINT64, Data: int64(100)}
	assert.False(t, IsNull(nonNullVal))
}

func TestIsNaN(t *testing.T) {
	// NaN float64
	nanVal := Value{Type: TypeFLOAT64, Data: math.NaN()}
	assert.True(t, IsNaN(nanVal))

	// Regular float64
	regularVal := Value{Type: TypeFLOAT64, Data: float64(3.14)}
	assert.False(t, IsNaN(regularVal))

	// Non-float type
	intVal := Value{Type: TypeINT64, Data: int64(100)}
	assert.False(t, IsNaN(intVal))
}
