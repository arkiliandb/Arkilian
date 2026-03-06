package format

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestArkWriterBasic(t *testing.T) {
	// Test basic ArkWriter creation and row appending
	writer := NewArkWriter(1, CompressionZSTD, TargetFileSize)

	// Test column count before any rows
	assert.Equal(t, uint16(0), writer.ColumnCount())

	// Append a row with multiple columns
	row := []Value{
		{Type: TypeINT64, Data: int64(1)},
		{Type: TypeSTRING, Data: "test"},
		{Type: TypeFLOAT64, Data: 3.14},
	}

	err := writer.AppendRow(row)
	assert.NoError(t, err)

	// Test column count after appending row
	assert.Equal(t, uint16(3), writer.ColumnCount())

	// Test row count
	assert.Equal(t, uint64(1), writer.rowCount)
}

func TestArkWriterMultipleRows(t *testing.T) {
	writer := NewArkWriter(1, CompressionZSTD, TargetFileSize)

	// Append multiple rows
	for i := uint64(0); i < 100; i++ {
		row := []Value{
			{Type: TypeINT64, Data: int64(i)},
			{Type: TypeSTRING, Data: "row"},
			{Type: TypeFLOAT64, Data: float64(i * 2)},
		}
		err := writer.AppendRow(row)
		assert.NoError(t, err)
	}

	// Test row count
	assert.Equal(t, uint64(100), writer.rowCount)
}

func TestArkWriterEstimatedSize(t *testing.T) {
	writer := NewArkWriter(1, CompressionZSTD, TargetFileSize)

	// Append some rows
	for i := uint64(0); i < 1000; i++ {
		row := []Value{
			{Type: TypeINT64, Data: int64(i)},
			{Type: TypeSTRING, Data: "test"},
		}
		err := writer.AppendRow(row)
		assert.NoError(t, err)
	}

	// Test estimated size
	size := writer.EstimatedSize()
	assert.Greater(t, size, int64(0))
}

func TestArkWriterSetClusterKey(t *testing.T) {
	writer := NewArkWriter(1, CompressionZSTD, TargetFileSize)

	// Set cluster key
	writer.SetClusterKey(0)

	// Test cluster key
	assert.Equal(t, uint16(0), writer.clusterKey)
}

func TestArkWriterComputeZoneMaps(t *testing.T) {
	writer := NewArkWriter(1, CompressionZSTD, TargetFileSize)

	// Append rows
	for i := uint64(0); i < 100; i++ {
		row := []Value{
			{Type: TypeINT64, Data: int64(i)},
		}
		err := writer.AppendRow(row)
		assert.NoError(t, err)
	}

	// Compute zone maps
	writer.ComputeZoneMaps()

	// Test zone maps were computed
	assert.Greater(t, len(writer.zoneMaps), 0)
	assert.Greater(t, len(writer.zoneMaps[0]), 0)
}

func TestArkWriterFlush(t *testing.T) {
	writer := NewArkWriter(1, CompressionZSTD, TargetFileSize)

	// Append rows
	for i := uint64(0); i < 100; i++ {
		row := []Value{
			{Type: TypeINT64, Data: int64(i)},
			{Type: TypeSTRING, Data: "test"},
		}
		err := writer.AppendRow(row)
		assert.NoError(t, err)
	}

	// Flush to file
	err := writer.Flush(context.Background(), nil, "", "")
	assert.NoError(t, err)
}

func TestArkWriterMetadata(t *testing.T) {
	writer := NewArkWriter(1, CompressionZSTD, TargetFileSize)

	// Append rows
	for i := uint64(0); i < 100; i++ {
		row := []Value{
			{Type: TypeINT64, Data: int64(i)},
		}
		err := writer.AppendRow(row)
		assert.NoError(t, err)
	}

	// Get metadata
	meta := writer.Metadata()

	// Test metadata
	assert.Equal(t, uint64(1), meta.TableID)
	assert.Equal(t, uint64(100), meta.RowCount)
	assert.Equal(t, uint16(1), meta.ColumnCount)
	assert.Equal(t, uint8(0), meta.Level)
}

func TestArkWriterColumnCountMismatch(t *testing.T) {
	writer := NewArkWriter(1, CompressionZSTD, TargetFileSize)

	// Append first row
	row1 := []Value{
		{Type: TypeINT64, Data: int64(1)},
		{Type: TypeSTRING, Data: "test"},
	}
	err := writer.AppendRow(row1)
	assert.NoError(t, err)

	// Try to append row with wrong column count
	row2 := []Value{
		{Type: TypeINT64, Data: int64(2)},
		{Type: TypeSTRING, Data: "test"},
		{Type: TypeFLOAT64, Data: 3.14},
	}
	err = writer.AppendRow(row2)
	assert.Error(t, err)
}
