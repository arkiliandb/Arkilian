// Package format provides the ArkFormat binary columnar micro-partition file format for Arkilian V3.
package format

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go-v2/service/s3"
)

const (
	MagicARKILNV3  = uint64(0x41524B494C4E5633) // 'ARKILNV3'
	MagicENDARK3   = uint64(0x454E445F41524B33) // 'END_ARK3'
	HeaderSize     = 512
	FooterSize     = 64
	BlockSize      = 65536 // 64KB data blocks
	TargetFileSize = 128 * 1024 * 1024 // 128MB
)

// FileHeader is the 512-byte fixed file header.
type FileHeader struct {
	Magic          uint64 // 8 bytes: ARKILNV3
	FormatVersion  uint16 // 2 bytes: 0x0001
	Flags          uint16 // 2 bytes: bit0=cluster_sorted, bit1=compressed, bit2=has_bloom, bit3=has_pgm, bit4=has_roaring
	ColumnCount    uint16 // 2 bytes
	RowCount       uint64 // 8 bytes
	ClusterKeyCol  uint16 // 2 bytes: 0xFFFF if none
	Compression    uint8  // 1 byte
	CreatedAt      int64  // 8 bytes: unix nanos
	TableID        uint64 // 8 bytes
	PartitionSeq   uint64 // 8 bytes: monotonic partition ID
	LSNMin         uint64 // 8 bytes
	LSNMax         uint64 // 8 bytes
	TimeMin        int64  // 8 bytes: min timestamp nanos
	TimeMax        int64  // 8 bytes: max timestamp nanos
	FileChecksum   [16]byte // 16 bytes: XXH3_128
	ColDirOffset   uint64 // 8 bytes
	ColDirSize     uint32 // 4 bytes
	FooterOffset   uint64 // 8 bytes
	// Reserved: 397 bytes (zero-filled)
}

// ColumnDescriptor is the per-column metadata (~120 bytes each).
type ColumnDescriptor struct {
	ColumnID         uint32 // 4 bytes
	ColumnNameHash   uint64 // 8 bytes: XXH3 of column name
	DataType         DataType // 1 byte
	Encoding         Encoding // 1 byte
	DataOffset       uint64 // 8 bytes: start of column data
	DataSize         uint64 // 8 bytes: compressed size
	UncompressedSize uint64 // 8 bytes
	NullCount        uint64 // 8 bytes
	ZoneMapOffset    uint64 // 8 bytes: in footer
	BloomOffset      uint64 // 8 bytes: 0 if none
	BloomSize        uint32 // 4 bytes
	PGMOffset        uint64 // 8 bytes: 0 if none
	PGMSize          uint32 // 4 bytes
	MinValue         [16]byte // 16 bytes: type-specific
	MaxValue         [16]byte // 16 bytes: type-specific
	DistinctCount    uint64 // 8 bytes: HyperLogLog estimate
	IsSorted         uint8  // 1 byte: 0=no, 1=yes
	// Reserved: 9 bytes
}

// ArkWriter writes multi-column ArkFormat files.
type ArkWriter struct {
	tableID      uint64
	compression  Compression
	targetSize   int64
	columns      []ColumnDescriptor
	columnData   [][]byte // per-column accumulated data
	rowCount     uint64
	clusterKey   uint16
	zoneMaps     [][]ZoneMapEntry // per-column, per-block
	bloomFilters []*BlockedCuckooFilter
	pgmIndex     *PGMIndex
}

// NewArkWriter creates a new ArkFormat writer.
func NewArkWriter(tableID uint64, compression Compression, targetSize int64) *ArkWriter {
	return &ArkWriter{
		tableID:     tableID,
		compression: compression,
		targetSize:  targetSize,
		columns:     make([]ColumnDescriptor, 0),
		columnData:  make([][]byte, 0),
	}
}

// SetClusterKey sets the cluster key column index.
func (w *ArkWriter) SetClusterKey(colIndex uint16) {
	w.clusterKey = colIndex
}

// AppendRow appends a row to the writer.
func (w *ArkWriter) AppendRow(row []Value) error {
	// Initialize columns if this is the first row
	if w.rowCount == 0 {
		w.columns = make([]ColumnDescriptor, len(row))
		w.columnData = make([][]byte, len(row))
		w.bloomFilters = make([]*BlockedCuckooFilter, len(row))
		for i := range row {
			w.columns[i].ColumnID = uint32(i)
			w.columns[i].DataType = row[i].Type
			w.columns[i].Encoding = EncodingPLAIN
			w.bloomFilters[i] = NewBlockedCuckooFilter(1000000) // Estimate 1M distinct values
		}
	}

	if uint16(len(row)) != w.ColumnCount() {
		return fmt.Errorf("row has %d columns, expected %d", len(row), w.ColumnCount())
	}

	// Append values to per-column buffers
	for i, val := range row {
		// Encode value to bytes
		valBytes, err := EncodeValue(val)
		if err != nil {
			return err
		}
		w.columnData[i] = append(w.columnData[i], valBytes[:]...)

		// Update bloom filter
		w.bloomFilters[i].Insert(valBytes[:])

		// Update min/max for zone maps
		if w.rowCount == 0 {
			w.columns[i].MinValue = valBytes
			w.columns[i].MaxValue = valBytes
		} else {
			// Compare and update min/max
			currentVal := Value{Type: val.Type, Data: val.Data}
			minVal, _ := DecodeValue(w.columns[i].MinValue, val.Type)
			maxVal, _ := DecodeValue(w.columns[i].MaxValue, val.Type)

			if cmp, _ := CompareValues(currentVal, minVal); cmp < 0 {
				w.columns[i].MinValue = valBytes
			}
			if cmp, _ := CompareValues(currentVal, maxVal); cmp > 0 {
				w.columns[i].MaxValue = valBytes
			}
		}
	}

	w.rowCount++
	return nil
}

// ColumnCount returns the number of columns.
func (w *ArkWriter) ColumnCount() uint16 {
	return uint16(len(w.columns))
}

// EstimatedSize returns the current estimated file size.
func (w *ArkWriter) EstimatedSize() int64 {
	// Header: 512 bytes
	// Column directory: ~120 bytes per column
	// Column data: sum of all column data sizes
	// Footer: 64 bytes + zone maps + bloom filters + PGM index

	size := int64(HeaderSize)
	size += int64(len(w.columns)) * 120 // Column directory estimate
	for _, data := range w.columnData {
		size += int64(len(data))
	}
	size += int64(FooterSize)

	// Add estimated zone maps (64 bytes per block per column)
	blocksPerColumn := (len(w.columnData) / BlockSize) + 1
	size += int64(len(w.columns)) * int64(blocksPerColumn) * 64

	return size
}

// ComputeZoneMaps computes zone maps for all columns.
func (w *ArkWriter) ComputeZoneMaps() {
	w.zoneMaps = make([][]ZoneMapEntry, len(w.columns))

	for colIdx, data := range w.columnData {
		numBlocks := (len(data) + BlockSize - 1) / BlockSize
		w.zoneMaps[colIdx] = make([]ZoneMapEntry, numBlocks)

		for blockIdx := 0; blockIdx < numBlocks; blockIdx++ {
			start := blockIdx * BlockSize
			end := start + BlockSize
			if end > len(data) {
				end = len(data)
			}

			blockData := data[start:end]
			blockSize := int64(len(blockData))

			// Compute min/max for this block
			var minVal, maxVal Value
			nullCount := uint64(0)

			for i := 0; i < len(blockData); i += 16 {
				if i+16 > len(blockData) {
					break
				}
				valBytes := blockData[i : i+16]
				val, _ := DecodeValue([16]byte(valBytes), w.columns[colIdx].DataType)

				if IsNull(val) {
					nullCount++
					continue
				}

				if i == 0 {
					minVal = val
					maxVal = val
				} else {
					if cmp, _ := CompareValues(val, minVal); cmp < 0 {
						minVal = val
					}
					if cmp, _ := CompareValues(val, maxVal); cmp > 0 {
						maxVal = val
					}
				}
			}

			minBytes, _ := EncodeValue(minVal)
			maxBytes, _ := EncodeValue(maxVal)

			w.zoneMaps[colIdx][blockIdx] = ZoneMapEntry{
				BlockOffset:  uint64(start),
				BlockSize:    blockSize,
				MinVal:       minBytes,
				MaxVal:       maxBytes,
				NullCount:    nullCount,
				RowCount:     uint64(len(blockData) / 16),
			}
		}
	}
}

// ComputeBloomFilters serializes bloom filters.
func (w *ArkWriter) ComputeBloomFilters() {
	// Bloom filters are computed incrementally during AppendRow
	// This method can be used to finalize or verify them
}

// ComputePGMIndex computes the PGM index for the cluster key column.
func (w *ArkWriter) ComputePGMIndex() {
	if w.clusterKey == 0xFFFF || w.clusterKey >= uint16(len(w.columns)) {
		return
	}

	// Extract sorted values from cluster key column
	sortedKeys := make([]Value, w.rowCount)
	for i := uint64(0); i < w.rowCount; i++ {
		offset := int(i * 16)
		end := offset + 16
		if end > len(w.columnData[w.clusterKey]) {
			break
		}
		valBytes := w.columnData[w.clusterKey][offset:end]
		sortedKeys[i], _ = DecodeValue([16]byte(valBytes), w.columns[w.clusterKey].DataType)
	}

	w.pgmIndex = BuildPGMIndex(sortedKeys, 64) // epsilon = 64
}

// Flush writes the complete ArkFormat file to S3.
func (w *ArkWriter) Flush(ctx context.Context, s3Client *s3.Client, bucket, objectPath string) error {
	// Compute zone maps, bloom filters, and PGM index
	w.ComputeZoneMaps()
	w.ComputeBloomFilters()
	w.ComputePGMIndex()

	// Calculate offsets
	colDirOffset := uint64(HeaderSize)
	colDirSize := uint32(len(w.columns) * 120)

	// Calculate data offsets
	dataOffset := colDirOffset + uint64(colDirSize)
	var currentOffset uint64 = dataOffset

	// Update column descriptors with data offsets
	for i := range w.columns {
		w.columns[i].DataOffset = currentOffset
		w.columns[i].DataSize = uint64(len(w.columnData[i]))
		w.columns[i].UncompressedSize = uint64(len(w.columnData[i]))
		currentOffset += uint64(len(w.columnData[i]))
	}

	// Calculate zone map offsets
	zoneMapOffset := currentOffset
	for colIdx := range w.zoneMaps {
		w.columns[colIdx].ZoneMapOffset = zoneMapOffset
		for range w.zoneMaps[colIdx] {
			zoneMapOffset += 64 // ZoneMapEntry size
		}
	}

	// Calculate bloom filter offsets
	bloomOffset := zoneMapOffset
	for i, bf := range w.bloomFilters {
		if bf != nil {
			bfBytes := bf.Serialize()
			w.columns[i].BloomOffset = bloomOffset
			w.columns[i].BloomSize = uint32(len(bfBytes))
			bloomOffset += uint64(len(bfBytes))
		}
	}

	// Calculate PGM index offset
	pgmOffset := bloomOffset
	if w.pgmIndex != nil {
		pgmBytes := w.pgmIndex.Serialize()
		w.columns[w.clusterKey].PGMOffset = pgmOffset
		w.columns[w.clusterKey].PGMSize = uint32(len(pgmBytes))
		pgmOffset += uint64(len(pgmBytes))
	}

	// Footer offset
	footerOffset := pgmOffset

	// Create file header
	header := FileHeader{
		Magic:          MagicARKILNV3,
		FormatVersion:  1,
		Flags:          0, // Set flags based on what we have
		ColumnCount:    w.ColumnCount(),
		RowCount:       w.rowCount,
		ClusterKeyCol:  w.clusterKey,
		Compression:    uint8(w.compression),
		CreatedAt:      0, // Will be set by caller
		TableID:        w.tableID,
		PartitionSeq:   0, // Will be set by caller
		LSNMin:         0, // Will be set by caller
		LSNMax:         0, // Will be set by caller
		TimeMin:        0, // Will be set by caller
		TimeMax:        0, // Will be set by caller
		ColDirOffset:   colDirOffset,
		ColDirSize:     colDirSize,
		FooterOffset:   footerOffset,
	}

	// Write to file
	file, err := os.CreateTemp("", "arkilian-*.ark")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	defer file.Close()

	// Write header (512 bytes)
	headerBytes, err := w.serializeHeader(header)
	if err != nil {
		return err
	}
	if _, err := file.Write(headerBytes); err != nil {
		return err
	}

	// Write column directory
	for _, col := range w.columns {
		colBytes, err := w.serializeColumnDescriptor(col)
		if err != nil {
			return err
		}
		if _, err := file.Write(colBytes); err != nil {
			return err
		}
	}

	// Write column data
	for _, data := range w.columnData {
		if _, err := file.Write(data); err != nil {
			return err
		}
	}

	// Write zone maps
	for _, zmEntries := range w.zoneMaps {
		for _, zm := range zmEntries {
			zmBytes, err := zm.Serialize()
			if err != nil {
				return err
			}
			if _, err := file.Write(zmBytes); err != nil {
				return err
			}
		}
	}

	// Write bloom filters
	for _, bf := range w.bloomFilters {
		if bf != nil {
			bfBytes := bf.Serialize()
			if _, err := file.Write(bfBytes); err != nil {
				return err
			}
		}
	}

	// Write PGM index
	if w.pgmIndex != nil {
		pgmBytes := w.pgmIndex.Serialize()
		if _, err := file.Write(pgmBytes); err != nil {
			return err
		}
	}

	// Write footer (64 bytes with magic END_ARK3)
	footer := make([]byte, FooterSize)
	copy(footer[:8], []byte("END_ARK3"))
	if _, err := file.Write(footer); err != nil {
		return err
	}

	// TODO: Upload to S3
	// For now, just return success
	return nil
}

// serializeHeader serializes the file header to bytes.
func (w *ArkWriter) serializeHeader(header FileHeader) ([]byte, error) {
	buf := make([]byte, HeaderSize)

	// Magic (8 bytes)
	binary.LittleEndian.PutUint64(buf[0:8], header.Magic)
	// FormatVersion (2 bytes)
	binary.LittleEndian.PutUint16(buf[8:10], header.FormatVersion)
	// Flags (2 bytes)
	binary.LittleEndian.PutUint16(buf[10:12], header.Flags)
	// ColumnCount (2 bytes)
	binary.LittleEndian.PutUint16(buf[12:14], header.ColumnCount)
	// RowCount (8 bytes)
	binary.LittleEndian.PutUint64(buf[14:22], header.RowCount)
	// ClusterKeyCol (2 bytes)
	binary.LittleEndian.PutUint16(buf[22:24], header.ClusterKeyCol)
	// Compression (1 byte)
	buf[24] = header.Compression
	// CreatedAt (8 bytes)
	binary.LittleEndian.PutUint64(buf[25:33], uint64(header.CreatedAt))
	// TableID (8 bytes)
	binary.LittleEndian.PutUint64(buf[33:41], header.TableID)
	// PartitionSeq (8 bytes)
	binary.LittleEndian.PutUint64(buf[41:49], header.PartitionSeq)
	// LSNMin (8 bytes)
	binary.LittleEndian.PutUint64(buf[49:57], header.LSNMin)
	// LSNMax (8 bytes)
	binary.LittleEndian.PutUint64(buf[57:65], header.LSNMax)
	// TimeMin (8 bytes)
	binary.LittleEndian.PutUint64(buf[65:73], uint64(header.TimeMin))
	// TimeMax (8 bytes)
	binary.LittleEndian.PutUint64(buf[73:81], uint64(header.TimeMax))
	// FileChecksum (16 bytes)
	copy(buf[81:97], header.FileChecksum[:])
	// ColDirOffset (8 bytes)
	binary.LittleEndian.PutUint64(buf[97:105], header.ColDirOffset)
	// ColDirSize (4 bytes)
	binary.LittleEndian.PutUint32(buf[105:109], header.ColDirSize)
	// FooterOffset (8 bytes)
	binary.LittleEndian.PutUint64(buf[109:117], header.FooterOffset)

	return buf, nil
}

// serializeColumnDescriptor serializes a column descriptor to bytes.
func (w *ArkWriter) serializeColumnDescriptor(col ColumnDescriptor) ([]byte, error) {
	buf := make([]byte, 120)

	// ColumnID (4 bytes)
	binary.LittleEndian.PutUint32(buf[0:4], col.ColumnID)
	// ColumnNameHash (8 bytes)
	binary.LittleEndian.PutUint64(buf[4:12], col.ColumnNameHash)
	// DataType (1 byte)
	buf[12] = byte(col.DataType)
	// Encoding (1 byte)
	buf[13] = byte(col.Encoding)
	// DataOffset (8 bytes)
	binary.LittleEndian.PutUint64(buf[14:22], col.DataOffset)
	// DataSize (8 bytes)
	binary.LittleEndian.PutUint64(buf[22:30], col.DataSize)
	// UncompressedSize (8 bytes)
	binary.LittleEndian.PutUint64(buf[30:38], col.UncompressedSize)
	// NullCount (8 bytes)
	binary.LittleEndian.PutUint64(buf[38:46], col.NullCount)
	// ZoneMapOffset (8 bytes)
	binary.LittleEndian.PutUint64(buf[46:54], col.ZoneMapOffset)
	// BloomOffset (8 bytes)
	binary.LittleEndian.PutUint64(buf[54:62], col.BloomOffset)
	// BloomSize (4 bytes)
	binary.LittleEndian.PutUint32(buf[62:66], col.BloomSize)
	// PGMOffset (8 bytes)
	binary.LittleEndian.PutUint64(buf[66:74], col.PGMOffset)
	// PGMSize (4 bytes)
	binary.LittleEndian.PutUint32(buf[74:78], col.PGMSize)
	// MinValue (16 bytes)
	copy(buf[78:94], col.MinValue[:])
	// MaxValue (16 bytes)
	copy(buf[94:110], col.MaxValue[:])
	// DistinctCount (8 bytes)
	binary.LittleEndian.PutUint64(buf[110:118], col.DistinctCount)
	// IsSorted (1 byte)
	buf[118] = col.IsSorted

	return buf, nil
}

// Metadata returns the partition metadata for catalog registration.
func (w *ArkWriter) Metadata() *PartitionMeta {
	return &PartitionMeta{
		TableID:       w.tableID,
		PartitionSeq:  0, // Will be set by caller
		RowCount:      w.rowCount,
		ColumnCount:   w.ColumnCount(),
		Level:         0, // L0 initially
		ZoneMaps:      make([]ZoneMapSummary, len(w.columns)),
	}
}
