// Package format provides the ArkFormat binary columnar micro-partition file format for Arkilian V3.
package format

import (
	"encoding/binary"
	"fmt"
	"os"
)

// ArkReader reads ArkFormat files with column-skip support.
type ArkReader struct {
	path       string
	header     *FileHeader
	colDir     []ColumnDescriptor
	footer     *FileFooter
	mmapRegion []byte // mmap'd footer for zero-copy metadata reads
	file       *os.File
}

// FileFooter is the 64-byte fixed footer at the end of the file.
type FileFooter struct {
	Magic [8]byte // 8 bytes: END_ARK3
	// Reserved: 56 bytes
}

// OpenArkFile opens an ArkFormat file for reading.
func OpenArkFile(path string) (*ArkReader, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	// Get file size
	_, err = file.Stat()
	if err != nil {
		file.Close()
		return nil, err
	}

	// Read header (512 bytes)
	headerBytes := make([]byte, HeaderSize)
	_, err = file.Read(headerBytes)
	if err != nil {
		file.Close()
		return nil, err
	}

	header, err := parseFileHeader(headerBytes)
	if err != nil {
		file.Close()
		return nil, err
	}

	// Read column directory
	colDirSize := int(header.ColDirSize)
	colDirBytes := make([]byte, colDirSize)
	_, err = file.Read(colDirBytes)
	if err != nil {
		file.Close()
		return nil, err
	}

	colDir, err := parseColumnDirectory(colDirBytes, int(header.ColumnCount))
	if err != nil {
		file.Close()
		return nil, err
	}

	// Read footer (mmap for zero-copy)
	footerSize := FooterSize
	footerOffset := int64(header.FooterOffset)
	footerBytes := make([]byte, footerSize)

	_, err = file.ReadAt(footerBytes, footerOffset)
	if err != nil {
		file.Close()
		return nil, err
	}

	footer, err := parseFileFooter(footerBytes)
	if err != nil {
		file.Close()
		return nil, err
	}

	return &ArkReader{
		path:       path,
		header:     header,
		colDir:     colDir,
		footer:     footer,
		file:       file,
	}, nil
}

// parseFileHeader parses the file header from bytes.
func parseFileHeader(data []byte) (*FileHeader, error) {
	if len(data) < HeaderSize {
		return nil, fmt.Errorf("header too short: %d bytes", len(data))
	}

	header := &FileHeader{}

	// Magic (8 bytes)
	header.Magic = binary.LittleEndian.Uint64(data[0:8])
	if header.Magic != MagicARKILNV3 {
		return nil, fmt.Errorf("invalid magic: %x", header.Magic)
	}

	// FormatVersion (2 bytes)
	header.FormatVersion = binary.LittleEndian.Uint16(data[8:10])

	// Flags (2 bytes)
	header.Flags = binary.LittleEndian.Uint16(data[10:12])

	// ColumnCount (2 bytes)
	header.ColumnCount = binary.LittleEndian.Uint16(data[12:14])

	// RowCount (8 bytes)
	header.RowCount = binary.LittleEndian.Uint64(data[14:22])

	// ClusterKeyCol (2 bytes)
	header.ClusterKeyCol = binary.LittleEndian.Uint16(data[22:24])

	// Compression (1 byte)
	header.Compression = data[24]

	// CreatedAt (8 bytes)
	header.CreatedAt = int64(binary.LittleEndian.Uint64(data[25:33]))

	// TableID (8 bytes)
	header.TableID = binary.LittleEndian.Uint64(data[33:41])

	// PartitionSeq (8 bytes)
	header.PartitionSeq = binary.LittleEndian.Uint64(data[41:49])

	// LSNMin (8 bytes)
	header.LSNMin = binary.LittleEndian.Uint64(data[49:57])

	// LSNMax (8 bytes)
	header.LSNMax = binary.LittleEndian.Uint64(data[57:65])

	// TimeMin (8 bytes)
	header.TimeMin = int64(binary.LittleEndian.Uint64(data[65:73]))

	// TimeMax (8 bytes)
	header.TimeMax = int64(binary.LittleEndian.Uint64(data[73:81]))

	// FileChecksum (16 bytes)
	copy(header.FileChecksum[:], data[81:97])

	// ColDirOffset (8 bytes)
	header.ColDirOffset = binary.LittleEndian.Uint64(data[97:105])

	// ColDirSize (4 bytes)
	header.ColDirSize = binary.LittleEndian.Uint32(data[105:109])

	// FooterOffset (8 bytes)
	header.FooterOffset = binary.LittleEndian.Uint64(data[109:117])

	return header, nil
}

// parseColumnDirectory parses the column directory from bytes.
func parseColumnDirectory(data []byte, numColumns int) ([]ColumnDescriptor, error) {
	colDir := make([]ColumnDescriptor, numColumns)

	for i := 0; i < numColumns; i++ {
		offset := i * 120
		if offset+120 > len(data) {
			return nil, fmt.Errorf("column directory too short at index %d", i)
		}

		col := &colDir[i]

		// ColumnID (4 bytes)
		col.ColumnID = binary.LittleEndian.Uint32(data[offset : offset+4])

		// ColumnNameHash (8 bytes)
		col.ColumnNameHash = binary.LittleEndian.Uint64(data[offset+4 : offset+12])

		// DataType (1 byte)
		col.DataType = DataType(data[offset+12])

		// Encoding (1 byte)
		col.Encoding = Encoding(data[offset+13])

		// DataOffset (8 bytes)
		col.DataOffset = binary.LittleEndian.Uint64(data[offset+14 : offset+22])

		// DataSize (8 bytes)
		col.DataSize = binary.LittleEndian.Uint64(data[offset+22 : offset+30])

		// UncompressedSize (8 bytes)
		col.UncompressedSize = binary.LittleEndian.Uint64(data[offset+30 : offset+38])

		// NullCount (8 bytes)
		col.NullCount = binary.LittleEndian.Uint64(data[offset+38 : offset+46])

		// ZoneMapOffset (8 bytes)
		col.ZoneMapOffset = binary.LittleEndian.Uint64(data[offset+46 : offset+54])

		// BloomOffset (8 bytes)
		col.BloomOffset = binary.LittleEndian.Uint64(data[offset+54 : offset+62])

		// BloomSize (4 bytes)
		col.BloomSize = binary.LittleEndian.Uint32(data[offset+62 : offset+66])

		// PGMOffset (8 bytes)
		col.PGMOffset = binary.LittleEndian.Uint64(data[offset+66 : offset+74])

		// PGMSize (4 bytes)
		col.PGMSize = binary.LittleEndian.Uint32(data[offset+74 : offset+78])

		// MinValue (16 bytes)
		copy(col.MinValue[:], data[offset+78:offset+94])

		// MaxValue (16 bytes)
		copy(col.MaxValue[:], data[offset+94:offset+110])

		// DistinctCount (8 bytes)
		col.DistinctCount = binary.LittleEndian.Uint64(data[offset+110 : offset+118])

		// IsSorted (1 byte)
		col.IsSorted = data[offset+118]
	}

	return colDir, nil
}

// parseFileFooter parses the file footer from bytes.
func parseFileFooter(data []byte) (*FileFooter, error) {
	if len(data) < FooterSize {
		return nil, fmt.Errorf("footer too short: %d bytes", len(data))
	}

	footer := &FileFooter{}
	copy(footer.Magic[:], data[0:8])

	// Verify magic
	expectedMagic := [8]byte{'E', 'N', 'D', '_', 'A', 'R', 'K', '3'}
	if footer.Magic != expectedMagic {
		return nil, fmt.Errorf("invalid footer magic: %x", footer.Magic)
	}

	return footer, nil
}

// ReadColumns reads only the requested columns via byte-range offsets.
func (r *ArkReader) ReadColumns(colIDs []uint32) ([]ColumnBatch, error) {
	batches := make([]ColumnBatch, 0, len(colIDs))

	for _, colID := range colIDs {
		col, err := r.getColumnByColID(colID)
		if err != nil {
			return nil, err
		}

		// Read column data
		data := make([]byte, col.DataSize)
		_, err = r.file.ReadAt(data, int64(col.DataOffset))
		if err != nil {
			return nil, err
		}

		batch := ColumnBatch{
			Values:   data,
			Nulls:    make([]byte, (len(data)+7)/8), // Bitmask for nulls
			RowCount: int(r.header.RowCount),
			DataType: col.DataType,
		}

		batches = append(batches, batch)
	}

	return batches, nil
}

// ReadColumnRange reads a range of rows from a specific column.
func (r *ArkReader) ReadColumnRange(colID uint32, startRow, endRow uint64) (*ColumnBatch, error) {
	col, err := r.getColumnByColID(colID)
	if err != nil {
		return nil, err
	}

	// Calculate byte offset for the row range
	rowSize := int64(16) // Each value is 16 bytes
	startOffset := int64(startRow) * rowSize
	endOffset := int64(endRow) * rowSize

	data := make([]byte, endOffset-startOffset)
	_, err = r.file.ReadAt(data, int64(col.DataOffset)+startOffset)
	if err != nil {
		return nil, err
	}

	batch := &ColumnBatch{
		Values:   data,
		Nulls:    make([]byte, (len(data)+7)/8),
		RowCount: int(endRow - startRow),
		DataType: col.DataType,
	}

	return batch, nil
}

// getColumnByColID finds a column by its ID.
func (r *ArkReader) getColumnByColID(colID uint32) (*ColumnDescriptor, error) {
	for i := range r.colDir {
		if r.colDir[i].ColumnID == colID {
			return &r.colDir[i], nil
		}
	}
	return nil, fmt.Errorf("column %d not found", colID)
}

// GetZoneMap returns the zone map for a column.
func (r *ArkReader) GetZoneMap(colID uint32) ([]ZoneMapEntry, error) {
	col, err := r.getColumnByColID(colID)
	if err != nil {
		return nil, err
	}

	if col.ZoneMapOffset == 0 {
		return nil, fmt.Errorf("no zone map for column %d", colID)
	}

	// Read zone map entries
	// Each entry is 64 bytes
	numEntries := int(col.DataSize / BlockSize)
	if numEntries == 0 {
		numEntries = 1
	}

	entries := make([]ZoneMapEntry, numEntries)
	entrySize := 64

	for i := 0; i < numEntries; i++ {
		offset := col.ZoneMapOffset + uint64(i*entrySize)
		entryBytes := make([]byte, entrySize)
		_, err = r.file.ReadAt(entryBytes, int64(offset))
		if err != nil {
			return nil, err
		}

		entry, err := parseZoneMapEntry(entryBytes)
		if err != nil {
			return nil, err
		}
		entries[i] = entry
	}

	return entries, nil
}

// parseZoneMapEntry parses a zone map entry from bytes.
func parseZoneMapEntry(data []byte) (ZoneMapEntry, error) {
	if len(data) < 64 {
		return ZoneMapEntry{}, fmt.Errorf("zone map entry too short: %d bytes", len(data))
	}

	entry := ZoneMapEntry{}

	entry.BlockOffset = binary.LittleEndian.Uint64(data[0:8])
	entry.BlockSize = int64(binary.LittleEndian.Uint64(data[8:16]))
	copy(entry.MinVal[:], data[16:32])
	copy(entry.MaxVal[:], data[32:48])
	entry.NullCount = binary.LittleEndian.Uint64(data[48:56])
	entry.RowCount = binary.LittleEndian.Uint64(data[56:64])

	return entry, nil
}

// GetBloomFilter returns the bloom filter for a column.
func (r *ArkReader) GetBloomFilter(colID uint32) (*BlockedCuckooFilter, error) {
	col, err := r.getColumnByColID(colID)
	if err != nil {
		return nil, err
	}

	if col.BloomOffset == 0 || col.BloomSize == 0 {
		return nil, fmt.Errorf("no bloom filter for column %d", colID)
	}

	// Read bloom filter data
	data := make([]byte, col.BloomSize)
	_, err = r.file.ReadAt(data, int64(col.BloomOffset))
	if err != nil {
		return nil, err
	}

	// Deserialize bloom filter
	bf := DeserializeBlockedCuckooFilter(data)
	return bf, nil
}

// GetPGMIndex returns the PGM index for a column.
func (r *ArkReader) GetPGMIndex(colID uint32) (*PGMIndex, error) {
	col, err := r.getColumnByColID(colID)
	if err != nil {
		return nil, err
	}

	if col.PGMOffset == 0 || col.PGMSize == 0 {
		return nil, fmt.Errorf("no PGM index for column %d", colID)
	}

	// Read PGM index data
	data := make([]byte, col.PGMSize)
	_, err = r.file.ReadAt(data, int64(col.PGMOffset))
	if err != nil {
		return nil, err
	}

	// Deserialize PGM index
	pgm := DeserializePGMIndex(data)
	return pgm, nil
}

// Header returns the file header.
func (r *ArkReader) Header() *FileHeader {
	return r.header
}

// Close closes the file.
func (r *ArkReader) Close() error {
	if r.file != nil {
		return r.file.Close()
	}
	return nil
}

// ColumnBatch is a batch of column values.
type ColumnBatch struct {
	Values   []byte
	Nulls    []byte
	RowCount int
	DataType DataType
}
