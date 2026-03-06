// Package compaction provides the hourly compaction engine for Arkilian V3.
package compaction

import (
	"context"
	"fmt"

	"github.com/arkilian/arkilian/internal/format"
	"github.com/arkilian/arkilian/internal/storage"
	"github.com/arkilian/arkilian/internal/wal"
)

// SortedRunWriter writes hot-column sorted runs.
type SortedRunWriter struct {
	s3client   *storage.S3Storage
	targetSize int64
	compression format.Compression
}

// NewSortedRunWriter creates a new sorted run writer.
func NewSortedRunWriter(s3 *storage.S3Storage, targetSize int64, compression string) *SortedRunWriter {
	comp := format.CompressionZSTD
	switch compression {
	case "zstd":
		comp = format.CompressionZSTD
	case "lz4":
		comp = format.CompressionLZ4
	case "snappy":
		comp = format.CompressionSNAPPY
	}

	return &SortedRunWriter{
		s3client:    s3,
		targetSize:  targetSize,
		compression: comp,
	}
}

// WriteSortedRun writes a hot-column sorted run.
// It extracts (value, row_id) pairs for the column, sorts by (value, row_id),
// and writes into single-column ArkFormat files with PGM index.
func (w *SortedRunWriter) WriteSortedRun(ctx context.Context, tableID uint64, colName string, entries []*wal.WalEntry) error {
	if len(entries) == 0 {
		return nil
	}

	// Create ArkFormat writer for single-column index
	arkWriter := format.NewArkWriter(tableID, w.compression, w.targetSize)

	// TODO: Set cluster key to the hot column
	// arkWriter.SetClusterKey(0)

	// TODO: Extract (value, row_id) pairs and sort
	// For now, just use the payload as value
	for _, entry := range entries {
		row := make([]format.Value, 0)
		row = append(row, format.Value{Data: entry.Payload})
		if err := arkWriter.AppendRow(row); err != nil {
			return fmt.Errorf("failed to append row: %w", err)
		}
	}

	// Compute PGM index for sorted column
	arkWriter.ComputePGMIndex()

	// TODO: Upload to S3
	// s3Path := fmt.Sprintf("tables/%d/indexes/%s/L0/%d.ark", tableID, colName, time.Now().UnixNano())
	// if err := w.s3client.Upload(ctx, localPath, s3Path); err != nil {
	// 	return fmt.Errorf("failed to upload to S3: %w", err)
	// }

	return nil
}

// WriteMergedSortedRun writes a merged hot-column sorted run at L1.
// L1 files have non-overlapping value ranges.
func (w *SortedRunWriter) WriteMergedSortedRun(ctx context.Context, tableID uint64, colName string, entries []*wal.WalEntry) error {
	if len(entries) == 0 {
		return nil
	}

	// Create ArkFormat writer for single-column index
	arkWriter := format.NewArkWriter(tableID, w.compression, w.targetSize)

	// TODO: Set cluster key to the hot column
	// arkWriter.SetClusterKey(0)

	// TODO: Sort entries by value before writing
	for _, entry := range entries {
		row := make([]format.Value, 0)
		row = append(row, format.Value{Data: entry.Payload})
		if err := arkWriter.AppendRow(row); err != nil {
			return fmt.Errorf("failed to append row: %w", err)
		}
	}

	// Compute PGM index for sorted column
	arkWriter.ComputePGMIndex()

	// TODO: Upload to S3
	// s3Path := fmt.Sprintf("tables/%d/indexes/%s/L1/%d.ark", tableID, colName, time.Now().UnixNano())
	// if err := w.s3client.Upload(ctx, localPath, s3Path); err != nil {
	// 	return fmt.Errorf("failed to upload to S3: %w", err)
	// }

	return nil
}
