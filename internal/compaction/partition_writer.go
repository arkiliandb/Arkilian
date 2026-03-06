// Package compaction provides the hourly compaction engine for Arkilian V3.
package compaction

import (
	"context"
	"fmt"

	"github.com/arkilian/arkilian/internal/format"
	"github.com/arkilian/arkilian/internal/storage"
	"github.com/arkilian/arkilian/internal/wal"
)

// PartitionWriter writes WAL entries to ArkFormat micro-partitions.
type PartitionWriter struct {
	s3client   *storage.S3Storage
	targetSize int64
	compression format.Compression
}

// NewPartitionWriter creates a new partition writer.
func NewPartitionWriter(s3 *storage.S3Storage, targetSize int64, compression string) *PartitionWriter {
	comp := format.CompressionZSTD
	switch compression {
	case "zstd":
		comp = format.CompressionZSTD
	case "lz4":
		comp = format.CompressionLZ4
	case "snappy":
		comp = format.CompressionSNAPPY
	}

	return &PartitionWriter{
		s3client:    s3,
		targetSize:  targetSize,
		compression: comp,
	}
}

// WritePrimaryPartition writes WAL entries to an ArkFormat micro-partition.
// It produces .ark files at tables/{table}/partitions/L0/{partition_seq}.ark
// and registers them in the catalog.
func (w *PartitionWriter) WritePrimaryPartition(ctx context.Context, tableID uint64, entries []*wal.WalEntry) error {
	if len(entries) == 0 {
		return nil
	}

	// Create ArkFormat writer
	arkWriter := format.NewArkWriter(tableID, w.compression, w.targetSize)

	// TODO: Set cluster key column
	// arkWriter.SetClusterKey(0)

	// Append entries to ArkFormat writer
	for _, entry := range entries {
		// TODO: Convert WalEntry to ArkFormat row
		// For now, just use the payload
		row := make([]format.Value, 0)
		for _, val := range entry.Payload {
			row = append(row, format.Value{Data: val})
		}
		if err := arkWriter.AppendRow(row); err != nil {
			return fmt.Errorf("failed to append row: %w", err)
		}
	}

	// Compute indexes
	arkWriter.ComputeZoneMaps()
	arkWriter.ComputeBloomFilters()
	arkWriter.ComputePGMIndex()

	// TODO: Get metadata for catalog registration
	// meta := arkWriter.Metadata()

	// TODO: Upload to S3
	// s3Path := fmt.Sprintf("tables/%d/partitions/L0/%d.ark", tableID, meta.PartitionSeq)
	// if err := w.s3client.Upload(ctx, localPath, s3Path); err != nil {
	// 	return fmt.Errorf("failed to upload to S3: %w", err)
	// }

	// TODO: Register in catalog
	// catalog.RegisterPartition(meta)

	return nil
}

// WriteHotColumnRun writes a hot-column sorted run.
// It produces .ark files at tables/{table}/indexes/{col_name}/L1/{run_seq}.ark
func (w *PartitionWriter) WriteHotColumnRun(ctx context.Context, tableID uint64, colName string, entries []*wal.WalEntry) error {
	if len(entries) == 0 {
		return nil
	}

	// Create ArkFormat writer for single-column index
	arkWriter := format.NewArkWriter(tableID, w.compression, w.targetSize)

	// TODO: Set cluster key to the hot column
	// arkWriter.SetClusterKey(0)

	// Append entries to ArkFormat writer
	for _, entry := range entries {
		// TODO: Convert (value, row_id) pairs to ArkFormat row
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
