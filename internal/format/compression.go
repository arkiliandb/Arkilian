// Package format provides the ArkFormat binary columnar micro-partition file format for Arkilian V3.
package format

import (
	"fmt"

	"github.com/golang/snappy"
)

// Compress compresses data using the specified compression algorithm.
func Compress(data []byte, compression Compression) ([]byte, error) {
	switch compression {
	case CompressionZSTD:
		return data, nil // Placeholder - ZSTD not available without external dependency
	case CompressionLZ4:
		return data, nil // Placeholder - LZ4 not available without external dependency
	case CompressionSNAPPY:
		return compressSnappy(data)
	case CompressionNONE:
		return data, nil
	default:
		return nil, fmt.Errorf("unsupported compression: %d", compression)
	}
}

// Decompress decompresses data using the specified compression algorithm.
func Decompress(data []byte, compression Compression) ([]byte, error) {
	switch compression {
	case CompressionZSTD:
		return data, nil // Placeholder - ZSTD not available without external dependency
	case CompressionLZ4:
		return data, nil // Placeholder - LZ4 not available without external dependency
	case CompressionSNAPPY:
		return decompressSnappy(data)
	case CompressionNONE:
		return data, nil
	default:
		return nil, fmt.Errorf("unsupported compression: %d", compression)
	}
}

// compressSnappy compresses data using Snappy.
func compressSnappy(data []byte) ([]byte, error) {
	return snappy.Encode(nil, data), nil
}

// decompressSnappy decompresses Snappy-compressed data.
func decompressSnappy(data []byte) ([]byte, error) {
	return snappy.Decode(nil, data)
}
