// Package search provides the 6-layer pruning stack for Arkilian V3 query execution.
package search

import (
	"sort"

	"github.com/RoaringBitmap/roaring"
)

// RoaringBitmapIntersection implements Layer 6: multi-predicate row_id intersection.
// Target: <1ms for million-element intersections.
type RoaringBitmapIntersection struct{}

// NewRoaringBitmapIntersection creates a new roaring bitmap intersection.
func NewRoaringBitmapIntersection() *RoaringBitmapIntersection {
	return &RoaringBitmapIntersection{}
}

// Intersect intersects multiple roaring bitmaps.
// Sorts bitmaps by cardinality (smallest first) for efficiency.
// Uses roaring.And() — SIMD-optimized internally.
// Target: <1ms for million-element intersections.
func (r *RoaringBitmapIntersection) Intersect(bitmaps []*roaring.Bitmap) *roaring.Bitmap {
	if len(bitmaps) == 0 {
		return roaring.New()
	}

	if len(bitmaps) == 1 {
		return bitmaps[0].Clone()
	}

	// Sort bitmaps by cardinality (smallest first) for efficiency
	sort.Slice(bitmaps, func(i, j int) bool {
		return bitmaps[i].GetCardinality() < bitmaps[j].GetCardinality()
	})

	// Intersect using roaring.And() - SIMD-optimized internally
	result := bitmaps[0].Clone()
	for i := 1; i < len(bitmaps); i++ {
		result.And(bitmaps[i])
	}

	return result
}

// Union unions multiple roaring bitmaps.
func (r *RoaringBitmapIntersection) Union(bitmaps []*roaring.Bitmap) *roaring.Bitmap {
	if len(bitmaps) == 0 {
		return roaring.New()
	}

	if len(bitmaps) == 1 {
		return bitmaps[0].Clone()
	}

	// Union using roaring.Or()
	result := bitmaps[0].Clone()
	for i := 1; i < len(bitmaps); i++ {
		result.Or(bitmaps[i])
	}

	return result
}

// IntersectWithMask intersects bitmaps with a mask.
func (r *RoaringBitmapIntersection) IntersectWithMask(bitmaps []*roaring.Bitmap, mask *roaring.Bitmap) *roaring.Bitmap {
	result := r.Intersect(bitmaps)
	result.And(mask)
	return result
}

// GetIntersectionStats returns intersection statistics.
func (r *RoaringBitmapIntersection) GetIntersectionStats() map[string]interface{} {
	// TODO: Implement stats
	return map[string]interface{}{
		"intersection_time_ms": 0,
	}
}
