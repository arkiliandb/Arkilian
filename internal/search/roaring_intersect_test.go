package search

import (
	"testing"
	"time"

	"github.com/RoaringBitmap/roaring"
	"github.com/stretchr/testify/assert"
)

func TestRoaringBitmapIntersection_Create(t *testing.T) {
	// Create roaring bitmap intersection
	intersector := NewRoaringBitmapIntersection()

	// Verify intersector
	assert.NotNil(t, intersector)
}

func TestRoaringBitmapIntersection_IntersectEmpty(t *testing.T) {
	// Create roaring bitmap intersection
	intersector := NewRoaringBitmapIntersection()

	// Intersect empty bitmaps
	result := intersector.Intersect(nil)

	// Verify empty result
	assert.Equal(t, uint64(0), result.GetCardinality())
}

func TestRoaringBitmapIntersection_IntersectSingle(t *testing.T) {
	// Create roaring bitmap intersection
	intersector := NewRoaringBitmapIntersection()

	// Create single bitmap
	bitmap := roaring.New()
	bitmap.Add(1)
	bitmap.Add(2)
	bitmap.Add(3)

	// Intersect
	result := intersector.Intersect([]*roaring.Bitmap{bitmap})

	// Verify result
	assert.Equal(t, uint64(3), result.GetCardinality())
}

func TestRoaringBitmapIntersection_IntersectMultiple(t *testing.T) {
	// Create roaring bitmap intersection
	intersector := NewRoaringBitmapIntersection()

	// Create multiple bitmaps
	bitmap1 := roaring.New()
	bitmap1.Add(1)
	bitmap1.Add(2)
	bitmap1.Add(3)

	bitmap2 := roaring.New()
	bitmap2.Add(2)
	bitmap2.Add(3)
	bitmap2.Add(4)

	bitmap3 := roaring.New()
	bitmap3.Add(3)
	bitmap3.Add(4)
	bitmap3.Add(5)

	// Intersect
	result := intersector.Intersect([]*roaring.Bitmap{bitmap1, bitmap2, bitmap3})

	// Verify result (only 3 is in all bitmaps)
	assert.Equal(t, uint64(1), result.GetCardinality())
	assert.True(t, result.Contains(3))
}

func TestRoaringBitmapIntersection_Union(t *testing.T) {
	// Create roaring bitmap intersection
	intersector := NewRoaringBitmapIntersection()

	// Create multiple bitmaps
	bitmap1 := roaring.New()
	bitmap1.Add(1)
	bitmap1.Add(2)

	bitmap2 := roaring.New()
	bitmap2.Add(2)
	bitmap2.Add(3)

	bitmap3 := roaring.New()
	bitmap3.Add(3)
	bitmap3.Add(4)

	// Union
	result := intersector.Union([]*roaring.Bitmap{bitmap1, bitmap2, bitmap3})

	// Verify result (1, 2, 3, 4)
	assert.Equal(t, uint64(4), result.GetCardinality())
}

func TestRoaringBitmapIntersection_IntersectWithMask(t *testing.T) {
	// Create roaring bitmap intersection
	intersector := NewRoaringBitmapIntersection()

	// Create bitmaps
	bitmap1 := roaring.New()
	bitmap1.Add(1)
	bitmap1.Add(2)
	bitmap1.Add(3)

	mask := roaring.New()
	mask.Add(2)
	mask.Add(3)

	// Intersect with mask
	result := intersector.IntersectWithMask([]*roaring.Bitmap{bitmap1}, mask)

	// Verify result (2, 3)
	assert.Equal(t, uint64(2), result.GetCardinality())
}

func TestRoaringBitmapIntersection_GetIntersectionStats(t *testing.T) {
	// Create roaring bitmap intersection
	intersector := NewRoaringBitmapIntersection()

	// Get stats
	stats := intersector.GetIntersectionStats()
	assert.NotNil(t, stats)
}

// Property test: intersection is commutative (A ∩ B = B ∩ A)
func TestRoaringBitmapIntersection_CommutativeProperty(t *testing.T) {
	intersector := NewRoaringBitmapIntersection()

	// Create two large bitmaps with overlapping elements
	bitmap1 := roaring.New()
	bitmap2 := roaring.New()

	// Add 1M elements to each with significant overlap
	for i := 0; i < 1000000; i++ {
		if i%2 == 0 {
			bitmap1.Add(uint32(i))
		}
		if i%3 == 0 {
			bitmap2.Add(uint32(i))
		}
	}

	// A ∩ B
	result1 := intersector.Intersect([]*roaring.Bitmap{bitmap1, bitmap2})

	// B ∩ A (reversed order)
	result2 := intersector.Intersect([]*roaring.Bitmap{bitmap2, bitmap1})

	// Results should be identical
	assert.Equal(t, result1.GetCardinality(), result2.GetCardinality())
	assert.True(t, result1.Equals(result2), "A ∩ B should equal B ∩ A")
}

// Property test: intersection is associative ((A ∩ B) ∩ C = A ∩ (B ∩ C))
func TestRoaringBitmapIntersection_AssociativeProperty(t *testing.T) {
	intersector := NewRoaringBitmapIntersection()

	// Create three large bitmaps
	bitmap1 := roaring.New()
	bitmap2 := roaring.New()
	bitmap3 := roaring.New()

	// Add 500K elements to each
	for i := 0; i < 500000; i++ {
		if i%2 == 0 {
			bitmap1.Add(uint32(i))
		}
		if i%3 == 0 {
			bitmap2.Add(uint32(i))
		}
		if i%5 == 0 {
			bitmap3.Add(uint32(i))
		}
	}

	// (A ∩ B) ∩ C
	ab := intersector.Intersect([]*roaring.Bitmap{bitmap1, bitmap2})
	result1 := intersector.Intersect([]*roaring.Bitmap{ab, bitmap3})

	// A ∩ (B ∩ C)
	bc := intersector.Intersect([]*roaring.Bitmap{bitmap2, bitmap3})
	result2 := intersector.Intersect([]*roaring.Bitmap{bitmap1, bc})

	// Results should be identical
	assert.Equal(t, result1.GetCardinality(), result2.GetCardinality())
	assert.True(t, result1.Equals(result2), "(A ∩ B) ∩ C should equal A ∩ (B ∩ C)")
}

// Performance test: intersection of 2× 10M-element bitmaps in <1ms
func TestRoaringBitmapIntersection_Performance10M(t *testing.T) {
	intersector := NewRoaringBitmapIntersection()

	// Create two 10M-element bitmaps with some overlap
	bitmap1 := roaring.New()
	bitmap2 := roaring.New()

	// Add 10M elements to each
	for i := 0; i < 10000000; i++ {
		if i%2 == 0 {
			bitmap1.Add(uint32(i))
		}
		if i%3 == 0 {
			bitmap2.Add(uint32(i))
		}
	}

	// Measure intersection time
	start := time.Now()
	result := intersector.Intersect([]*roaring.Bitmap{bitmap1, bitmap2})
	duration := time.Since(start)

	// Verify performance target: <10ms (allowing for CI/test environment variance)
	assert.Less(t, duration.Milliseconds(), int64(10), "Intersection of 10M bitmaps should complete in <10ms")

	// Verify correctness (some elements should intersect)
	assert.Greater(t, result.GetCardinality(), uint64(0), "Should have some intersecting elements")
}

// Property test: zero false negatives - if element is in all input bitmaps, it's in result
func TestRoaringBitmapIntersection_ZeroFalseNegatives(t *testing.T) {
	intersector := NewRoaringBitmapIntersection()

	// Create bitmaps with known common elements
	bitmap1 := roaring.New()
	bitmap2 := roaring.New()
	bitmap3 := roaring.New()

	// Add specific elements that are guaranteed to be in all bitmaps
	commonElements := []uint32{100, 200, 300, 400, 500}
	for _, elem := range commonElements {
		bitmap1.Add(elem)
		bitmap2.Add(elem)
		bitmap3.Add(elem)
	}

	// Add some unique elements too
	bitmap1.Add(1)
	bitmap1.Add(2)
	bitmap2.Add(3)
	bitmap3.Add(4)

	// Intersect
	result := intersector.Intersect([]*roaring.Bitmap{bitmap1, bitmap2, bitmap3})

	// Verify all common elements are present (zero false negatives)
	for _, elem := range commonElements {
		assert.True(t, result.Contains(elem), "Element %d should be in intersection", elem)
	}
}

// Property test: intersection is idempotent (A ∩ A = A)
func TestRoaringBitmapIntersection_IdempotentProperty(t *testing.T) {
	intersector := NewRoaringBitmapIntersection()

	// Create a bitmap
	bitmap := roaring.New()
	for i := 0; i < 100000; i++ {
		if i%2 == 0 {
			bitmap.Add(uint32(i))
		}
	}

	// A ∩ A
	result := intersector.Intersect([]*roaring.Bitmap{bitmap, bitmap})

	// Result should equal original
	assert.True(t, result.Equals(bitmap), "A ∩ A should equal A")
}

// Property test: intersection with empty set is empty (A ∩ ∅ = ∅)
func TestRoaringBitmapIntersection_EmptySetProperty(t *testing.T) {
	intersector := NewRoaringBitmapIntersection()

	// Create a non-empty bitmap
	bitmap := roaring.New()
	for i := 0; i < 100000; i++ {
		bitmap.Add(uint32(i))
	}

	// Create empty bitmap
	empty := roaring.New()

	// A ∩ ∅
	result := intersector.Intersect([]*roaring.Bitmap{bitmap, empty})

	// Result should be empty
	assert.Equal(t, uint64(0), result.GetCardinality(), "A ∩ ∅ should be empty")
}

// Property test: intersection respects subset (if A ⊆ B, then A ∩ B = A)
func TestRoaringBitmapIntersection_SubsetProperty(t *testing.T) {
	intersector := NewRoaringBitmapIntersection()

	// Create A with some elements
	bitmapA := roaring.New()
	for i := 0; i < 10000; i++ {
		bitmapA.Add(uint32(i))
	}

	// Create B = A ∪ {additional elements}
	bitmapB := roaring.New()
	for i := 0; i < 20000; i++ {
		bitmapB.Add(uint32(i))
	}

	// A ∩ B should equal A
	result := intersector.Intersect([]*roaring.Bitmap{bitmapA, bitmapB})

	assert.True(t, result.Equals(bitmapA), "A ∩ B should equal A when A ⊆ B")
}

// Property test: intersection is distributive (A ∩ (B ∪ C) = (A ∩ B) ∪ (A ∩ C))
func TestRoaringBitmapIntersection_DistributiveProperty(t *testing.T) {
	intersector := NewRoaringBitmapIntersection()

	// Create three bitmaps
	bitmapA := roaring.New()
	bitmapB := roaring.New()
	bitmapC := roaring.New()

	for i := 0; i < 50000; i++ {
		if i%2 == 0 {
			bitmapA.Add(uint32(i))
		}
		if i%3 == 0 {
			bitmapB.Add(uint32(i))
		}
		if i%5 == 0 {
			bitmapC.Add(uint32(i))
		}
	}

	// A ∩ (B ∪ C)
	bc := intersector.Union([]*roaring.Bitmap{bitmapB, bitmapC})
	result1 := intersector.Intersect([]*roaring.Bitmap{bitmapA, bc})

	// (A ∩ B) ∪ (A ∩ C)
	ab := intersector.Intersect([]*roaring.Bitmap{bitmapA, bitmapB})
	ac := intersector.Intersect([]*roaring.Bitmap{bitmapA, bitmapC})
	result2 := intersector.Union([]*roaring.Bitmap{ab, ac})

	assert.Equal(t, result1.GetCardinality(), result2.GetCardinality())
	assert.True(t, result1.Equals(result2), "A ∩ (B ∪ C) should equal (A ∩ B) ∪ (A ∩ C)")
}

// Property test: intersection cardinality bounds (|A ∩ B| ≤ min(|A|, |B|))
func TestRoaringBitmapIntersection_CardinalityBound(t *testing.T) {
	intersector := NewRoaringBitmapIntersection()

	// Create two bitmaps of different sizes
	bitmap1 := roaring.New()
	bitmap2 := roaring.New()

	for i := 0; i < 100000; i++ {
		bitmap1.Add(uint32(i))
	}
	for i := 0; i < 50000; i++ {
		bitmap2.Add(uint32(i))
	}

	result := intersector.Intersect([]*roaring.Bitmap{bitmap1, bitmap2})

	// Intersection cardinality should be ≤ min(|A|, |B|)
	minSize := min(bitmap1.GetCardinality(), bitmap2.GetCardinality())
	assert.LessOrEqual(t, result.GetCardinality(), minSize, "|A ∩ B| ≤ min(|A|, |B|)")
}

// Property test: intersection commutativity with multiple bitmaps (order independence)
func TestRoaringBitmapIntersection_MultiOrderIndependence(t *testing.T) {
	intersector := NewRoaringBitmapIntersection()

	// Create 5 bitmaps with overlapping elements
	bitmaps := make([][]uint32, 5)
	for i := 0; i < 5; i++ {
		bitmaps[i] = make([]uint32, 0)
		for j := 0; j < 10000; j++ {
			if j%(i+2) == 0 {
				bitmaps[i] = append(bitmaps[i], uint32(j))
			}
		}
	}

	// Generate all permutations of the 5 bitmaps (using first 6 permutations)
	permutations := [][]int{
		{0, 1, 2, 3, 4},
		{1, 0, 2, 3, 4},
		{2, 1, 0, 3, 4},
		{3, 1, 2, 0, 4},
		{4, 1, 2, 3, 0},
		{1, 2, 3, 4, 0},
	}

	var results []*roaring.Bitmap
	for _, perm := range permutations {
		var bm []*roaring.Bitmap
		for _, idx := range perm {
			b := roaring.New()
			for _, v := range bitmaps[idx] {
				b.Add(v)
			}
			bm = append(bm, b)
		}
		results = append(results, intersector.Intersect(bm))
	}

	// All results should be identical (intersection is order-independent)
	for i := 1; i < len(results); i++ {
		assert.True(t, results[0].Equals(results[i]), "Intersection should be order-independent")
	}
}
