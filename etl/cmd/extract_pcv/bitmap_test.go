// bitmap_test.go contains unit tests for the Bitmap type used by the CSV
// processing pipeline. The Bitmap is a simple bitset backed by a slice of
// uint64 values and is used for fast membership checks on integer IDs.
//
// These tests focus on correctness at boundaries, including behavior for
// in-range and out-of-range IDs, and ensure that the Add and Has methods
// remain safe and deterministic under normal usage.
package main

import "testing"

/*
TestBitmapRoundTrip verifies that an ID added to the Bitmap can be
subsequently observed via Has.

The test exercises a range of IDs that cross 64-bit word boundaries to
ensure that bit indexing logic is correct across all words in the underlying
slice.
*/
func TestBitmapRoundTrip(t *testing.T) {
	t.Parallel()

	bm := NewBitmap(200)

	ids := []int{
		0,   // first bit in first word
		1,   // second bit
		63,  // last bit in first word
		64,  // first bit in second word
		65,  // second bit in second word
		127, // last bit in second word
		128, // first bit in third word
		199, // arbitrary in-range value
	}

	for _, id := range ids {
		if bm.Has(id) {
			t.Fatalf("Has(%d) = true before Add; expected false", id)
		}
		bm.Add(id)
		if !bm.Has(id) {
			t.Fatalf("Has(%d) = false after Add; expected true", id)
		}
	}
}

/*
TestBitmapOutOfRange verifies that calling Add and Has with out-of-range IDs
does not cause panics and does not accidentally flip bits for valid IDs.

The Bitmap is allocated with a small maximum ID, and large IDs are tested
to ensure that bounds checks are correctly enforced.
*/
func TestBitmapOutOfRange(t *testing.T) {
	t.Parallel()

	const maxID = 100
	bm := NewBitmap(maxID)

	// Capture the state of a known in-range ID before manipulating
	// out-of-range values.
	const inRangeID = 10
	if bm.Has(inRangeID) {
		t.Fatalf("Has(%d) = true before any Add; expected false", inRangeID)
	}

	// Add an ID far beyond the configured maximum. This must not panic, and
	// must not change the membership state for valid IDs.
	const outOfRangeID = 10_000
	bm.Add(outOfRangeID)

	if bm.Has(outOfRangeID) {
		t.Fatalf("Has(%d) = true for out-of-range ID; expected false", outOfRangeID)
	}
	if bm.Has(inRangeID) {
		t.Fatalf("Has(%d) = true after out-of-range Add; expected false", inRangeID)
	}
}
