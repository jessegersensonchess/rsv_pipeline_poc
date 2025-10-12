package x

import "testing"

// TestMax covers the basic ordering, equal inputs, and negative values.
// This is intentionally simple and explicit to serve as a correctness lock.
func TestMax(t *testing.T) {
	t.Parallel()

	type tc struct {
		a, b int
		want int
	}
	cases := []tc{
		{0, 0, 0},    // equal
		{1, 0, 1},    // a>b
		{0, 1, 1},    // b>a
		{-1, -5, -1}, // negatives
		{-3, 2, 2},   // mixed signs
		{2, 2, 2},    // equal non-zero
	}
	for i, c := range cases {
		if got := Max(c.a, c.b); got != c.want {
			t.Fatalf("case %d: Max(%d,%d)=%d want %d", i, c.a, c.b, got, c.want)
		}
	}
}
