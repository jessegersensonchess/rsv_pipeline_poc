package transformer

import (
	"context"
	"testing"
	"time"
)

// TestTransformLoop_Coercion verifies that strings are coerced into typed
// values according to the CoerceSpec and aligned to the provided columns.
func TestTransformLoop_Coercion(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	columns := []string{
		"pcv", "typ", "platnost_od", "platnost_do", "aktualni",
	}

	spec := BuildCoerceSpecFromTypes(
		map[string]string{
			"pcv":         "int",
			"typ":         "text",
			"platnost_od": "date",
			"platnost_do": "date",
			"aktualni":    "bool",
		},
		"02.01.2006",
		[]string{"1", "true", "yes", "y"},
		[]string{"0", "false", "no", "n"},
	)

	if err := ValidateSpecSanity(columns, spec); err != nil {
		t.Fatalf("spec sanity: %v", err)
	}

	in := make(chan []string, 3)
	out := make(chan []any, 3)

	go TransformLoop(ctx, columns, in, out, spec)

	// Send two rows; one valid, one invalid (bad int)
	in <- []string{"7263067", "E - Evidenční", "07.10.2011", "07.10.2011", "False"}
	in <- []string{"ahoj", "E - Evidenční", "07.10.2011", "07.10.2011", "True"}
	close(in)

	// Receive only the valid one.
	got := <-out

	if len(got) != len(columns) {
		t.Fatalf("got width %d, want %d", len(got), len(columns))
	}

	// pcv -> int
	if _, ok := got[0].(int64); !ok {
		t.Errorf("pcv type = %T, want int64", got[0])
	}
	// typ -> text (string or nil)
	if s, ok := got[1].(string); !ok || s == "" {
		t.Errorf("typ got %#v, want non-empty string", got[1])
	}
	// dates -> time.Time
	if _, ok := got[2].(time.Time); !ok {
		t.Errorf("platnost_od type = %T, want time.Time", got[2])
	}
	if _, ok := got[3].(time.Time); !ok {
		t.Errorf("platnost_do type = %T, want time.Time", got[3])
	}
	// aktualni -> bool (False)
	if v, ok := got[4].(bool); !ok || v != false {
		t.Errorf("aktualni = %#v, want false", got[4])
	}

	// Ensure the bad row was dropped (channel should now be empty and closed).
	select {
	case _, ok := <-out:
		if ok {
			t.Errorf("unexpected extra row; invalid row should have been dropped")
		}
	default:
		// closed by TransformLoop when input drains; absence of more rows is fine
	}
}

// TestTransformLoop_ContextCancel ensures loop exits promptly on context cancel.
func TestTransformLoop_ContextCancel(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())

	columns := []string{"pcv"}
	spec := BuildCoerceSpecFromTypes(map[string]string{"pcv": "int"}, "", nil, nil)

	in := make(chan []string)
	out := make(chan []any)

	done := make(chan struct{})
	go func() {
		TransformLoop(ctx, columns, in, out, spec)
		close(done)
	}()

	in <- []string{"123"}
	cancel() // cancel while goroutine may be blocked on channels
	close(in)

	select {
	case <-done:
		// ok
	case <-time.After(2 * time.Second):
		t.Fatal("TransformLoop did not respect context cancellation")
	}
}
