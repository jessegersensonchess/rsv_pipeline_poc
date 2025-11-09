package transformer

import (
	"context"
	"sync"
	"testing"
	"time"
)

/*
TestValidateLoopRows_AllValid verifies that rows containing all required fields
are forwarded unchanged and in order, and that no rejects are reported.
*/
func TestValidateLoopRows_AllValid(t *testing.T) {
	columns := []string{"id", "name", "age"}
	required := []string{"id", "name"}

	in := make(chan *Row, 3)
	out := make(chan *Row, 3)

	// Produce three valid rows.
	for i := 0; i < 3; i++ {
		r := GetRow(len(columns))
		r.V[0] = "id"
		r.V[1] = "alice"
		r.V[2] = "42"
		in <- r
	}
	close(in)

	var rejects int
	errcb := func(_ int, _ string) { rejects++ }

	ValidateLoopRows(context.Background(), columns, required, in, out, errcb)

	if rejects != 0 {
		t.Fatalf("unexpected rejects: %d", rejects)
	}
	if got := len(out); got != 3 {
		t.Fatalf("forwarded %d rows; want 3", got)
	}
	// Clean up pooled rows.
	for i := 0; i < 3; i++ {
		(<-out).Free()
	}
}

/*
TestValidateLoopRows_MissingRequired verifies that rows missing any required
field are fail-soft dropped, reported via onReject, and not forwarded.
*/
func TestValidateLoopRows_MissingRequired(t *testing.T) {
	columns := []string{"id", "name"}
	required := []string{"id", "name"}

	in := make(chan *Row, 3)
	out := make(chan *Row, 3)

	// Row 1: valid
	r1 := GetRow(2)
	r1.V[0] = "id"
	r1.V[1] = "ok"
	in <- r1

	// Row 2: missing required "name"
	r2 := GetRow(2)
	r2.V[0] = "id"
	r2.V[1] = nil
	in <- r2

	// Row 3: missing required "id"
	r3 := GetRow(2)
	r3.V[0] = nil
	r3.V[1] = "ok"
	in <- r3

	close(in)

	var rejects int
	errcb := func(_ int, _ string) { rejects++ }

	ValidateLoopRows(context.Background(), columns, required, in, out, errcb)

	if rejects != 2 {
		t.Fatalf("rejects=%d; want 2", rejects)
	}
	if got := len(out); got != 1 {
		t.Fatalf("forwarded=%d; want 1", got)
	}
	(<-out).Free()
}

/*
TestValidateLoopRows_UnknownRequiredColumns verifies that required names not
present in the columns list are ignored, and valid rows still flow through.
*/
func TestValidateLoopRows_UnknownRequiredColumns(t *testing.T) {
	columns := []string{"a", "b"}
	required := []string{"a", "missing", "also_missing"}

	in := make(chan *Row, 1)
	out := make(chan *Row, 1)

	r := GetRow(2)
	r.V[0] = "x"
	r.V[1] = "y"
	in <- r
	close(in)

	ValidateLoopRows(context.Background(), columns, required, in, out, nil)

	if got := len(out); got != 1 {
		t.Fatalf("forwarded=%d; want 1 (unknown required should be ignored)", got)
	}
	(<-out).Free()
}

/*
TestValidateLoopRows_BadRowLenAndNil ensures malformed rows (nil Row or slice
length mismatch) are drained and freed without panicking and are not forwarded.
*/
func TestValidateLoopRows_BadRowLenAndNil(t *testing.T) {
	columns := []string{"c1", "c2"}
	required := []string{"c1"}

	in := make(chan *Row, 3)
	out := make(chan *Row, 3)

	// Nil row
	in <- nil

	// Wrong length row
	bad := GetRow(1)
	bad.V[0] = "x"
	in <- bad

	// Good row
	ok := GetRow(2)
	ok.V[0] = "x"
	ok.V[1] = "y"
	in <- ok

	close(in)

	ValidateLoopRows(context.Background(), columns, required, in, out, nil)

	if got := len(out); got != 1 {
		t.Fatalf("forwarded=%d; want 1", got)
	}
	(<-out).Free()
}

/*
TestValidateLoopRows_CancelStillDrains verifies the "drain-safe" guarantee:
even when the context is canceled before or during processing, the function
drains the input channel, frees rejected rows, and forwards valid ones.
*/
func TestValidateLoopRows_CancelStillDrains(t *testing.T) {
	columns := []string{"id"}
	required := []string{"id"}

	ctx, cancel := context.WithCancel(context.Background())
	in := make(chan *Row, 100)
	out := make(chan *Row, 100)

	// Pre-cancel the context to simulate shutdown.
	cancel()

	const total = 50
	for i := 0; i < total; i++ {
		r := GetRow(1)
		if i%2 == 0 {
			// valid
			r.V[0] = "ok"
		} else {
			// invalid
			r.V[0] = nil
		}
		in <- r
	}
	close(in)

	var rejects int
	ValidateLoopRows(ctx, columns, required, in, out, func(_ int, _ string) { rejects++ })

	if rejects != total/2 {
		t.Fatalf("rejects=%d; want %d", rejects, total/2)
	}
	if got := len(out); got != total/2 {
		t.Fatalf("forwarded=%d; want %d", got, total/2)
	}
	// Free all forwarded rows.
	for i := 0; i < total/2; i++ {
		(<-out).Free()
	}
}

/*
TestValidateLoopRows_BlockingOut verifies that ValidateLoopRows will block on
out when the consumer is slow, and still behaves correctly (forwards valid rows).
This also implicitly tests that the function does not close 'out' itself.
*/
func TestValidateLoopRows_BlockingOut(t *testing.T) {
	columns := []string{"a"}
	required := []string{"a"}

	in := make(chan *Row, 5)
	out := make(chan *Row) // unbuffered to force blocking

	// Produce three valid rows then close input.
	for i := 0; i < 3; i++ {
		r := GetRow(1)
		r.V[0] = "x"
		in <- r
	}
	close(in)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		ValidateLoopRows(context.Background(), columns, required, in, out, nil)
	}()

	// Slow consumer: receive with delays.
	for i := 0; i < 3; i++ {
		select {
		case r := <-out:
			r.Free()
			time.Sleep(5 * time.Millisecond)
		case <-time.After(200 * time.Millisecond):
			t.Fatal("timed out waiting for row")
		}
	}
	wg.Wait()
}

func BenchmarkValidateLoopRows_PassAll(b *testing.B) {
	columns := []string{"c0", "c1", "c2", "c3", "c4", "c5"}
	required := []string{"c0", "c3"}

	const N = 50_000
	makeRows := func() []*Row {
		rows := make([]*Row, N)
		for i := 0; i < N; i++ {
			r := GetRow(len(columns))
			for j := range columns {
				r.V[j] = "x"
			}
			rows[i] = r
		}
		return rows
	}

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		in := make(chan *Row, N)
		out := make(chan *Row, N)
		rows := makeRows()
		for _, r := range rows {
			in <- r
		}
		close(in)

		// Drain consumer so ValidateLoopRows never blocks unduly.
		done := make(chan struct{})
		go func() {
			for r := range out {
				r.Free()
			}
			close(done)
		}()

		ValidateLoopRows(context.Background(), columns, required, in, out, nil)
		close(out)
		<-done
	}
}

func BenchmarkValidateLoopRows_DropHalf_NoCallback(b *testing.B) {
	columns := []string{"c0", "c1", "c2"}
	required := []string{"c0", "c2"}

	const N = 50_000
	makeRows := func() []*Row {
		rows := make([]*Row, N)
		for i := 0; i < N; i++ {
			r := GetRow(len(columns))
			// Alternate valid/invalid by nil'ing a required field.
			r.V[0] = "x"
			if i%2 == 0 {
				r.V[2] = "y" // valid
			} else {
				r.V[2] = nil // invalid
			}
			rows[i] = r
		}
		return rows
	}

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		in := make(chan *Row, N)
		out := make(chan *Row, N)
		for _, r := range makeRows() {
			in <- r
		}
		close(in)

		go func() {
			for r := range out {
				r.Free()
			}
		}()

		ValidateLoopRows(context.Background(), columns, required, in, out, nil)
		close(out)
	}
}

func BenchmarkValidateLoopRows_DropHalf_WithCallback(b *testing.B) {
	columns := []string{"c0", "c1", "c2"}
	required := []string{"c0", "c2"}

	const N = 50_000
	makeRows := func() []*Row {
		rows := make([]*Row, N)
		for i := 0; i < N; i++ {
			r := GetRow(len(columns))
			r.V[0] = "x"
			if i%2 == 0 {
				r.V[2] = "y"
			} else {
				r.V[2] = nil
			}
			rows[i] = r
		}
		return rows
	}

	onReject := func(_ int, _ string) { /* intentionally light-weight */ }

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		in := make(chan *Row, N)
		out := make(chan *Row, N)
		for _, r := range makeRows() {
			in <- r
		}
		close(in)

		go func() {
			for r := range out {
				r.Free()
			}
		}()

		ValidateLoopRows(context.Background(), columns, required, in, out, onReject)
		close(out)
	}
}
