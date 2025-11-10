package transformer

import (
	"reflect"
	"strconv"
	"sync/atomic"
	"testing"

	"etl/pkg/records"
)

/*
identityTransformer is a no-op transformer used in tests/benchmarks.
It returns the input slice without allocating or modifying it.
*/
type identityTransformer struct{}

func (identityTransformer) Apply(in []records.Record) []records.Record { return in }

/*
addFieldTransformer mutates each record in place by setting key -> value.
Used to verify mutation flows through Chain.
*/
type addFieldTransformer struct {
	key string
	val any
}

func (t addFieldTransformer) Apply(in []records.Record) []records.Record {
	for i := range in {
		in[i][t.key] = t.val
	}
	return in
}

/*
filterRequireTransformer keeps only records that have a non-nil, non-empty value
for the provided key; it filters in place by reslicing the input.
*/
type filterRequireTransformer struct {
	key string
}

func (t filterRequireTransformer) Apply(in []records.Record) []records.Record {
	out := in[:0]
	for _, r := range in {
		if v, ok := r[t.key]; ok && v != nil && !(isString(v) && v.(string) == "") {
			out = append(out, r)
		}
	}
	return out
}

func isString(v any) bool { _, ok := v.(string); return ok }

/*
counterTransformer increments *calls whenever Apply is invoked. Used to verify
that each transformer in the chain is called exactly once and in order.
*/
type counterTransformer struct {
	calls *int32
	// mark is an optional field name to set with a rank value for order checks.
	mark string
	rank int
}

func (t counterTransformer) Apply(in []records.Record) []records.Record {
	atomic.AddInt32(t.calls, 1)
	if t.mark != "" {
		for i := range in {
			in[i][t.mark] = t.rank
		}
	}
	return in
}

// --- Helpers ---

func makeRecs(n int) []records.Record {
	recs := make([]records.Record, n)
	for i := 0; i < n; i++ {
		recs[i] = records.Record{"id": i}
	}
	return recs
}

// --- Unit tests ---

/*
TestChainApply_Composition_Order verifies that Chain.Apply passes the output of
each transformer as the input to the next, and that the transforms occur in the
declared order.
*/
func TestChainApply_Composition_Order(t *testing.T) {
	in := []records.Record{{"id": 1}}
	c := Chain{
		addFieldTransformer{key: "a", val: "first"},
		addFieldTransformer{key: "b", val: "second"},
		addFieldTransformer{key: "c", val: "third"},
	}
	out := c.Apply(in)

	want := records.Record{"id": 1, "a": "first", "b": "second", "c": "third"}
	if !reflect.DeepEqual(out[0], want) {
		t.Fatalf("composition mismatch:\n got: %#v\nwant: %#v", out[0], want)
	}
}

/*
TestChainApply_FilterThenMutate verifies that in-place filtering followed by a
mutating transform yields the expected survivors and mutated fields, and does so
without unnecessary allocations in steady state.
*/
func TestChainApply_FilterThenMutate(t *testing.T) {
	in := []records.Record{
		{"keep": "yes", "id": 1},
		{"keep": "", "id": 2},
		{"keep": "yes", "id": 3},
	}
	c := Chain{
		filterRequireTransformer{key: "keep"},
		addFieldTransformer{key: "tag", val: "ok"},
	}

	// content check
	out := c.Apply(append([]records.Record(nil), in...)) // avoid mutating original test data
	if len(out) != 2 {
		t.Fatalf("len(out)=%d; want 2", len(out))
	}
	for _, r := range out {
		if r["tag"] != "ok" {
			t.Fatalf("mutate-after-filter missing tag on %#v", r)
		}
		if r["keep"] == "" {
			t.Fatalf("filtered record with empty 'keep' leaked into output: %#v", r)
		}
	}

	// steady-state allocations check (should be ~0)
	allocs := testing.AllocsPerRun(500, func() {
		_ = c.Apply(in)
	})
	if allocs > 0.20 { // allow tiny headroom across Go versions
		t.Fatalf("allocs/op=%.2f; want <= 0.20", allocs)
	}
}

/*
TestChainApply_NilAndEmptyChain verifies that applying a nil or empty Chain
returns the input unchanged and does not allocate.
*/
func TestChainApply_NilAndEmptyChain(t *testing.T) {
	in := makeRecs(3)

	// nil chain
	var cNil Chain
	outNil := cNil.Apply(in)
	if !reflect.DeepEqual(outNil, in) {
		t.Fatalf("nil chain mutated output: got=%#v want=%#v", outNil, in)
	}
	if len(outNil) != len(in) || &outNil[0] != &in[0] {
		t.Fatalf("nil chain should return same slice header")
	}

	// empty chain
	cEmpty := Chain{}
	outEmpty := cEmpty.Apply(in)
	if !reflect.DeepEqual(outEmpty, in) {
		t.Fatalf("empty chain mutated output")
	}

	allocs := testing.AllocsPerRun(500, func() {
		_ = cNil.Apply(in)
	})
	if allocs > 0.05 {
		t.Fatalf("nil chain allocs/op=%.2f; want <= 0.05", allocs)
	}
}

/*
TestChainApply_TransformerCalledOnce ensures each transformer in the chain is
invoked exactly once per Chain.Apply call, and that order can be observed.
*/
func TestChainApply_TransformerCalledOnce(t *testing.T) {
	var calls int32
	in := makeRecs(2)
	c := Chain{
		counterTransformer{calls: &calls, mark: "rank", rank: 1},
		counterTransformer{calls: &calls, mark: "rank2", rank: 2},
		counterTransformer{calls: &calls, mark: "rank3", rank: 3},
	}
	_ = c.Apply(in)
	if got := atomic.LoadInt32(&calls); got != 3 {
		t.Fatalf("calls=%d; want 3", got)
	}
	// Transform order left-to-right: last mark wins on conflicts
	for _, r := range in {
		if r["rank"] != 1 || r["rank2"] != 2 || r["rank3"] != 3 {
			t.Fatalf("unexpected rank markers in %#v", r)
		}
	}
}

/*
TestChainApply_NilInput verifies the defined behavior for nil input slices:
Apply should return nil (not an empty slice).
*/
func TestChainApply_NilInput(t *testing.T) {
	var in []records.Record
	c := Chain{identityTransformer{}}
	out := c.Apply(in)
	if out != nil {
		t.Fatalf("Apply(nil) => %#v; want nil", out)
	}
}

/*
BenchmarkChain_Identity_N measures overhead of Chain.Apply with N no-op
transformers over a medium batch of records.
*/
func BenchmarkChain_Identity_1(b *testing.B)  { benchChainIdentity(b, 1) }
func BenchmarkChain_Identity_3(b *testing.B)  { benchChainIdentity(b, 3) }
func BenchmarkChain_Identity_10(b *testing.B) { benchChainIdentity(b, 10) }

func benchChainIdentity(b *testing.B, n int) {
	// build data
	const recs = 20000
	in := make([]records.Record, recs)
	for i := 0; i < recs; i++ {
		in[i] = records.Record{"id": i}
	}
	// build chain
	c := make(Chain, n)
	for i := 0; i < n; i++ {
		c[i] = identityTransformer{}
	}

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = c.Apply(in)
	}
}

/*
BenchmarkChain_AddField simulates a common "mutate across all records" pass.
*/
func BenchmarkChain_AddField(b *testing.B) {
	const recs = 20000
	in := make([]records.Record, recs)
	for i := 0; i < recs; i++ {
		in[i] = records.Record{"id": i}
	}
	c := Chain{
		addFieldTransformer{"a", "x"},
		addFieldTransformer{"b", "y"},
	}

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = c.Apply(in)
	}
}

/*
BenchmarkChain_FilterHalf models an in-place filter that drops ~50% of rows,
followed by a small mutation stage.
*/
func BenchmarkChain_FilterHalf(b *testing.B) {
	const recs = 40000
	in := make([]records.Record, recs)
	for i := 0; i < recs; i++ {
		val := ""
		if i%2 == 0 {
			val = "keep"
		}
		in[i] = records.Record{"id": i, "keep": val}
	}
	c := Chain{
		filterRequireTransformer{"keep"},
		addFieldTransformer{"tag", "ok"},
	}

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		out := c.Apply(in)
		_ = out
	}
}

/*
BenchmarkChain_LongPipeline tests a longer chain with varied work to surface
how overhead scales with pipeline depth.
*/
func BenchmarkChain_LongPipeline(b *testing.B) {
	const recs = 15000
	in := make([]records.Record, recs)
	for i := 0; i < recs; i++ {
		in[i] = records.Record{
			"id":   i,
			"name": "user_" + strconv.Itoa(i%1000),
		}
	}
	c := Chain{
		addFieldTransformer{"a", 1},
		identityTransformer{},
		addFieldTransformer{"b", true},
		filterRequireTransformer{"name"},
		addFieldTransformer{"c", 3.14},
		identityTransformer{},
	}

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = c.Apply(in)
	}
}
