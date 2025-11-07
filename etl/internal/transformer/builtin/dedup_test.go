package builtin

import (
	"reflect"
	"testing"

	"etl/pkg/records"
)

func mk(pcv int, df string, fields map[string]any) records.Record {
	r := records.Record{
		"pcv":       pcv,
		"date_from": df,
	}
	for k, v := range fields {
		r[k] = v
	}
	return r
}

func TestDeDupKeepFirst(t *testing.T) {
	in := []records.Record{
		mk(1, "2020-01-01", map[string]any{"reason": "A"}),
		mk(1, "2020-01-01", map[string]any{"reason": "B"}),
		mk(2, "2020-01-01", map[string]any{"reason": "C"}),
	}
	d := DeDup{Keys: []string{"pcv", "date_from"}, Policy: "keep-first"}
	got := d.Apply(in)
	want := []records.Record{
		mk(1, "2020-01-01", map[string]any{"reason": "A"}),
		mk(2, "2020-01-01", map[string]any{"reason": "C"}),
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("keep-first: got %#v want %#v", got, want)
	}
}

func TestDeDupKeepLast(t *testing.T) {
	in := []records.Record{
		mk(1, "2020-01-01", map[string]any{"reason": "A"}),
		mk(1, "2020-01-01", map[string]any{"reason": "B"}),
		mk(2, "2020-01-01", map[string]any{"reason": "C"}),
	}
	d := DeDup{Keys: []string{"pcv", "date_from"}, Policy: "keep-last"}
	got := d.Apply(in)
	want := []records.Record{
		mk(1, "2020-01-01", map[string]any{"reason": "B"}),
		mk(2, "2020-01-01", map[string]any{"reason": "C"}),
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("keep-last: got %#v want %#v", got, want)
	}
}

func TestDeDupMostComplete(t *testing.T) {
	in := []records.Record{
		mk(1, "2020-01-01", map[string]any{"reason": ""}),                // 2 non-empty
		mk(1, "2020-01-01", map[string]any{"reason": "B", "rm_code": 1}), // 3 non-empty
		mk(2, "2020-01-01", map[string]any{"reason": "C"}),
	}
	d := DeDup{Keys: []string{"pcv", "date_from"}, Policy: "most-complete"}
	got := d.Apply(in)
	want := []records.Record{
		mk(1, "2020-01-01", map[string]any{"reason": "B", "rm_code": 1}),
		mk(2, "2020-01-01", map[string]any{"reason": "C"}),
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("most-complete: got %#v want %#v", got, want)
	}
}
