package domain

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"
)

//
// =====================================
//  Tests for domain.Record + Layout
// =====================================
//
// Rationale:
//   The domain layer defines the shape of our business object for the
//   “ownership” CSV. While there is no behavior here today (no methods),
//   we still validate the following invariants to protect the contract:
//
//   • Zero-value semantics: ints/bools are zeroed, pointer fields are nil.
//   • Pointer field handling: non-nil pointers preserve values as expected.
//   • Date layout compatibility: Layout is "02.01.2006" and can parse dates
//     we expect to store in DatumOd/DatumDo (and round-trip via time.Time).
//   • JSON shape: nil pointers marshal to JSON null; time pointers marshal
//     as RFC3339 timestamps (encoding/json default for time.Time).
//
// These tests are hermetic (no I/O) and deterministic.
//

// TestRecord_ZeroValue verifies zero-value semantics are preserved.
// This protects callers that rely on Go zero-initialization.
func TestRecord_ZeroValue(t *testing.T) {
	t.Parallel()

	var r Record // zero value

	// ints and bool should be zero-values; all pointers should be nil.
	if r.PCV != 0 || r.TypSubjektu != 0 || r.VztahKVozidlu != 0 || r.Aktualni != false {
		t.Fatalf("zero-value ints/bool not zeroed: %#v", r)
	}
	if r.ICO != nil || r.Nazev != nil || r.Adresa != nil || r.DatumOd != nil || r.DatumDo != nil {
		t.Fatalf("zero-value pointers should be nil: %#v", r)
	}
}

// TestRecord_WithPointersAndDates validates that non-nil pointers hold the
// expected values and dates can be parsed using the declared Layout.
func TestRecord_WithPointersAndDates(t *testing.T) {
	t.Parallel()

	// Helper constructors for pointer fields.
	intPtr := func(v int) *int { return &v }
	strPtr := func(s string) *string { return &s }

	// Parse dates using the domain layout ("02.01.2006").
	// Use UTC to ensure stable RFC3339 JSON encoding later if needed.
	mustParse := func(date string) *time.Time {
		tm, err := time.ParseInLocation(Layout, date, time.UTC)
		if err != nil {
			t.Fatalf("parse %q with layout %q: %v", date, Layout, err)
		}
		return &tm
	}

	r := Record{
		PCV:           12345,
		TypSubjektu:   1,
		VztahKVozidlu: 2,
		Aktualni:      true,
		ICO:           intPtr(98765432),
		Nazev:         strPtr("Acme s.r.o."),
		Adresa:        strPtr("Main Street 1"),
		DatumOd:       mustParse("01.02.2020"),
		DatumDo:       mustParse("03.04.2021"),
	}

	// Basic field assertions (values and non-nil pointers).
	if r.PCV != 12345 || r.TypSubjektu != 1 || r.VztahKVozidlu != 2 || !r.Aktualni {
		t.Fatalf("scalar fields mismatch: %#v", r)
	}
	if r.ICO == nil || *r.ICO != 98765432 {
		t.Fatalf("ICO pointer mismatch: %+v", r.ICO)
	}
	if r.Nazev == nil || *r.Nazev != "Acme s.r.o." {
		t.Fatalf("Nazev pointer mismatch: %+v", r.Nazev)
	}
	if r.Adresa == nil || *r.Adresa != "Main Street 1" {
		t.Fatalf("Adresa pointer mismatch: %+v", r.Adresa)
	}

	// Dates parsed using Layout must match expected year/month/day.
	expect := func(got *time.Time, y int, m time.Month, d int) {
		if got == nil {
			t.Fatalf("date pointer is nil; want %04d-%02d-%02d", y, m, d)
		}
		if got.Year() != y || got.Month() != m || got.Day() != d {
			t.Fatalf("date value mismatch: got=%s want=%04d-%02d-%02d", got.UTC().Format(time.RFC3339), y, m, d)
		}
	}
	expect(r.DatumOd, 2020, time.February, 1)
	expect(r.DatumDo, 2021, time.April, 3)
}

// TestRecord_JSONShape validates the default encoding/json behavior for the
// struct: nil pointers become null, time pointers become RFC3339 strings,
// and scalars are encoded as expected. We deserialize into a map to avoid
// coupling to JSON tag choices (none are present today).
func TestRecord_JSONShape(t *testing.T) {
	t.Parallel()

	// Build a partially-populated record to observe nulls for nil pointers.
	ico := 123
	name := "Someone"
	when, _ := time.ParseInLocation(Layout, "10.11.2022", time.UTC)

	r := Record{
		PCV:           7,
		TypSubjektu:   8,
		VztahKVozidlu: 9,
		Aktualni:      false,
		ICO:           &ico,
		Nazev:         &name,
		// Adresa is nil (should become null)
		DatumOd: &when,
		// DatumDo is nil (should become null)
	}

	b, err := json.Marshal(r)
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}

	// Decode to generic map to make assertions on presence and value shapes.
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatalf("json.Unmarshal: %v", err)
	}

	// Scalar checks.
	wantScalars := map[string]any{
		"PCV":           float64(7), // JSON numbers decode to float64
		"TypSubjektu":   float64(8),
		"VztahKVozidlu": float64(9),
		"Aktualni":      false,
	}
	for k, v := range wantScalars {
		if !reflect.DeepEqual(m[k], v) {
			t.Fatalf("scalar %s mismatch: got=%#v want=%#v (json=%s)", k, m[k], v, string(b))
		}
	}

	// Non-nil pointer fields encoded as their values.
	if got := m["ICO"]; !reflect.DeepEqual(got, float64(123)) {
		t.Fatalf("ICO json mismatch: got=%#v want=123 (json=%s)", got, string(b))
	}
	if got := m["Nazev"]; !reflect.DeepEqual(got, "Someone") {
		t.Fatalf("Nazev json mismatch: got=%#v want=%q (json=%s)", got, "Someone", string(b))
	}

	// Nil pointer fields should appear as null (encoding/json default).
	if got := m["Adresa"]; got != nil {
		t.Fatalf("Adresa json mismatch: got=%#v want=null (json=%s)", got, string(b))
	}
	if got := m["DatumDo"]; got != nil {
		t.Fatalf("DatumDo json mismatch: got=%#v want=null (json=%s)", got, string(b))
	}

	// time.Time pointer marshals to RFC3339 string in UTC.
	gotWhen, ok := m["DatumOd"].(string)
	if !ok {
		t.Fatalf("DatumOd should be string, got %T (%#v) (json=%s)", m["DatumOd"], m["DatumOd"], string(b))
	}
	if parsed, err := time.Parse(time.RFC3339, gotWhen); err != nil || !parsed.Equal(when.UTC()) {
		t.Fatalf("DatumOd RFC3339 mismatch: got=%q parsed=%v err=%v want=%v", gotWhen, parsed, err, when.UTC())
	}
}
