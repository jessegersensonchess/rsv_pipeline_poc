package builtin

import (
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"etl/internal/schema"
	"etl/pkg/records"
)

/*
TestValidateApply_Table verifies end-to-end validation behavior of Apply:
  - required fields are enforced (nil/empty string => reject),
  - type checks for int/bool/date work per spec,
  - enum lists are enforced,
  - non-required empty values pass,
  - rejected rows invoke the Reject callback with Reason and Stage,
  - output preserves original order of surviving records and does not mutate
    the input slice header or replace underlying maps.
*/
func TestValidateApply_Table(t *testing.T) {
	contract := schema.Contract{
		Fields: []schema.Field{
			{Name: "id", Type: "int", Required: true},
			// Use defaults for bool so both "YES" and "true" are accepted.
			{Name: "flag", Type: "bool"},
			{Name: "d1", Type: "date", Layout: "02.01.2006"}, // field-specific layout
			{Name: "d2", Type: "date"},                       // uses ISO or global fallback
			{Name: "status", Type: "string", Enum: []string{"new", "ok", "done"}},
			{Name: "note", Type: "text"}, // free-form
		},
	}

	var rejects []RejectedRow
	v := Validate{
		Contract:   contract,
		DateLayout: "2006/01/02", // global fallback
		Policy:     "lenient",
		Reject:     func(r RejectedRow) { rejects = append(rejects, r) },
	}

	in := []records.Record{
		// 0: valid (int, YES truthy via defaults, date via field layout, enum ok, ISO date)
		{"id": "7", "flag": "YES", "d1": "09.11.2025", "d2": "2025-11-09", "status": "ok", "note": "free"},
		// 1: valid (bool default truthy "true", status enum "new", d2 via global fallback)
		{"id": "8", "flag": "true", "d1": "", "d2": "2025/11/09", "status": "new"},
		// 2: reject: required id missing
		{"flag": "NO", "status": "ok"},
		// 3: reject: id not an int
		{"id": "x3", "flag": "NO", "status": "ok"},
		// 4: reject: flag unrecognized (neither default truthy/falsy)
		{"id": "3", "flag": "MAYBE", "status": "ok"},
		// 5: reject: enum mismatch
		{"id": "9", "flag": "no", "status": "bad"},
		// 6: accept: non-required empty strings pass
		{"id": "10", "flag": "", "d1": "", "d2": "", "status": "done"},
	}

	out := v.Apply(in)

	// Expect rows 0,1,6 to survive in order.
	if len(out) != 3 {
		t.Fatalf("survivors=%d; want 3", len(out))
	}
	if !reflect.DeepEqual(out[0], in[0]) || !reflect.DeepEqual(out[1], in[1]) || !reflect.DeepEqual(out[2], in[6]) {
		t.Fatalf("order/content mismatch: got=%#v", out)
	}

	// Record map identity preserved.
	if reflect.ValueOf(out[0]).Pointer() != reflect.ValueOf(in[0]).Pointer() {
		t.Fatalf("record map at 0 was replaced; want same map instance")
	}

	// Reject callback fired with reasons and Stage "validate".
	if len(rejects) != 4 {
		t.Fatalf("rejects=%d; want 4", len(rejects))
	}
	for _, r := range rejects {
		if r.Stage != "validate" || strings.TrimSpace(r.Reason) == "" || r.Raw == nil {
			t.Fatalf("bad reject payload: %#v", r)
		}
	}
}

/*
Test_validateRecord_Cases exercises validateRecord() directly for edge cases:
  - bool defaults for Czech "ano"/"ne",
  - time.Time zero vs. non-zero,
  - type mismatches for date/int,
  - unknown field type => accept (documented behavior).
*/
func Test_validateRecord_Cases(t *testing.T) {
	contract := schema.Contract{
		Fields: []schema.Field{
			{Name: "f", Type: "bool"},    // uses default truthy/falsy (includes "ano"/"ne")
			{Name: "t", Type: "date"},    // will test with time.Time
			{Name: "x", Type: "date"},    // wrong type (int) -> reject
			{Name: "y", Type: "int"},     // string "  5  " -> ok
			{Name: "z", Type: "unknown"}, // unknown type -> accept
			{Name: "req", Type: "string", Required: true},
		},
	}
	v := Validate{Contract: contract, DateLayout: ""}

	// Invalid because x is not date-convertible.
	ok, reason := v.validateRecord(records.Record{
		"f":   "ANO",
		"t":   time.Date(2025, 11, 9, 0, 0, 0, 0, time.UTC),
		"x":   123,
		"y":   "  5  ",
		"z":   3.14,
		"req": "present",
	})
	if ok {
		t.Fatalf("expected rejection because x is not date-convertible; got ok with reason=%q", reason)
	}

	// Now provide proper x and confirm success.
	ok2, reason2 := v.validateRecord(records.Record{
		"f":   "ne",         // default falsy
		"t":   time.Time{},  // zero value -> treated as empty (passes non-required)
		"x":   "2025-11-09", // ISO date ok
		"y":   "5",
		"z":   struct{}{},
		"req": "x",
	})
	if !ok2 {
		t.Fatalf("unexpected reject: %s", reason2)
	}
}

/*
Test_helpers_asString_isBool_parseAnyDate checks helper functions for boundary
behavior: asString on diverse inputs; isBoolTrueFalse with custom lists and
defaults; parseAnyDate against field/global layouts and ISO.
*/
//func Test_helpers_asString_isBool_parseAnyDate(t *testing.T) {
//	// asString
//	if asString(nil) != "" || asString("x") != "x" || !strings.Contains(asString(123), "123") {
//		t.Fatalf("asString unexpected output")
//	}
//
//	// isBoolTrueFalse with explicit truthy/falsy (case-insensitive match)
//	customTrue := []string{"Y", "OK"}
//	customFalse := []string{"N", "NOPE"}
//	if !isBoolTrueFalse("y", customTrue, customFalse) {
//		t.Fatalf("expected y to be truthy")
//	}
//	if !isBoolTrueFalse("nope", customTrue, customFalse) {
//		t.Fatalf("expected nope to be falsy")
//	}
//	if isBoolTrueFalse("maybe", customTrue, customFalse) {
//		t.Fatalf("expected maybe to be unrecognized")
//	}
//
//	// default sets (should accept "ano"/"ne")
//	if !isBoolTrueFalse("ano", nil, nil) || !isBoolTrueFalse("ne", nil, nil) {
//		t.Fatalf("default czech truthy/falsy not recognized")
//	}
//
//	// parseAnyDate
//	if !parseAnyDate("", "", "") { // empty is allowed
//		t.Fatalf("empty string should be accepted")
//	}
//	if !parseAnyDate("2025-11-09", "", "") {
//		t.Fatalf("ISO date should be accepted")
//	}
//	if !parseAnyDate("09.11.2025", "02.01.2006", "") {
//		t.Fatalf("field layout should be used")
//	}
//	if !parseAnyDate("2025/11/09", "", "2006/01/02") {
//		t.Fatalf("global fallback layout should be used")
//	}
//	if parseAnyDate("11/09/25", "", "") {
//		t.Fatalf("unexpected acceptance of invalid date")
//	}
//}

/*
BenchmarkValidate_AllValid measures throughput when all rows pass validation.
*/
func BenchmarkValidate_AllValid(b *testing.B) {
	contract := schema.Contract{
		Fields: []schema.Field{
			{Name: "id", Type: "int", Required: true},
			{Name: "flag", Type: "bool"},
			{Name: "d", Type: "date"},
			{Name: "status", Type: "string", Enum: []string{"new", "ok", "done"}},
		},
	}
	v := Validate{Contract: contract, DateLayout: "2006-01-02"}

	const N = 30000
	in := make([]records.Record, N)
	for i := 0; i < N; i++ {
		in[i] = records.Record{
			"id":     strconv.Itoa(i + 1),
			"flag":   "true",
			"d":      "2025-11-09",
			"status": "ok",
		}
	}

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		out := v.Apply(in)
		if len(out) != N {
			b.Fatalf("got %d survivors; want %d", len(out), N)
		}
	}
}

/*
BenchmarkValidate_MixedRejects measures the cost when roughly half the rows
are rejected due to type/enum errors.
*/
func BenchmarkValidate_MixedRejects(b *testing.B) {
	contract := schema.Contract{
		Fields: []schema.Field{
			{Name: "id", Type: "int", Required: true},
			{Name: "flag", Type: "bool"},
			{Name: "d", Type: "date"},
			{Name: "status", Type: "string", Enum: []string{"new", "ok", "done"}},
		},
	}
	v := Validate{Contract: contract, DateLayout: "2006-01-02"}

	const N = 30000
	in := make([]records.Record, N)
	for i := 0; i < N; i++ {
		ok := i%2 == 0
		id := strconv.Itoa(i + 1)
		if !ok {
			id = "x" // bad int
		}
		status := "ok"
		if !ok {
			status = "bad" // enum fail
		}
		date := "2025-11-09"
		if !ok && i%4 == 1 {
			date = "11/09/25" // invalid format
		}
		in[i] = records.Record{
			"id":     id,
			"flag":   "true",
			"d":      date,
			"status": status,
		}
	}

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = v.Apply(in)
	}
}

/*
BenchmarkValidate_WithRejectCallback adds a lightweight Reject that simulates
counting / buffering of rejected rows to expose callback overhead.
*/
func BenchmarkValidate_WithRejectCallback(b *testing.B) {
	contract := schema.Contract{
		Fields: []schema.Field{
			{Name: "id", Type: "int", Required: true},
			{Name: "flag", Type: "bool"},
			{Name: "d", Type: "date", Layout: "02.01.2006"},
		},
	}
	var rejCount int
	v := Validate{
		Contract:   contract,
		DateLayout: "2006-01-02",
		Reject:     func(RejectedRow) { rejCount++ },
	}

	const N = 20000
	in := make([]records.Record, N)
	for i := 0; i < N; i++ {
		// Alternate a few failure reasons.
		switch i % 3 {
		case 0:
			in[i] = records.Record{"id": "x", "flag": "yes", "d": "09.11.2025"} // bad int, ok date
		case 1:
			in[i] = records.Record{"id": "1", "flag": "maybe", "d": "2025-11-09"} // bad bool
		default:
			in[i] = records.Record{"id": "2", "flag": "no", "d": "11/09/25"} // bad date
		}
	}

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		rejCount = 0
		_ = v.Apply(in)
	}
	_ = rejCount
}
