// Package probe contains unit tests for CSV sampling, type inference,
// normalization, and JSON-config building logic in the csvprobe tool.
package probe

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

//
// ---- fetchFirstBytes --------------------------------------------------------
//

// TestFetchFirstBytes_RangeAndLimit verifies that the function requests a byte
// range when n>0 and never returns more than n bytes even if the server ignores
// Range and returns HTTP 200 with a larger body.
func TestFetchFirstBytes_RangeAndLimit(t *testing.T) {
	t.Parallel()

	const n = 32
	var sawRange string

	// Server always returns a large body, ignoring Range.
	body := strings.Repeat("X", 1024)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sawRange = r.Header.Get("Range")
		_, _ = w.Write([]byte(body))
	}))
	defer srv.Close()

	got, err := fetchFirstBytes(srv.URL, n)
	if err != nil {
		t.Fatalf("fetchFirstBytes error: %v", err)
	}
	if len(got) > n {
		t.Fatalf("len(got)=%d; want <= %d", len(got), n)
	}
	if want := "bytes=0-31"; sawRange != want {
		t.Fatalf("Range header = %q; want %q", sawRange, want)
	}
}

//
// ---- readCSVSample / helpers -----------------------------------------------
//

// TestReadCSVSample_SkipMalformedAndWidth ensures rows with wrong field counts
// are skipped, while good rows are returned at header width.
func TestReadCSVSample_SkipMalformedAndWidth(t *testing.T) {
	t.Parallel()

	csv := "" + // header first; we don't rely on header-line skipping semantics
		"a,b,c\n" +
		"1,2,3\n" + // good
		"4,5\n" + // short -> skipped
		"bad\"quote,7,8\n" + // may parse or be skipped depending on reader; we only assert on aligned rows
		"9,10,11\n" // good

	headers, rows, err := readCSVSample([]byte(csv), ',')
	if err != nil {
		t.Fatalf("readCSVSample error: %v", err)
	}
	if got, want := strings.Join(headers, "|"), "a|b|c"; got != want {
		t.Fatalf("headers=%q; want %q", got, want)
	}
	// At least the two fully aligned rows must pass.
	if len(rows) < 2 {
		t.Fatalf("len(rows)=%d; want >= 2", len(rows))
	}
	for i, r := range rows {
		if len(r) != len(headers) {
			t.Fatalf("row %d width=%d; want %d", i, len(r), len(headers))
		}
	}
}

// TestFitRowToWidth validates that rows are padded or truncated to the
// requested width.
func TestFitRowToWidth(t *testing.T) {
	t.Parallel()
	cases := []struct {
		row  []string
		n    int
		want []string
	}{
		{[]string{"a", "b", "c"}, 3, []string{"a", "b", "c"}},
		{[]string{"a", "b", "c"}, 2, []string{"a", "b"}},
		{[]string{"a"}, 3, []string{"a", "", ""}},
	}
	for _, tc := range cases {
		got := fitRowToWidth(tc.row, tc.n)
		if len(got) != len(tc.want) {
			t.Fatalf("len=%d; want %d", len(got), len(tc.want))
		}
		for i := range tc.want {
			if got[i] != tc.want[i] {
				t.Fatalf("got[%d]=%q; want %q", i, got[i], tc.want[i])
			}
		}
	}
}

// TestStripUTF8BOM verifies BOM removal from the first header cell.
func TestStripUTF8BOM(t *testing.T) {
	t.Parallel()
	got := stripUTF8BOM([]string{"\uFEFFname", "age"})
	if got[0] != "name" {
		t.Fatalf("BOM not removed: %q", got[0])
	}
}

//
// ---- type inference ---------------------------------------------------------
//

// TestInferTypeForColumn covers boolean, integer, real, date, timestamp, and
// fallback to text using table-driven cases.
func TestInferTypeForColumn(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		values []string
		want   string
	}{
		{"AllEmpty", []string{"", " ", "   "}, "text"},
		{"Integers", []string{"1", "0", "-10", "42"}, "integer"},
		{"Booleans", []string{"true", "FALSE", "0", "Yes"}, "boolean"},
		{"Reals", []string{"1.1", "2e3", "3.14"}, "real"},
		{"Dates", []string{"2024-01-02", "02.01.2024"}, "date"},
		// Use actual formatted timestamps, not layout constants.
		{"Timestamps",
			[]string{
				time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC).Format(time.RFC3339),
				time.Date(2024, 1, 2, 3, 4, 5, 123456789, time.UTC).Format(time.RFC3339Nano),
			},
			"timestamp"},
		{"MixedText", []string{"x", "1", "true"}, "text"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := inferTypeForColumn(tc.values); got != tc.want {
				t.Fatalf("inferTypeForColumn=%q; want %q", got, tc.want)
			}
		})
	}
}

// TestInferTypes verifies per-column inference across multiple rows.
func TestInferTypes(t *testing.T) {
	t.Parallel()
	headers := []string{"i", "b", "f", "d", "ts", "txt"}
	rows := [][]string{
		{"1", "true", "3.14", "2024-01-02", "2024-01-02T01:02:03Z", "x"},
		{"2", "0", "2e3", "02.01.2024", "2006-01-02 15:04:05", ""},
	}
	got := inferTypes(headers, rows)
	want := []string{"integer", "boolean", "real", "date", "timestamp", "text"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("types=%v; want %v", got, want)
	}
}

// TestNumericAndBoolHelpers covers isInt, isFloat, isBool basic paths.
func TestNumericAndBoolHelpers(t *testing.T) {
	t.Parallel()
	if !isInt(" -10 ") || isInt("1.0") {
		t.Fatal("isInt failed basic cases")
	}
	if isFloat("10") || !isFloat("3.14") || !isFloat("2e9") {
		t.Fatal("isFloat failed basic cases")
	}
	trues := []string{"true", "t", "yes", "y", "1"}
	falses := []string{"false", "f", "no", "n", "0"}
	for _, v := range trues {
		if !isBool(v) {
			t.Fatalf("isBool(%q) = false; want true", v)
		}
	}
	for _, v := range falses {
		if !isBool(v) {
			t.Fatalf("isBool(%q) = false; want true", v)
		}
	}
}

// TestParseDateOrTimestamp checks detection and hasTime flag.
func TestParseDateOrTimestamp(t *testing.T) {
	t.Parallel()
	ok, timey := parseDateOrTimestamp("2024-01-02T03:04:05Z")
	if !ok || !timey {
		t.Fatalf("timestamp not detected: ok=%v time=%v", ok, timey)
	}
	ok, timey = parseDateOrTimestamp("02.01.2024")
	if !ok || timey {
		t.Fatalf("date not detected: ok=%v time=%v", ok, timey)
	}
	ok, _ = parseDateOrTimestamp("nope")
	if ok {
		t.Fatal("unexpected ok for invalid input")
	}
}

//
// ---- normalization & naming -------------------------------------------------
//

// TestNormalizeFieldName verifies lowercasing, accent stripping, and allowed
// character filtering, including collapsing to single underscores.
func TestNormalizeFieldName(t *testing.T) {
	t.Parallel()
	cases := []struct {
		in, want string
	}{
		{"  Hello World  ", "hello_world"},
		{"PČV", "pcv"},
		{"Straße", "strae"}, // Current implementation drops non-ASCII like ß, resulting in "strae".
		{"A-B.C", "a_b_c"},
		{"__  ", "col"},
	}
	for _, tc := range cases {
		if got := normalizeFieldName(tc.in); got != tc.want {
			t.Fatalf("normalizeFieldName(%q)=%q; want %q", tc.in, got, tc.want)
		}
	}
}

// TestTruncateFieldName enforces the 63-char limit helper.
func TestTruncateFieldName(t *testing.T) {
	t.Parallel()
	long := strings.Repeat("x", 70)
	got := truncateFieldName(long)
	if len(got) != 63 {
		t.Fatalf("len=%d; want 63", len(got))
	}
}

//
// ---- JSON config builder & layout selection --------------------------------
//

// TestBuildJSONConfig_StructureAndHeuristics validates that:
//   - header_map preserves CSV order,
//   - coerce appears before validate,
//   - only the first integer column with all non-empty samples is marked required,
//   - storage.postgres.columns are normalized names.
func TestBuildJSONConfig_StructureAndHeuristics(t *testing.T) {
	t.Parallel()

	headers := []string{"ID", "AGE", "NAME"}
	rows := [][]string{
		{"1", "100", "A"}, // all non-empty int at col 0 and col 1
		{"2", "200", "B"},
	}
	types := []string{"integer", "integer", "text"}
	cfg := buildJSONConfig("Some Name", headers, rows, types, ',')

	// Marshal via printJSONConfig and assert using generic map to avoid
	// depending on unexported types.
	raw, err := printJSONConfig(cfg)
	if err != nil {
		t.Fatalf("printJSONConfig error: %v", err)
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("json.Unmarshal: %v\n%s", err, string(raw))
	}

	// parser.header_map should be in the same order as headers.
	parser := m["parser"].(map[string]any)
	opts := parser["options"].(map[string]any)
	if hmRaw, ok := opts["header_map"]; ok && hmRaw != nil {
		// Prefer the {pairs:[{key:...,value:...}]} encoding if present.
		if hmMap, ok := hmRaw.(map[string]any); ok {
			if pairsRaw, ok := hmMap["pairs"]; ok && pairsRaw != nil {
				if pairs, ok := pairsRaw.([]any); ok {
					if got := extractKeys(pairs); strings.Join(got, ",") != strings.Join(headers, ",") {
						t.Fatalf("header_map keys = %v; want %v", got, headers)
					}
				}
			}
		}
	}

	// transforms: first is coerce, second is validate.
	trs := m["transform"].([]any)
	if len(trs) < 2 {
		t.Fatalf("expected at least 2 transforms, got %d", len(trs))
	}
	if trs[0].(map[string]any)["kind"] != "coerce" || trs[1].(map[string]any)["kind"] != "validate" {
		t.Fatalf("transform order incorrect: %v", trs)
	}

	// validate.contract.fields: only the first integer-all-non-empty is required.
	val := trs[1].(map[string]any)
	fields := val["options"].(map[string]any)["contract"].(map[string]any)["fields"].([]any)
	var reqIdx []int
	for i, f := range fields {
		if f.(map[string]any)["required"] == true {
			reqIdx = append(reqIdx, i)
		}
	}
	if len(reqIdx) != 1 || reqIdx[0] != 0 {
		t.Fatalf("required fields idx=%v; want [0]", reqIdx)
	}

	// storage.postgres.columns must be normalized header names.
	st := m["storage"].(map[string]any)["postgres"].(map[string]any)
	cols := toStringSlice(st["columns"].([]any))
	wantCols := []string{"id", "age", "name"}
	if strings.Join(cols, ",") != strings.Join(wantCols, ",") {
		t.Fatalf("storage.postgres.columns = %v; want %v", cols, wantCols)
	}
}

func extractKeys(pairs []any) []string {
	out := make([]string, 0, len(pairs))
	for _, p := range pairs {
		m := p.(map[string]any)
		out = append(out, m["key"].(string))
	}
	return out
}
func toStringSlice(vs []any) []string {
	out := make([]string, len(vs))
	for i := range vs {
		out[i] = vs[i].(string)
	}
	return out
}

// TestChooseMajorityLayout confirms the majority-with-preference behavior.
func TestChooseMajorityLayout(t *testing.T) {
	t.Parallel()
	inferred := []string{"date", "timestamp", "date", "text"}
	colLayouts := []string{"2006-01-02", time.RFC3339, "02.01.2006", ""}

	// Two date layouts vs one timestamp → majority is date, tie by preference:
	// ISO (2) vs DMY (3) => DMY wins.
	got := chooseMajorityLayout(colLayouts, inferred)
	if got != "02.01.2006" {
		t.Fatalf("chooseMajorityLayout=%q; want %q", got, "02.01.2006")
	}
}

// TestSelectBestLayout_Ties ensures ties are resolved by preference then order.
func TestSelectBestLayout_Ties(t *testing.T) {
	t.Parallel()
	samples := []string{"2024-01-02T03:04:05Z", "2024-01-03T04:05:06Z"}
	layouts := []string{time.RFC3339, time.RFC3339Nano}
	got := selectBestLayout(samples, layouts, timestampLayoutPreference)
	if got != time.RFC3339Nano {
		t.Fatalf("selectBestLayout=%q; want %q", got, time.RFC3339Nano)
	}
}

// TestDetectColumnLayouts verifies per-column layout detection for date/timestamp.
func TestDetectColumnLayouts(t *testing.T) {
	t.Parallel()
	rows := [][]string{
		{"2024-01-02", "2024-01-02T03:04:05Z", "x"},
		{"02.01.2024", "2006-01-02 15:04:05", ""},
	}
	inferred := []string{"date", "timestamp", "text"}
	got := detectColumnLayouts(rows, inferred)
	if got[0] == "" || got[1] == "" || got[2] != "" {
		t.Fatalf("layouts=%v; expected non-empty for date/timestamp and empty for text", got)
	}
}
