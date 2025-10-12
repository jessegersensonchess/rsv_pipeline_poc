package csvutil

import (
	"bufio"
	"strings"
	"testing"
)

// TestReadLogicalCSVLine_SingleLineNoQuotes verifies the simplest case:
// one physical line with a trailing newline, no quotes to track.
func TestReadLogicalCSVLine_SingleLineNoQuotes(t *testing.T) {
	r := bufio.NewReader(strings.NewReader("a,b,c\n"))
	got, err := ReadLogicalCSVLine(r)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if got != "a,b,c" {
		t.Fatalf("got %q, want %q", got, "a,b,c")
	}
}

// TestReadLogicalCSVLine_QuotedCRLFInsideField ensures that logical lines
// spanning multiple physical lines (quoted CRLF) are stitched back with
// a CRLF boundary.
func TestReadLogicalCSVLine_QuotedCRLFInsideField(t *testing.T) {
	in := "\"a\r\nb\",c\n"
	r := bufio.NewReader(strings.NewReader(in))
	got, err := ReadLogicalCSVLine(r)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	want := "\"a\r\nb\",c"
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

// TestReadLogicalCSVLine_EOFEmpty confirms that attempting to read from
// an empty reader yields io.EOF.
func TestReadLogicalCSVLine_EOFEmpty(t *testing.T) {
	r := bufio.NewReader(strings.NewReader(""))
	_, err := ReadLogicalCSVLine(r)
	if err == nil {
		t.Fatalf("expected EOF")
	}
}

// TestReadLogicalCSVLine_LastLineNoNewline validates that a final line
// without a trailing newline is still returned as a logical record.
func TestReadLogicalCSVLine_LastLineNoNewline(t *testing.T) {
	r := bufio.NewReader(strings.NewReader("x,y,z"))
	got, err := ReadLogicalCSVLine(r)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if got != "x,y,z" {
		t.Fatalf("got %q want %q", got, "x,y,z")
	}
}

// TestReadLogicalCSVLine_UnbalancedQuoteAtEOF ensures that when EOF is reached
// while still "in quotes", the function returns the accumulated content rather
// than erroring. This matches the tolerant intent of the reader.
func TestReadLogicalCSVLine_UnbalancedQuoteAtEOF(t *testing.T) {
	r := bufio.NewReader(strings.NewReader(`"partial`))
	got, err := ReadLogicalCSVLine(r)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if got != `"partial"[:9]`[:8] { // simple sanity: starts with quote and includes text
		// Fallback explicit check to avoid clever slicing above if confusing:
		if !strings.HasPrefix(got, `"partial`) {
			t.Fatalf("got %q, want prefix %q", got, `"partial`)
		}
	}
}

// TestParseCSVLineLoose_Simple sanity-checks unquoted fields.
func TestParseCSVLineLoose_Simple(t *testing.T) {
	fields, err := ParseCSVLineLoose("a,b,c")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	want := []string{"a", "b", "c"}
	if got, want := len(fields), len(want); got != want {
		t.Fatalf("len=%d, want %d (%v)", got, want, fields)
	}
	for i := range want {
		if fields[i] != want[i] {
			t.Fatalf("i=%d got %q want %q", i, fields[i], want[i])
		}
	}
}

// TestParseCSVLineLoose_EmbeddedQuotesAndEscapes hits tolerant paths:
// inner doubled quotes, spaces after quotes, and commas in quoted fields.
func TestParseCSVLineLoose_EmbeddedQuotesAndEscapes(t *testing.T) {
	line := `"a""b", "x", "y, z", unq"outed, last`
	fields, err := ParseCSVLineLoose(line)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(fields) < 4 {
		t.Fatalf("expected at least 4 fields, got %d: %v", len(fields), fields)
	}
	if fields[0] != `a"b` {
		t.Fatalf("fields[0]=%q want %q", fields[0], `a"b`)
	}
	joined := strings.Join(fields, "|")
	if !strings.Contains(joined, "y") || !strings.Contains(joined, "z") {
		t.Fatalf("expected y and z present in some field(s): %v", fields)
	}
	if !strings.Contains(joined, "unq") || !strings.Contains(joined, "last") {
		t.Fatalf("expected 'unq' and 'last' fragments in %v", fields)
	}
}

// TestParseCSVLineLoose_TrailingComma ensures a trailing delimiter yields an
// empty final field (common CSV edge case).
func TestParseCSVLineLoose_TrailingComma(t *testing.T) {
	fields, err := ParseCSVLineLoose(`"a",b,`)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(fields) != 3 || fields[2] != "" {
		t.Fatalf("want 3 fields with trailing empty, got %v", fields)
	}
}

// TestParseCSVLineLoose_DoubleQuoteFollowedByNonDelimiter drives the branch
// where a doubled quote is followed by a non-delimiter non-EOF character,
// which should be treated as a literal quote (not closing), remaining inQuotes.
func TestParseCSVLineLoose_DoubleQuoteFollowedByNonDelimiter(t *testing.T) {
	fields, err := ParseCSVLineLoose(`"ab""Xc",t`)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if fields[0] != `ab"Xc` {
		t.Fatalf("fields[0]=%q want %q", fields[0], `ab"Xc`)
	}
	if fields[1] != "t" {
		t.Fatalf("fields[1]=%q want %q", fields[1], "t")
	}
}

// TestLexBySpaceWithQuotes validates tokenization with doubled quotes,
// collapsed whitespace outside quotes, and preserved whitespace inside quotes.
func TestLexBySpaceWithQuotes(t *testing.T) {
	line := `1  2   "three four"  five""six   "a ""quoted"" token"`
	toks := LexBySpaceWithQuotes(line)
	want := []string{"1", "2", "three four", `fivesix`, `a "quoted" token`}
	if len(toks) != len(want) {
		t.Fatalf("len=%d want %d: %v", len(toks), len(want), toks)
	}
	for i := range want {
		if toks[i] != want[i] {
			t.Errorf("i=%d got %q want %q", i, toks[i], want[i])
		}
	}
}

// TestLexBySpaceWithQuotes_LeadingTrailingSpace ensures leading/trailing
// whitespace does not create empty tokens.
func TestLexBySpaceWithQuotes_LeadingTrailingSpace(t *testing.T) {
	line := `   alpha  beta   `
	toks := LexBySpaceWithQuotes(line)
	if len(toks) != 2 || toks[0] != "alpha" || toks[1] != "beta" {
		t.Fatalf("unexpected tokens: %v", toks)
	}
}

// TestParseSpaceSeparatedRow_OK verifies the happy path reconstruction into
// the 9-field canonical shape with both dates present.
func TestParseSpaceSeparatedRow_OK(t *testing.T) {
	line := `123 4 5 True 100 "ACME, Inc." "Main St 5" 01.01.2020 02.01.2020`
	fields, ok := ParseSpaceSeparatedRow(line)
	if !ok {
		t.Fatalf("expected ok")
	}
	if len(fields) != 9 {
		t.Fatalf("len=%d want 9: %v", len(fields), fields)
	}
	if fields[0] != "123" || fields[5] != "ACME, Inc." {
		t.Fatalf("unexpected fields: %v", fields)
	}
}

// TestParseSpaceSeparatedRow_MissingDatesStillPads ensures missing date
// columns are padded with empty strings.
func TestParseSpaceSeparatedRow_MissingDatesStillPads(t *testing.T) {
	line := `123 4 5 True 100 Name Address  `
	fields, ok := ParseSpaceSeparatedRow(line)
	if !ok {
		t.Fatalf("expected ok for input without dates")
	}
	if len(fields) != 9 {
		t.Fatalf("len=%d want 9: %v", len(fields), fields)
	}
	wantPrefix := []string{"123", "4", "5", "True", "100", "Name", "Address"}
	for i := range wantPrefix {
		if fields[i] != wantPrefix[i] {
			t.Fatalf("fields[%d]=%q want %q (all=%v)", i, fields[i], wantPrefix[i], fields)
		}
	}
	if fields[7] != "" || fields[8] != "" {
		t.Fatalf("expected empty date fields, got d1=%q d2=%q", fields[7], fields[8])
	}
}

// TestParseSpaceSeparatedRow_TooShort exercises the early arity guard.
func TestParseSpaceSeparatedRow_TooShort(t *testing.T) {
	line := `123 4 5` // clearly too few tokens
	_, ok := ParseSpaceSeparatedRow(line)
	if ok {
		t.Fatalf("expected not ok for too-short input")
	}
}

// TestParseSpaceSeparatedRow_InsufficientAfterDateStrip covers the case where
// initial arity >= 6 but after peeling dates there are < 5 core tokens.
func TestParseSpaceSeparatedRow_InsufficientAfterDateStrip(t *testing.T) {
	line := `A B C D 01.01.2020 02.01.2020`
	_, ok := ParseSpaceSeparatedRow(line)
	if ok {
		t.Fatalf("expected not ok after date strip leaves <5 tokens")
	}
}

// TestRepairOverlongCommaFields validates a successful repair when the last
// two fields are dates and the "name" was split by commas.
func TestRepairOverlongCommaFields(t *testing.T) {
	in := []string{"1", "2", "3", "True", "100", "Doe", "John, Jr.", "Main st 5", "01.01.2020", "02.01.2020"}
	out, ok := RepairOverlongCommaFields(in)
	if !ok {
		t.Fatalf("expected repair")
	}
	if len(out) != 9 {
		t.Fatalf("len=%d want 9: %v", len(out), out)
	}
	if out[5] != "Doe,John, Jr." { // no space added after the first comma by design
		t.Fatalf("name repaired = %q", out[5])
	}
}

// TestRepairOverlongCommaFields_TooShort ensures inputs with <= 9 fields
// cannot be repaired.
func TestRepairOverlongCommaFields_TooShort(t *testing.T) {
	in := []string{"1", "2", "3", "4", "5", "6", "7", "8", "9"}
	if out, ok := RepairOverlongCommaFields(in); ok || out != nil {
		t.Fatalf("expected not repairable, got %v %v", ok, out)
	}
}

// TestRepairOverlongCommaFields_InvalidDates ensures the last two fields must
// be dates (or empty) for the function to attempt a repair.
func TestRepairOverlongCommaFields_InvalidDates(t *testing.T) {
	in := []string{"1", "2", "3", "4", "5", "A", "B", "C", "notadate", "alsonot"}
	if out, ok := RepairOverlongCommaFields(in); ok || out != nil {
		t.Fatalf("expected failure due to invalid dates, got %v %v", ok, out)
	}
}

// TestRepairOverlongCommaFields_EmptyDatePlaceholders exercises the path where
// the trailing date fields are empty strings (allowed).
func TestRepairOverlongCommaFields_EmptyDatePlaceholders(t *testing.T) {
	in := []string{"1", "2", "3", "True", "100", "Last", "First, M.", "Main st 5", "", ""}
	out, ok := RepairOverlongCommaFields(in)
	if !ok {
		t.Fatalf("expected repair with empty date placeholders")
	}
	if len(out) != 9 || out[7] != "" || out[8] != "" {
		t.Fatalf("unexpected repaired record: %v", out)
	}
}

// TestDateRe verifies the date pattern matcher used by the repair logic.
func TestDateRe(t *testing.T) {
	cases := []struct {
		in   string
		want bool
	}{
		{"01.01.2020", true},
		{"1.1.2020", false},
		{"2020-01-01", false},
	}
	for _, c := range cases {
		got := DateRe.MatchString(c.in)
		if got != c.want {
			t.Errorf("%q -> %v want %v", c.in, got, c.want)
		}
	}
}

// --- ParseSpaceSeparatedRow branches ---

// Triggers: if len(rest) == 0 { return nil, false }
func TestParseSpaceSeparatedRow_NoRestAfterCoreFields(t *testing.T) {
	// 6 tokens total so we pass the initial len(toks) < 6 guard.
	// The last token is a date, which gets peeled off -> len(toks) == 5.
	// That leaves rest := toks[5:] with length 0 and hits the early return.
	line := `PCV TYP VZTAH AKT IČO 01.01.2020`
	if out, ok := ParseSpaceSeparatedRow(line); ok || out != nil {
		t.Fatalf("expected not ok due to empty rest; got ok=%v out=%v", ok, out)
	}
}

// Covers evaluation of: if len(out) != 9 { ... } in the success path.
// (The condition is false by construction, but the line is executed.)
func TestParseSpaceSeparatedRow_LenOutCheckCovered(t *testing.T) {
	line := `123 4 5 True 100 "Name" "Addr"`
	out, ok := ParseSpaceSeparatedRow(line)
	if !ok {
		t.Fatalf("expected ok")
	}
	if len(out) != 9 {
		t.Fatalf("expected len(out)=9, got %d (%v)", len(out), out)
	}
}

// --- RepairOverlongCommaFields branches ---

// Triggers: if !(d1 == "" || DateRe.MatchString(strings.TrimSpace(d1))) { return nil, false }
func TestRepairOverlongCommaFields_InvalidD1(t *testing.T) {
	// len(fields) > 9 so we get past the first guard.
	// Make d2 valid (empty) but d1 invalid ("not-a-date") to hit the branch.
	in := []string{"1", "2", "3", "True", "100", "Doe", "John, Jr.", "Main st 5", "not-a-date", ""}
	if out, ok := RepairOverlongCommaFields(in); ok || out != nil {
		t.Fatalf("expected failure due to invalid d1; got ok=%v out=%v", ok, out)
	}
}

// Covers evaluation of: if len(out) != 9 { ... } in the success path.
// (Again, condition is false but the line is executed.)
func TestRepairOverlongCommaFields_LenOutCheckCovered(t *testing.T) {
	in := []string{"1", "2", "3", "True", "100", "Last", "First, M.", "Main st 5", "01.01.2020", "02.01.2020"}
	out, ok := RepairOverlongCommaFields(in)
	if !ok {
		t.Fatalf("expected repair to succeed")
	}
	if len(out) != 9 {
		t.Fatalf("expected len(out)=9, got %d (%v)", len(out), out)
	}
}
