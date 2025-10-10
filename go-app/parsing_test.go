package main

import (
	"bufio"
	"regexp"
	"strings"
	"testing"
	"time"
)

func TestParseCSVLineLoose(t *testing.T) {
	line := `"12","34","56","True","123","ACME, a.s.",""U Lesa" 10, Praha","01.09.2025",""`
	got, err := parseCSVLineLoose(line)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if len(got) != 9 {
		t.Fatalf("want 9 fields, got %d: %#v", len(got), got)
	}
	if got[5] != "ACME, a.s." {
		t.Fatalf("name not parsed, got %q", got[5])
	}
}

func TestRepairOverlongCommaFields(t *testing.T) {
	fields := []string{
		"1", "2", "3", "True", "321",
		"ACME", "a.s.", "Praha 1",
		"01.09.2025", "", // valid dates at tail
	}
	// Overlong -> expect merge ACME + a.s. into one
	out, ok := repairOverlongCommaFields(fields)
	if !ok {
		t.Fatalf("expected repair OK")
	}
	if len(out) != 9 {
		t.Fatalf("want 9, got %d", len(out))
	}
	if out[5] != "ACME,a.s." {
		t.Fatalf("expected merged name with comma, got %q", out[5])
	}
}

// go-app/parsing_test.go

func TestParseSpaceSeparatedRow(t *testing.T) {
	line := `1 2 3 True 444 "ACME s.r.o." "Václavské nám. 1, Praha" 01.09.2025`
	got, ok := parseSpaceSeparatedRow(line)
	if !ok {
		t.Fatalf("expected ok")
	}
	if len(got) != 9 {
		t.Fatalf("want 9 fields, got %d", len(got))
	}
	if got[5] != "ACME s.r.o." {
		t.Fatalf("bad name: %q", got[5])
	}
	// Quotes are not preserved by the lexer
	if got[6] != "Václavské nám. 1, Praha" {
		t.Fatalf("bad addr: %q", got[6])
	}
}

func TestReadLogicalCSVLine_MultilineQuoted(t *testing.T) {
	in := `"h1","h2"
"foo","bar
baz"
`
	r := bufio.NewReader(strings.NewReader(in))
	// header
	if _, err := readLogicalCSVLine(r); err != nil {
		t.Fatalf("header read failed: %v", err)
	}
	// row with embedded newline inside quotes
	row, err := readLogicalCSVLine(r)
	if err != nil {
		t.Fatalf("row read failed: %v", err)
	}
	fields, err := parseCSVLineLoose(row)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	if len(fields) != 2 || fields[1] != "bar\r\nbaz" && fields[1] != "bar\nbaz" {
		t.Fatalf("embedded newline not preserved, got %#v", fields)
	}
}

func TestParseRecord_OK(t *testing.T) {
	fields := []string{"1", "2", "3", "True", "123", "ACME", "Praha", "01.09.2025", ""}
	rec, err := parseRecord(fields)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !rec.Aktualni || rec.PCV != 1 || *rec.ICO != 123 {
		t.Fatalf("parsed values mismatch: %#v", rec)
	}
	if rec.DatumOd == nil || rec.DatumDo != nil {
		t.Fatalf("dates mismatch: %#v", rec)
	}
	want, _ := time.Parse(layout, "01.09.2025")
	if !rec.DatumOd.Equal(want) {
		t.Fatalf("wrong DatumOd: %v", rec.DatumOd)
	}
}

func TestFindPCVIndexAndRowToJSON(t *testing.T) {
	hs := []string{"A", "PČV", "X"}
	if idx := findPCVIndex(hs); idx != 1 {
		t.Fatalf("want 1, got %d", idx)
	}
	fields := []string{"a", "123", "x"}
	js, err := rowToJSON(hs, fields)
	if err != nil {
		t.Fatalf("json err: %v", err)
	}
	if !strings.Contains(string(js), `"PČV":"123"`) {
		t.Fatalf("json missing pcv: %s", string(js))
	}
}

func TestExtractPCV_MisalignedAndStatus(t *testing.T) {
	headers := []string{"Status", "PČV", "Foo", "Bar"}
	// case 1: aligned
	fields := []string{"OK", "99999", "x", "y"}
	pcv, s := extractPCV(headers, fields, 1, 0, regexpDigits())
	if pcv != 99999 || s != "99999" {
		t.Fatalf("aligned parse failed, got %d from %q", pcv, s)
	}
	// case 2: misaligned tail
	fields2 := []string{"OK", "x", "y", "99999", "EXTRA"}
	pcv2, _ := extractPCV(headers, fields2, 1, 0, regexpDigits())
	if pcv2 != 99999 {
		t.Fatalf("misaligned tail parse failed, got %d", pcv2)
	}
	// case 3: scan after Status
	headers3 := []string{"Status", "Foo", "Bar", "Baz"}
	fields3 := []string{"OK", "zz", "999999", "aa"}
	pcv3, _ := extractPCV(headers3, fields3, -1, 0, regexpDigits())
	if pcv3 != 999999 {
		t.Fatalf("status-scan parse failed, got %d", pcv3)
	}
}

func regexpDigits() *regexp.Regexp {
	return regexp.MustCompile(`^\d+$`)
}
