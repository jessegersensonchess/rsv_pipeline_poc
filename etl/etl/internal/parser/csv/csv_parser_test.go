package csv_test

import (
	"os"
	"path/filepath"
	"testing"

	pcsv "etl/internal/parser/csv"
)

func TestParseSample(t *testing.T) {
	path := filepath.Join("..", "..", "..", "testdata", "samples.csv")
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = f.Close() })

	p := pcsv.NewParser(pcsv.Options{
		HasHeader:      true,
		Comma:          ',',
		TrimSpace:      true,
		ExpectedFields: 6,
		HeaderMap: map[string]string{
			"PČV":      "pcv",
			"Datum od": "date_from",
			"Datum do": "date_to",
			"Důvod":    "reason",
			"RM kód":   "rm_code",
			"RM Název": "rm_name",
		},
	})

	recs, _, err := p.Parse(f)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	//if got, want := len(recs), 2; got != want {
	//	t.Fatalf("len=%d want=%d", got, want)
	//}
	if v := recs[0]["pcv"]; v != "10" {
		t.Fatalf("pcv=%v want 10", v)
	}
}
