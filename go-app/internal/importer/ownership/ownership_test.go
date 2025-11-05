package importer

import (
	"bufio"
	"context"
	"csvloader/internal/config"
	"csvloader/internal/csvutil"
	"csvloader/internal/db"
	"encoding/csv"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

// ---- parseRecord tests ----

func TestParseRecord_OK(t *testing.T) {
	fields := []string{"123", "2", "3", "True", "100", "ACME", "Main st 5", "01.01.2020", "02.01.2020"}
	rec, err := parseRecord(fields)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if rec.PCV != 123 || !rec.Aktualni {
		t.Fatalf("unexpected: %+v", rec)
	}
	if rec.ICO == nil || *rec.ICO != 100 {
		t.Fatalf("ico missing: %+v", rec)
	}
	if rec.Nazev == nil || *rec.Nazev != "ACME" {
		t.Fatalf("nazev mismatch")
	}
	if rec.DatumOd == nil || rec.DatumDo == nil {
		t.Fatalf("dates missing")
	}
}

func TestParseRecord_FieldErrors(t *testing.T) {
	bad := [][]string{
		{"x", "2", "3", "True", "", "", "", "", ""},           // PCV
		{"1", "x", "3", "True", "", "", "", "", ""},           // Typ
		{"1", "2", "x", "True", "", "", "", "", ""},           // Vztah
		{"1", "2", "3", "True", "", "", "", "99.99.9999", ""}, // bad DatumOd
	}
	for i, f := range bad {
		if _, err := parseRecord(f); err == nil {
			t.Fatalf("case %d expected error", i)
		}
	}
}

// ---- fakes for ImportOwnershipParallel ----

type fakeSmallDB struct {
	mu      sync.Mutex
	closed  bool
	ddlOK   bool
	batches [][][9]interface{}
}

func (f *fakeSmallDB) CreateOwnershipTable(_ context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ddlOK = true
	return nil
}
func (f *fakeSmallDB) CopyOwnership(_ context.Context, records [][]interface{}) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	// snapshot and check shape
	b := make([][9]interface{}, 0, len(records))
	for _, r := range records {
		if len(r) != 9 {
			return nil
		}
		var row [9]interface{}
		copy(row[:], r[:9])
		b = append(b, row)
	}
	f.batches = append(f.batches, b)
	return nil
}

func (f *fakeSmallDB) Close(_ context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = true
	return nil
}

func (f *fakeSmallDB) CopyTechInspections(ctx context.Context, rows [][]interface{}) error {
	// If you want visibility in those tests, you can count rows here.
	// For tests that don't exercise tech inspections, a no-op is fine.
	return nil
}

func (f *fakeSmallDB) CreateTechInspectionsTable(ctx context.Context) error { return nil }

func (f *fakeSmallDB) CopyRSVZpravy(ctx context.Context, rows [][]interface{}) error { return nil }

// CreateRSVZpravyTable is part of db.SmallDB; this fake just satisfies the interface.
func (f *fakeSmallDB) CreateRSVZpravyTable(ctx context.Context) error {
	return nil
}

func fakeFactory(f *fakeSmallDB) db.SmallDBFactory {
	return func(ctx context.Context) (db.SmallDB, error) { return f, nil }
}

func writeTempCSV(t *testing.T, header string, lines ...string) string {
	t.Helper()
	dir := t.TempDir()
	p := filepath.Join(dir, "own.csv")
	f, err := os.Create(p)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	w := csv.NewWriter(f)
	defer w.Flush()
	if header != "" {
		_ = w.Write(strings.Split(header, ","))
	}
	for _, ln := range lines {
		// We pass raw lines through the robust reader; write them as provided:
		_, _ = f.WriteString(ln + "\n")
	}
	return p
}

func TestImportOwnershipParallel_HappyPathWithBatching(t *testing.T) {
	// CSV with header and a few rows; include one malformed to exercise skip path.
	header := "pcv,typ_subjektu,vztah_k_vozidlu,aktualni,ico,nazev,adresa,datum_od,datum_do"
	ok1 := "1,2,3,True,100,Acme,Addr,01.01.2020,02.01.2020"
	ok2 := "2,2,3,False,,Foo,Bar,01.02.2020,"               // missing DatumDo
	bad := "x,2,3,True,100,Acme,Addr,01.01.2020,02.01.2020" // PCV bad => skipped

	path := writeTempCSV(t, header, ok1, ok2, bad)

	cfg := &config.Config{
		BatchSize:    1, // force multiple CopyOwnership calls
		Workers:      2,
		OwnershipCSV: path,
		SkippedDir:   filepath.Join(t.TempDir(), "skipped"),
	}
	f := &fakeSmallDB{}
	err := ImportOwnershipParallel(context.Background(), cfg, fakeFactory(f), path)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	if !f.ddlOK || !f.closed {
		t.Fatalf("expected ddl and close to be called")
	}

	if len(f.batches) == 0 {
		t.Fatalf("expected at least one batch, got 0")
	}
	inserted := 0
	for _, b := range f.batches {
		inserted += len(b)
	}
	if inserted < 1 {
		t.Fatalf("expected at least one inserted row, got %d (batches=%#v)", inserted, f.batches)
	}
	// Spot-check some row content exists and is coherent
	found := false
	for _, b := range f.batches {
		for _, r := range b {
			// expect ints in first columns and a name/adresa somewhere
			if _, ok := r[0].(int); ok {
				found = true
				break
			}
		}
	}
	if !found {
		t.Fatalf("did not find expected inserted shape in batches: %#v", f.batches)
	}

}

type fakeDB struct {
	mu   sync.Mutex
	sqls []string
	err  error
}

func (d *fakeDB) Exec(_ context.Context, q string, _ ...any) error {
	d.mu.Lock()
	d.sqls = append(d.sqls, q)
	d.mu.Unlock()
	return d.err
}
func (d *fakeDB) BeginTx(context.Context) (db.Tx, error) { return nil, nil }
func (d *fakeDB) Close(context.Context) error            { return nil }

func TestEnsureOwnershipTable_PostgresAndMSSQL(t *testing.T) {
	for _, driver := range []string{"postgres", "mssql"} {
		fd := &fakeDB{}
		if err := ensureOwnershipTable(context.Background(), fd, true, driver); err != nil {
			t.Fatalf("driver %s err: %v", driver, err)
		}
		if len(fd.sqls) == 0 {
			t.Fatalf("driver %s: expected Exec calls", driver)
		}
	}
}

func TestEnsureOwnershipTable_Unknown(t *testing.T) {
	fd := &fakeDB{}
	if err := ensureOwnershipTable(context.Background(), fd, false, "oracle"); err == nil {
		t.Fatalf("expected error for unknown driver")
	}
}

// Small sanity: verify our generated CSV is actually read as logical row(s)
func Test_readLogicalCSVLine_integration(t *testing.T) {
	in := "\"a,b\"\nzzz\n"
	r := bufio.NewReader(strings.NewReader(in))
	s, err := csvutil.ReadLogicalCSVLine(r)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if s != `"a,b"` {
		t.Fatalf("got %q", s)
	}
	// next line
	s, err = csvutil.ReadLogicalCSVLine(r)
	if err != nil || s != "zzz" {
		t.Fatalf("next %q err=%v", s, err)
	}
}

// Reference equality helper (debug)
func eq(a, b interface{}) bool { return reflect.DeepEqual(a, b) }

// Keep linter quiet about time import
var _ = time.Time{}
