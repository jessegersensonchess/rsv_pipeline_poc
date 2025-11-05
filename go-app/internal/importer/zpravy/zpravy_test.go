package rsvzpravy

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"csvloader/internal/config"
	"csvloader/internal/db"
)

//
// --- Fakes + helpers (same pattern as techinspections) ---
//

// fakeRecorder tracks insert and create events.
type fakeRecorder struct {
	mu            sync.Mutex
	totalRows     int
	batchCounts   []int
	tablesCreated int32
}

func (r *fakeRecorder) addBatch(n int) {
	r.mu.Lock()
	r.totalRows += n
	r.batchCounts = append(r.batchCounts, n)
	r.mu.Unlock()
}

// fakeSmallDB implements db.SmallDB for the RSV importer.
type fakeSmallDB struct {
	rec *fakeRecorder
}

func (f *fakeSmallDB) CreateRSVZpravyTable(ctx context.Context) error {
	atomic.AddInt32(&f.rec.tablesCreated, 1)
	return nil
}
func (f *fakeSmallDB) CopyRSVZpravy(ctx context.Context, rows [][]interface{}) error {
	f.rec.addBatch(len(rows))
	return nil
}

// Satisfy other SmallDB interface methods (no-ops)
func (f *fakeSmallDB) Close(ctx context.Context) error                      { return nil }
func (f *fakeSmallDB) CreateTechInspectionsTable(ctx context.Context) error { return nil }
func (f *fakeSmallDB) CopyTechInspections(ctx context.Context, _ [][]interface{}) error {
	return nil
}
func (f *fakeSmallDB) CreateOwnershipTable(ctx context.Context) error { return nil }
func (f *fakeSmallDB) CopyOwnership(ctx context.Context, _ [][]interface{}) error {
	return nil
}

func makeFactory(rec *fakeRecorder) db.SmallDBFactory {
	return func(ctx context.Context) (db.SmallDB, error) {
		return &fakeSmallDB{rec: rec}, nil
	}
}

func writeTempCSV(t *testing.T, s string) string {
	t.Helper()
	dir := t.TempDir()
	p := filepath.Join(dir, "rsv.csv")
	if err := os.WriteFile(p, []byte(s), 0o644); err != nil {
		t.Fatalf("write temp csv: %v", err)
	}
	return p
}

func cfg() *config.Config {
	return &config.Config{
		Workers:   2,
		BatchSize: 3,
	}
}

//
// --- Tests ---
//

// TestImport_InsertsAllValidRecords verifies the importer handles a clean CSV.
func TestImport_InsertsAllValidRecords(t *testing.T) {
	const csv = `PČV,Krátký text
12345,"Some text"
67890,"Other text"
`

	rec := &fakeRecorder{}
	path := writeTempCSV(t, csv)

	if err := ImportRSVZpravyParallel(context.Background(), cfg(), makeFactory(rec), path); err != nil {
		t.Fatalf("import error: %v", err)
	}

	if got, want := rec.totalRows, 2; got != want {
		t.Fatalf("inserted rows = %d, want %d", got, want)
	}
	if atomic.LoadInt32(&rec.tablesCreated) < 1 {
		t.Fatalf("expected CreateRSVZpravyTable to be called at least once")
	}
}

// TestImport_HandlesMissingText ensures empty text field is accepted (NULL).
func TestImport_HandlesMissingText(t *testing.T) {
	const csv = `PČV,Krátký text
99999,
`

	rec := &fakeRecorder{}
	path := writeTempCSV(t, csv)

	if err := ImportRSVZpravyParallel(context.Background(), cfg(), makeFactory(rec), path); err != nil {
		t.Fatalf("import error: %v", err)
	}
	if got, want := rec.totalRows, 1; got != want {
		t.Fatalf("inserted rows = %d, want %d", got, want)
	}
}

// TestImport_SkipsMalformedRows verifies importer drops records
// with wrong column count or non-integer PČV gracefully.
func TestImport_SkipsMalformedRows(t *testing.T) {
	const csv = `PČV,Krátký text
BROKEN_ROW
54321,"OK record"
`

	rec := &fakeRecorder{}
	path := writeTempCSV(t, csv)

	if err := ImportRSVZpravyParallel(context.Background(), cfg(), makeFactory(rec), path); err != nil {
		t.Fatalf("import error: %v", err)
	}
	if got, want := rec.totalRows, 1; got != want {
		t.Fatalf("inserted rows = %d, want %d (malformed row should be skipped)", got, want)
	}
}

// TestImport_RespectsBatching checks that small BatchSize triggers multiple Copy calls.
func TestImport_RespectsBatching(t *testing.T) {
	const csv = `PČV,Krátký text
1,a
2,b
3,c
4,d
`

	rec := &fakeRecorder{}
	c := cfg()
	c.BatchSize = 2 // force batching
	path := writeTempCSV(t, csv)

	if err := ImportRSVZpravyParallel(context.Background(), c, makeFactory(rec), path); err != nil {
		t.Fatalf("import error: %v", err)
	}

	if len(rec.batchCounts) < 2 {
		t.Fatalf("expected multiple batch inserts, got %#v", rec.batchCounts)
	}
	if got, want := rec.totalRows, 4; got != want {
		t.Fatalf("inserted rows = %d, want %d", got, want)
	}
}
