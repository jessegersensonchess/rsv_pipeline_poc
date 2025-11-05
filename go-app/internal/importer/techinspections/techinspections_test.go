package techinspections

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
// Fakes + helpers
//

// fakeRecorder is a threadsafe sink the fake DBs write into.
// It lets tests assert how many rows reached the DB layer across workers/batches.
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

// fakeSmallDB implements db.SmallDB used by the importer.
// All instances returned by the factory share a single recorder so we can assert global effects.
type fakeSmallDB struct {
	rec *fakeRecorder
}

// CreateTechInspectionsTable records that the importer ensured the table exists.
func (f *fakeSmallDB) CreateTechInspectionsTable(ctx context.Context) error {
	atomic.AddInt32(&f.rec.tablesCreated, 1)
	return nil
}

// CopyTechInspections collects batch sizes so tests can assert inserts.
func (f *fakeSmallDB) CopyTechInspections(ctx context.Context, rows [][]interface{}) error {
	f.rec.addBatch(len(rows))
	return nil
}

// CopyOwnership is part of db.SmallDB in your codebase; provide a no-op to satisfy the interface.
func (f *fakeSmallDB) CopyOwnership(ctx context.Context, rows [][]interface{}) error { return nil }

// If your db.SmallDB also includes CreateOwnershipTable (many versions do), include a stub.
// This is harmless even if the interface doesn’t require it.
func (f *fakeSmallDB) CreateOwnershipTable(ctx context.Context) error { return nil }

// CreateRSVZpravyTable is part of db.SmallDB; this fake just satisfies the interface.
func (f *fakeSmallDB) CreateRSVZpravyTable(ctx context.Context) error {
	return nil
}

// CopyRSVZpravy is part of db.SmallDB; this fake just satisfies the interface.
func (f *fakeSmallDB) CopyRSVZpravy(ctx context.Context, rows [][]interface{}) error {
	return nil
}

// Close is a no-op for the fake.
func (f *fakeSmallDB) Close(ctx context.Context) error { return nil }

// makeFactory builds a db.SmallDBFactory that returns distinct fakeSmallDB instances
// all pointing at the same recorder.
func makeFactory(rec *fakeRecorder) db.SmallDBFactory {
	return func(ctx context.Context) (db.SmallDB, error) {
		return &fakeSmallDB{rec: rec}, nil
	}
}

// writeTempCSV writes s to a temp file and returns the path.
func writeTempCSV(t *testing.T, s string) string {
	t.Helper()
	dir := t.TempDir()
	p := filepath.Join(dir, "tech.csv")
	if err := os.WriteFile(p, []byte(s), 0o644); err != nil {
		t.Fatalf("write temp csv: %v", err)
	}
	return p
}

// cfg returns a small test config with 2 workers and tiny batches to exercise batching.
// Note: keep only fields your Config actually has (Workers, BatchSize, …).
func cfg() *config.Config {
	return &config.Config{
		Workers:   2,
		BatchSize: 3,
	}
}

//
// Tests
//

// TestImport_InsertsAllValidRecords verifies the happy path: a strictly valid,
// single-line-per-record CSV is fully imported.
//
// Idea:
//
//	Given a header and two well-formed records, the importer should parse both
//	and insert exactly two rows into the DB.
//
// Assertions:
//   - recorder.totalRows == 2
//   - CreateTechInspectionsTable is invoked at least once (preflight create)
func TestImport_InsertsAllValidRecords(t *testing.T) {
	const csv = `PČV,Typ,Stav,Kód STK,Název STK,Platnost od,Platnost do,Číslo protokolu,Aktuální
9727714,E - Evidenční,Nezjištěno,3841,"NÁVSÍ 976, 739 92, NÁVSÍ",01.02.2006,01.02.2006,,True
9652252,P - Pravidelná,A,3609,"NA RYBÁRNĚ 203/5, 50002, HRADEC KRÁLOVÉ",28.12.2011,28.12.2013,CZ-3609-11-12-1081,False
`
	rec := &fakeRecorder{}
	path := writeTempCSV(t, csv)

	if err := ImportTechInspectionsParallel(context.Background(), cfg(), makeFactory(rec), path); err != nil {
		t.Fatalf("import error: %v", err)
	}

	if got, want := rec.totalRows, 2; got != want {
		t.Fatalf("inserted rows = %d, want %d", got, want)
	}
	if atomic.LoadInt32(&rec.tablesCreated) < 1 {
		t.Fatalf("expected CreateTechInspectionsTable to be called at least once")
	}
}

// TestImport_RepairsDoubleQuotedTextField checks the importer-specific scrubber
// that normalizes fields like ,""TEXT"" -> ,"TEXT".
//
// Idea:
//
//	When a field is entirely wrapped in a doubled-quote pair (the dataset’s
//	recurring defect), the importer should normalize it in-memory and proceed.
//
// Assertions:
//   - recorder.totalRows == 1 (record is accepted)
//   - No panics/exceptions occur
func TestImport_RepairsDoubleQuotedTextField(t *testing.T) {
	const csv = `PČV,Typ,Stav,Kód STK,Název STK,Platnost od,Platnost do,Číslo protokolu,Aktuální
10125950,P - Pravidelná,A,350400015,""ADMINISTRATIVNÍ OMEZENÍ - NOVÁ VOZIDLA"",26.01.2011,26.01.2013,,False
`
	rec := &fakeRecorder{}
	path := writeTempCSV(t, csv)

	if err := ImportTechInspectionsParallel(context.Background(), cfg(), makeFactory(rec), path); err != nil {
		t.Fatalf("import error: %v", err)
	}
	if got, want := rec.totalRows, 1; got != want {
		t.Fatalf("inserted rows = %d, want %d", got, want)
	}
}

// TestImport_HandlesMultilineField ensures a quoted field containing a newline
// is correctly accumulated and parsed as a single record.
//
// Idea:
//
//	CSV permits newlines inside quoted fields. We simulate a record whose
//	Název STK spans two physical lines, still producing 9 fields.
//
// Assertions:
//   - recorder.totalRows == 1 (the single logical record is accepted)
//   - No rows are dropped/skipped
func TestImport_HandlesMultilineField(t *testing.T) {
	// The quoted field contains a literal newline between address parts.
	const csv = "PČV,Typ,Stav,Kód STK,Název STK,Platnost od,Platnost do,Číslo protokolu,Aktuální\r\n" +
		"1005104,P - Pravidelná,A,3708,\"Brněnská 74,\r\n693 01, Hustopeče\",18.08.2004,18.08.2006,,True\r\n"

	rec := &fakeRecorder{}
	path := writeTempCSV(t, csv)

	if err := ImportTechInspectionsParallel(context.Background(), cfg(), makeFactory(rec), path); err != nil {
		t.Fatalf("import error: %v", err)
	}
	if got, want := rec.totalRows, 1; got != want {
		t.Fatalf("inserted rows = %d, want %d", got, want)
	}
}

// TestImport_DropsUnrecoverableFragment verifies that if a physical chunk cannot
// be repaired into a valid 9-field record (even after normalization), it is
// dropped gracefully while valid neighbors still import.
//
// Idea:
//
//	Feed an unrecoverable line (e.g., missing several commas) followed by a
//	valid record. The importer should discard the bad fragment and import the
//	good record.
//
// Assertions:
//   - recorder.totalRows == 1 (only the valid record is inserted)
//   - The importer returns nil (non-fatal data quality issue)
func TestImport_DropsUnrecoverableFragment(t *testing.T) {
	const csv = `PČV,Typ,Stav,Kód STK,Název STK,Platnost od,Platnost do,Číslo protokolu,Aktuální
12345 BROKEN RECORD WITH NO COMMAS OR QUOTES
9652252,P - Pravidelná,A,3609,"NA RYBÁRNĚ 203/5, 50002, HRADEC KRÁLOVÉ",28.12.2011,28.12.2013,CZ-3609-11-12-1081,False
`
	rec := &fakeRecorder{}
	path := writeTempCSV(t, csv)

	if err := ImportTechInspectionsParallel(context.Background(), cfg(), makeFactory(rec), path); err != nil {
		t.Fatalf("import error: %v", err)
	}
	if got, want := rec.totalRows, 1; got != want {
		t.Fatalf("inserted rows = %d, want %d", got, want)
	}
}
