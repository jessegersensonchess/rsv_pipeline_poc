package vehicletech

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"csvloader/internal/db"
)

// ===========================
// Shared fakes and test utils
// ===========================
//
// Rationale:
//   Multiple tests need the same small DB fake and filesystem helpers.
//   Centralizing them avoids redeclaration conflicts and keeps each test
//   file focused on behavior instead of plumbing.

// fakeDB is a minimal DB that satisfies db.DB but never touches a real socket.
// It’s used anywhere we need a DBFactory to succeed without side-effects.
type fakeDB struct{}

func (fakeDB) Exec(context.Context, string, ...any) error { return nil }
func (fakeDB) BeginTx(context.Context) (db.Tx, error)     { return nil, nil }
func (fakeDB) Close(context.Context) error                { return nil }

// backendCapture implements WriterBackend and records behavior.
// It treats (pcv>0 && payload != nil) as "insert", else calls addSkip.
type backendCapture struct {
	seenLogEvery int
	insertCount  int
	err          error
}

func (b *backendCapture) Write(_ context.Context, ch <-chan encodedJob, addSkip func(string, int, string, string), logEvery int) (int, error) {
	b.seenLogEvery = logEvery
	for j := range ch {
		if j.pcv > 0 && len(j.payload) > 0 {
			b.insertCount++
		} else {
			addSkip("bad", j.lineNum, j.pcvField, j.raw)
		}
	}
	return b.insertCount, b.err
}

// withChdir temporarily changes the working directory for a test and restores it.
func withChdir(t *testing.T, dir string, f func()) {
	t.Helper()
	old, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	if err := os.Chdir(dir); err != nil {
		t.Fatalf("chdir(%s): %v", dir, err)
	}
	defer func() { _ = os.Chdir(old) }()
	f()
}

// csvNameFor mirrors writerFn’s filename format for deterministic assertions.
func csvNameFor(id int) string {
	return "skipped_vehicle_tech_w" + strconvItoa(id) + ".csv"
}

// strconvItoa is a tiny local helper to avoid importing strconv everywhere.
func strconvItoa(i int) string {
	const digits = "0123456789"
	if i == 0 {
		return "0"
	}
	var b [20]byte
	pos := len(b)
	for n := i; n > 0; n /= 10 {
		pos--
		b[pos] = digits[n%10]
	}
	return string(b[pos:])
}

// touchDir ensures a directory exists in the CWD.
func touchDir(t *testing.T, path string) {
	t.Helper()
	if err := os.MkdirAll(path, 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", path, err)
	}
}

// touchFile writes a file with content in the CWD.
func touchFile(t *testing.T, path string, content []byte) {
	t.Helper()
	dir := filepath.Dir(path)
	if dir != "." && dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", dir, err)
		}
	}
	if err := os.WriteFile(path, content, 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}
