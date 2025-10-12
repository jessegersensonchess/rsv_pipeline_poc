package vehicletech

import (
	"bufio"
	"context"
	"encoding/csv"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"csvloader/internal/config"
	"csvloader/internal/db"
)

// Test_newVehicleTechPipeline_ConfigAndFactory
// Validates constructor wiring (channels, worker counts, regex) and that
// the backend factory is set for both Postgres and MSSQL branches.
func Test_newVehicleTechPipeline_ConfigAndFactory(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	headers := []string{"Status", "PČV", "foo"}
	pcvIdx, statusIdx := 1, 0
	factory := func(context.Context) (db.DB, error) { return fakeDB{}, nil }

	t.Run("postgres_branch", func(t *testing.T) {
		cfg := &config.Config{DBDriver: "postgres", Workers: 8, BatchSize: 100}
		p := newVehicleTechPipeline(ctx, cfg, factory, headers, pcvIdx, statusIdx)

		if !p.isPG {
			t.Fatalf("isPG should be true")
		}
		if p.parserWorkers != cfg.Workers || p.encoderWorkers != max(8, cfg.Workers/2) || p.initialWriters != max(8, cfg.Workers/4) {
			t.Fatalf("unexpected worker derivation: parsers=%d encoders=%d writers=%d", p.parserWorkers, p.encoderWorkers, p.initialWriters)
		}
		if got := int(p.activeWriters.Load()); got != p.initialWriters {
			t.Fatalf("activeWriters=%d want %d", got, p.initialWriters)
		}
		if !p.digitsOnly.MatchString("12345") || p.digitsOnly.MatchString("x23") {
			t.Fatalf("digitsOnly regex mismatch")
		}
		if _, err := p.backendFactory(ctx); err != nil {
			t.Fatalf("backend factory err: %v", err)
		}
	})

	t.Run("mssql_branch", func(t *testing.T) {
		cfg := &config.Config{DBDriver: "mssql", Workers: 6, BatchSize: 10}
		p := newVehicleTechPipeline(ctx, cfg, factory, headers, pcvIdx, statusIdx)
		if p.isPG {
			t.Fatalf("isPG should be false for mssql")
		}
		if _, err := p.backendFactory(ctx); err != nil {
			t.Fatalf("backend factory err: %v", err)
		}
	})
}

// Test_writerFn_ErrorPaths
// Exercises early-return error branches inside writerFn with strict isolation.
// We override skippedRoot to a unique path per subtest to avoid collisions.
func Test_writerFn_ErrorPaths(t *testing.T) {
	//nolint:tparallel // overrides package-level var 'skippedRoot'; must not be parallel
	// Do NOT run in parallel: this test overrides the global skippedRoot.
	//	t.Parallel()
	ctx := context.Background()

	cases := []struct {
		name     string
		prepare  func(t *testing.T, root string, id int) // prepare FS under this test's skippedRoot
		factory  WriterBackendFactory
		wantSub  string
		writerID int
	}{
		{
			name: "mkdirall fails when root is a file",
			prepare: func(t *testing.T, root string, _ int) {
				// Create a *file* at skippedRoot so MkdirAll(skippedRoot) fails.
				touchFile(t, root, []byte("not a dir"))
			},
			factory:  func(context.Context) (WriterBackend, error) { return &backendCapture{}, nil },
			wantSub:  "create skipped dir",
			writerID: 1,
		},
		{
			name: "create CSV fails when target path is a directory",
			prepare: func(t *testing.T, root string, id int) {
				// Make skippedRoot a directory, then also create a directory at the exact
				// file path the writer intends to create (so os.Create fails).
				touchDir(t, root)
				touchDir(t, filepath.Join(root, csvNameFor(id)))
			},
			factory:  func(context.Context) (WriterBackend, error) { return &backendCapture{}, nil },
			wantSub:  "skipped file",
			writerID: 2,
		},
		{
			name:     "backend factory error",
			prepare:  func(t *testing.T, root string, _ int) { touchDir(t, root) },
			factory:  func(context.Context) (WriterBackend, error) { return nil, os.ErrPermission },
			wantSub:  "backend",
			writerID: 3,
		},
		{
			name:     "backend write error",
			prepare:  func(t *testing.T, root string, _ int) { touchDir(t, root) },
			factory:  func(context.Context) (WriterBackend, error) { return &backendCapture{err: os.ErrClosed}, nil },
			wantSub:  "write",
			writerID: 4,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			// Subtests also must not be parallel: they each override skippedRoot.
			//			t.Parallel()

			// Unique sandbox and unique skippedRoot per case (prevents cross-test collisions).
			tmp := t.TempDir()
			withChdir(t, tmp, func() {
				oldRoot := skippedRoot
				skippedRoot = filepath.Join(tmp, "skipped_"+t.Name())
				t.Cleanup(func() { skippedRoot = oldRoot })

				cfg := &config.Config{DBDriver: "postgres", Workers: 4, BatchSize: 100}
				headers := []string{"Status", "PČV"}
				p := newVehicleTechPipeline(ctx, cfg,
					func(context.Context) (db.DB, error) { return fakeDB{}, nil },
					headers, 1, 0)

				// Make writer return quickly after setup.
				p.encodedCh = make(chan encodedJob)
				close(p.encodedCh)

				// Prepare FS under our per-test skippedRoot and inject backend behavior.
				tc.prepare(t, skippedRoot, tc.writerID)
				p.backendFactory = tc.factory

				statsCh := make(chan writerStats, 1)
				p.writeWG.Add(1)
				go p.writerFn(tc.writerID, statsCh)
				got := <-statsCh

				if got.err == nil || !strings.Contains(got.err.Error(), tc.wantSub) {
					t.Fatalf("want error containing %q; got %v", tc.wantSub, got.err)
				}
			})
		})
	}
}

// Test_writerFn_SuccessAndLogEvery
// Happy path under a per-test skippedRoot: verifies logEvery, counts, and CSV header.
func Test_writerFn_SuccessAndLogEvery(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	tmp := t.TempDir()
	withChdir(t, tmp, func() {
		oldRoot := skippedRoot
		skippedRoot = filepath.Join(tmp, "skipped_"+t.Name())
		t.Cleanup(func() { skippedRoot = oldRoot })

		cfg := &config.Config{DBDriver: "postgres", Workers: 8, BatchSize: 2000} // → logEvery=10000
		headers := []string{"Status", "PČV"}
		p := newVehicleTechPipeline(ctx, cfg, func(context.Context) (db.DB, error) { return fakeDB{}, nil }, headers, 1, 0)

		// One valid row and one skipped row.
		p.encodedCh = make(chan encodedJob, 2)
		p.encodedCh <- encodedJob{pcv: 123, pcvField: "123", payload: []byte(`{}`), lineNum: 2, raw: "ok,123"}
		p.encodedCh <- encodedJob{pcv: 0, pcvField: "", payload: nil, lineNum: 3, raw: "bad,"}
		close(p.encodedCh)

		bc := &backendCapture{}
		p.backendFactory = func(context.Context) (WriterBackend, error) { return bc, nil }

		statsCh := make(chan writerStats, 1)
		p.writeWG.Add(1)
		go p.writerFn(9, statsCh)
		got := <-statsCh

		if bc.seenLogEvery != 10000 {
			t.Fatalf("logEvery=%d want 10000", bc.seenLogEvery)
		}
		if got.inserted != 1 || got.skipped != 1 {
			t.Fatalf("writer stats: inserted=%d skipped=%d", got.inserted, got.skipped)
		}

		// Verify the skipped CSV was written under the per-test root.
		path := filepath.Join(skippedRoot, csvNameFor(9))
		f, err := os.Open(path)
		if err != nil {
			t.Fatalf("open %s: %v", path, err)
		}
		defer f.Close()
		r := csv.NewReader(f)
		hdr, err := r.Read()
		if err != nil {
			t.Fatalf("read header: %v", err)
		}
		wantHdr := []string{"reason", "line_number", "pcv_field", "raw_line"}
		if strings.Join(hdr, ",") != strings.Join(wantHdr, ",") {
			t.Fatalf("header got %v want %v", hdr, wantHdr)
		}
	})
}

// TestPipeline_Flow_WithFakeBackend
// End-to-end smoke: reader → parser → encoder → writer with an in-memory CSV
// and a counting backend. Keeps workers at 1 for determinism.
func TestPipeline_Flow_WithFakeBackend(t *testing.T) {
	ctx := context.Background()

	headers := []string{"Status", "PČV", "foo"}
	pcvIdx, statusIdx := 1, 0
	cfg := &config.Config{DBDriver: "postgres", Workers: 2, BatchSize: 10}
	factory := func(context.Context) (db.DB, error) { return fakeDB{}, nil }

	p := newVehicleTechPipeline(ctx, cfg, factory, headers, pcvIdx, statusIdx)
	p.parserWorkers, p.encoderWorkers = 1, 1
	p.initialWriters, p.maxWriters = 1, 1
	p.activeWriters.Store(1)

	fb := &backendCapture{}
	p.backendFactory = func(context.Context) (WriterBackend, error) { return fb, nil }

	body := strings.NewReader("ok,123,x\nok,,x\n")
	r := bufio.NewReader(body)

	p.startReader(r)
	p.startParsers()
	p.startEncoders()

	statsCh := make(chan writerStats, 1)
	p.startWritersAndAutoscale(statsCh)

	if _, _, _, err := p.waitCloseStats(statsCh); err != nil {
		t.Fatalf("unexpected writer error: %v", err)
	}
	if fb.insertCount != 1 {
		t.Fatalf("backend wrote %d, want 1", fb.insertCount)
	}
}
