package vehicletech

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"csvloader/internal/config"
	"csvloader/internal/db"
)

// TestImportVehicleTech_HappyPath_HeaderPresent_NoData
// Verifies that ImportVehicleTech succeeds when the CSV contains a valid header
// (including PČV and Status) but no data rows. The pipeline constructor is hooked
// to inject a no-op backend and deterministic worker counts.
func TestImportVehicleTech_HappyPath_HeaderPresent_NoData(t *testing.T) {
	//	t.Parallel()

	tmp := t.TempDir()
	path := filepath.Join(tmp, "veh.csv")
	touchFile(t, path, []byte("Status,PČV,foo\n"))

	// Run in tmp for hermetic FS behavior
	withChdir(t, tmp, func() {
		// >>> Critical: per-test skipped root <<<
		oldRoot := skippedRoot
		skippedRoot = filepath.Join(tmp, "skipped_"+t.Name())
		t.Cleanup(func() { skippedRoot = oldRoot })

		cfg := &config.Config{DBDriver: "postgres", Workers: 1, BatchSize: 10}

		orig := newVehicleTechPipelineHook
		t.Cleanup(func() { newVehicleTechPipelineHook = orig })
		newVehicleTechPipelineHook = func(ctx context.Context, cfg *config.Config, factory db.DBFactory, headers []string, pcvIdx, statusIdx int) *VehicleTechPipeline {
			p := newVehicleTechPipeline(ctx, cfg, factory, headers, pcvIdx, statusIdx)
			p.backendFactory = func(context.Context) (WriterBackend, error) { return writerBackendNoop{}, nil }
			p.initialWriters, p.maxWriters = 1, 1
			p.parserWorkers, p.encoderWorkers = 1, 1
			return p
		}

		factory := func(context.Context) (db.DB, error) { return fakeDB{}, nil }

		if err := ImportVehicleTech(context.Background(), cfg, factory, path); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}

// TestImportVehicleTech_HeaderMissingPCV_Errors
// Ensures ImportVehicleTech validates the header and fails when PČV is absent.
func TestImportVehicleTech_HeaderMissingPCV_Errors(t *testing.T) {
	t.Parallel()

	tmp := t.TempDir()
	path := filepath.Join(tmp, "veh_bad.csv")
	touchFile(t, path, []byte("Status,NOPE,foo\n"))

	cfg := &config.Config{DBDriver: "postgres", Workers: 1, BatchSize: 10}
	factory := func(context.Context) (db.DB, error) { return fakeDB{}, nil }

	orig := newVehicleTechPipelineHook
	t.Cleanup(func() { newVehicleTechPipelineHook = orig })
	newVehicleTechPipelineHook = func(ctx context.Context, cfg *config.Config, factory db.DBFactory, headers []string, pcvIdx, statusIdx int) *VehicleTechPipeline {
		p := newVehicleTechPipeline(ctx, cfg, factory, headers, pcvIdx, statusIdx)
		p.backendFactory = func(context.Context) (WriterBackend, error) { return writerBackendNoop{}, nil }
		return p
	}

	err := ImportVehicleTech(context.Background(), cfg, factory, path)
	if err == nil || !strings.Contains(err.Error(), "PČV column not found") {
		t.Fatalf("expected missing PČV error, got %v", err)
	}
}
