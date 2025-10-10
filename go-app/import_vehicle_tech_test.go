package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestImportVehicleTech_WithMockDB(t *testing.T) {
	tmp := t.TempDir()
	// Header: include Status and PČV so the extractor has easy work
	csv := "Status,PČV,ColA,ColB\n" +
		"OK,10001,a,b\n" +
		"OK,10002,c,d\n"
	path := filepath.Join(tmp, "veh.csv")
	if err := os.WriteFile(path, []byte(csv), 0o644); err != nil {
		t.Fatal(err)
	}

	sharedDB := newMockDB() // returned for both ensure-table and worker
	factory := func(ctx context.Context) (DB, error) { return sharedDB, nil }

	cfg := &Config{
		BatchSize:      1, // force multiple CopyInto calls
		Workers:        1, // deterministic
		UnloggedTables: false,
		DBDriver:       "postgres", // affects ensureVehicleTechTable DDL
	}

	if err := importVehicleTech(context.Background(), cfg, factory, path); err != nil {
		t.Fatalf("importVehicleTech err: %v", err)
	}

	// ensureVehicleTechTable should have executed CREATE TABLE (pg)
	foundCreate := false
	for _, q := range sharedDB.execs {
		if strings.Contains(strings.ToLower(q), "create table if not exists vehicle_tech") {
			foundCreate = true
			break
		}
	}
	if !foundCreate {
		t.Fatalf("expected CREATE TABLE exec")
	}

	// Worker should have begun a tx and committed with 2 rows copied
	if sharedDB.beginCount != 1 {
		t.Fatalf("expected 1 BeginTx, got %d", sharedDB.beginCount)
	}
	tx := sharedDB.tx
	if tx == nil || !tx.committed {
		t.Fatalf("transaction not committed")
	}
	if got := len(tx.rows); got != 2 {
		t.Fatalf("expected 2 inserted rows, got %d", got)
	}
	// Basic shape of inserted rows: [pcv, payloadJSON]
	row := tx.rows[0]
	if len(row) != 2 {
		t.Fatalf("row shape mismatch: %#v", row)
	}
	if row[0].(int64) != 10001 && row[0].(int) != 10001 { // tolerate platform int vs int64 in tests
		t.Fatalf("pcv not captured correctly: %#v", row[0])
	}
}
