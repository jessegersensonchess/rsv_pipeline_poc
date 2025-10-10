package main

import (
	"context"
	"errors"
	"sync"
)

// ---------------- Mock for SmallDB (ownership import path) ----------------

// replace the mockSmallDB struct + CopyOwnership with this

type mockSmallDB struct {
	mu            sync.Mutex
	created       bool
	copiedRows    [][]interface{} // store rows directly
	insertedCount int             // convenient counter
	closed        bool
	failCreate    error
	failCopy      error
}

func (m *mockSmallDB) CopyOwnership(ctx context.Context, records [][]interface{}) error {
	if m.failCopy != nil {
		return m.failCopy
	}
	m.mu.Lock()
	m.copiedRows = append(m.copiedRows, records...) // <-- keep [][]interface{} intact
	m.insertedCount += len(records)
	m.mu.Unlock()
	return nil
}

func (m *mockSmallDB) Close(ctx context.Context) error {
	m.mu.Lock()
	m.closed = true
	m.mu.Unlock()
	return nil
}

func (m *mockSmallDB) CreateOwnershipTable(ctx context.Context) error {
	if m.failCreate != nil {
		return m.failCreate
	}
	m.mu.Lock()
	m.created = true
	m.mu.Unlock()
	return nil
}

func flatten(in [][]interface{}) []interface{} {
	out := make([]interface{}, 0, len(in))
	for _, r := range in {
		out = append(out, r)
	}
	return out
}

// ---------------- Mock for DB/Tx (vehicle_tech path) ----------------

type mockDB struct {
	mu         sync.Mutex
	execs      []string
	beginCount int
	closed     bool
	failExec   error
	failBegin  error
	tx         *mockTx
}

func newMockDB() *mockDB {
	return &mockDB{}
}

func (m *mockDB) Exec(ctx context.Context, q string, args ...any) error {
	if m.failExec != nil {
		return m.failExec
	}
	m.mu.Lock()
	m.execs = append(m.execs, q)
	m.mu.Unlock()
	return nil
}

func (m *mockDB) BeginTx(ctx context.Context) (Tx, error) {
	if m.failBegin != nil {
		return nil, m.failBegin
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.beginCount++
	m.tx = &mockTx{}
	return m.tx, nil
}

func (m *mockDB) Close(ctx context.Context) error {
	m.mu.Lock()
	m.closed = true
	m.mu.Unlock()
	return nil
}

type mockTx struct {
	mu         sync.Mutex
	execs      []string
	rows       [][]interface{}
	committed  bool
	rolled     bool
	failCopy   error
	failExec   error
	failCommit error
}

func (t *mockTx) Exec(ctx context.Context, q string, args ...any) error {
	if t.failExec != nil {
		return t.failExec
	}
	t.mu.Lock()
	t.execs = append(t.execs, q)
	t.mu.Unlock()
	return nil
}

func (t *mockTx) CopyInto(ctx context.Context, table string, columns []string, rows [][]interface{}) (int64, error) {
	if t.failCopy != nil {
		return 0, t.failCopy
	}
	t.mu.Lock()
	t.rows = append(t.rows, rows...)
	t.mu.Unlock()
	return int64(len(rows)), nil
}

func (t *mockTx) Commit(ctx context.Context) error {
	if t.failCommit != nil {
		return t.failCommit
	}
	t.mu.Lock()
	t.committed = true
	t.mu.Unlock()
	return nil
}

func (t *mockTx) Rollback(ctx context.Context) error {
	t.mu.Lock()
	t.rolled = true
	t.mu.Unlock()
	return nil
}

// Helpful error for tests
var errBoom = errors.New("boom")
