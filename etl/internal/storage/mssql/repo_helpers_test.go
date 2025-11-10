// Package mssql contains tests for helper utilities used by the MSSQL adapter.
package mssql

import (
	"strings"
	"testing"
)

// TestMsIdent verifies that msIdent properly brackets SQL Server identifiers
// and escapes closing brackets to avoid syntax errors and injection issues.
func TestMsIdent(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		{"simple", "[simple]"},
		{"dbo", "[dbo]"},
		{"brack]et", "[brack]]et]"},
		{`weird]]name`, `[weird]]]]name]`},
	}
	for _, tc := range cases {
		if got := msIdent(tc.in); got != tc.want {
			t.Fatalf("msIdent(%q) = %q; want %q", tc.in, got, tc.want)
		}
	}
}

// TestMsFQN verifies that msFQN correctly quotes schema-qualified names using
// bracketed identifier segments, preserving multi-part names.
func TestMsFQN(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		{"table", "[table]"},
		{"dbo.table", "[dbo].[table]"},
		{"sales.q4.table", "[sales].[q4].[table]"},
	}
	for _, tc := range cases {
		if got := msFQN(tc.in); got != tc.want {
			t.Fatalf("msFQN(%q) = %q; want %q", tc.in, got, tc.want)
		}
	}
}

// TestBuildDeleteCondition ensures that the join predicate for DELETE ... JOIN
// is constructed correctly from the provided key column list.
func TestBuildDeleteCondition(t *testing.T) {
	cases := []struct {
		keys []string
		want string
	}{
		{nil, ""},
		{[]string{"id"}, "T.[id] = S.[id]"},
		{[]string{"pcv", "date_from"}, "T.[pcv] = S.[pcv] AND T.[date_from] = S.[date_from]"},
	}
	for _, tc := range cases {
		got := buildDeleteCondition(tc.keys)
		if len(tc.keys) == 0 && got != "" {
			t.Fatalf("expected empty condition for no keys; got %q", got)
		}
		if len(tc.keys) > 0 && got != tc.want {
			t.Fatalf("condition = %q; want %q", got, tc.want)
		}
	}
}

// TestFilterConflictKeys validates that conflict key columns are removed from
// the generated SET clause parts, leaving only non-key updates.
func TestFilterConflictKeys(t *testing.T) {
	setParts := []string{"pcv = EXCLUDED.pcv", "date_from = EXCLUDED.date_from", "name = EXCLUDED.name"}
	keys := []string{"pcv", "date_from"}
	got := filterConflictKeys(setParts, keys)
	joined := strings.Join(got, ",")
	if strings.Contains(joined, "pcv = ") || strings.Contains(joined, "date_from = ") {
		t.Fatalf("keys should be filtered out, got %q", joined)
	}
	if !strings.Contains(joined, "name = EXCLUDED.name") {
		t.Fatalf("expected non-key update to remain, got %q", joined)
	}
}
