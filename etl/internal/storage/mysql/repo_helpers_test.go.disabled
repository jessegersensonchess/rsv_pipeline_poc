// Package mysql contains tests for helper utilities used by the MySQL adapter.
package mysql

import (
	"strings"
	"testing"
)

// TestMyIdent verifies that myIdent correctly backtick-quotes identifiers and
// escapes backticks by doubling them.
func TestMyIdent(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		{"simple", "`simple`"},
		{"hr", "`hr`"},
		{"tick`name", "`tick``name`"},
		{"weird``x", "`weird````x`"},
	}
	for _, tc := range cases {
		if got := myIdent(tc.in); got != tc.want {
			t.Fatalf("myIdent(%q) = %q; want %q", tc.in, got, tc.want)
		}
	}
}

// TestMyFQN verifies that myFQN correctly quotes schema-qualified names using
// backtick-quoted identifier segments.
func TestMyFQN(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		{"table", "`table`"},
		{"hr.table", "`hr`.`table`"},
		{"sales.q4.table", "`sales`.`q4`.`table`"},
	}
	for _, tc := range cases {
		if got := myFQN(tc.in); got != tc.want {
			t.Fatalf("myFQN(%q) = %q; want %q", tc.in, got, tc.want)
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
		{[]string{"id"}, "T.`id` = S.`id`"},
		{[]string{"pcv", "date_from"}, "T.`pcv` = S.`pcv` AND T.`date_from` = S.`date_from`"},
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
	setParts := []string{"pcv = VALUES(pcv)", "date_from = VALUES(date_from)", "name = VALUES(name)"}
	keys := []string{"pcv", "date_from"}
	got := filterConflictKeys(setParts, keys)
	joined := strings.Join(got, ",")
	if strings.Contains(joined, "pcv = ") || strings.Contains(joined, "date_from = ") {
		t.Fatalf("keys should be filtered out, got %q", joined)
	}
	if !strings.Contains(joined, "name = VALUES(name)") {
		t.Fatalf("expected non-key update to remain, got %q", joined)
	}
}
