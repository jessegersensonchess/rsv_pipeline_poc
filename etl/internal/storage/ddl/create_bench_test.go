package ddl

import (
	"strconv"
	"testing"
)

/*
Micro-benchmarks focus on allocation and string-join costs for common shapes.
*/

// BenchmarkQuoteFQN measures FQN quoting and escaping.
func BenchmarkQuoteFQN(b *testing.B) {
	inputs := []string{
		"tbl",
		"sch.tbl",
		`sch"name.tbl"name`,
		"a.b.c",
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		for _, s := range inputs {
			_ = quoteFQN(s)
		}
	}
}

// BenchmarkBuildCreateTableSQL_Small measures a small 3-column table.
func BenchmarkBuildCreateTableSQL_Small(b *testing.B) {
	td := TableDef{
		FQN: "app.small",
		Columns: []ColumnDef{
			{Name: "id", SQLType: "INTEGER", Nullable: false},
			{Name: "ts", SQLType: "TIMESTAMP", Default: "CURRENT_TIMESTAMP", Nullable: false},
			{Name: "payload", SQLType: "TEXT", Nullable: true},
		},
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := BuildCreateTableSQL(td); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkBuildCreateTableSQL_Wide measures a wide table (e.g., 64 columns).
func BenchmarkBuildCreateTableSQL_Wide(b *testing.B) {
	const ncol = 64
	td := TableDef{FQN: "app.wide"}
	td.Columns = make([]ColumnDef, ncol)
	for i := 0; i < ncol; i++ {
		td.Columns[i] = ColumnDef{
			Name:     "c" + strconv.Itoa(i),
			SQLType:  "TEXT",
			Default:  "",
			Nullable: i%3 != 0, // vary NOT NULL a bit
		}
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := BuildCreateTableSQL(td); err != nil {
			b.Fatal(err)
		}
	}
}
