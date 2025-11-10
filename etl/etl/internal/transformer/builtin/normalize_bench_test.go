package builtin

import (
	"strconv"
	"testing"

	"etl/pkg/records"
)

/*
BenchmarkRequire_PassAll measures in-place filtering when all records pass.
This stresses the hot path that only does map lookups and appends.
*/
func BenchmarkRequire_PassAll(b *testing.B) {
	const (
		nrecs  = 50000
		fields = 3
	)
	data := make([]records.Record, nrecs)
	for i := 0; i < nrecs; i++ {
		data[i] = records.Record{
			"a": "x",
			"b": "y",
			"c": "z" + strconv.Itoa(i%10),
			"n": i,
		}
	}
	req := Require{Fields: []string{"a", "b", "c"}}

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		// Use the same backing array each iteration (in-place).
		_ = req.Apply(data)
	}
}

/*
BenchmarkRequire_DropHalf alternates between records that pass and records that
fail due to an empty string in a required field.
*/
func BenchmarkRequire_DropHalf(b *testing.B) {
	const nrecs = 50000
	data := make([]records.Record, nrecs)
	for i := 0; i < nrecs; i++ {
		val := "ok"
		if i%2 == 1 {
			val = "" // failing on 'b'
		}
		data[i] = records.Record{"a": "x", "b": val}
	}
	req := Require{Fields: []string{"a", "b"}}

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = req.Apply(data)
	}
}

/*
BenchmarkRequire_ManyFields tests the cost of checking a larger number of
required fields per record (map lookup loop overhead).
*/
func BenchmarkRequire_ManyFields(b *testing.B) {
	const (
		nrecs   = 20000
		nfields = 16
	)
	// Construct records with keys k0..k15; all present and non-empty.
	data := make([]records.Record, nrecs)
	for i := 0; i < nrecs; i++ {
		rec := make(records.Record, nfields)
		for k := 0; k < nfields; k++ {
			rec["k"+strconv.Itoa(k)] = "v"
		}
		data[i] = rec
	}
	var reqFields []string
	for k := 0; k < nfields; k++ {
		reqFields = append(reqFields, "k"+strconv.Itoa(k))
	}
	req := Require{Fields: reqFields}

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = req.Apply(data)
	}
}
