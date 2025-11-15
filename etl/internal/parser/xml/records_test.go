package xmlparser

import (
	"etl/pkg/records"
	"testing"
)

func TestRecordConversions(t *testing.T) {
	r := Record{"a": 1, "b": "x"}
	pr := ToPkgRecord(r)
	if pr["b"] != "x" {
		t.Fatalf("bad convert to pkg")
	}
	back := FromPkgRecord(records.Record{"z": nil})
	if _, ok := back["z"]; !ok {
		t.Fatalf("missing z")
	}
}
