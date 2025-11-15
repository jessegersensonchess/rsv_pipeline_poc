package xmlparser

import "etl/pkg/records"

// Record is the canonical map representation emitted by the XML parser.
// It mirrors pkg/records.Record for convenience; we convert explicitly
// to avoid importing cycles from internal packages.
type Record map[string]any

// ToPkgRecord converts a local Record to the public pkg/records.Record.
func ToPkgRecord(r Record) records.Record {
	out := make(records.Record, len(r))
	for k, v := range r {
		out[k] = v
	}
	return out
}

// FromPkgRecord converts pkg/records.Record to a local Record.
func FromPkgRecord(r records.Record) Record {
	out := make(Record, len(r))
	for k, v := range r {
		out[k] = v
	}
	return out
}
