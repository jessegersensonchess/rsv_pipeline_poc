// Package builtin contains simple, reusable transformers used in the ETL.
package builtin

import "etl/pkg/records"

// Require removes any record missing a value for all specified fields.
type Require struct {
	Fields []string
}

// Apply returns a filtered slice containing only records that
// have all required fields present and non-empty.
func (r Require) Apply(in []records.Record) []records.Record {
	out := in[:0]
	for _, rec := range in {
		ok := true
		for _, f := range r.Fields {
			v, exists := rec[f]
			if !exists || v == nil || v == "" {
				ok = false
				break
			}
		}
		if ok {
			out = append(out, rec)
		}
	}
	return out
}
