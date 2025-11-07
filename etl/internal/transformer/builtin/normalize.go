package builtin

import (
	"strings"
	"etl/pkg/records"
)

type Normalize struct{}

func (Normalize) Apply(in []records.Record) []records.Record {
	for _, r := range in {
		for k, v := range r {
			if s, ok := v.(string); ok {
				s = strings.TrimSpace(strings.ReplaceAll(s, " ", " "))
				r[k] = s
			}
		}
	}
	return in
}
