package transformer

import "etl/pkg/records"

type Transformer interface {
	Apply([]records.Record) []records.Record
}

// Chain is an ordered list of transformers.
type Chain []Transformer

func (c Chain) Apply(in []records.Record) []records.Record {
	if len(c) == 0 {
		return in
	}

	out := in
	for _, t := range c {
		if t == nil {
			continue
		}
		out = t.Apply(out)
	}
	return out
}
