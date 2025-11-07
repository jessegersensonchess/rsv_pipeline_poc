package transformer

import "etl/pkg/records"

type Transformer interface { Apply([]records.Record) []records.Record }

// Chain is an ordered list of transformers.
type Chain []Transformer

func (c Chain) Apply(in []records.Record) []records.Record {
	out := in
	for _, t := range c { out = t.Apply(out) }
	return out
}
