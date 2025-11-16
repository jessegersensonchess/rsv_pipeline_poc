package builtin

import (
	"etl/pkg/records"
	"strconv"
	"time"
)

type Coerce struct {
	Types  map[string]string // field -> one of: int, bool, date, string
	Layout string            // date layout
}

func (c Coerce) Apply(in []records.Record) []records.Record {
	for _, r := range in {
		if len(c.Types) == 0 {
			return in
		}
		for field, typ := range c.Types {
			v, ok := r[field]
			if !ok || v == nil {
				continue
			}
			s, isStr := v.(string)
			if !isStr {
				continue
			}
			switch typ {
			case "int":
				if i, err := strconv.Atoi(s); err == nil {
					r[field] = i
				}
			case "bool":
				if b, err := strconv.ParseBool(s); err == nil {
					r[field] = b
				}
			case "date":
				if t, err := time.Parse(c.Layout, s); err == nil {
					r[field] = t
				}
			case "string":
				// already string
			}
		}
	}
	return in
}
