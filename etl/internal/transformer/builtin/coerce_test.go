package builtin_test

import (
	"testing"
	"time"
	"etl/internal/transformer/builtin"
	"etl/pkg/records"
)

func TestCoerce(t *testing.T) {
	recs := []records.Record{{
		"pcv": "10",
		"date_from": "13.08.2018",
		"rm_code": "3203",
	}}
	c := builtin.Coerce{Types: map[string]string{"pcv":"int","rm_code":"int","date_from":"date"}, Layout: "02.01.2006"}
	out := c.Apply(recs)
	if _, ok := out[0]["pcv"].(int); !ok { t.Fatalf("pcv not int") }
	if _, ok := out[0]["rm_code"].(int); !ok { t.Fatalf("rm_code not int") }
	if _, ok := out[0]["date_from"].(time.Time); !ok { t.Fatalf("date_from not time.Time") }
}
