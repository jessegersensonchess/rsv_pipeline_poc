package parser

import (
	"etl/pkg/records"
	"io"
)

type Parser interface {
	Parse(r io.Reader) ([]records.Record, int, error)
}
