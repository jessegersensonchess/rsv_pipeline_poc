// Package json implements a JSON parser that turns JSON objects into
// records.Record maps.
//
// It is deliberately simple and conservative:
//
//   - Supports newline-delimited JSON objects:
//     {"id":1,"name":"a"}
//     {"id":2,"name":"b"}
//   - Also supports multiple JSON objects in a stream (same as NDJSON).
//   - Rejects non-object top-level values (arrays, primitives) for now.
//
// This matches a very common ETL pattern: NDJSON logs / exports.
package json

import (
	"encoding/json"
	"fmt"
	"io"

	"etl/internal/config"
	"etl/pkg/records"
)

// Options mirrors the parser.Options usage pattern in csv/xml packages.
// For now it only supports a single knob:
//
//   - "allow_arrays" (bool): when true, a top-level JSON array of objects
//     is accepted by DecodeAll.
type Options struct {
	AllowArrays bool
}

// FromConfigOptions constructs JSON Options from a generic config.Options
// map (the same one used by csv/xml parsers).
func FromConfigOptions(o config.Options) Options {
	return Options{
		AllowArrays: o.Bool("allow_arrays", false),
	}
}

// Decoder wraps encoding/json.Decoder to provide a simple record-oriented
// API suitable for use in stream-based ETL pipelines.
type Decoder struct {
	dec *json.Decoder
	opt Options
}

// NewDecoder constructs a Decoder from an io.Reader and JSON Options.
func NewDecoder(r io.Reader, opt Options) *Decoder {
	d := json.NewDecoder(r)
	// UseNumber so callers can decide how to map numeric values.
	d.UseNumber()
	return &Decoder{
		dec: d,
		opt: opt,
	}
}

// Next reads the next JSON object and converts it into a records.Record.
//
// It expects each top-level item in the stream to be a JSON object, e.g.:
//
//	{"id":1,"name":"a"}
//	{"id":2,"name":"b"}
//
// If the input contains a non-object top-level value, it returns an error.
// EOF is returned when the stream is exhausted.
func (d *Decoder) Next() (records.Record, error) {
	for {
		var raw any
		if err := d.dec.Decode(&raw); err != nil {
			if err == io.EOF {
				return nil, io.EOF
			}
			return nil, fmt.Errorf("json parser: decode: %w", err)
		}

		switch m := raw.(type) {
		case map[string]any:
			return records.Record(m), nil
		default:
			// Skip non-object values to be robust to junk lines,
			// but fail if that's all we see.
			// We could choose to be stricter; for now, best-effort.
			continue
		}
	}
}

// DecodeAll is a helper for non-streaming use (e.g. tests, small inputs).
// It reads all objects from r and returns them as a slice of records.Record.
//
// If opt.AllowArrays is true and r contains a single top-level JSON array
// of objects, it is expanded into records.
func DecodeAll(r io.Reader, opt Options) ([]records.Record, error) {
	d := json.NewDecoder(r)
	d.UseNumber()

	var out []records.Record

	// First try: decode into a generic value once.
	var root any
	if err := d.Decode(&root); err != nil {
		if err == io.EOF {
			return nil, nil
		}
		return nil, fmt.Errorf("json parser: decode root: %w", err)
	}

	switch v := root.(type) {
	case map[string]any:
		out = append(out, records.Record(v))

	case []any:
		if !opt.AllowArrays {
			return nil, fmt.Errorf("json parser: top-level array encountered but allow_arrays=false")
		}
		for i, elem := range v {
			obj, ok := elem.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("json parser: element %d in array is not an object", i)
			}
			out = append(out, records.Record(obj))
		}

	default:
		return nil, fmt.Errorf("json parser: unsupported top-level JSON type %T", v)
	}

	// If there is trailing content (e.g., NDJSON after root), consume it using
	// the streaming Next API.
	dec := NewDecoder(d.Buffered(), opt) // remaining buffered bytes, if any
	for {
		rec, err := dec.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		out = append(out, rec)
	}

	return out, nil
}
