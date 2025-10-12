// Fast JSON encoder (UTF-8 safe, no maps)
//
// Idea: precompute `"Header":` byte prefixes once; for each row,
// write `{<prefix><quoted value>,...}` into a pooled bytes.Buffer.
// We use strconv.AppendQuote to safely JSON-escape values without
// the overhead of map hashing or reflect.
//
// This is dramatically cheaper than building map[string]string
// and calling encoding/json for each row.
package jsonutil

import (
	"bytes"
	"strconv"
	"sync"
)

type FastJSONEncoder struct {
	prefixes [][]byte
	bufPool  sync.Pool
}

func NewFastJSONEncoder(headers []string) *FastJSONEncoder {
	pfx := make([][]byte, len(headers))
	for i, h := range headers {
		var b bytes.Buffer
		b.WriteByte('"')
		qb := strconv.AppendQuote(nil, h)
		b.Write(qb[1 : len(qb)-1])
		b.WriteString(`":`)
		pfx[i] = append([]byte(nil), b.Bytes()...)
	}
	return &FastJSONEncoder{prefixes: pfx, bufPool: sync.Pool{New: func() any { return new(bytes.Buffer) }}}
}

func (e *FastJSONEncoder) EncodeRow(fields []string) []byte {
	buf := e.bufPool.Get().(*bytes.Buffer)
	buf.Reset()
	buf.WriteByte('{')
	for i := range e.prefixes {
		if i > 0 {
			buf.WriteByte(',')
		}
		buf.Write(e.prefixes[i])
		buf.Write(strconv.AppendQuote(nil, fields[i]))
	}
	buf.WriteByte('}')
	out := append([]byte(nil), buf.Bytes()...)
	e.bufPool.Put(buf)
	return out
}
