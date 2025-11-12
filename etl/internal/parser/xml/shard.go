package xmlparser

import (
	"bufio"
	"bytes"
	"encoding/xml"
	"errors"
	"io"
)

// Job represents a single record shard to parse.
type Job struct {
	Index int
	Bytes []byte // in zerocopy mode, this is a slice into the underlying buffer.
}

// Result is a worker outcome for a Job.
type Result struct {
	Index  int
	Record Record
	Err    error
}

// ShardStreaming splits a stream into <record_tag> ... </record_tag> chunks by
// re-encoding the subtree into a buffer. It is tolerant to truncated inputs:
// incomplete trailing records are ignored.
func ShardStreaming(r io.Reader, recordTag string, bufSize int, jobs chan<- Job) error {
	br := bufio.NewReaderSize(r, bufSize)
	dec := xml.NewDecoder(br)
	dec.Strict = false

	i := 0
	for {
		tok, err := dec.Token()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			// Treat truncation like EOF; other errors bubble.
			if isTruncErr(err) {
				return nil
			}
			return err
		}
		se, ok := tok.(xml.StartElement)
		if !ok || se.Name.Local != recordTag {
			continue
		}
		var buf bytes.Buffer
		enc := xml.NewEncoder(&buf)
		_ = enc.EncodeToken(se)
		depth := 1
		for depth > 0 {
			tok, err = dec.Token()
			if err != nil {
				// truncated record; drop it and finish
				return nil
			}
			switch tok.(type) {
			case xml.StartElement:
				depth++
			case xml.EndElement:
				depth--
			}
			_ = enc.EncodeToken(tok)
		}
		_ = enc.Flush()
		jobs <- Job{Index: i, Bytes: buf.Bytes()}
		i++
	}
}

// ShardZeroCopy slices raw bytes between <recordTag ...> and </recordTag>.
// It requires the next byte after the tag name to be a delimiter to avoid
// matching <recordTagXYZ>. Truncation at the end simply stops the scan.
func ShardZeroCopy(data []byte, recordTag string, jobs chan<- Job) error {
	if recordTag == "" {
		return errors.New("recordTag required")
	}
	startPat := []byte("<" + recordTag)
	endPat := []byte("</" + recordTag + ">")

	isDelim := func(b byte) bool {
		switch b {
		case '>', '/', ' ', '\t', '\n', '\r':
			return true
		default:
			return false
		}
	}

	i := 0
	idx := 0
	n := len(data)
	for i < n {
		j := bytes.Index(data[i:], startPat)
		if j < 0 {
			return nil
		}
		start := i + j
		after := start + len(startPat)
		if after >= n || !isDelim(data[after]) {
			i = after
			continue
		}
		k := bytes.Index(data[after:], endPat)
		if k < 0 {
			return nil
		}
		end := after + k + len(endPat)
		jobs <- Job{Index: idx, Bytes: data[start:end]}
		idx++
		i = end
	}
	return nil
}
