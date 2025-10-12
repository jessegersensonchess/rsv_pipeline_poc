package vehicletech

import (
	"encoding/json"
	"strconv"
)

type writerStats struct {
	inserted int
	skipped  int
	reasons  map[string]int
	err      error
}

// pgxEncodedSource implements pgx.CopyFromSource to stream rows from a channel,
// while emitting periodic progress logs and recording skips via callback.
type pgxEncodedSource struct {
	ch      <-chan encodedJob
	current []interface{}
	err     error
	addSkip func(reason string, ln int, pcvField, raw string)
	isPG    bool

	// progress
	inserted int
	logEvery int                             // e.g. 50_000
	logf     func(inserted int, skipped int) // writer-provided logger
	// skipped is tracked by addSkip in writer; we pass via closure
}

func (s *pgxEncodedSource) Next() bool {
	for job := range s.ch {
		// Skip invalid/errored rows; mirror existing reasons
		if job.pcv == 0 || job.payload == nil {
			if job.payload == nil && job.pcv == 0 && job.pcvField == "" {
				s.addSkip("parse_error", job.lineNum, "", job.raw)
			} else if job.pcv == 0 {
				s.addSkip("pcv_not_numeric", job.lineNum, job.pcvField, job.raw)
			} else {
				s.addSkip("json_marshal_error", job.lineNum, strconv.FormatInt(job.pcv, 10), job.raw)
			}
			continue
		}
		if !json.Valid(job.payload) {
			s.addSkip("invalid_json", job.lineNum, strconv.FormatInt(job.pcv, 10), job.raw)
			continue
		}

		// Build one row with no extra allocs.
		if s.isPG {
			s.current = []interface{}{job.pcv, job.payload}
		} else {
			s.current = []interface{}{job.pcv, string(job.payload)}
		}

		// Progress
		s.inserted++
		if s.logEvery > 0 && (s.inserted%s.logEvery) == 0 && s.logf != nil {
			// skipped count is maintained inside writer via addSkip closure; logf captures it.
			s.logf(s.inserted, 0)
		}
		return true
	}
	return false
}
func (s *pgxEncodedSource) Values() ([]interface{}, error) { return s.current, nil }
func (s *pgxEncodedSource) Err() error                     { return s.err }
