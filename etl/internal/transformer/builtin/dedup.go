// Package builtin contains reusable ETL transformers.
//
// DeDup is the primary, policy-driven de-duplication transformer for the
// pipeline. It collapses duplicate records by a configured key and chooses
// a winner according to a configurable policy:
//
//   - "keep-first"   : keep the earliest occurrence in the batch
//   - "keep-last"    : keep the latest occurrence in the batch (default)
//   - "most-complete": keep the record that has the most non-empty fields;
//     ties break by "keep-last"
//
// This runs in-memory on a single batch (slice) of records. It is intended
// to remove intra-batch duplicates *before* hitting the database, reducing
// write amplification and avoiding constraint errors. The database should
// still maintain UNIQUE/PK constraints as a backstop.
//
// Keys: a record's key is constructed from the concatenation of configured
// fields as strings (nil -> "\x00"). For stable semantics across transforms,
// run DeDup *after* Normalize/Coerce so that types/empty values are consistent.
package builtin

import (
	"fmt"
	"sort"
	"strings"

	"etl/pkg/records"
)

// DeDup implements a configurable, in-memory de-duplication policy.
type DeDup struct {
	// Keys are the field names that form the business key, e.g. ["pcv","date_from"].
	Keys []string

	// Policy selects the winner among duplicates: "keep-first", "keep-last",
	// or "most-complete" (default is "keep-last").
	Policy string

	// PreferFields optionally lists fields that should weigh more heavily in
	// "most-complete" selection; present/non-empty values in these fields add
	// an extra weight. This is a soft signal; ties still break by keep-last.
	PreferFields []string
}

// Apply executes the de-duplication and returns a new slice containing only
// the winning records for each key. Input order is preserved for keep-first;
// for other policies the relative order is by last-wins selection.
func (d DeDup) Apply(in []records.Record) []records.Record {
	if len(in) == 0 || len(d.Keys) == 0 {
		// Nothing to do; return input as-is.
		return in
	}

	// Normalize policy.
	policy := strings.ToLower(strings.TrimSpace(d.Policy))
	if policy == "" {
		policy = "keep-last"
	}

	type slot struct {
		rec   records.Record
		index int // original position in input (0-based)
		score int // completeness score (for most-complete)
	}

	winners := make(map[string]slot, len(in))

	// Pre-compute a weight set for PreferFields to speed scoring.
	prefer := make(map[string]struct{}, len(d.PreferFields))
	for _, f := range d.PreferFields {
		prefer[f] = struct{}{}
	}

	keyOf := func(r records.Record) (string, bool) {
		var b strings.Builder
		for _, k := range d.Keys {
			v, ok := r[k]
			if !ok {
				// Missing key field -> cannot key this record; drop from de-dup domain.
				return "", false
			}
			if b.Len() > 0 {
				b.WriteByte('\x1f') // unlikely separator
			}
			switch t := v.(type) {
			case nil:
				b.WriteByte('\x00')
			case string:
				b.WriteString(t)
			default:
				// Use fmt to stabilize across types coerced earlier.
				b.WriteString(fmt.Sprint(t))
			}
		}
		return b.String(), true
	}

	scoreOf := func(r records.Record) int {
		// Count non-empty values; nil / "" don't count.
		score := 0
		// PreferFields add an extra weight if present and non-empty.
		bonus := 0
		for k, v := range r {
			if v == nil {
				continue
			}
			switch t := v.(type) {
			case string:
				if t == "" {
					continue
				}
			}
			score++
			if _, ok := prefer[k]; ok {
				bonus++
			}
		}
		return score*10 + bonus // simple linear combo; 10x amplifies base signal
	}

	// Select winners according to the policy.
	for i, r := range in {
		key, ok := keyOf(r)
		if !ok {
			// Keep passthrough records with missing key by appending later.
			continue
		}
		switch policy {
		case "keep-first":
			if _, exists := winners[key]; !exists {
				winners[key] = slot{rec: r, index: i}
			}
		case "most-complete":
			s := slot{rec: r, index: i, score: scoreOf(r)}
			if prev, exists := winners[key]; !exists {
				winners[key] = s
			} else if s.score > prev.score || (s.score == prev.score && s.index > prev.index) {
				// Tie-break: prefer later record for determinism.
				winners[key] = s
			}
		default: // "keep-last"
			winners[key] = slot{rec: r, index: i}
		}
	}

	// Compose output:
	// 1) winners in stable index order (ascending index for keep-first/most-complete,
	//    for keep-last this yields the position of the winning line).
	out := make([]records.Record, 0, len(winners))
	indexes := make([]int, 0, len(winners))
	for _, s := range winners {
		indexes = append(indexes, s.index)
	}
	sort.Ints(indexes)
	posByIndex := make(map[int]records.Record, len(winners))
	for _, s := range winners {
		posByIndex[s.index] = s.rec
	}
	for _, idx := range indexes {
		out = append(out, posByIndex[idx])
	}

	// 2) Append pass-through (non-keyed) records in original order.
	for _, r := range in {
		if _, ok := keyOf(r); !ok {
			out = append(out, r)
		}
	}
	return out
}
