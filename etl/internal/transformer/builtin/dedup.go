package builtin

import (
	"etl/pkg/records"
	"fmt"
	"sort"
	"strconv"
	"strings"
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

	// We’ll track whether a record is keyed in the first pass to avoid recomputing in the second.
	keyed := make([]bool, len(in))

	// Key construction function
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
			appendKeyPart(&b, v)
		}
		return b.String(), true
	}

	// Score computation function
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
			// Mark this as a non-keyed record.
			keyed[i] = false
			continue
		}

		// Mark this as a keyed record
		keyed[i] = true

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

	// Output the winners in order of their original index
	winnersSorted := make([]slot, 0, len(winners))
	for _, s := range winners {
		winnersSorted = append(winnersSorted, s)
	}
	sort.Slice(winnersSorted, func(i, j int) bool {
		return winnersSorted[i].index < winnersSorted[j].index
	})

	// Compose output with winners in order of appearance
	out := make([]records.Record, len(winnersSorted))
	for i, s := range winnersSorted {
		out[i] = s.rec
	}

	// Append passthrough records (non-keyed records) in their original order
	for i, r := range in {
		if !keyed[i] {
			out = append(out, r)
		}
	}

	return out
}

// appendKeyPart appends a record's key field to the given string builder,
// converting common types directly to string without fmt.Sprint (for performance).
func appendKeyPart(b *strings.Builder, v any) {
	switch t := v.(type) {
	case nil:
		b.WriteByte('\x00')
	case string:
		b.WriteString(t)
	case int:
		b.WriteString(strconv.Itoa(t))
	case int64:
		b.WriteString(strconv.FormatInt(t, 10))
	case bool:
		if t {
			b.WriteString("true")
		} else {
			b.WriteString("false")
		}
	default:
		b.WriteString(fmt.Sprint(t))
	}
}
