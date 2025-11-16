package builtin

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"etl/internal/schema"
	"etl/pkg/records"
)

// Validate validates records against a schema.Contract. This optimized version
// precomputes per-field metadata (including O(1) lookup sets) and minimizes
// repeated string conversions and lowercasing work on the hot path.
type Validate struct {
	Contract   schema.Contract
	DateLayout string            // optional global fallback date layout
	Policy     string            // "lenient"|"strict"|"repair" (currently informational)
	Reject     func(RejectedRow) // optional sink

	// ---- precomputed metadata (built lazily) ----
	metaOnce sync.Once
	meta     []fieldMeta
}

type RejectedRow struct {
	Line   int
	Raw    records.Record
	Reason string
	Stage  string
}

// fieldMeta captures hot-path data for a single contract field.
type fieldMeta struct {
	name     string
	kind     string // "int","bool","date","text"/"string"/""
	required bool
	layout   string // field-specific date layout (if any)

	// Fast lookups:
	enumSet   map[string]struct{}
	truthySet map[string]struct{}
	falsySet  map[string]struct{}

	// Keep original enum list for error messages
	enumList []string
}

// default truthy/falsy sets (lowercased). Includes Czech "ano"/"ne".
var (
	defaultTruthy = map[string]struct{}{
		"1": {}, "t": {}, "True": {}, "true": {}, "yes": {}, "y": {}, "ano": {},
	}
	defaultFalsy = map[string]struct{}{
		"0": {}, "f": {}, "False": {}, "false": {}, "no": {}, "n": {}, "ne": {},
	}
)

func (v *Validate) buildMeta() {
	v.metaOnce.Do(func() {
		if len(v.Contract.Fields) == 0 {
			v.meta = nil
			return
		}
		v.meta = make([]fieldMeta, 0, len(v.Contract.Fields))
		for _, f := range v.Contract.Fields {

			m := fieldMeta{
				name:     f.Name,
				kind:     normalizeKind(f.Type),
				required: f.Required,
				layout:   f.Layout,
			}
			// Enum -> set for O(1) checks
			if len(f.Enum) > 0 {
				m.enumSet = make(map[string]struct{}, len(f.Enum))
				for _, s := range f.Enum {
					m.enumSet[s] = struct{}{} // enum is compared on exact string form per original behavior
				}
				m.enumList = append(m.enumList, f.Enum...)
			}

			// Truthy/Falsy -> lowercase sets; if none provided, keep nil and use defaults.
			if len(f.Truthy) > 0 {
				m.truthySet = make(map[string]struct{}, len(f.Truthy))
				for _, s := range f.Truthy {
					m.truthySet[strings.ToLower(s)] = struct{}{}
				}
			}
			if len(f.Falsy) > 0 {
				m.falsySet = make(map[string]struct{}, len(f.Falsy))
				for _, s := range f.Falsy {
					m.falsySet[strings.ToLower(s)] = struct{}{}
				}
			}
			v.meta = append(v.meta, m)
		}
	})
}

// Apply validates each record. Valid records are appended to a new slice.
// Invalid ones are (leniently) dropped, optionally reported via Reject.
func (v Validate) Apply(in []records.Record) []records.Record {
	fmt.Println("DEBUG: rejected")
	v.buildMeta()
	out := make([]records.Record, 0, len(in))
	for _, rec := range in {
		if ok, reason := v.validateRecord(rec); ok {
			out = append(out, rec)
		} else {
			if v.Reject != nil {
				v.Reject(RejectedRow{Raw: rec, Reason: reason, Stage: "validate"})
			}
			// lenient -> drop
		}
	}
	return out
}

// validateRecord is the hot-path validator. It uses precomputed meta to avoid
// per-row map iterations and expensive string ops where possible.
func (v *Validate) validateRecord(r records.Record) (bool, string) {
	v.buildMeta()

	for i := range v.meta {
		fm := v.meta[i]
		val, exists := r[fm.name]

		// Required: treat nil or empty-string as missing (no TrimSpace here to preserve semantics).
		if fm.required && (!exists || val == nil || (isString(val) && val.(string) == "")) {
			return false, fmt.Sprintf("required field %q missing", fm.name)
		}
		// Optional and effectively empty -> accept and continue
		if !exists || val == nil || (isString(val) && val.(string) == "") {
			continue
		}

		switch fm.kind {
		case "int":
			switch t := val.(type) {
			case int, int32, int64:
				// ok
			case float64:
				// ok (json numbers)
			case string:
				if HasEdgeSpace(t) {
					if _, err := strconv.ParseInt(strings.TrimSpace(t), 10, 64); err != nil {
						return false, fmt.Sprintf("field %q: %q not an int", fm.name, t)
					}
					return true, ""
				}

				if _, err := strconv.ParseInt(t, 10, 64); err != nil {
					return false, fmt.Sprintf("field %q: %q not an int", fm.name, t)
				}

			default:
				return false, fmt.Sprintf("field %q: type %T not int-convertible", fm.name, t)
			}

		case "bool":
			// Lowercase once; also trim once.
			var s string
			raw := asString(val)
			// wrapped in HasEdgeSpace to save CPU cycles in hot-path
			if HasEdgeSpace(raw) {
				s = strings.ToLower(strings.TrimSpace(raw))
			} else {
				s = strings.ToLower(raw)
			}

			if s == "" {
				// empty already handled above for required; allow for non-required
				break
			}
			if !isBoolInSets(s, fm.truthySet, fm.falsySet) {
				return false, fmt.Sprintf("field %q: %q not a recognized boolean", fm.name, asString(val))
			}

		case "date":
			switch t := val.(type) {
			case time.Time:
				// Accept any non-zero time.
				if t.IsZero() {
					// empty is allowed for non-required; the early empty check already handled required
					break
				}
			case string:
				var s string
				if HasEdgeSpace(t) {
					s = strings.TrimSpace(t)
				}
				if s == "" {
					break
				}
				if !parseAnyDate(s, fm.layout, v.DateLayout) {
					return false, fmt.Sprintf("field %q: invalid date %q", fm.name, s)
				}
			default:
				return false, fmt.Sprintf("field %q: type %T not date-convertible", fm.name, t)
			}

		case "text", "string", "":
			// accept anything

		default:
			// Unknown type -> accept (matches original behavior). If you prefer strict, reject here.
		}

		// Enum check (exact match on string form per original)
		if fm.enumSet != nil {
			s := asString(r[fm.name])
			if _, ok := fm.enumSet[s]; !ok {
				return false, fmt.Sprintf("field %q: %q not in enum %v", fm.name, s, fm.enumList)
			}
		}
	}

	return true, ""
}

// asString converts common types to string without incurring the overhead
// of fmt.Sprint; falls back to fmt.Sprint for uncommon types.
func asString(v any) string {
	switch t := v.(type) {
	case nil:
		return ""
	case string:
		return t
	case int:
		return strconv.Itoa(t)
	case int32:
		return strconv.FormatInt(int64(t), 10)
	case int64:
		return strconv.FormatInt(t, 10)
	case float64:
		// Use 'g' to match fmt.Sprint general behavior without allocations
		return strconv.FormatFloat(t, 'g', -1, 64)
	case bool:
		if t {
			return "true"
		}
		return "false"
	case time.Time:
		// RFC3339 by default; adjust if you prefer date-only etc.
		return t.Format(time.RFC3339)
	default:
		return fmt.Sprint(t)
	}
}

func isString(v any) bool {
	_, ok := v.(string)
	return ok
}

// isBoolInSets checks membership against custom sets if provided; otherwise
// falls back to the default truthy/falsy sets. 's' must already be lowercased.
func isBoolInSets(s string, truthy, falsy map[string]struct{}) bool {
	if s == "" {
		return true // empty handled by required logic
	}
	if truthy == nil && falsy == nil {
		if _, ok := defaultTruthy[s]; ok {
			return true
		}
		if _, ok := defaultFalsy[s]; ok {
			return true
		}
		return false
	}
	if _, ok := truthy[s]; ok {
		return true
	}
	if _, ok := falsy[s]; ok {
		return true
	}
	return false
}

// parseAnyDate attempts (in order): field layout, ISO (2006-01-02),
// then global layout if provided. Returns true if any parse succeeds.
func parseAnyDate(s, fieldLayout, globalLayout string) bool {
	if s == "" {
		return true
	}
	// field-specific layout (from contract)
	if fieldLayout != "" {
		if _, err := time.Parse(fieldLayout, s); err == nil {
			return true
		}
	}
	// ISO
	if _, err := time.Parse("2006-01-02", s); err == nil {
		return true
	}
	// global fallback layout
	if globalLayout != "" {
		if _, err := time.Parse(globalLayout, s); err == nil {
			return true
		}
	}
	return false
}

// normalizeKind maps schema field types onto the small set of validator kinds.
//
// It accepts database-ish types (bigint, int8, integer, boolean, date, etc.)
// and returns a normalized value used by the hot-path switch in validateRecord.
//
// Examples:
//
//	"bigint", "int8", "integer" → "int"
//	"boolean"                    → "bool"
//	"date", "timestamp"          → "date"
//	"text", "string"             → "string"
//	"unknown"                    → "unknown" (lowercased as-is)
func normalizeKind(t string) string {
	s := strings.ToLower(t)
	switch s {
	case "bigint", "int8", "integer", "int4", "int2", "int":
		return "int"
	case "boolean", "bool":
		return "bool"
	case "date", "timestamp", "timestamptz":
		return "date"
	case "text", "string":
		return "string"
	default:
		return s
	}
}
