package builtin

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"etl/internal/schema"
	"etl/pkg/records"
)

type Validate struct {
	Contract   schema.Contract
	DateLayout string            // optional global fallback date layout
	Policy     string            // "lenient"|"strict"|"repair"
	Reject     func(RejectedRow) // optional sink
}

type RejectedRow struct {
	Line   int
	Raw    records.Record
	Reason string
	Stage  string
}

func (v Validate) Apply(in []records.Record) []records.Record {
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

func (v Validate) validateRecord(r records.Record) (bool, string) {
	for _, f := range v.Contract.Fields {
		val := r[f.Name]

		// Required: treat nil or empty-string as missing
		if f.Required && (val == nil || asString(val) == "") {
			return false, fmt.Sprintf("required field %q missing", f.Name)
		}
		// If value is effectively empty and not required, accept
		if val == nil || asString(val) == "" {
			continue
		}

		switch f.Type {
		case "int":
			switch t := val.(type) {
			case int, int32, int64:
				// ok
			case float64:
				// ok (json numbers); allow
			case string:
				if _, err := strconv.ParseInt(strings.TrimSpace(t), 10, 64); err != nil {
					return false, fmt.Sprintf("field %q: %q not an int", f.Name, t)
				}
			default:
				return false, fmt.Sprintf("field %q: type %T not int-convertible", f.Name, t)
			}

		case "bool":
			s := strings.ToLower(strings.TrimSpace(asString(val)))
			if !isBoolTrueFalse(s, f.Truthy, f.Falsy) {
				return false, fmt.Sprintf("field %q: %q not a recognized boolean", f.Name, asString(val))
			}

		case "date":
			switch t := val.(type) {
			case time.Time:
				// Accept non-zero time values as valid dates.
				if t.IsZero() {
					// treat zero as empty (allowed if not required)
					// already handled by the early "empty" check above
					break
				}
				// ok
			case string:
				s := strings.TrimSpace(t)
				if s == "" {
					break
				}
				if !parseAnyDate(s, f.Layout, v.DateLayout) {
					return false, fmt.Sprintf("field %q: invalid date %q", f.Name, s)
				}
			default:
				// Anything else is not date-convertible
				return false, fmt.Sprintf("field %q: type %T not date-convertible", f.Name, t)
			}

		case "text", "string", "":
			// accept anything

		default:
			// unknown type -> accept (or fail if you prefer strict typing)
		}

		// Enum (if present): check equality on string form
		if len(f.Enum) > 0 {
			s := asString(r[f.Name])
			ok := false
			for _, ev := range f.Enum {
				if s == ev {
					ok = true
					break
				}
			}
			if !ok {
				return false, fmt.Sprintf("field %q: %q not in enum %v", f.Name, s, f.Enum)
			}
		}
	}
	return true, ""
}

func asString(v any) string {
	switch t := v.(type) {
	case nil:
		return ""
	case string:
		return t
	default:
		return fmt.Sprint(t)
	}
}

// isBoolTrueFalse validates a string against per-field truthy/falsy if provided,
// otherwise falls back to a broad default set (including Czech "ano"/"ne").
func isBoolTrueFalse(s string, truthy, falsy []string) bool {
	if s == "" {
		return true // empty handled earlier by required checks
	}
	if len(truthy) == 0 && len(falsy) == 0 {
		truthy = []string{"1", "t", "true", "yes", "y", "ano"}
		falsy = []string{"0", "f", "false", "no", "n", "ne"}
	}
	for _, v := range truthy {
		if s == strings.ToLower(v) {
			return true
		}
	}
	for _, v := range falsy {
		if s == strings.ToLower(v) {
			return true
		}
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
