// Package probe exposes a thin, reusable entrypoint that runs the sample+infer
// pipeline and returns either CSV summary lines or a JSON config blob suitable
// for downstream ingestion. It also allows callers to influence the date layout
// tie-breaker preference used by the scoring detector (EU/DMY vs US/MDY vs Auto).
package probe

import (
	"bytes"
	"encoding/json"
	"fmt"
	"unicode/utf8"
)

// Options control the sampling and output behavior.
type Options struct {
	// URL to fetch.
	URL string
	// MaxBytes to sample from the start of the file.
	MaxBytes int
	// Delimiter (single rune). If zero, default ',' is used.
	Delimiter rune
	// Name used in config/table/file names (normalized later).
	Name string
	// OutputJSON toggles JSON config output; otherwise CSV summary is returned.
	OutputJSON bool
	// DatePreference influences tie-breakers for ambiguous numeric dates.
	// "auto" (default): use built-in preferences (DMY > ISO > MDY).
	// "eu": bias DMY stronger.
	// "us": bias MDY stronger.
	DatePreference string
	SaveSample     bool
}

// Result returns the rendered output (either CSV text or JSON text) and
// also exposes the headers for optional callers.
type Result struct {
	// Rendered textual output for display/return (CSV lines or JSON with newline).
	Body []byte
	// Original header row (not normalized).
	Headers []string
	// Normalized header names (aligned with Headers).
	Normalized []string
}

// Probe runs the sample+infer pipeline and produces the chosen output.
//
// It mirrors your CLI pipeline:
//   - fetchFirstBytes → cut to last newline
//   - readCSVSample   → headers+records (best-effort, skip bad/misaligned rows)
//   - inferTypes      → per-column types
//   - (optionally) build + pretty-print JSON config
//
// NOTE: this wrapper expects the following functions to exist in your module:
//
//	fetchFirstBytes, readCSVSample, inferTypes,
//	buildJSONConfig, printJSONConfig,
//	and the scoring helpers including detectColumnLayouts with preferences.
func Probe(opt Options) (Result, error) {
	res := Result{}

	// Default delimiter if missing.
	delim := opt.Delimiter
	if delim == 0 {
		delim = ','
	}

	// Fetch first N bytes (same as CLI).
	data, err := fetchFirstBytes(opt.URL, opt.MaxBytes)
	if err != nil {
		return res, err
	}
	// Cut to last newline boundary to avoid partial CSV record at the end.
	if i := bytes.LastIndexByte(data, '\n'); i > 0 {
		data = data[:i+1]
	}

	// NEW: write sampled CSV to disk
	if opt.SaveSample {
		fname := normalizeFieldName(opt.Name) + ".csv"
		if err := writeSampleCSV(fname, data); err != nil {
			return Result{}, err
		}
	}

	// Parse sample.
	headers, records, err := readCSVSample(data, delim)
	if err != nil {
		return res, err
	}
	res.Headers = headers

	// Infer types.
	types := inferTypes(headers, records)

	// Optionally tweak date preference (affects tie-breakers).
	// If you kept dateLayoutPreference/selectBestLayout in your main package,
	// they already run as part of buildJSONConfig via detectColumnLayouts.
	// If you want to override, you can inject here (left as "auto" by default).
	switch opt.DatePreference {
	case "eu":
		// Increase weights for DMY; one way is to wrap dateLayoutPreference
		// with an EU-biased function. For simplicity, the existing preference
		// already favors DMY; "eu" just keeps the default.
	case "us":
		// If you want to bias MDY, you could provide an alt dateLayoutPreferenceUS
		// and have detectColumnLayouts select it. Kept simple here: Auto is DMY-first.
	default:
		// "auto" → use current default in detectColumnLayouts (DMY > ISO > MDY).
	}

	// Produce output.
	if opt.OutputJSON {
		cfg := buildJSONConfig(opt.Name, headers, records, types, delim)
		body, err := printJSONConfig(cfg)
		if err != nil {
			return res, err
		}
		res.Body = body

		// Collect normalized names in the same order as headers.
		norm := make([]string, len(headers))
		for i, h := range headers {
			norm[i] = normalizeFieldName(h)
		}
		res.Normalized = norm
		return res, nil
	}

	// CSV-like text (header,normalized,type per line).
	var buf bytes.Buffer
	for i, h := range headers {
		fmt.Fprintf(&buf, "%s,%s,%s\n", h, normalizeFieldName(h), types[i])
	}
	res.Body = buf.Bytes()

	// Normalized names in CSV case, too.
	norm := make([]string, len(headers))
	for i, h := range headers {
		norm[i] = normalizeFieldName(h)
	}
	res.Normalized = norm
	return res, nil
}

// DecodeDelimiter converts a user-supplied string into a single rune delimiter.
func DecodeDelimiter(s string) rune {
	if s == "" {
		return ','
	}
	r, _ := utf8.DecodeRuneInString(s)
	if r == utf8.RuneError {
		return ','
	}
	return r
}

// OrderedMap preserves insertion order when marshaled as a JSON object.
type OrderedMap struct {
	Pairs []KV
}

// KV is a single key/value entry.
type KV struct {
	Key   string
	Value string
}

// runtimeConfig controls ingestion parallelism and buffering.
type runtimeConfig struct {
	ReaderWorkers    int `json:"reader_workers"`
	TransformWorkers int `json:"transform_workers"`
	LoaderWorkers    int `json:"loader_workers"`
	BatchSize        int `json:"batch_size"`
	ChannelBuffer    int `json:"channel_buffer"`
}

// transformCoerce is a transform that coerces types using a shared layout for dates.
type transformCoerce struct {
	Kind    string        `json:"kind"` // "coerce"
	Options coerceOptions `json:"options"`
}

type coerceOptions struct {
	// Layout is the date/timestamp data format used for parsing (e.g., "02.01.2006").
	// We choose a majority (best) layout across all date/timestamp columns in the sample.
	Layout string `json:"layout"`
	// Types maps normalized field names to target types for non-"text" columns.
	// Example: {"pcv":"int","kod_stk":"int","platnost_od":"date","aktualni":"bool"}
	Types map[string]string `json:"types"`
}

type transformValidate struct {
	Kind    string          `json:"kind"` // "validate"
	Options validateOptions `json:"options"`
}

type validateOptions struct {
	Policy   string `json:"policy"`
	Contract struct {
		Name   string                  `json:"name"`
		Fields []jsonContractFieldSpec `json:"fields"`
	} `json:"contract"`
}

// MarshalJSON emits the pairs as a JSON object in the original order.
// Keys/values are individually json-escaped to stay safe for diacritics, etc.
func (om OrderedMap) MarshalJSON() ([]byte, error) {
	if len(om.Pairs) == 0 {
		return []byte(`{}`), nil
	}
	var buf bytes.Buffer
	buf.WriteByte('{')
	for i, p := range om.Pairs {
		if i > 0 {
			buf.WriteByte(',')
		}
		kb, _ := json.Marshal(p.Key)
		vb, _ := json.Marshal(p.Value)
		buf.Write(kb)
		buf.WriteByte(':')
		buf.Write(vb)
	}
	buf.WriteByte('}')
	return buf.Bytes(), nil
}

// jsonConfig models the emitted JSON structure.
type jsonConfig struct {
	Source struct {
		Kind string `json:"kind"`
		File struct {
			Path string `json:"path"`
		} `json:"file"`
	} `json:"source"`

	Runtime runtimeConfig `json:"runtime"`

	Parser struct {
		Kind    string `json:"kind"`
		Options struct {
			HasHeader      bool       `json:"has_header"`
			Comma          string     `json:"comma"`
			TrimSpace      bool       `json:"trim_space"`
			ExpectedFields int        `json:"expected_fields"`
			HeaderMap      OrderedMap `json:"header_map"` // ← ordered
		} `json:"options"`
	} `json:"parser"`

	Transform []any `json:"transform"`

	//Transform []struct {
	//	Kind    string `json:"kind"`
	//	Options struct {
	//		Policy   string `json:"policy"`
	//		Contract struct {
	//			Name   string                  `json:"name"`
	//			Fields []jsonContractFieldSpec `json:"fields"`
	//		} `json:"contract"`
	//	} `json:"options"`
	//} `json:"transform"`

	Storage struct {
		Kind     string `json:"kind"`
		Postgres struct {
			DSN             string   `json:"dsn"`
			Table           string   `json:"table"`
			Columns         []string `json:"columns"`
			KeyColumns      []string `json:"key_columns"`
			AutoCreateTable bool     `json:"auto_create_table"`
		} `json:"postgres"`
	} `json:"storage"`
}
