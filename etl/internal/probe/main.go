// Command csvprobe samples the first N bytes of a remote CSV file and prints
// header names with inferred SQL-like types.
// It prefers HTTP Range requests but also defensively limits reads client-side,
// so it works even when Range is ignored.
//
// Example:
//
//	csvprobe -url="https://example.com/test.csv" -bytes=8192 -delimiter=","
package probe

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
	"unicode"

	"golang.org/x/text/runes"
	"golang.org/x/text/transform"
	"golang.org/x/text/unicode/norm"
)

// fetchFirstBytes retrieves up to n bytes from url using HTTP GET. It sets a
// "Range: bytes=0-(n-1)" header, but also applies a client-side read limit so
// it succeeds even when the server ignores Range and returns 200 OK.
//
// Returned slice length is <= n.
func fetchFirstBytes(url string, n int) ([]byte, error) {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	if n > 0 {
		req.Header.Set("Range", fmt.Sprintf("bytes=0-%d", n-1))
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	// Regardless of 206 or 200, only read up to n bytes.
	lr := &io.LimitedReader{R: resp.Body, N: int64(n)}
	var buf bytes.Buffer
	_, err = buf.ReadFrom(lr)
	if err != nil && err != io.EOF {
		return nil, err
	}
	return buf.Bytes(), nil
}

// readCSVSample parses CSV data using delim and returns headers and up to a
// capped number of data rows. It is tolerant of trimmed samples and malformed lines.
// DATA ROWS WITH A DIFFERENT COLUMN COUNT THAN THE HEADER ARE SKIPPED.
//
// BEST-EFFORT MODE:
//   - Allows variable field counts (FieldsPerRecord = -1).
//   - Skips records that cause parse errors instead of failing the whole read.
//   - Pads/truncates rows to header length so downstream consumers can rely on
//     stable column indexes.
func readCSVSample(data []byte, delim rune) ([]string, [][]string, error) {
	r := csv.NewReader(bytes.NewReader(data))
	r.Comma = delim
	r.LazyQuotes = true
	r.TrimLeadingSpace = true
	r.FieldsPerRecord = -1 // allow variable fields; we'll enforce width ourselves

	// Read header: skip malformed/empty lines until a usable one or EOF.
	var headers []string
	for {
		rec, err := r.Read()
		if err == io.EOF {
			return []string{}, [][]string{}, nil
		}
		if err != nil || len(rec) == 0 {
			// Bad/empty line: best-effort skip.
			continue
		}
		headers = stripUTF8BOM(rec)
		break
	}

	// Read data rows, skipping:
	//  - parse errors
	//  - empty lines
	//  - rows whose field count != header length
	const maxRows = 150000 // hardcoded...move to config
	rows := make([][]string, 0, maxRows)
	want := len(headers)

	for len(rows) < maxRows {
		rec, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil || len(rec) == 0 {
			continue // skip malformed/empty line
		}
		if len(rec) != want {
			continue // skip misaligned row to keep type inference accurate
		}
		rows = append(rows, rec)
	}

	return headers, rows, nil
}

// fitRowToWidth truncates or pads a CSV record to exactly n fields.
func fitRowToWidth(row []string, n int) []string {
	if len(row) == n {
		return row
	}
	if len(row) > n {
		cp := make([]string, n)
		copy(cp, row[:n])
		return cp
	}
	cp := make([]string, n)
	copy(cp, row)
	// Missing fields are left as empty strings.
	return cp
}

// stripUTF8BOM removes a UTF-8 BOM from the first header field if present.
func stripUTF8BOM(headers []string) []string {
	if len(headers) == 0 {
		return headers
	}
	if strings.HasPrefix(headers[0], "\uFEFF") {
		headers[0] = strings.TrimPrefix(headers[0], "\uFEFF")
	}
	return headers
}

// inferTypes returns one inferred SQL-like type per header based on the sampled rows.
func inferTypes(headers []string, rows [][]string) []string {
	n := len(headers)
	cols := make([][]string, n)
	for _, row := range rows {
		for i := 0; i < n && i < len(row); i++ {
			cols[i] = append(cols[i], row[i])
		}
	}
	types := make([]string, n)
	for i := 0; i < n; i++ {
		types[i] = inferTypeForColumn(cols[i])
	}
	return types
}

// inferTypeForColumn guesses a SQL-friendly type among:
// boolean, integer, real, date, timestamp, text.
// Heuristic: require all non-empty values to satisfy a narrower type.
func inferTypeForColumn(values []string) string {
	allEmpty := true
	nonEmpty := nonEmptyTrimmed(values)
	if len(nonEmpty) > 0 {
		allEmpty = false
	}
	if allEmpty {
		return "text"
	}
	if allMatch(nonEmpty, isInt) {
		return "integer"
	}
	if allMatch(nonEmpty, isBool) {
		return "boolean"
	}
	// Distinguish float from int to keep ints as integer.
	if allMatch(nonEmpty, isFloat) {
		return "real"
	}
	// Dates and timestamps (prefer timestamp when any time component exists).
	allDate := true
	anyTime := false
	for _, v := range nonEmpty {
		ok, hasTime := parseDateOrTimestamp(v)
		if !ok {
			allDate = false
			break
		}
		if hasTime {
			anyTime = true
		}
	}
	if allDate {
		if anyTime {
			return "timestamp"
		}
		return "date"
	}
	return "text"
}

// nonEmptyTrimmed returns the non-empty, trimmed values.
func nonEmptyTrimmed(vals []string) []string {
	out := make([]string, 0, len(vals))
	for _, v := range vals {
		v = strings.TrimSpace(v)
		if v != "" {
			out = append(out, v)
		}
	}
	return out
}

// allMatch reports whether every value satisfies fn.
func allMatch(vals []string, fn func(string) bool) bool {
	for _, v := range vals {
		if !fn(v) {
			return false
		}
	}
	return true
}

// isBool accepts common textual booleans and 1/0.
func isBool(s string) bool {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "true", "false", "t", "f", "yes", "no", "y", "n", "1", "0":
		return true
	default:
		return false
	}
}

// isInt requires a signed base-10 integer that fits in int64.
func isInt(s string) bool {
	_, err := strconv.ParseInt(strings.TrimSpace(s), 10, 64)
	return err == nil
}

// isFloat accepts decimal or scientific notation floats.
// If s parses as int, we treat it as NOT float (to keep ints as integer).
func isFloat(s string) bool {
	if isInt(s) {
		return false
	}
	_, err := strconv.ParseFloat(strings.TrimSpace(s), 64)
	return err == nil
}

// parseDateOrTimestamp tries to parse s as a timestamp first, then a date.
// It returns ok=true when one of the layouts matched and hasTime whether time
// components were present.
func parseDateOrTimestamp(s string) (ok bool, hasTime bool) {
	st := strings.TrimSpace(s)
	for _, layout := range timestampLayouts {
		if _, err := time.Parse(layout, st); err == nil {
			return true, true
		}
	}
	for _, layout := range dateLayouts {
		if _, err := time.Parse(layout, st); err == nil {
			return true, false
		}
	}
	return false, false
}

// normalizeFieldName converts arbitrary header text into a lowercase ASCII
// identifier suitable for SQL schemas:
//  1. lowercase
//  2. strip accents (NFD → remove Mn → NFC)
//  3. keep [a-z0-9_]; convert space/dash/dot to underscore; drop others
//  4. fallback to "col" if empty
func normalizeFieldName(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))

	// Decompose → remove nonspacing marks (accents) → recompose.
	t := transform.Chain(
		norm.NFD,
		runes.Remove(runes.In(unicode.Mn)),
		norm.NFC,
	)
	ascii, _, _ := transform.String(t, s)

	var b strings.Builder
	prevUnderscore := false
	for _, r := range ascii {
		switch {
		case r >= 'a' && r <= 'z':
			b.WriteRune(r)
			prevUnderscore = false
		case r >= '0' && r <= '9':
			b.WriteRune(r)
			prevUnderscore = false
		case r == '_' || r == ' ' || r == '-' || r == '.':
			if !prevUnderscore {
				b.WriteRune('_')
				prevUnderscore = true
			}
		default:
			// drop anything else
		}
	}
	name := b.String()
	// Trim leading/trailing underscores and collapse multiples already handled.
	name = strings.Trim(name, "_")
	if name == "" {
		return "col"
	}
	return name
}

// jsonContractFieldSpec represents a column contract entry.
type jsonContractFieldSpec struct {
	Name     string   `json:"name"`
	Type     string   `json:"type"`
	Required bool     `json:"required,omitempty"`
	Layout   string   `json:"layout,omitempty"`
	Truthy   []string `json:"truthy,omitempty"`
	Falsy    []string `json:"falsy,omitempty"`
}

// truncateFieldName ensures the field name does not exceed PostgreSQL's 63-character limit,
// returning the first 10 and last 53 characters if the name exceeds the limit.
func truncateFieldName(s string) string {
	if len(s) > 63 {
		return s[:10] + s[len(s)-53:] // First 10 + last 53 characters
	}
	return s
}

// buildJSONConfig constructs the JSON config using headers, rows, inferred types, and delimiter.
// It preserves CSV order in header_map, adds a runtime block, inserts a 'coerce'
// transform with a shared layout and non-text types, then adds the existing validate transform.
func buildJSONConfig(name string, headers []string, rows [][]string, inferred []string, delim rune) jsonConfig {
	normName := normalizeFieldName(name)
	n := len(headers)

	// Header map in original CSV order, and a lookup map for field building.
	pairs := make([]KV, 0, n)
	normByHeader := make(map[string]string, n)
	normalizedCols := make([]string, 0, n)
	//for _, h := range headers {
	//	normalized := normalizeFieldName(h)
	//	normalized = truncateFieldName(normalized)
	//	pairs = append(pairs, KV{Key: h, Value: normalized})
	//	normalizedCols = append(normalizedCols, normalized)
	//}

	for _, h := range headers {
		norm := normalizeFieldName(h)
		norm = truncateFieldName(norm)
		pairs = append(pairs, KV{Key: h, Value: norm})
		normByHeader[h] = norm
		normalizedCols = append(normalizedCols, norm)
	}

	// Detect date/timestamp layouts per column (scoring-based).
	colLayouts := detectColumnLayouts(rows, inferred)

	// ----- NEW: choose a single "coerce layout" for the dataset -----
	// We pick a majority layout among all date/timestamp columns; prefer date layouts first.
	coerceLayout := chooseMajorityLayout(colLayouts, inferred)

	// Contract fields derived from headers/types.
	fields := make([]jsonContractFieldSpec, 0, n)
	requiredCounter := 0
	for i, h := range headers {
		col := jsonContractFieldSpec{
			Name: normByHeader[h],
			Type: contractTypeFromInference(inferred[i]),
		}
		// Heuristic: mark integer column required if no empties in sample.
		if allNonEmptySample(rows, i) && (inferred[i] == "integer") {
			if requiredCounter == 0 {
				col.Required = true
			}
			requiredCounter++
		}
		// Add per-column layouts for date/timestamp if detected.
		if inferred[i] == "date" || inferred[i] == "timestamp" {
			if lay := colLayouts[i]; lay != "" {
				col.Layout = lay
			}
		}
		// Truthy/falsy sets for booleans.
		if inferred[i] == "boolean" {
			col.Truthy = []string{"1", "t", "true", "yes", "y"}
			col.Falsy = []string{"0", "f", "false", "no", "n"}
		}
		fields = append(fields, col)
	}

	// ----- NEW: build the coerce types map (exclude "text") -----
	coerceTypes := make(map[string]string, n)
	for i, h := range headers {
		t := contractTypeFromInference(inferred[i])
		if t != "text" {
			coerceTypes[normByHeader[h]] = t
		}
	}

	var cfg jsonConfig

	// source
	cfg.Source.Kind = "file"
	cfg.Source.File.Path = "testdata/" + normName + ".csv"

	// NEW: runtime defaults (tweak as you wish or plumb via Options)
	cfg.Runtime = runtimeConfig{
		ReaderWorkers:    1,
		TransformWorkers: 1,
		LoaderWorkers:    1,
		BatchSize:        5000,
		ChannelBuffer:    1000,
	}

	// parser
	cfg.Parser.Kind = "csv"
	cfg.Parser.Options.HasHeader = true
	cfg.Parser.Options.Comma = string(delim)
	cfg.Parser.Options.TrimSpace = true
	cfg.Parser.Options.ExpectedFields = len(headers)
	cfg.Parser.Options.HeaderMap = OrderedMap{Pairs: pairs} // preserve CSV order

	// ----- NEW: transform list with "coerce" first, then "validate" -----
	coerce := transformCoerce{
		Kind: "coerce",
		Options: coerceOptions{
			Layout: coerceLayout,
			Types:  coerceTypes,
		},
	}
	validate := transformValidate{
		Kind: "validate",
		Options: validateOptions{
			Policy: "lenient",
		},
	}
	validate.Options.Contract.Name = normName
	validate.Options.Contract.Fields = fields

	cfg.Transform = []any{coerce, validate}

	// storage
	cfg.Storage.Kind = "postgres"
	cfg.Storage.Postgres.DSN = "postgresql://user:password@0.0.0.0:5432/testdb?sslmode=disable"
	cfg.Storage.Postgres.Table = "public." + normName
	cfg.Storage.Postgres.Columns = normalizedCols
	cfg.Storage.Postgres.KeyColumns = []string{} // keep as-is or set based on your key inference
	cfg.Storage.Postgres.AutoCreateTable = true  // set per your earlier addition

	return cfg
}

// chooseMajorityLayout picks a single dataset-level layout for the "coerce" transform.
// It counts occurrences of per-column detected layouts for date/timestamp columns.
// If multiple layouts tie, it prefers date layouts first (by dateLayoutPreference),
// then timestamp preferences (timestampLayoutPreference), then declaration order.
// Returns "" if nothing was detected.
func chooseMajorityLayout(colLayouts []string, inferred []string) string {
	type cand struct {
		layout string
		count  int
		pref   int
	}

	// Count layouts across date/timestamp columns.
	counts := map[string]*cand{}
	for i := range colLayouts {
		t := inferred[i]
		lay := colLayouts[i]
		if lay == "" {
			continue
		}
		if t != "date" && t != "timestamp" {
			continue
		}
		if counts[lay] == nil {
			// prefer date preference if the column is date; otherwise timestamp preference
			pref := 0
			if t == "date" {
				pref = dateLayoutPreference(lay)
			} else {
				pref = timestampLayoutPreference(lay)
			}
			counts[lay] = &cand{layout: lay, count: 0, pref: pref}
		}
		counts[lay].count++
	}

	// Select the winner by (count, preference); stable on insertion order otherwise.
	var best *cand
	for _, c := range counts {
		if best == nil || c.count > best.count || (c.count == best.count && c.pref > best.pref) {
			cp := *c
			best = &cp
		}
	}
	if best != nil {
		return best.layout
	}
	return ""
}

// printJSONConfig marshals cfg into pretty-printed JSON and returns the bytes.
// Callers (main/tests) can write/inspect the returned bytes.
func printJSONConfig(cfg jsonConfig) ([]byte, error) {
	b, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return nil, err
	}
	// Add trailing newline to match typical CLI output.
	return append(b, '\n'), nil
}

// contractTypeFromInference maps the inferred SQL-like types
// to contract types expected in the JSON.
func contractTypeFromInference(inf string) string {
	switch inf {
	case "integer":
		return "bigint"
	case "real":
		return "float"
	case "boolean":
		return "bool"
	case "date":
		return "date"
	case "timestamp":
		return "timestamp"
	default:
		return "text"
	}
}

// allNonEmptySample reports true if every sampled row has a non-empty value at colIdx.
func allNonEmptySample(rows [][]string, colIdx int) bool {
	if len(rows) == 0 {
		return false
	}
	for _, r := range rows {
		if colIdx >= len(r) || strings.TrimSpace(r[colIdx]) == "" {
			return false
		}
	}
	return true
}

// detectColumnLayouts returns a slice of layout strings (empty when unknown) for
// columns inferred as date/timestamp. It picks the layout that maximizes matches
// across all non-empty samples in that column. Ties are broken by a preference
// function (DMY > ISO > MDY for dates; RFC3339Nano > RFC3339 > others for timestamps),
// then by the declaration order in the layout slice.
func detectColumnLayouts(rows [][]string, inferred []string) []string {
	n := len(inferred)
	out := make([]string, n)
	if len(rows) == 0 {
		return out
	}

	// Collect samples per column for efficiency.
	cols := make([][]string, n)
	for _, r := range rows {
		for c := 0; c < n && c < len(r); c++ {
			v := strings.TrimSpace(r[c])
			if v != "" {
				cols[c] = append(cols[c], v)
			}
		}
	}

	for col := 0; col < n; col++ {
		switch inferred[col] {
		case "timestamp":
			out[col] = selectBestLayout(cols[col], timestampLayouts, timestampLayoutPreference)
		case "date":
			out[col] = selectBestLayout(cols[col], dateLayouts, dateLayoutPreference)
		default:
			out[col] = ""
		}
	}
	return out
}

// dateLayouts are common date formats (no time component).
// Preferred order isn't critical anymore (scoring is used), but we keep a sensible mix.
var dateLayouts = []string{
	"2006-01-02",  // ISO
	"02.01.2006",  // DMY dot
	"01.02.2006",  // MDY dot
	"02/01/2006",  // DMY slash
	"01/02/2006",  // MDY slash
	"2 Jan 2006",  // DMY textual day
	"02-Jan-2006", // DMY dash textual month
	"2006/01/02",  // ISO slashy
	"20060102",    // basic ISO
}

// timestampLayouts are common timestamp formats (with time component).
var timestampLayouts = []string{
	time.RFC3339Nano,
	time.RFC3339,
	"2006-01-02 15:04:05",
	"2006-01-02 15:04:05.999999999",
	"2006/01/02 15:04:05",
	"02/01/2006 15:04:05", // DMY
	"01/02/2006 15:04:05", // MDY
	"2006-01-02T15:04:05Z0700",
	"2006-01-02 15:04:05 -0700",
}

// dateLayoutPreference returns a tie-break weight for a date layout.
// Higher numbers = stronger preference. We prefer DMY (common in CZ/EU) over ISO over MDY.
func dateLayoutPreference(layout string) int {
	switch layout {
	// DMY
	case "02.01.2006", "02/01/2006", "2 Jan 2006", "02-Jan-2006":
		return 3
	// ISO-ish
	case "2006-01-02", "2006/01/02", "20060102":
		return 2
	// MDY
	case "01.02.2006", "01/02/2006":
		return 1
	// default (least preferred)
	default:
		return 0
	}
}

// timestampLayoutPreference returns a tie-break weight for timestamp layouts.
// Prefer strict RFC3339Nano, then RFC3339, then others equally.
func timestampLayoutPreference(layout string) int {
	switch layout {
	case time.RFC3339Nano:
		return 3
	case time.RFC3339:
		return 2
	default:
		return 1
	}
}

// selectBestLayout scores each candidate layout by how many samples it matches.
// The layout with the highest score is chosen. On ties, prefer the layout with
// the higher preference (as given by pref), and finally the one that appears
// first in the layouts slice.
//
// samples should be a collection of raw, non-empty string values from a single column.
func selectBestLayout(samples []string, layouts []string, pref func(string) int) string {
	if len(samples) == 0 || len(layouts) == 0 {
		return ""
	}
	scores := make([]int, len(layouts))

	// Score: count successful parses.
	for _, s := range samples {
		for i, lay := range layouts {
			if _, err := time.Parse(lay, s); err == nil {
				scores[i]++
			}
		}
	}

	// Pick the best by (score, preference, order).
	bestIdx := -1
	bestScore := -1
	bestPref := -1
	for i := range layouts {
		sc := scores[i]
		if sc < bestScore {
			continue
		}
		if sc > bestScore {
			bestIdx, bestScore, bestPref = i, sc, pref(layouts[i])
			continue
		}
		// tie on score → preference
		p := pref(layouts[i])
		if p > bestPref {
			bestIdx, bestPref = i, p
			continue
		}
		// tie on preference → keep earlier (stable)
	}
	if bestIdx >= 0 && bestScore > 0 {
		return layouts[bestIdx]
	}
	return ""
}
