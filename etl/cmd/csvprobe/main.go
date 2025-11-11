package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"unicode/utf8"

	"etl/internal/probe"
)

var (
	flagURL       = flag.String("url", "https://example.com/test.csv", "URL of the CSV file to sample")
	flagBytes     = flag.Int("bytes", 20000, "Number of bytes to sample from the start of the file")
	flagDelimiter = flag.String("delimiter", ",", "CSV field delimiter (single character)")
	flagName      = flag.String("name", "data_set_name", "Connector name (used in DB table and file name). Example: technicke_prohlidky")
	flagJSON      = flag.Bool("json", false, "Output JSON config derived from the headers/types instead of CSV lines")
	// Optional: expose date preference if you want to surface it in CLI (defaults to auto)
	flagDatePref = flag.String("datepref", "auto", "Date layout preference tie-breaker: auto|eu|us")
	flagSave     = flag.Bool("save", false, "Write sampled bytes to [name].csv file")
)

func main() {
	flag.Parse()

	// Determine delimiter rune (keep behavior identical to your current code).
	delim := ','
	if *flagDelimiter != "" {
		if r, _ := utf8.DecodeRuneInString(*flagDelimiter); r != utf8.RuneError {
			delim = r
		}
	}

	// Friendly validation — no panic
	if flag.NArg() > 0 {
		// someone passed positional args by mistake
		fmt.Fprintf(os.Stderr, "unexpected args: %v\n", flag.Args())
		flag.Usage()
		os.Exit(2)
	}

	if *flagURL == "" {
		fmt.Fprintf(os.Stderr,
			"missing -url\n\nExample:\n  csvprobe -url https://example.com/data.csv -json\n")
		flag.Usage()
		os.Exit(2)
	}

	res, err := probe.Probe(probe.Options{
		URL:            *flagURL,
		MaxBytes:       *flagBytes,
		Delimiter:      delim,
		Name:           *flagName,
		OutputJSON:     *flagJSON,
		DatePreference: *flagDatePref, // "auto" by default; current scoring already prefers DMY
		SaveSample:     *flagSave,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "probe failed: %v\n", err)
		os.Exit(1)
	}

	// Only transform JSON if -json was requested.
	// Otherwise just stream CSV sample output unmodified.
	if *flagJSON {
		// Parse probe JSON
		var cfg map[string]any
		if err := json.Unmarshal(res.Body, &cfg); err == nil {

			// Dig into cfg["storage"]["postgres"]["key_columns"]
			if storage, ok := cfg["storage"].(map[string]any); ok {
				if pg, ok := storage["postgres"].(map[string]any); ok {
					if kc, ok := pg["key_columns"].([]any); ok && len(kc) == 0 {

						// Look at storage["postgres"]["columns"] and contract
						if cols, ok := pg["columns"].([]any); ok {
							for _, col := range cols {
								name, _ := col.(string)
								// Bump first required int column to key
								if looksLikeIntField(cfg, name) {
									pg["key_columns"] = []any{name}
									break
								}
							}
						}
					}
				}
			}

			// Re-marshal with key_columns defaulted
			out, _ := json.MarshalIndent(cfg, "", "  ")
			fmt.Println(string(out))
			return
		}
	}

	fmt.Print(string(res.Body))
}

// helper to detect integer+required fields from validate.contract
func looksLikeIntField(cfg map[string]any, col string) bool {
	x, ok := cfg["transform"].([]any)
	if !ok {
		return false
	}

	for _, t := range x {
		block, _ := t.(map[string]any)
		if block["kind"] != "validate" {
			continue
		}
		opt, _ := block["options"].(map[string]any)
		contract, _ := opt["contract"].(map[string]any)
		fields, _ := contract["fields"].([]any)

		for _, f := range fields {
			m, _ := f.(map[string]any)
			if m["name"] == col {
				typ, _ := m["type"].(string)
				req, _ := m["required"].(bool)
				if req && (typ == "bigint" || typ == "int" || typ == "integer") {
					return true
				}
			}
		}
	}
	return false
}
