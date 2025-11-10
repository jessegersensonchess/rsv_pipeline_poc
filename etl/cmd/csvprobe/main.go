package main

import (
	"flag"
	"fmt"
	"unicode/utf8"

	"csv_header_fetcher/internal/probe"
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
		// keep simple like before
		panic(err)
	}

	fmt.Print(string(res.Body))
}
