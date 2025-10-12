package skiplog

import (
	"encoding/csv"
	"log"
	"os"
	"path/filepath"
	"strconv"
)

// not sure which of these structs will be used
type Stats struct {
	reasons map[string]int
	w       *csv.Writer
}

// not sure which of these structs will be used
type skipStats struct {
	reasons map[string]int
	w       *csv.Writer
}

func NewSkipStats(path string) (*skipStats, func()) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		log.Fatalf("create dir %s: %v", filepath.Dir(path), err)
	}
	f, err := os.Create(path)
	if err != nil {
		log.Fatalf("open %s: %v", path, err)
	}
	w := csv.NewWriter(f)
	_ = w.Write([]string{"reason", "line_number", "pcv_field", "raw_line"})
	return &skipStats{reasons: make(map[string]int), w: w}, func() {
		w.Flush()
		_ = f.Close()
	}
}

func (s *skipStats) Add(reason string, lineNum int, pcvField string, raw string) {
	s.reasons[reason]++
	_ = s.w.Write([]string{reason, strconv.Itoa(lineNum), pcvField, raw})
}
