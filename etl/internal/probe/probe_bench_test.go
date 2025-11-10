// Package probe contains micro-benchmarks for hot paths in csvprobe:
// CSV reading, type inference, normalization, and layout selection.
package probe

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
	"time"
)

//
// ---- readCSVSample ----------------------------------------------------------
//

// BenchmarkReadCSVSample measures parsing throughput on aligned CSV data.
func BenchmarkReadCSVSample(b *testing.B) {
	var sb strings.Builder
	sb.WriteString("a,b,c,d,e\n")
	for i := 0; i < 10000; i++ {
		fmt.Fprintf(&sb, "%d,%d,%d,%s,%s\n", i, i+1, i+2, "2024-01-02", "2024-01-02T03:04:05Z")
	}
	data := []byte(sb.String())

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, err := readCSVSample(data, ','); err != nil {
			b.Fatal(err)
		}
	}
}

//
// ---- inferTypeForColumn / inferTypes ---------------------------------------
//

// BenchmarkInferTypeForColumn tests the tight loop over column samples.
func BenchmarkInferTypeForColumn(b *testing.B) {
	ints := make([]string, 1000)
	floats := make([]string, 1000)
	dates := make([]string, 1000)
	timestamps := make([]string, 1000)
	for i := range ints {
		ints[i] = fmt.Sprintf("%d", i-500)
		floats[i] = "3.14159"
		dates[i] = "2024-01-02"
		timestamps[i] = time.RFC3339
	}

	b.Run("int", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = inferTypeForColumn(ints)
		}
	})
	b.Run("float", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = inferTypeForColumn(floats)
		}
	})
	b.Run("date", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = inferTypeForColumn(dates)
		}
	})
	b.Run("timestamp", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = inferTypeForColumn(timestamps)
		}
	})
}

// BenchmarkInferTypes measures full-table inference with 5 columns.
func BenchmarkInferTypes(b *testing.B) {
	headers := []string{"i", "b", "f", "d", "ts"}
	row := []string{"123", "true", "3.14", "2024-01-02", "2024-01-02T03:04:05Z"}
	rows := make([][]string, 2000)
	for i := range rows {
		rows[i] = row
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = inferTypes(headers, rows)
	}
}

//
// ---- normalizeFieldName -----------------------------------------------------
//

// BenchmarkNormalizeFieldName includes both ASCII and accented inputs to
// exercise the Unicode normalization path.
func BenchmarkNormalizeFieldName(b *testing.B) {
	inputs := []string{"Hello World", "PČV číslo", "Straße", "x_y-z.a"}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = normalizeFieldName(inputs[i%len(inputs)])
	}
}

//
// ---- selectBestLayout / detectColumnLayouts --------------------------------
//

// BenchmarkSelectBestLayout stresses time.Parse across candidate layouts.
func BenchmarkSelectBestLayout(b *testing.B) {
	samples := []string{"2024-01-02T03:04:05Z", "2024-01-03T04:05:06Z"}
	layouts := []string{time.RFC3339, "2006-01-02 15:04:05", time.RFC3339Nano}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = selectBestLayout(samples, layouts, timestampLayoutPreference)
	}
}

// BenchmarkDetectColumnLayouts measures per-column detection over many rows.
func BenchmarkDetectColumnLayouts(b *testing.B) {
	var buf bytes.Buffer
	buf.WriteString("d,ts\n")
	for i := 0; i < 10000; i++ {
		fmt.Fprintf(&buf, "2024-01-02,2024-01-02T03:04:05Z\n")
	}
	_, rows, _ := readCSVSample(buf.Bytes(), ',')

	inferred := []string{"date", "timestamp"}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = detectColumnLayouts(rows, inferred)
	}
}
