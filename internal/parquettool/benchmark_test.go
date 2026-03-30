package parquettool

import (
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/parquet-go/parquet-go"
)

type benchmarkRow struct {
	ID      int64   `parquet:"id"`
	Group   int64   `parquet:"group"`
	Name    string  `parquet:"name"`
	City    string  `parquet:"city"`
	Score   float64 `parquet:"score"`
	Active  bool    `parquet:"active"`
	Payload string  `parquet:"payload"`
}

var benchmarkDataset struct {
	once sync.Once
	path string
	err  error
}

func benchmarkDatasetPath(tb testing.TB) string {
	tb.Helper()

	benchmarkDataset.once.Do(func() {
		dir := filepath.Join(os.TempDir(), "go-parquet-tools-bench")
		if err := os.MkdirAll(dir, 0o755); err != nil {
			benchmarkDataset.err = err
			return
		}

		path := filepath.Join(dir, "flat-100k.parquet")
		if _, err := os.Stat(path); err == nil {
			benchmarkDataset.path = path
			return
		}

		rows := make([]benchmarkRow, 100_000)
		payload := strings.Repeat("payload-", 8)
		cities := []string{"Tokyo", "Shanghai", "Berlin", "New York", "Singapore"}
		for i := range rows {
			rows[i] = benchmarkRow{
				ID:      int64(i + 1),
				Group:   int64(i % 32),
				Name:    "user-" + strconv.Itoa(i+1),
				City:    cities[i%len(cities)],
				Score:   float64((i*37)%1000) / 10.0,
				Active:  i%3 == 0,
				Payload: payload + strconv.Itoa(i%97),
			}
		}

		benchmarkDataset.err = parquet.WriteFile(path, rows)
		benchmarkDataset.path = path
	})

	if benchmarkDataset.err != nil {
		tb.Fatalf("prepare benchmark dataset: %v", benchmarkDataset.err)
	}
	return benchmarkDataset.path
}

func BenchmarkReadRowsLimit100(b *testing.B) {
	path := benchmarkDatasetPath(b)
	opts := ReadOptions{Limit: 100}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		data, err := ReadRows([]string{path}, opts)
		if err != nil {
			b.Fatalf("ReadRows: %v", err)
		}
		if got := len(data.Rows); got != 100 {
			b.Fatalf("ReadRows rows = %d, want 100", got)
		}
	}
}

func BenchmarkWriteCSVLimit10000(b *testing.B) {
	path := benchmarkDatasetPath(b)
	opts := ReadOptions{Limit: 10_000}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := WriteCSVWithOptions(io.Discard, []string{path}, opts, true); err != nil {
			b.Fatalf("WriteCSVWithOptions: %v", err)
		}
	}
}

func BenchmarkInspectSingleFile(b *testing.B) {
	path := benchmarkDatasetPath(b)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		inspections, err := Inspect([]string{path})
		if err != nil {
			b.Fatalf("Inspect: %v", err)
		}
		if len(inspections) != 1 {
			b.Fatalf("Inspect files = %d, want 1", len(inspections))
		}
	}
}

func BenchmarkCountRows(b *testing.B) {
	path := benchmarkDatasetPath(b)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		count, err := CountRows([]string{path}, nil)
		if err != nil {
			b.Fatalf("CountRows: %v", err)
		}
		if count != 100_000 {
			b.Fatalf("CountRows = %d, want 100000", count)
		}
	}
}
