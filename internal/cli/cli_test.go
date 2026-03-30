package cli

import (
	"bytes"
	"strings"
	"testing"
)

const sampleParquetPath = "/Users/safeoy/go/pkg/mod/github.com/parquet-go/parquet-go@v0.17.0/testdata/small.parquet"

func TestShowCommand(t *testing.T) {
	stdout := new(bytes.Buffer)
	stderr := new(bytes.Buffer)

	code := Run([]string{"show", "--limit", "2", sampleParquetPath}, stdout, stderr)
	if code != 0 {
		t.Fatalf("show exit code = %d, stderr = %s", code, stderr.String())
	}

	out := stdout.String()
	if !strings.Contains(out, "+") || !strings.Contains(out, "|") {
		t.Fatalf("show output does not look like a table: %s", out)
	}
	if len(strings.TrimSpace(out)) == 0 {
		t.Fatalf("show output is empty")
	}
}

func TestCSVCommand(t *testing.T) {
	stdout := new(bytes.Buffer)
	stderr := new(bytes.Buffer)

	code := Run([]string{"csv", "--limit", "2", sampleParquetPath}, stdout, stderr)
	if code != 0 {
		t.Fatalf("csv exit code = %d, stderr = %s", code, stderr.String())
	}

	out := stdout.String()
	lines := strings.Split(strings.TrimSpace(out), "\n")
	if len(lines) < 2 {
		t.Fatalf("csv output should include header and data rows: %s", out)
	}
	if !strings.Contains(lines[0], ",") {
		t.Fatalf("csv header should contain commas: %s", lines[0])
	}
}

func TestInspectCommand(t *testing.T) {
	stdout := new(bytes.Buffer)
	stderr := new(bytes.Buffer)

	code := Run([]string{"inspect", sampleParquetPath}, stdout, stderr)
	if code != 0 {
		t.Fatalf("inspect exit code = %d, stderr = %s", code, stderr.String())
	}

	out := stdout.String()
	if !strings.Contains(out, "Rows:") {
		t.Fatalf("inspect output missing rows: %s", out)
	}
	if !strings.Contains(out, "Schema") || !strings.Contains(out, "Leaf Columns") {
		t.Fatalf("inspect output missing schema sections: %s", out)
	}
}
