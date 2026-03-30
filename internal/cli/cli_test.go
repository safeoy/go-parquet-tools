package cli

import (
	"bytes"
	"path/filepath"
	"strings"
	"testing"

	"github.com/parquet-go/parquet-go"
)

const sampleParquetPath = "/Users/safeoy/go/pkg/mod/github.com/parquet-go/parquet-go@v0.17.0/testdata/small.parquet"

type filterRow struct {
	ID    int64  `parquet:"id"`
	Name  string `parquet:"name"`
	Group int64  `parquet:"group"`
}

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

func TestShowCommandWithGlob(t *testing.T) {
	stdout := new(bytes.Buffer)
	stderr := new(bytes.Buffer)
	pattern := strings.Replace(sampleParquetPath, "small.parquet", "small*.parquet", 1)

	code := Run([]string{"show", "--limit", "2", pattern}, stdout, stderr)
	if code != 0 {
		t.Fatalf("show exit code = %d, stderr = %s", code, stderr.String())
	}

	out := stdout.String()
	if !strings.Contains(out, "kafka_partition") {
		t.Fatalf("show output missing expected header: %s", out)
	}
}

func TestShowCommandWithJSONFormat(t *testing.T) {
	stdout := new(bytes.Buffer)
	stderr := new(bytes.Buffer)

	code := Run([]string{"show", "--limit", "1", "--format", "json", sampleParquetPath}, stdout, stderr)
	if code != 0 {
		t.Fatalf("show json exit code = %d, stderr = %s", code, stderr.String())
	}
	out := strings.TrimSpace(stdout.String())
	if !strings.HasPrefix(out, "[") || !strings.Contains(out, "\"kafka_partition\"") {
		t.Fatalf("unexpected json output: %s", out)
	}
}

func TestHeadCommandWithJSONLFormat(t *testing.T) {
	stdout := new(bytes.Buffer)
	stderr := new(bytes.Buffer)

	code := Run([]string{"head", "-n", "2", "--format", "jsonl", sampleParquetPath}, stdout, stderr)
	if code != 0 {
		t.Fatalf("head jsonl exit code = %d, stderr = %s", code, stderr.String())
	}
	lines := strings.Split(strings.TrimSpace(stdout.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("expected 2 jsonl lines, got %d: %s", len(lines), stdout.String())
	}
	if !strings.HasPrefix(lines[0], "{") {
		t.Fatalf("unexpected jsonl line: %s", lines[0])
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

func TestShowCommandWithColumns(t *testing.T) {
	path := writeFilterSample(t)
	stdout := new(bytes.Buffer)
	stderr := new(bytes.Buffer)

	code := Run([]string{"show", "--limit", "1", "--columns", "group,name", path}, stdout, stderr)
	if code != 0 {
		t.Fatalf("show columns exit code = %d, stderr = %s", code, stderr.String())
	}
	out := stdout.String()
	if !strings.Contains(out, "group") || !strings.Contains(out, "name") {
		t.Fatalf("projection missing expected columns: %s", out)
	}
	if strings.Contains(out, "id") {
		t.Fatalf("projection kept unexpected columns: %s", out)
	}
}

func TestShowCommandWithWhereFilter(t *testing.T) {
	path := writeFilterSample(t)
	stdout := new(bytes.Buffer)
	stderr := new(bytes.Buffer)

	code := Run([]string{"show", "--where", "group=2", "--columns", "group", path}, stdout, stderr)
	if code != 0 {
		t.Fatalf("show filter exit code = %d, stderr = %s", code, stderr.String())
	}
	out := stdout.String()
	if !strings.Contains(out, "| 2") {
		t.Fatalf("filter output missing expected row: %s", out)
	}
	if strings.Contains(out, "| 1") {
		t.Fatalf("filter output contains unexpected row: %s", out)
	}
}

func TestCountCommand(t *testing.T) {
	stdout := new(bytes.Buffer)
	stderr := new(bytes.Buffer)

	code := Run([]string{"count", sampleParquetPath}, stdout, stderr)
	if code != 0 {
		t.Fatalf("count exit code = %d, stderr = %s", code, stderr.String())
	}
	if strings.TrimSpace(stdout.String()) != "1297" {
		t.Fatalf("unexpected count output: %s", stdout.String())
	}
}

func TestCountCommandWithWhereFilter(t *testing.T) {
	path := writeFilterSample(t)
	stdout := new(bytes.Buffer)
	stderr := new(bytes.Buffer)

	code := Run([]string{"count", "--where", "group=2", path}, stdout, stderr)
	if code != 0 {
		t.Fatalf("count filter exit code = %d, stderr = %s", code, stderr.String())
	}
	if strings.TrimSpace(stdout.String()) != "2" {
		t.Fatalf("unexpected filtered count output: %s", stdout.String())
	}
}

func TestCountCommandWithJSONFormat(t *testing.T) {
	stdout := new(bytes.Buffer)
	stderr := new(bytes.Buffer)

	code := Run([]string{"count", "--format", "json", sampleParquetPath}, stdout, stderr)
	if code != 0 {
		t.Fatalf("count json exit code = %d, stderr = %s", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "\"count\": 1297") {
		t.Fatalf("unexpected count json output: %s", stdout.String())
	}
}

func TestSchemaCommand(t *testing.T) {
	stdout := new(bytes.Buffer)
	stderr := new(bytes.Buffer)

	code := Run([]string{"schema", sampleParquetPath}, stdout, stderr)
	if code != 0 {
		t.Fatalf("schema exit code = %d, stderr = %s", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "- kafka_partition") {
		t.Fatalf("schema output missing expected field: %s", stdout.String())
	}
}

func TestSchemaCommandWithJSONFormat(t *testing.T) {
	stdout := new(bytes.Buffer)
	stderr := new(bytes.Buffer)

	code := Run([]string{"schema", "--format", "json", sampleParquetPath}, stdout, stderr)
	if code != 0 {
		t.Fatalf("schema json exit code = %d, stderr = %s", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "\"Path\"") || !strings.Contains(stdout.String(), "\"Schema\"") {
		t.Fatalf("unexpected schema json output: %s", stdout.String())
	}
}

func TestTailCommand(t *testing.T) {
	stdout := new(bytes.Buffer)
	stderr := new(bytes.Buffer)

	code := Run([]string{"tail", "-n", "1", sampleParquetPath}, stdout, stderr)
	if code != 0 {
		t.Fatalf("tail exit code = %d, stderr = %s", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "|") {
		t.Fatalf("tail output missing table rendering: %s", stdout.String())
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
	if !strings.Contains(out, "############ file meta data ############") {
		t.Fatalf("inspect output missing rows: %s", out)
	}
	if !strings.Contains(out, "############ Columns ############") || !strings.Contains(out, "############ Column(") {
		t.Fatalf("inspect output missing schema sections: %s", out)
	}
}

func TestInspectCommandWithJSONFormat(t *testing.T) {
	stdout := new(bytes.Buffer)
	stderr := new(bytes.Buffer)

	code := Run([]string{"inspect", "--format", "json", sampleParquetPath}, stdout, stderr)
	if code != 0 {
		t.Fatalf("inspect json exit code = %d, stderr = %s", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "\"Path\"") || !strings.Contains(stdout.String(), "\"LeafColumns\"") {
		t.Fatalf("unexpected inspect json output: %s", stdout.String())
	}
}

func writeFilterSample(t *testing.T) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "filter.parquet")
	rows := []filterRow{
		{ID: 1, Name: "alice", Group: 1},
		{ID: 2, Name: "bob", Group: 2},
		{ID: 3, Name: "cathy", Group: 2},
	}
	if err := parquet.WriteFile(path, rows); err != nil {
		t.Fatalf("write filter parquet: %v", err)
	}
	return path
}
