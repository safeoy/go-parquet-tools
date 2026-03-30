package cli

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"strings"

	"github.com/safeoy/go-parquet-tools/internal/parquettool"
	"github.com/safeoy/go-parquet-tools/internal/render"
)

type stringListFlag []string

func (s *stringListFlag) String() string {
	return strings.Join(*s, ",")
}

func (s *stringListFlag) Set(value string) error {
	*s = append(*s, value)
	return nil
}

func Run(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		printUsage(stderr)
		return 2
	}

	switch args[0] {
	case "-h", "--help", "help":
		printUsage(stdout)
		return 0
	case "head":
		return runHead(args[1:], stdout, stderr)
	case "tail":
		return runTail(args[1:], stdout, stderr)
	case "count":
		return runCount(args[1:], stdout, stderr)
	case "schema":
		return runSchema(args[1:], stdout, stderr)
	case "show":
		return runShow(args[1:], stdout, stderr)
	case "csv":
		return runCSV(args[1:], stdout, stderr)
	case "inspect":
		return runInspect(args[1:], stdout, stderr)
	default:
		fmt.Fprintf(stderr, "unknown command %q\n\n", args[0])
		printUsage(stderr)
		return 2
	}
}

func runShow(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("show", flag.ContinueOnError)
	fs.SetOutput(stderr)

	limit := fs.Int("limit", 0, "maximum number of rows to print; use 0 for all rows")
	width := fs.Int("width", 40, "maximum display width for each cell")
	format := fs.String("format", "table", "output format: table, json, jsonl")
	columnsFlag := fs.String("columns", "", "comma-separated list of columns to include")
	var where stringListFlag
	fs.Var(&where, "where", "filter expression: column=value, column!=value, column~=substr, column^=prefix, column$=suffix")

	if err := fs.Parse(args); err != nil {
		return 2
	}
	if fs.NArg() < 1 {
		fmt.Fprintln(stderr, "show requires at least one parquet file or pattern")
		return 2
	}
	if *limit < 0 {
		fmt.Fprintln(stderr, "--limit must be >= 0")
		return 2
	}
	if *width < 8 {
		fmt.Fprintln(stderr, "--width must be >= 8")
		return 2
	}

	filters, ok := parseFilters(where, stderr)
	if !ok {
		return 2
	}
	data, err := parquettool.ReadRows(fs.Args(), parquettool.ReadOptions{
		Limit:   *limit,
		Columns: parseColumns(*columnsFlag),
		Filters: filters,
	})
	if err != nil {
		return printCommandError(stderr, err)
	}

	output, err := formatRowOutput(data, *format, *width)
	if err != nil {
		return printCommandError(stderr, err)
	}
	if _, err := io.WriteString(stdout, output); err != nil {
		fmt.Fprintf(stderr, "write output: %v\n", err)
		return 1
	}

	if data.Truncated && *format == "table" {
		_, _ = fmt.Fprintf(stdout, "\nshowing %d of %d rows\n", len(data.Rows), data.TotalRows)
	}

	return 0
}

func runHead(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("head", flag.ContinueOnError)
	fs.SetOutput(stderr)

	limit := fs.Int("n", 10, "number of rows to print")
	width := fs.Int("width", 40, "maximum display width for each cell")
	format := fs.String("format", "table", "output format: table, json, jsonl")
	columnsFlag := fs.String("columns", "", "comma-separated list of columns to include")
	var where stringListFlag
	fs.Var(&where, "where", "filter expression: column=value, column!=value, column~=substr, column^=prefix, column$=suffix")

	if err := fs.Parse(args); err != nil {
		return 2
	}
	if fs.NArg() < 1 {
		fmt.Fprintln(stderr, "head requires at least one parquet file or pattern")
		return 2
	}
	if *limit < 0 {
		fmt.Fprintln(stderr, "--n must be >= 0")
		return 2
	}

	filters, ok := parseFilters(where, stderr)
	if !ok {
		return 2
	}
	data, err := parquettool.ReadRows(fs.Args(), parquettool.ReadOptions{
		Limit:   *limit,
		Columns: parseColumns(*columnsFlag),
		Filters: filters,
	})
	if err != nil {
		return printCommandError(stderr, err)
	}
	output, err := formatRowOutput(data, *format, *width)
	if err != nil {
		return printCommandError(stderr, err)
	}
	if _, err := io.WriteString(stdout, output); err != nil {
		fmt.Fprintf(stderr, "write output: %v\n", err)
		return 1
	}
	return 0
}

func runTail(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("tail", flag.ContinueOnError)
	fs.SetOutput(stderr)

	limit := fs.Int("n", 10, "number of rows to print")
	width := fs.Int("width", 40, "maximum display width for each cell")
	format := fs.String("format", "table", "output format: table, json, jsonl")
	columnsFlag := fs.String("columns", "", "comma-separated list of columns to include")
	var where stringListFlag
	fs.Var(&where, "where", "filter expression: column=value, column!=value, column~=substr, column^=prefix, column$=suffix")

	if err := fs.Parse(args); err != nil {
		return 2
	}
	if fs.NArg() < 1 {
		fmt.Fprintln(stderr, "tail requires at least one parquet file or pattern")
		return 2
	}
	if *limit < 0 {
		fmt.Fprintln(stderr, "--n must be >= 0")
		return 2
	}

	filters, ok := parseFilters(where, stderr)
	if !ok {
		return 2
	}
	data, err := parquettool.ReadTailRowsWithOptions(fs.Args(), parquettool.ReadOptions{
		Limit:   *limit,
		Columns: parseColumns(*columnsFlag),
		Filters: filters,
	})
	if err != nil {
		return printCommandError(stderr, err)
	}
	output, err := formatRowOutput(data, *format, *width)
	if err != nil {
		return printCommandError(stderr, err)
	}
	if _, err := io.WriteString(stdout, output); err != nil {
		fmt.Fprintf(stderr, "write output: %v\n", err)
		return 1
	}
	return 0
}

func runCSV(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("csv", flag.ContinueOnError)
	fs.SetOutput(stderr)

	limit := fs.Int("limit", 0, "maximum number of rows to export; use 0 for all rows")
	noHeader := fs.Bool("no-header", false, "omit the header row")
	columnsFlag := fs.String("columns", "", "comma-separated list of columns to include")
	var where stringListFlag
	fs.Var(&where, "where", "filter expression: column=value, column!=value, column~=substr, column^=prefix, column$=suffix")

	if err := fs.Parse(args); err != nil {
		return 2
	}
	if fs.NArg() < 1 {
		fmt.Fprintln(stderr, "csv requires at least one parquet file or pattern")
		return 2
	}
	if *limit < 0 {
		fmt.Fprintln(stderr, "--limit must be >= 0")
		return 2
	}

	filters, ok := parseFilters(where, stderr)
	if !ok {
		return 2
	}
	if err := parquettool.WriteCSVWithOptions(stdout, fs.Args(), parquettool.ReadOptions{
		Limit:   *limit,
		Columns: parseColumns(*columnsFlag),
		Filters: filters,
	}, !*noHeader); err != nil {
		return printCommandError(stderr, err)
	}
	return 0
}

func runCount(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("count", flag.ContinueOnError)
	fs.SetOutput(stderr)
	format := fs.String("format", "text", "output format: text, json")
	var where stringListFlag
	fs.Var(&where, "where", "filter expression: column=value, column!=value, column~=substr, column^=prefix, column$=suffix")

	if err := fs.Parse(args); err != nil {
		return 2
	}
	if fs.NArg() < 1 {
		fmt.Fprintln(stderr, "count requires at least one parquet file or pattern")
		return 2
	}

	filters, ok := parseFilters(where, stderr)
	if !ok {
		return 2
	}
	count, err := parquettool.CountRows(fs.Args(), filters)
	if err != nil {
		return printCommandError(stderr, err)
	}
	switch *format {
	case "text":
		_, _ = fmt.Fprintf(stdout, "%d\n", count)
	case "json":
		_, _ = fmt.Fprintf(stdout, "{\n  \"count\": %d\n}\n", count)
	default:
		return printCommandError(stderr, &parquettool.UsageError{Message: fmt.Sprintf("unsupported format %q", *format)})
	}
	return 0
}

func runSchema(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("schema", flag.ContinueOnError)
	fs.SetOutput(stderr)
	format := fs.String("format", "text", "output format: text, json")

	if err := fs.Parse(args); err != nil {
		return 2
	}
	if fs.NArg() < 1 {
		fmt.Fprintln(stderr, "schema requires at least one parquet file or pattern")
		return 2
	}

	schemas, err := parquettool.LoadSchemas(fs.Args())
	if err != nil {
		return printCommandError(stderr, err)
	}
	output, err := formatSchemaOutput(schemas, *format)
	if err != nil {
		return printCommandError(stderr, err)
	}
	if _, err := io.WriteString(stdout, output); err != nil {
		fmt.Fprintf(stderr, "write output: %v\n", err)
		return 1
	}
	return 0
}

func runInspect(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("inspect", flag.ContinueOnError)
	fs.SetOutput(stderr)
	format := fs.String("format", "text", "output format: text, json")

	if err := fs.Parse(args); err != nil {
		return 2
	}
	if fs.NArg() < 1 {
		fmt.Fprintln(stderr, "inspect requires at least one parquet file or pattern")
		return 2
	}

	inspections, err := parquettool.Inspect(fs.Args())
	if err != nil {
		return printCommandError(stderr, err)
	}

	output, err := formatInspectOutput(inspections, *format)
	if err != nil {
		return printCommandError(stderr, err)
	}
	if _, err := io.WriteString(stdout, output); err != nil {
		fmt.Fprintf(stderr, "write output: %v\n", err)
		return 1
	}
	return 0
}

func formatRowOutput(data *parquettool.RowData, format string, width int) (string, error) {
	switch format {
	case "table":
		return render.FormatRows(data.Columns, data.Rows, width), nil
	case "json":
		return render.FormatRowDataJSON(data)
	case "jsonl":
		return render.FormatRowDataJSONL(data)
	default:
		return "", &parquettool.UsageError{Message: fmt.Sprintf("unsupported format %q", format)}
	}
}

func formatSchemaOutput(schemas []parquettool.SchemaView, format string) (string, error) {
	switch format {
	case "text":
		return render.FormatSchemas(schemas), nil
	case "json":
		return render.FormatSchemasJSON(schemas)
	default:
		return "", &parquettool.UsageError{Message: fmt.Sprintf("unsupported format %q", format)}
	}
}

func formatInspectOutput(inspections []parquettool.Inspection, format string) (string, error) {
	switch format {
	case "text":
		return render.FormatInspections(inspections), nil
	case "json":
		return render.FormatInspectionsJSON(inspections)
	default:
		return "", &parquettool.UsageError{Message: fmt.Sprintf("unsupported format %q", format)}
	}
}

func parseColumns(raw string) []string {
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

func parseFilters(exprs []string, stderr io.Writer) ([]parquettool.RowFilter, bool) {
	filters, err := parquettool.ParseFilters(exprs)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, err.Error())
		return nil, false
	}
	return filters, true
}

func printCommandError(stderr io.Writer, err error) int {
	if err == nil {
		return 0
	}

	var usageErr *parquettool.UsageError
	if errors.As(err, &usageErr) {
		fmt.Fprintln(stderr, usageErr.Error())
		return 2
	}

	fmt.Fprintf(stderr, "error: %v\n", err)
	return 1
}

func printUsage(w io.Writer) {
	_, _ = io.WriteString(w, strings.TrimSpace(`
go-parquet-tools reads parquet files from the command line.

Usage:
  go-parquet-tools <command> [flags] <file-or-pattern> ...

Commands:
  head      Print the first N rows
  tail      Print the last N rows
  count     Print the total number of rows
  schema    Print the parquet schema
  show      Print rows as a readable table
  csv       Export rows as CSV
  inspect   Print schema and file metadata

Examples:
  go-parquet-tools head -n 10 sample.parquet
  go-parquet-tools head -n 10 --format json sample.parquet
  go-parquet-tools tail -n 10 sample.parquet
  go-parquet-tools count data/*.parquet
  go-parquet-tools schema sample.parquet
  go-parquet-tools show sample.parquet
  go-parquet-tools show --limit 5 --width 24 data/*.parquet
  go-parquet-tools csv s3://bucket/path/*.parquet > sample.csv
  go-parquet-tools inspect sample.parquet
`)+"\n")
}
