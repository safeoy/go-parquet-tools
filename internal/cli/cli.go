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

func Run(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		printUsage(stderr)
		return 2
	}

	switch args[0] {
	case "-h", "--help", "help":
		printUsage(stdout)
		return 0
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

	limit := fs.Int("limit", 20, "maximum number of rows to print; use 0 for all rows")
	width := fs.Int("width", 40, "maximum display width for each cell")

	if err := fs.Parse(args); err != nil {
		return 2
	}
	if fs.NArg() != 1 {
		fmt.Fprintln(stderr, "show requires exactly one parquet file")
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

	data, err := parquettool.ReadRows(fs.Arg(0), *limit)
	if err != nil {
		return printCommandError(stderr, err)
	}

	table := render.FormatRows(data.Columns, data.Rows, *width)
	if _, err := io.WriteString(stdout, table); err != nil {
		fmt.Fprintf(stderr, "write output: %v\n", err)
		return 1
	}

	if data.Truncated {
		_, _ = fmt.Fprintf(stdout, "\nshowing %d of %d rows\n", len(data.Rows), data.TotalRows)
	}

	return 0
}

func runCSV(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("csv", flag.ContinueOnError)
	fs.SetOutput(stderr)

	limit := fs.Int("limit", 0, "maximum number of rows to export; use 0 for all rows")
	noHeader := fs.Bool("no-header", false, "omit the header row")

	if err := fs.Parse(args); err != nil {
		return 2
	}
	if fs.NArg() != 1 {
		fmt.Fprintln(stderr, "csv requires exactly one parquet file")
		return 2
	}
	if *limit < 0 {
		fmt.Fprintln(stderr, "--limit must be >= 0")
		return 2
	}

	if err := parquettool.WriteCSV(stdout, fs.Arg(0), *limit, !*noHeader); err != nil {
		return printCommandError(stderr, err)
	}
	return 0
}

func runInspect(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("inspect", flag.ContinueOnError)
	fs.SetOutput(stderr)

	if err := fs.Parse(args); err != nil {
		return 2
	}
	if fs.NArg() != 1 {
		fmt.Fprintln(stderr, "inspect requires exactly one parquet file")
		return 2
	}

	inspection, err := parquettool.Inspect(fs.Arg(0))
	if err != nil {
		return printCommandError(stderr, err)
	}

	if _, err := io.WriteString(stdout, render.FormatInspection(inspection)); err != nil {
		fmt.Fprintf(stderr, "write output: %v\n", err)
		return 1
	}
	return 0
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
  go-parquet-tools <command> [flags] <file>

Commands:
  show      Print rows as a readable table
  csv       Export rows as CSV
  inspect   Print schema and file metadata

Examples:
  go-parquet-tools show sample.parquet
  go-parquet-tools show --limit 5 --width 24 sample.parquet
  go-parquet-tools csv sample.parquet > sample.csv
  go-parquet-tools inspect sample.parquet
`)+"\n")
}
