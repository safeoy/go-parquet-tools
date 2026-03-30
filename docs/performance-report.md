# Performance Report

This report compares the Go CLI in this repository against the Python `parquet-tools` package on the overlapping commands: `show`, `csv`, and `inspect`.

## Environment

- Date: 2026-03-30
- Platform: macOS-26.4-x86_64-i386-64bit
- Machine: x86_64
- go version go1.26.1 darwin/arm64
- Python 3.11.2
- python parquet-tools: 0.2.16

## Dataset

- Path: `.tmp/perf/flat-100k.parquet`
- Rows: 100,000
- Columns: 7
- Schema: `id`, `group`, `name`, `city`, `score`, `active`, `payload`
- File size: 1.23 MiB
- Compression: snappy

## Method

- The Go binary is built once before benchmarking.
- The Python CLI runs from a local virtual environment at `.venv-perf`.
- Each command is warmed up once, then measured 5 times.
- Reported latency is median wall-clock time.
- Command output is discarded to focus on command execution time.

## Results

| Command | Go median | Python median | Go speedup |
| --- | ---: | ---: | ---: |
| `show first 100 rows` | 0.0145s | 0.7777s | 53.53x |
| `csv first 10000 rows` | 0.0274s | 0.8468s | 30.95x |
| `inspect file metadata` | 0.0114s | 0.7589s | 66.83x |

## Commands

```bash
.tmp/perf/go-parquet-tools show --limit 100 .tmp/perf/flat-100k.parquet
.venv-perf/bin/parquet-tools show --head 100 .tmp/perf/flat-100k.parquet
.tmp/perf/go-parquet-tools csv --limit 10000 .tmp/perf/flat-100k.parquet
.venv-perf/bin/parquet-tools csv --head 10000 .tmp/perf/flat-100k.parquet
.tmp/perf/go-parquet-tools inspect .tmp/perf/flat-100k.parquet
.venv-perf/bin/parquet-tools inspect .tmp/perf/flat-100k.parquet
```

## Notes

- This is an end-to-end CLI benchmark, not a library microbenchmark.
- The comparison only covers commands shared by both implementations.
- The current Go implementation is optimized for single-binary distribution, while the Python implementation relies on pandas and pyarrow.
- Re-run this report with `python3 ./scripts/perf_compare.py` after changing I/O, rendering, or schema handling code.
