# go-parquet-tools

[English](./README.md) | [中文](./README.zh-CN.md) | [日本語](./README.ja.md)

A Parquet command-line tool written in Go. The goal is to cover the day-to-day workflows of the Python [`parquet-tools`](https://pypi.org/project/parquet-tools/) package while keeping distribution simple as a single binary.

The current version already provides a practical command set:

- `show`: display rows as a table
- `head`: display the first N rows
- `tail`: display the last N rows
- `csv`: export rows as CSV
- `count`: count rows
- `schema`: print the schema
- `inspect`: print file metadata, schema, row groups, and leaf-column details

The input model is intentionally close to the Python version:

- local files
- local glob patterns such as `data/*.parquet`
- S3 URIs
- S3 glob patterns such as `s3://bucket/prefix/*.parquet`

## Quick Start

```bash
go run . show ./sample.parquet
go run . show --limit 5 --width 24 ./data/*.parquet
go run . head -n 10 ./sample.parquet
go run . tail -n 10 ./sample.parquet
go run . csv ./sample.parquet > sample.csv
go run . csv s3://bucket/path/*.parquet > sample.csv
go run . count ./sample.parquet
go run . schema ./sample.parquet
go run . inspect ./sample.parquet
```

## Output Formats

Row-oriented commands support:

- `show --format table|json|jsonl`
- `head --format table|json|jsonl`
- `tail --format table|json|jsonl`

Structured metadata commands support:

- `count --format text|json`
- `schema --format text|json`
- `inspect --format text|json`

Examples:

```bash
go run . show --limit 1 --format json ./sample.parquet
go run . head -n 10 --format jsonl ./sample.parquet
go run . inspect --format json ./sample.parquet
```

## Column Selection And Filtering

Row-oriented commands support projection and simple string filters:

- `--columns a,b,c`
- `--where column=value`
- `--where column!=value`
- `--where column~=substr`
- `--where column^=prefix`
- `--where column$=suffix`

These options are available on:

- `show`
- `head`
- `tail`
- `csv`
- `count`

Examples:

```bash
go run . show --columns group,name --where group=2 ./sample.parquet
go run . head -n 5 --columns name --format jsonl --where name~=al ./sample.parquet
go run . count --where group=2 ./sample.parquet
```

## Design Choices

- Parquet decoding is based on `github.com/parquet-go/parquet-go`
- The CLI intentionally avoids Cobra for now and uses the standard library `flag` package to keep startup cost and dependencies low
- `show` and `csv` operate on top-level columns; complex nested values are currently serialized as JSON strings
- S3 reading currently downloads the full object into memory before parsing, prioritizing functionality and compatibility first

## Next Steps

- Add more input sources such as HTTP or HDFS
- Add numeric comparison filters and richer expression support
- Add column renaming, derived columns, and more flexible output control
- Improve streaming reads and memory control for very large files
