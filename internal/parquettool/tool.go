package parquettool

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/format"
)

type UsageError struct {
	Message string
}

func (e *UsageError) Error() string {
	return e.Message
}

type RowData struct {
	Columns   []string
	Rows      [][]string
	TotalRows int64
	Truncated bool
}

type Inspection struct {
	Path             string
	Size             int64
	Rows             int64
	RowGroups        []RowGroup
	LeafColumns      []LeafColumn
	Schema           []SchemaNode
	CreatedBy        string
	FormatVersion    string
	KeyValueMetadata []KeyValue
}

type RowGroup struct {
	Index              int
	Rows               int64
	TotalByteSize      int64
	TotalCompressed    int64
	TotalUncompressed  int64
	SortingColumnCount int
}

type LeafColumn struct {
	Name               string
	Path               string
	Physical           string
	Logical            string
	Repetition         string
	Compression        string
	MaxDefinitionLevel int
	MaxRepetitionLevel int
}

type SchemaNode struct {
	Name       string
	Physical   string
	Logical    string
	Repetition string
	Children   []SchemaNode
}

type KeyValue struct {
	Key   string
	Value string
}

type sourceRef struct {
	Display string
	Open    func() (*openedSource, error)
}

type openedSource struct {
	Path   string
	Reader *parquet.File
	Size   int64
	Close  func() error
}

type rowRecord map[string]string

func ReadRows(patterns []string, limit int) (*RowData, error) {
	refs, err := resolveInputs(patterns)
	if err != nil {
		return nil, err
	}

	columns := make([]string, 0)
	records := make([]rowRecord, 0)
	var totalRows int64
	var truncated bool

	for _, ref := range refs {
		src, err := ref.Open()
		if err != nil {
			return nil, err
		}

		reader := parquet.NewGenericReader[any](src.Reader)
		fileRows := reader.NumRows()
		totalRows += fileRows

		remaining := int(fileRows)
		if limit > 0 {
			left := limit - len(records)
			if left <= 0 {
				truncated = true
				reader.Close()
				_ = src.Close()
				break
			}
			if remaining > left {
				remaining = left
				truncated = true
			}
		}

		fileColumns := topLevelColumns(src.Reader.Root())
		appendMissingColumns(&columns, fileColumns)

		readErr := readRowsInBatches(reader, remaining, func(row any) {
			record := stringifyTopLevelRow(row, fileColumns)
			records = append(records, record)
		})
		reader.Close()
		closeErr := src.Close()
		if readErr != nil {
			return nil, fmt.Errorf("read rows from %s: %w", ref.Display, readErr)
		}
		if closeErr != nil {
			return nil, closeErr
		}
	}

	renderedRows := make([][]string, 0, len(records))
	for _, record := range records {
		renderedRows = append(renderedRows, renderRecord(record, columns))
	}

	return &RowData{
		Columns:   columns,
		Rows:      renderedRows,
		TotalRows: totalRows,
		Truncated: truncated,
	}, nil
}

func WriteCSV(w io.Writer, patterns []string, limit int, includeHeader bool) error {
	data, err := ReadRows(patterns, limit)
	if err != nil {
		return err
	}

	cw := csv.NewWriter(w)
	if includeHeader {
		if err := cw.Write(data.Columns); err != nil {
			return fmt.Errorf("write header: %w", err)
		}
	}
	for _, row := range data.Rows {
		if err := cw.Write(row); err != nil {
			return fmt.Errorf("write row: %w", err)
		}
	}
	cw.Flush()
	if err := cw.Error(); err != nil {
		return fmt.Errorf("flush csv: %w", err)
	}
	return nil
}

func Inspect(patterns []string) ([]Inspection, error) {
	refs, err := resolveInputs(patterns)
	if err != nil {
		return nil, err
	}

	inspections := make([]Inspection, 0, len(refs))
	for _, ref := range refs {
		src, err := ref.Open()
		if err != nil {
			return nil, err
		}

		metadata := src.Reader.Metadata()
		rowGroups := make([]RowGroup, 0, len(metadata.RowGroups))
		for i, rg := range metadata.RowGroups {
			rowGroup := RowGroup{
				Index:              i,
				Rows:               rg.NumRows,
				TotalByteSize:      rg.TotalByteSize,
				SortingColumnCount: len(rg.SortingColumns),
			}
			for _, col := range rg.Columns {
				rowGroup.TotalCompressed += col.MetaData.TotalCompressedSize
				rowGroup.TotalUncompressed += col.MetaData.TotalUncompressedSize
			}
			rowGroups = append(rowGroups, rowGroup)
		}

		keyValues := make([]KeyValue, 0, len(metadata.KeyValueMetadata))
		for _, item := range metadata.KeyValueMetadata {
			keyValues = append(keyValues, KeyValue{Key: item.Key, Value: item.Value})
		}

		inspection := Inspection{
			Path:             ref.Display,
			Size:             src.Size,
			Rows:             src.Reader.NumRows(),
			RowGroups:        rowGroups,
			LeafColumns:      collectLeafColumns(src.Reader.Root()),
			Schema:           buildSchemaTree(src.Reader.Root()),
			CreatedBy:        metadata.CreatedBy,
			FormatVersion:    formatVersion(metadata.Version),
			KeyValueMetadata: keyValues,
		}

		if err := src.Close(); err != nil {
			return nil, err
		}
		inspections = append(inspections, inspection)
	}

	return inspections, nil
}

func resolveInputs(patterns []string) ([]sourceRef, error) {
	if len(patterns) == 0 {
		return nil, &UsageError{Message: "at least one parquet path is required"}
	}

	refs := make([]sourceRef, 0)
	for _, pattern := range patterns {
		if strings.TrimSpace(pattern) == "" {
			return nil, &UsageError{Message: "file path cannot be empty"}
		}

		if strings.HasPrefix(pattern, "s3://") {
			s3Refs, err := resolveS3Pattern(pattern)
			if err != nil {
				return nil, err
			}
			refs = append(refs, s3Refs...)
			continue
		}

		localRefs, err := resolveLocalPattern(pattern)
		if err != nil {
			return nil, err
		}
		refs = append(refs, localRefs...)
	}

	if len(refs) == 0 {
		return nil, &UsageError{Message: "no parquet files matched the input"}
	}
	return refs, nil
}

func resolveLocalPattern(pattern string) ([]sourceRef, error) {
	matches := []string{pattern}
	if hasGlob(pattern) {
		var err error
		matches, err = filepath.Glob(pattern)
		if err != nil {
			return nil, &UsageError{Message: fmt.Sprintf("invalid glob pattern %q", pattern)}
		}
		if len(matches) == 0 {
			return nil, &UsageError{Message: fmt.Sprintf("no parquet files matched %q", pattern)}
		}
	}

	slices.Sort(matches)
	refs := make([]sourceRef, 0, len(matches))
	for _, match := range matches {
		clean := filepath.Clean(match)
		info, err := os.Stat(clean)
		if err != nil {
			return nil, fmt.Errorf("stat file: %w", err)
		}
		if info.IsDir() {
			return nil, &UsageError{Message: fmt.Sprintf("path %q is a directory", clean)}
		}

		localPath := clean
		refs = append(refs, sourceRef{
			Display: localPath,
			Open: func() (*openedSource, error) {
				f, err := os.Open(localPath)
				if err != nil {
					return nil, fmt.Errorf("open parquet file: %w", err)
				}
				info, err := f.Stat()
				if err != nil {
					_ = f.Close()
					return nil, fmt.Errorf("stat parquet file: %w", err)
				}
				pf, err := parquet.OpenFile(f, info.Size())
				if err != nil {
					_ = f.Close()
					return nil, fmt.Errorf("open parquet metadata: %w", err)
				}
				return &openedSource{
					Path:   localPath,
					Reader: pf,
					Size:   info.Size(),
					Close:  f.Close,
				}, nil
			},
		})
	}
	return refs, nil
}

func resolveS3Pattern(raw string) ([]sourceRef, error) {
	bucket, keyPattern, err := parseS3URI(raw)
	if err != nil {
		return nil, err
	}

	client, err := newS3Client(context.Background())
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}

	keys := []string{keyPattern}
	if hasGlob(keyPattern) {
		keys, err = listMatchingS3Keys(context.Background(), client, bucket, keyPattern)
		if err != nil {
			return nil, err
		}
		if len(keys) == 0 {
			return nil, &UsageError{Message: fmt.Sprintf("no parquet files matched %q", raw)}
		}
	}

	refs := make([]sourceRef, 0, len(keys))
	for _, key := range keys {
		display := "s3://" + bucket + "/" + key
		b := bucket
		k := key
		refs = append(refs, sourceRef{
			Display: display,
			Open: func() (*openedSource, error) {
				body, size, err := downloadS3Object(context.Background(), client, b, k)
				if err != nil {
					return nil, fmt.Errorf("read %s: %w", display, err)
				}
				reader := bytes.NewReader(body)
				pf, err := parquet.OpenFile(reader, size)
				if err != nil {
					return nil, fmt.Errorf("open parquet metadata: %w", err)
				}
				return &openedSource{
					Path:   display,
					Reader: pf,
					Size:   size,
					Close:  func() error { return nil },
				}, nil
			},
		})
	}
	return refs, nil
}

func newS3Client(ctx context.Context) (*s3.Client, error) {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, err
	}
	return s3.NewFromConfig(cfg), nil
}

func listMatchingS3Keys(ctx context.Context, client *s3.Client, bucket, keyPattern string) ([]string, error) {
	prefix := s3ListPrefix(keyPattern)
	matches := make([]string, 0)
	p := s3.NewListObjectsV2Paginator(client, &s3.ListObjectsV2Input{
		Bucket: &bucket,
		Prefix: &prefix,
	})

	for p.HasMorePages() {
		page, err := p.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("list s3 objects: %w", err)
		}
		for _, obj := range page.Contents {
			if obj.Key == nil {
				continue
			}
			ok, err := path.Match(keyPattern, *obj.Key)
			if err != nil {
				return nil, &UsageError{Message: fmt.Sprintf("invalid s3 glob pattern %q", keyPattern)}
			}
			if ok {
				matches = append(matches, *obj.Key)
			}
		}
	}

	slices.Sort(matches)
	return matches, nil
}

func s3ListPrefix(pattern string) string {
	for i, r := range pattern {
		if r == '*' || r == '?' || r == '[' {
			j := strings.LastIndex(pattern[:i], "/")
			if j < 0 {
				return ""
			}
			return pattern[:j+1]
		}
	}
	return pattern
}

func downloadS3Object(ctx context.Context, client *s3.Client, bucket, key string) ([]byte, int64, error) {
	out, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &bucket,
		Key:    &key,
	})
	if err != nil {
		return nil, 0, err
	}
	defer out.Body.Close()

	body, err := io.ReadAll(out.Body)
	if err != nil {
		return nil, 0, err
	}
	return body, int64(len(body)), nil
}

func parseS3URI(raw string) (bucket string, key string, err error) {
	trimmed := strings.TrimPrefix(raw, "s3://")
	parts := strings.SplitN(trimmed, "/", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", "", &UsageError{Message: fmt.Sprintf("invalid s3 uri %q", raw)}
	}
	return parts[0], parts[1], nil
}

func hasGlob(pattern string) bool {
	return strings.ContainsAny(pattern, "*?[")
}

func topLevelColumns(root *parquet.Column) []string {
	columns := make([]string, 0, len(root.Columns()))
	for _, child := range root.Columns() {
		columns = append(columns, child.Name())
	}
	return columns
}

func appendMissingColumns(dst *[]string, columns []string) {
	for _, column := range columns {
		if !slices.Contains(*dst, column) {
			*dst = append(*dst, column)
		}
	}
}

func stringifyTopLevelRow(row any, columns []string) rowRecord {
	record := make(rowRecord, len(columns))
	rowMap, ok := row.(map[string]any)
	if !ok {
		if len(columns) > 0 {
			record[columns[0]] = stringifyValue(row)
		}
		return record
	}

	for _, column := range columns {
		record[column] = stringifyValue(rowMap[column])
	}
	return record
}

func renderRecord(record rowRecord, columns []string) []string {
	values := make([]string, len(columns))
	for i, column := range columns {
		values[i] = record[column]
	}
	return values
}

func stringifyValue(v any) string {
	if v == nil {
		return "null"
	}

	switch value := v.(type) {
	case string:
		return value
	case []byte:
		return string(value)
	case bool:
		return strconv.FormatBool(value)
	case int:
		return strconv.Itoa(value)
	case int32:
		return strconv.FormatInt(int64(value), 10)
	case int64:
		return strconv.FormatInt(value, 10)
	case uint32:
		return strconv.FormatUint(uint64(value), 10)
	case uint64:
		return strconv.FormatUint(value, 10)
	case float32:
		return strconv.FormatFloat(float64(value), 'f', -1, 32)
	case float64:
		return strconv.FormatFloat(value, 'f', -1, 64)
	case fmt.Stringer:
		return value.String()
	default:
		buf, err := json.Marshal(v)
		if err != nil {
			return fmt.Sprint(v)
		}
		return string(buf)
	}
}

func readRowsInBatches(reader *parquet.GenericReader[any], total int, emit func(any)) error {
	remaining := total
	for remaining > 0 {
		batchSize := min(remaining, 128)
		rows := make([]any, batchSize)
		n, err := reader.Read(rows)
		if n > 0 {
			for _, row := range rows[:n] {
				emit(row)
			}
			remaining -= n
		}
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		if n == 0 {
			break
		}
	}
	return nil
}

func formatVersion(version int32) string {
	if version == 0 {
		return "unknown"
	}
	return strconv.Itoa(int(version))
}

func collectLeafColumns(root *parquet.Column) []LeafColumn {
	leaves := make([]LeafColumn, 0)
	var walk func(*parquet.Column)
	walk = func(col *parquet.Column) {
		if len(col.Columns()) == 0 {
			leaf := LeafColumn{
				Name:               col.Name(),
				Path:               strings.Join(col.Path(), "."),
				Physical:           col.Type().Kind().String(),
				Logical:            logicalTypeString(col.Type().LogicalType()),
				Repetition:         repetitionString(col),
				Compression:        col.Compression().String(),
				MaxDefinitionLevel: col.MaxDefinitionLevel(),
				MaxRepetitionLevel: col.MaxRepetitionLevel(),
			}
			leaves = append(leaves, leaf)
			return
		}
		for _, child := range col.Columns() {
			walk(child)
		}
	}
	for _, child := range root.Columns() {
		walk(child)
	}
	sortLeafColumns(leaves)
	return leaves
}

func buildSchemaTree(root *parquet.Column) []SchemaNode {
	nodes := make([]SchemaNode, 0, len(root.Columns()))
	for _, child := range root.Columns() {
		nodes = append(nodes, buildSchemaNode(child))
	}
	return nodes
}

func buildSchemaNode(col *parquet.Column) SchemaNode {
	node := SchemaNode{
		Name:       col.Name(),
		Physical:   "group",
		Logical:    logicalTypeString(col.Type().LogicalType()),
		Repetition: repetitionString(col),
	}
	if len(col.Columns()) == 0 {
		node.Physical = col.Type().Kind().String()
	}
	for _, child := range col.Columns() {
		node.Children = append(node.Children, buildSchemaNode(child))
	}
	return node
}

func logicalTypeString(logical *format.LogicalType) string {
	if logical == nil {
		return "None"
	}
	return logical.String()
}

func repetitionString(col *parquet.Column) string {
	switch {
	case col.Optional():
		return "OPTIONAL"
	case col.Repeated():
		return "REPEATED"
	default:
		return "REQUIRED"
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
