package parquettool

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

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
	Path        string
	Physical    string
	Logical     string
	Repetition  string
	Compression string
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

func ReadRows(path string, limit int) (*RowData, error) {
	file, closeFile, err := openFile(path)
	if err != nil {
		return nil, err
	}
	defer closeFile()

	reader := parquet.NewGenericReader[any](file)
	defer reader.Close()

	totalRows := reader.NumRows()
	readCount := int(totalRows)
	truncated := false
	if limit > 0 && int64(limit) < totalRows {
		readCount = limit
		truncated = true
	}

	rows := make([]any, readCount)
	n, err := reader.Read(rows)
	if err != nil && err != io.EOF {
		return nil, fmt.Errorf("read rows: %w", err)
	}
	rows = rows[:n]

	columns := topLevelColumns(file.Root())
	renderedRows := make([][]string, 0, len(rows))
	for _, row := range rows {
		renderedRows = append(renderedRows, stringifyTopLevelRow(row, columns))
	}

	return &RowData{
		Columns:   columns,
		Rows:      renderedRows,
		TotalRows: totalRows,
		Truncated: truncated,
	}, nil
}

func WriteCSV(w io.Writer, path string, limit int, includeHeader bool) error {
	data, err := ReadRows(path, limit)
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

func Inspect(path string) (*Inspection, error) {
	file, closeFile, err := openFile(path)
	if err != nil {
		return nil, err
	}
	defer closeFile()

	metadata := file.Metadata()
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

	return &Inspection{
		Path:             filepath.Clean(path),
		Rows:             file.NumRows(),
		RowGroups:        rowGroups,
		LeafColumns:      collectLeafColumns(file.Root()),
		Schema:           buildSchemaTree(file.Root()),
		CreatedBy:        metadata.CreatedBy,
		FormatVersion:    formatVersion(metadata.Version),
		KeyValueMetadata: keyValues,
	}, nil
}

func openFile(path string) (*parquet.File, func() error, error) {
	if strings.TrimSpace(path) == "" {
		return nil, nil, &UsageError{Message: "file path cannot be empty"}
	}
	if stat, err := os.Stat(path); err != nil {
		return nil, nil, fmt.Errorf("stat file: %w", err)
	} else if stat.IsDir() {
		return nil, nil, &UsageError{Message: "path must point to a parquet file, not a directory"}
	}

	f, err := os.Open(path)
	if err != nil {
		return nil, nil, fmt.Errorf("open parquet file: %w", err)
	}
	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, nil, fmt.Errorf("stat parquet file: %w", err)
	}

	pf, err := parquet.OpenFile(f, info.Size())
	if err != nil {
		_ = f.Close()
		return nil, nil, fmt.Errorf("open parquet metadata: %w", err)
	}

	return pf, f.Close, nil
}

func topLevelColumns(root *parquet.Column) []string {
	columns := make([]string, 0, len(root.Columns()))
	for _, child := range root.Columns() {
		columns = append(columns, child.Name())
	}
	return columns
}

func stringifyTopLevelRow(row any, columns []string) []string {
	values := make([]string, len(columns))

	record, ok := row.(map[string]any)
	if !ok {
		if len(values) > 0 {
			values[0] = stringifyValue(row)
		}
		return values
	}

	for i, column := range columns {
		values[i] = stringifyValue(record[column])
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
			leaves = append(leaves, LeafColumn{
				Path:        strings.Join(col.Path(), "."),
				Physical:    col.Type().Kind().String(),
				Logical:     logicalTypeString(col.Type().LogicalType()),
				Repetition:  repetitionString(col),
				Compression: col.Compression().String(),
			})
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
		return ""
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
