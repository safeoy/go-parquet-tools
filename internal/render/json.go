package render

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/safeoy/go-parquet-tools/internal/parquettool"
)

func FormatRowDataJSON(data *parquettool.RowData) (string, error) {
	records := rowRecords(data.Columns, data.Rows)
	buf, err := json.MarshalIndent(records, "", "  ")
	if err != nil {
		return "", fmt.Errorf("marshal json: %w", err)
	}
	return string(buf) + "\n", nil
}

func FormatRowDataJSONL(data *parquettool.RowData) (string, error) {
	records := rowRecords(data.Columns, data.Rows)
	var b strings.Builder
	for _, record := range records {
		buf, err := json.Marshal(record)
		if err != nil {
			return "", fmt.Errorf("marshal jsonl: %w", err)
		}
		b.Write(buf)
		b.WriteByte('\n')
	}
	return b.String(), nil
}

func FormatInspectionsJSON(items []parquettool.Inspection) (string, error) {
	buf, err := json.MarshalIndent(items, "", "  ")
	if err != nil {
		return "", fmt.Errorf("marshal inspections json: %w", err)
	}
	return string(buf) + "\n", nil
}

func FormatSchemasJSON(items []parquettool.SchemaView) (string, error) {
	buf, err := json.MarshalIndent(items, "", "  ")
	if err != nil {
		return "", fmt.Errorf("marshal schemas json: %w", err)
	}
	return string(buf) + "\n", nil
}

func rowRecords(columns []string, rows [][]string) []map[string]string {
	records := make([]map[string]string, 0, len(rows))
	for _, row := range rows {
		record := make(map[string]string, len(columns))
		for i, column := range columns {
			if i < len(row) {
				record[column] = row[i]
			} else {
				record[column] = ""
			}
		}
		records = append(records, record)
	}
	return records
}
