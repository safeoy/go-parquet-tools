package render

import (
	"fmt"
	"strings"

	"github.com/safeoy/go-parquet-tools/internal/parquettool"
)

func FormatInspections(items []parquettool.Inspection) string {
	var parts []string
	for _, item := range items {
		parts = append(parts, FormatInspection(item))
	}
	return strings.Join(parts, "\n")
}

func FormatInspection(in parquettool.Inspection) string {
	var b strings.Builder

	b.WriteString("############ file meta data ############\n")
	fmt.Fprintf(&b, "file: %s\n", in.Path)
	if in.CreatedBy != "" {
		fmt.Fprintf(&b, "created_by: %s\n", in.CreatedBy)
	}
	fmt.Fprintf(&b, "num_columns: %d\n", len(in.LeafColumns))
	fmt.Fprintf(&b, "num_rows: %d\n", in.Rows)
	fmt.Fprintf(&b, "num_row_groups: %d\n", len(in.RowGroups))
	fmt.Fprintf(&b, "format_version: %s\n", in.FormatVersion)
	fmt.Fprintf(&b, "serialized_size: %d\n", in.Size)

	if len(in.KeyValueMetadata) > 0 {
		b.WriteString("\n############ key_value_metadata ############\n")
		for _, item := range in.KeyValueMetadata {
			fmt.Fprintf(&b, "%s=%s\n", item.Key, item.Value)
		}
	}

	b.WriteString("\n############ Columns ############\n")
	for _, column := range in.LeafColumns {
		b.WriteString(column.Path)
		b.WriteByte('\n')
	}

	for _, column := range in.LeafColumns {
		fmt.Fprintf(&b, "\n############ Column(%s) ############\n", column.Name)
		fmt.Fprintf(&b, "name: %s\n", column.Name)
		fmt.Fprintf(&b, "path: %s\n", column.Path)
		fmt.Fprintf(&b, "max_definition_level: %d\n", column.MaxDefinitionLevel)
		fmt.Fprintf(&b, "max_repetition_level: %d\n", column.MaxRepetitionLevel)
		fmt.Fprintf(&b, "physical_type: %s\n", column.Physical)
		fmt.Fprintf(&b, "logical_type: %s\n", column.Logical)
		fmt.Fprintf(&b, "repetition_type: %s\n", column.Repetition)
		fmt.Fprintf(&b, "compression: %s\n", column.Compression)
	}

	if len(in.RowGroups) > 0 {
		b.WriteString("\n############ RowGroups ############\n")
		b.WriteString(FormatRows(
			[]string{"index", "rows", "bytes", "compressed", "uncompressed", "sorting_cols"},
			rowGroupRows(in.RowGroups),
			18,
		))
	}

	return b.String()
}

func rowGroupRows(groups []parquettool.RowGroup) [][]string {
	rows := make([][]string, 0, len(groups))
	for _, group := range groups {
		rows = append(rows, []string{
			fmt.Sprintf("%d", group.Index),
			fmt.Sprintf("%d", group.Rows),
			fmt.Sprintf("%d", group.TotalByteSize),
			fmt.Sprintf("%d", group.TotalCompressed),
			fmt.Sprintf("%d", group.TotalUncompressed),
			fmt.Sprintf("%d", group.SortingColumnCount),
		})
	}
	return rows
}
