package render

import (
	"fmt"
	"strings"

	"github.com/safeoy/go-parquet-tools/internal/parquettool"
)

func FormatInspection(in *parquettool.Inspection) string {
	var b strings.Builder

	fmt.Fprintf(&b, "File: %s\n", in.Path)
	fmt.Fprintf(&b, "Rows: %d\n", in.Rows)
	fmt.Fprintf(&b, "Row Groups: %d\n", len(in.RowGroups))
	fmt.Fprintf(&b, "Leaf Columns: %d\n", len(in.LeafColumns))
	fmt.Fprintf(&b, "Format Version: %s\n", in.FormatVersion)
	if in.CreatedBy != "" {
		fmt.Fprintf(&b, "Created By: %s\n", in.CreatedBy)
	}

	if len(in.KeyValueMetadata) > 0 {
		b.WriteString("\nKey Value Metadata\n")
		for _, item := range in.KeyValueMetadata {
			fmt.Fprintf(&b, "- %s=%s\n", item.Key, item.Value)
		}
	}

	b.WriteString("\nSchema\n")
	for _, node := range in.Schema {
		writeSchemaNode(&b, node, 0)
	}

	if len(in.LeafColumns) > 0 {
		b.WriteString("\nLeaf Columns\n")
		b.WriteString(FormatRows(
			[]string{"path", "physical", "logical", "repetition", "compression"},
			leafColumnRows(in.LeafColumns),
			32,
		))
	}

	if len(in.RowGroups) > 0 {
		b.WriteString("\nRow Groups\n")
		b.WriteString(FormatRows(
			[]string{"index", "rows", "bytes", "compressed", "uncompressed", "sorting_cols"},
			rowGroupRows(in.RowGroups),
			18,
		))
	}

	return b.String()
}

func writeSchemaNode(b *strings.Builder, node parquettool.SchemaNode, depth int) {
	indent := strings.Repeat("  ", depth)
	label := node.Name
	parts := make([]string, 0, 3)
	if node.Repetition != "" {
		parts = append(parts, strings.ToLower(node.Repetition))
	}
	if node.Physical != "" {
		parts = append(parts, strings.ToLower(node.Physical))
	}
	if node.Logical != "" {
		parts = append(parts, strings.ToLower(node.Logical))
	}
	if len(parts) > 0 {
		label += " [" + strings.Join(parts, ", ") + "]"
	}
	fmt.Fprintf(b, "%s- %s\n", indent, label)
	for _, child := range node.Children {
		writeSchemaNode(b, child, depth+1)
	}
}

func leafColumnRows(columns []parquettool.LeafColumn) [][]string {
	rows := make([][]string, 0, len(columns))
	for _, col := range columns {
		rows = append(rows, []string{
			col.Path,
			col.Physical,
			col.Logical,
			col.Repetition,
			col.Compression,
		})
	}
	return rows
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
