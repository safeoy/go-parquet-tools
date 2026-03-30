package render

import (
	"strings"
)

func FormatRows(columns []string, rows [][]string, maxWidth int) string {
	if len(columns) == 0 {
		return "(no columns)\n"
	}

	widths := make([]int, len(columns))
	for i, column := range columns {
		widths[i] = min(maxWidth, max(4, runeLen(column)))
	}
	for _, row := range rows {
		for i, cell := range row {
			if i >= len(widths) {
				continue
			}
			widths[i] = min(maxWidth, max(widths[i], runeLen(cell)))
		}
	}

	var b strings.Builder
	writeBorder(&b, widths)
	writeRow(&b, columns, widths)
	writeBorder(&b, widths)
	for _, row := range rows {
		writeRow(&b, row, widths)
	}
	writeBorder(&b, widths)
	return b.String()
}

func writeBorder(b *strings.Builder, widths []int) {
	b.WriteByte('+')
	for _, width := range widths {
		b.WriteString(strings.Repeat("-", width+2))
		b.WriteByte('+')
	}
	b.WriteByte('\n')
}

func writeRow(b *strings.Builder, row []string, widths []int) {
	b.WriteByte('|')
	for i, width := range widths {
		cell := ""
		if i < len(row) {
			cell = truncate(row[i], width)
		}
		b.WriteByte(' ')
		b.WriteString(padRight(cell, width))
		b.WriteByte(' ')
		b.WriteByte('|')
	}
	b.WriteByte('\n')
}

func padRight(s string, width int) string {
	padding := width - runeLen(s)
	if padding <= 0 {
		return s
	}
	return s + strings.Repeat(" ", padding)
}

func truncate(s string, width int) string {
	if runeLen(s) <= width {
		return s
	}
	if width <= 1 {
		return s[:width]
	}

	runes := []rune(s)
	return string(runes[:width-1]) + "…"
}

func runeLen(s string) int {
	return len([]rune(s))
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
