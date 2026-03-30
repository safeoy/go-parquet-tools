package parquettool

import "sort"

func sortLeafColumns(columns []LeafColumn) {
	sort.Slice(columns, func(i, j int) bool {
		return columns[i].Path < columns[j].Path
	})
}
