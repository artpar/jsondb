package jsondb
import (
	"code.google.com/p/vitess/go/vt/sqlparser"
)

type ColumnExpression struct {
	Node sqlparser.Node
}

type FromExpression struct {
	Node sqlparser.Node
}

type ExecutionModel struct {
	IsDistinct bool
	State      int
	Columns    []ColumnExpression
	FromTable  []FromExpression
}

type TableMap  map[string]string
func (t TableMap) GetTableByAlias(s string) string {
	val, ok := t[s]
	if !ok {
		log.Error("Table not found for alias - %s", s)
		return s
	}
	return val
}


