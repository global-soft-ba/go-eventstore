package sqExtend

import (
	"bytes"
	sq "github.com/Masterminds/squirrel"
)

func Lag(column string) sq.Sqlizer {
	return windowFunctions{
		operation: "LAG",
		column:    column,
	}
}

func Lead(column string) sq.Sqlizer {
	return windowFunctions{
		operation: "LEAD",
		column:    column,
	}
}
func FirstValue(column string) sq.Sqlizer {
	return windowFunctions{
		operation: "FIRST_VALUE",
		column:    column,
	}
}

func LastValue(column string) sq.Sqlizer {
	return windowFunctions{
		operation: "LAST_VALUE",
		column:    column,
	}
}

func Rank() sq.Sqlizer {
	return windowFunctions{
		operation: "RANK",
	}
}

func RowNumber() sq.Sqlizer {
	return windowFunctions{
		operation: "ROW_NUMBER",
	}
}

func DenseRank() sq.Sqlizer {
	return windowFunctions{
		operation: "DENSE_RANK",
	}
}

type windowFunctions struct {
	operation string //LAG or LEAD
	column    string
}

func (l windowFunctions) ToSql() (string, []interface{}, error) {
	var buf bytes.Buffer

	buf.WriteString(l.operation)
	buf.WriteString("(")
	if l.column != "" {
		buf.WriteString(l.column)
	}
	buf.WriteString(")")

	return buf.String(), []interface{}{}, nil
}
