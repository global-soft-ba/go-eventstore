package sqExtend

import (
	"fmt"
	sq "github.com/Masterminds/squirrel"
	"github.com/samber/lo"
)

func TupleComparison(ops string, column string, values []interface{}) sq.Sqlizer {
	return tupleComparison{
		operator: ops,
		column:   column,
		values:   values,
	}
}

type tupleComparison struct {
	operator string
	column   string
	values   []interface{}
}

func (t tupleComparison) ToSql() (string, []interface{}, error) {
	if t.column == "" {
		return "", nil, fmt.Errorf("no column was given")
	}

	if !lo.Contains([]string{"IN", "NOT IN"}, t.operator) {
		return "", nil, fmt.Errorf("operator %q is not supported", t.operator)
	}

	var valMarks string
	for range t.values {
		valMarks += "?,"
	}
	valMarks = valMarks[:len(valMarks)-1]

	return sq.Expr(fmt.Sprintf("(%s) %s (%s)", t.column, t.operator, valMarks), t.values...).ToSql()
}
