package sqExtend

import (
	"fmt"
	sq "github.com/Masterminds/squirrel"
	"github.com/samber/lo"
	"strings"
)

func TuplesComparison(ops string, columns []string, values []interface{}) sq.Sqlizer {
	return tuplesComparison{
		operator: ops,
		columns:  columns,
		values:   values,
	}
}

type tuplesComparison struct {
	operator string
	columns  []string
	values   []interface{}
}

func (t tuplesComparison) ToSql() (string, []interface{}, error) {
	if len(t.columns) != len(t.values) {
		return "", nil, fmt.Errorf("columns and values must have the same length")
	}

	if !lo.Contains([]string{"=", "<", ">", "<=", ">=", "<>", "!=", "IN", "NOT IN"}, t.operator) {
		return "", nil, fmt.Errorf("operator %q is not supported", t.operator)
	}

	var valMarks string
	for range t.columns {
		valMarks += "?,"
	}
	valMarks = valMarks[:len(valMarks)-1]

	return sq.Expr(fmt.Sprintf("(%s) %s (%s)", strings.Join(t.columns[:], ","), t.operator, valMarks), t.values...).ToSql()
}
