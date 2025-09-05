package sqExtend

import (
	"fmt"
	sq "github.com/Masterminds/squirrel"
)

func RangeContainsElement(rangeColumn string, anyElement interface{}) sq.Sqlizer {
	return rangeComparison{
		operator:        "@>",
		column:          rangeColumn,
		value:           anyElement,
		anyElementFirst: false,
	}
}

func ElementContainedInRange(anyElement interface{}, rangeColumn string) sq.Sqlizer {
	return rangeComparison{
		operator:        "<@",
		column:          rangeColumn,
		value:           anyElement,
		anyElementFirst: true,
	}
}

type rangeComparison struct {
	operator        string
	column          string
	value           interface{}
	anyElementFirst bool
}

func (t rangeComparison) ToSql() (string, []interface{}, error) {
	if t.column == "" {
		return "", nil, fmt.Errorf("no column was given")
	}

	var valMarks = "?"
	switch t.anyElementFirst {
	case true:
		return sq.Expr(fmt.Sprintf("%s %s %s", valMarks, t.operator, t.column), t.value).ToSql()
	default:
		return sq.Expr(fmt.Sprintf("%s %s %s", t.column, t.operator, valMarks), t.value).ToSql()
	}
}
