package sqExtend

import (
	sq "github.com/Masterminds/squirrel"
)

func AtTimeZone(column string, timeZone string) sq.Sqlizer {
	return atTimeZone{
		column:   column,
		timeZone: timeZone,
	}
}

type atTimeZone struct {
	column   string
	timeZone string
}

func (a atTimeZone) ToSql() (string, []interface{}, error) {
	return sq.Expr("? AT TIME ZONE ?", a.column, a.timeZone).ToSql()
}
