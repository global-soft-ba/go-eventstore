package sqExtend

import sq "github.com/Masterminds/squirrel"

func IsNull(expr sq.Sqlizer) sq.Sqlizer {
	return is{
		expression: expr,
		null:       true,
	}
}

func IsNotNull(expr sq.Sqlizer) sq.Sqlizer {
	return is{
		expression: expr,
		null:       false,
	}
}

type is struct {
	expression sq.Sqlizer
	null       bool
}

func (i is) ToSql() (string, []interface{}, error) {
	stmt, val, err := i.expression.ToSql()
	if err != nil {
		return "", []interface{}{}, err
	}

	switch i.null {
	case true:
		return sq.Expr(stmt+" IS NULL", val).ToSql()
	default:
		return sq.Expr(stmt+" IS NOT NULL", val).ToSql()
	}

}
