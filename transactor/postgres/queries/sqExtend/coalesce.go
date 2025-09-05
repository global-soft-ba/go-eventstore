package sqExtend

import (
	"bytes"
	sq "github.com/Masterminds/squirrel"
)

func Coalesce(args ...sq.Sqlizer) sq.Sqlizer {
	return coalesce{
		args: args,
	}
}

type coalesce struct {
	args []sq.Sqlizer
}

func (c coalesce) ToSql() (string, []interface{}, error) {
	var buf bytes.Buffer
	var values []interface{}

	buf.WriteString("COALESCE(")

	for i := 0; i < len(c.args); i++ {
		stmt, val, err := c.args[i].ToSql()
		if err != nil {
			return "", nil, err
		}
		buf.WriteString(stmt)
		if i < len(c.args)-1 {
			buf.WriteString(",")
		}
		values = append(values, val...)
	}

	buf.WriteString(")")

	return buf.String(), values, nil
}
