package sqExtend

import (
	"bytes"
	sq "github.com/Masterminds/squirrel"
)

func ArrayAppend(in sq.Sqlizer, out sq.Sqlizer) sq.Sqlizer {
	return arrayAppend{
		in:  in,
		out: out,
	}
}

type arrayAppend struct {
	in, out sq.Sqlizer
}

func (a arrayAppend) ToSql() (string, []interface{}, error) {

	stmtIn, valIn, err := a.in.ToSql()
	if err != nil {
		return "", nil, err
	}
	stmtOut, valOut, err := a.out.ToSql()
	if err != nil {
		return "", nil, err
	}

	var buf bytes.Buffer

	buf.WriteString("ARRAY_APPEND(")
	buf.WriteString(stmtIn)
	buf.WriteString(",")
	buf.WriteString(stmtOut)
	buf.WriteString(")")

	return buf.String(), append(valIn, valOut...), nil
}
