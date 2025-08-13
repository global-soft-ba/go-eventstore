package sqExtend

import (
	"bytes"
	sq "github.com/Masterminds/squirrel"
	"github.com/samber/lo"
	"strings"
)

func Window(expr sq.Sqlizer, partition PartitionExpr, order OrderExpression) sq.Sqlizer {
	return window{
		expression: expr,
		partition:  partition,
		order:      order,
	}
}

type PartitionExpr []string

type OrderExpression struct {
	SortExpression []string
	Order          string //[ASC | DESC]
}

type window struct {
	expression sq.Sqlizer
	partition  PartitionExpr
	order      OrderExpression
}

func (p window) ToSql() (string, []interface{}, error) {
	var buf bytes.Buffer

	stmt, val, err := p.expression.ToSql()
	if err != nil {
		return "", []interface{}{}, err
	}

	buf.WriteString(stmt)
	buf.WriteString(" OVER (")

	if len(p.partition) > 0 {
		buf.WriteString("PARTITION BY ")
		buf.WriteString(strings.Join(p.partition, ", "))
	}

	if len(p.order.SortExpression) > 0 {
		buf.WriteString(" ORDER BY ")
		buf.WriteString(strings.Join(p.order.SortExpression, ", "))

		if lo.Contains([]string{"ASC", "DESC"}, p.order.Order) {
			buf.WriteString(" ")
			buf.WriteString(p.order.Order)
		}
	}

	buf.WriteString(")")

	return buf.String(), val, nil
}
