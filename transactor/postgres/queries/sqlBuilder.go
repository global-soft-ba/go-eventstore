package queries

import (
	"fmt"
	"github.com/global-soft-ba/go-eventstore/transactor/postgres/temporal"
	"strings"

	sq "github.com/Masterminds/squirrel"
)

type SqlBuilder interface {
	GetDatabaseSchema() string
	GetPlaceholder() sq.PlaceholderFormat
	Build() sq.StatementBuilderType
	GetUsedTablesOfViews(viewSchema string, viewNames ...string) (string, []interface{}, error)
	DoesTableExist(schema string, tableName ...string) (string, []interface{}, error)

	TableWithSchema(name string) string
	TableWithSchemaAndAlias(name, alias string) string
	WithAlias(name, alias string) string
	WithColumnsPrefix(prefix string, columns ...string) []string
	WithColumnPrefix(prefix string, column string) string
	WithEscapedName(name string) string

	DistinctClauseON(columns ...string) string
	PrefixSelector(prefix string, columns map[string]interface{}) map[string]interface{}
	AsCTE(selectQuery sq.SelectBuilder, alias string) sq.SelectBuilder
	UnionAll(first sq.SelectBuilder, other ...sq.SelectBuilder) sq.SelectBuilder
	Union(first sq.SelectBuilder, other ...sq.SelectBuilder) sq.SelectBuilder
	JoinUsing(table1, table2 string, columns ...string) string
	AddJoinUsing(table string, columns ...string) string
	JoinLeftUsing(table1, table2 string, columns ...string) string
	InParenthese(clause string) string
	Greater(arg1, arg2 string) string
	IsNull(arg1 string) string
	DateTrunc(gran temporal.Granularity, column string) string
	AtTimeZone(column string, timeZone string) string
	Sum(column string) string
	Upper(column string) string
	Lower(column string) string
}

type OnConflict []string

func (c OnConflict) DoUpdateOrNothing(columns ...string) OnConflictWithReturning {
	var sb strings.Builder
	sb.WriteString("ON CONFLICT ")

	if len(c) > 0 {
		sb.WriteString(`(`)
		sb.WriteString(strings.Join(c, `,`))
		sb.WriteString(`) `)
	}

	sb.WriteString("DO ")

	if len(columns) == 0 {
		sb.WriteString("NOTHING")
		return OnConflictWithReturning{
			query:     sb.String(),
			isNothing: true,
		}
	}

	sb.WriteString("UPDATE SET ")
	const excluded = `"excluded".`

	for it, column := range columns {
		if it+1 == len(columns) {
			sb.WriteString(column + " = " + excluded + column + " ")
			continue
		}
		sb.WriteString(column + " = " + excluded + column + ", ")
	}

	return OnConflictWithReturning{
		query:     sb.String(),
		isNothing: false,
	}
}

type OnConflictWithReturning struct {
	query     string
	isNothing bool
}

func (r OnConflictWithReturning) WithReturningOrNothing(columns ...string) string {
	if r.isNothing || len(columns) == 0 {
		return r.query
	}

	var sb strings.Builder
	sb.WriteString(r.query + "RETURNING ")

	for it, column := range columns {
		if it+1 != len(columns) {
			sb.WriteString(column)
			continue
		}
		sb.WriteString(column)
	}

	return sb.String()
}

type sqlBuilder struct {
	placeholder    sq.PlaceholderFormat
	databaseSchema string
}

func NewSqlBuilder(dataBaseSchema string, placeholder sq.PlaceholderFormat) SqlBuilder {
	return sqlBuilder{
		placeholder:    placeholder,
		databaseSchema: dataBaseSchema,
	}
}

func (q sqlBuilder) GetDatabaseSchema() string {
	return q.databaseSchema
}

func (q sqlBuilder) GetPlaceholder() sq.PlaceholderFormat {
	return q.placeholder
}

func (q sqlBuilder) Build() sq.StatementBuilderType {
	return sq.StatementBuilder.PlaceholderFormat(q.placeholder)
}

func (q sqlBuilder) GetUsedTablesOfViews(viewSchema string, viewNames ...string) (string, []interface{}, error) {
	query := q.Build().Select("table_name").
		From("information_schema.view_table_usage").
		Where(sq.Eq{"view_schema": viewSchema}).
		Where(sq.Eq{"view_name": viewNames})
	return query.ToSql()
}

func (q sqlBuilder) DoesTableExist(schema string, tableName ...string) (string, []interface{}, error) {
	query := q.Build().Select("table_name").
		From("information_schema.tables").
		Where(sq.Eq{"table_schema": schema}).
		Where(sq.Eq{"table_name": tableName})
	return query.ToSql()
}

func (q sqlBuilder) TableWithSchema(name string) string {
	return fmt.Sprintf("%s.%s", q.databaseSchema, name)
}

func (q sqlBuilder) TableWithSchemaAndAlias(name, alias string) string {
	return q.WithAlias(q.TableWithSchema(name), alias)
}

func (q sqlBuilder) WithAlias(name, alias string) string {
	return fmt.Sprintf("%s AS %s", name, alias)
}

func (q sqlBuilder) WithColumnsPrefix(prefix string, columns ...string) []string {
	var result []string
	if prefix == "" {
		return columns
	}
	for _, column := range columns {
		result = append(result, prefix+"."+column)
	}
	return result
}

func (q sqlBuilder) WithColumnPrefix(prefix string, column string) string {
	if prefix == "" {
		return column
	}
	return prefix + "." + column
}

func (q sqlBuilder) WithEscapedName(name string) string {
	return fmt.Sprintf(`"%s"`, name)
}

func (q sqlBuilder) DistinctClauseON(columns ...string) string {
	return fmt.Sprintf("ON (%s)", strings.Join(columns[:], ","))
}

func (q sqlBuilder) PrefixSelector(prefix string, columns map[string]interface{}) map[string]interface{} {
	var result = make(map[string]interface{})
	for name, data := range columns {
		columnName := q.WithColumnPrefix(prefix, name)
		result[columnName] = data
	}
	return result
}

func (q sqlBuilder) AsCTE(selectQuery sq.SelectBuilder, alias string) sq.SelectBuilder {
	return selectQuery.Prefix(fmt.Sprintf("WITH %s AS (", alias)).Suffix(")")
}

func (q sqlBuilder) UnionAll(first sq.SelectBuilder, other ...sq.SelectBuilder) sq.SelectBuilder {
	result := first
	for _, sel := range other {
		result = result.Suffix(" UNION ALL ").SuffixExpr(sel)
	}
	return result
}

func (q sqlBuilder) Union(first sq.SelectBuilder, other ...sq.SelectBuilder) sq.SelectBuilder {
	result := first
	for _, sel := range other {
		result = result.Suffix(" UNION ").SuffixExpr(sel)
	}
	return result
}

func (q sqlBuilder) JoinUsing(table1, table2 string, columns ...string) string {
	var columnList string
	for i, column := range columns {
		if i == 0 {
			columnList = fmt.Sprintf("%s", column)
		} else {
			columnList = fmt.Sprintf("%s,%s", columnList, column)
		}
	}

	result := table1 + " JOIN " + table2 + " USING (" + columnList + ")"
	return result
}

func (q sqlBuilder) AddJoinUsing(table string, columns ...string) string {
	var columnList string
	for i, column := range columns {
		if i == 0 {
			columnList = fmt.Sprintf("%s", column)
		} else {
			columnList = fmt.Sprintf("%s,%s", columnList, column)
		}
	}

	result := table + " USING (" + columnList + ")"
	return result
}

func (q sqlBuilder) JoinLeftUsing(table1, table2 string, columns ...string) string {
	var columnList string
	for i, column := range columns {
		if i == 0 {
			columnList = fmt.Sprintf("%s", column)
		} else {
			columnList = fmt.Sprintf("%s,%s", columnList, column)
		}
	}

	result := table1 + " LEFT JOIN " + table2 + " USING (" + columnList + ")"
	return result
}

func (q sqlBuilder) InParenthese(clause string) string {
	return fmt.Sprintf("(%s)", clause)
}

func (q sqlBuilder) Greater(arg1, arg2 string) string {
	return fmt.Sprintf("%s>%s", arg1, arg2)
}

func (q sqlBuilder) IsNull(arg1 string) string {
	return fmt.Sprintf("%s IS NULL", arg1)
}

func (q sqlBuilder) DateTrunc(gran temporal.Granularity, column string) string {
	return fmt.Sprintf("date_trunc('%s',%s)", gran, column)
}

func (q sqlBuilder) AtTimeZone(column string, timeZone string) string {
	return fmt.Sprintf("%s AT TIME ZONE '%s'", column, timeZone)
}

func (q sqlBuilder) Sum(column string) string {
	return fmt.Sprintf("SUM(%s)", column)
}

func (q sqlBuilder) Upper(column string) string {
	return fmt.Sprintf("UPPER(%s)", column)
}

func (q sqlBuilder) Lower(column string) string {
	return fmt.Sprintf("LOWER(%s)", column)
}
