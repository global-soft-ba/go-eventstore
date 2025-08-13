package queries

import (
	"fmt"
	sq "github.com/Masterminds/squirrel"
	"hash/crc32"
	"strings"
)

const advisoryLockIDSalt uint = 1486364155

// generateAdvisoryLockId - we are mapping two UUIDs and a string to an integer. Due to the size this is not collision-free.
// If we notice in the future that locks could not be made because a lock already exists, this may be caused by a collision.
func generateAdvisoryLockId(id string, additionalIds ...string) uint32 {
	if len(additionalIds) > 0 {
		id = strings.Join(append(additionalIds, id), "\x00")
	}
	sum := crc32.ChecksumIEEE([]byte(id))
	sum = sum * uint32(advisoryLockIDSalt)
	return sum
}

type SqlBuilder struct {
	placeholder    sq.PlaceholderFormat
	databaseSchema string
}

func (q SqlBuilder) GetDatabaseSchema() string {
	return q.databaseSchema
}

func (q SqlBuilder) build() sq.StatementBuilderType {
	return sq.StatementBuilder.PlaceholderFormat(q.placeholder)
}

func (q SqlBuilder) tableWithSchema(name string) string {
	return fmt.Sprintf("%s.%s", q.databaseSchema, name)
}

func (q SqlBuilder) tableWithSchemaAndAlias(name, alias string) string {
	return q.withAlias(q.tableWithSchema(name), alias)
}

func (q SqlBuilder) withAlias(name, alias string) string {
	return fmt.Sprintf("%s AS %s", name, alias)
}

func (q SqlBuilder) withQuotes(value string) string {
	return fmt.Sprintf("'%s'", value)
}

func (q SqlBuilder) withColumnsPrefix(prefix string, columns ...string) []string {
	var result []string
	if prefix == "" {
		return columns
	}
	for _, column := range columns {
		result = append(result, prefix+"."+column)
	}
	return result
}

func (q SqlBuilder) withColumnPrefix(prefix string, column string) string {
	if prefix == "" {
		return column
	}
	return prefix + "." + column
}

func (q SqlBuilder) distinctClauseON(columns ...string) string {
	return fmt.Sprintf("ON (%s)", strings.Join(columns[:], ","))
}

func (q SqlBuilder) prefixSelector(prefix string, columns map[string]interface{}) map[string]interface{} {
	var result = make(map[string]interface{})
	for name, data := range columns {
		columnName := q.withColumnPrefix(prefix, name)
		result[columnName] = data
	}
	return result
}

func (q SqlBuilder) asCTE(selectQuery sq.SelectBuilder, alias string) sq.SelectBuilder {
	return selectQuery.Prefix(fmt.Sprintf("WITH %s AS (", alias)).Suffix(")")
}

func (q SqlBuilder) unionAll(first sq.SelectBuilder, other ...sq.SelectBuilder) sq.SelectBuilder {
	result := first
	for _, sel := range other {
		result = result.Suffix(" UNION ALL ").SuffixExpr(sel)
	}
	return result
}

func (q SqlBuilder) joinUsing(table1, table2 string, columns ...string) string {
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

func (q SqlBuilder) addJoinUsing(table string, columns ...string) string {
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

func (q SqlBuilder) joinLeftUsing(table1, table2 string, columns ...string) string {
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

func (q SqlBuilder) inParenthese(clause string) string {
	return fmt.Sprintf("(%s)", clause)
}

func (q SqlBuilder) greater(arg1, arg2 string) string {
	return fmt.Sprintf("%s>%s", arg1, arg2)
}

func (q SqlBuilder) isNull(arg1 string) string {
	return fmt.Sprintf("%s IS NULL", arg1)
}

func (q SqlBuilder) pgAdvisoryLock(key uint32, name string) string {
	return q.withAlias(fmt.Sprintf("pg_try_advisory_lock(%d)", key), name)
}

func (q SqlBuilder) pgAdvisoryUnLock(key uint32, name string) string {
	return q.withAlias(fmt.Sprintf("pg_advisory_unlock(%d)", key), name)
}

func (q SqlBuilder) pgAdvisoryLockForTX(key uint32, name string) string {
	return q.withAlias(fmt.Sprintf("pg_try_advisory_xact_lock(%d)", key), name)
}

func (q SqlBuilder) fetchFirstRowsOnly(query sq.SelectBuilder, n int) sq.SelectBuilder {
	return query.Suffix(
		fmt.Sprintf("FETCH FIRST %d ROWS ONLY", n),
	)
}

func (q SqlBuilder) columnIn(column string, values ...string) sq.Or {
	some := sq.Or{}
	for _, val := range values {
		keys := sq.Eq{
			column: val,
		}
		some = append(some, keys)
	}

	return some
}

func (q SqlBuilder) numRangeFromColumns(start, end string) string {
	return fmt.Sprintf("numrange(%s,%s)", start, end)
}
func (q SqlBuilder) numMultiRangeFromRange(rnge string) string {
	return fmt.Sprintf("nummultirange(%s)", rnge)
}

func (q SqlBuilder) numRange(start, end int64) string {
	return fmt.Sprintf("numrange(%d,%d)", start, end)
}

func (q SqlBuilder) rangeIntersection(range1, range2 string) string {
	return fmt.Sprintf("%s - %s", range1, range2)
}

func (q SqlBuilder) rangeAgg(rnge string) string {
	return fmt.Sprintf("range_agg(%s)", rnge)
}

func (q SqlBuilder) unNest(value string) string {
	return fmt.Sprintf("unnest(%s)", value)
}

func (q SqlBuilder) lower(rnge string) string {
	return fmt.Sprintf("lower(%s)", rnge)
}

func (q SqlBuilder) upper(rnge string) string {
	return fmt.Sprintf("upper(%s)", rnge)
}
