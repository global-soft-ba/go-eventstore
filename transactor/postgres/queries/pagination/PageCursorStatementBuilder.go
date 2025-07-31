package pagination

import (
	"fmt"
	sq "github.com/Masterminds/squirrel"
)

func CursorPagination(placeholder sq.PlaceholderFormat, q sq.SelectBuilder, cursor PageCursor) sq.Sqlizer {
	return cursorPagination{
		query:       q,
		cursor:      cursor,
		placeHolder: placeholder,
	}
}

type cursorPagination struct {
	query       sq.SelectBuilder
	cursor      PageCursor
	placeHolder sq.PlaceholderFormat
}

func (c cursorPagination) ToSql() (string, []interface{}, error) {
	// empty cases - no pagination
	if c.cursor.IsEmpty() {
		return c.query.ToSql()
	}

	var where sq.Sqlizer
	fetch := fmt.Sprintf("FETCH FIRST %d ROWS ONLY", c.cursor.PageSize())

	switch {
	case len(c.cursor.Values()) == 0: // first page call
		q := c.addSortColumns(c.query)
		return q.Suffix(fetch).ToSql()

	default: // x-page call
		where = c.buildWhereClauseStatement()
	}

	query := c.addSortColumns(c.query.Where(where)).Suffix(fetch)

	// We have to adjust the result set here. In terms of logic, we run backwards through the data,
	// but we have to output them forwards. Example: 1-2-|-3-4-|-5 by navigating backwards we would get the sequence 2-1 for
	// Page 1, but we should return 1-2. We therefore sort in the DB again the results according to the sort fields.
	if c.cursor.Backward() {
		tmpBuilder := sq.StatementBuilder.PlaceholderFormat(c.placeHolder)
		surrQuery := tmpBuilder.Select("*").FromSelect(query, "tmp")
		surrQuery = surrQuery.OrderBy(buildSortingClause(c.cursor.SortFields())...)
		return surrQuery.ToSql()
	}

	return query.ToSql()
}

func (c cursorPagination) addSortColumns(query sq.SelectBuilder) sq.SelectBuilder {
	if !c.cursor.Backward() {
		return query.OrderBy(buildSortingClause(c.cursor.SortFields())...)
	} else {
		// a backward cursor is a forward cursor with the sort order reversed,
		// so all ASC fields become DESC fields and vice versa
		return query.OrderBy(buildSortingClause(reverseSortOrdering(c.cursor.SortFields()))...)
	}
}

func (c cursorPagination) buildWhereClauseStatement() sq.Sqlizer {
	where, values := buildOrClausesWithValues(c.cursor.SortFields(), c.cursor.Values(), c.cursor.Backward())
	return sq.Expr(fmt.Sprintf("( %s )", where), values...)
}

// -------------------------------------------------------  cursor build-----------------------------------------------

// When selecting the operator(s) of the cursor, we must consider both the sorting of the fields and the type of cursor.
// Both influence each other. The main problem is that the cursor is implemented via FETCH FIRST ROWS.
// This means that we can only navigate "forwards" through the result set from an SQL perspective. I.e. we can only
// select the first 1-n elements from the result set.

// With a (predefined) ascending sort order, all elements, starting from a given value, are greater than this value.
// When navigating forwards through the pages, all elements that are greater than the given value must be selected (operator = >).
// However, the opposite is the case for a (predefined) descending sort order, i.e. the operator must be reversed (operator = <).
// This applies to each individual sort field of the cursor (or to all sort fields of the cursor if they are all asc or desc)

// If we also add backward navigation, things get complicated again.
// We cannot run directly "backwards" through the result set. However, we can simulate this,
// by reversing the sorting of the result (i.e. asc sorting to desc sorting and vice versa).
// However, the comparison operator must be reversed at the same time.
// Thus, a backward navigation with ASC sort order corresponds to a forward navigation with desc sort order.
// And a backward navigation with desc sort order corresponds to a forward navigation with asc sort order.
//
// FORWARD NAVIGATION
//
//	     	                                      | (x>5)-->
//	result set with asc sorting, e.g.  |-1-2-3-4-[5]-6-7-8-9-10-....
//
//	                                                 | (x<5)-->
//	result set with desc sorting,e.g.  |-10-9-8-7-6-[5]-4-3-2-1-....
//
// BACKWARD NAVIGATION (simulated by reversing the sorting)
//
//	     	                                  <-?-|                                         | (x<5)-->
//	result set with asc sorting, e.g.  |-1-2-3-4-[5]-6-7-8-9-10-....    =>    |-10-9-8-7-6-[5]-4-3-2-1-....
//
//
//	     	                                     <-?-|                                   | (x>5)-->
//	result set with desc sorting, e.g. |-10-9-8-7-6-[5]-4-3-2-1-....    =>    |-1-2-3-4-[5]-6-7-8-9-10-....
//
// -----------------------------------------------------------------------------------------------------------------

func getCursorOperator(backward bool, descField bool) string {
	var comparator string

	if !backward {
		if descField {
			// descending sort order, we navigate forward in the result to items with lower values
			comparator = "<"
		} else {
			// ascending sort order, we navigate along in the result to items with higher values
			comparator = ">"
		}
	} else {
		// backward navigation
		if descField {
			// descending sort order, but because we navigate backward in the result, we navigate to items with higher values
			comparator = ">"
		} else {
			// ascending sort order, but because we navigate backward in the result, we navigate to items with lower values
			comparator = "<"
		}
	}

	return comparator
}

// ------------------------------------------------------- statement builder-----------------------------------------

func buildOrClausesWithValues(fields []SortField, val []interface{}, backward bool) (string, []interface{}) {
	if len(fields) == 1 {
		return buildAndClause(fields, backward), copyValues(val)
	}

	orTerm, valOut := buildOrClausesWithValues(fields[:len(fields)-1], val[:len(val)-1], backward)
	return fmt.Sprintf("%s OR ( %s )", orTerm, buildAndClause(fields, backward)), append(valOut, copyValues(val)...)
}

func buildAndClause(fields []SortField, backward bool) string {
	if len(fields) == 1 {
		return fmt.Sprintf("%s %s ?", fields[0].Name, getCursorOperator(backward, fields[0].Desc))
	}

	andTerm := buildAndClause(fields[1:], backward)
	return fmt.Sprintf("%s = ? AND %s", fields[0].Name, andTerm)
}

func buildSortingClause(sortFields []SortField) []string {
	var sorting []string
	for _, sortField := range sortFields {
		if sortField.Desc {
			sorting = append(sorting, fmt.Sprintf("%s DESC", sortField.Name))
		} else {
			sorting = append(sorting, fmt.Sprintf("%s ASC", sortField.Name))
		}
	}
	return sorting
}

func reverseSortOrdering(fields []SortField) []SortField {
	var reversed []SortField
	for _, field := range fields {
		if field.Desc {
			field.Desc = false
		} else {
			field.Desc = true
		}
		reversed = append(reversed, field)
	}
	return reversed
}

// ------------------------------------------------------- helper ---------------------------------------------------

func copyValues(in []interface{}) []interface{} {
	newVal := make([]interface{}, len(in))
	copy(newVal, in)

	return newVal
}
