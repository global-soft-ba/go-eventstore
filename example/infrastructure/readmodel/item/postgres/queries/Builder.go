package queries

import (
	"example/core/port/readmodel/item"
	"example/infrastructure/persistence/postgres/tables"
	"example/presentation/paginator"
	"fmt"
	sq "github.com/Masterminds/squirrel"
	"github.com/global-soft-ba/go-eventstore"
	"github.com/global-soft-ba/go-eventstore/instrumentation/port/logger"
	"github.com/global-soft-ba/go-eventstore/transactor/postgres/queries"
	"github.com/global-soft-ba/go-eventstore/transactor/postgres/queries/pagination"
)

var (
	defaultSearchFields = []string{
		item.SearchItemID,
		item.SearchItemName,
	}
)

func NewSqlBuilder(dataBaseSchema string, placeHolder sq.PlaceholderFormat) Builder {
	return Builder{
		SqlBuilder: queries.NewSqlBuilder(dataBaseSchema, placeHolder),
	}
}

type Builder struct {
	queries.SqlBuilder
}

func (b Builder) baseQuery(tenantID string) sq.SelectBuilder {
	aggView := tables.ItemsTable
	return b.Build().
		Select(aggView.AllReadColumns()...).
		From(aggView.TableName).
		Where(sq.Eq{aggView.TenantID: tenantID})
}

func (b Builder) paginateQuery(query sq.SelectBuilder, cursor pagination.PageCursor) sq.Sqlizer {
	if cursor.IsEmpty() {
		return query.OrderBy(tables.ItemsTable.ID)
	}
	return pagination.CursorPagination(b.GetPlaceholder(), query, cursor)
}

func (b Builder) createSearchClause(searchFields []event.SearchField) []sq.Sqlizer {
	var out []sq.Sqlizer
	for _, field := range searchFields {
		if field.Name == paginator.SearchAny {
			out = append(out, b.buildSearchAnyClause(field.Value))
		} else {
			term, err := b.buildSearchClause(field)
			if err != nil {
				continue
			} else {
				out = append(out, term)
			}
		}
	}
	return out
}

func (b Builder) buildSearchAnyClause(fieldValue string) sq.Sqlizer {
	anyClause := sq.Or{}
	for _, field := range defaultSearchFields {
		searchClause, err := b.buildSearchClause(event.SearchField{Name: field, Value: fieldValue})
		if err != nil {
			logger.Warn("could not apply default search column %q for items search: %v", field, err)
			continue
		} else {
			anyClause = append(anyClause, searchClause)
		}
	}
	return anyClause
}

func (b Builder) buildSearchClause(field event.SearchField) (sq.Sqlizer, error) {
	aggView := tables.ItemsTable

	switch field.Name {
	case item.SearchItemID:
		return sq.Expr(fmt.Sprintf("%s = ?", b.WithColumnPrefix(aggView.TableName, aggView.ItemID)), field.Value), nil
	case item.SearchItemName:
		return sq.Expr(fmt.Sprintf("%s ILIKE ?", b.WithColumnPrefix(aggView.TableName, aggView.ItemName)), "%"+field.Value+"%"), nil
	}
	return nil, fmt.Errorf("unknown search field or wrong data type: %s", field.Name)
}

func (b Builder) FindItems(tenantID string, cursor pagination.PageCursor, searchFields []event.SearchField) (statement string, args []interface{}, err error) {
	q := b.baseQuery(tenantID)

	for _, clause := range b.createSearchClause(searchFields) {
		q = q.Where(clause)
	}

	stmt, args, err := b.paginateQuery(q, cursor).ToSql()
	if err != nil {
		return "", nil, err
	}
	return stmt, args, err
}

func (b Builder) FindItemByID(tenantID, itemID string) (statement string, args []interface{}, err error) {
	return b.baseQuery(tenantID).
		Where(sq.Eq{tables.ItemsTable.ItemID: itemID}).
		OrderBy(tables.ItemsTable.ItemID).
		Limit(1).
		ToSql()
}
