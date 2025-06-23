package queries

import (
	"context"
	"fmt"
	sq "github.com/Masterminds/squirrel"
	"github.com/global-soft-ba/go-eventstore"
	"github.com/global-soft-ba/go-eventstore/eventstore/infrastructure/persistence/postgres/internal/mapper"
	"github.com/global-soft-ba/go-eventstore/eventstore/infrastructure/persistence/postgres/internal/pagination"
	"github.com/global-soft-ba/go-eventstore/eventstore/infrastructure/persistence/postgres/internal/tables"
	"github.com/global-soft-ba/go-eventstore/instrumentation/port/logger"
	"strconv"
	"time"
)

func NewSqlLoader(databaseSchema string, placeholder sq.PlaceholderFormat) SqlLoader {
	return SqlLoader{SqlBuilder{
		placeholder:    placeholder,
		databaseSchema: databaseSchema,
	}}
}

type SqlLoader struct {
	SqlBuilder
}

func (l SqlLoader) LoadAsAt(ctx context.Context, selector map[string]interface{}, projectionTime time.Time) (statement string, args []interface{}, err error) {
	return l.buildLoaderQuery(selector, projectionTime, projectionTime, loadWithAsAtSemantic{}).ToSql()
}

func (l SqlLoader) LoadAsOf(ctx context.Context, selector map[string]interface{}, projectionTime time.Time) (statement string, args []interface{}, err error) {
	return l.buildLoaderQuery(selector, projectionTime, projectionTime, loadWithAsOfSemantic{}).ToSql()
}

func (l SqlLoader) LoadAsOfTill(ctx context.Context, selector map[string]interface{}, projectionTime, reportTime time.Time) (statement string, args []interface{}, err error) {
	return l.buildLoaderQuery(selector, projectionTime, reportTime, loadWithAsOfTill{}).ToSql()
}

func (l SqlLoader) GetAggregateState(ctx context.Context, tenantID, aggregateType, aggregateID string) (statement string, args []interface{}, err error) {
	query := l.build().
		Select("*").
		From(l.tableWithSchema(tables.AggregateTable.Name)).
		Where(
			sq.And{
				sq.Eq{tables.AggregateTable.TenantID: tenantID},
				sq.Eq{tables.AggregateTable.AggregateType: aggregateType},
				sq.Eq{tables.AggregateTable.AggregateID: aggregateID},
			},
		).
		OrderBy(tables.AggregateTable.AggregateID)
	return query.ToSql()
}

func (l SqlLoader) GetAggregateStatesForAggregateType(ctx context.Context, tenantID string, aggregateType string) (statement string, args []interface{}, err error) {
	query := l.build().
		Select("*").
		From(l.tableWithSchema(tables.AggregateTable.Name)).
		Where(
			sq.Eq{tables.AggregateTable.TenantID: tenantID},
		).
		Where(
			sq.Eq{tables.AggregateTable.AggregateType: aggregateType},
		)
	return query.ToSql()
}

func (l SqlLoader) GetAggregateStatesForAggregateTypeTill(ctx context.Context, tenantID string, aggregateType string, until time.Time) (statement string, args []interface{}, err error) {
	query := l.build().
		Select("*").
		From(l.tableWithSchema(tables.AggregateTable.Name)).
		Where(
			sq.Eq{tables.AggregateTable.TenantID: tenantID}).
		Where(
			sq.Eq{tables.AggregateTable.AggregateType: aggregateType}).
		Where(
			sq.LtOrEq{tables.AggregateTable.CreateTime: mapper.MapToNanoseconds(until)})

	return query.ToSql()

}

func (l SqlLoader) GetPatchFreePeriodsForInterval(ctx context.Context, tenantID, aggregateTyp, aggregateID string, start time.Time, end time.Time) (statement string, args []interface{}, err error) {
	const rangeColumn = "range"
	const cteName = "event_ranges"
	const startRange = "StartRange"
	const endRange = "EndRange"
	const finalRanges = "z"
	const rangeArray = "ranges"

	caseRangeStart := sq.Case().
		When(l.greater(tables.AggregateEventTable.ValidTime, tables.AggregateEventTable.TransactionTime), tables.AggregateEventTable.TransactionTime). //future patch
		Else(tables.AggregateEventTable.ValidTime)                                                                                                     //historical patch and instant events

	caseRangeEnd := sq.Case().
		When(l.greater(tables.AggregateEventTable.ValidTime, tables.AggregateEventTable.TransactionTime), tables.AggregateEventTable.ValidTime). //future patch
		Else(tables.AggregateEventTable.TransactionTime)                                                                                         //historical patch and instant events

	caseEmptyResult := sq.Case().
		When(l.greater("count(*)", "0"),
			l.rangeIntersection(l.numMultiRangeFromRange(l.numRange(mapper.MapToNanoseconds(start), mapper.MapToNanoseconds(end))), l.rangeAgg(rangeColumn))).
		Else(l.numMultiRangeFromRange(l.numRange(mapper.MapToNanoseconds(start), mapper.MapToNanoseconds(end))))

	query := l.asCTE(
		l.build().
			Select(
				l.withAlias(l.numRangeFromColumns(startRange, endRange), rangeColumn)).FromSelect(
			l.build().Select().
				Column(sq.Alias(caseRangeStart, startRange)).
				Column(sq.Alias(caseRangeEnd, endRange)).
				From(l.tableWithSchema(tables.AggregateEventTable.Name)).
				Where(sq.Eq{tables.AggregateEventTable.TenantID: tenantID}).
				Where(sq.Eq{tables.AggregateEventTable.AggregateType: aggregateTyp}).
				Where(sq.Eq{tables.AggregateEventTable.AggregateID: aggregateID}).
				Where(sq.Or{
					sq.Gt{tables.AggregateEventTable.TransactionTime: mapper.MapToNanoseconds(start)},
					sq.Gt{tables.AggregateEventTable.ValidTime: mapper.MapToNanoseconds(start)},
				}).
				Where(sq.Or{
					sq.Lt{tables.AggregateEventTable.TransactionTime: mapper.MapToNanoseconds(end)},
					sq.Lt{tables.AggregateEventTable.ValidTime: mapper.MapToNanoseconds(end)},
				}), "s"),
		cteName).SuffixExpr(
		l.build().Select().
			Columns(l.withAlias(l.lower(l.withColumnPrefix(finalRanges, rangeArray)), startRange)).
			Columns(l.withAlias(l.upper(l.withColumnPrefix(finalRanges, rangeArray)), endRange)).
			FromSelect(
				l.build().Select(l.withAlias(l.unNest(rangeArray), rangeArray)).
					FromSelect(l.build().Select().Column(sq.Alias(caseEmptyResult, rangeArray)).From(cteName), finalRanges), "z"))

	return query.ToSql()
}

func (l SqlLoader) GetAggregatesEvents(ctx context.Context, tenantID string, cursor pagination.PageCursor, searchFields []event.SearchField) (statement string, args []interface{}, err error) {
	aggEvt := tables.AggregateEventTable

	query := l.build().Select(
		aggEvt.AllColumns()...).
		From(l.tableWithSchema(tables.AggregateEventTable.Name)).
		Where(
			sq.Eq{aggEvt.TenantID: tenantID})

	for _, clause := range l.createSearchClause(searchFields) {
		query = query.Where(clause)
	}

	if cursor.IsEmpty() && !cursor.HasSortFields() {
		return query.OrderBy(aggEvt.ValidTime).ToSql()
	}
	return pagination.CursorPagination(l.placeholder, query, cursor).ToSql()

}

func (l SqlLoader) createSearchClause(searchFields []event.SearchField) []sq.Sqlizer {
	var clauses []sq.Sqlizer
	for _, field := range searchFields {
		term, err := l.buildSearchClause(field)
		if err != nil {
			logger.Info(fmt.Errorf("could not build search clause %w", err).Error())
			continue
		} else {
			clauses = append(clauses, term)
		}
	}

	return clauses

}

func (l SqlLoader) buildSearchClause(field event.SearchField) (sq.Sqlizer, error) {
	table := tables.AggregateEventTable

	switch field.Name {
	case event.SearchAggregateEventID:
		return l.buildComparison(table.ID, field.Operator, field.Value)
	case event.SearchAggregateID:
		return l.buildComparison(table.AggregateID, field.Operator, field.Value)
	case event.SearchAggregateType:
		return l.buildComparison(table.AggregateType, field.Operator, field.Value)
	case event.SearchAggregateVersion:
		if field.Operator == event.SearchMatch {
			//type cast need to text to make match working
			return l.buildComparison(fmt.Sprintf("%s::TEXT", table.Version), field.Operator, field.Value)
		} else {
			intValue, err := strconv.ParseInt(field.Value, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("could not convert %s to int", field.Value)
			}
			return l.buildComparison(table.Version, field.Operator, intValue)
		}
	case event.SearchAggregateEventType:
		return l.buildComparison(table.Type, field.Operator, field.Value)
	case event.SearchAggregateClass:
		return l.buildComparison(table.Class, field.Operator, field.Value)
	case event.SearchValidTime:
		// type cast need to text to make match working
		if field.Operator == event.SearchMatch {
			return l.buildComparison(fmt.Sprintf("%s::TEXT", table.ValidTime), field.Operator, field.Value)
		} else {
			// type cast to nanoseconds
			dateTime, err := time.Parse(time.RFC3339, field.Value)
			if err != nil {
				return nil, fmt.Errorf("could not convert %s to valid time", field.Value)
			}

			return l.buildComparison(table.ValidTime, field.Operator, mapper.MapToNanoseconds(dateTime))
		}
	case event.SearchTransactionTime:
		// type cast need to text to make match working
		if field.Operator == event.SearchMatch {
			return l.buildComparison(fmt.Sprintf("%s::TEXT", table.TransactionTime), field.Operator, field.Value)
		} else {
			// type cast to nanoseconds
			dateTime, err := time.Parse(time.RFC3339, field.Value)
			if err != nil {
				return nil, fmt.Errorf("could not convert %s to transaction time", field.Value)
			}

			return l.buildComparison(table.TransactionTime, field.Operator, mapper.MapToNanoseconds(dateTime))
		}
	case event.SearchData:
		// type cast always needed
		return l.buildComparison(fmt.Sprintf("%s::TEXT", table.Data), field.Operator, field.Value)
	default:
		return nil, fmt.Errorf("unsupported search field %s", field.Name)
	}
}

func (l SqlLoader) buildComparison(fieldName string, operator event.SearchOperator, value any) (sq.Sqlizer, error) {
	switch operator {
	case event.SearchEqual:
		return sq.Eq{fieldName: value}, nil
	case event.SearchNotEqual:
		return sq.NotEq{fieldName: value}, nil
	case event.SearchGreaterThan:
		return sq.Gt{fieldName: value}, nil
	case event.SearchGreaterThanOrEqual:
		return sq.GtOrEq{fieldName: value}, nil
	case event.SearchLessThan:
		return sq.Lt{fieldName: value}, nil
	case event.SearchLessThanOrEqual:
		return sq.LtOrEq{fieldName: value}, nil
	case event.SearchMatch:
		return sq.Expr(fmt.Sprintf("%s ~* ?", fieldName), value), nil
	default:
		return nil, fmt.Errorf("unsupported comparison operator %s", operator)
	}
}
