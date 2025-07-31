package queries

import (
	sq "github.com/Masterminds/squirrel"
	"github.com/global-soft-ba/go-eventstore"
	"github.com/global-soft-ba/go-eventstore/eventstore/infrastructure/persistence/postgres/internal/mapper"
	"github.com/global-soft-ba/go-eventstore/eventstore/infrastructure/persistence/postgres/internal/tables"
	"time"
)

// -------------------------------------------LOADER SEMANTIC HELPER----------------------------------------------------
type loadWithSemantic interface {
	load(projectionTime time.Time, reportTime time.Time, vTimeColumn, tTimeColumn string) sq.And
}

type loadWithAsAtSemantic struct{}

func (l loadWithAsAtSemantic) load(projectionTime time.Time, _ time.Time, vTimeColumn, tTimeColumn string) sq.And {
	var loadAsAt = sq.And{
		sq.LtOrEq{vTimeColumn: mapper.MapToNanoseconds(projectionTime)},
		sq.LtOrEq{tTimeColumn: mapper.MapToNanoseconds(projectionTime)},
	}
	return loadAsAt
}

type loadWithAsOfSemantic struct{}

func (l loadWithAsOfSemantic) load(projectionTime time.Time, _ time.Time, vTimeColumn, _ string) sq.And {
	var loadAsAt = sq.And{
		sq.LtOrEq{vTimeColumn: mapper.MapToNanoseconds(projectionTime)},
	}
	return loadAsAt
}

type loadWithAsOfTill struct{}

func (l loadWithAsOfTill) load(projectionTime time.Time, reportTime time.Time, vTimeColumn, tTimeColumn string) sq.And {
	var loadAsOfTill = sq.And{
		sq.LtOrEq{vTimeColumn: mapper.MapToNanoseconds(projectionTime)},
		sq.LtOrEq{tTimeColumn: mapper.MapToNanoseconds(reportTime)},
	}
	return loadAsOfTill
}

// ------------------------------------------BUILDER---------------------------------------------------------------------
func (l SqlLoader) buildLoaderQuery(selector map[string]interface{}, pTime, rTime time.Time, semantic loadWithSemantic) sq.SelectBuilder {
	const final = "final"
	const cteName = "mostRecentSnapShot"
	var currentVersionColumn = tables.AggregateTable.CurrentVersion
	return l.build().Select("*").FromSelect(
		l.asCTE(
			l.getMostRecentSnapShot(
				selector,
				semantic.load(pTime, rTime, tables.AggregateSnapsShotTable.ValidTime, tables.AggregateSnapsShotTable.TransactionTime)),
			cteName).
			SuffixExpr(
				l.unionAll(
					l.getSnapShotDataWithAggregateCurrentVersion(cteName, currentVersionColumn),
					l.getAllEventsOfAggregateAfterSnapShots(selector, cteName, func(vColumn, tColum string) sq.And {
						return semantic.load(pTime, rTime, vColumn, tColum)
					}),
					l.getAllEventsOfAggregatesWithoutSnapShots(selector, cteName, func(vColumn, tColum string) sq.And {
						return semantic.load(pTime, rTime, vColumn, tColum)
					}),
				)), final).
		Where(
			sq.NotEq{tables.AggregateEventTable.Class: event.DeletePatch}).
		OrderBy(
			tables.AggregateEventTable.TenantID,
			tables.AggregateEventTable.AggregateType,
			tables.AggregateEventTable.AggregateID,
			tables.AggregateEventTable.ValidTime,
			tables.AggregateEventTable.Version,
		)
}

func (l SqlLoader) getMostRecentSnapShot(selector map[string]interface{}, loadSemantic sq.And) sq.SelectBuilder {
	stmt := l.build().
		Select().Distinct().
		Columns(l.distinctClauseON(
			tables.AggregateSnapsShotTable.TenantID,
			tables.AggregateSnapsShotTable.AggregateType,
			tables.AggregateSnapsShotTable.AggregateID)+" *").
		From(l.tableWithSchema(tables.AggregateSnapsShotTable.Name)).
		Where(selector).
		Where(loadSemantic).
		OrderBy(
			tables.AggregateSnapsShotTable.TenantID,
			tables.AggregateSnapsShotTable.AggregateType,
			tables.AggregateSnapsShotTable.AggregateID,
			tables.AggregateSnapsShotTable.Type,
			tables.AggregateSnapsShotTable.ValidTime+" DESC",
		)
	return stmt
}

func (l SqlLoader) getSnapShotDataWithAggregateCurrentVersion(cteName string, currentVersionColumnName string) sq.SelectBuilder {
	const aggregate = "a"
	const mostRecent = "mr"
	return l.build().
		Select(append(
			l.withColumnsPrefix(mostRecent, tables.AggregateSnapsShotTable.AllColumns()...),
			l.withColumnPrefix(aggregate, tables.AggregateTable.CurrentVersion))...).
		From(l.joinLeftUsing(
			l.withAlias(cteName, mostRecent),
			l.tableWithSchemaAndAlias(tables.AggregateTable.Name, aggregate),
			tables.AggregateTable.TenantID,
			tables.AggregateTable.AggregateType,
			tables.AggregateTable.AggregateID,
		))
}

func (l SqlLoader) getAllEventsOfAggregateAfterSnapShots(selector map[string]interface{}, mostRecentSnapShots string, loadSemantic func(vColumn string, tColumn string) sq.And) sq.SelectBuilder {
	const aggregateEvents = "ae"
	const aggRecent = "mrae"

	stmt := l.build().
		Select(append(
			l.withColumnsPrefix(aggregateEvents, tables.AggregateEventTable.AllColumns()...),
			l.withColumnsPrefix(aggRecent, tables.AggregateTable.CurrentVersion)...)...).
		From(l.joinUsing(
			l.tableWithSchemaAndAlias(tables.AggregateEventTable.Name, aggregateEvents),
			l.withAlias(
				l.inParenthese(
					l.joinUsing(
						mostRecentSnapShots,
						l.tableWithSchema(tables.AggregateTable.Name),
						tables.AggregateTable.TenantID,
						tables.AggregateTable.AggregateType,
						tables.AggregateTable.AggregateID,
					)),
				aggRecent,
			),
			tables.AggregateTable.TenantID,
			tables.AggregateTable.AggregateType,
			tables.AggregateTable.AggregateID,
		)).
		Where(
			l.greater(
				l.withColumnPrefix(aggregateEvents, tables.AggregateEventTable.ValidTime),
				l.withColumnPrefix(aggRecent, tables.AggregateSnapsShotTable.ValidTime),
			),
		).
		Where(loadSemantic(
			l.withColumnPrefix(aggregateEvents, tables.AggregateEventTable.ValidTime),
			l.withColumnPrefix(aggregateEvents, tables.AggregateEventTable.TransactionTime),
		)).
		Where(
			l.prefixSelector(aggRecent, selector),
		)

	return stmt
}

func (l SqlLoader) getAllEventsOfAggregatesWithoutSnapShots(selector map[string]interface{}, mostRecentSnapShots string, loadSemantic func(vColumn string, tColumn string) sq.And) sq.SelectBuilder {
	const aggregateEvents = "ae"
	const aggRecent = "mrae"

	stmt := l.build().
		Select(append(
			l.withColumnsPrefix(aggregateEvents, tables.AggregateEventTable.AllColumns()...),
			l.withColumnsPrefix(aggregateEvents, tables.AggregateTable.CurrentVersion)...)...).
		From(
			l.joinLeftUsing(
				l.withAlias(
					l.inParenthese(
						l.joinUsing(
							l.tableWithSchema(tables.AggregateTable.Name),
							l.tableWithSchema(tables.AggregateEventTable.Name),
							tables.AggregateTable.TenantID,
							tables.AggregateTable.AggregateType,
							tables.AggregateTable.AggregateID)),
					aggregateEvents),
				l.withAlias(mostRecentSnapShots, aggRecent),
				tables.AggregateTable.TenantID,
				tables.AggregateTable.AggregateType,
				tables.AggregateTable.AggregateID,
			),
		).
		Where(
			l.isNull(l.withColumnPrefix(aggRecent, tables.AggregateEventTable.ValidTime)),
		).
		Where(loadSemantic(
			l.withColumnPrefix(aggregateEvents, tables.AggregateEventTable.ValidTime),
			l.withColumnPrefix(aggregateEvents, tables.AggregateEventTable.TransactionTime),
		)).
		Where(
			l.prefixSelector(aggregateEvents, selector),
		)

	return stmt
}
