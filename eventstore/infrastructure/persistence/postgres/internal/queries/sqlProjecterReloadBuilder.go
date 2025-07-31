package queries

import (
	sq "github.com/Masterminds/squirrel"
	"github.com/global-soft-ba/go-eventstore/eventstore/infrastructure/persistence/postgres/internal/mapper"
	"github.com/global-soft-ba/go-eventstore/eventstore/infrastructure/persistence/postgres/internal/tables"
	"time"
)

func (p SqlProjecter) buildReloadQuery(selector map[string]interface{}, sinceTime time.Time, eventTypes ...string) sq.SelectBuilder {
	const cteName = "mostRecentSnapShot"
	return p.asCTE(
		p.getMostRecentSnapShot(selector, sinceTime, eventTypes...), cteName).
		SuffixExpr(
			p.unionAll(
				p.getSnapShotData(cteName),
				p.getAllEventsOfAggregateAfterSnapShots(selector, cteName, sinceTime, eventTypes...),
				p.getAllEventsOfAggregatesWithoutSnapShots(selector, cteName, sinceTime, eventTypes...),
			))
}

func (p SqlProjecter) getMostRecentSnapShot(selector map[string]interface{}, sinceTime time.Time, eventTypes ...string) sq.SelectBuilder {
	stmt := p.build().
		Select().Distinct().
		Columns(p.distinctClauseON(
			tables.AggregateSnapsShotTable.TenantID,
			tables.AggregateSnapsShotTable.AggregateType,
			tables.AggregateSnapsShotTable.AggregateID)+" *").
		From(p.tableWithSchema(tables.AggregateSnapsShotTable.Name)).
		Where(selector).
		Where(sq.GtOrEq{tables.AggregateSnapsShotTable.ValidTime: mapper.MapToNanoseconds(sinceTime)}).
		Where(p.columnIn(tables.AggregateSnapsShotTable.Type, eventTypes...)).
		OrderBy(
			tables.AggregateSnapsShotTable.TenantID,
			tables.AggregateSnapsShotTable.AggregateType,
			tables.AggregateSnapsShotTable.AggregateID,
			tables.AggregateSnapsShotTable.Type,
			tables.AggregateSnapsShotTable.ValidTime+" DESC",
		)
	return stmt
}

func (p SqlProjecter) getSnapShotData(cteName string) sq.SelectBuilder {
	return p.build().
		Select(tables.AggregateSnapsShotTable.AllColumns()...).
		From(cteName)
}

func (p SqlProjecter) getAllEventsOfAggregateAfterSnapShots(selector map[string]interface{}, mostRecentSnapShots string, sinceTime time.Time, eventTypes ...string) sq.SelectBuilder {
	const aggregateEvents = "ae"
	const mostRecentSnap = "mrae"

	stmt := p.build().
		Select(p.withColumnsPrefix(aggregateEvents, tables.AggregateEventTable.AllColumns()...)...).
		From(
			p.joinUsing(
				p.tableWithSchemaAndAlias(tables.AggregateEventTable.Name, aggregateEvents),
				p.withAlias(mostRecentSnapShots, mostRecentSnap),
				tables.AggregateTable.TenantID,
				tables.AggregateTable.AggregateType,
				tables.AggregateTable.AggregateID,
			)).
		Where(
			p.greater(
				p.withColumnPrefix(aggregateEvents, tables.AggregateEventTable.ValidTime),
				p.withColumnPrefix(mostRecentSnap, tables.AggregateSnapsShotTable.ValidTime),
			),
		).
		Where(p.prefixSelector(aggregateEvents, selector)).
		Where(sq.GtOrEq{p.withColumnPrefix(aggregateEvents, tables.AggregateEventTable.ValidTime): mapper.MapToNanoseconds(sinceTime)}).
		Where(p.columnIn(p.withColumnPrefix(aggregateEvents, tables.AggregateEventTable.Type), eventTypes...))

	return stmt
}

func (p SqlProjecter) getAllEventsOfAggregatesWithoutSnapShots(selector map[string]interface{}, mostRecentSnapShots string, sinceTime time.Time, eventTypes ...string) sq.SelectBuilder {
	const aggregateEvents = "ae"
	const mostRecentSnap = "mrae"

	stmt := p.build().
		Select(
			p.withColumnsPrefix(aggregateEvents, tables.AggregateEventTable.AllColumns()...)...).
		From(
			p.joinLeftUsing(
				p.tableWithSchemaAndAlias(tables.AggregateEventTable.Name, aggregateEvents),
				p.withAlias(mostRecentSnapShots, mostRecentSnap),
				tables.AggregateTable.TenantID,
				tables.AggregateTable.AggregateType,
				tables.AggregateTable.AggregateID,
			),
		).
		Where(p.isNull(p.withColumnPrefix(mostRecentSnap, tables.AggregateSnapsShotTable.ValidTime))).
		Where(sq.GtOrEq{p.withColumnPrefix(aggregateEvents, tables.AggregateEventTable.ValidTime): mapper.MapToNanoseconds(sinceTime)}).
		Where(p.prefixSelector(aggregateEvents, selector)).
		Where(p.columnIn(p.withColumnPrefix(aggregateEvents, tables.AggregateEventTable.Type), eventTypes...))

	return stmt
}
