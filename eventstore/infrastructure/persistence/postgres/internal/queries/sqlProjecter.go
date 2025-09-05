package queries

import (
	"context"
	sq "github.com/Masterminds/squirrel"
	"github.com/global-soft-ba/go-eventstore"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/port/persistence/projection"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/shared"
	"github.com/global-soft-ba/go-eventstore/eventstore/infrastructure/persistence/postgres/internal/mapper"
	"github.com/global-soft-ba/go-eventstore/eventstore/infrastructure/persistence/postgres/internal/tables"
	"time"
)

func NewSqlProjecter(databaseSchema string, placeholder sq.PlaceholderFormat) SqlProjecter {
	return SqlProjecter{SqlBuilder{
		placeholder:    placeholder,
		databaseSchema: databaseSchema,
	}}
}

type SqlProjecter struct {
	SqlBuilder
}

func (p SqlProjecter) Lock(ctx context.Context, id shared.ProjectionID, alias string) (string, []interface{}, error) {
	key := generateAdvisoryLockId(id.TenantID, id.ProjectionID)
	query := p.build().
		Select(p.pgAdvisoryLockForTX(key, alias))
	return query.ToSql()
}

func (p SqlProjecter) UnLock(ctx context.Context, id shared.ProjectionID, alias string) (string, []interface{}, error) {
	key := generateAdvisoryLockId(id.TenantID, id.ProjectionID)
	query := p.build().
		Select(p.pgAdvisoryUnLock(key, alias))
	return query.ToSql()
}

func (p SqlProjecter) Get(ctx context.Context, ids ...shared.ProjectionID) (string, []interface{}, error) {
	some := sq.Or{}
	for _, data := range ids {
		some = append(some, sq.Eq{
			tables.ProjectionsTable.TenantID:     data.TenantID,
			tables.ProjectionsTable.ProjectionID: data.ProjectionID,
		})
	}

	return p.build().
		Select(tables.ProjectionsTable.AllColumns()...).
		From(p.tableWithSchema(tables.ProjectionsTable.Name)).
		Where(some).ToSql()
}

func (p SqlProjecter) GetAllForTenant(ctx context.Context, tenantID string) (string, []interface{}, error) {
	return p.build().
		Select(tables.ProjectionsTable.AllColumns()...).
		From(p.tableWithSchema(tables.ProjectionsTable.Name)).
		Where(sq.Eq{
			tables.ProjectionsTable.TenantID: tenantID,
		}).ToSql()
}

func (p SqlProjecter) GetAllForAllTenants(ctx context.Context) (string, []interface{}, error) {
	return p.build().
		Select(tables.ProjectionsTable.AllColumns()...).
		From(p.tableWithSchema(tables.ProjectionsTable.Name)).ToSql()
}

func (p SqlProjecter) SaveStates(ctx context.Context, projections ...projection.DTO) (string, []interface{}, error) {
	query := p.build().
		Insert(p.tableWithSchema(tables.ProjectionsTable.Name)).
		Columns(tables.ProjectionsTable.AllInsertColumns()...).
		Suffix(
			"ON CONFLICT ON CONSTRAINT projections_pkey " +
				"DO UPDATE SET " +
				tables.ProjectionsTable.State + "= excluded." + tables.ProjectionsTable.State,
		)
	for _, proj := range projections {
		//order of columns must be same as in function AllColumns()
		query = query.Values(
			proj.TenantID,
			proj.ProjectionID,
			proj.State,
		)
	}

	return query.ToSql()
}

func (p SqlProjecter) SaveProjectionEvents(ctx context.Context, events ...tables.ProjectionsEventRow) (statement string, args []interface{}, err error) {
	query := p.build().
		Insert(p.tableWithSchema(tables.ProjectionsEventsTable.Name)).
		Columns(tables.ProjectionsEventsTable.AllColumns()...)
	for _, evt := range events {
		query = query.Values(
			mapper.ProjectionEventRowsToArrayOfValues(evt)...,
		)
	}

	return query.ToSql()
}

func (p SqlProjecter) GetSinceLastRun(ctx context.Context, id shared.ProjectionID, loadOpt projection.LoadOptions, sinceTimeStamp int64) (statement string, args []interface{}, err error) {
	query :=
		p.fetchFirstRowsOnly(
			p.build().
				Select(append(tables.ProjectionsEventsTable.AllColumns(), tables.ProjectionsTable.State)...).
				From(p.joinLeftUsing(
					p.tableWithSchema(tables.ProjectionsEventsTable.Name),
					p.tableWithSchema(tables.ProjectionsTable.Name),
					tables.ProjectionsTable.TenantID,
					tables.ProjectionsTable.ProjectionID,
				)).
				Where(sq.Eq{
					tables.ProjectionsTable.TenantID:     id.TenantID,
					tables.ProjectionsTable.ProjectionID: id.ProjectionID,
				}).
				Where(sq.Lt{
					tables.ProjectionsEventsTable.ValidTime: sinceTimeStamp,
				}).
				Where(
					sq.NotEq{tables.AggregateEventTable.Class: event.DeletePatch}). //ignore delete patches
				OrderBy(
					tables.ProjectionsEventsTable.ValidTime,
					tables.ProjectionsEventsTable.AggregateID,
					tables.ProjectionsEventsTable.Version,
				),

			loadOpt.ChunkSize)

	return query.ToSql()
}

func (p SqlProjecter) DeleteRows(ctx context.Context, rows ...tables.ProjectionsEventsLoadRow) (statement string, args []interface{}, err error) {
	some := sq.Or{}
	for _, row := range rows {
		keys := sq.Eq{
			tables.ProjectionsEventsTable.TenantID:     row.TenantID,
			tables.ProjectionsEventsTable.ProjectionID: row.ProjectionID,
			tables.ProjectionsEventsTable.ID:           row.ID,
		}
		some = append(some, keys)
	}

	query := p.build().
		Delete(p.tableWithSchema(tables.ProjectionsEventsTable.Name)).
		Where(some)

	return query.ToSql()
}

func (p SqlProjecter) DeleteEvents(ctx context.Context, id shared.ProjectionID) (statement string, args []interface{}, err error) {
	query := p.build().
		Delete(p.tableWithSchema(tables.ProjectionsEventsTable.Name)).
		Where(sq.Eq{
			tables.ProjectionsEventsTable.TenantID: id.TenantID,
			tables.ProjectionsTable.ProjectionID:   id.ProjectionID,
		})

	return query.ToSql()
}

func (p SqlProjecter) ResetSince(ctx context.Context, id shared.ProjectionID, sinceTime time.Time, eventTypes ...string) (statement string, args []interface{}, err error) {
	const sel = "sel"

	query := p.build().
		Insert(p.tableWithSchema(tables.ProjectionsEventsTable.Name)).
		Columns(tables.ProjectionsEventsTable.AllColumns()...).
		Select(
			p.build().
				Select(p.withAlias(p.withQuotes(id.ProjectionID), tables.ProjectionsEventsTable.ProjectionID)).
				Columns(p.withColumnsPrefix(sel, tables.AggregateEventTable.AllColumns()...)...).
				FromSelect(p.getAllEventsOfProjectionSince(ctx, id, sinceTime, eventTypes...), sel))
	return query.ToSql()
}

func (p SqlProjecter) getAllEventsOfProjectionSince(ctx context.Context, id shared.ProjectionID, sinceTime time.Time, eventTypes ...string) sq.SelectBuilder {
	selector := map[string]interface{}{
		tables.AggregateEventTable.TenantID: id.TenantID,
	}
	return p.buildReloadQuery(selector, sinceTime, eventTypes...)
}

func (p SqlProjecter) RemoveProjectionState(ctx context.Context, projectionID string) (string, []interface{}, error) {
	query := p.build().
		Delete(p.tableWithSchema(tables.ProjectionsTable.Name)).
		Where(sq.Eq{
			tables.ProjectionsTable.ProjectionID: projectionID,
		})

	return query.ToSql()
}

func (p SqlProjecter) RemoveProjectionEvents(ctx context.Context, projectionID string) (string, []interface{}, error) {
	query := p.build().
		Delete(p.tableWithSchema(tables.ProjectionsEventsTable.Name)).
		Where(sq.Eq{
			tables.ProjectionsEventsTable.ProjectionID: projectionID,
		})

	return query.ToSql()
}

func (p SqlProjecter) DeleteEventFromQueue(ctx context.Context, eventID string, ids []shared.ProjectionID) (string, []interface{}, error) {
	query := p.build().
		Delete(p.tableWithSchema(tables.ProjectionsEventsTable.Name))

	var tenantIDs []string
	var projectionIDs []string
	for _, id := range ids {
		tenantIDs = append(tenantIDs, id.TenantID)
		projectionIDs = append(projectionIDs, id.ProjectionID)
	}

	query = query.Where(sq.And{
		sq.Eq{tables.ProjectionsEventsTable.ID: eventID},
		sq.Eq{tables.ProjectionsEventsTable.TenantID: tenantIDs},
		sq.Eq{tables.ProjectionsEventsTable.ProjectionID: projectionIDs},
	})

	return query.ToSql()
}

func (p SqlProjecter) GetProjectionsWithEventInQueue(ctx context.Context, id shared.AggregateID, eventID string) (string, []interface{}, error) {
	query := p.build().
		Select(tables.ProjectionsEventsTable.ProjectionID,
			tables.ProjectionsEventsTable.TenantID).
		From(p.tableWithSchema(tables.ProjectionsEventsTable.Name)).
		Where(sq.And{
			sq.Eq{tables.ProjectionsEventsTable.AggregateID: id.AggregateID},
			sq.Eq{tables.ProjectionsEventsTable.AggregateType: id.AggregateType},
			sq.Eq{tables.ProjectionsEventsTable.ID: eventID},
		})

	return query.ToSql()
}
