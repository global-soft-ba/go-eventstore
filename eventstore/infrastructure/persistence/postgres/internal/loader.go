package internal

import (
	"context"
	"fmt"
	sq "github.com/Masterminds/squirrel"
	"github.com/georgysavva/scany/v2/pgxscan"
	"github.com/global-soft-ba/go-eventstore"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/port/persistence/aggregate"
	trans "github.com/global-soft-ba/go-eventstore/eventstore/core/port/persistence/transactor"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/shared"
	"github.com/global-soft-ba/go-eventstore/eventstore/infrastructure/persistence/postgres/internal/dbtx"
	"github.com/global-soft-ba/go-eventstore/eventstore/infrastructure/persistence/postgres/internal/mapper"
	"github.com/global-soft-ba/go-eventstore/eventstore/infrastructure/persistence/postgres/internal/queries"
	"github.com/global-soft-ba/go-eventstore/eventstore/infrastructure/persistence/postgres/internal/tables"
	"github.com/global-soft-ba/go-eventstore/instrumentation/port/metrics"
	"time"
)

func newLoader(dataBaseSchema string, placeholder sq.PlaceholderFormat, trans trans.Port) loader {
	querier := queries.NewSqlLoader(dataBaseSchema, placeholder)
	return loader{sql: querier, trans: trans}
}

type loader struct {
	sql   queries.SqlLoader
	trans trans.Port
}

func (s loader) GetTx(ctx context.Context) (dbtx.DBTX, error) {
	t, err := s.trans.GetTX(ctx)
	return t.(dbtx.DBTX), err
}

func (s loader) LoadAsAt(ctx context.Context, projectionTime time.Time, key shared.AggregateID) (event.PersistenceEvents, error) {
	selector := map[string]interface{}{
		tables.AggregateEventTable.TenantID:      key.TenantID,
		tables.AggregateEventTable.AggregateID:   key.AggregateID,
		tables.AggregateEventTable.AggregateType: key.AggregateType,
	}

	result, err := s.loadAsAt(ctx, projectionTime, selector)
	if err != nil {
		return event.PersistenceEvents{}, err
	}
	if len(result) == 0 {
		return event.PersistenceEvents{}, &event.ErrorEmptyEventStream{
			AggregateID:   key.AggregateID,
			TenantID:      key.TenantID,
			AggregateType: key.AggregateType,
			Err:           err,
		}
	}

	return result[0], err
}

func (s loader) LoadAsOf(ctx context.Context, projectionTime time.Time, key shared.AggregateID) (event.PersistenceEvents, error) {
	selector := map[string]interface{}{
		tables.AggregateEventTable.TenantID:      key.TenantID,
		tables.AggregateEventTable.AggregateID:   key.AggregateID,
		tables.AggregateEventTable.AggregateType: key.AggregateType,
	}

	result, err := s.loadAsOf(ctx, projectionTime, selector)
	if err != nil {
		return event.PersistenceEvents{}, err
	}
	if len(result) == 0 {
		return event.PersistenceEvents{}, &event.ErrorEmptyEventStream{
			AggregateID:   key.AggregateID,
			TenantID:      key.TenantID,
			AggregateType: key.AggregateType,
			Err:           err,
		}
	}

	return result[0], err
}

func (s loader) LoadAsOfTill(ctx context.Context, projectionTime, reportTime time.Time, key shared.AggregateID) (event.PersistenceEvents, error) {
	selector := map[string]interface{}{
		tables.AggregateEventTable.TenantID:      key.TenantID,
		tables.AggregateEventTable.AggregateID:   key.AggregateID,
		tables.AggregateEventTable.AggregateType: key.AggregateType,
	}

	result, err := s.loadAsOfTill(ctx, projectionTime, reportTime, selector)
	if err != nil {
		return event.PersistenceEvents{}, err
	}
	if len(result) == 0 {
		return event.PersistenceEvents{}, &event.ErrorEmptyEventStream{
			AggregateID:   key.AggregateID,
			TenantID:      key.TenantID,
			AggregateType: key.AggregateType,
			Err:           err,
		}
	}

	return result[0], err
}

func (s loader) LoadAllOfAggregateAsAt(ctx context.Context, tenantID, aggregateType string, projectionTime time.Time) (eventStreams []event.PersistenceEvents, err error) {
	selector := map[string]interface{}{
		tables.AggregateEventTable.TenantID:      tenantID,
		tables.AggregateEventTable.AggregateType: aggregateType,
	}

	result, err := s.loadAsAt(ctx, projectionTime, selector)
	if err != nil {
		return nil, err
	}

	if len(result) == 0 {
		return nil, &event.ErrorEmptyEventStream{
			TenantID:      tenantID,
			AggregateType: aggregateType,
			Err:           err,
		}
	}

	return result, err
}

func (s loader) LoadAllOfAggregateAsOf(ctx context.Context, tenantID, aggregateType string, projectionTime time.Time) (eventStreams []event.PersistenceEvents, err error) {
	selector := map[string]interface{}{
		tables.AggregateEventTable.TenantID:      tenantID,
		tables.AggregateEventTable.AggregateType: aggregateType,
	}

	result, err := s.loadAsOf(ctx, projectionTime, selector)
	if err != nil {
		return nil, err
	}

	if len(result) == 0 {
		return nil, &event.ErrorEmptyEventStream{
			TenantID:      tenantID,
			AggregateType: aggregateType,
			Err:           err,
		}
	}

	return result, err
}

func (s loader) LoadAllOfAggregateAsOfTill(ctx context.Context, tenantID, aggregateType string, projectionTime, reportTime time.Time) (eventStreams []event.PersistenceEvents, err error) {
	selector := map[string]interface{}{
		tables.AggregateEventTable.TenantID:      tenantID,
		tables.AggregateEventTable.AggregateType: aggregateType,
	}

	result, err := s.loadAsOfTill(ctx, projectionTime, reportTime, selector)
	if err != nil {
		return nil, err
	}

	if len(result) == 0 {
		return nil, &event.ErrorEmptyEventStream{
			TenantID:      tenantID,
			AggregateType: aggregateType,
			Err:           err,
		}
	}

	return result, err
}

func (s loader) LoadAllAsAt(ctx context.Context, tenantID string, projectionTime time.Time) (eventStreams []event.PersistenceEvents, err error) {
	selector := map[string]interface{}{
		tables.AggregateEventTable.TenantID: tenantID,
	}

	result, err := s.loadAsAt(ctx, projectionTime, selector)
	if err != nil {
		return nil, err
	}

	if len(result) == 0 {
		return nil, &event.ErrorEmptyEventStream{
			TenantID: tenantID,
			Err:      err,
		}
	}

	return result, err
}

func (s loader) LoadAllAsOf(ctx context.Context, tenantID string, projectionTime time.Time) (eventStreams []event.PersistenceEvents, err error) {
	selector := map[string]interface{}{
		tables.AggregateEventTable.TenantID: tenantID,
	}

	result, err := s.loadAsOf(ctx, projectionTime, selector)
	if err != nil {
		return nil, err
	}
	if len(result) == 0 {
		return nil, &event.ErrorEmptyEventStream{
			TenantID: tenantID,
			Err:      err,
		}
	}

	return result, err
}

func (s loader) LoadAllAsOfTill(ctx context.Context, tenantID string, projectionTime time.Time, reportTime time.Time) (eventStreams []event.PersistenceEvents, err error) {
	selector := map[string]interface{}{
		tables.AggregateEventTable.TenantID: tenantID,
	}

	result, err := s.loadAsOfTill(ctx, projectionTime, reportTime, selector)
	if err != nil {
		return nil, err
	}
	if len(result) == 0 {
		return nil, &event.ErrorEmptyEventStream{
			TenantID: tenantID,
			Err:      err,
		}
	}

	return result, err
}

func (s loader) GetAggregateState(ctx context.Context, tenantID, aggregateType, aggregateID string) (event.AggregateState, error) {
	ctx, endSpan := metrics.StartSpan(ctx, "GetAggregateState (loader)", map[string]interface{}{"tenantID": tenantID, "aggregateType": aggregateType, "aggregateID": aggregateID})
	defer endSpan()

	var row tables.AggregateRow
	stmt, args, err := s.sql.GetAggregateState(ctx, tenantID, aggregateType, aggregateID)
	if err != nil {
		return event.AggregateState{}, err
	}

	tx, err := s.GetTx(ctx)
	if err != nil {
		return event.AggregateState{}, err
	}

	err = pgxscan.Get(ctx, tx, &row, stmt, args...)
	if err != nil {
		return event.AggregateState{}, err
	}

	return mapper.ToAggregateState(row), nil
}

func (s loader) GetAggregateStatesForAggregateType(ctx context.Context, tenantID string, aggregateType string) ([]event.AggregateState, error) {
	ctx, endSpan := metrics.StartSpan(ctx, "GetAggregateStatesForAggregateType (loader)", map[string]interface{}{"tenantID": tenantID, "aggregateType": aggregateType})
	defer endSpan()

	var rows []tables.AggregateRow
	stmt, args, err := s.sql.GetAggregateStatesForAggregateType(ctx, tenantID, aggregateType)
	if err != nil {
		return nil, err
	}
	tx, err := s.GetTx(ctx)
	if err != nil {
		return nil, err
	}
	err = pgxscan.Select(ctx, tx, &rows, stmt, args...)
	if err != nil {
		return nil, err
	}

	return mapper.ToAggregateStates(rows), nil
}

func (s loader) GetAggregateStatesForAggregateTypeTill(ctx context.Context, tenantID string, aggregateType string, until time.Time) ([]event.AggregateState, error) {
	ctx, endSpan := metrics.StartSpan(ctx, "GetAggregateStatesForAggregateTypeTill (loader)", map[string]interface{}{"tenantID": tenantID, "aggregateType": aggregateType, "until": until})
	defer endSpan()

	var rows []tables.AggregateRow
	stmt, args, err := s.sql.GetAggregateStatesForAggregateTypeTill(ctx, tenantID, aggregateType, until)
	if err != nil {
		return nil, err
	}
	tx, err := s.GetTx(ctx)
	if err != nil {
		return nil, err
	}

	err = pgxscan.Select(ctx, tx, &rows, stmt, args...)
	if err != nil {
		return nil, err
	}

	return mapper.ToAggregateStates(rows), nil
}

func (s loader) GetPatchFreePeriodsForInterval(ctx context.Context, tenantID, aggregateType, aggregateID string, start time.Time, end time.Time) ([]event.TimeInterval, error) {
	ctx, endSpan := metrics.StartSpan(ctx, "GetPatchFreePeriodsForInterval (loader)", map[string]interface{}{"tenantID": tenantID, "aggregateType": aggregateType, "aggregateID": aggregateID, "start": start, "end": end})
	defer endSpan()

	var rows []struct {
		StartRange int64 `db:"startrange"`
		EndRange   int64 `db:"endrange"`
	}

	stmt, args, err := s.sql.GetPatchFreePeriodsForInterval(ctx, tenantID, aggregateType, aggregateID, start, end)
	if err != nil {
		return nil, err
	}
	tx, err := s.GetTx(ctx)
	if err != nil {
		return nil, err
	}

	err = pgxscan.Select(ctx, tx, &rows, stmt, args...)
	if err != nil {
		return nil, err
	}

	return mapper.ToTimeIntervalls([]struct {
		StartRange int64
		EndRange   int64
	}(rows)), err
}

func (s loader) loadAsAt(ctx context.Context, projectionTime time.Time, selector map[string]interface{}) (streams []event.PersistenceEvents, err error) {
	stmt, args, err := s.sql.LoadAsAt(ctx, selector, projectionTime)
	if err != nil {
		return nil, err
	}

	return s.load(ctx, stmt, args)
}

func (s loader) loadAsOf(ctx context.Context, projectionTime time.Time, selector map[string]interface{}) (streams []event.PersistenceEvents, err error) {
	stmt, args, err := s.sql.LoadAsOf(ctx, selector, projectionTime)
	if err != nil {
		return nil, err
	}

	return s.load(ctx, stmt, args)
}

func (s loader) loadAsOfTill(ctx context.Context, projectionTime, reportTime time.Time, selector map[string]interface{}) (streams []event.PersistenceEvents, err error) {
	stmt, args, err := s.sql.LoadAsOfTill(ctx, selector, projectionTime, reportTime)
	if err != nil {
		return nil, err
	}

	return s.load(ctx, stmt, args)
}

func (s loader) load(ctx context.Context, statement string, args []interface{}) (streams []event.PersistenceEvents, err error) {
	ctx, endSpan := metrics.StartSpan(ctx, "load (loader)", nil)
	defer endSpan()

	var pEvents []tables.AggregatePersistentEventLoadRow
	tx, err := s.GetTx(ctx)
	if err != nil {
		return nil, err
	}
	err = pgxscan.Select(ctx, tx, &pEvents, statement, args...)

	if err != nil {
		return nil, err
	}

	return mapper.ToPersistenceEvents(pEvents)
}

func (s loader) GetAggregatesEvents(ctx context.Context, tenantID string, page event.PageDTO) ([]event.PersistenceEvent, event.PagesDTO, error) {
	ctx, endSpan := metrics.StartSpan(ctx, "GetAggregatesEvents (loader)", map[string]interface{}{"tenantID": tenantID})
	defer endSpan()

	if page.PageSize > aggregate.MaxPageSizeAggregatesEvents {
		return nil, event.PagesDTO{}, fmt.Errorf("page size is too large: %d > %d", page.PageSize, aggregate.MaxPageSizeAggregatesEvents)
	}

	paginator, err := mapper.ToPageCursor(tables.AggregateEventTable.ID, page)
	if err != nil {
		return nil, event.PagesDTO{}, fmt.Errorf("could not build page cursor: %w", err)
	}

	stmt, args, err := s.sql.GetAggregatesEvents(ctx, tenantID, paginator, page.SearchFields)
	if err != nil {
		return nil, event.PagesDTO{}, err
	}

	tx, err := s.GetTx(ctx)
	if err != nil {
		return nil, event.PagesDTO{}, err
	}

	var rows []tables.AggregateEventRow
	err = pgxscan.Select(ctx, tx, &rows, stmt, args...)
	if err != nil {
		return nil, event.PagesDTO{}, fmt.Errorf("could not load events: %w", err)
	}

	// no result
	if len(rows) == 0 {
		return nil, event.PagesDTO{}, nil
	}

	firstCursor := mapper.ToNewPage(page, mapper.MapAggregateEventRowToSortValues(rows[0], page.SortFields), true)
	lastCursor := mapper.ToNewPage(page, mapper.MapAggregateEventRowToSortValues(rows[len(rows)-1], page.SortFields), false)

	return mapper.ToPersistenceEventArray(rows), event.PagesDTO{Previous: firstCursor, Next: lastCursor}, nil
}
