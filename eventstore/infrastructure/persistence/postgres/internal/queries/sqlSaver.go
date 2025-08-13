package queries

import (
	"context"
	sq "github.com/Masterminds/squirrel"
	"github.com/global-soft-ba/go-eventstore"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/port/persistence/aggregate"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/shared"
	"github.com/global-soft-ba/go-eventstore/eventstore/infrastructure/persistence/postgres/internal/mapper"
	"github.com/global-soft-ba/go-eventstore/eventstore/infrastructure/persistence/postgres/internal/tables"
	"time"
)

func NewSqlSaver(databaseSchema string, placeholder sq.PlaceholderFormat) SqlSaver {
	return SqlSaver{SqlBuilder{
		placeholder:    placeholder,
		databaseSchema: databaseSchema,
	}}
}

type SqlSaver struct {
	SqlBuilder
}

func (s SqlSaver) Lock(ctx context.Context, id shared.AggregateID, alias string) (string, []interface{}, error) {
	key := generateAdvisoryLockId(id.TenantID, id.AggregateType, id.AggregateID)
	query := s.build().
		Select(s.pgAdvisoryLockForTX(key, alias))
	return query.ToSql()
}

func (s SqlSaver) Get(ctx context.Context, ids ...shared.AggregateID) (string, []interface{}, error) {
	some := sq.Or{}
	for _, data := range ids {
		some = append(some, sq.Eq{
			tables.AggregateTable.TenantID:      data.TenantID,
			tables.AggregateTable.AggregateID:   data.AggregateID,
			tables.AggregateTable.AggregateType: data.AggregateType,
		})
	}
	return s.build().
		Select(tables.AggregateTable.AllColumns()...).
		From(s.tableWithSchema(tables.AggregateTable.Name)).
		Where(some).
		ToSql()
}

func (s SqlSaver) SaveAggregates(ctx context.Context, states []aggregate.DTO) (statement string, args []interface{}, err error) {
	query := s.build().
		Insert(s.tableWithSchema(tables.AggregateTable.Name)).
		Columns(tables.AggregateTable.AllColumns()...).
		Suffix(
			"ON CONFLICT ON CONSTRAINT aggregates_pkey " +
				"DO UPDATE SET " +
				tables.AggregateTable.CurrentVersion + "= excluded." + tables.AggregateTable.CurrentVersion + " , " +
				tables.AggregateTable.CloseTime + "= excluded." + tables.AggregateTable.CloseTime + " , " +
				tables.AggregateTable.LastTransactionTime + "= excluded." + tables.AggregateTable.LastTransactionTime + " , " +
				tables.AggregateTable.LatestValidTime + "= excluded." + tables.AggregateTable.LatestValidTime,
		)

	for _, state := range states {
		query = query.Values(
			mapper.AggregateRowToArrayOfValues(
				mapper.ToAggregateRow(state))...,
		)
	}

	return query.ToSql()
}

func (s SqlSaver) SaveAggregateEvents(ctx context.Context, events []event.PersistenceEvent) (statement string, args []interface{}, err error) {
	query := s.build().Insert(s.tableWithSchema(tables.AggregateEventTable.Name)).Columns(tables.AggregateEventTable.AllColumns()...)
	for _, evt := range events {
		query = query.Values(
			mapper.AggregateEventRowToArrayOfValues(
				mapper.ToAggregateEventRow(evt))...,
		)
	}
	return query.ToSql()
}

func (s SqlSaver) SaveAggregateSnapshots(ctx context.Context, snapShots []event.PersistenceEvent) (statement string, args []interface{}, err error) {
	query := s.build().Insert(s.tableWithSchema(tables.AggregateSnapsShotTable.Name)).Columns(tables.AggregateSnapsShotTable.AllColumns()...)
	for _, evt := range snapShots {
		query = query.Values(
			mapper.AggregateEventRowToArrayOfValues(
				mapper.ToAggregateEventRow(evt))...,
		)
	}
	return query.ToSql()
}

func (s SqlSaver) IsSnapShotsInPatchIntervall(ctx context.Context, snapShots ...event.PersistenceEvent) (statement string, args []interface{}, err error) {
	some := sq.Or{}
	for _, data := range snapShots {
		keys := sq.Eq{
			tables.AggregateEventTable.TenantID:      data.TenantID,
			tables.AggregateEventTable.AggregateID:   data.AggregateID,
			tables.AggregateEventTable.AggregateType: data.AggregateType,
		}
		futurePatch := sq.And{
			sq.LtOrEq{tables.AggregateEventTable.ValidTime: mapper.MapToNanoseconds(data.ValidTime)},
			sq.GtOrEq{tables.AggregateEventTable.TransactionTime: mapper.MapToNanoseconds(data.ValidTime)}}
		historicalPatch := sq.And{
			sq.LtOrEq{tables.AggregateEventTable.TransactionTime: mapper.MapToNanoseconds(data.ValidTime)},
			sq.GtOrEq{tables.AggregateEventTable.ValidTime: mapper.MapToNanoseconds(data.ValidTime)}}
		some = append(some, sq.And{keys, sq.Or{futurePatch, historicalPatch}})
	}

	query := s.build().
		Select(tables.AggregateEventTable.ID).
		From(s.tableWithSchema(tables.AggregateEventTable.Name)).
		Where(
			sq.Or{
				sq.Eq{tables.AggregateEventTable.Class: event.HistoricalPatch},
				sq.Eq{tables.AggregateEventTable.Class: event.FuturePatch}},
		).
		Where(some)

	return query.ToSql()
}

func (s SqlSaver) DeleteSnapShot(ctx context.Context, id shared.AggregateID, sinceTime time.Time) (statement string, args []interface{}, err error) {
	query := s.build().
		Delete(s.tableWithSchema(tables.AggregateSnapsShotTable.Name)).
		Where(sq.Eq{
			tables.AggregateSnapsShotTable.TenantID:      id.TenantID,
			tables.AggregateSnapsShotTable.AggregateID:   id.AggregateID,
			tables.AggregateSnapsShotTable.AggregateType: id.AggregateType,
		}).
		Where(sq.GtOrEq{
			tables.AggregateSnapsShotTable.ValidTime: mapper.MapToNanoseconds(sinceTime),
		})

	return query.ToSql()
}

func (s SqlSaver) DeleteAllInvalidSnapsShots(ctx context.Context, patchEvents ...aggregate.PatchDTO) (statement string, args []interface{}, err error) {
	some := sq.Or{}
	for _, patchEvent := range patchEvents {
		keys := sq.Eq{
			tables.AggregateEventTable.TenantID:      patchEvent.TenantID,
			tables.AggregateEventTable.AggregateID:   patchEvent.AggregateID,
			tables.AggregateEventTable.AggregateType: patchEvent.AggregateType,
		}
		since := sq.GtOrEq{
			tables.AggregateSnapsShotTable.ValidTime: mapper.MapToNanoseconds(patchEvent.PatchTime),
		}
		some = append(some, sq.And{keys, since})
	}

	query := s.build().
		Delete(s.tableWithSchema(tables.AggregateSnapsShotTable.Name)).
		Where(some)

	return query.ToSql()
}

func (s SqlSaver) HardDeleteEvent(ctx context.Context, id shared.AggregateID, evt event.PersistenceEvent) (statement string, args []interface{}, err error) {
	query := s.build().
		Delete(s.tableWithSchema(tables.AggregateEventTable.Name)).
		Where(sq.Eq{
			tables.AggregateEventTable.TenantID:      id.TenantID,
			tables.AggregateEventTable.AggregateID:   id.AggregateID,
			tables.AggregateEventTable.AggregateType: id.AggregateType,
			tables.AggregateEventTable.ID:            evt.ID,
		})

	return query.ToSql()
}

func (s SqlSaver) SoftDeleteEvent(ctx context.Context, id shared.AggregateID, evt event.PersistenceEvent) (statement string, args []interface{}, err error) {
	query := s.build().
		Update(s.tableWithSchema(tables.AggregateEventTable.Name)).
		Set(tables.AggregateEventTable.Class, evt.Class).
		Set(tables.AggregateEventTable.Data, evt.Data).
		Where(sq.Eq{
			tables.AggregateEventTable.TenantID:      id.TenantID,
			tables.AggregateEventTable.AggregateID:   id.AggregateID,
			tables.AggregateEventTable.AggregateType: id.AggregateType,
			tables.AggregateEventTable.ID:            evt.ID,
		})

	return query.ToSql()
}

func (s SqlSaver) DeleteCloseTime(ctx context.Context, id shared.AggregateID) (statement string, args []interface{}, err error) {
	query := s.build().
		Update(s.tableWithSchema(tables.AggregateTable.Name)).
		Set(tables.AggregateTable.CloseTime, mapper.MapToNanoseconds(time.Time{})).
		Where(sq.Eq{
			tables.AggregateTable.TenantID:      id.TenantID,
			tables.AggregateTable.AggregateID:   id.AggregateID,
			tables.AggregateTable.AggregateType: id.AggregateType,
		})

	return query.ToSql()
}
