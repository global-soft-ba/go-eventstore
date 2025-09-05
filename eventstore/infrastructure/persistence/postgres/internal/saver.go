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
	copy2 "github.com/global-soft-ba/go-eventstore/eventstore/infrastructure/persistence/postgres/internal/copy"
	"github.com/global-soft-ba/go-eventstore/eventstore/infrastructure/persistence/postgres/internal/dbtx"
	"github.com/global-soft-ba/go-eventstore/eventstore/infrastructure/persistence/postgres/internal/mapper"
	"github.com/global-soft-ba/go-eventstore/eventstore/infrastructure/persistence/postgres/internal/queries"
	"github.com/global-soft-ba/go-eventstore/eventstore/infrastructure/persistence/postgres/internal/tables"
	"github.com/global-soft-ba/go-eventstore/instrumentation/port/metrics"
	"time"
)

func newSaver(dataBaseSchema string, placeholder sq.PlaceholderFormat, trans trans.Port) saver {
	querier := queries.NewSqlSaver(dataBaseSchema, placeholder)
	return saver{sql: querier, trans: trans}
}

type saver struct {
	sql   queries.SqlSaver
	trans trans.Port
}

func (s saver) GetTx(ctx context.Context) (dbtx.DBTX, error) {
	t, err := s.trans.GetTX(ctx)
	return t.(dbtx.DBTX), err
}

// Lock is done with advisory locks, i.e. pg_try_advisory_xact_lock. Such locks are released at the end of a transaction (automatically).
// This is the preferred method here. It's unlikely that we  will hold a transaction open indefinitely for hours.
func (s saver) Lock(ctx context.Context, ids ...shared.AggregateID) error {
	for _, id := range ids {
		var lock tables.AdvisoryLock

		stmt, _, err := s.sql.Lock(ctx, id, lock.LockAlias())
		if err != nil {
			return err
		}
		tx, err := s.GetTx(ctx)
		if err != nil {
			return err
		}
		err = pgxscan.Get(ctx, tx, &lock, stmt)
		if err != nil {
			return fmt.Errorf("LockAggregates failed: could not query locks: %w", err)
		}

		if !lock.Locked {
			return &event.ErrorConcurrentAggregateAccess{
				TenantID:      id.TenantID,
				AggregateType: id.AggregateType,
				AggregateID:   id.AggregateID}
		}
	}

	return nil
}

// UnLock the used advisory lock type (pg_try_advisory_xact_lock) is released automatically, and no unlock is needed.
// However, in order to keep the implementation consistent, unlocking is simulated by simply returning nil.
func (s saver) UnLock(_ context.Context, _ ...shared.AggregateID) error {
	return nil
}

func (s saver) Get(ctx context.Context, ids ...shared.AggregateID) ([]aggregate.DTO, []aggregate.NotFoundError, error) {
	stmt, args, err := s.sql.Get(ctx, ids...)
	if err != nil {
		return nil, nil, err
	}

	var rows []tables.AggregateRow
	tx, err := s.GetTx(ctx)
	if err != nil {
		return nil, nil, err
	}
	err = pgxscan.Select(ctx, tx, &rows, stmt, args...)
	if err != nil {
		return nil, nil, err
	}

	idMap := make(map[shared.AggregateID]bool)
	var result []aggregate.DTO
	for _, row := range rows {
		idMap[shared.NewAggregateID(row.TenantID, row.AggregateType, row.AggregateID)] = true
		result = append(result, mapper.ToAggregate(row)...)
	}

	var notFound []aggregate.NotFoundError
	for _, id := range ids {
		if _, ok := idMap[id]; !ok {
			notFound = append(notFound, aggregate.NotFoundError{ID: id})
		}
	}

	return result, notFound, nil
}

func (s saver) Save(ctx context.Context, states []aggregate.DTO, events []event.PersistenceEvent, snapShots []event.PersistenceEvent) error {
	ctx, endSpan := metrics.StartSpan(ctx, "save (postgres)", map[string]interface{}{"numberOfEvents": len(events) + len(states) + len(snapShots)})
	defer endSpan()

	if err := s.saveAggregates(ctx, states); err != nil {
		return err
	}
	if err := s.saveAggregatesEvents(ctx, events); err != nil {
		return err
	}
	if err := s.saveSnapShots(ctx, snapShots); err != nil {
		return err
	}
	return nil
}

func (s saver) saveAggregates(ctx context.Context, states []aggregate.DTO) error {
	ctx, endSpan := metrics.StartSpan(ctx, "save aggregates (postgres)", map[string]interface{}{"numberOfStates": len(states)})
	defer endSpan()

	if len(states) == 0 {
		return nil
	}

	stmt, arg, err := s.sql.SaveAggregates(ctx, states)
	if err != nil {
		return err
	}

	tx, err := s.GetTx(ctx)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, stmt, arg...)

	return err
}

func (s saver) saveAggregatesEvents(ctx context.Context, events []event.PersistenceEvent) error {
	ctx, endSpan := metrics.StartSpan(ctx, "save events (postgres)", map[string]interface{}{"numberOfStates": len(events)})
	defer endSpan()

	switch len(events) {
	case 0:
	case 1, 2, 3, 4:
		stmt, arg, err := s.sql.SaveAggregateEvents(ctx, events)
		if err != nil {
			return err
		}
		tx, err := s.GetTx(ctx)
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx, stmt, arg...)
		if err != nil {
			return err
		}
	default:
		tx, err := s.GetTx(ctx)
		if err != nil {
			return err
		}
		inserted, err := tx.CopyFrom(
			ctx,
			[]string{s.sql.GetDatabaseSchema(), tables.AggregateEventTable.Name},
			tables.AggregateEventTable.AllColumns(),
			copy2.NewAggregateEventIterator(mapper.ToAggregateEventRows(events...)))
		if err != nil {
			return err
		}
		if int(inserted) != len(events) {
			return fmt.Errorf("could not save events: only %v from %v events would have been inserted", inserted, len(events))
		}
	}
	return nil
}

func (s saver) saveSnapShots(ctx context.Context, snapShots []event.PersistenceEvent) error {
	ctx, endSpan := metrics.StartSpan(ctx, "save snapshots (postgres)", map[string]interface{}{"numberOfSnapshots": len(snapShots)})
	defer endSpan()

	if len(snapShots) > 0 {
		if err := s.isSnapShotInPatchIntervall(ctx, snapShots); err != nil {
			return err
		}

		switch len(snapShots) {
		case 0:
		case 1, 2, 3, 4:
			stmt, args, err := s.sql.SaveAggregateSnapshots(ctx, snapShots)
			if err != nil {
				return err
			}
			tx, err := s.GetTx(ctx)
			if err != nil {
				return err
			}
			_, err = tx.Exec(ctx, stmt, args...)
			if err != nil {
				return err
			}
		default:
			tx, err := s.GetTx(ctx)
			if err != nil {
				return err
			}
			inserted, err := tx.CopyFrom(
				ctx,
				[]string{s.sql.GetDatabaseSchema(), tables.AggregateSnapsShotTable.Name},
				tables.AggregateSnapsShotTable.AllColumns(),
				copy2.NewAggregateEventIterator(mapper.ToAggregateEventRows(snapShots...)))
			if err != nil {
				return err
			}
			if int(inserted) != len(snapShots) {
				return fmt.Errorf("could not save snapshots: only %v from %v events would have been inserted", inserted, len(snapShots))
			}
		}
	}
	return nil
}

func (s saver) isSnapShotInPatchIntervall(ctx context.Context, snapShots []event.PersistenceEvent) error {
	stmt, args, err := s.sql.IsSnapShotsInPatchIntervall(ctx, snapShots...)
	if err != nil {
		return err
	}
	tx, err := s.GetTx(ctx)
	if err != nil {
		return err
	}

	rows, err := tx.Query(ctx, stmt, args...)
	defer rows.Close()
	if err != nil {
		return err
	}

	if rows.Next() {
		return fmt.Errorf("SaveSnapShots failed: snapshot event of aggregate is within a patch interval")
	}
	return err
}

func (s saver) DeleteSnapShot(ctx context.Context, id shared.AggregateID, sinceTime time.Time) error {
	stmt, args, err := s.sql.DeleteSnapShot(ctx, id, sinceTime)
	if err != nil {
		return err
	}
	tx, err := s.GetTx(ctx)
	if err != nil {
		return err
	}

	_, err = tx.Exec(ctx, stmt, args...)
	if err != nil {
		return err
	}

	return err
}

func (s saver) DeleteAllInvalidSnapsShots(ctx context.Context, patchEvents []aggregate.PatchDTO) error {
	ctx, endSpan := metrics.StartSpan(ctx, "deleteAllInvalidSnapsShots (postgres)", map[string]interface{}{"numberOfPatches": len(patchEvents)})
	defer endSpan()

	if patchEvents == nil {
		return nil
	}

	stmt, args, err := s.sql.DeleteAllInvalidSnapsShots(ctx, patchEvents...)
	if err != nil {
		return err
	}
	tx, err := s.GetTx(ctx)
	if err != nil {
		return err
	}

	_, err = tx.Exec(ctx, stmt, args...)
	if err != nil {
		return err
	}

	return err
}

func (s saver) HardDeleteEvent(ctx context.Context, id shared.AggregateID, evt event.PersistenceEvent) error {
	stmt, args, err := s.sql.HardDeleteEvent(ctx, id, evt)
	if err != nil {
		return err
	}
	tx, err := s.GetTx(ctx)
	if err != nil {
		return err
	}

	_, err = tx.Exec(ctx, stmt, args...)
	if err != nil {
		return err
	}

	return err
}

func (s saver) SoftDeleteEvent(ctx context.Context, id shared.AggregateID, evt event.PersistenceEvent) error {
	stmt, args, err := s.sql.SoftDeleteEvent(ctx, id, evt)
	if err != nil {
		return err
	}
	tx, err := s.GetTx(ctx)
	if err != nil {
		return err
	}

	_, err = tx.Exec(ctx, stmt, args...)
	if err != nil {
		return err
	}

	return err
}

func (s saver) UndoCloseStream(ctx context.Context, id shared.AggregateID) error {
	stmt, args, err := s.sql.DeleteCloseTime(ctx, id)
	if err != nil {
		return err
	}
	tx, err := s.GetTx(ctx)
	if err != nil {
		return err
	}

	_, err = tx.Exec(ctx, stmt, args...)
	if err != nil {
		return err
	}

	return err
}
