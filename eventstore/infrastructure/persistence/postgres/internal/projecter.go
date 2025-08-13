package internal

import (
	"context"
	"fmt"
	sq "github.com/Masterminds/squirrel"
	"github.com/georgysavva/scany/v2/pgxscan"
	"github.com/global-soft-ba/go-eventstore"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/port/persistence/projection"
	trans "github.com/global-soft-ba/go-eventstore/eventstore/core/port/persistence/transactor"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/shared"
	copy2 "github.com/global-soft-ba/go-eventstore/eventstore/infrastructure/persistence/postgres/internal/copy"
	"github.com/global-soft-ba/go-eventstore/eventstore/infrastructure/persistence/postgres/internal/dbtx"
	"github.com/global-soft-ba/go-eventstore/eventstore/infrastructure/persistence/postgres/internal/mapper"
	"github.com/global-soft-ba/go-eventstore/eventstore/infrastructure/persistence/postgres/internal/queries"
	"github.com/global-soft-ba/go-eventstore/eventstore/infrastructure/persistence/postgres/internal/tables"
	"time"
)

func NewProjecter(dataBaseSchema string, placeholder sq.PlaceholderFormat, trans trans.Port) projection.Port {
	querier := queries.NewSqlProjecter(dataBaseSchema, placeholder)
	return &projecter{sql: querier, trans: trans}
}

type projecter struct {
	sql   queries.SqlProjecter
	trans trans.Port
}

func (p projecter) GetTx(ctx context.Context) (dbtx.DBTX, error) {
	t, err := p.trans.GetTX(ctx)
	return t.(dbtx.DBTX), err
}

// Lock is done with advisory locks, i.e. pg_try_advisory_xact_lock. Such locks are released at the end of a transaction (automatically).
// This is the preferred method here. It's unlikely that we  will hold a transaction open indefinitely for hours.
func (p projecter) Lock(ctx context.Context, ids ...shared.ProjectionID) error {
	for _, id := range ids {
		var lock tables.AdvisoryLock

		stmt, _, err := p.sql.Lock(ctx, id, lock.LockAlias()) //same name as in struct advisoryLock
		if err != nil {
			return err
		}

		tx, err := p.GetTx(ctx)
		if err != nil {
			return err
		}
		err = pgxscan.Get(ctx, tx, &lock, stmt)
		if err != nil {
			return fmt.Errorf("LockProjections failed: could not query locks: %w", err)
		}

		if !lock.Locked {
			return &event.ErrorConcurrentProjectionAccess{
				TenantID:     id.TenantID,
				ProjectionID: id.ProjectionID}
		}
	}

	return nil
}

// UnLock the used advisory lock type (pg_try_advisory_xact_lock) is released automatically, and no unlock is needed.
// However, in order to keep the implementation consistent, unlocking is simulated by simply returning nil.
func (p projecter) UnLock(_ context.Context, _ ...shared.ProjectionID) error {
	return nil
}

func (p projecter) Get(ctx context.Context, ids ...shared.ProjectionID) ([]projection.DTO, []projection.NotFoundError, error) {
	stmt, args, err := p.sql.Get(ctx, ids...)
	if err != nil {
		return nil, nil, err
	}

	var rows []tables.ProjectionsRow
	tx, err := p.GetTx(ctx)
	if err != nil {
		return nil, nil, err
	}
	err = pgxscan.Select(ctx, tx, &rows, stmt, args...)
	if err != nil {
		return nil, nil, err
	}

	idMap := make(map[shared.ProjectionID]bool)
	var result []projection.DTO
	for _, row := range rows {
		idMap[shared.NewProjectionID(row.TenantID, row.ProjectionID)] = true
		result = append(result, mapper.ToProjections(row)...)
	}

	var notFound []projection.NotFoundError
	for _, id := range ids {
		if _, ok := idMap[id]; !ok {
			notFound = append(notFound, projection.NotFoundError{ID: id})
		}
	}

	return result, notFound, nil

}

func (p projecter) GetAllForTenant(ctx context.Context, tenantID string) ([]projection.DTO, error) {
	stmt, args, err := p.sql.GetAllForTenant(ctx, tenantID)
	if err != nil {
		return nil, err
	}

	var rows []tables.ProjectionsRow
	tx, err := p.GetTx(ctx)
	if err != nil {
		return nil, err
	}
	err = pgxscan.Select(ctx, tx, &rows, stmt, args...)
	if err != nil {
		return nil, err
	}

	return mapper.ToProjections(rows...), err
}

func (p projecter) SaveStates(ctx context.Context, projections ...projection.DTO) error {
	if projections == nil {
		return nil
	}

	stmt, arg, err := p.sql.SaveStates(ctx, projections...)
	if err != nil {
		return err
	}
	tx, err := p.GetTx(ctx)
	if err != nil {
		return err
	}

	_, err = tx.Exec(ctx, stmt, arg...)

	return err
}

func (p projecter) SaveEvents(ctx context.Context, projections ...projection.DTO) error {
	if projections == nil {
		return nil
	}

	projectionEvents := mapper.ToProjectionsEventRows(projections...)
	switch len(projectionEvents) {
	case 0:
	case 1, 2, 3, 4:
		stmt, arg, err := p.sql.SaveProjectionEvents(ctx, projectionEvents...)
		if err != nil {
			return err
		}

		tx, err := p.GetTx(ctx)
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx, stmt, arg...)
		if err != nil {
			return err
		}
	default:
		tx, err := p.GetTx(ctx)
		if err != nil {
			return err
		}
		inserted, err := tx.CopyFrom(
			ctx,
			[]string{p.sql.GetDatabaseSchema(), tables.ProjectionsEventsTable.Name},
			tables.ProjectionsEventsTable.AllColumns(),
			copy2.NewProjectionEventIterator(projectionEvents))
		if err != nil {
			return err
		}
		if int(inserted) != len(projectionEvents) {
			return fmt.Errorf("could not save events: only %v from %v events would have been inserted", inserted, len(projectionEvents))
		}
	}
	return nil
}

func (p projecter) GetAllForAllTenants(ctx context.Context) ([]projection.DTO, error) {
	stmt, args, err := p.sql.GetAllForAllTenants(ctx)
	if err != nil {
		return nil, err
	}

	var rows []tables.ProjectionsRow
	tx, err := p.GetTx(ctx)
	if err != nil {
		return nil, err
	}
	err = pgxscan.Select(ctx, tx, &rows, stmt, args...)
	if err != nil {
		return nil, err
	}

	return mapper.ToProjections(rows...), err
}

func (p projecter) GetSinceLastRun(ctx context.Context, id shared.ProjectionID, loadOpt projection.LoadOptions) (projection.DTO, error) {
	sinceTime := time.Now()
	sinceTimeStamp := mapper.MapToNanoseconds(sinceTime)
	stmt, args, err := p.sql.GetSinceLastRun(ctx, id, loadOpt, sinceTimeStamp)
	if err != nil {
		return projection.DTO{}, err
	}

	var rows []tables.ProjectionsEventsLoadRow
	tx, err := p.GetTx(ctx)
	if err != nil {
		return projection.DTO{}, err
	}
	err = pgxscan.Select(ctx, tx, &rows, stmt, args...)
	if err != nil {
		return projection.DTO{}, err
	}

	stmt, args, err = p.sql.DeleteRows(ctx, rows...)
	if err != nil {
		return projection.DTO{}, err
	}

	_, err = tx.Exec(ctx, stmt, args...)
	if err != nil {
		return projection.DTO{}, err
	}

	return mapper.ToProjection(sinceTimeStamp, rows...), err
}

func (p projecter) ResetSince(ctx context.Context, id shared.ProjectionID, sinceTime time.Time, eventTypes ...string) error {
	stmt, args, err := p.sql.DeleteEvents(ctx, id)
	if err != nil {
		return err
	}
	tx, err := p.GetTx(ctx)
	if err != nil {
		return err
	}

	_, err = tx.Exec(ctx, stmt, args...)

	stmt, args, err = p.sql.ResetSince(ctx, id, sinceTime, eventTypes...)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, stmt, args...)

	return err
}

func (p projecter) RemoveProjection(ctx context.Context, projectionID string) error {
	if err := p.removeProjectionState(ctx, projectionID); err != nil {
		return fmt.Errorf("could not remove projection state: %w", err)
	}

	if err := p.removeProjectionEvents(ctx, projectionID); err != nil {
		return fmt.Errorf("could not remove projection events: %w", err)
	}

	return nil
}

func (p projecter) removeProjectionState(ctx context.Context, projectionID string) error {
	stmt, args, err := p.sql.RemoveProjectionState(ctx, projectionID)
	if err != nil {
		return err
	}

	tx, err := p.GetTx(ctx)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, stmt, args...)
	return err
}

func (p projecter) removeProjectionEvents(ctx context.Context, projectionID string) error {
	stmt, args, err := p.sql.RemoveProjectionEvents(ctx, projectionID)
	if err != nil {
		return err
	}

	tx, err := p.GetTx(ctx)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, stmt, args...)
	return err

}

func (p projecter) DeleteEventFromQueue(txCtx context.Context, eventID string, ids ...shared.ProjectionID) error {
	stmt, args, err := p.sql.DeleteEventFromQueue(txCtx, eventID, ids)
	if err != nil {
		return err
	}

	tx, err := p.GetTx(txCtx)
	if err != nil {
		return err
	}

	_, err = tx.Exec(txCtx, stmt, args...)
	return err

}

func (p projecter) GetProjectionsWithEventInQueue(txCtx context.Context, id shared.AggregateID, eventID string) ([]shared.ProjectionID, error) {
	stmt, args, err := p.sql.GetProjectionsWithEventInQueue(txCtx, id, eventID)
	if err != nil {
		return nil, err
	}

	var rows []tables.ProjectionsEventRow
	tx, err := p.GetTx(txCtx)
	if err != nil {
		return nil, err
	}
	err = pgxscan.Select(txCtx, tx, &rows, stmt, args...)
	if err != nil {
		return nil, err
	}

	var result []shared.ProjectionID
	for _, row := range rows {
		result = append(result, shared.NewProjectionID(row.TenantID, row.ProjectionID))
	}

	return result, nil
}
