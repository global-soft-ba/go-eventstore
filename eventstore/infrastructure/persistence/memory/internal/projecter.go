package internal

import (
	"context"
	"fmt"
	"github.com/global-soft-ba/go-eventstore"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/port/persistence/projection"
	trans "github.com/global-soft-ba/go-eventstore/eventstore/core/port/persistence/transactor"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/shared"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/shared/kvTable"
	"github.com/global-soft-ba/go-eventstore/eventstore/infrastructure/persistence/memory/internal/db"
	"sort"
	"time"
)

var maxTime = time.Unix(1<<63-62135596801, 999999999)

type projectedEvent struct {
	event.PersistenceEvent
	ProjectionID string
}

func NewProjecter(trans trans.Port) projection.Port {
	return &projecter{
		trans:                trans,
		loader:               newLoader(trans),
		lockTableProjections: kvTable.NewKeyValuesTable[shared.ProjectionID](),
	}
}

type projecter struct {
	trans                trans.Port
	loader               loader
	lockTableProjections kvTable.IKVTable[shared.ProjectionID]
}

func (p projecter) GetTx(ctx context.Context) *db.MemDBTX {
	t, err := p.trans.GetTX(ctx)
	if err != nil {
		return nil
	}
	return t.(*db.MemDBTX)
}

func (p projecter) Lock(_ context.Context, ids ...shared.ProjectionID) error {
	for _, id := range ids {
		if _, err := kvTable.GetFirst(p.lockTableProjections, kvTable.NewKey(id.TenantID, id.ProjectionID)); err != nil {
			if kvTable.IsKeyNotFound(err) {
				if err = kvTable.Add(p.lockTableProjections, kvTable.NewKey(id.TenantID, id.ProjectionID)); err != nil {
					return fmt.Errorf("LockProjections failed: could not save lock for projection %q: %w", id, err)
				}
			} else {
				return fmt.Errorf("LockProjections failed: could not query locks: %w", err)
			}
		} else {
			return &event.ErrorConcurrentProjectionAccess{
				TenantID:     id.TenantID,
				ProjectionID: id.ProjectionID,
			}
		}
	}

	return nil
}

func (p projecter) UnLock(_ context.Context, ids ...shared.ProjectionID) (err error) {
	for _, id := range ids {
		if err = kvTable.Del(p.lockTableProjections, kvTable.NewKey(id.TenantID, id.ProjectionID)); err != nil {
			if !kvTable.IsKeyNotFound(err) {
				return fmt.Errorf("UnLockProjections failed: could not unlock projection %q: %w", id, err)
			}
		}

	}
	return nil
}

func (p projecter) Get(ctx context.Context, ids ...shared.ProjectionID) ([]projection.DTO, []projection.NotFoundError, error) {
	var result []projection.DTO
	var notFound []projection.NotFoundError
	for _, id := range ids {
		obj, err := p.GetTx(ctx).First(db.TableProjections, db.IdxUnique, id.TenantID, id.ProjectionID)
		if err != nil {
			return nil, nil, fmt.Errorf("get projection failed: %w", err)
		}
		if obj == nil {
			notFound = append(notFound, projection.NotFoundError{ID: id})
		} else {
			proj, ok := obj.(projection.DTO)
			if !ok {
				return nil, nil, fmt.Errorf("get projection type cast failed for value %q", obj)
			}
			result = append(result, proj)
		}
	}
	return result, notFound, nil
}

func (p projecter) GetAllForTenant(ctx context.Context, tenantID string) ([]projection.DTO, error) {
	var dtos []projection.DTO
	it, err := p.GetTx(ctx).Get(db.TableProjections, db.IdxTenantId, tenantID)
	if err != nil {
		return nil, fmt.Errorf("getAllForTenant failed: %w", err)
	}

	for obj := it.Next(); obj != nil; obj = it.Next() {
		proj, ok := obj.(projection.DTO)
		if !ok {
			return nil, fmt.Errorf("getAllForTenant type cast failed %q", obj)
		}
		dtos = append(dtos, proj)
	}

	return dtos, err
}

func (p projecter) GetAllForAllTenants(ctx context.Context) ([]projection.DTO, error) {
	var dtos []projection.DTO
	it, err := p.GetTx(ctx).Get(db.TableProjections, db.IdxUnique)
	if err != nil {
		return nil, fmt.Errorf("getAllForAllTenants failed: %w", err)
	}

	for obj := it.Next(); obj != nil; obj = it.Next() {
		proj, ok := obj.(projection.DTO)
		if !ok {
			return nil, fmt.Errorf("getAllForAllTenants type cast failed %q", obj)
		}
		dtos = append(dtos, proj)
	}

	return dtos, err
}

func (p projecter) SaveStates(ctx context.Context, projections ...projection.DTO) (err error) {
	for _, dto := range projections {
		newDto := projection.DTO{
			TenantID:     dto.TenantID,
			ProjectionID: dto.ProjectionID,
			State:        dto.State,
			UpdatedAt:    time.Now(),
			Events:       nil,
		}
		err = p.GetTx(ctx).Insert(db.TableProjections, newDto)
		if err != nil {
			return fmt.Errorf("save projection failed: %w", err)
		}
	}
	return nil
}

func (p projecter) SaveEvents(ctx context.Context, projections ...projection.DTO) (err error) {
	for _, dto := range projections {
		for _, evt := range dto.Events {
			if err = p.GetTx(ctx).Insert(db.TableProjectionsQueue, projectedEvent{
				PersistenceEvent: evt,
				ProjectionID:     dto.ProjectionID,
			}); err != nil {
				return fmt.Errorf("save projections events failed: %w", err)
			}
		}

	}
	return nil
}

func (p projecter) GetSinceLastRun(ctx context.Context, id shared.ProjectionID, args projection.LoadOptions) (projection.DTO, error) {
	dtos, notFound, err := p.Get(ctx, id)
	if err != nil {
		return projection.DTO{}, err
	}
	if len(notFound) != 0 {
		return projection.DTO{}, &projection.NotFoundError{ID: id}
	}

	dto := dtos[0]
	it, err := p.GetTx(ctx).Get(db.TableProjectionsQueue, db.IdxSetOfId, id.TenantID, id.ProjectionID)
	if err != nil {
		return projection.DTO{}, fmt.Errorf("GetWithNewEventsSinceLastRun failed: %w", err)
	}

	for obj := it.Next(); obj != nil; obj = it.Next() {
		a := obj.(projectedEvent)
		//no future patches
		if a.ValidTime.Before(time.Now()) {
			pEvent := event.PersistenceEvent{
				ID:              a.ID,
				AggregateID:     a.AggregateID,
				TenantID:        a.TenantID,
				AggregateType:   a.AggregateType,
				Version:         a.Version,
				Type:            a.Type,
				Class:           a.Class,
				TransactionTime: a.TransactionTime,
				ValidTime:       a.ValidTime,
				FromMigration:   a.FromMigration,
				Data:            a.Data,
			}
			dto.Events = append(dto.Events, pEvent)
		}
	}

	dto.Events = p.sortEventsWithValidTimeAggIdVersion(dto.Events)
	//care about the chunkSize
	dto.Events = p.restrictWithChunkSize(dto.Events, args.ChunkSize)

	// Delete from queue
	for _, evt := range dto.Events {
		if _, err = p.GetTx(ctx).DeleteAll(db.TableProjectionsQueue, db.IdxUnique, evt.ID, id.TenantID, id.ProjectionID); err != nil {
			return projection.DTO{}, fmt.Errorf("delete of projection queue failed:%w", err)
		}
	}

	return dto, nil
}

func (p projecter) sortEventsWithValidTimeAggIdVersion(stream []event.PersistenceEvent) []event.PersistenceEvent {
	//general sort criteria for all adapter
	sort.SliceStable(stream, func(i, j int) bool {
		if stream[i].ValidTime.Before(stream[j].ValidTime) {
			return true
		} else if stream[i].ValidTime.Equal(stream[j].ValidTime) {
			if stream[i].AggregateID < stream[j].AggregateID {
				return true
			} else if stream[i].AggregateID == stream[j].AggregateID {
				if stream[i].Version < stream[j].Version {
					return true
				}
			}
		}
		return false
	})
	return stream
}

func (p projecter) restrictWithChunkSize(events []event.PersistenceEvent, chunkSize int) []event.PersistenceEvent {
	if len(events) > chunkSize {
		events = events[0:chunkSize]
	}

	return events
}

// ResetSince sinceTime is as valid-time
func (p projecter) ResetSince(txCtx context.Context, id shared.ProjectionID, sinceTime time.Time, eventTypes ...string) error {
	if err := p.emptyQueue(txCtx, id); err != nil {
		return fmt.Errorf("empty queue failed: %w", err)
	}

	// We deleted all events in the queue of the projection, including possible future patches. That's why we have to
	// reload all events, with maxtime
	events, err := p.loadIntoQueue(txCtx, id.TenantID, sinceTime, eventTypes...)
	for _, projEvent := range events {
		if err = p.GetTx(txCtx).Insert(db.TableProjectionsQueue, projectedEvent{
			PersistenceEvent: projEvent,
			ProjectionID:     id.ProjectionID,
		}); err != nil {
			return fmt.Errorf("fill projection queue failed: %w", err)
		}
	}

	return nil
}

func (p projecter) RemoveProjection(ctx context.Context, projectionID string) error {
	iter, err := p.GetTx(ctx).Get(db.TableProjections, db.IdxTenantId)
	if err != nil {
		return fmt.Errorf("get all tenants failed: %w", err)
	}

	var dtos []projection.DTO
	for obj := iter.Next(); obj != nil; obj = iter.Next() {
		proj, ok := obj.(projection.DTO)
		if !ok {
			return fmt.Errorf("getAllForAllTenants type cast failed %q", obj)
		}
		dtos = append(dtos, proj)
	}

	for _, dto := range dtos {
		// delete projection state
		if _, err = p.GetTx(ctx).DeleteAll(db.TableProjections, db.IdxUnique, dto.TenantID, projectionID); err != nil {
			return fmt.Errorf("delete of projection %s failed:%w", dto.ProjectionID, err)
		}

		// delete projection queue
		if _, err = p.GetTx(ctx).DeleteAll(db.TableProjectionsQueue, db.IdxSetOfId, dto.TenantID, projectionID); err != nil {
			return fmt.Errorf("delete of projection queue failed:%w", err)
		}
	}
	return nil
}

func (p projecter) emptyQueue(ctx context.Context, id shared.ProjectionID) error {
	if _, err := p.GetTx(ctx).DeleteAll(db.TableProjectionsQueue, db.IdxSetOfId, id.TenantID, id.ProjectionID); err != nil {
		return fmt.Errorf("delete of projection queue failed:%w", err)
	}
	return nil
}

func (p projecter) loadIntoQueue(ctx context.Context, tenantID string, sinceTime time.Time, eventTypes ...string) ([]event.PersistenceEvent, error) {
	loadAllEventsSinceFilter := func(e event.PersistenceEvent, p, r time.Time) bool {
		if e.ValidTime.After(p) || e.ValidTime.Equal(p) {
			return true
		}
		return false
	}

	stream, err := p.loader.loadAllEventsOfAggregates(ctx, sinceTime, maxTime, loadAllEventsSinceFilter, db.IdxUnique)
	if err != nil {
		return nil, err
	}

	var eventStream []event.PersistenceEvent
	for _, evts := range stream {
		for _, evt := range evts.Events {
			for _, eventType := range eventTypes {
				if evt.Type == eventType && evt.TenantID == tenantID {
					eventStream = append(eventStream, evt)
					break
				}
			}
		}
	}

	return p.sortEventsWithValidTimeAggIdVersion(eventStream), nil
}

func (p projecter) DeleteEventFromQueue(txCtx context.Context, eventID string, id ...shared.ProjectionID) error {
	for _, projectionID := range id {
		if err := p.deleteEventFormPorjectionQueue(txCtx, eventID, projectionID.TenantID, projectionID.ProjectionID); err != nil {
			return fmt.Errorf("delete of projection queue failed: %w", err)
		}
	}
	return nil
}

func (p projecter) deleteEventFormPorjectionQueue(txCtx context.Context, eventID string, tenantID, projectionID string) error {
	it, err := p.GetTx(txCtx).Get(db.TableProjectionsQueue, db.IdxUnique, eventID, tenantID, projectionID)
	if err != nil {
		return fmt.Errorf("retrieveung projection events failed: %w", err)
	}

	for obj := it.Next(); obj != nil; obj = it.Next() {
		projEvent := obj.(projectedEvent)
		if err := p.GetTx(txCtx).Delete(db.TableProjectionsQueue, projEvent); err != nil {
			return fmt.Errorf("delete of projection queue failed: %w", err)
		}
	}

	return nil
}

func (p projecter) GetProjectionsWithEventInQueue(txCtx context.Context, id shared.AggregateID, eventID string) ([]shared.ProjectionID, error) {
	projections, err := p.GetAllForTenant(txCtx, id.TenantID)
	if err != nil {
		return nil, err
	}
	if len(projections) == 0 {
		return nil, nil
	}

	var result []shared.ProjectionID
	for _, dto := range projections {
		if stillInQueue, err := p.isEventStillInQueue(txCtx, eventID, dto.TenantID, dto.ProjectionID); err != nil {
			return nil, fmt.Errorf("checking if event is still in queue failed: %w", err)
		} else if stillInQueue {
			result = append(result, shared.ProjectionID{TenantID: dto.TenantID, ProjectionID: dto.ProjectionID})
		}
	}

	return result, nil
}

func (p projecter) isEventStillInQueue(txCtx context.Context, eventID string, tenantID, projectionID string) (bool, error) {
	it, err := p.GetTx(txCtx).Get(db.TableProjectionsQueue, db.IdxUnique, eventID, tenantID, projectionID)
	if err != nil {
		return false, fmt.Errorf("retrieveung projection events failed: %w", err)
	}

	for obj := it.Next(); obj != nil; obj = it.Next() {
		a := obj.(projectedEvent)
		if a.ID == eventID {
			return true, nil
		}
	}

	return false, nil
}
