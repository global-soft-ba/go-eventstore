package internal

import (
	"context"
	"fmt"
	"github.com/global-soft-ba/go-eventstore"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/port/persistence/aggregate"
	trans "github.com/global-soft-ba/go-eventstore/eventstore/core/port/persistence/transactor"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/shared"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/shared/kvTable"
	"github.com/global-soft-ba/go-eventstore/eventstore/infrastructure/persistence/memory/internal/db"
	"time"
)

func during(startInterval, endInterval, time time.Time) bool {
	if (startInterval.Before(time) || startInterval.Equal(time)) && endInterval.After(time) || endInterval.Equal(time) {
		return true
	}

	return false
}

func newSaver(trans trans.Port) saver {
	return saver{
		trans:               trans,
		lockTableAggregates: kvTable.NewKeyValuesTable[shared.AggregateID](),
	}
}

type saver struct {
	trans               trans.Port
	lockTableAggregates kvTable.IKVTable[shared.AggregateID]
}

func (s saver) GetTx(ctx context.Context) *db.MemDBTX {
	t, err := s.trans.GetTX(ctx)
	if err != nil {
		return nil
	}
	return t.(*db.MemDBTX)
}

func (s saver) Save(ctx context.Context, states []aggregate.DTO, events []event.PersistenceEvent, snapShots []event.PersistenceEvent) error {
	if err := s.saveStreamState(ctx, states); err != nil {
		return err
	}
	if err := s.saveEvents(ctx, events); err != nil {
		return err
	}
	if err := s.saveSnapShots(ctx, snapShots); err != nil {
		return err
	}
	return nil
}

func (s saver) Get(ctx context.Context, ids ...shared.AggregateID) (aggregates []aggregate.DTO, notFound []aggregate.NotFoundError, err error) {
	var raw interface{}
	for _, id := range ids {
		raw, err = s.GetTx(ctx).First(db.TableAggregates, db.IdxUnique,
			id.TenantID, id.AggregateType, id.AggregateID)
		if err != nil {
			return nil, nil, fmt.Errorf("RetrieveCurrentAggregateStates faield: retrieve of aggregate %q version failed %w", id, err)
		}
		if raw == nil {
			notFound = append(notFound, aggregate.NotFoundError{ID: id})
		} else {
			agg, ok := raw.(aggregate.DTO)
			if !ok {
				return nil, nil, fmt.Errorf("get aggregate type cast failed for value %q", raw)
			}
			aggregates = append(aggregates, agg)
		}
	}
	return aggregates, notFound, err
}

func (s saver) Lock(_ context.Context, ids ...shared.AggregateID) error {
	for _, id := range ids {
		if _, err := kvTable.GetFirst(s.lockTableAggregates, kvTable.NewKey(id.TenantID, id.AggregateType, id.AggregateID)); err != nil {
			if kvTable.IsKeyNotFound(err) {
				if err = kvTable.Add(s.lockTableAggregates, kvTable.NewKey(id.TenantID, id.AggregateType, id.AggregateID)); err != nil {
					return fmt.Errorf("LockAggregates failed: could not save lock for aggregate %q: %w", id.AggregateID, err)
				}
			} else {
				return fmt.Errorf("LockAggregates failed: could not query locks: %w", err)
			}
		} else {
			return &event.ErrorConcurrentAggregateAccess{
				TenantID:      id.TenantID,
				AggregateType: id.AggregateType,
				AggregateID:   id.AggregateID}
		}
	}
	return nil
}

func (s saver) UnLock(_ context.Context, ids ...shared.AggregateID) (err error) {
	for _, id := range ids {
		if err = kvTable.Del(s.lockTableAggregates, kvTable.NewKey(id.TenantID, id.AggregateType, id.AggregateID)); err != nil {
			if !kvTable.IsKeyNotFound(err) {
				return fmt.Errorf("UnLockAggregates failed: could not unlock aggregateID %q: %w", id.AggregateID, err)
			}
		}
	}
	return nil
}

func (s saver) saveStreamState(ctx context.Context, events []aggregate.DTO) (err error) {
	for _, stream := range events {
		err = s.GetTx(ctx).Insert(db.TableAggregates, stream)
		if err != nil {
			return fmt.Errorf("saveStreamState failed: %w", err)
		}
	}
	return nil
}

func (s saver) saveEvents(ctx context.Context, events []event.PersistenceEvent) (err error) {
	for _, evt := range events {
		err = s.GetTx(ctx).InsertEventWithAutoIncrement(db.TableEvent, evt)
		if err != nil {
			return fmt.Errorf("SaveEvents failed: %w", err)
		}
	}
	return nil
}

func (s saver) saveSnapShots(ctx context.Context, events []event.PersistenceEvent) (err error) {
	for _, evt := range events {
		//snapshots become invalid if their valid time is within a patch interval (interval between valid time
		//and transaction time of a patch). We only insert snapshots if they not touch such interval
		var inPatchTimeInterval bool
		inPatchTimeInterval, err = s.isSnapShotInPatchInterval(ctx, evt)
		if err != nil {
			return fmt.Errorf("SaveSnapShots failed: %w", err)
		}

		if inPatchTimeInterval {
			return fmt.Errorf("SaveSnapShots failed: snapshot event of aggregate %s with aggregate type %s and tenant %s is within an patch interval", evt.AggregateID, evt.AggregateType, evt.TenantID)
		}

		err = s.GetTx(ctx).Insert(db.TableSnapShot, evt)
		if err != nil {
			return fmt.Errorf("SaveSnapShots failed: %w", err)
		}

	}
	return nil
}

func (s saver) isSnapShotInPatchInterval(ctx context.Context, evt event.PersistenceEvent) (bool, error) {
	hPatches, err := s.GetTx(ctx).Get(db.TableEvent, db.IdxSetOfIdClass, evt.TenantID, evt.AggregateType, evt.AggregateID, string(event.HistoricalPatch))
	if err != nil {
		return false, err
	}

	fPatches, err := s.GetTx(ctx).Get(db.TableEvent, db.IdxSetOfIdClass, evt.TenantID, evt.AggregateType, evt.AggregateID, string(event.FuturePatch))
	if err != nil {
		return false, err
	}
	var hPatchesHit = false
	var fPatchesHit = false

	for obj := hPatches.Next(); obj != nil; obj = hPatches.Next() {
		e, ok := obj.(db.AutoIncrementEvent)
		if !ok {
			return false, fmt.Errorf("type cast failed %q", obj)
		}
		if during(e.Event.ValidTime, e.Event.TransactionTime, evt.ValidTime) {
			hPatchesHit = true
		}
	}

	for obj := fPatches.Next(); obj != nil; obj = fPatches.Next() {
		e, ok := obj.(db.AutoIncrementEvent)
		if !ok {
			return false, fmt.Errorf("type cast failed %q", obj)
		}
		if during(e.Event.TransactionTime, e.Event.ValidTime, evt.ValidTime) {
			fPatchesHit = true
		}
	}

	return hPatchesHit || fPatchesHit, nil
}

func (s saver) DeleteAllInvalidSnapsShots(ctx context.Context, patches []aggregate.PatchDTO) error {
	for _, patch := range patches {
		existingSnapshots, err := s.GetTx(ctx).Get(db.TableSnapShot, db.IdxSetOfId, patch.TenantID, patch.AggregateType, patch.AggregateID)
		if err != nil {
			return fmt.Errorf("DeleteAllInvalidSnapsShots failed: error accessing existing snapshots %w ", err)
		}

		for obj := existingSnapshots.Next(); obj != nil; obj = existingSnapshots.Next() {
			shot, ok := obj.(event.PersistenceEvent)
			if !ok {
				return fmt.Errorf("type cast failed %q", obj)
			}
			if patch.PatchTime.Before(shot.ValidTime) {
				err = s.GetTx(ctx).Delete(db.TableSnapShot, shot)
				if err != nil {
					return fmt.Errorf("DeleteAllInvalidSnapsShots failed: error updating validity of snapshot %q: %w ", shot.AggregateID, err)
				}
			}
		}
	}
	return nil
}

func (s saver) DeleteSnapShot(ctx context.Context, id shared.AggregateID, sinceTime time.Time) error {
	existingSnapshots, err := s.GetTx(ctx).Get(db.TableSnapShot, db.IdxSetOfId, id.TenantID, id.AggregateType, id.AggregateID)
	if err != nil {
		return fmt.Errorf("DeleteSnapShots failed %w", err)
	}

	for obj := existingSnapshots.Next(); obj != nil; obj = existingSnapshots.Next() {
		shot := obj.(event.PersistenceEvent)
		if !(shot.ValidTime.Before(sinceTime) || shot.Version == 1) { //>1 to make sure that we do not delete the init snapshot
			err = s.GetTx(ctx).Delete(db.TableSnapShot, shot)
			if err != nil {
				return fmt.Errorf("DeleteSnapShots faield: error updating validity of snapshot %v: %w ", shot, err)
			}
		}
	}
	return nil
}

func (s saver) HardDeleteEvent(ctx context.Context, id shared.AggregateID, evt event.PersistenceEvent) error {
	deleteEvent, err := s.GetTx(ctx).Get(db.TableEvent, db.IdxWithEventID, id.TenantID, id.AggregateType, id.AggregateID, evt.ID)
	if err != nil {
		return fmt.Errorf("HardDeleteEvent failed: %w", err)
	}

	for obj := deleteEvent.Next(); obj != nil; obj = deleteEvent.Next() {
		evtToDelete := obj.(db.AutoIncrementEvent)
		err = s.GetTx(ctx).DeleteEventWithAutoIncrement(db.TableEvent, evtToDelete.ID, evt)
		if err != nil {
			return fmt.Errorf("HardDeleteEvent failed: %w", err)
		}
	}
	return nil
}

func (s saver) SoftDeleteEvent(ctx context.Context, id shared.AggregateID, evt event.PersistenceEvent) error {
	updateEvent, err := s.GetTx(ctx).Get(db.TableEvent, db.IdxWithEventID, id.TenantID, id.AggregateType, id.AggregateID, evt.ID)
	if err != nil {
		return fmt.Errorf("HardDeleteEvent failed: %w", err)
	}

	for obj := updateEvent.Next(); obj != nil; obj = updateEvent.Next() {
		evtToUpdate := obj.(db.AutoIncrementEvent)
		evtToUpdate.Event = evt

		err = s.GetTx(ctx).Insert(db.TableEvent, evtToUpdate)
		if err != nil {
			return fmt.Errorf("SoftDeleteEvent failed: %w", err)
		}
	}
	return nil
}

func (s saver) UndoCloseStream(ctx context.Context, id shared.AggregateID) error {
	agg, notFound, err := s.Get(ctx, id)
	if err != nil {
		return err
	}
	if len(notFound) > 0 {
		return fmt.Errorf("aggregate %q not found", id)
	}

	if len(agg) != 1 {
		return fmt.Errorf("multiple (%q) aggregates found with id %q", len(agg), id)
	}

	agg[0].CloseTime = time.Time{}
	return s.saveStreamState(ctx, []aggregate.DTO{agg[0]})
}
