package internal

import (
	"context"
	"fmt"
	"github.com/global-soft-ba/go-eventstore"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/port/persistence/aggregate"
	trans "github.com/global-soft-ba/go-eventstore/eventstore/core/port/persistence/transactor"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/shared"
	"github.com/global-soft-ba/go-eventstore/eventstore/infrastructure/persistence/memory/internal/db"
	"github.com/hashicorp/go-memdb"
	"reflect"

	"sort"
	"time"
)

// ErrorEmptyEventStream used for aggregates without event streams
type emptyAggregateVersionError struct {
}

func (c *emptyAggregateVersionError) Error() string {
	return fmt.Sprintf("retrieval of aggregate version failed")
}

func newLoader(trans trans.Port) loader {
	return loader{
		trans: trans,
	}
}

type loader struct {
	trans trans.Port
}

func (l loader) GetTx(ctx context.Context) *db.MemDBTX {
	t, err := l.trans.GetTX(ctx)
	if err != nil {
		return nil
	}
	return t.(*db.MemDBTX)
}

func (l loader) LoadAsAt(ctx context.Context, projectionTime time.Time, id shared.AggregateID) (event.PersistenceEvents, error) {
	pEvent, err := l.loadAsAt(ctx, projectionTime, db.IdxSetOfId, id.TenantID, id.AggregateType, id.AggregateID)
	if err != nil {
		return event.PersistenceEvents{}, err
	}

	if len(pEvent) == 0 {
		return event.PersistenceEvents{}, &event.ErrorEmptyEventStream{
			AggregateID:   id.AggregateID,
			TenantID:      id.TenantID,
			AggregateType: id.AggregateType,
			Err:           err,
		}
	}

	return pEvent[0], nil
}

func (l loader) LoadAsOf(ctx context.Context, projectionTime time.Time, id shared.AggregateID) (event.PersistenceEvents, error) {
	pEvent, err := l.loadAsOf(ctx, projectionTime, db.IdxSetOfId, id.TenantID, id.AggregateType, id.AggregateID)
	if err != nil {
		return event.PersistenceEvents{}, err
	}

	if len(pEvent) == 0 {
		return event.PersistenceEvents{}, &event.ErrorEmptyEventStream{
			AggregateID:   id.AggregateID,
			TenantID:      id.TenantID,
			AggregateType: id.AggregateType,
			Err:           err,
		}
	}

	return pEvent[0], nil
}

func (l loader) LoadAsOfTill(ctx context.Context, projectionTime time.Time, reportTime time.Time, id shared.AggregateID) (event.PersistenceEvents, error) {
	pEvent, err := l.loadAsOfTill(ctx, projectionTime, reportTime, db.IdxSetOfId, id.TenantID, id.AggregateType, id.AggregateID)
	if err != nil {
		return event.PersistenceEvents{}, err
	}

	if len(pEvent) == 0 {
		return event.PersistenceEvents{}, &event.ErrorEmptyEventStream{
			AggregateID:   id.AggregateID,
			TenantID:      id.TenantID,
			AggregateType: id.AggregateType,
			Err:           err,
		}
	}

	return pEvent[0], nil
}

func (l loader) LoadAllOfAggregateAsAt(ctx context.Context, tenantID, aggregateType string, projectionTime time.Time) (eventStreams []event.PersistenceEvents, err error) {
	pEvent, err := l.loadAsAt(ctx, projectionTime, db.IdxTenantIdAggregateType, tenantID, aggregateType)
	if err != nil {
		return nil, err
	}

	if len(pEvent) == 0 {
		return nil, &event.ErrorEmptyEventStream{
			TenantID:      tenantID,
			AggregateType: aggregateType,
			Err:           err,
		}
	}

	return pEvent, err
}

func (l loader) LoadAllOfAggregateAsOf(ctx context.Context, tenantID, aggregateType string, projectionTime time.Time) (eventStreams []event.PersistenceEvents, err error) {
	pEvent, err := l.loadAsOf(ctx, projectionTime, db.IdxTenantIdAggregateType, tenantID, aggregateType)
	if err != nil {
		return nil, err
	}

	if len(pEvent) == 0 {
		return nil, &event.ErrorEmptyEventStream{
			TenantID:      tenantID,
			AggregateType: aggregateType,
			Err:           err,
		}
	}

	return pEvent, err
}

func (l loader) LoadAllOfAggregateAsOfTill(ctx context.Context, tenantID, aggregateType string, projectionTime time.Time, reportTime time.Time) (eventStreams []event.PersistenceEvents, err error) {
	pEvent, err := l.loadAsOfTill(ctx, projectionTime, reportTime, db.IdxTenantIdAggregateType, tenantID, aggregateType)
	if err != nil {
		return nil, err
	}

	if len(pEvent) == 0 {
		return nil, &event.ErrorEmptyEventStream{
			TenantID:      tenantID,
			AggregateType: aggregateType,
			Err:           err,
		}
	}

	return pEvent, err
}

func (l loader) LoadAllAsAt(ctx context.Context, tenantID string, projectionTime time.Time) (eventStreams []event.PersistenceEvents, err error) {
	pEvent, err := l.loadAsAt(ctx, projectionTime, db.IdxTenantId, tenantID)
	if err != nil {
		return nil, err
	}

	if len(pEvent) == 0 {
		return nil, &event.ErrorEmptyEventStream{
			TenantID: tenantID,
			Err:      err,
		}
	}

	return pEvent, err
}

func (l loader) LoadAllAsOf(ctx context.Context, tenantID string, projectionTime time.Time) (eventStreams []event.PersistenceEvents, err error) {
	pEvent, err := l.loadAsOf(ctx, projectionTime, db.IdxTenantId, tenantID)
	if err != nil {
		return nil, err
	}

	if len(pEvent) == 0 {
		return nil, &event.ErrorEmptyEventStream{
			TenantID: tenantID,
			Err:      err,
		}
	}

	return pEvent, err
}

func (l loader) LoadAllAsOfTill(ctx context.Context, tenantID string, projectionTime time.Time, reportTime time.Time) (eventStreams []event.PersistenceEvents, err error) {
	pEvent, err := l.loadAsOfTill(ctx, projectionTime, reportTime, db.IdxTenantId, tenantID)
	if err != nil {
		return nil, err
	}

	if len(pEvent) == 0 {
		return nil, &event.ErrorEmptyEventStream{
			TenantID: tenantID,
			Err:      err,
		}
	}

	return pEvent, err
}

func (l loader) loadAsAt(ctx context.Context, projectionTime time.Time, index string, keys ...interface{}) (eventStream []event.PersistenceEvents, err error) {
	loadAsAtFilter := func(e event.PersistenceEvent, p, r time.Time) bool {
		if (e.TransactionTime.Before(p) || e.TransactionTime.Equal(p)) && (e.ValidTime.Before(p) || e.ValidTime.Equal(p)) {
			return true
		}
		return false
	}

	return l.loadAllEventsOfAggregates(ctx, projectionTime, projectionTime, loadAsAtFilter, index, keys...)
}

func (l loader) loadAsOf(ctx context.Context, projectionTime time.Time, index string, keys ...interface{}) (eventStream []event.PersistenceEvents, err error) {
	loadAsOfFilter := func(e event.PersistenceEvent, p, r time.Time) bool {
		if e.ValidTime.Before(p) || e.ValidTime.Equal(p) {
			return true
		}
		return false
	}
	return l.loadAllEventsOfAggregates(ctx, projectionTime, projectionTime, loadAsOfFilter, index, keys...)
}

func (l loader) loadAsOfTill(ctx context.Context, projectionTime time.Time, reportTime time.Time, index string, keys ...interface{}) (eventStream []event.PersistenceEvents, err error) {
	loadAsOfTillFilter := func(e event.PersistenceEvent, p, r time.Time) bool {
		if (e.ValidTime.Before(p) || e.ValidTime.Equal(p)) && (e.TransactionTime.Before(reportTime) || e.TransactionTime.Equal(reportTime)) {
			return true
		}
		return false
	}
	return l.loadAllEventsOfAggregates(ctx, projectionTime, projectionTime, loadAsOfTillFilter, index, keys...)
}

func (l loader) loadAllEventsOfAggregates(ctx context.Context, projectionTime, reportTime time.Time,
	filter func(evt event.PersistenceEvent, projectionTime, reportingTime time.Time) bool, idx string, keys ...interface{}) (eventStream []event.PersistenceEvents, err error) {

	retrieveAggregates, err := l.retrieveAggregates(ctx, idx, keys...)
	if err != nil {
		return nil, fmt.Errorf("retrieval of aggregate version failed: %w", err)
	}

	for _, agg := range retrieveAggregates {
		stream, err := l.loadEventStreamOfAggregate(ctx, agg, projectionTime, reportTime, filter)
		if err != nil {
			return nil, fmt.Errorf("loadAsAt failed: %w", err)
		}

		if len(stream) > 0 {
			pEvent := event.PersistenceEvents{
				Events:  stream,
				Version: int(agg.CurrentVersion)}
			eventStream = append(eventStream, pEvent)
		}
	}

	return eventStream, err
}

func (l loader) retrieveAggregates(ctx context.Context, index string, keys ...interface{}) (aggregates []aggregate.DTO, err error) {
	var it memdb.ResultIterator

	it, err = l.GetTx(ctx).Get(db.TableAggregates, index, keys...)
	if err != nil {
		return nil, fmt.Errorf("access on aggregate table failed %w for key %v", err, keys)
	}
	if it == nil {
		return nil, &emptyAggregateVersionError{}
	}

	for obj := it.Next(); obj != nil; obj = it.Next() {
		a, ok := obj.(aggregate.DTO)
		if !ok {
			return nil, fmt.Errorf("type cast failed for value %q", obj)
		}
		aggregates = append(aggregates, a)
	}

	return aggregates, err
}

func (l loader) loadEventStreamOfAggregate(
	ctx context.Context,
	aggregate aggregate.DTO,
	projectionTime, reportTime time.Time,
	filter func(evt event.PersistenceEvent, projectionTime, reportingTime time.Time) bool) (stream []event.PersistenceEvent, err error) {

	//retrieve most recent snapshot with respect to loadAsOf, loadAsAt and loadAsOfTill semantic
	snapshot, err := l.retrieveMostRecentSnapshot(ctx, aggregate, projectionTime, reportTime, filter)
	if err != nil {
		return nil, err
	}

	snapShotTime := time.Time{}
	snapShotVersion := 0
	if !reflect.DeepEqual(snapshot, event.PersistenceEvent{}) {
		stream = append(stream, snapshot)
		snapShotTime = snapshot.ValidTime
		snapShotVersion = snapshot.Version
	}

	//retrieve event stream
	entireStream, err := l.GetTx(ctx).Get(db.TableEvent, db.IdxSetOfId, aggregate.TenantID, aggregate.AggregateType, aggregate.AggregateID)
	if err != nil || entireStream == nil {
		return nil, &event.ErrorEmptyEventStream{
			AggregateID:   aggregate.AggregateID,
			TenantID:      aggregate.TenantID,
			AggregateType: aggregate.AggregateType,
			Err:           err,
		}
	}

	//return all events after the snapshot with respect to loadAsOf, loadAsAt and loadAsOfTill semantic
	for obj := entireStream.Next(); obj != nil; obj = entireStream.Next() {
		incEvt, ok := obj.(db.AutoIncrementEvent)
		if !ok {
			return nil, fmt.Errorf("type cast failed for value %q", obj)
		}

		evt := incEvt.Event
		if filter(evt, projectionTime, reportTime) &&
			(evt.ValidTime.After(snapShotTime) ||
				(evt.ValidTime.Equal(snapShotTime) && evt.Version > snapShotVersion)) /* case create and first events at the same time*/ {
			stream = append(stream, evt)
		}
	}

	//sorting is independent of loadAsOf, loadAsAt and loadAsOfTill semantic
	sort.SliceStable(stream, func(i, j int) bool {
		return stream[i].ValidTime.Before(stream[j].ValidTime) || (stream[i].ValidTime.Equal(stream[j].ValidTime) && stream[i].Version < stream[j].Version)
	})

	//remove deleted events
	for i := 0; i < len(stream); i++ {
		if stream[i].Class == event.DeletePatch {
			stream = append(stream[:i], stream[i+1:]...)
			i--
		}

	}

	return stream, err
}

func (l loader) retrieveMostRecentSnapshot(ctx context.Context, agg aggregate.DTO, projectionTime, reportTime time.Time,
	semanticFilter func(evt event.PersistenceEvent, projectionTime, reportingTime time.Time) bool) (snapshot event.PersistenceEvent, err error) {

	snapshots, err := l.GetTx(ctx).Get(db.TableSnapShot, db.IdxSetOfId, agg.TenantID, agg.AggregateType, agg.AggregateID)
	if err != nil {
		return event.PersistenceEvent{}, fmt.Errorf("retrieval of at least initial snapshots failed for aggreagte %v:%w", agg.AggregateID, err)
	}

	//use all events with respect to loadAsOf, loadAsAt and loadAsOfTill semantic which is covered by semanticFilter
	//and get the most recent snapshot for given projection time.
	//Wrap the iterator and use filter. FilterFunc is a function that takes the results of an iterator and
	//returns whether the result should be filtered out.
	filterFunc := func(raw interface{}) bool {
		obj, ok := raw.(event.PersistenceEvent)
		if !ok {
			return true
		}
		filterOut := semanticFilter(obj, projectionTime, reportTime) && (obj.ValidTime.After(snapshot.ValidTime) || obj.ValidTime.Equal(snapshot.ValidTime))
		return !filterOut
	}
	filterIt := memdb.NewFilterIterator(snapshots, filterFunc)

	for obj := filterIt.Next(); obj != nil; obj = filterIt.Next() {
		var ok bool
		snapshot, ok = obj.(event.PersistenceEvent)
		if !ok {
			return event.PersistenceEvent{}, fmt.Errorf("type cast failed %s", snapshot.AggregateID)
		}
	}

	return snapshot, err
}
