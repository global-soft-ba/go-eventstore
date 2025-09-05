package repository

import (
	"context"
	"fmt"
	"github.com/global-soft-ba/go-eventstore"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/application/registry"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/domain/model/aggregate"
	aggPort "github.com/global-soft-ba/go-eventstore/eventstore/core/port/persistence/aggregate"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/shared"
	"github.com/global-soft-ba/go-eventstore/instrumentation/port/metrics"
	"sort"
	"time"
)

func NewAggregateRepository(aggPort aggPort.Port, aggReg *registry.Registries) AggregateRepository {
	return AggregateRepository{aggPort, aggReg}
}

type AggregateRepository struct {
	port aggPort.Port

	registry *registry.Registries
}

func (a AggregateRepository) LoadAsAt(txCtx context.Context, id shared.AggregateID, projectionTime time.Time) (events event.PersistenceEvents, err error) {
	return a.port.LoadAsAt(txCtx, projectionTime, id)
}

func (a AggregateRepository) LoadAsOf(txCtx context.Context, id shared.AggregateID, projectionTime time.Time) (events event.PersistenceEvents, err error) {
	return a.port.LoadAsOf(txCtx, projectionTime, id)
}

func (a AggregateRepository) LoadAsOfTill(txCtx context.Context, id shared.AggregateID, projectionTime, reportTime time.Time) (events event.PersistenceEvents, err error) {
	return a.port.LoadAsOfTill(txCtx, projectionTime, reportTime, id)
}

func (a AggregateRepository) LoadAllOfAggregateAsAt(txCtx context.Context, tenantID, aggregateType string, projectionTime time.Time) (eventStreams []event.PersistenceEvents, err error) {
	return a.port.LoadAllOfAggregateAsAt(txCtx, tenantID, aggregateType, projectionTime)
}

func (a AggregateRepository) LoadAllOfAggregateAsOf(txCtx context.Context, tenantID, aggregateType string, projectionTime time.Time) (eventStreams []event.PersistenceEvents, err error) {
	return a.port.LoadAllOfAggregateAsOf(txCtx, tenantID, aggregateType, projectionTime)
}

func (a AggregateRepository) LoadAllOfAggregateAsOfTill(txCtx context.Context, tenantID, aggregateType string, projectionTime, reportTime time.Time) (eventStreams []event.PersistenceEvents, err error) {
	return a.port.LoadAllOfAggregateAsOfTill(txCtx, tenantID, aggregateType, projectionTime, reportTime)
}

func (a AggregateRepository) LoadAllAsAt(txCtx context.Context, tenantID string, projectionTime time.Time) (eventStreams []event.PersistenceEvents, err error) {
	return a.port.LoadAllAsAt(txCtx, tenantID, projectionTime)
}

func (a AggregateRepository) LoadAllAsOf(txCtx context.Context, tenantID string, projectionTime time.Time) (eventStreams []event.PersistenceEvents, err error) {
	return a.port.LoadAllAsOf(txCtx, tenantID, projectionTime)
}

func (a AggregateRepository) LoadAllAsOfTill(txCtx context.Context, tenantID string, projectionTime time.Time, reportTime time.Time) (eventStreams []event.PersistenceEvents, err error) {
	return a.port.LoadAllAsOfTill(txCtx, tenantID, projectionTime, reportTime)
}

func (a AggregateRepository) GetAggregateState(txCtx context.Context, tenantID, aggregateType, aggregateID string) (event.AggregateState, error) {
	return a.port.GetAggregateState(txCtx, tenantID, aggregateType, aggregateID)
}

func (a AggregateRepository) GetAggregateStatesForAggregateType(txCtx context.Context, tenantID string, aggregateType string) ([]event.AggregateState, error) {
	return a.port.GetAggregateStatesForAggregateType(txCtx, tenantID, aggregateType)
}

func (a AggregateRepository) GetAggregateStatesForAggregateTypeTill(txCtx context.Context, tenantID string, aggregateType string, until time.Time) ([]event.AggregateState, error) {
	return a.port.GetAggregateStatesForAggregateTypeTill(txCtx, tenantID, aggregateType, until)
}

func (a AggregateRepository) GetPatchFreePeriodsForInterval(txCtx context.Context, tenantID, aggregateType, aggregateID string, start time.Time, end time.Time) ([]event.TimeInterval, error) {
	if start.Equal(end) || start.After(end) {
		return nil, fmt.Errorf("GetPatchFreePeriodsForInterval failed: start %q must be smaller then end %q", start, end)
	}

	// see trimming below, why we need the distance between start and end
	if end.Sub(start) < 2*time.Nanosecond {
		return nil, fmt.Errorf("GetPatchFreePeriodsForInterval failed: start %q and end %q are not at least 2 nanoseconds apart", start, end)
	}

	intervals, err := a.port.GetPatchFreePeriodsForInterval(txCtx, tenantID, aggregateType, aggregateID, start, end)
	if err != nil {
		return nil, err
	}

	// Start and end point of the interval can be the start/end of a patch-interval. When inserting snapshots they must be
	// always be larger/smaller than the patch interval and not touch the boundary. Therefore, the interval is trimmed here
	var trimmedIntervals []event.TimeInterval
	for _, interval := range intervals {
		if interval.End.Sub(interval.Start) > 2*time.Nanosecond {
			trimmedIntervals = append(trimmedIntervals, event.TimeInterval{
				Start: interval.Start.Add(time.Nanosecond),
				End:   interval.End.Add(-1 * time.Nanosecond),
			})
		}
	}

	sort.Slice(trimmedIntervals, func(i, j int) bool {
		return trimmedIntervals[i].Start.Before(trimmedIntervals[j].End)
	})

	return trimmedIntervals, err
}

func (a AggregateRepository) Lock(txCtx context.Context, ids ...shared.AggregateID) error {
	txCtx, endSpan := metrics.StartSpan(txCtx, "Lock (repository)", map[string]interface{}{"numberOfAggregates": len(ids)})
	defer endSpan()

	err := a.port.Lock(txCtx, ids...)
	if err != nil {
		return fmt.Errorf("lock() of aggregates failed %q:%w", ids, err)
	}
	return err
}

func (a AggregateRepository) UnLock(txCtx context.Context, ids ...shared.AggregateID) error {
	txCtx, endSpan := metrics.StartSpan(txCtx, "Unlock (repository)", map[string]interface{}{"numberOfAggregates": len(ids)})
	defer endSpan()

	err := a.port.UnLock(txCtx, ids...)
	if err != nil {
		return fmt.Errorf("UnLock() unlock of aggregates failed %q:%w", ids, err)
	}
	return err
}

func (a AggregateRepository) GetAggregate(txCtx context.Context, id shared.AggregateID) (aggregate.Stream, error) {
	txCtx, endSpan := metrics.StartSpan(txCtx, "GetAggregate (repository)", map[string]interface{}{"tenantID": id.TenantID, "aggregateType": id.AggregateType, "aggregateID": id.AggregateID})
	defer endSpan()
	streams, err := a.Get(txCtx, id)
	if err != nil {
		return aggregate.Stream{}, err
	}
	if len(streams) == 0 {
		return aggregate.Stream{}, fmt.Errorf("aggregate %s not found", id)
	} else if len(streams) > 1 {
		return aggregate.Stream{}, fmt.Errorf("more than one aggregate %s found", id)
	}

	return streams[0], nil

}

func (a AggregateRepository) Get(txCtx context.Context, ids ...shared.AggregateID) ([]aggregate.Stream, error) {
	txCtx, endSpan := metrics.StartSpan(txCtx, "GetNotInitialized (repository)", map[string]interface{}{"amount of aggregates": len(ids)})
	defer endSpan()

	var streams []aggregate.Stream

	dtos, _, err := a.port.Get(txCtx, ids...)
	if err != nil {
		return nil, fmt.Errorf("get() get of aggregates failed %q:%w", ids, err)
	}
	for _, dto := range dtos {
		stream := aggregate.LoadStreamFromDTO(dto, a.registry.AggregateRegistry.Options(dto.AggregateType))
		streams = append(streams, stream)
	}

	return streams, err
}

func (a AggregateRepository) GetOrCreate(txCtx context.Context, ids ...shared.AggregateID) (streams []aggregate.Stream, err error) {
	txCtx, endSpan := metrics.StartSpan(txCtx, "GetOrCreate (repository)", map[string]interface{}{"numberOfAggregates": len(ids)})
	defer endSpan()

	dtos, notFound, err := a.port.Get(txCtx, ids...)

	if err != nil {
		return nil, fmt.Errorf("GetOrCreate() get of aggregates failed %q:%w", ids, err)
	}
	for _, dto := range dtos {
		stream := aggregate.LoadStreamFromDTO(dto, a.registry.AggregateRegistry.Options(dto.AggregateType))
		streams = append(streams, stream)
	}

	for _, idNotFound := range notFound {
		stream := aggregate.CreateEmptyStream(idNotFound.ID, a.registry.AggregateRegistry.Options(idNotFound.ID.AggregateType))
		streams = append(streams, stream)
	}
	return streams, err
}

func (a AggregateRepository) Save(txCtx context.Context, streams ...aggregate.Stream) error {
	txCtx, endSpan := metrics.StartSpan(txCtx, "Save (repository)", map[string]interface{}{"numberOfStreams": len(streams)})
	defer endSpan()

	var states []aggPort.DTO
	var events []event.PersistenceEvent
	var snapShots []event.PersistenceEvent
	var patches []aggPort.PatchDTO

	for _, stream := range streams {
		states = append(states, aggPort.DTO{
			TenantID:            stream.ID().TenantID,
			AggregateType:       stream.ID().AggregateType,
			AggregateID:         stream.ID().AggregateID,
			CurrentVersion:      stream.CurrentVersion(),
			LastTransactionTime: stream.LastTransactionTime(),
			LatestValidTime:     stream.LatestValidTime(),
			CreateTime:          stream.CreateTime(),
			CloseTime:           stream.CloseTime(),
		})

		if !stream.EarliestPatchInCurrentStream().IsZero() {
			patch := aggPort.PatchDTO{
				TenantID:      stream.ID().TenantID,
				AggregateType: stream.ID().AggregateType,
				AggregateID:   stream.ID().AggregateID,
				PatchTime:     stream.EarliestPatchInCurrentStream(),
			}
			patches = append(patches, patch)
		}
		events = append(events, stream.Events()...)
		snapShots = append(snapShots, stream.SnapShots()...)
	}

	err := a.port.Save(txCtx, states, events, snapShots)
	if err != nil {
		return fmt.Errorf("save() aggregates failed:%w", err)
	}

	err = a.port.DeleteAllInvalidSnapsShots(txCtx, patches)
	if err != nil {
		return fmt.Errorf("save() update of snapshots failed:%w", err)
	}
	return err
}

func (a AggregateRepository) DisableSnapShots(txCtx context.Context, id shared.AggregateID, sinceTime time.Time) error {
	txCtx, endSpan := metrics.StartSpan(txCtx, "DisableSnapShots (repository)", map[string]interface{}{"tenantID": id.TenantID, "aggregateType": id.AggregateType, "aggregateID": id.AggregateID})
	defer endSpan()

	return a.port.DeleteSnapShot(txCtx, id, sinceTime)
}

func (a AggregateRepository) GetAggregatesEvents(ctx context.Context, tenantID string, page event.PageDTO) ([]event.PersistenceEvent, event.PagesDTO, error) {
	ctx, endSpan := metrics.StartSpan(ctx, "GetAggregatesEvents (repository)", map[string]interface{}{"tenantId": tenantID})
	defer endSpan()

	return a.port.GetAggregatesEvents(ctx, tenantID, page)
}

func (a AggregateRepository) DeleteEvent(txCtx context.Context, id shared.AggregateID, evt event.PersistenceEvent) error {
	txCtx, endSpan := metrics.StartSpan(txCtx, "DeletePatch (repository)", map[string]interface{}{"tenantID": id.TenantID, "aggregateType": id.AggregateType, "aggregateID": id.AggregateID})
	defer endSpan()

	switch a.registry.AggregateRegistry.Options(id.AggregateType).DeleteStrategy {
	case event.HardDelete:
		//delete event
		return a.port.HardDeleteEvent(txCtx, id, evt)
	case event.SoftDelete:
		//mark event as deleted
		return a.port.SoftDeleteEvent(txCtx, id, evt)
	}

	// invalid snapshots
	dto := []aggPort.PatchDTO{{
		TenantID:      id.TenantID,
		AggregateType: id.AggregateType,
		AggregateID:   id.AggregateID,
		PatchTime:     evt.ValidTime}}

	if err := a.port.DeleteAllInvalidSnapsShots(txCtx, dto); err != nil {
		return fmt.Errorf("delete event failed: could not invalid snapshots: %w", err)
	}

	return nil
}

func (a AggregateRepository) UndoDeleteAggregate(txCtx context.Context, id shared.AggregateID) error {
	txCtx, endSpan := metrics.StartSpan(txCtx, "UndoDeleteAggregate (repository)", map[string]interface{}{"tenantID": id.TenantID, "aggregateType": id.AggregateType, "aggregateID": id.AggregateID})
	defer endSpan()

	if err := a.port.UndoCloseStream(txCtx, id); err != nil {
		return fmt.Errorf("delete event failed: could not undo close stream: %w", err)
	}

	return nil
}
