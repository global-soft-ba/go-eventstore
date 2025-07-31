package service

import (
	"context"
	"fmt"
	"github.com/global-soft-ba/go-eventstore"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/domain/model/aggregate"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/domain/model/projection"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/port/consistentClock"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/shared"
)

type addArgs struct {
	AggStream  *aggregate.Stream
	ProjStream []*projection.Stream
}

type DomainService struct {
	Clock consistentClock.Port
}

func (d DomainService) GetUniqueAggregateIDsAndEventTypes(_ context.Context, tenantID string, events []event.PersistenceEvents) (result []shared.AggregateID, eventTypes []string, err error) {
	uniqueSelectors := make(map[shared.AggregateID]bool)
	uniqueEventTypes := make(map[string]bool)

	for _, persistenceEvents := range events {
		if len(persistenceEvents.Events) == 0 {
			return nil, nil, fmt.Errorf("GetUniqueAggregateIDsAndEventTypes() no events in stream found for tenant %q", tenantID)
		}

		aggregateID := shared.NewAggregateID(tenantID, persistenceEvents.Events[0].AggregateType, persistenceEvents.Events[0].AggregateID)
		if _, exists := uniqueSelectors[aggregateID]; !exists {
			result = append(result, aggregateID)
			uniqueSelectors[aggregateID] = true
		}

		for _, evt := range persistenceEvents.Events {
			if _, exists := uniqueEventTypes[evt.Type]; !exists {
				eventTypes = append(eventTypes, evt.Type)
				uniqueEventTypes[evt.Type] = true
			}

		}
	}

	return result, eventTypes, err
}

func (d DomainService) AddEventsInStreams(ctx context.Context, events []event.PersistenceEvents, cache *StreamCollection) error {
	for _, pEvents := range events {
		if !(len(pEvents.Events) != 0) {
			return fmt.Errorf("no event stream found")
		}
		evt := pEvents.Events[0]
		id := shared.NewAggregateID(evt.TenantID, evt.AggregateType, evt.AggregateID)
		aggStream, err := cache.GetAggregate(ctx, id)
		if err != nil {

			return fmt.Errorf("get from collection failed: %w", err)
		}
		// open checks if version / modification strategy etc. is correct
		if err = aggStream.Open(pEvents.Version); err != nil {
			return fmt.Errorf("open stream failed: %w", err)
		}
		if err = d.addEventsToAggregate(ctx, pEvents.Events, aggStream, cache); err != nil {
			return err
		}
	}
	return nil
}

func (d DomainService) addEventsToAggregate(ctx context.Context, events []event.PersistenceEvent, stream *aggregate.Stream, cache *StreamCollection) (err error) {
	for _, evt := range events {
		var projStream []*projection.Stream
		projStream = cache.GetProjectionsForEventType(ctx, evt.Type)
		if err = d.addEventToAggregate(ctx, evt, addArgs{
			AggStream:  stream,
			ProjStream: projStream,
		}); err != nil {
			return err
		}
	}
	return err
}

func (d DomainService) addEventToAggregate(_ context.Context, evt event.PersistenceEvent, streams addArgs) error {
	versionedEvent, err := streams.AggStream.AddEvent(evt, d.Clock.Now()) //d.Now is a consistent clock
	if err != nil {
		return fmt.Errorf("add event to aggregate %q  failed :%w", streams.AggStream.ID(), err)
	}

	for _, stream := range streams.ProjStream {
		err = stream.AddEvents(versionedEvent)
		if err != nil {
			return fmt.Errorf("add event to projection %q failed :%w", stream.ID(), err)
		}
	}

	return err
}
