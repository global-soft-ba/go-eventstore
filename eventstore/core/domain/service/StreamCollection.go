package service

import (
	"context"
	"fmt"
	"github.com/global-soft-ba/go-eventstore"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/application/local/eventBus"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/application/local/eventBus/domainEvents"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/domain/model/aggregate"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/domain/model/projection"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/port/scheduler"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/shared"
)

func NewStreamCollection(aggregates []aggregate.Stream, projections []projection.Stream) *StreamCollection {
	aggs := make(map[shared.AggregateID]*aggregate.Stream)
	for _, stream := range aggregates {
		insert := stream
		aggs[insert.ID()] = &insert
	}

	eventProj := make(map[string][]*projection.Stream)
	var consProjections []*projection.Stream
	var evtlConsProjections []*projection.Stream
	for _, stream := range projections {
		insert := stream
		for _, pE := range insert.EventTypes() {
			eventProj[pE] = append(eventProj[pE], &insert)
		}

		if insert.Options().ProjectionType == event.CCS || insert.Options().ProjectionType == event.CSS {
			consProjections = append(consProjections, &insert)
		} else {
			evtlConsProjections = append(evtlConsProjections, &insert)
		}

	}

	return &StreamCollection{
		aggregates:                aggs,
		evenTypeToProjections:     eventProj,
		consistentProjections:     consProjections,
		evtlConsistentProjections: evtlConsProjections,
	}
}

type StreamCollection struct {
	aggregates            map[shared.AggregateID]*aggregate.Stream
	evenTypeToProjections map[string][]*projection.Stream

	consistentProjections     []*projection.Stream
	evtlConsistentProjections []*projection.Stream
}

func (s *StreamCollection) Aggregates() []aggregate.Stream {
	var result []aggregate.Stream
	for _, stream := range s.aggregates {
		result = append(result, *stream)
	}

	return result
}

func (s *StreamCollection) EvtlConsistentProjections() []projection.Stream {
	var result []projection.Stream
	for _, stream := range s.evtlConsistentProjections {
		result = append(result, *stream)
	}
	return result
}

func (s *StreamCollection) ConsistentProjections() []projection.Stream {
	var result []projection.Stream
	for _, stream := range s.consistentProjections {
		result = append(result, *stream)
	}
	return result
}

func (s *StreamCollection) EventsDuringSaving() []eventBus.Event {
	var result []eventBus.Event
	for _, stream := range append(s.consistentProjections, s.evtlConsistentProjections...) {
		if stream.Options().ProjectionType == event.ESS || stream.Options().ProjectionType == event.ECS {
			result = append(result, domainEvents.EventProjectionSaved(stream.ID(), stream.EarliestHPatch()))
		}
		if len(stream.FuturePatches()) != 0 {
			for _, evt := range stream.FuturePatches() {
				result = append(result, domainEvents.EventFuturePatchSaved(scheduler.ScheduledProjectionTask{
					Id:      stream.ID(),
					Time:    evt.ValidTime,
					Version: evt.Version,
				}))
			}
		}
	}
	return result
}

func (s *StreamCollection) GetProjectionsForEventType(_ context.Context, eventType string) []*projection.Stream {
	return s.evenTypeToProjections[eventType]
}

func (s *StreamCollection) GetAggregate(_ context.Context, id shared.AggregateID) (*aggregate.Stream, error) {
	stream, ok := s.aggregates[id]
	if !ok {
		return nil, fmt.Errorf("agreggate %q not found", id)
	}

	return stream, nil
}
