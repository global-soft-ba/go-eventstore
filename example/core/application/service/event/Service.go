package event

import (
	"context"
	"example/core/application/service/event/mapper"
	service "example/core/port/service/event"
	"fmt"
	"github.com/global-soft-ba/go-eventstore"
	"github.com/global-soft-ba/go-eventstore/instrumentation/port/logger"
	"github.com/global-soft-ba/go-eventstore/instrumentation/port/metrics"
	"time"
)

type Service struct {
	eventStore event.EventStore
}

func NewEventService(eventStore event.EventStore) *Service {
	return &Service{
		eventStore: eventStore,
	}
}

func (h *Service) GetAggregateEvents(ctx context.Context, tenantID string, pageDTO event.PageDTO) ([]service.DTO, event.PagesDTO, error) {
	ctx, endSpan := metrics.StartSpan(ctx, "get-aggregate-events", map[string]interface{}{"tenantID": tenantID})
	defer endSpan()

	events, pages, err := h.eventStore.GetAggregatesEvents(ctx, tenantID, pageDTO)
	if err != nil {
		return nil, event.PagesDTO{}, fmt.Errorf("could not get aggregate events: %w", err)
	}

	logger.Info("Retrieved %d events for tenant %q", len(events), tenantID)

	return mapper.FromEventStoreToServiceDTOs(events), pages, nil
}

func (h *Service) DeleteScheduledEvent(ctx context.Context, tenantID, aggregateID, aggregateType, eventID string) error {
	ctx, endSpan := metrics.StartSpan(ctx, "remove-future-patch", map[string]interface{}{"tenantID": tenantID})
	defer endSpan()

	events, _, err := h.eventStore.GetAggregatesEvents(ctx, tenantID, event.PageDTO{
		PageSize: 10,
		SortFields: []event.SortField{
			{
				Name:   event.SortValidTime,
				IsDesc: false,
			},
		},
		SearchFields: []event.SearchField{{
			Name:     event.SearchAggregateEventID,
			Value:    eventID,
			Operator: "=",
		}},
		Values:     nil,
		IsBackward: false,
	})
	if err != nil {
		return fmt.Errorf("could not delete event with aggregateType %q, aggregateID %q, eventID %q: %w", aggregateType, aggregateID, eventID, err)
	}
	if len(events) < 1 {
		return fmt.Errorf("could not delete event with aggregateType %q, aggregateID %q, eventID %q: empty array of events", aggregateType, aggregateID, eventID)
	}

	eventTime := events[0].ValidTime
	timeNow := time.Now()

	if !eventTime.After(timeNow) {
		return fmt.Errorf("could not delete event with aggregateType %q, aggregateID %q, eventID %q: event is not future patch", aggregateType, aggregateID, eventID)
	}

	err = h.eventStore.DeleteEvent(ctx, tenantID, aggregateType, aggregateID, eventID)
	if err != nil {
		return fmt.Errorf("could not delete event with aggregateType %q, aggregateID %q, eventID %q: %w", aggregateType, aggregateID, eventID, err)
	}

	logger.Info("Deleted event with tenantID %q, aggregateType %q, aggregateID %q, eventID %q successfully", tenantID, aggregateType, aggregateID, eventID)
	return nil
}
