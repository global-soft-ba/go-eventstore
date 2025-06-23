package event

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"reflect"
	"time"
)

const (
	CtxKeyUserID = "userIDKey"
)

type AggregateWithEventSourcingSupport interface {
	GetID() string
	GetTenantID() string
	GetVersion() int
	GetUnsavedChanges() []IEvent
}

func GetUserID(ctx context.Context) string {
	valAny := ctx.Value(CtxKeyUserID)
	if valAny == nil {
		return ""
	}
	return valAny.(string)
}

type EventStream struct {
	Stream  []IEvent
	Version int
}

func SaveAggregate(ctx context.Context, eventStore EventStore, aggregate AggregateWithEventSourcingSupport) (chan error, error) {
	if aggregate == nil {
		errCh := make(chan error, 1)
		close(errCh)
		return errCh, nil
	}

	return SaveAggregates(ctx, eventStore, aggregate)
}

func SaveAggregates(ctx context.Context, eventStore EventStore, aggregates ...AggregateWithEventSourcingSupport) (chan error, error) {
	var allEvents []PersistenceEvents
	var tenantID string

	if len(aggregates) == 0 {
		errCh := make(chan error, 1)
		close(errCh)
		return errCh, nil
	}

	for _, aggregate := range aggregates {
		// we do not allow storage of aggregates with different tenants in one single save call
		if tenantID == "" {
			tenantID = aggregate.GetTenantID()
		} else {
			if tenantID != aggregate.GetTenantID() {
				return nil, fmt.Errorf("aggregates with different tenants in one single save call:  %q vs. %q ", tenantID, aggregate.GetTenantID())
			}
		}

		aggregateType := reflect.TypeOf(aggregate)
		if aggregateType.Kind() == reflect.Ptr {
			aggregateType = aggregateType.Elem()
		}

		if len(aggregate.GetUnsavedChanges()) > 0 {
			events := make([]PersistenceEvent, len(aggregate.GetUnsavedChanges()))
			for i, evt := range aggregate.GetUnsavedChanges() {
				eventID, _ := uuid.NewUUID()
				eType := EventType(evt)
				evt.setUserID(GetUserID(ctx))
				data, err := SerializeEvent(evt)
				if err != nil {
					return nil, fmt.Errorf("could not serialize action %q: %w", EventType(evt), err)
				}
				events[i] = PersistenceEvent{
					ID:              eventID.String(),
					AggregateID:     evt.GetAggregateID(),
					TenantID:        evt.GetTenantID(),
					AggregateType:   aggregateType.Name(),
					Type:            eType,
					ValidTime:       evt.GetValidTime(),
					TransactionTime: evt.GetTransactionTime(),
					Data:            data,
					Class:           evt.GetClass(),
					FromMigration:   evt.GetMigration(),
				}
			}

			allEvents = append(allEvents, PersistenceEvents{
				Events:  events,
				Version: aggregate.GetVersion(),
			})
		}
	}

	return eventStore.SaveAll(ctx, tenantID, allEvents)
}

func LoadAggregateAsAt(ctx context.Context, tenantID, aggregateType, aggregateID string, projectionTime time.Time, eventStore EventStore) (eventStream []IEvent, version int, err error) {
	var persistenceEvents []PersistenceEvent
	persistenceEvents, version, err = eventStore.LoadAsAt(ctx, tenantID, aggregateType, aggregateID, projectionTime)
	if err != nil || persistenceEvents == nil {
		return nil, 0, err
	}
	eventStream = make([]IEvent, len(persistenceEvents))
	for i, event := range persistenceEvents {
		eventStream[i], err = DeserializeEvent(event)
		if err != nil {
			return nil, 0, err
		}
	}
	return eventStream, version, err
}

func LoadAggregateAsOf(ctx context.Context, tenantID, aggregateType, aggregateID string, projectionTime time.Time, eventStore EventStore) (eventStream []IEvent, version int, err error) {
	var persistenceEvents []PersistenceEvent
	persistenceEvents, version, err = eventStore.LoadAsOf(ctx, tenantID, aggregateType, aggregateID, projectionTime)
	if err != nil {
		return nil, 0, err
	}
	eventStream = make([]IEvent, len(persistenceEvents))
	for i, event := range persistenceEvents {
		eventStream[i], err = DeserializeEvent(event)
		if err != nil {
			return nil, 0, err
		}
	}
	return eventStream, version, err
}

func LoadAggregateAsOfTill(ctx context.Context, tenantID, aggregateType, aggregateID string, projectionTime time.Time, reportTime time.Time, eventStore EventStore) (eventStream []IEvent, version int, err error) {
	var persistenceEvents []PersistenceEvent
	persistenceEvents, version, err = eventStore.LoadAsOfTill(ctx, tenantID, aggregateType, aggregateID, projectionTime, reportTime)
	if err != nil {
		return nil, 0, err
	}
	eventStream = make([]IEvent, len(persistenceEvents))
	for i, event := range persistenceEvents {
		eventStream[i], err = DeserializeEvent(event)
		if err != nil {
			return nil, 0, err
		}
	}
	return eventStream, version, err
}

func LoadAllOfAggregateTypeAsAt(ctx context.Context, tenantID, aggregateType string, projectionTime time.Time, eventStore EventStore) (eventStreams []EventStream, err error) {
	persistenceStreams, err := eventStore.LoadAllOfAggregateTypeAsAt(ctx, tenantID, aggregateType, projectionTime)
	if err != nil {
		return nil, err
	}
	eventStreams, err = deserializeEventStreams(persistenceStreams)
	if err != nil {
		return nil, err
	}
	return eventStreams, nil
}

func LoadAllOfAggregateTypeAsOf(ctx context.Context, tenantID, aggregateType string, projectionTime time.Time, eventStore EventStore) (eventStreams []EventStream, err error) {
	persistenceStreams, err := eventStore.LoadAllOfAggregateTypeAsOf(ctx, tenantID, aggregateType, projectionTime)
	if err != nil {
		return nil, err
	}
	eventStreams, err = deserializeEventStreams(persistenceStreams)
	if err != nil {
		return nil, err
	}
	return eventStreams, nil
}

func LoadAllOfAggregateTypeAsOfTill(ctx context.Context, tenantID, aggregateType string, projectionTime time.Time, reportTime time.Time, eventStore EventStore) (eventStreams []EventStream, err error) {
	persistenceStreams, err := eventStore.LoadAllOfAggregateTypeAsOfTill(ctx, tenantID, aggregateType, projectionTime, reportTime)
	if err != nil {
		return nil, err
	}
	eventStreams, err = deserializeEventStreams(persistenceStreams)
	if err != nil {
		return nil, err
	}
	return eventStreams, nil
}

func LoadAllAggregatesAsAt(ctx context.Context, tenantID string, projectionTime time.Time, eventStore EventStore) (eventStreams []EventStream, err error) {
	persistenceStreams, err := eventStore.LoadAllAsAt(ctx, tenantID, projectionTime)
	if err != nil {
		return nil, err
	}
	eventStreams, err = deserializeEventStreams(persistenceStreams)
	if err != nil {
		return nil, err
	}
	return eventStreams, nil
}

func LoadAllAggregatesAsOf(ctx context.Context, tenantID string, projectionTime time.Time, eventStore EventStore) (eventStreams []EventStream, err error) {
	persistenceStreams, err := eventStore.LoadAllAsOf(ctx, tenantID, projectionTime)
	if err != nil {
		return nil, err
	}
	eventStreams, err = deserializeEventStreams(persistenceStreams)
	if err != nil {
		return nil, err
	}
	return eventStreams, nil
}

func LoadAllAggregatesAsOfTill(ctx context.Context, tenantID string, projectionTime, reportTime time.Time, eventStore EventStore) (eventStreams []EventStream, err error) {
	persistenceStreams, err := eventStore.LoadAllAsOfTill(ctx, tenantID, projectionTime, reportTime)
	if err != nil {
		return nil, err
	}
	eventStreams, err = deserializeEventStreams(persistenceStreams)
	if err != nil {
		return nil, err
	}
	return eventStreams, nil
}

func deserializeEventStreams(persistenceStreams []PersistenceEvents) (eventStreams []EventStream, err error) {
	eventStreams = make([]EventStream, len(persistenceStreams))
	for p, persistenceStream := range persistenceStreams {
		eventStream := make([]IEvent, len(persistenceStream.Events))
		for i, event := range persistenceStream.Events {
			eventStream[i], err = DeserializeEvent(event)
			if err != nil {
				return nil, err
			}
		}
		eventStreams[p] = EventStream{
			Stream:  eventStream,
			Version: persistenceStream.Version,
		}
	}
	return eventStreams, nil
}
