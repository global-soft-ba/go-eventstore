package eventstore

import (
	"context"
	"example/core/domain/model/item"
	"fmt"
	"github.com/global-soft-ba/go-eventstore"
	"github.com/global-soft-ba/go-eventstore/instrumentation/port/metrics"
	"reflect"
	"time"
)

func NewReadRepository(ctx context.Context, eventStore event.EventStore) *ReadRepository {
	_, endSpan := metrics.StartSpan(ctx, "init-read-repository", nil)
	defer endSpan()

	return &ReadRepository{
		eventStore: eventStore,
	}
}

type ReadRepository struct {
	eventStore event.EventStore
}

func (r *ReadRepository) GetAllItems(ctx context.Context, tenantID string) ([]item.Item, error) {
	ctx, endSpan := metrics.StartSpan(ctx, "get-items", map[string]interface{}{"tenantID": tenantID})
	defer endSpan()

	eventStreams, err := event.LoadAllOfAggregateTypeAsOf(ctx, tenantID, reflect.TypeOf(item.Item{}).Name(), time.Now(), r.eventStore)
	if err != nil {
		return nil, fmt.Errorf("could not load accounts from event store: %w", err)
	}

	multipleItems := make([]item.Item, len(eventStreams))
	for i, stream := range eventStreams {
		multipleItems[i], err = item.LoadFromEventStream(stream.Version, stream.Stream...)
		if err != nil {
			return nil, fmt.Errorf("could not load accounts from event store: %w", err)
		}
	}
	return multipleItems, nil
}

func (r *ReadRepository) GetItem(ctx context.Context, tenantID, itemID string, start time.Time) (item.Item, error) {
	ctx, endSpan := metrics.StartSpan(ctx, "get-item", map[string]interface{}{"tenantID": tenantID, "itemID": itemID})
	defer endSpan()
	if start.IsZero() {
		start = time.Now()
	}
	eventStream, version, err := event.LoadAggregateAsOf(ctx, tenantID, reflect.TypeOf(item.Item{}).Name(), itemID, start, r.eventStore)
	if err != nil {
		return item.Item{}, fmt.Errorf("could not load item %q from event store: %w", itemID, err)
	}
	return item.LoadFromEventStream(version, eventStream...)
}
