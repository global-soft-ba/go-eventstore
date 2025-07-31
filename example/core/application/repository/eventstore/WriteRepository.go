package eventstore

import (
	"context"
	"example/core/domain/model/item"
	"github.com/global-soft-ba/go-eventstore"
	"github.com/global-soft-ba/go-eventstore/instrumentation/port/metrics"
)

func NewWriteRepository(ctx context.Context, eventStore event.EventStore) *WriteRepository {
	_, endSpan := metrics.StartSpan(ctx, "init-write-repository", nil)
	defer endSpan()

	return &WriteRepository{
		eventStore: eventStore,
	}
}

type WriteRepository struct {
	eventStore event.EventStore
}

func (r *WriteRepository) SaveItems(ctx context.Context, items ...item.Item) error {
	ctx, endSpan := metrics.StartSpan(ctx, "save-items", map[string]interface{}{"numberOfItems": len(items)})
	defer endSpan()

	var aggregates []event.AggregateWithEventSourcingSupport

	for _, i := range items {
		aggregates = append(aggregates, i)
	}
	_, err := event.SaveAggregates(ctx, r.eventStore, aggregates...)
	return err
}

func (r *WriteRepository) SaveItem(ctx context.Context, item item.Item) error {
	ctx, endSpan := metrics.StartSpan(ctx, "save-item", nil)
	defer endSpan()

	_, err := event.SaveAggregates(ctx, r.eventStore, item)
	return err
}
