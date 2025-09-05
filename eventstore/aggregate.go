package eventstore

import (
	"context"
	"github.com/global-soft-ba/go-eventstore"
	"github.com/global-soft-ba/go-eventstore/instrumentation/port/metrics"
	"time"
)

func (e eventStore) GetAggregatesEvents(ctx context.Context, tenantID string, page event.PageDTO) ([]event.PersistenceEvent, event.PagesDTO, error) {
	ctx, endSpan := metrics.StartSpan(ctx, "GetAggregatesEvents", map[string]interface{}{"tenantID": tenantID})
	defer endSpan()

	return e.loader.GetAggregatesEvents(ctx, tenantID, page)
}

func (e eventStore) GetAggregateState(ctx context.Context, tenantID, aggregateType, aggregateID string) (event.AggregateState, error) {
	ctx, endSpan := metrics.StartSpan(ctx, "GetAggregateState (store)", map[string]interface{}{"tenantID": tenantID, "aggregateType": aggregateType, "aggregateID": aggregateID})
	defer endSpan()

	return e.loader.GetAggregateStates(ctx, tenantID, aggregateType, aggregateID)
}

func (e eventStore) GetAggregateStatesForAggregateType(ctx context.Context, tenantID string, aggregateType string) ([]event.AggregateState, error) {
	ctx, endSpan := metrics.StartSpan(ctx, "GetAggregateStatesForAggregateType (store)", map[string]interface{}{"tenantID": tenantID, "aggregateType": aggregateType})
	defer endSpan()

	return e.loader.GetAllAggregateStatesForAggregateType(ctx, tenantID, aggregateType)
}

func (e eventStore) GetAggregateStatesForAggregateTypeTill(ctx context.Context, tenantID string, aggregateType string, until time.Time) ([]event.AggregateState, error) {
	ctx, endSpan := metrics.StartSpan(ctx, "GetAggregateStatesForAggregateType (store)", map[string]interface{}{"tenantID": tenantID, "aggregateType": aggregateType})
	defer endSpan()

	return e.loader.GetAllAggregateStatesForAggregateTypeTill(ctx, tenantID, aggregateType, until)
}
