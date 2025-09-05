package eventstore

import (
	"context"
	"github.com/global-soft-ba/go-eventstore"
	"github.com/global-soft-ba/go-eventstore/instrumentation/port/metrics"
	"time"
)

func (e eventStore) DeleteSnapShots(ctx context.Context, tenantID, aggregateType, aggregateID string, sinceTime time.Time) error {
	ctx, endSpan := metrics.StartSpan(ctx, "DisableSnapShot (store)", map[string]interface{}{"tenantID": tenantID, "aggregateType": aggregateType, "aggregateID": aggregateID, "sinceTime": sinceTime})
	defer endSpan()

	return e.saver.DisableSnapShot(ctx, tenantID, aggregateType, aggregateID, sinceTime)
}

func (e eventStore) GetPatchFreePeriodsForInterval(ctx context.Context, tenantID, aggregateType, aggregateID string, start time.Time, end time.Time) ([]event.TimeInterval, error) {
	ctx, endSpan := metrics.StartSpan(ctx, "GetPatchFreePeriodsForInterval (store)", map[string]interface{}{"tenantID": tenantID, "aggregateType": aggregateType, "aggregateID": aggregateID, "start": start, "end": end})
	defer endSpan()

	return e.loader.GetPatchFreePeriodsForInterval(ctx, tenantID, aggregateType, aggregateID, start, end)
}
