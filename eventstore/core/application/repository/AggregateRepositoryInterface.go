package repository

import (
	"context"
	"github.com/global-soft-ba/go-eventstore"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/domain/model/aggregate"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/shared"
	"time"
)

type AggregateRepositoryInterface interface {
	LoaderInterface
	SaverInterface
}

type LoaderInterface interface {
	LoadAsAt(txCtx context.Context, id shared.AggregateID, projectionTime time.Time) (events event.PersistenceEvents, err error)
	LoadAsOf(txCtx context.Context, id shared.AggregateID, projectionTime time.Time) (events event.PersistenceEvents, err error)
	LoadAsOfTill(txCtx context.Context, id shared.AggregateID, projectionTime, reportTime time.Time) (events event.PersistenceEvents, err error)

	LoadAllOfAggregateAsAt(txCtx context.Context, tenantID, aggregateType string, projectionTime time.Time) (eventStreams []event.PersistenceEvents, err error)
	LoadAllOfAggregateAsOf(txCtx context.Context, tenantID, aggregateType string, projectionTime time.Time) (eventStreams []event.PersistenceEvents, err error)
	LoadAllOfAggregateAsOfTill(txCtx context.Context, tenantID, aggregateType string, projectionTime, reportTime time.Time) (eventStreams []event.PersistenceEvents, err error)

	LoadAllAsAt(txCtx context.Context, tenantID string, projectionTime time.Time) (eventStreams []event.PersistenceEvents, err error)
	LoadAllAsOf(txCtx context.Context, tenantID string, projectionTime time.Time) (eventStreams []event.PersistenceEvents, err error)
	LoadAllAsOfTill(txCtx context.Context, tenantID string, projectionTime time.Time, reportTime time.Time) (eventStreams []event.PersistenceEvents, err error)

	GetAggregateState(txCtx context.Context, tenantID, aggregateType, aggregateID string) (event.AggregateState, error)
	GetAggregateStatesForAggregateType(txCtx context.Context, tenantID string, aggregateType string) ([]event.AggregateState, error)
	GetAggregateStatesForAggregateTypeTill(txCtx context.Context, tenantID string, aggregateType string, until time.Time) ([]event.AggregateState, error)

	GetPatchFreePeriodsForInterval(txCtx context.Context, tenantID, aggregateType, aggregateID string, start time.Time, end time.Time) ([]event.TimeInterval, error)

	GetAggregatesEvents(txCtx context.Context, tenantID string, page event.PageDTO) (events []event.PersistenceEvent, pages event.PagesDTO, err error)
}

type SaverInterface interface {
	Lock(txCtx context.Context, ids ...shared.AggregateID) error
	UnLock(txCtx context.Context, ids ...shared.AggregateID) error

	GetAggregate(txCtx context.Context, id shared.AggregateID) (aggregate.Stream, error)
	Get(txCtx context.Context, id ...shared.AggregateID) ([]aggregate.Stream, error)
	GetOrCreate(ctx context.Context, ids ...shared.AggregateID) (streams []aggregate.Stream, err error)
	Save(txCtx context.Context, streams ...aggregate.Stream) error
	DisableSnapShots(ctx context.Context, id shared.AggregateID, sinceTime time.Time) error

	DeleteEvent(txCtx context.Context, id shared.AggregateID, evt event.PersistenceEvent) error
	UndoDeleteAggregate(txCtx context.Context, id shared.AggregateID) error
}
