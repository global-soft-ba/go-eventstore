package aggregate

import (
	"context"
	"fmt"
	"github.com/global-soft-ba/go-eventstore"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/shared"
	"time"
)

const MaxPageSizeAggregatesEvents = 1000

type NotFoundError struct {
	ID shared.AggregateID
}

func (c *NotFoundError) Error() string {
	return fmt.Sprintf("aggregate %q of type %q not found for tenant %q", c.ID.AggregateID, c.ID.AggregateType, c.ID.TenantID)
}

type DTO struct {
	TenantID            string
	AggregateType       string
	AggregateID         string
	CurrentVersion      int64
	LastTransactionTime time.Time
	LatestValidTime     time.Time
	CreateTime          time.Time
	CloseTime           time.Time
}

type PatchDTO struct {
	TenantID      string
	AggregateType string
	AggregateID   string
	PatchTime     time.Time
}

type Port interface {
	LoadAsAt(ctx context.Context, projectionTime time.Time, key shared.AggregateID) (event.PersistenceEvents, error)
	LoadAsOf(ctx context.Context, projectionTime time.Time, key shared.AggregateID) (event.PersistenceEvents, error)
	LoadAsOfTill(ctx context.Context, projectionTime, reportTime time.Time, key shared.AggregateID) (event.PersistenceEvents, error)
	LoadAllOfAggregateAsAt(ctx context.Context, tenantID, aggregateType string, projectionTime time.Time) (eventStreams []event.PersistenceEvents, err error)
	LoadAllOfAggregateAsOf(ctx context.Context, tenantID, aggregateType string, projectionTime time.Time) (eventStreams []event.PersistenceEvents, err error)
	LoadAllOfAggregateAsOfTill(ctx context.Context, tenantID, aggregateType string, projectionTime, reportTime time.Time) (eventStreams []event.PersistenceEvents, err error)
	LoadAllAsAt(ctx context.Context, tenantID string, projectionTime time.Time) (eventStreams []event.PersistenceEvents, err error)
	LoadAllAsOf(ctx context.Context, tenantID string, projectionTime time.Time) (eventStreams []event.PersistenceEvents, err error)
	LoadAllAsOfTill(ctx context.Context, tenantID string, projectionTime time.Time, reportTime time.Time) (eventStreams []event.PersistenceEvents, err error)

	Lock(ctx context.Context, ids ...shared.AggregateID) error
	UnLock(ctx context.Context, ids ...shared.AggregateID) error

	Get(ctx context.Context, ids ...shared.AggregateID) ([]DTO, []NotFoundError, error)
	Save(ctx context.Context, states []DTO, events []event.PersistenceEvent, snapShots []event.PersistenceEvent) error
	HardDeleteEvent(ctx context.Context, id shared.AggregateID, evt event.PersistenceEvent) error
	SoftDeleteEvent(ctx context.Context, id shared.AggregateID, evt event.PersistenceEvent) error

	GetAggregateState(ctx context.Context, tenantID, aggregateType, aggregateID string) (event.AggregateState, error)
	GetAggregateStatesForAggregateType(ctx context.Context, tenantID string, aggregateType string) ([]event.AggregateState, error)
	GetAggregateStatesForAggregateTypeTill(ctx context.Context, tenantID string, aggregateType string, until time.Time) ([]event.AggregateState, error)

	DeleteSnapShot(ctx context.Context, id shared.AggregateID, sinceTime time.Time) error
	DeleteAllInvalidSnapsShots(ctx context.Context, patchEvents []PatchDTO) error
	UndoCloseStream(ctx context.Context, id shared.AggregateID) error

	GetPatchFreePeriodsForInterval(ctx context.Context, tenantID, aggregateType, aggregateID string, start time.Time, end time.Time) ([]event.TimeInterval, error)
	GetAggregatesEvents(ctx context.Context, tenantID string, page event.PageDTO) (events []event.PersistenceEvent, pages event.PagesDTO, err error)
}
