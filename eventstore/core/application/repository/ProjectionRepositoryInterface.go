package repository

import (
	"context"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/domain/model/projection"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/shared"
	"time"
)

type ProjectionRepositoryInterface interface {
	GetProjectionIDsForEventTypes(tenantID string, eventTyps ...string) (eventualConsistent []shared.ProjectionID, consistent []shared.ProjectionID, err error)

	Lock(txCtx context.Context, ids ...shared.ProjectionID) error
	UnLock(txCtx context.Context, ids ...shared.ProjectionID) error

	Create(txCtx context.Context, id ...shared.ProjectionID) (streams []projection.Stream, err error)

	Get(txCtx context.Context, id shared.ProjectionID) (projection.Stream, error)
	GetProjections(txCtx context.Context, ids ...shared.ProjectionID) ([]projection.Stream, error)

	GetNotInitialized(txCtx context.Context, ids ...shared.ProjectionID) ([]shared.ProjectionID, error)

	GetAllForTenant(txCtx context.Context, tenantID string) ([]projection.Stream, error)
	GetAllForAllTenants(txCtx context.Context) ([]projection.Stream, error)

	GetWithNewEventsSinceLastRun(txCtx context.Context, id shared.ProjectionID) (projection.Stream, error)
	Reset(txCtx context.Context, id shared.ProjectionID, sinceTime time.Time, eventTypes ...string) error

	RemoveProjection(ctx context.Context, projectionID string) error

	SaveStates(txCtx context.Context, stream ...projection.Stream) error
	SaveEvents(txCtx context.Context, stream ...projection.Stream) error

	DeleteEventFromQueue(txCtx context.Context, eventID string, id ...shared.ProjectionID) error
	GetProjectionsWithEventInQueue(txCtx context.Context, id shared.AggregateID, eventID string) ([]shared.ProjectionID, error)
}
