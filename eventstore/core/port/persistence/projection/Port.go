package projection

import (
	"context"
	"fmt"
	"github.com/global-soft-ba/go-eventstore"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/shared"
	"time"
)

type NotFoundError struct {
	ID shared.ProjectionID
}

func (c *NotFoundError) Error() string {
	return fmt.Sprintf("projection %q not found for tenant %q", c.ID.ProjectionID, c.ID.TenantID)
}

type LoadOptions struct {
	ChunkSize int
}

type DTO struct {
	TenantID     string
	ProjectionID string
	State        string
	UpdatedAt    time.Time
	Events       []event.PersistenceEvent
}

type Port interface {
	Lock(ctx context.Context, ids ...shared.ProjectionID) error
	UnLock(ctx context.Context, ids ...shared.ProjectionID) error

	Get(ctx context.Context, ids ...shared.ProjectionID) ([]DTO, []NotFoundError, error)
	GetAllForTenant(ctx context.Context, tenantID string) ([]DTO, error)
	GetAllForAllTenants(ctx context.Context) ([]DTO, error)

	GetSinceLastRun(ctx context.Context, id shared.ProjectionID, args LoadOptions) (DTO, error)

	SaveStates(ctx context.Context, projections ...DTO) error
	SaveEvents(ctx context.Context, projections ...DTO) error

	RemoveProjection(ctx context.Context, projectionID string) error

	ResetSince(txCtx context.Context, id shared.ProjectionID, sinceTime time.Time, eventTypes ...string) error

	DeleteEventFromQueue(txCtx context.Context, eventID string, id ...shared.ProjectionID) error
	GetProjectionsWithEventInQueue(txCtx context.Context, id shared.AggregateID, eventID string) ([]shared.ProjectionID, error)
}
