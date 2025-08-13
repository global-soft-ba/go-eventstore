package event

import (
	"context"
	"time"
)

type ProjectionType string

const (
	// ESS = Eventual-Consistent Single Stream: events are passed to the projections in a background job. The projection handles only events from one aggregate types.
	ESS ProjectionType = "eventual-consistence single stream"
	// ECS = Eventual-Consistent Cross Stream: events are passed to the projections in a background job. The projection handles events from multiple aggregate types. This is the default ProjectionType for projections.
	ECS ProjectionType = "eventual-consistence cross stream"
	// CSS = Consistent Single Stream: events are passed to projections in the same transaction as the save. The projection handles only events from one aggregate types.
	CSS ProjectionType = "consistence single stream"
	// CCS = Consistent Cross Stream: events are passed to projections in the same transaction as the save. The projection handles events from multiple aggregate types.
	CCS ProjectionType = "consistence cross stream"
)

type ProjectionState struct {
	TenantID                string
	ProjectionID            string
	State                   string
	UpdatedAt               time.Time
	HPatchStrategy          ProjectionPatchStrategy
	ExecutionTimeOut        time.Duration
	PreparationTimeOut      time.Duration
	FinishingTimeOut        time.Duration
	RebuildExecutionTimeOut time.Duration
	InputQueueLength        int
	ProjectionType          ProjectionType
	RetryDurations          []time.Duration
}

// A projection is a set of events for which a separate storage or execution model is used in the domain.
// The eventStore independently monitors which of the events have already been passed to the domain. An accidental
// double sending of events is impossible.

type Projection interface {
	ID() string
	EventTypes() []string
	ChunkSize() int
	Execute(ctxWithTimeOut context.Context, events []IEvent) error
	PrepareRebuild(ctx context.Context, tenantID string) error
	FinishRebuild(ctx context.Context, tenantID string) error
}

type ProjectionSince interface {
	PrepareRebuildSince(ctx context.Context, tenantID string, since time.Time) error
}

type ProjectionManagement interface {
	// StartProjection  start/re-start a projection (initially all projection are started with the eventStore)
	StartProjection(ctx context.Context, tenantID, projectionID string) (chan error, error)

	// StopProjection stops or pause a projection
	StopProjection(ctx context.Context, tenantID, projectionID string) error

	// ExecuteAllProjections executes all projections of all tenants
	ExecuteAllProjections(ctx context.Context, projectionsID ...string) error

	// RebuildAllProjection RebuildAllProjectionSince RebuildProjection RebuildProjectionSince A projection can be
	// re-triggered by these functions in case of errors on the domain side (projection domain). The functions always try
	// to use existing snapshots to minimize amount of events. If snapshots are supported/used than the projection implementation must
	// deal with snapshot event too; The snapshot event must be part of the projection.GetEventType (or it throws error).
	//
	// In case of eventual consistent projection, the projection will not be updated during the rebuild. However, it is
	// still possible to save events.
	//
	// In case of consistent projection, the projection is not updated during the rebuild. Saving events is also NOT
	// possible during rebuilds and will fail with error. For the projection to remain consistent, the rebuild must be
	// completed before new events are added.
	//
	RebuildAllProjection(ctx context.Context, tenantID string) chan error

	// RebuildAllProjectionSince sinceTime means domain time (valid time) not transaction time
	RebuildAllProjectionSince(ctx context.Context, tenantID string, sinceTime time.Time) chan error
	// RebuildProjection starts from the first event ever or latest snapshot (if available)
	RebuildProjection(ctx context.Context, tenantID, projectionID string) chan error
	// RebuildProjectionSince sinceTime means domain time (valid time) not transaction time
	RebuildProjectionSince(ctx context.Context, tenantID, projectionID string, sinceTime time.Time) chan error

	RemoveProjection(ctx context.Context, projectionID string) error

	GetProjectionStates(ctx context.Context, tenantID string, projectionID ...string) ([]ProjectionState, error)
	GetAllProjectionStates(ctx context.Context, tenantID string) ([]ProjectionState, error)
}
