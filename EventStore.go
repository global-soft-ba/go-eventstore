package event

import (
	"context"
	"encoding/json"
	"time"
)

type ConcurrentModificationStrategy string

const (
	// Fail strategy: The save fails on concurrent modification. This is the default ConcurrentModificationStrategy
	Fail ConcurrentModificationStrategy = "no incorrect version allowed"
	// Ignore strategy: The save ignores concurrent modification and increase version number until the event stream is consistent and can be saved.
	Ignore ConcurrentModificationStrategy = "use the stored version and count ahead"
)

type ProjectionPatchStrategy string

const (
	// Rebuild strategy: The projection is completely rebuilt if a patch is passed to the projection
	Rebuild ProjectionPatchStrategy = "rebuild"
	// RebuildSince strategy: The projection is rebuilt since the patches (valid) time if a patch is passed to the projection
	RebuildSince ProjectionPatchStrategy = "rebuild since"
	// Projected strategy: The patch is sent to the projection directly after saving it to the event store. Use this for temporal-depending projections that deal with temporal relations themselves.
	Projected ProjectionPatchStrategy = "projected"
	// Manual strategy: The patch is not sent to the projection (but stays in the queue). The rebuild/project execution must be triggered manual (if wanted). This is the default ProjectionPatchStrategy for delete patches
	Manual ProjectionPatchStrategy = "manual"
	// Error strategy: An error is thrown if a patch is passed to the projection. This is the default ProjectionPatchStrategy for historical patches
	Error ProjectionPatchStrategy = "error"
)

type DeleteStrategy string

const (
	// NoDelete strategy: A delete event is not allowed and will throw an error. This is the default DeleteStrategy
	NoDelete DeleteStrategy = "not allowed"

	// HardDelete strategy: An event is deleted from the event store and creates holes in version numbers of the event stream
	HardDelete DeleteStrategy = "hard"

	// SoftDelete strategy: The event is marked as deleted and will not be loaded or sent to projections anymore
	// (but can be retrieved by using GetAggregatesEvents)
	SoftDelete DeleteStrategy = "soft"

	// not yet implemented
	// RevisionDelete a dedicated delete event is used with a new version.
	// The delete event will not be loaded but send to projection
	// RevisionDelete DeleteStrategy = "revision"
)

type PersistenceEvents struct {
	Events  []PersistenceEvent
	Version int
}

type PersistenceEvent struct {
	ID              string          `json:"id"`
	AggregateID     string          `json:"aggregateID"`
	TenantID        string          `json:"tenantID"`
	AggregateType   string          `json:"aggregateType"`
	Version         int             `json:"version"`
	Type            string          `json:"type"`
	Class           Class           `json:"class"`
	TransactionTime time.Time       `json:"transactionTime"`
	ValidTime       time.Time       `json:"validTime"`
	FromMigration   bool            `json:"FromMigration"`
	Data            json.RawMessage `json:"data"`
}

type EventStore interface {
	AggregateManagement
	SnapshotManagement
	ProjectionManagement

	Save(ctx context.Context, tenantID string, events []PersistenceEvent, version int) (chan error, error)
	SaveAll(ctx context.Context, tenantID string, events []PersistenceEvents) (chan error, error)

	// LoadAsAt is a projection that answers the question: "How was the event stream as we knew at projection time?"
	// This means in practise that "patch events" that were applied later with a valid time before the projection time are ignored.
	LoadAsAt(ctx context.Context, tenantID, aggregateType, aggregateID string, projectionTime time.Time) (eventStream []PersistenceEvent, version int, err error)
	// LoadAsOf is a projection that answers the question: "How was the event stream as we should have known at projection time?"
	// This means in practise that "patch events" that came in later are still considered, when their valid time was before the projection time.
	LoadAsOf(ctx context.Context, tenantID, aggregateType, aggregateID string, projectionTime time.Time) (eventStream []PersistenceEvent, version int, err error)
	// LoadAsOfTill is a projection that takes a viewpoint in time with the following question in mind:
	// "How would the event stream have looked like at projection time, when we asked this at report time?"
	// This means in practise that "patch events" are considered if their valid time is before the projection time, but only if their transaction time is before the report time.
	// Confused? Take a look here: https://www.youtube.com/watch?v=xzekp1RuZbM and https://www.youtube.com/watch?v=GZA1fNVGEV0
	LoadAsOfTill(ctx context.Context, tenantID, aggregateType, aggregateID string, projectionTime, reportTime time.Time) (eventStream []PersistenceEvent, version int, err error)

	LoadAllOfAggregateTypeAsAt(ctx context.Context, tenantID, aggregateType string, projectionTime time.Time) (eventStreams []PersistenceEvents, err error)
	LoadAllOfAggregateTypeAsOf(ctx context.Context, tenantID, aggregateType string, projectionTime time.Time) (eventStreams []PersistenceEvents, err error)
	LoadAllOfAggregateTypeAsOfTill(ctx context.Context, tenantID, aggregateType string, projectionTime, reportTime time.Time) (eventStreams []PersistenceEvents, err error)

	LoadAllAsAt(ctx context.Context, tenantID string, projectionTime time.Time) (eventStreams []PersistenceEvents, err error)
	LoadAllAsOf(ctx context.Context, tenantID string, projectionTime time.Time) (eventStreams []PersistenceEvents, err error)
	LoadAllAsOfTill(ctx context.Context, tenantID string, projectionTime time.Time, reportTime time.Time) (eventStreams []PersistenceEvents, err error)

	DeleteEvent(ctx context.Context, tenantID, aggregateType, aggregateID, eventID string) error
}
