package event

import (
	"context"
	"time"
)

// The eventStore supports inserting or appending of snapshot to an aggregate. A snapshot represents
// a summarized version of the aggregate. When loading the aggregate (depending on the temporal semantic, e.g. as-at) it will
// try to find the most recent snapshot at projection/transaction time and load all subsequent events from it.
// This increases performance when loading events with large event streams. However, in the domain
// make sure that the data stored in the snapshot is correct and complete. Snapshots are only allowed in patch-free
// periods (patch = event with different valid and transaction time; patch-period = time interval between valid and
// transaction time or vice versa). If an attempt is made to save a snapshot in a patch period this will result in
// an error.

type TimeInterval struct {
	Start time.Time
	End   time.Time
}

type SnapshotManagement interface {
	GetPatchFreePeriodsForInterval(ctx context.Context, tenantID, aggregateType, aggregateID string, start time.Time, end time.Time) ([]TimeInterval, error)
	DeleteSnapShots(ctx context.Context, tenantID, aggregateType, aggregateID string, sinceTime time.Time) error
}
