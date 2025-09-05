package scheduler

import (
	"context"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/shared"
	"time"
)

type ScheduledProjectionTask struct {
	Id      shared.ProjectionID
	Time    time.Time
	Version int
}

type Port interface {
	GetOpenTasks(ctx context.Context, id shared.ProjectionID) ([]ScheduledProjectionTask, error)
	AddTasks(ctx context.Context, tasks ...ScheduledProjectionTask) error
	DelTasks(ctx context.Context, tasks ...ScheduledProjectionTask) error
}
