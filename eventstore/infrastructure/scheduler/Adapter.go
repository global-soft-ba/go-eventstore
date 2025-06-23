package scheduler

import (
	"context"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/port/scheduler"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/shared"
)

func NewAdapter() scheduler.Port {
	return &Adapter{}
}

// Adapter TODO - implement adapter for scheduler
type Adapter struct {
}

func (a Adapter) GetOpenTasks(ctx context.Context, id shared.ProjectionID) ([]scheduler.ScheduledProjectionTask, error) {
	return nil, nil
}

func (a Adapter) AddTasks(ctx context.Context, tasks ...scheduler.ScheduledProjectionTask) error {
	return nil
}

func (a Adapter) DelTasks(ctx context.Context, tasks ...scheduler.ScheduledProjectionTask) error {
	return nil
}
