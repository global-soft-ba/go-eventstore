package eventstore

import (
	"context"
	"fmt"
	"github.com/global-soft-ba/go-eventstore"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/shared"
	"github.com/global-soft-ba/go-eventstore/instrumentation/port/metrics"
	"time"
)

func (e eventStore) StartProjection(ctx context.Context, tenantID, projectionID string) (chan error, error) {
	ctx, endSpan := metrics.StartSpan(ctx, "StartProjection (store)", map[string]interface{}{"tenantID": tenantID, "projectionID": projectionID})
	defer endSpan()

	ch, err := e.projecter.Start(ctx, shared.ProjectionID{
		TenantID:     tenantID,
		ProjectionID: projectionID,
	})
	if err != nil {
		return ch, fmt.Errorf("StartProjection failed: %w", err)
	}
	return ch, err
}

func (e eventStore) ExecuteAllProjections(ctx context.Context, projectionID ...string) error {
	ctx, endSpan := metrics.StartSpan(ctx, "ExecuteAllProjections (store)", nil)
	defer endSpan()

	return e.projecter.ExecuteAllProjections(ctx, projectionID...)
}

func (e eventStore) StopProjection(ctx context.Context, tenantID, projectionID string) error {
	ctx, endSpan := metrics.StartSpan(ctx, "StopProjection (store)", map[string]interface{}{"tenantID": tenantID, "projectionID": projectionID})
	defer endSpan()

	err := e.projecter.Stop(ctx, shared.ProjectionID{
		TenantID:     tenantID,
		ProjectionID: projectionID,
	})
	if err != nil {
		return fmt.Errorf("StopProjection failed: %w", err)
	}
	return err
}

func (e eventStore) RebuildAllProjection(ctx context.Context, tenantID string) chan error {
	ctx, endSpan := metrics.StartSpan(ctx, "RebuildAllProjection (store)", map[string]interface{}{"tenantID": tenantID})
	defer endSpan()
	errCh := make(chan error, len(e.registries.ProjectionRegistry.All())+1)

	go func(resultCh chan error) {
		for _, projId := range e.registries.ProjectionRegistry.All() {
			ch := e.projecter.Rebuild(ctx, shared.ProjectionID{TenantID: tenantID, ProjectionID: projId.ID()}, time.Time{})
			select {
			case err := <-ch:
				if err != nil {
					resultCh <- err
				} // executed in time
			case <-ctx.Done(): // timeout
				err := fmt.Errorf("rebuild projection %q failed: execution deadline exceeded", shared.ProjectionID{TenantID: tenantID, ProjectionID: projId.ID()})
				resultCh <- err
			}
		}
		close(resultCh)
	}(errCh)
	return errCh
}

func (e eventStore) RebuildAllProjectionSince(ctx context.Context, tenantID string, sinceTime time.Time) chan error {
	ctx, endSpan := metrics.StartSpan(ctx, "RebuildAllProjectionSince (store)", map[string]interface{}{"tenantID": tenantID, "sinceTime": sinceTime})
	defer endSpan()
	errCh := make(chan error, len(e.registries.ProjectionRegistry.All())+1)

	go func(resultCh chan error) {
		for _, projId := range e.registries.ProjectionRegistry.All() {
			ch := e.projecter.Rebuild(ctx, shared.ProjectionID{TenantID: tenantID, ProjectionID: projId.ID()}, sinceTime)
			select {
			case err := <-ch:
				if err != nil {
					resultCh <- err
				} // executed in time
			case <-ctx.Done(): // timeout
				err := fmt.Errorf("rebuild projection since %q failed: execution deadline exceeded", shared.ProjectionID{TenantID: tenantID, ProjectionID: projId.ID()})
				resultCh <- err
			}
		}
		close(errCh)
	}(errCh)

	return errCh
}

func (e eventStore) RebuildProjection(ctx context.Context, tenantID, projectionID string) chan error {
	ctx, endSpan := metrics.StartSpan(ctx, "RebuildProjection (store)", map[string]interface{}{"tenantID": tenantID, "projectionID": projectionID})
	defer endSpan()

	return e.projecter.Rebuild(ctx, shared.ProjectionID{TenantID: tenantID, ProjectionID: projectionID}, time.Time{})
}

func (e eventStore) RebuildProjectionSince(ctx context.Context, tenantID, projectionID string, sinceTime time.Time) chan error {
	ctx, endSpan := metrics.StartSpan(ctx, "RebuildProjectionSince (store)", map[string]interface{}{"tenantID": tenantID, "projectionID": projectionID, "sinceTime": sinceTime})
	defer endSpan()

	return e.projecter.Rebuild(ctx, shared.ProjectionID{TenantID: tenantID, ProjectionID: projectionID}, sinceTime)
}

func (e eventStore) GetProjectionStates(ctx context.Context, tenantID string, projectionID ...string) ([]event.ProjectionState, error) {
	ctx, endSpan := metrics.StartSpan(ctx, "GetProjectionState", map[string]interface{}{"tenantID": tenantID, "projectionID": projectionID})
	defer endSpan()

	return e.projecter.GetProjectionStates(ctx, tenantID, projectionID...)
}

func (e eventStore) GetAllProjectionStates(ctx context.Context, tenantID string) ([]event.ProjectionState, error) {
	ctx, endSpan := metrics.StartSpan(ctx, "GetProjectionStates", map[string]interface{}{"tenantID": tenantID})
	defer endSpan()

	return e.projecter.GetAllProjectionStates(ctx, tenantID)
}

func (e eventStore) RemoveProjection(ctx context.Context, projectionID string) error {
	ctx, endSpan := metrics.StartSpan(ctx, "RemoveProjection", map[string]interface{}{"projectionID": projectionID})
	defer endSpan()

	return e.projecter.RemoveProjection(ctx, projectionID)
}
