package repository

import (
	"context"
	"fmt"
	"github.com/global-soft-ba/go-eventstore"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/application/registry"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/domain/model/projection"
	projPort "github.com/global-soft-ba/go-eventstore/eventstore/core/port/persistence/projection"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/shared"
	"github.com/global-soft-ba/go-eventstore/instrumentation/port/metrics"
	"time"
)

func NewProjectionRepository(projPort projPort.Port, registries *registry.Registries) ProjectionRepository {
	return ProjectionRepository{
		projPort,
		registries,
	}
}

type ProjectionRepository struct {
	projPort projPort.Port

	registries *registry.Registries
}

func (p ProjectionRepository) GetProjectionIDsForEventTypes(tenantID string, eventTypes ...string) (eventualConsistent []shared.ProjectionID, consistent []shared.ProjectionID, err error) {
	projIDs := p.registries.ProjectionRegistry.ForEventTypes(eventTypes...)
	for _, id := range projIDs {
		t := p.registries.ProjectionRegistry.Options(id).ProjectionType
		if t == event.CCS || t == event.CSS {
			consistent = append(consistent, shared.NewProjectionID(tenantID, id))
		} else {
			eventualConsistent = append(eventualConsistent, shared.NewProjectionID(tenantID, id))
		}
	}
	return eventualConsistent, consistent, nil
}

func (p ProjectionRepository) Lock(txCtx context.Context, ids ...shared.ProjectionID) error {
	if err := p.projPort.Lock(txCtx, ids...); err != nil {
		return fmt.Errorf("lock of projections %v failed: %w", ids, err)
	}
	return nil
}

func (p ProjectionRepository) UnLock(txCtx context.Context, ids ...shared.ProjectionID) error {
	if err := p.projPort.UnLock(txCtx, ids...); err != nil {
		return fmt.Errorf("unlock of projections %v failed: %w", ids, err)
	}
	return nil
}

func (p ProjectionRepository) Create(ctx context.Context, projectionIDs ...shared.ProjectionID) (streams []projection.Stream, err error) {
	ctx, endSpan := metrics.StartSpan(ctx, "Create projections (repository)", map[string]interface{}{"numberOfProjections": len(projectionIDs)})
	defer endSpan()

	projStreams := make([]projection.Stream, len(projectionIDs))
	for i, id := range projectionIDs {
		var stream projection.Stream
		stream, err = p.create(ctx, id, projection.Running, time.Time{})
		if err != nil {
			return nil, fmt.Errorf("could not create projections: could not create projection %v: %w", id, err)
		}
		projStreams[i] = stream
	}

	if err = p.SaveStates(ctx, projStreams...); err != nil {
		return nil, fmt.Errorf("could not create projections: %w", err)
	}

	return projStreams, nil
}

func (p ProjectionRepository) Get(txCtx context.Context, id shared.ProjectionID) (projection.Stream, error) {
	streams, err := p.GetProjections(txCtx, id)
	if err != nil {
		return projection.Stream{}, err
	}

	return streams[0], nil
}

func (p ProjectionRepository) GetProjections(txCtx context.Context, ids ...shared.ProjectionID) ([]projection.Stream, error) {
	txCtx, endSpan := metrics.StartSpan(txCtx, "GetNotInitialized (repository)", map[string]interface{}{"numberOfProjections": len(ids)})
	defer endSpan()

	dtos, notFound, err := p.projPort.Get(txCtx, ids...)
	if err != nil {
		return nil, fmt.Errorf("get() projections failed for %q :%w", ids, err)
	}

	if len(notFound) != 0 {
		return nil, &projPort.NotFoundError{ID: notFound[0].ID}
	}

	return p.mapToProjectionStream(txCtx, dtos)
}

func (p ProjectionRepository) GetNotInitialized(txCtx context.Context, ids ...shared.ProjectionID) ([]shared.ProjectionID, error) {
	txCtx, endSpan := metrics.StartSpan(txCtx, "GetNotInitialized (repository)", map[string]interface{}{"numberOfProjections": len(ids)})
	defer endSpan()

	_, notFound, err := p.projPort.Get(txCtx, ids...)
	if err != nil {
		return nil, fmt.Errorf("getNotInitialized() projections failed for %q :%w", ids, err)
	}

	if len(notFound) == 0 {
		return nil, nil
	}

	return p.mapToProjectionID(txCtx, notFound)
}

func (p ProjectionRepository) GetAllForTenant(txCtx context.Context, tenantID string) ([]projection.Stream, error) {
	txCtx, endSpan := metrics.StartSpan(txCtx, "GetAllForTenant (repository)", map[string]interface{}{"tenantID": tenantID})
	defer endSpan()

	dtos, err := p.projPort.GetAllForTenant(txCtx, tenantID)
	if err != nil {
		return nil, fmt.Errorf("getAllForTenant() projections failed for %q :%w", tenantID, err)
	}

	return p.mapToProjectionStream(txCtx, dtos)
}

func (p ProjectionRepository) GetWithNewEventsSinceLastRun(txCtx context.Context, id shared.ProjectionID) (projection.Stream, error) {
	txCtx, endSpan := metrics.StartSpan(txCtx, "GetWithNewEventsSinceLastRun (repository)", map[string]interface{}{"tenantID": id.TenantID, "projectionID": id.ProjectionID})
	defer endSpan()

	opt := p.registries.ProjectionRegistry.Options(id.ProjectionID)
	project, err := p.registries.ProjectionRegistry.Projection(id.ProjectionID)
	if err != nil {
		return projection.Stream{}, fmt.Errorf("GetWithNewEventsSinceLastRun() retrieve from registry failed for  %q :%w", id, err)
	}

	dto, err := p.projPort.GetSinceLastRun(txCtx, id, projPort.LoadOptions{ChunkSize: project.ChunkSize()})
	if err != nil {
		return projection.Stream{}, fmt.Errorf("GetWithNewEventsSinceLastRun() retrieve events failed for  %q :%w", id, err)
	}

	return projection.LoadFromDTO(dto, project, opt), err
}

func (p ProjectionRepository) GetAllForAllTenants(txCtx context.Context) ([]projection.Stream, error) {
	dtos, err := p.projPort.GetAllForAllTenants(txCtx)
	if err != nil {
		return nil, fmt.Errorf("GetAllForAllTenants() retrieve projections failed :%w", err)
	}
	return p.mapToProjectionStream(txCtx, dtos)
}

func (p ProjectionRepository) SaveStates(txCtx context.Context, streams ...projection.Stream) error {
	txCtx, endSpan := metrics.StartSpan(txCtx, "SaveStates (repository)", map[string]interface{}{"numberOfStreams": len(streams)})
	defer endSpan()

	projDtos := make([]projPort.DTO, len(streams))
	for i, stream := range streams {
		projDtos[i] = projPort.DTO{
			TenantID:     stream.ID().TenantID,
			ProjectionID: stream.ID().ProjectionID,
			State:        string(stream.State()),
			UpdatedAt:    stream.UpdatedAt(),
		}
	}

	if err := p.projPort.SaveStates(txCtx, projDtos...); err != nil {
		return fmt.Errorf("save() projections failed :%w", err)
	}
	return nil
}

func (p ProjectionRepository) SaveEvents(txCtx context.Context, streams ...projection.Stream) error {
	txCtx, endSpan := metrics.StartSpan(txCtx, "SaveEvents (repository)", map[string]interface{}{"numberOfStreams": len(streams)})
	defer endSpan()

	projDtos := make([]projPort.DTO, len(streams))
	for i, stream := range streams {
		projDtos[i] = projPort.DTO{
			TenantID:     stream.ID().TenantID,
			ProjectionID: stream.ID().ProjectionID,
			UpdatedAt:    stream.UpdatedAt(),
			Events:       stream.Events(),
		}
	}

	if err := p.projPort.SaveEvents(txCtx, projDtos...); err != nil {
		return fmt.Errorf("save() projections failed :%w", err)
	}
	return nil
}

func (p ProjectionRepository) Reset(txCtx context.Context, id shared.ProjectionID, sinceTime time.Time, eventTypes ...string) error {
	return p.projPort.ResetSince(txCtx, id, sinceTime, eventTypes...)
}

func (p ProjectionRepository) RemoveProjection(txCtx context.Context, projectionID string) error {
	txCtx, endSpan := metrics.StartSpan(txCtx, "RemoveProjection (repository)", map[string]interface{}{"projectionID": projectionID})
	defer endSpan()

	return p.projPort.RemoveProjection(txCtx, projectionID)
}

func (p ProjectionRepository) DeleteEventFromQueue(txCtx context.Context, eventID string, ids ...shared.ProjectionID) error {
	txCtx, endSpan := metrics.StartSpan(txCtx, "DeleteEventFromQueue (repository)", map[string]interface{}{"eventID": eventID, "numberOfProjections": len(ids)})
	defer endSpan()

	return p.projPort.DeleteEventFromQueue(txCtx, eventID, ids...)
}

func (p ProjectionRepository) GetProjectionsWithEventInQueue(txCtx context.Context, id shared.AggregateID, eventID string) ([]shared.ProjectionID, error) {
	txCtx, endSpan := metrics.StartSpan(txCtx, "GetProjectionsWithEventInQueue (repository)", map[string]interface{}{"tenantID": id.TenantID, "aggregateType": id.AggregateType, "aggregateID": id.AggregateID, "eventID": eventID})
	defer endSpan()

	return p.projPort.GetProjectionsWithEventInQueue(txCtx, id, eventID)
}

func (p ProjectionRepository) create(_ context.Context, id shared.ProjectionID, state projection.State, updatedAt time.Time) (projection.Stream, error) {
	opt := p.registries.ProjectionRegistry.Options(id.ProjectionID)
	proj, err := p.registries.ProjectionRegistry.Projection(id.ProjectionID)
	if err != nil {
		return projection.Stream{}, fmt.Errorf("could not create ProjectionRepository for Projection %v: %w", id, err)
	}

	return projection.CreateStream(id, state, updatedAt, proj, opt), nil
}

func (p ProjectionRepository) mapToProjectionStream(txCtx context.Context, dtos []projPort.DTO) ([]projection.Stream, error) {
	var result []projection.Stream
	for _, dto := range dtos {
		var stream projection.Stream
		stream, err := p.create(txCtx, shared.NewProjectionID(dto.TenantID, dto.ProjectionID), projection.State(dto.State), dto.UpdatedAt)
		if err != nil {
			return nil, fmt.Errorf("could not mapToProjectionStream: %w", err)
		}

		result = append(result, stream)
	}
	return result, nil
}

func (p ProjectionRepository) mapToProjectionID(_ context.Context, notFound []projPort.NotFoundError) ([]shared.ProjectionID, error) {
	var result []shared.ProjectionID
	for _, dto := range notFound {
		result = append(result, dto.ID)
	}
	return result, nil
}
