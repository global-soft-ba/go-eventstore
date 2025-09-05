package projection

import (
	"context"
	"example/core/port/service/projection"
	"fmt"
	"github.com/global-soft-ba/go-eventstore"
	"github.com/global-soft-ba/go-eventstore/instrumentation/port/logger"
	"github.com/global-soft-ba/go-eventstore/instrumentation/port/metrics"
	"github.com/samber/lo"
	"strings"
	"time"
)

type Service struct {
	eventStore  event.EventStore
	projections []event.Projection
}

func NewProjectionService(eventStore event.EventStore, projections ...event.Projection) *Service {
	return &Service{
		eventStore:  eventStore,
		projections: projections,
	}
}

func (p *Service) GetProjectionStates(ctx context.Context, tenantID string, projectionIDs ...string) ([]projection.StateResponseDTO, error) {
	ctx, endSpan := metrics.StartSpan(ctx, "get-projection-states", map[string]interface{}{"tenantID": tenantID, "projectionIDs": projectionIDs})
	defer endSpan()
	states, err := p.eventStore.GetProjectionStates(ctx, tenantID, p.getValidProjectionsIDs(projectionIDs)...)
	if err != nil {
		return nil, fmt.Errorf("could not get projection states: %w", err)
	}

	logger.Info("Retrieved %d projection states for tenant %q", len(states), tenantID)

	return projection.FromEventStoreProjectionStates(states), err
}

func (p *Service) RebuildProjections(ctx context.Context, tenantID string, projectionIDs ...string) (err error) {
	ctx, endSpan := metrics.StartSpan(ctx, "rebuild-projections", map[string]interface{}{"tenantID": tenantID, "projectionIDs": projectionIDs})
	defer endSpan()

	if err = p.rebuildProjections(ctx, tenantID, projectionIDs); err != nil {
		err = fmt.Errorf("could not rebuild projections: %w", err)
		return err
	}

	logger.Info("Rebuilt %d projections successfully", len(projectionIDs))
	return nil
}

func (p *Service) rebuildProjections(ctx context.Context, tenantID string, projectionIDs []string) (err error) {
	now := time.Now()
	if len(projectionIDs) == 0 {
		for _, projectionService := range p.projections {
			if err = p.rebuildProjection(ctx, tenantID, projectionService, now); err != nil {
				return err
			}
		}
		return nil
	}
	for projectionType := range toLowerHashSet(projectionIDs) {
		for _, projectionService := range p.projections {
			if projectionService.ID() == projectionType {
				err = p.rebuildProjection(ctx, tenantID, projectionService, now)
				break
			}
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (p *Service) rebuildProjection(ctx context.Context, tenantID string, projection event.Projection, until time.Time) (err error) {
	ctx, endSpan := metrics.StartSpan(ctx, "rebuild-projection", map[string]interface{}{"tenantID": tenantID, "projectionID": projection.ID(), "until": until})
	defer endSpan()

	_ = p.eventStore.RebuildProjection(ctx, tenantID, projection.ID())
	return nil
}

func (p *Service) StopProjections(ctx context.Context, tenantID string, projectionIDs ...string) (err error) {
	ctx, endSpan := metrics.StartSpan(ctx, "stop-projections", map[string]interface{}{"tenantID": tenantID, "numberOfProjections": len(projectionIDs)})
	defer endSpan()
	for projectionID := range toLowerHashSet(projectionIDs) {
		if err = p.eventStore.StopProjection(ctx, tenantID, projectionID); err != nil {
			return fmt.Errorf("could not stop projection %q: %w", projectionID, err)
		}
	}

	logger.Info("Stopped %d projections successfully for tenant %q", len(projectionIDs), tenantID)
	return nil
}

func (p *Service) StartProjections(ctx context.Context, tenantID string, projectionIDs ...string) (err error) {
	ctx, endSpan := metrics.StartSpan(ctx, "start-projections", map[string]interface{}{"tenantID": tenantID, "numberOfProjections": len(projectionIDs)})
	defer endSpan()
	for projectionID := range toLowerHashSet(projectionIDs) {
		if _, err = p.eventStore.StartProjection(ctx, tenantID, projectionID); err != nil {
			return fmt.Errorf("could not stop projection %q: %w", projectionID, err)
		}
	}

	logger.Info("Started %d projections successfully for tenant %q", len(projectionIDs), tenantID)
	return nil
}

func (p *Service) DeleteProjection(ctx context.Context, tenantID, projectionID string) error {
	ctx, endSpan := metrics.StartSpan(ctx, "delete-projection", map[string]interface{}{"tenantID": tenantID, "projectionID": projectionID})
	defer endSpan()
	if projectionID == "" {
		return fmt.Errorf("projectionID is empty")
	}
	if err := p.eventStore.RemoveProjection(ctx, projectionID); err != nil {
		return fmt.Errorf("could not delete projection %q: %w", projectionID, err)
	}

	logger.Info("Deleted projection %q successfully for tenant %q", projectionID, tenantID)
	return nil
}

func (p *Service) getValidProjectionsIDs(unCleanIDs []string) []string {
	var validIDs []string
	if len(unCleanIDs) != 0 {
		for lowerID := range lo.Associate(unCleanIDs, func(str string) (string, interface{}) { return strings.ToLower(str), nil }) {
			for _, proj := range p.projections {
				if proj.ID() == lowerID {
					validIDs = append(validIDs, lowerID)
					break
				}
			}
		}
	} else {
		for _, proj := range p.projections {
			validIDs = append(validIDs, proj.ID())
		}
	}
	return validIDs
}

func toLowerHashSet(entities []string) map[string]interface{} {
	entityMap := make(map[string]interface{})
	for _, entity := range entities {
		entityMap[strings.ToLower(entity)] = nil
	}
	return entityMap
}
