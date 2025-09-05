package ProjectionRegistry

import (
	"fmt"
	"github.com/global-soft-ba/go-eventstore"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/domain/model/projection"
	kvTable2 "github.com/global-soft-ba/go-eventstore/eventstore/core/shared/kvTable"
	"github.com/global-soft-ba/go-eventstore/instrumentation/port/logger"
	"time"
)

const (
	defaultProjectionHPatchStrategy = event.Error
	defaultProjectionDPatchStrategy = event.Manual

	defaultProjectionType = event.ECS

	// timeout in which the projection in the domain must be done
	// (default TransactionTimeout Postgres = 30 sec.)
	defaultProjectionExecutionTimeOut = 20 * time.Second

	// number of possible parallel requests for projection execution
	defaultProjectionWorkerInputQueue = 100
)

var defaultOptions = projection.Options{
	HPatchStrategy:          defaultProjectionHPatchStrategy,
	DPatchStrategy:          defaultProjectionDPatchStrategy,
	ExecutionTimeOut:        defaultProjectionExecutionTimeOut,
	PreparationTimeOut:      defaultProjectionExecutionTimeOut,
	FinishingTimeOut:        defaultProjectionExecutionTimeOut,
	RebuildExecutionTimeOut: defaultProjectionExecutionTimeOut,
	InputQueueLength:        defaultProjectionWorkerInputQueue,
	ProjectionType:          defaultProjectionType,
}

func NewRegistry() *Registry {
	return &Registry{
		projections:      kvTable2.NewKeyValuesTable[event.Projection](),
		registeredEvents: kvTable2.NewKeyValuesTable[string](),
		options:          kvTable2.NewKeyValuesTable[projection.Options](),
	}
}

type Registry struct {
	projections      kvTable2.IKVTable[event.Projection]   // key[projectionID]
	registeredEvents kvTable2.IKVTable[string]             // key[eventName]
	options          kvTable2.IKVTable[projection.Options] // key[projections]
}

func (r Registry) currentOrDefaultOptions(projectionID string) projection.Options {
	currOptions, err := kvTable2.GetFirst(r.options, kvTable2.NewKey(projectionID))
	if err != nil {
		if !kvTable2.IsKeyNotFound(err) {
			logger.Error(fmt.Errorf("send default optiosn for for projections %s due to failed repository retrieval: %w", projectionID, err))
			return defaultOptions
		}
		return defaultOptions
	}
	return currOptions
}

func (r Registry) Options(projectionID string) projection.Options {
	return r.currentOrDefaultOptions(projectionID)
}

func (r Registry) All() []event.Projection {
	return kvTable2.DataAsSlice(r.projections)
}

func (r Registry) Projection(projectionID string) (event.Projection, error) {
	proj, err := kvTable2.GetFirst(r.projections, kvTable2.NewKey(projectionID))
	return proj, err
}

func (r Registry) Register(projection event.Projection) (err error) {
	if err = kvTable2.Set(r.projections, kvTable2.NewKey(projection.ID()), projection); err != nil {
		return fmt.Errorf("could not store projection: %w", err)
	}

	for _, evt := range projection.EventTypes() {
		if err = kvTable2.Add(r.registeredEvents, kvTable2.NewKey(evt), projection.ID()); err != nil {
			return fmt.Errorf("could not add projection to registered events: %w", err)
		}
	}
	return err
}

func (r Registry) SetHPatchStrategy(projectionID string, strategy event.ProjectionPatchStrategy) error {
	currOptions := r.currentOrDefaultOptions(projectionID)
	currOptions.HPatchStrategy = strategy
	if err := kvTable2.Set(r.options, kvTable2.NewKey(projectionID), currOptions); err != nil {
		return fmt.Errorf("could not store projection options: %w", err)
	}
	return nil
}

func (r Registry) SetDPatchStrategy(projectionID string, strategy event.ProjectionPatchStrategy) error {
	currOptions := r.currentOrDefaultOptions(projectionID)
	currOptions.DPatchStrategy = strategy
	if err := kvTable2.Set(r.options, kvTable2.NewKey(projectionID), currOptions); err != nil {
		return fmt.Errorf("could not store projection options: %w", err)
	}
	return nil
}

func (r Registry) SetProjectionType(projectionID string, projectionTyp event.ProjectionType) error {
	currOptions := r.currentOrDefaultOptions(projectionID)
	currOptions.ProjectionType = projectionTyp
	if err := kvTable2.Set(r.options, kvTable2.NewKey(projectionID), currOptions); err != nil {
		return fmt.Errorf("could not store projection options: %w", err)
	}
	return nil
}

func (r Registry) SetTimeOut(projectionID string, timeOut time.Duration) error {
	currOptions := r.currentOrDefaultOptions(projectionID)
	currOptions.ExecutionTimeOut = timeOut
	if err := kvTable2.Set(r.options, kvTable2.NewKey(projectionID), currOptions); err != nil {
		return fmt.Errorf("could not store projection options: %w", err)
	}
	return nil
}

func (r Registry) SetRebuildTimeOut(projectionID string, timeOut time.Duration) error {
	currOptions := r.currentOrDefaultOptions(projectionID)
	currOptions.RebuildExecutionTimeOut = timeOut
	if err := kvTable2.Set(r.options, kvTable2.NewKey(projectionID), currOptions); err != nil {
		return fmt.Errorf("could not store projection options: %w", err)
	}
	return nil
}

func (r Registry) SetPreparationTimeOut(projectionID string, timeOut time.Duration) error {
	currOptions := r.currentOrDefaultOptions(projectionID)
	currOptions.PreparationTimeOut = timeOut
	if err := kvTable2.Set(r.options, kvTable2.NewKey(projectionID), currOptions); err != nil {
		return fmt.Errorf("could not store projection options: %w", err)
	}
	return nil
}

func (r Registry) SetFinishTimeOut(projectionID string, timeOut time.Duration) error {
	currOptions := r.currentOrDefaultOptions(projectionID)
	currOptions.FinishingTimeOut = timeOut
	if err := kvTable2.Set(r.options, kvTable2.NewKey(projectionID), currOptions); err != nil {
		return fmt.Errorf("could not store projection options: %w", err)
	}
	return nil
}

func (r Registry) SetWorkerQueueLength(projectionID string, length int) error {
	currOptions := r.currentOrDefaultOptions(projectionID)
	currOptions.InputQueueLength = length
	if err := kvTable2.Set(r.options, kvTable2.NewKey(projectionID), currOptions); err != nil {
		return fmt.Errorf("could not store projection options: %w", err)
	}
	return nil
}

func (r Registry) ForEventTypes(eventTypes ...string) []string {
	var result []string
	uniqueIds := make(map[string]struct{})
	for _, eventType := range eventTypes {
		projs, err := kvTable2.Get(r.registeredEvents, kvTable2.NewKey(eventType))
		if err != nil && !kvTable2.IsKeyNotFound(err) {
			logger.Error(fmt.Errorf("ForEventTypes() in projections registry failed: %w", err))
			return nil
		}
		for _, proj := range projs {
			if _, exists := uniqueIds[proj]; !exists {
				result = append(result, proj)
				uniqueIds[proj] = struct{}{}
			}
		}
	}

	return result
}
