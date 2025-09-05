package eventstore

import (
	"context"
	"fmt"
	"github.com/global-soft-ba/go-eventstore"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/application/local/commandBus"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/application/local/eventBus"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/application/local/eventBus/domainEvents"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/application/registry"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/application/repository"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/application/services"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/application/services/projection"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/port/persistence"
	noopLogger "github.com/global-soft-ba/go-eventstore/instrumentation/adapter/logger/noop"
	noopMetrics "github.com/global-soft-ba/go-eventstore/instrumentation/adapter/metrics/noop"
	"github.com/global-soft-ba/go-eventstore/instrumentation/port/logger"
	"github.com/global-soft-ba/go-eventstore/instrumentation/port/metrics"
	"time"
)

func NewForTestWithTxCTX(txCtx context.Context, adapter persistence.Port, options ...func(store *eventStore) error) (event.EventStore, error, chan error) {
	adpAgg := adapter.AggregatePort()
	projAgg := adapter.ProjectionPort()
	trans := adapter.Transactor()

	registries := registry.NewRegistries()
	aggRepro := repository.NewAggregateRepository(adpAgg, registries)
	projRepro := repository.NewProjectionRepository(projAgg, registries)

	evtBus := eventBus.NewEventPublisher()
	cmdBus := commandBus.NewCommandPublisher()

	saver := services.NewSaverService(aggRepro, projRepro, trans, evtBus, cmdBus, registries)
	loader := services.NewLoaderService(aggRepro, trans)
	projecter := projection.NewProjectionService(projRepro, trans, evtBus, cmdBus, registries)

	evtStore := &eventStore{saver, loader, projecter, registries}
	for _, opt := range options {
		err := opt(evtStore)
		if err != nil {
			return eventStore{}, fmt.Errorf("could not configure eventstore: %w", err), nil
		}
	}
	evtStore.ensureLoggerAndMetrics()

	return evtStore, nil, evtBus.Publish(txCtx, domainEvents.EventStoreStarted())
}

func New(adapter persistence.Port, options ...func(store *eventStore) error) (event.EventStore, error, chan error) {
	adpAgg := adapter.AggregatePort()
	projAgg := adapter.ProjectionPort()
	trans := adapter.Transactor()

	registries := registry.NewRegistries()
	aggRepro := repository.NewAggregateRepository(adpAgg, registries)
	projRepro := repository.NewProjectionRepository(projAgg, registries)

	evtBus := eventBus.NewEventPublisher()
	cmdBus := commandBus.NewCommandPublisher()

	saver := services.NewSaverService(aggRepro, projRepro, trans, evtBus, cmdBus, registries)
	loader := services.NewLoaderService(aggRepro, trans)
	projecter := projection.NewProjectionService(projRepro, trans, evtBus, cmdBus, registries)

	evtStore := &eventStore{saver, loader, projecter, registries}
	for _, opt := range options {
		err := opt(evtStore)
		if err != nil {
			return eventStore{}, fmt.Errorf("could not configure eventstore: %w", err), nil
		}
	}

	return evtStore, nil, evtBus.Publish(context.Background(), domainEvents.EventStoreStarted())
}

func WithConcurrentModificationStrategy(aggregateType string, strategy event.ConcurrentModificationStrategy) func(store *eventStore) error {
	return func(s *eventStore) error {
		err := s.registries.AggregateRegistry.SetConcurrentModificationStrategy(aggregateType, strategy)
		if err != nil {
			return err
		}
		return nil
	}
}

// WithEphemeralEventTypes sets the event types that are not persisted in the event store
func WithEphemeralEventTypes(aggregateType string, eventTypes []string) func(store *eventStore) error {
	return func(s *eventStore) error {
		err := s.registries.AggregateRegistry.SetEphemeralEventTypes(aggregateType, eventTypes)
		if err != nil {
			return err
		}
		return nil
	}
}

func WithDeleteStrategy(aggregateType string, strategy event.DeleteStrategy) func(store *eventStore) error {
	return func(s *eventStore) error {
		err := s.registries.AggregateRegistry.SetDeleteStrategy(aggregateType, strategy)
		if err != nil {
			return err
		}
		return nil
	}
}

func WithSaveRetryDurations(retryAfterMilliseconds []time.Duration) func(store *eventStore) error {
	return func(s *eventStore) error {
		s.saver.SetSaveRetryDuration(retryAfterMilliseconds)
		return nil
	}
}

func WithProjection(proj event.Projection) func(store *eventStore) error {
	return func(s *eventStore) error {
		err := s.registries.ProjectionRegistry.Register(proj)
		if err != nil {
			return err
		}
		return nil
	}
}

func WithProjectionWorkerQueueLength(projectionID string, workerQueueLength int) func(store *eventStore) error {
	return func(s *eventStore) error {
		err := s.registries.ProjectionRegistry.SetWorkerQueueLength(projectionID, workerQueueLength)
		if err != nil {
			return err
		}
		return nil
	}
}

// WithProjectionTimeOut sets the timeout for the projection execution phase of a projection (default: 20 * time.second)
func WithProjectionTimeOut(projectionID string, timeOut time.Duration) func(store *eventStore) error {
	return func(s *eventStore) error {
		err := s.registries.ProjectionRegistry.SetTimeOut(projectionID, timeOut)
		if err != nil {
			return err
		}
		return nil
	}
}

// WithRebuildTimeOut sets the timeout for the rebuild execution phase of a projection (default: 20 * time.second)
func WithRebuildTimeOut(projectionID string, timeOut time.Duration) func(store *eventStore) error {
	return func(s *eventStore) error {
		err := s.registries.ProjectionRegistry.SetRebuildTimeOut(projectionID, timeOut)
		if err != nil {
			return err
		}
		return nil
	}
}

// WithPreparationTimeOut sets the timeout for the preparation phase of a projection or rebuild (default: 20 * time.Second)
func WithPreparationTimeOut(projectionID string, timeOut time.Duration) func(store *eventStore) error {
	return func(s *eventStore) error {
		err := s.registries.ProjectionRegistry.SetPreparationTimeOut(projectionID, timeOut)
		if err != nil {
			return err
		}
		return nil
	}
}

// WithFinishTimeOut sets the timeout for the finish phase of a projection or rebuild (default: 20 * time.Second)
func WithFinishTimeOut(projectionID string, timeOut time.Duration) func(store *eventStore) error {
	return func(s *eventStore) error {
		err := s.registries.ProjectionRegistry.SetFinishTimeOut(projectionID, timeOut)
		if err != nil {
			return err
		}
		return nil
	}
}

func WithHistoricalPatchStrategy(projectionID string, strategy event.ProjectionPatchStrategy) func(store *eventStore) error {
	return func(s *eventStore) error {
		err := s.registries.ProjectionRegistry.SetHPatchStrategy(projectionID, strategy)
		if err != nil {
			return err
		}
		return nil
	}
}

func WithDeletePatchStrategy(projectionID string, strategy event.ProjectionPatchStrategy) func(store *eventStore) error {
	return func(s *eventStore) error {
		err := s.registries.ProjectionRegistry.SetDPatchStrategy(projectionID, strategy)
		if err != nil {
			return err
		}
		return nil
	}
}

func WithProjectionType(projectionID string, projectionType event.ProjectionType) func(store *eventStore) error {
	return func(s *eventStore) error {
		err := s.registries.ProjectionRegistry.SetProjectionType(projectionID, projectionType)
		if err != nil {
			return err
		}
		return nil
	}
}

func WithMetrics(metricsPort metrics.Port) func(store *eventStore) error {
	return func(s *eventStore) error {
		metrics.SetMetrics(metricsPort)
		return nil
	}
}

func WithLogger(loggerPort logger.Port) func(store *eventStore) error {
	return func(s *eventStore) error {
		logger.SetLogger(loggerPort)
		return nil
	}
}

type eventStore struct {
	saver      services.SaverService
	loader     services.LoaderService
	projecter  projection.ProjectionService
	registries *registry.Registries
}

func (e eventStore) Save(ctx context.Context, tenantID string, events []event.PersistenceEvent, version int) (chan error, error) {
	ctx, endSpan := metrics.StartSpan(ctx, "Save (store)", map[string]interface{}{"tenantID": tenantID, "amount of events": len(events)})
	defer endSpan()

	return e.SaveAll(ctx, tenantID, []event.PersistenceEvents{{
		Events:  events,
		Version: version,
	},
	})
}

func (e eventStore) SaveAll(ctx context.Context, tenantID string, events []event.PersistenceEvents) (chan error, error) {
	ctx, endSpan := metrics.StartSpan(ctx, "SaveAll (store)", map[string]interface{}{"tenantID": tenantID, "amount of events": len(events)})
	defer endSpan()

	return e.saver.SaveWithRetry(ctx, tenantID, events)
}

func (e eventStore) LoadAsAt(ctx context.Context, tenantID, aggregateType, aggregateID string, projectionTime time.Time) (eventStream []event.PersistenceEvent, version int, err error) {
	ctx, endSpan := metrics.StartSpan(ctx, "LoadAsAt (store)", map[string]interface{}{"tenantID": tenantID, "aggregateType": aggregateType, "aggregateID": aggregateID})
	defer endSpan()

	return e.loader.LoadAsAt(ctx, tenantID, aggregateType, aggregateID, projectionTime)
}

func (e eventStore) LoadAsOf(ctx context.Context, tenantID, aggregateType, aggregateID string, projectionTime time.Time) (eventStream []event.PersistenceEvent, version int, err error) {
	ctx, endSpan := metrics.StartSpan(ctx, "LoadAsOf (store)", map[string]interface{}{"tenantID": tenantID, "aggregateType": aggregateType, "aggregateID": aggregateID})
	defer endSpan()

	return e.loader.LoadAsOf(ctx, tenantID, aggregateType, aggregateID, projectionTime)
}

func (e eventStore) LoadAsOfTill(ctx context.Context, tenantID, aggregateType, aggregateID string, projectionTime, reportTime time.Time) (eventStream []event.PersistenceEvent, version int, err error) {
	ctx, endSpan := metrics.StartSpan(ctx, "LoadAsOfTill (store)", map[string]interface{}{"tenantID": tenantID, "aggregateType": aggregateType, "aggregateID": aggregateID})
	defer endSpan()

	return e.loader.LoadAsOfTill(ctx, tenantID, aggregateType, aggregateID, projectionTime, reportTime)
}

func (e eventStore) LoadAllOfAggregateTypeAsAt(ctx context.Context, tenantID, aggregateType string, projectionTime time.Time) (eventStreams []event.PersistenceEvents, err error) {
	ctx, endSpan := metrics.StartSpan(ctx, "LoadAllOfAggregateTypeAsAt (store)", map[string]interface{}{"tenantID": tenantID, "aggregateType": aggregateType})
	defer endSpan()

	return e.loader.LoadAllOfAggregateAsAt(ctx, tenantID, aggregateType, projectionTime)
}

func (e eventStore) LoadAllOfAggregateTypeAsOf(ctx context.Context, tenantID, aggregateType string, projectionTime time.Time) (eventStreams []event.PersistenceEvents, err error) {
	ctx, endSpan := metrics.StartSpan(ctx, "LoadAllOfAggregateTypeAsOf (store)", map[string]interface{}{"tenantID": tenantID, "aggregateType": aggregateType})
	defer endSpan()

	return e.loader.LoadAllOfAggregateAsOf(ctx, tenantID, aggregateType, projectionTime)
}

func (e eventStore) LoadAllOfAggregateTypeAsOfTill(ctx context.Context, tenantID, aggregateType string, projectionTime, reportTime time.Time) (eventStreams []event.PersistenceEvents, err error) {
	ctx, endSpan := metrics.StartSpan(ctx, "LoadAllOfAggregateTypeAsOfTill (store)", map[string]interface{}{"tenantID": tenantID, "aggregateType": aggregateType})
	defer endSpan()

	return e.loader.LoadAllOfAggregateAsOfTill(ctx, tenantID, aggregateType, projectionTime, reportTime)
}

func (e eventStore) LoadAllAsAt(ctx context.Context, tenantID string, projectionTime time.Time) (eventStreams []event.PersistenceEvents, err error) {
	ctx, endSpan := metrics.StartSpan(ctx, "LoadAllAsAt (store)", map[string]interface{}{"tenantID": tenantID, "projectionTime": projectionTime})
	defer endSpan()

	return e.loader.LoadAllAsAt(ctx, tenantID, projectionTime)
}

func (e eventStore) LoadAllAsOf(ctx context.Context, tenantID string, projectionTime time.Time) (eventStreams []event.PersistenceEvents, err error) {
	ctx, endSpan := metrics.StartSpan(ctx, "LoadAllAsOf (store)", map[string]interface{}{"tenantID": tenantID, "projectionTime": projectionTime})
	defer endSpan()

	return e.loader.LoadAllAsOf(ctx, tenantID, projectionTime)
}

func (e eventStore) LoadAllAsOfTill(ctx context.Context, tenantID string, projectionTime time.Time, reportTime time.Time) (eventStreams []event.PersistenceEvents, err error) {
	ctx, endSpan := metrics.StartSpan(ctx, "LoadAllAsOfTill (store)", map[string]interface{}{"tenantID": tenantID, "projectionTime": projectionTime})
	defer endSpan()

	return e.loader.LoadAllAsOfTill(ctx, tenantID, projectionTime, reportTime)
}

func (e eventStore) DeleteEvent(ctx context.Context, tenantID, aggregateType, aggregateID, eventID string) error {
	ctx, endspan := metrics.StartSpan(ctx, "DeletedEvent (store)", map[string]interface{}{"tenantID": tenantID, "aggregateType": aggregateType, "aggregateID": aggregateID, "eventID": eventID})
	defer endspan()

	return e.saver.DeleteEvent(ctx, tenantID, aggregateType, aggregateID, eventID)
}

func (e eventStore) ensureLoggerAndMetrics() {
	if !logger.IsLoggerSet() {
		logger.SetLogger(noopLogger.Adapter{})
	}
	if !metrics.IsMetricsSet() {
		metrics.SetMetrics(noopMetrics.Adapter{})
	}
}
