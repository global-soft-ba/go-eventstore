package services

import (
	"context"
	"errors"
	"fmt"
	"github.com/global-soft-ba/go-eventstore"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/application/local/commandBus"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/application/local/commandBus/commands"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/application/local/eventBus"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/application/registry"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/application/repository"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/domain/model/projection"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/domain/service"
	transactor2 "github.com/global-soft-ba/go-eventstore/eventstore/core/port/persistence/transactor"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/shared"
	"github.com/global-soft-ba/go-eventstore/eventstore/infrastructure/consistentClock"
	"github.com/global-soft-ba/go-eventstore/instrumentation/port/logger"
	"github.com/global-soft-ba/go-eventstore/instrumentation/port/metrics"
	"github.com/samber/lo"
	"time"
)

var defaultSaveRetryDurations = []time.Duration{5, 10, 100, 385, 500}

func NewSaverService(aggRepro repository.AggregateRepositoryInterface, projRepro repository.ProjectionRepositoryInterface, transactor transactor2.Port, evtBus *eventBus.EventPublisher, cmdBus *commandBus.CommandPublisher, registries *registry.Registries) SaverService {
	return SaverService{
		domain:                 service.DomainService{Clock: consistentClock.New()},
		evtBus:                 evtBus,
		cmdBus:                 cmdBus,
		aggregateRepository:    aggRepro,
		projectionRepository:   projRepro,
		retryAfterMilliseconds: defaultSaveRetryDurations,
		transactor:             transactor,
		registries:             registries,
	}
}

type SaverService struct {
	domain     service.DomainService
	evtBus     *eventBus.EventPublisher
	cmdBus     *commandBus.CommandPublisher
	registries *registry.Registries

	aggregateRepository  repository.AggregateRepositoryInterface
	projectionRepository repository.ProjectionRepositoryInterface

	retryAfterMilliseconds []time.Duration
	transactor             transactor2.Port
}

func (s *SaverService) SetSaveRetryDuration(durations []time.Duration) {
	s.retryAfterMilliseconds = durations
}

func (s *SaverService) DisableSnapShot(ctx context.Context, tenantID, aggregateType, aggregateID string, sinceTime time.Time) error {
	errTx := s.transactor.WithinTX(ctx, func(txCtx context.Context) error {
		return s.aggregateRepository.DisableSnapShots(txCtx, shared.AggregateID{TenantID: tenantID, AggregateType: aggregateType, AggregateID: aggregateID}, sinceTime)
	})

	if errTx != nil {
		return fmt.Errorf("DeleteSnapShots() failed:%w", errTx)
	}
	return errTx
}

func (s *SaverService) SaveWithRetry(ctx context.Context, tenantID string, persistenceEvents []event.PersistenceEvents) (chan error, error) {
	ctx, endSpan := metrics.StartSpan(ctx, "SaveWithRetry (service)", map[string]interface{}{"tenantID": tenantID, "numberOfEvents": len(persistenceEvents)})
	defer endSpan()

	var concurrentAggregateAccessError *event.ErrorConcurrentAggregateAccess
	var concurrentProjectionAccessError *event.ErrorConcurrentProjectionAccess

	for _, retryDuration := range s.retryAfterMilliseconds {
		errCh, err := s.Save(ctx, tenantID, persistenceEvents)
		switch {
		case err == nil:
			return errCh, nil
		case errors.As(err, &concurrentAggregateAccessError):
			time.Sleep(retryDuration * time.Millisecond)
		case errors.As(err, &concurrentProjectionAccessError):
			time.Sleep(retryDuration * time.Millisecond)
		default:
			return nil, fmt.Errorf("save() failed :%w", err)
		}
	}
	// Last retry attempt
	ch, err := s.Save(ctx, tenantID, persistenceEvents)
	if err != nil {
		return nil, fmt.Errorf("save() failed: %w", err)
	}
	return ch, err
}

func (s *SaverService) Save(ctx context.Context, tenantID string, persistenceEvents []event.PersistenceEvents) (chan error, error) {
	ctx, endSpan := metrics.StartSpan(ctx, "Save (service)", map[string]interface{}{"tenantID": tenantID, "numberOfEvents": len(persistenceEvents)})
	defer endSpan()

	// Due to the fact that we allow new TenantIDs to be added dynamically at runtime, it is not possible to initialize
	// all tenants and associated projections upfront during the start of the service. Instead, we check if the tenantID
	// is already known, if not, the tenant will be initialized.
	if exists := s.registries.TenantRegistry.Exists(tenantID); !exists {
		err := s.registerAndInitNewTenant(ctx, tenantID)
		if err != nil {
			return nil, fmt.Errorf("registerAndInitNewTenant() failed :%w", err)
		}
	}

	aggregateIDs, eventTypes, err := s.domain.GetUniqueAggregateIDsAndEventTypes(ctx, tenantID, persistenceEvents)
	if err != nil {
		return nil, fmt.Errorf("GetUniqueAggregateIDsAndEventTypes() failed :%w", err)
	}

	eventualConsistentProjIDs, consistentProjIDs, err := s.projectionRepository.GetProjectionIDsForEventTypes(tenantID, eventTypes...)
	if err != nil {
		return nil, fmt.Errorf("GetProjectionIDsForEventTypes() failed :%w", err)
	}

	var streamCollection *service.StreamCollection
	if err = s.transactor.WithinTX(ctx, func(txCtx context.Context) (err error) {
		// Lock aggregates BEFORE retrieve/creating the aggregate stream
		if err = s.aggregateRepository.Lock(txCtx, aggregateIDs...); err != nil {
			return fmt.Errorf("locking of aggregates failed: %w", err)
		}

		// Lock consistent projections BEFORE building the streams (with versioning and timestamping)
		// We are using the valid time as global-id for cross stream consistent projections.
		// Because of the projection lock, no other request can save during the lock (with the same projection).
		// This prevents new timestamps from being inserted into the projection by other requests during the lock.
		// By doing so, the global order of the events in the stream is guaranteed. In exactly, each event has its own unique time,
		// which corresponds to the input sequence in the projection. Exceptions are patches, which have their own valid timestamps by nature.
		// But with the transaction time, we can always rebuild the correct insert order of events in the store.
		// This insert order is for consistent projections crucial (in case of rebuild or replay of events).
		if err = s.projectionRepository.Lock(txCtx, consistentProjIDs...); err != nil {
			return fmt.Errorf("locking of projections failed: %w", err)
		}

		// Unlock aggregates and projections in case of an error from here on
		defer func() {
			if errUnLock := s.aggregateRepository.UnLock(txCtx, aggregateIDs...); errUnLock != nil {
				logger.Error(fmt.Errorf("unlocking of aggregates failed: %w", errUnLock))
			}

			if errUnLockProj := s.projectionRepository.UnLock(txCtx, consistentProjIDs...); errUnLockProj != nil {
				logger.Error(fmt.Errorf("unlocking of projections failed: %w", errUnLockProj))
			}
		}()

		aggregates, err := s.aggregateRepository.GetOrCreate(txCtx, aggregateIDs...)
		if err != nil {
			return fmt.Errorf("GetOrCreate failed: %w", err)
		}

		// Because we register all new tenants above, we can assume here that all projections of it are already
		// initialized and stored in the repository.
		projections, err := s.projectionRepository.GetProjections(txCtx, append(consistentProjIDs, eventualConsistentProjIDs...)...)
		if err != nil {
			return fmt.Errorf("GetProjections failed: %w", err)
		}

		streamCollection = service.NewStreamCollection(aggregates, projections)
		return s.saveTX(txCtx, persistenceEvents, streamCollection)
	}); err != nil {
		return nil, s.wrapProjectionOutOfSyncError(err, consistentProjIDs...)
	}

	// we need a new context here, because the surrounding save or rather its context can be canceled before the projection
	// is finished (what lead to an error), e.g. gin-ctx in api for front ends
	return s.evtBus.Publish(context.Background(), streamCollection.EventsDuringSaving()...), err
}

func (s *SaverService) registerAndInitNewTenant(ctx context.Context, tenantID string) error {
	// TODO: Improvement - Handle unknown tenant with respect to fraud attacks
	if err := s.registries.TenantRegistry.Register(tenantID); err != nil {
		return fmt.Errorf("could not register and init new tenant %q: %w", tenantID, err)
	}
	if err := s.cmdBus.Execute(ctx, commands.CmdCreateTenant(tenantID)); err != nil {
		return fmt.Errorf("could not register and init new tenant %q: %w", tenantID, err)
	}
	return nil
}

func (s *SaverService) saveTX(txCtx context.Context, persistenceEvents []event.PersistenceEvents, streamCollection *service.StreamCollection) (err error) {
	txCtx, endSpan := metrics.StartSpan(txCtx, "saveTX (service)", map[string]interface{}{"numberOfEvents": len(persistenceEvents)})
	defer endSpan()

	if err = s.domain.AddEventsInStreams(txCtx, persistenceEvents, streamCollection); err != nil {
		return fmt.Errorf("saveTX() failed: %w", err)
	}
	if err = s.aggregateRepository.Save(txCtx, streamCollection.Aggregates()...); err != nil {
		return fmt.Errorf("saveTX() failed: %w", err)
	}
	if err = s.executeConsistentProjections(txCtx, streamCollection.ConsistentProjections()...); err != nil {
		return fmt.Errorf("saveTX() failed: %w", err)
	}
	if err = s.projectionRepository.SaveEvents(txCtx, streamCollection.EvtlConsistentProjections()...); err != nil {
		return fmt.Errorf("saveTX() failed: %w", err)
	}
	return nil
}

func (s *SaverService) executeConsistentProjections(txCtx context.Context, streams ...projection.Stream) (err error) {
	return s.cmdBus.Execute(txCtx, commands.CmdExecuteProjections(streams))
}

func (s *SaverService) wrapProjectionOutOfSyncError(err error, ids ...shared.ProjectionID) error {
	_, endSpan := metrics.StartSpan(context.Background(), "wrap (service)", map[string]interface{}{"numberOfProjections": len(ids)})
	defer endSpan()

	var roll *transactor2.ErrorRollbackFailed
	var commit *transactor2.ErrorCommitFailed

	switch {
	case errors.As(err, &roll):
		err = event.NewErrorProjectionOutOfSync(err, ids...)
		logger.Error(err)
		return err
	case errors.As(err, &commit):
		err = event.NewErrorProjectionOutOfSync(err, ids...)
		logger.Error(err)
		return err
	}
	return err
}

func (s *SaverService) DeleteEvent(ctx context.Context, tenantID, aggregateType, aggregateID, eventID string) error {
	ctx, endSpan := metrics.StartSpan(ctx, "DeletePatch (service)", map[string]interface{}{"tenantID": tenantID, "aggregateType": aggregateType, "aggregateID": aggregateID, "eventID": eventID})
	defer endSpan()

	if s.registries.AggregateRegistry.Options(aggregateType).DeleteStrategy == event.NoDelete {
		return fmt.Errorf("delete event is not allowed for aggregate %s of type %s", aggregateID, aggregateType)
	}

	errTX := s.transactor.WithinTX(ctx, func(txCtx context.Context) (err error) {
		evts, _, err := s.aggregateRepository.GetAggregatesEvents(txCtx, tenantID, event.PageDTO{
			PageSize:   1,
			SortFields: nil,
			SearchFields: []event.SearchField{
				{
					Name:     event.SearchAggregateID,
					Value:    aggregateID,
					Operator: event.SearchEqual,
				},
				{
					Name:     event.SearchAggregateType,
					Value:    aggregateType,
					Operator: event.SearchEqual,
				},
				{
					Name:     event.SearchAggregateEventID,
					Value:    eventID,
					Operator: event.SearchEqual,
				},
			},
			Values:     nil,
			IsBackward: false,
		})
		if err != nil {
			return fmt.Errorf("error retrieving event %s for aggregate %s of type %s for tenant %s:%w ", eventID, aggregateID, aggregateType, tenantID, err)
		}
		if len(evts) == 0 {
			return fmt.Errorf("could not find event %s for aggregate %s of type %s for tenant %s:%w ", eventID, aggregateID, aggregateType, tenantID, err)
		}

		evt := evts[0]
		err = s.deleteEvent(txCtx, shared.AggregateID{
			TenantID:      tenantID,
			AggregateType: aggregateType,
			AggregateID:   aggregateID,
		}, evt)

		if err != nil {
			return fmt.Errorf("error deleting event %s for aggregate %s of type %s for tenant %s: %w", eventID, aggregateID, aggregateType, tenantID, err)
		}

		return err
	})
	if errTX != nil {
		return fmt.Errorf("delete event failed: %w", errTX)
	}

	return errTX
}

func (s *SaverService) deleteEvent(txCtx context.Context, id shared.AggregateID, evt event.PersistenceEvent) error {
	eventualConsistentProjIDs, consistentProjIDs, err := s.projectionRepository.GetProjectionIDsForEventTypes(id.TenantID, evt.Type)
	if err != nil {
		return fmt.Errorf("could not find projection for the event typ %s :%w", evt.Type, err)
	}

	allProjections := append(consistentProjIDs, eventualConsistentProjIDs...)

	// Lock aggregates BEFORE changing the event
	if err = s.aggregateRepository.Lock(txCtx, id); err != nil {
		return fmt.Errorf("locking of aggregates failed: %w", err)
	}

	// Lock all projections
	if err = s.projectionRepository.Lock(txCtx, allProjections...); err != nil {
		return fmt.Errorf("locking of projections failed: %w", err)
	}

	defer func() {
		if errUnLock := s.aggregateRepository.UnLock(txCtx, id); errUnLock != nil {
			logger.Error(fmt.Errorf("unlocking of aggregates failed: %w", errUnLock))
		}

		if errUnLockProj := s.projectionRepository.UnLock(txCtx, allProjections...); errUnLockProj != nil {
			logger.Error(fmt.Errorf("unlocking of projections failed: %w", errUnLockProj))
		}

	}()

	// delete event from aggregate stream
	var deletedEvt event.PersistenceEvent
	deletedEvt, err = s.deleteEventFromAggregate(txCtx, id, evt)
	if err != nil {
		return err
	}

	// delete event from projections
	var streamsToSendToProjections []projection.Stream
	streamsToSendToProjections, err = s.deleteEventFromProjections(txCtx, id, deletedEvt, allProjections)

	// send delete event to projections
	if err = s.cmdBus.Execute(txCtx, commands.CmdDeleteEvent(streamsToSendToProjections)); err != nil {
		return fmt.Errorf("execute delete for projections failed: %w", err)
	}

	return nil

}

func (s *SaverService) deleteEventFromAggregate(txCtx context.Context, id shared.AggregateID, evt event.PersistenceEvent) (event.PersistenceEvent, error) {
	stream, err := s.aggregateRepository.GetAggregate(txCtx, id)
	if err != nil {
		return event.PersistenceEvent{}, fmt.Errorf("get aggregate failed: %w", err)
	}

	var deletedEvt event.PersistenceEvent
	deletedEvt, err = stream.DeleteEvent(evt, event.GetUserID(txCtx))
	if err != nil {
		return event.PersistenceEvent{}, err
	}

	if err = s.aggregateRepository.DeleteEvent(txCtx, id, deletedEvt); err != nil {
		return event.PersistenceEvent{}, fmt.Errorf("deleted event from aggreagte failed: %w", err)
	}

	if evt.Class == event.CloseStreamEvent {
		if err = s.aggregateRepository.UndoDeleteAggregate(txCtx, id); err != nil {
			return event.PersistenceEvent{}, fmt.Errorf("undo delete aggregate failed: %w", err)
		}
	}
	return deletedEvt, nil

}

func (s *SaverService) deleteEventFromProjections(txCtx context.Context, id shared.AggregateID, evt event.PersistenceEvent, projections []shared.ProjectionID) ([]projection.Stream, error) {
	// in eventual consistent projections the event is not yet sent to the projection and can simply delete from the queue
	alreadyDeletedProjections, err := s.deleteEventFromQueue(txCtx, id, evt.ID)
	if err != nil {
		return nil, fmt.Errorf("delete event from queue failed: %w", err)
	}

	// get all projections that are affected by the delete event (and not in the queue anymore)
	projectionsToDelete, _ := lo.Difference(projections, alreadyDeletedProjections)

	// prepare the projection stream and add the delete event
	var projectionsStream []projection.Stream
	projectionsStream, err = s.projectionRepository.GetProjections(txCtx, projectionsToDelete...)
	if err != nil {
		return nil, err
	}

	var streamWithDelete []projection.Stream
	for _, stream := range projectionsStream {
		if err = stream.AddEvents(evt); err != nil {
			return nil, fmt.Errorf("add event to projection failed: %w", err)
		}

		streamWithDelete = append(streamWithDelete, stream)
	}
	return streamWithDelete, nil
}

func (s *SaverService) deleteEventFromQueue(txCtx context.Context, id shared.AggregateID, eventID string) ([]shared.ProjectionID, error) {
	projs, err := s.projectionRepository.GetProjectionsWithEventInQueue(txCtx, id, eventID)
	if err != nil {
		return nil, fmt.Errorf("check if event is in projection queue failed: %w", err)
	}

	if err = s.projectionRepository.DeleteEventFromQueue(txCtx, eventID, projs...); err != nil {
		return nil, fmt.Errorf("delete event from projection queue failed: %w", err)
	}

	return projs, nil
}
