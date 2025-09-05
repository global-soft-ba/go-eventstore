package services

import (
	"context"
	"fmt"
	"github.com/global-soft-ba/go-eventstore"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/application/repository"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/domain/service"
	transactor2 "github.com/global-soft-ba/go-eventstore/eventstore/core/port/persistence/transactor"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/shared"
	"time"
)

func NewLoaderService(aggRepro repository.AggregateRepositoryInterface, transactor transactor2.Port) LoaderService {
	return LoaderService{
		domain:              service.DomainService{},
		aggregateRepository: aggRepro,
		transactor:          transactor,
	}
}

type LoaderService struct {
	domain              service.DomainService
	aggregateRepository repository.AggregateRepositoryInterface

	transactor transactor2.Port
}

func (l *LoaderService) LoadAsAt(ctx context.Context, tenantID, aggregateType, aggregateID string, projectionTime time.Time) (eventStream []event.PersistenceEvent, version int, err error) {
	var events event.PersistenceEvents
	errTrans := l.transactor.WithoutTX(ctx, func(txCtx context.Context) error {
		events, err = l.aggregateRepository.LoadAsAt(txCtx, shared.NewAggregateID(tenantID, aggregateType, aggregateID), projectionTime)
		return err
	})

	if errTrans != nil {
		return nil, 0, fmt.Errorf("LoadAsAt failed for tenant %q and aggregate %q:%w", tenantID, aggregateID, errTrans)
	}

	return events.Events, events.Version, err
}

func (l *LoaderService) LoadAsOf(ctx context.Context, tenantID, aggregateType, aggregateID string, projectionTime time.Time) (eventStream []event.PersistenceEvent, version int, err error) {
	var events event.PersistenceEvents
	errTrans := l.transactor.WithoutTX(ctx, func(txCtx context.Context) error {
		events, err = l.aggregateRepository.LoadAsOf(txCtx, shared.NewAggregateID(tenantID, aggregateType, aggregateID), projectionTime)
		return err
	})

	if errTrans != nil {
		return nil, 0, fmt.Errorf("LoadAsOf failed for tenant %q and aggregate %q:%w", tenantID, aggregateID, errTrans)
	}

	return events.Events, events.Version, err
}

func (l *LoaderService) LoadAsOfTill(ctx context.Context, tenantID, aggregateType, aggregateID string, projectionTime, reportTime time.Time) (eventStream []event.PersistenceEvent, version int, err error) {
	var events event.PersistenceEvents
	errTrans := l.transactor.WithoutTX(ctx, func(txCtx context.Context) error {
		events, err = l.aggregateRepository.LoadAsOfTill(txCtx, shared.NewAggregateID(tenantID, aggregateType, aggregateID), projectionTime, reportTime)
		return err
	})

	if errTrans != nil {
		return nil, 0, fmt.Errorf("LoadAsOfTill failed for tenant %q and aggregate %q:%w", tenantID, aggregateID, errTrans)
	}

	return events.Events, events.Version, err
}

func (l *LoaderService) LoadAllOfAggregateAsAt(ctx context.Context, tenantID, aggregateType string, projectionTime time.Time) (eventStreams []event.PersistenceEvents, err error) {
	errTrans := l.transactor.WithoutTX(ctx, func(txCtx context.Context) error {
		eventStreams, err = l.aggregateRepository.LoadAllOfAggregateAsAt(txCtx, tenantID, aggregateType, projectionTime)
		return err
	})

	if errTrans != nil {
		return nil, fmt.Errorf("LoadAllOfAggregateTypeAsAt failed for tenant %q and type %q:%w", tenantID, aggregateType, errTrans)
	}

	return eventStreams, err
}

func (l *LoaderService) LoadAllOfAggregateAsOf(ctx context.Context, tenantID, aggregateType string, projectionTime time.Time) (eventStreams []event.PersistenceEvents, err error) {
	errTrans := l.transactor.WithoutTX(ctx, func(txCtx context.Context) error {
		eventStreams, err = l.aggregateRepository.LoadAllOfAggregateAsOf(txCtx, tenantID, aggregateType, projectionTime)
		return err
	})

	if errTrans != nil {
		return nil, fmt.Errorf("LoadAllOfAggregateTypeAsOf failed for tenant %q and type %q:%w", tenantID, aggregateType, errTrans)
	}

	return eventStreams, err
}

func (l *LoaderService) LoadAllOfAggregateAsOfTill(ctx context.Context, tenantID, aggregateType string, projectionTime, reportTime time.Time) (eventStreams []event.PersistenceEvents, err error) {
	errTrans := l.transactor.WithoutTX(ctx, func(txCtx context.Context) error {
		eventStreams, err = l.aggregateRepository.LoadAllOfAggregateAsOfTill(txCtx, tenantID, aggregateType, projectionTime, reportTime)
		return err
	})

	if errTrans != nil {
		return nil, fmt.Errorf("LoadAllOfAggregateTypeAsOfTill failed for tenant %q and type %q:%w", tenantID, aggregateType, errTrans)
	}

	return eventStreams, err
}

func (l *LoaderService) LoadAllAsAt(ctx context.Context, tenantID string, projectionTime time.Time) (eventStreams []event.PersistenceEvents, err error) {
	errTrans := l.transactor.WithoutTX(ctx, func(txCtx context.Context) error {
		eventStreams, err = l.aggregateRepository.LoadAllAsAt(txCtx, tenantID, projectionTime)
		return err
	})

	if errTrans != nil {
		return nil, fmt.Errorf("LoadAllAsAt failed for tenant %q:%w", tenantID, errTrans)
	}

	return eventStreams, err
}

func (l *LoaderService) LoadAllAsOf(ctx context.Context, tenantID string, projectionTime time.Time) (eventStreams []event.PersistenceEvents, err error) {
	errTrans := l.transactor.WithoutTX(ctx, func(txCtx context.Context) error {
		eventStreams, err = l.aggregateRepository.LoadAllAsOf(txCtx, tenantID, projectionTime)
		return err
	})

	if errTrans != nil {
		return nil, fmt.Errorf("LoadAllAsOf failed for tenant %q:%w", tenantID, errTrans)
	}

	return eventStreams, err
}

func (l *LoaderService) LoadAllAsOfTill(ctx context.Context, tenantID string, projectionTime time.Time, reportTime time.Time) (eventStreams []event.PersistenceEvents, err error) {
	errTrans := l.transactor.WithoutTX(ctx, func(txCtx context.Context) error {
		eventStreams, err = l.aggregateRepository.LoadAllAsOfTill(txCtx, tenantID, projectionTime, reportTime)
		return err
	})

	if errTrans != nil {
		return nil, fmt.Errorf("LoadAllAsOfTill failed for tenant %q:%w", tenantID, errTrans)
	}

	return eventStreams, err
}

func (l *LoaderService) GetAggregateStates(ctx context.Context, tenantID, aggregateType, aggregateID string) (state event.AggregateState, err error) {
	errTrans := l.transactor.WithoutTX(ctx, func(txCtx context.Context) error {
		state, err = l.aggregateRepository.GetAggregateState(txCtx, tenantID, aggregateType, aggregateID)
		return err
	})

	if errTrans != nil {
		return event.AggregateState{}, fmt.Errorf("GetAggregateState failed for aggregate %q of tenant %q:%w", aggregateID, tenantID, errTrans)
	}

	return state, err
}

func (l *LoaderService) GetAllAggregateStatesForAggregateType(ctx context.Context, tenantID string, aggregateType string) (states []event.AggregateState, err error) {
	errTrans := l.transactor.WithoutTX(ctx, func(txCtx context.Context) error {
		states, err = l.aggregateRepository.GetAggregateStatesForAggregateType(txCtx, tenantID, aggregateType)
		return err
	})

	if errTrans != nil {
		return nil, fmt.Errorf("GetAggregateStatesForAggregateType failed for aggregate type %q of tenant %q:%w", aggregateType, tenantID, errTrans)
	}

	return states, err
}

func (l *LoaderService) GetAllAggregateStatesForAggregateTypeTill(ctx context.Context, tenantID string, aggregateType string, until time.Time) (states []event.AggregateState, err error) {
	errTrans := l.transactor.WithoutTX(ctx, func(txCtx context.Context) error {
		states, err = l.aggregateRepository.GetAggregateStatesForAggregateTypeTill(txCtx, tenantID, aggregateType, until)
		return err
	})

	if errTrans != nil {
		return nil, fmt.Errorf("GetAggregateStatesForAggregateTypeTill failed for aggregate type %q of tenant %q and time %s:%w", aggregateType, tenantID, until, errTrans)
	}

	return states, err
}

func (l *LoaderService) GetPatchFreePeriodsForInterval(ctx context.Context, tenantID, aggregateType, aggregateID string, start time.Time, end time.Time) (intervals []event.TimeInterval, err error) {
	errTrans := l.transactor.WithoutTX(ctx, func(txCtx context.Context) error {
		intervals, err = l.aggregateRepository.GetPatchFreePeriodsForInterval(txCtx, tenantID, aggregateType, aggregateID, start, end)
		return err
	})

	if errTrans != nil {
		return nil, fmt.Errorf("GetPatchFreePeriodsForInterval failed for aggregate %q of aggregate type %q from tenant %q:%w", aggregateID, aggregateType, tenantID, errTrans)
	}

	return intervals, err
}

func (l *LoaderService) GetAggregatesEvents(ctx context.Context, tenantID string, page event.PageDTO) (events []event.PersistenceEvent, pages event.PagesDTO, err error) {
	errTrans := l.transactor.WithoutTX(ctx, func(txCtx context.Context) error {
		events, pages, err = l.aggregateRepository.GetAggregatesEvents(txCtx, tenantID, page)
		return err
	})

	if errTrans != nil {
		return nil, event.PagesDTO{}, fmt.Errorf("GetAggregatesEvents failed for tenant %q and page %v:%w", tenantID, page, errTrans)
	}

	return events, pages, err
}
