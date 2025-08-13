package AggregateRegistry

import (
	"fmt"
	"github.com/global-soft-ba/go-eventstore"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/domain/model/aggregate"
	kvTable2 "github.com/global-soft-ba/go-eventstore/eventstore/core/shared/kvTable"
	"github.com/global-soft-ba/go-eventstore/instrumentation/port/logger"
)

var defaultOptions = aggregate.Options{
	ConcurrentModificationStrategy: event.Fail,
	EphemeralEvents:                nil,
	DeleteStrategy:                 event.NoDelete,
}

func NewRegistry() *Registry {
	return &Registry{
		options: kvTable2.NewKeyValuesTable[aggregate.Options](),
	}
}

type Registry struct {
	options kvTable2.IKVTable[aggregate.Options] // key[aggregateType]
}

func (r Registry) currentOrDefaultOptions(aggregateType string) aggregate.Options {
	currOptions, err := kvTable2.GetFirst(r.options, kvTable2.NewKey(aggregateType))
	if err != nil {
		if !kvTable2.IsKeyNotFound(err) {
			logger.Error(fmt.Errorf("send default options for aggregate type %s due to failed repository retrieval: %w", aggregateType, err))
			return defaultOptions
		}
		return defaultOptions
	}
	return currOptions
}

func (r Registry) Options(aggregateType string) aggregate.Options {
	return r.currentOrDefaultOptions(aggregateType)
}

func (r Registry) SetConcurrentModificationStrategy(aggregateType string, strategy event.ConcurrentModificationStrategy) error {
	currOptions := r.currentOrDefaultOptions(aggregateType)
	currOptions.ConcurrentModificationStrategy = strategy
	if err := kvTable2.Set(r.options, kvTable2.NewKey(aggregateType), currOptions); err != nil {
		return fmt.Errorf("could not store concurrent modification options: %w", err)
	}
	return nil
}

func (r Registry) SetEphemeralEventTypes(aggregateType string, eventTypes []string) error {
	currOptions := r.currentOrDefaultOptions(aggregateType)

	if currOptions.EphemeralEvents == nil {
		currOptions.EphemeralEvents = make(map[string]bool)
	}

	for _, eventType := range eventTypes {
		currOptions.EphemeralEvents[eventType] = true
	}
	if err := kvTable2.Set(r.options, kvTable2.NewKey(aggregateType), currOptions); err != nil {
		return fmt.Errorf("could not store ephemeral eventypes options: %w", err)
	}
	return nil
}

func (r Registry) SetDeleteStrategy(aggregateType string, strategy event.DeleteStrategy) error {
	currOptions := r.currentOrDefaultOptions(aggregateType)
	currOptions.DeleteStrategy = strategy
	if err := kvTable2.Set(r.options, kvTable2.NewKey(aggregateType), currOptions); err != nil {
		return fmt.Errorf("could not store delete strategy options: %w", err)
	}
	return nil
}
