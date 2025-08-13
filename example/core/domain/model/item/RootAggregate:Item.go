package item

import (
	"fmt"
	"github.com/global-soft-ba/go-eventstore"
	"reflect"
)

var eventHandlers = make(map[reflect.Type]func(i Item, evt event.IEvent) (Item, error))

func LoadFromEventStream(version int, eventStream ...event.IEvent) (i Item, err error) {
	for _, evt := range eventStream {
		i, err = i.mutateInternalState(evt)
		if err != nil {
			return Item{}, err
		}
	}
	i.version = version
	return i, nil
}

type Item struct {
	id       string
	tenantID string
	name     string

	created bool
	deleted bool
	changes []event.IEvent
	version int
}

func (i Item) ApplyEvent(evt event.IEvent) (Item, error) {
	i, err := i.mutateInternalState(evt)
	if err != nil {
		return Item{}, err
	}
	i.changes = append(i.changes, evt)
	return i, nil
}

func (i Item) mutateInternalState(evt event.IEvent) (Item, error) {
	eventHandler, ok := eventHandlers[reflect.TypeOf(evt)]
	if !ok {
		return Item{}, fmt.Errorf("no event handler found for event %v", evt)
	}
	return eventHandler(i, evt)
}

func (i Item) GetID() string {
	return i.id
}

func (i Item) GetTenantID() string {
	return i.tenantID
}

func (i Item) GetName() string {
	return i.name
}

func (i Item) IsDeleted() bool {
	return i.deleted
}

func (i Item) GetUnsavedChanges() []event.IEvent {
	return i.changes
}

func (i Item) GetVersion() int {
	return i.version
}
