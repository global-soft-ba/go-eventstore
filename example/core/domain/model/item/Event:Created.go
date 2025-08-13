package item

import (
	"fmt"
	"github.com/global-soft-ba/go-eventstore"
	"github.com/global-soft-ba/go-eventstore/instrumentation/port/logger"
	"reflect"
)

func init() {
	eventHandlers[reflect.TypeOf(&Created{})] = handleItemCreated
	event.RegisterEventAndAggregate(Created{}, reflect.TypeOf(Item{}).Name())
}

type Created struct {
	event.Event
}

func handleItemCreated(i Item, e event.IEvent) (Item, error) {
	evt := e.(*Created)
	if i.created {
		return Item{}, fmt.Errorf("could not create item with id %q: item was already created", evt.GetAggregateID())
	}
	i.id = evt.GetAggregateID()
	i.tenantID = evt.GetTenantID()
	i.created = true

	logger.Info("Create item mutation applied successfully for tenantID: %q, itemID: %q", i.tenantID, i.id)
	return i, nil
}

func CreateItem(itemID, tenantID string) (Item, error) {
	logger.Info("Requesting create item mutation for tenantID: %q, itemID: %q", tenantID, itemID)
	return Item{}.ApplyEvent(&Created{
		Event: event.NewCreateEvent(itemID, tenantID),
	})
}
