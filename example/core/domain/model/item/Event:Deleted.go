package item

import (
	"github.com/global-soft-ba/go-eventstore"
	"github.com/global-soft-ba/go-eventstore/instrumentation/port/logger"
	"reflect"
)

func init() {
	eventHandlers[reflect.TypeOf(&Deleted{})] = handleItemDeleted
	event.RegisterEventAndAggregate(Deleted{}, reflect.TypeOf(Item{}).Name())
}

type Deleted struct {
	event.Event
}

func handleItemDeleted(i Item, e event.IEvent) (Item, error) {
	i.deleted = true

	logger.Info("Delete item mutation applied successfully for tenantID: %q, itemID: %q", i.GetTenantID(), i.GetID())
	return i, nil
}

func (i Item) Delete() (Item, error) {
	logger.Info("Requesting delete item mutation for tenantID: %q, itemID: %q", i.GetTenantID(), i.GetID())
	return i.ApplyEvent(&Deleted{
		Event: event.NewCloseEvent(i.GetID(), i.GetTenantID()),
	})
}
