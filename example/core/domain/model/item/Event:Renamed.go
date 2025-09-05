package item

import (
	"fmt"
	"github.com/global-soft-ba/go-eventstore"
	"github.com/global-soft-ba/go-eventstore/instrumentation/port/logger"
	"reflect"
	"time"
)

func init() {
	eventHandlers[reflect.TypeOf(&Renamed{})] = handleItemRenamed
	event.RegisterEventAndAggregate(Renamed{}, reflect.TypeOf(Item{}).Name())
}

type Renamed struct {
	event.Event
	Name string `json:"name"`
}

func handleItemRenamed(i Item, e event.IEvent) (Item, error) {
	evt := e.(*Renamed)
	i.name = evt.Name

	logger.Info("Rename item mutation applied successfully for tenantID: %q, itemID: %q", i.GetTenantID(), i.GetID())
	return i, nil
}

func (i Item) Rename(name string, start time.Time) (Item, error) {
	if !start.IsZero() && !start.After(time.Now()) {
		return Item{}, fmt.Errorf("could not rename item %q: provided time value must be in the future or a zero time instant", i.GetID())
	}
	if i.name == name {
		return i, nil
	}

	logger.Info("Requesting rename item mutation for tenantID: %q, itemID: %q", i.GetTenantID(), i.GetID())
	return i.ApplyEvent(&Renamed{
		Event: event.NewEventWithOptionalValidTime(i.GetID(), i.GetTenantID(), start),
		Name:  name,
	})
}
