package event

import (
	"encoding/json"
	"fmt"
	"reflect"
	"sync"
	"time"
)

var (
	eventDataFactoriesMu   sync.RWMutex
	eventRegistry          = make(map[string]any)
	eventAggregateRelation = make(map[string]string)
)

type Class string

const (
	CreateStreamEvent  Class = "create stream event" // must be unique
	CloseStreamEvent   Class = "close stream event"  // must be unique
	InstantEvent       Class = "instant event"
	HistoricalPatch    Class = "historical patch"
	FuturePatch        Class = "future patch"
	DeletePatch        Class = "delete patch"
	SnapShot           Class = "snapshot"
	HistoricalSnapShot Class = "historical snapshot"
)

type IEvent interface {
	isEvent()
	setUserID(id string)
	setFromPersistenceEvent(evt PersistenceEvent)
	GetMigration() bool
	GetClass() Class
	GetAggregateID() string
	GetTenantID() string
	GetTransactionTime() time.Time
	GetValidTime() time.Time
	GetEventID() string
}

type IEvents struct {
	Events  []IEvent
	Version int
}

func NewCreateEvent(aggregateID string, tenantID string) Event {
	return Event{
		AggregateID:   aggregateID,
		TenantID:      tenantID,
		Class:         CreateStreamEvent, //init version is represented as snapshot
		FromMigration: false,
	}
}

func NewCreateEventWithValidTime(aggregateID string, tenantID string, validTimeStamp time.Time) Event {
	return Event{
		AggregateID:   aggregateID,
		TenantID:      tenantID,
		ValidTime:     validTimeStamp,
		Class:         CreateStreamEvent, //init version is represented as snapshot
		FromMigration: false,
	}
}

func NewCloseEvent(aggregateID string, tenantID string) Event {
	return Event{
		AggregateID:   aggregateID,
		TenantID:      tenantID,
		Class:         CloseStreamEvent, //init version is represented as snapshot
		FromMigration: false,
	}
}

func NewCloseEventWithValidTime(aggregateID string, tenantID string, validTimestamp time.Time) Event {
	return Event{
		AggregateID:   aggregateID,
		TenantID:      tenantID,
		ValidTime:     validTimestamp,
		Class:         CloseStreamEvent,
		FromMigration: false,
	}
}
func NewEvent(aggregateID string, tenantID string) Event {
	return Event{
		AggregateID:   aggregateID,
		TenantID:      tenantID,
		Class:         InstantEvent,
		FromMigration: false,
	}
}

func NewEventWithValidTimestamp(aggregateID string, tenantID string, validTimestamp time.Time) Event {
	eventClass := HistoricalPatch
	if validTimestamp.After(time.Now()) {
		eventClass = FuturePatch
	}
	return Event{
		AggregateID:   aggregateID,
		TenantID:      tenantID,
		ValidTime:     validTimestamp,
		Class:         eventClass,
		FromMigration: false,
	}
}

// NewEventWithOptionalValidTime Instead of passing time.now() as the start time for unset timestamps, we leave it to the event store to do this.
// This way we avoid creating unnecessary historical patches that would occur because the valid time and transaction time would differ.
func NewEventWithOptionalValidTime(aggregateID, tenantID string, time time.Time) Event {
	if time.IsZero() {
		return NewEvent(aggregateID, tenantID)
	} else {
		return NewEventWithValidTimestamp(aggregateID, tenantID, time)
	}
}

func NewSnapShot(aggregateID string, tenantID string) Event {
	return Event{
		AggregateID:   aggregateID,
		TenantID:      tenantID,
		Class:         SnapShot,
		FromMigration: false,
	}
}

func NewSnapShotWithValidTime(aggregateID string, tenantID string, validTimestamp time.Time) Event {
	return Event{
		AggregateID:   aggregateID,
		TenantID:      tenantID,
		ValidTime:     validTimestamp,
		Class:         HistoricalSnapShot,
		FromMigration: false,
	}
}

// NewMigrationEvent ATTENTION: This function should only be used for FromMigration cases or test cases. By using the function
// it is possible to create "historical event streams" which are BEFORE the currently stored event streams. Thus, should
// only be used for a FromMigration, i.e. unloading an old event store and filling a new empty event store.
func NewMigrationEvent(aggregateID string, tenantID string, validTimestamp, transactionTime time.Time, class Class) Event {
	return Event{
		AggregateID:     aggregateID,
		TenantID:        tenantID,
		TransactionTime: transactionTime,
		ValidTime:       validTimestamp,
		Class:           class,
		FromMigration:   true,
	}
}

type Deleted struct {
	DeletedAt time.Time
	DeletedBy string
}

func (d *Deleted) IsDeleted() bool {
	return d != nil && !d.DeletedAt.IsZero()
}

type Event struct {
	EventID         string    `json:"-"`
	AggregateID     string    `json:"-"`
	TenantID        string    `json:"-"`
	TransactionTime time.Time `json:"-"`
	ValidTime       time.Time `json:"-"`
	UserID          string
	Deleted         *Deleted `json:"Deleted,omitempty"`
	Class           Class    `json:"-"`
	FromMigration   bool     `json:"-"`
}

func (e *Event) isEvent() {}

func (e *Event) setUserID(id string) {
	e.UserID = id
}

func (e *Event) setFromPersistenceEvent(evt PersistenceEvent) {
	e.EventID = evt.ID
	e.AggregateID = evt.AggregateID
	e.TenantID = evt.TenantID
	e.TransactionTime = evt.TransactionTime
	e.ValidTime = evt.ValidTime
	e.Class = evt.Class
	e.FromMigration = evt.FromMigration
}

func (e *Event) GetMigration() bool {
	return e.FromMigration
}

func (e *Event) GetClass() Class {
	return e.Class
}

func (e *Event) GetAggregateID() string {
	return e.AggregateID
}

func (e *Event) GetTenantID() string {
	return e.TenantID
}

func (e *Event) GetTransactionTime() time.Time {
	return e.TransactionTime
}

func (e *Event) GetValidTime() time.Time {
	return e.ValidTime
}

func (e *Event) GetEventID() string {
	return e.EventID
}

func EventType(e any) string {
	t := reflect.TypeOf(e)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	return t.PkgPath() + "/" + t.Name()
}

func RegisterEvent(e any) {
	eventDataFactoriesMu.Lock()
	defer eventDataFactoriesMu.Unlock()

	eventRegistry[EventType(e)] = e
}

func RegisterEventAndAggregate(e any, aggregateType string) {
	eventDataFactoriesMu.Lock()
	defer eventDataFactoriesMu.Unlock()

	eventRegistry[EventType(e)] = e
	eventAggregateRelation[EventType(e)] = aggregateType
}

func GetAggregateForEvent(aggregateType string) string {
	eventDataFactoriesMu.RLock()
	defer eventDataFactoriesMu.RUnlock()
	return eventAggregateRelation[aggregateType]
}

func CreateEventForDeserialization(eventType string) (IEvent, error) {
	eventDataFactoriesMu.RLock()
	defer eventDataFactoriesMu.RUnlock()

	if eventTemplate, ok := eventRegistry[eventType]; ok {
		return reflect.New(reflect.TypeOf(eventTemplate)).Elem().Addr().Interface().(IEvent), nil
	}

	return nil, fmt.Errorf("event %q is not registered", eventType)
}

func SerializeEvent(evt IEvent) ([]byte, error) {
	return json.Marshal(evt)
}

func DeserializeEvent(persistenceEvent PersistenceEvent) (evt IEvent, err error) {
	if evt, err = CreateEventForDeserialization(persistenceEvent.Type); err != nil {
		return nil, err
	} else {
		err = json.Unmarshal(persistenceEvent.Data, &evt)
		if err != nil {
			return nil, err
		}
		evt.setFromPersistenceEvent(persistenceEvent)
		return evt, nil
	}
}
