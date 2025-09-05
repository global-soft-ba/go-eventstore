package tests

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/global-soft-ba/go-eventstore"
	"go.uber.org/atomic"
	"reflect"
	"sync"
	"time"
)

type testType string

const UnitTest testType = "unit"
const IntegrationTest testType = "integration"

func init() {
	event.RegisterEventAndAggregate(forTestEvent{}, reflect.TypeOf(forTestConcreteAggregate{}).Name())
	event.RegisterEventAndAggregate(forTestEvent2{}, reflect.TypeOf(forTestConcreteAggregate2{}).Name())
	event.RegisterEventAndAggregate(forTestEvent3{}, reflect.TypeOf(forTestConcreteAggregate{}).Name())
}

func clean() {

}

func cleanRegistries() {
}

type forTestConcreteAggregate struct {
	id       string
	name     string
	version  int
	changes  []event.IEvent
	tenantId string
}

type forTestEvent struct {
	event.Event
}

func newForTestConcreteAggregate(id string, name string, version int, tenantId string, changes []event.IEvent) forTestConcreteAggregate {
	return forTestConcreteAggregate{
		id:       id,
		name:     name,
		version:  version,
		tenantId: tenantId,
		changes:  changes,
	}
}

func (a forTestConcreteAggregate) GetID() string {
	return a.id
}

func (a forTestConcreteAggregate) GetVersion() int {
	return a.version
}

func (a forTestConcreteAggregate) GetTenantID() string {
	return a.tenantId
}

func (a forTestConcreteAggregate) Name() string {
	return a.name
}

func (a forTestConcreteAggregate) GetUnsavedChanges() []event.IEvent {
	return a.changes
}

func (a forTestConcreteAggregate) LoadFromEventStream(version int, eventStream ...event.IEvent) (forTestConcreteAggregate, error) {
	for _, iEvent := range eventStream {
		a.id = iEvent.GetAggregateID()
		a.tenantId = iEvent.GetTenantID()
		break
	}
	a.version = version
	return a, nil
}

func (a forTestConcreteAggregate) ApplyEvent(evt event.IEvent) (forTestConcreteAggregate, error) {
	a.changes = append(a.changes, evt)
	return a, nil
}

type forTestConcreteAggregate2 struct {
	id       string
	version  int
	changes  []event.IEvent
	tenantId string
}

func (f forTestConcreteAggregate2) GetID() string {
	return f.id
}

func (f forTestConcreteAggregate2) GetTenantID() string {
	return f.tenantId
}

func (f forTestConcreteAggregate2) GetVersion() int {
	return f.version
}

func (f forTestConcreteAggregate2) GetUnsavedChanges() []event.IEvent {
	return f.changes
}

type forTestEvent2 struct {
	event.Event
}

type forTestEvent3 struct {
	event.Event
	AdditionalProperty string
}

func ForTestMakeCreateEvent(aggregateID, tenantID string, transactionTimestamp, validTimestamp time.Time) event.IEvent {
	e := event.NewMigrationEvent(aggregateID, tenantID, validTimestamp, transactionTimestamp, event.CreateStreamEvent)
	return &forTestEvent{Event: e}
}

func ForTestMakeCreateEventWithoutMigration(aggregateID, tenantID string, validTimestamp time.Time) event.IEvent {
	e := event.NewCreateEventWithValidTime(aggregateID, tenantID, validTimestamp)
	return &forTestEvent{Event: e}
}

func ForTestMakeCreateEvent3(aggregateID, tenantID string, transactionTimestamp, validTimestamp time.Time, additionalProperty string) event.IEvent {
	e := event.NewMigrationEvent(aggregateID, tenantID, validTimestamp, transactionTimestamp, event.CreateStreamEvent)
	return &forTestEvent3{Event: e, AdditionalProperty: additionalProperty}
}

func ForTestMakeCreateEventNow(aggregateID, tenantID string) event.IEvent {
	e := event.NewCreateEvent(aggregateID, tenantID)
	return &forTestEvent{Event: e}
}

func ForTestMakeCloseEvent(aggregateID, tenantID string, transactionTimestamp, validTimestamp time.Time) event.IEvent {
	e := event.NewMigrationEvent(aggregateID, tenantID, validTimestamp, transactionTimestamp, event.CloseStreamEvent)
	return &forTestEvent{Event: e}
}

func ForTestMakeCloseEventWithoutMigration(aggregateID, tenantID string, validTimestamp time.Time) event.IEvent {
	e := event.NewCloseEventWithValidTime(aggregateID, tenantID, validTimestamp)
	return &forTestEvent{Event: e}
}

func ForTestMakeEvent(aggregateID, tenantID string, transactionTimestamp, validTimestamp time.Time) event.IEvent {
	e := event.NewMigrationEvent(aggregateID, tenantID, validTimestamp, transactionTimestamp, event.InstantEvent)
	return &forTestEvent{Event: e}
}

func ForTestMakeEventWithValidTime(aggregateID, tenantID string, validTimestamp time.Time) event.IEvent {
	e := event.NewEventWithOptionalValidTime(aggregateID, tenantID, validTimestamp)
	return &forTestEvent{Event: e}
}

func ForTestMakeDeleteEvent(aggregateID, tenantID string, transactionTimestamp, validTimestamp time.Time) event.IEvent {
	e := event.NewMigrationEvent(aggregateID, tenantID, validTimestamp, transactionTimestamp, event.DeletePatch)
	return &forTestEvent{Event: e}
}

func ForTestMakeEvent3(aggregateID, tenantID string, transactionTimestamp, validTimestamp time.Time, additionalProperty string) event.IEvent {
	e := event.NewMigrationEvent(aggregateID, tenantID, validTimestamp, transactionTimestamp, event.InstantEvent)
	return &forTestEvent3{Event: e, AdditionalProperty: additionalProperty}
}

func ForTestMakeCreateEventWithUserID(aggregateID, tenantID, userID string, transactionTimestamp, validTimestamp time.Time, additionalProperty string) event.IEvent {
	e := event.NewMigrationEvent(aggregateID, tenantID, validTimestamp, transactionTimestamp, event.CreateStreamEvent)
	e.UserID = userID
	return &forTestEvent3{Event: e, AdditionalProperty: additionalProperty}
}

func ForTestMakeEventNow(aggregateID, tenantID string) event.IEvent {
	e := event.NewEvent(aggregateID, tenantID)
	return &forTestEvent{Event: e}
}

func ForTestMakePatchEvent(aggregateID, tenantID string, transactionTimestamp, validTimestamp time.Time) event.IEvent {
	e := event.NewEventWithValidTimestamp(aggregateID, tenantID, validTimestamp)
	e = event.NewMigrationEvent(aggregateID, tenantID, validTimestamp, transactionTimestamp, e.Class)
	return &forTestEvent{Event: e}
}

func ForTestMakeSnapshot(aggregateID, tenantID string, transactionTimestamp, validTimestamp time.Time) event.IEvent {
	e := event.NewMigrationEvent(aggregateID, tenantID, validTimestamp, transactionTimestamp, event.SnapShot)
	return &forTestEvent{Event: e}
}

func ForTestMakeHistoricalSnapshot(aggregateID, tenantID string, transactionTimestamp, validTimestamp time.Time) event.IEvent {
	e := event.NewMigrationEvent(aggregateID, tenantID, validTimestamp, transactionTimestamp, event.HistoricalSnapShot)
	return &forTestEvent{Event: e}
}

func ForTestMakeEvent2(aggregateID, tenantID string, transactionTimestamp, validTimestamp time.Time) event.IEvent {
	e := event.NewMigrationEvent(aggregateID, tenantID, validTimestamp, transactionTimestamp, event.InstantEvent)
	return &forTestEvent2{Event: e}
}

func ForTestMakeEvent2Now(aggregateID string, tenantID string) event.IEvent {
	e := event.NewEvent(aggregateID, tenantID)
	return &forTestEvent2{Event: e}
}

func ForTestMakePatchEvent2(aggregateID, tenantID string, transactionTimestamp, validTimestamp time.Time) event.IEvent {
	e := event.NewEventWithValidTimestamp(aggregateID, tenantID, validTimestamp)
	e = event.NewMigrationEvent(aggregateID, tenantID, validTimestamp, transactionTimestamp, e.Class)
	return &forTestEvent2{Event: e}
}

func ForTestMakeCreateEvent2(aggregateID, tenantID string, transactionTimestamp, validTimestamp time.Time) event.IEvent {
	e := event.NewMigrationEvent(aggregateID, tenantID, validTimestamp, transactionTimestamp, event.CreateStreamEvent)
	return &forTestEvent2{Event: e}
}

func ForTestMakeSnapshot2(aggregateID, tenantID string, transactionTimestamp, validTimestamp time.Time) event.IEvent {
	e := event.NewMigrationEvent(aggregateID, tenantID, validTimestamp, transactionTimestamp, event.SnapShot)
	return &forTestEvent2{Event: e}
}

func ForTestEvent0Legacy() event.PersistenceEvent {
	return event.PersistenceEvent{
		ID:              "0",
		AggregateID:     "1",
		TenantID:        "0000-0000-0000",
		AggregateType:   "forTestConcreteAggregate",
		Version:         1,
		Type:            "github.com/global-soft-ba/go-eventstore/tests/forTestEvent3",
		TransactionTime: time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC),
		ValidTime:       time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC),
		Data:            json.RawMessage(`{"AggregateID":"1","TenantID":"0000-0000-0000","TransactionTime":"2020-12-31T23:59:59.999999999Z","ValidTime":"2020-12-31T23:59:59.999999999Z","UserID":"","Class":"create stream event","FromMigration":true,"AdditionalProperty":"foobar"}`),
		Class:           event.CreateStreamEvent,
		FromMigration:   true,
	}
}

func ForTestEvent0() event.PersistenceEvent {
	return event.PersistenceEvent{
		ID:              "0",
		AggregateID:     "1",
		TenantID:        "0000-0000-0000",
		AggregateType:   "forTestConcreteAggregate",
		Version:         1,
		Type:            "github.com/global-soft-ba/go-eventstore/tests/forTestEvent",
		TransactionTime: time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC),
		ValidTime:       time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC),
		Data:            json.RawMessage(`{"UserID":""}`),
		Class:           event.CreateStreamEvent,
		FromMigration:   true,
	}
}

func ForTestEvent1() event.PersistenceEvent {
	return event.PersistenceEvent{
		ID:              "1",
		AggregateID:     "1",
		TenantID:        "0000-0000-0000",
		AggregateType:   "forTestConcreteAggregate",
		Version:         2,
		Type:            "github.com/global-soft-ba/go-eventstore/tests/forTestEvent",
		TransactionTime: time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC),
		ValidTime:       time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC),
		Data:            json.RawMessage(`{"UserID":""}`),
		Class:           event.InstantEvent,
		FromMigration:   true,
	}
}

func ForTestEvent2() event.PersistenceEvent {
	return event.PersistenceEvent{
		ID:              "2",
		AggregateID:     "1",
		TenantID:        "0000-0000-0000",
		AggregateType:   "forTestConcreteAggregate",
		Version:         3,
		Type:            "github.com/global-soft-ba/go-eventstore/tests/forTestEvent",
		TransactionTime: time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC),
		ValidTime:       time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC),
		Data:            json.RawMessage(`{"UserID":""}`),
		Class:           event.InstantEvent,
		FromMigration:   true,
	}
}

func ForTestEvent2Snapshot() event.PersistenceEvent {
	return event.PersistenceEvent{
		ID:              "s2",
		AggregateID:     "1",
		TenantID:        "0000-0000-0000",
		AggregateType:   "forTestConcreteAggregate",
		Version:         3,
		Type:            "github.com/global-soft-ba/go-eventstore/tests/forTestEvent",
		TransactionTime: time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC),
		ValidTime:       time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC),
		Data:            json.RawMessage(`{"UserID":""}`),
		Class:           event.SnapShot,
		FromMigration:   true,
	}
}

func ForTestEvent3() event.PersistenceEvent {
	return event.PersistenceEvent{
		ID:              "3",
		AggregateID:     "1",
		TenantID:        "0000-0000-0000",
		AggregateType:   "forTestConcreteAggregate",
		Version:         4,
		Type:            "github.com/global-soft-ba/go-eventstore/tests/forTestEvent",
		TransactionTime: time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC),
		ValidTime:       time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC),
		Data:            json.RawMessage(`{"UserID":""}`),
		Class:           event.InstantEvent,
		FromMigration:   true,
	}
}

func ForTestEvent4() event.PersistenceEvent {
	return event.PersistenceEvent{
		ID:              "4",
		AggregateID:     "1",
		TenantID:        "0000-0000-0000",
		AggregateType:   "forTestConcreteAggregate",
		Version:         5,
		Type:            "github.com/global-soft-ba/go-eventstore/tests/forTestEvent",
		TransactionTime: time.Date(2021, 1, 1, 1, 1, 1, 4, time.UTC),
		ValidTime:       time.Date(2021, 1, 1, 1, 1, 1, 4, time.UTC),
		Data:            json.RawMessage(`{"UserID":""}`),
		Class:           event.InstantEvent,
		FromMigration:   true,
	}
}

func ForTestEvent5() event.PersistenceEvent {
	return event.PersistenceEvent{
		ID:              "5",
		AggregateID:     "1",
		TenantID:        "0000-0000-0000",
		AggregateType:   "forTestConcreteAggregate",
		Version:         6,
		Type:            "github.com/global-soft-ba/go-eventstore/tests/forTestEvent",
		TransactionTime: time.Date(2021, 1, 1, 1, 1, 1, 5, time.UTC),
		ValidTime:       time.Date(2021, 1, 1, 1, 1, 1, 5, time.UTC),
		Data:            json.RawMessage(`{"UserID":""}`),
		Class:           event.InstantEvent,
		FromMigration:   true,
	}
}

func ForTestEvent5b() event.PersistenceEvent {
	return event.PersistenceEvent{
		ID:              "5",
		AggregateID:     "1",
		TenantID:        "0000-0000-0000",
		AggregateType:   "forTestConcreteAggregate",
		Version:         2,
		Type:            "github.com/global-soft-ba/go-eventstore/tests/forTestEvent",
		TransactionTime: time.Date(2021, 1, 1, 1, 1, 1, 5, time.UTC),
		ValidTime:       time.Date(2021, 1, 1, 1, 1, 1, 5, time.UTC),
		Data:            json.RawMessage(`{"UserID":""}`),
		Class:           event.InstantEvent,
		FromMigration:   true,
	}
}

func ForTestEvent1b() event.PersistenceEvent {
	return event.PersistenceEvent{
		ID:              "1b",
		AggregateID:     "2",
		TenantID:        "0000-0000-0000",
		AggregateType:   "forTestConcreteAggregate",
		Version:         1,
		Type:            "github.com/global-soft-ba/go-eventstore/tests/forTestEvent",
		TransactionTime: time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC),
		ValidTime:       time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC),
		Data:            json.RawMessage(`{"UserID":""}`),
		Class:           event.CreateStreamEvent,
		FromMigration:   true,
	}
}

func ForTestEventPatch() event.PersistenceEvent {
	return event.PersistenceEvent{
		ID:              "Patch",
		AggregateID:     "1",
		TenantID:        "0000-0000-0000",
		AggregateType:   "forTestConcreteAggregate",
		Version:         4,
		Type:            "github.com/global-soft-ba/go-eventstore/tests/forTestEvent",
		TransactionTime: time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC),
		ValidTime:       time.Date(2021, 1, 1, 1, 1, 1, 0, time.UTC),
		Data:            json.RawMessage(`{"UserID":""}`),
		Class:           event.HistoricalPatch,
		FromMigration:   true,
	}
}

func ForTestEventFuturePatch() event.PersistenceEvent {
	return event.PersistenceEvent{
		ID:              "FuturePatch",
		AggregateID:     "1",
		TenantID:        "0000-0000-0000",
		AggregateType:   "forTestConcreteAggregate",
		Version:         5,
		Type:            "github.com/global-soft-ba/go-eventstore/tests/forTestEvent",
		TransactionTime: time.Date(2021, 1, 1, 1, 1, 1, 4, time.UTC),
		ValidTime:       time.Date(2050, 1, 1, 1, 1, 1, 0, time.UTC),
		Data:            json.RawMessage(`{"UserID":""}`),
		Class:           event.FuturePatch,
		FromMigration:   true,
	}
}

func ForTestEvent0_Aggregate2() event.PersistenceEvent {
	return event.PersistenceEvent{
		ID:              "0_2",
		AggregateID:     "2",
		TenantID:        "0000-0000-0000",
		AggregateType:   "forTestConcreteAggregate",
		Version:         1,
		Type:            "github.com/global-soft-ba/go-eventstore/tests/forTestEvent",
		TransactionTime: time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC),
		ValidTime:       time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC),
		Data:            json.RawMessage(`{"UserID":""}`),
		Class:           event.CreateStreamEvent,
		FromMigration:   true,
	}
}

func ForTestEvent0_Aggregate3() event.PersistenceEvent {
	return event.PersistenceEvent{
		ID:              "0_3",
		AggregateID:     "3",
		TenantID:        "0000-0000-0000",
		AggregateType:   "forTestConcreteAggregate2",
		Version:         1,
		Type:            "github.com/global-soft-ba/go-eventstore/tests/forTestEvent2",
		TransactionTime: time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC),
		ValidTime:       time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC),
		Data:            json.RawMessage(`{"UserID":""}`),
		Class:           event.CreateStreamEvent,
		FromMigration:   true,
	}
}

func resetVersionForSave(evt event.PersistenceEvent) event.PersistenceEvent {
	evt.Version--
	return evt
}

func ForTestGetFilledRepoWitCtx(txCtx context.Context, eventStore event.EventStore, tenantID string) (chan error, event.EventStore) {
	chErr, err := eventStore.Save(txCtx, tenantID, []event.PersistenceEvent{
		resetVersionForSave(ForTestEvent0()),
		resetVersionForSave(ForTestEvent1()),
		resetVersionForSave(ForTestEvent2()),
		resetVersionForSave(ForTestEventPatch()),
		resetVersionForSave(ForTestEventFuturePatch()),
	}, 0)
	if err != nil {
		panic(err)
	}
	return chErr, eventStore
}

func ForTestGetFilledRepo(eventStore event.EventStore, tenantID string) (chan error, event.EventStore) {
	chErr, err := eventStore.Save(context.Background(), tenantID, []event.PersistenceEvent{
		resetVersionForSave(ForTestEvent0()),
		resetVersionForSave(ForTestEvent1()),
		resetVersionForSave(ForTestEvent2()),
		resetVersionForSave(ForTestEventPatch()),
		resetVersionForSave(ForTestEventFuturePatch()),
	}, 0)
	if err != nil {
		panic(err)
	}
	return chErr, eventStore
}

func ForTestGetFilledRepoWithMultipleAggregates(eventStore event.EventStore, tenantID string) (chan error, event.EventStore) {
	chErr, err := eventStore.SaveAll(context.Background(), tenantID, []event.PersistenceEvents{
		{
			Events: []event.PersistenceEvent{
				resetVersionForSave(ForTestEvent0()),
			},
			Version: 0,
		},
		{
			Events: []event.PersistenceEvent{
				resetVersionForSave(ForTestEvent0_Aggregate2()),
			},
			Version: 0,
		},
		{
			Events: []event.PersistenceEvent{
				resetVersionForSave(ForTestEvent0_Aggregate3()),
			},
			Version: 0,
		},
	})
	if err != nil {
		panic(err)
	}
	return chErr, eventStore
}

func ForTestGetFilledRepoWithoutPatch(eventStore event.EventStore, tenantID string) (chan error, event.EventStore) {
	chErr, err := eventStore.Save(context.Background(), tenantID, []event.PersistenceEvent{
		resetVersionForSave(ForTestEvent0()),
		resetVersionForSave(ForTestEvent1()),
		resetVersionForSave(ForTestEvent2()),
	}, 0)
	if err != nil {
		panic(err)
	}
	return chErr, eventStore
}

func ForTestGetFilledReproWithSnapshot(eventStore event.EventStore, tenantID string) (chan error, event.EventStore) {
	chErr, err := eventStore.Save(context.Background(), tenantID, []event.PersistenceEvent{
		resetVersionForSave(ForTestEvent0()),
		resetVersionForSave(ForTestEvent1()),
		resetVersionForSave(ForTestEvent2()),
		ForTestEvent2Snapshot(),
	}, 0)
	if err != nil {
		panic(err)
	}
	return chErr, eventStore
}

func ForTestGetFilledRepoWithLegacy(eventStore event.EventStore, tenantID string) (chan error, event.EventStore) {
	chErr, err := eventStore.Save(context.Background(), tenantID, []event.PersistenceEvent{
		resetVersionForSave(ForTestEvent0Legacy()),
	}, 0)
	if err != nil {
		panic(err)
	}
	return chErr, eventStore
}

func newTestProjectionTypeOne(id string, tenantID string, sleep time.Duration, chunkSize int) event.Projection {
	return &forTestProjection{
		id:                    id,
		eventTypes:            []string{event.EventType(&forTestEvent{})},
		events:                []event.IEvent{},
		failExecute:           false,
		executeDuration:       sleep,
		chunkSize:             chunkSize,
		timeOutInExecuteCount: 1,
		eventCounter:          atomic.NewInt32(0),
		executeCounter:        atomic.NewInt32(0),
		prepareCounter:        atomic.NewInt32(0),
		finishCounter:         atomic.NewInt32(0),
	}
}

func newTestProjectionTypeOneWithDelays(id, tenantID string, prepSleep, executeSleep, finishSleep time.Duration, timeOutExecuteCount int32, chunkSize int) event.Projection {
	return &forTestProjection{
		id:                    id,
		eventTypes:            []string{event.EventType(&forTestEvent{})},
		events:                []event.IEvent{},
		failExecute:           false,
		executeDuration:       executeSleep,
		prepDuration:          prepSleep,
		finishDuration:        finishSleep,
		timeOutInExecuteCount: timeOutExecuteCount,
		chunkSize:             chunkSize,
		eventCounter:          atomic.NewInt32(0),
		executeCounter:        atomic.NewInt32(0),
		prepareCounter:        atomic.NewInt32(0),
		finishCounter:         atomic.NewInt32(0),
	}
}

func newTestProjectionTypeOneWithExecuteFail(id string, tenantID string, chunkSize int, failExecute bool, failExecuteCounter int32) event.Projection {
	return &forTestProjection{
		id:                 id,
		eventTypes:         []string{event.EventType(&forTestEvent{})},
		events:             []event.IEvent{},
		failExecute:        failExecute,
		failOnExecuteCount: failExecuteCounter,
		executeDuration:    1,
		chunkSize:          chunkSize,
		eventCounter:       atomic.NewInt32(0),
		executeCounter:     atomic.NewInt32(0),
		prepareCounter:     atomic.NewInt32(0),
		finishCounter:      atomic.NewInt32(0),
	}
}

func newTestProjectionTypeTwo(id string, tenantID string, sleep time.Duration, chunkSize int) event.Projection {
	return &forTestProjection{
		id:                    id,
		eventTypes:            []string{event.EventType(&forTestEvent2{})},
		events:                []event.IEvent{},
		failExecute:           false,
		executeDuration:       sleep,
		chunkSize:             chunkSize,
		timeOutInExecuteCount: 1,
		eventCounter:          atomic.NewInt32(0),
		executeCounter:        atomic.NewInt32(0),
		prepareCounter:        atomic.NewInt32(0),
		finishCounter:         atomic.NewInt32(0),
	}
}

func newTestProjectionTwoTypes(id string, tenantID string, sleep time.Duration, chunkSize int) event.Projection {
	return &forTestProjection{
		id:                    id,
		eventTypes:            []string{event.EventType(&forTestEvent{}), event.EventType(&forTestEvent2{})},
		events:                []event.IEvent{},
		failExecute:           false,
		executeDuration:       sleep,
		chunkSize:             chunkSize,
		timeOutInExecuteCount: 1,
		eventCounter:          atomic.NewInt32(0),
		executeCounter:        atomic.NewInt32(0),
		prepareCounter:        atomic.NewInt32(0),
		finishCounter:         atomic.NewInt32(0),
	}
}

var eventMutex sync.RWMutex

type forTestProjection struct {
	id                    string
	chunkSize             int
	eventTypes            []string
	events                []event.IEvent
	failExecute           bool
	failOnExecuteCount    int32
	timeOutInExecuteCount int32
	prepDuration          time.Duration
	executeDuration       time.Duration
	finishDuration        time.Duration
	eventCounter          *atomic.Int32
	executeCounter        *atomic.Int32
	prepareCounter        *atomic.Int32
	finishCounter         *atomic.Int32
}

func (f *forTestProjection) ID() string {
	return f.id
}

func (f *forTestProjection) PrepareRebuild(ctxWithTimout context.Context, tenantID string) error {
	eventMutex.Lock()
	f.prepareCounter.Add(1)
	eventMutex.Unlock()

	timeOutDuration := 0 * time.Millisecond
	if f.timeOutInExecuteCount == f.prepareCounter.Load() {
		timeOutDuration = f.prepDuration
	}

	select {
	case <-ctxWithTimout.Done():
		return fmt.Errorf("context closed")
	// waiting - instead of sleep
	case <-time.After(timeOutDuration):
	}

	return nil
}

func (f *forTestProjection) PrepareRebuildSince(ctxWithTimout context.Context, tenantID string, since time.Time) error {
	eventMutex.Lock()
	f.prepareCounter.Add(1)
	eventMutex.Unlock()

	timeOutDuration := 0 * time.Millisecond
	if f.timeOutInExecuteCount == f.prepareCounter.Load() {
		timeOutDuration = f.prepDuration
	}

	select {
	case <-ctxWithTimout.Done():
		return fmt.Errorf("context closed")
	// waiting - instead of sleep
	case <-time.After(timeOutDuration):
	}

	return nil
}

func (f *forTestProjection) Execute(ctxWithTimout context.Context, events []event.IEvent) error {
	eventMutex.Lock()
	f.executeCounter.Add(1)
	eventMutex.Unlock()

	if f.failExecute && f.executeCounter.Load() == f.failOnExecuteCount {
		return fmt.Errorf("provoked error on excution count %d", f.executeCounter.Load())
	}

	timeOutDuration := 0 * time.Millisecond
	if f.timeOutInExecuteCount == f.executeCounter.Load() {
		timeOutDuration = f.executeDuration
	}

	//fmt.Println("execute", f.id, f.executeCounter.Load(), f.timeOutInExecuteCount, timeOutDuration, events)

	select {
	case <-ctxWithTimout.Done():
		// rollback
		return fmt.Errorf("context closed")
	// waiting - instead of sleep
	case <-time.After(timeOutDuration):
	}

	eventMutex.Lock()
	f.eventCounter.Add(int32(len(events)))
	f.events = append(f.events, events...)
	eventMutex.Unlock()
	return nil
}

func (f *forTestProjection) FinishRebuild(ctxWithTimout context.Context, tenantID string) error {
	eventMutex.Lock()
	f.finishCounter.Add(1)
	eventMutex.Unlock()

	timeOutDuration := 0 * time.Millisecond
	if f.timeOutInExecuteCount == f.finishCounter.Load() {
		timeOutDuration = f.finishDuration
	}

	select {
	case <-ctxWithTimout.Done():
		return fmt.Errorf("context closed")
	// waiting - instead of sleep
	case <-time.After(timeOutDuration):
	}

	return nil
}

func (f *forTestProjection) EventTypes() []string {
	return f.eventTypes
}

func (f *forTestProjection) ChunkSize() int {
	return f.chunkSize
}

func (f *forTestProjection) ForTestResetAll() {
	eventMutex.Lock()
	f.events = []event.IEvent{}
	f.eventCounter = atomic.NewInt32(0)
	f.prepareCounter = atomic.NewInt32(0)
	f.executeCounter = atomic.NewInt32(0)
	f.finishCounter = atomic.NewInt32(0)
	eventMutex.Unlock()
}

func (f *forTestProjection) ForTestResetEventsOnly() {
	eventMutex.Lock()
	f.events = []event.IEvent{}
	eventMutex.Unlock()
}

func (f *forTestProjection) ForTestReset(event []event.IEvent, prepCounter int32, execCounter int32, finishCounter int32) {
	lenghtEvent := len(event)
	eventMutex.Lock()
	f.events = event
	f.eventCounter = atomic.NewInt32(int32(lenghtEvent))
	f.prepareCounter = atomic.NewInt32(prepCounter)
	f.executeCounter = atomic.NewInt32(execCounter)
	f.finishCounter = atomic.NewInt32(finishCounter)
	eventMutex.Unlock()
}

func (f *forTestProjection) ForTestGetEvents() []event.IEvent {
	eventMutex.RLock()
	defer eventMutex.RUnlock()
	return f.events
}
