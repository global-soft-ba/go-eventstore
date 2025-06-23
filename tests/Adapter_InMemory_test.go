//go:build unit

package tests

import (
	"context"
	"github.com/global-soft-ba/go-eventstore"
	hexStore "github.com/global-soft-ba/go-eventstore/eventstore"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/port/persistence"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/port/persistence/transactor"
	"github.com/global-soft-ba/go-eventstore/eventstore/infrastructure/persistence/memory"
	"testing"
)

func NewTestAdapter() persistence.Port {
	adp := memory.New()
	return adp
}

func NewPassThroughAdapter() persistence.Port {
	adp := memory.NewTXPassThrough()
	return adp
}

func _MemoryTransactor() transactor.Port {
	return memory.NewTransactor()
}

func NewSlowAdapterForTest() persistence.Port {
	adp := memory.NewSlowAdapterForTest()
	return adp
}

func NewEventStore(_ context.Context, adapter persistence.Port) event.EventStore {
	evt, err, resCh := hexStore.New(adapter)
	if err != nil {
		panic("error in test to init event store")
	}

	//wait until init is done
	<-resCh

	return evt
}

func TestSaveAggregate(t *testing.T) {
	testSaveAggregate(t,
		_MemoryTransactor(),
		func(txCtx context.Context) event.EventStore {
			return NewEventStore(txCtx, NewPassThroughAdapter())
		}, cleanRegistries)
}

func TestSaveAggregateWithValidTime(t *testing.T) {
	testSaveAggregateWithValidTime(t,
		_MemoryTransactor(),
		func(txCtx context.Context) event.EventStore {
			return NewEventStore(txCtx, NewPassThroughAdapter())
		}, cleanRegistries)
}

func TestSaveAggregates(t *testing.T) {
	testSaveAggregates(t, func() persistence.Port {
		return NewTestAdapter()
	}, cleanRegistries)
}

func TestSaveAggregatesConcurrently(t *testing.T) {
	t.Skip("memDb does not support multiple writer")
	testSaveAggregatesConcurrently(t, func() persistence.Port {
		return NewSlowAdapterForTest()
	}, clean)
}

func TestSaveAggregateWithConcurrentModificationException(t *testing.T) {
	testSaveAggregateWithConcurrentModificationException(t, func() persistence.Port {
		return NewTestAdapter()
	}, cleanRegistries)
}

func TestSaveAggregateWithEphemeralEventTypes(t *testing.T) {
	testSaveAggregateWithEphemeralEventTypes(t, func() persistence.Port {
		return NewTestAdapter()
	}, cleanRegistries)
}

func TestSaveAggregateWithSnapshots(t *testing.T) {
	testSaveAggregateWithSnapShot(t, func() persistence.Port {
		return NewTestAdapter()
	}, cleanRegistries)
}

func TestSaveAggregatesWithSnapshots(t *testing.T) {
	testSaveAggregatesWithSnapShot(t, func() event.EventStore {
		return NewEventStore(context.Background(), NewTestAdapter())
	}, cleanRegistries)
}

func TestValidityOfSnapsShots(t *testing.T) {
	testAggregateWithSnapShotValidity(t, func() event.EventStore {
		return NewEventStore(context.Background(), NewTestAdapter())
	}, cleanRegistries)
}

func TestSaveAggregateWithProjection(t *testing.T) {
	testSaveAggregateWithProjection(t, UnitTest, event.ECS, func() persistence.Port { return NewTestAdapter() }, cleanRegistries)
	testSaveAggregateWithProjection(t, UnitTest, event.CCS, func() persistence.Port { return NewTestAdapter() }, cleanRegistries)
}

func TestSaveAggregatesWithProjection(t *testing.T) {
	testSaveAggregatesWithProjection(t, event.ECS, func() persistence.Port { return NewTestAdapter() }, cleanRegistries)
	testSaveAggregatesWithProjection(t, event.CCS, func() persistence.Port { return NewTestAdapter() }, cleanRegistries)
}

func TestSaveAggregatesWithConcurrentProjection(t *testing.T) {
	t.Skip("memDb does not support multiple writer/transactions")
	testSaveAggregatesWithConcurrentProjections(t, func() persistence.Port {
		return NewTestAdapter()
	}, cleanRegistries)
}

func TestRebuildProjection(t *testing.T) {
	testRebuildProjection(t, func() persistence.Port {
		return NewTestAdapter()
	}, cleanRegistries)
}

func TestRebuildProjectionSince(t *testing.T) {
	testRebuildProjectionSince(t, func() persistence.Port {
		return NewTestAdapter()
	}, cleanRegistries)
}

func TestRebuildAllProjection(t *testing.T) {
	testRebuildAllProjection(t, func() persistence.Port {
		return NewTestAdapter()
	}, cleanRegistries)
}

func TestSaveAggregatesConcurrentWithProjection(t *testing.T) {
	t.Skip("memDb does not support multiple writer/transactions")
	testSaveAggregatesConcurrentWithProjection(t, func() persistence.Port {
		return NewTestAdapter()
	}, cleanRegistries)
}

func TestExecuteAllExistingProjections(t *testing.T) {
	testExecuteAllExistingProjections(t, func() persistence.Port { return NewTestAdapter() }, cleanRegistries)
}

func TestLoadAggregateAsAt(t *testing.T) {
	testLoadAggregateAsAt(t, func() event.EventStore {
		return NewEventStore(context.Background(), NewTestAdapter())
	}, cleanRegistries)
}

func TestLoadAggregateAsOf(t *testing.T) {
	testLoadAggregateAsOf(t, func() event.EventStore {
		return NewEventStore(context.Background(), NewTestAdapter())
	}, cleanRegistries)
}

func TestLoadAggregateAsOfTill(t *testing.T) {
	testLoadAggregateAsOfTill(t, func() event.EventStore {
		return NewEventStore(context.Background(), NewTestAdapter())
	}, cleanRegistries)
}

func TestLoadAllAggregatesOfAggregateAsAt(t *testing.T) {
	testLoadAllAggregatesOfAggregateAsAt(t, func() event.EventStore {
		return NewEventStore(context.Background(), NewTestAdapter())
	}, cleanRegistries)
}

func TestLoadAllAggregatesOfAggregateAsOf(t *testing.T) {
	testLoadAllAggregatesOfAggregateAsOf(t, func() event.EventStore {
		return NewEventStore(context.Background(), NewTestAdapter())
	}, cleanRegistries)
}

func TestLoadAllAggregatesOfAggregateAsOfTill(t *testing.T) {
	testLoadAllAggregatesOfAggregateAsOfTill(t, func() event.EventStore {
		return NewEventStore(context.Background(), NewTestAdapter())
	}, cleanRegistries)
}

func TestLoadAllAggregatesAsAt(t *testing.T) {
	testLoadAllAggregatesAsAt(t, func() event.EventStore {
		return NewEventStore(context.Background(), NewTestAdapter())
	}, cleanRegistries)
}

func TestLoadAllAggregatesAsOf(t *testing.T) {
	testLoadAllAggregatesAsOf(t, func() event.EventStore {
		return NewEventStore(context.Background(), NewTestAdapter())
	}, cleanRegistries)
}

func TestLoadAllAggregatesAsOfTill(t *testing.T) {
	testLoadAllAggregatesAsOfTill(t, func() event.EventStore {
		return NewEventStore(context.Background(), NewTestAdapter())
	}, cleanRegistries)
}

func TestGetAggregateState(t *testing.T) {
	testGetAggregateStates(t, func() event.EventStore {
		return NewEventStore(context.Background(), NewTestAdapter())
	}, cleanRegistries)
}

func TestGetAggregateStatesForAggregateType(t *testing.T) {
	testGetAggregateStatesForAggregateType(t, func() event.EventStore {
		return NewEventStore(context.Background(), NewTestAdapter())
	}, cleanRegistries)
}

func TestGetAggregateStatesForAggregateTypeTill(t *testing.T) {
	testGetAggregateStatesForAggregateTypeTill(t, func() event.EventStore {
		return NewEventStore(context.Background(), NewTestAdapter())
	}, cleanRegistries)
}

func TestGetPatchFreePeriodsForInterval(t *testing.T) {
	testGetPatchFreePeriodsForInterval(t, func() persistence.Port { return NewTestAdapter() }, cleanRegistries)
}

func TestGetProjectionStates(t *testing.T) {
	testGetProjectionStates(t, func() persistence.Port { return NewTestAdapter() }, cleanRegistries)
}

func TestGetAggregatesEvents(t *testing.T) {
	testGetAggregatesEventsSortAndSearch(t, func() persistence.Port { return NewTestAdapter() }, cleanRegistries)
	testGetAggregatesEventsPaginated(t, func() persistence.Port { return NewTestAdapter() }, cleanRegistries)
}

func TestRemoveProjection(t *testing.T) {
	t.Skip("we cannot test this, because to remove the projection we first have to unregister them by " +
		"not defining it anymore in the constructor of the event store (and restart the store). ")
	testRemoveProjection(t, func() persistence.Port { return NewTestAdapter() }, cleanRegistries)
}

func TestDeleteEvent(t *testing.T) {
	testDeleteEvents(t, func() persistence.Port { return NewTestAdapter() }, cleanRegistries)
	testProjectionsAfterDeleteEvents(t, func() persistence.Port { return NewTestAdapter() }, cleanRegistries)
}
