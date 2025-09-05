package tests

import (
	"context"
	"fmt"
	"github.com/global-soft-ba/go-eventstore"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/domain/model/projection"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/shared"
	"github.com/stretchr/testify/assert"
	"reflect"
	"testing"
	"time"
)

type projectionAsserts struct {
	id                 shared.ProjectionID
	wantPrepCounter    int32
	wantExecuteCounter int32
	wantFinishCounter  int32
	wantState          projection.State
	wantUpdatedAt      bool
	wantEventStream    []event.IEvent
}

func assertProjectionError(errChan chan error, wantProjectionError bool) error {
	if errChan != nil {
		for err := range errChan {
			if (err != nil) != wantProjectionError {
				return fmt.Errorf("failed projection gotErr = %v, wantErr %v", err, wantProjectionError)
			}
		}
	}
	return nil
}

func assertProjection(ctx context.Context, assert projectionAsserts, store event.EventStore, proj event.Projection) error {

	//assert state
	if err := assertExecutionState(ctx, assert.id, assert.wantState, assert.wantUpdatedAt, store); err != nil {
		return err
	}
	//assert eventStream
	if err := assertEventStream(proj, assert.wantEventStream); err != nil {
		return err
	}

	//assert counter
	if err := assertProjectionCounter(assert, proj); err != nil {
		return err
	}

	return nil
}

func assertExecutionState(ctx context.Context, id shared.ProjectionID, wantState projection.State, wantUpdatedAt bool, store event.EventStore) error {

	var (
		state []event.ProjectionState
		err   error
	)
	if wantState != "" || wantUpdatedAt {
		state, err = store.GetProjectionStates(ctx, id.TenantID, id.ProjectionID)
		if err != nil {
			return err
		}
		if wantState != "" {
			if state[0].State != string(wantState) {
				return fmt.Errorf("wrong state got = %v, wantErr %v", state[0].State, wantState)
			}
		}
		if wantUpdatedAt {
			if err = IsTimeWithinDuration(state[0].UpdatedAt, time.Now(), time.Second); err != nil {
				return fmt.Errorf("wrong updatedAt: %w", err)
			}
		}
	}
	return nil
}

func IsTimeWithinDuration(t, ref time.Time, d time.Duration) error {
	diff := t.Sub(ref).Abs()
	if diff > d {
		return fmt.Errorf("time %v is not within %v of %v", t, d, ref)
	}
	return nil
}

func assertEventStream(proj event.Projection, wantEventStreamInProjection []event.IEvent) error {
	gotEventStream := proj.(*forTestProjection).ForTestGetEvents()

	//if len(gotEventStream) != len(wantEventStreamInProjection) {
	//	return fmt.Errorf("wrong eventStream length got = %v, want %v", len(gotEventStream), len(wantEventStreamInProjection))
	//}

	for idx, iEvent := range wantEventStreamInProjection {
		evt, ok := iEvent.(*forTestEvent)
		if !ok {
			evt2, ok2 := iEvent.(*forTestEvent2)
			if !ok2 {
				return fmt.Errorf("wrong eventStream type got = %v, want %v", reflect.TypeOf(iEvent), reflect.TypeOf(evt))
			}
			evt2.EventID = gotEventStream[idx].GetEventID()
			continue
		}
		evt.EventID = gotEventStream[idx].GetEventID()
	}

	if !reflect.DeepEqual(gotEventStream, wantEventStreamInProjection) {
		return fmt.Errorf("wrong eventStream got = %v, want %v", gotEventStream, wantEventStreamInProjection)
	}

	return nil
}

func assertProjectionCounter(assert projectionAsserts, proj event.Projection) error {
	//assert execute  counter
	gotCalls := proj.(*forTestProjection).executeCounter.Load()
	if gotCalls != assert.wantExecuteCounter {
		return fmt.Errorf("wrong execute count got = %v, want %v", gotCalls, assert.wantExecuteCounter)
	}

	//assert prep counter
	gotCalls = proj.(*forTestProjection).prepareCounter.Load()
	if gotCalls != assert.wantPrepCounter {
		return fmt.Errorf("wrong prep count got = %v, want %v", gotCalls, assert.wantPrepCounter)
	}

	//assert finish counter
	gotCalls = proj.(*forTestProjection).finishCounter.Load()
	if gotCalls != assert.wantFinishCounter {
		return fmt.Errorf("wrong finish count got = %v, want %v", gotCalls, assert.wantFinishCounter)
	}

	return nil
}

func assertProjectionIDs(t *testing.T, got []event.ProjectionState, want []string) {
	assert.Equal(t, len(want), len(got))

	var gotID []string
	for _, state := range got {
		gotID = append(gotID, state.ProjectionID)
	}
	assert.ElementsMatch(t, gotID, want)
}

func assertProjectionState(a, b event.ProjectionState) error {
	aTime, bTime := a.UpdatedAt, b.UpdatedAt
	a.UpdatedAt, b.UpdatedAt = time.Time{}, time.Time{}
	if !reflect.DeepEqual(a, b) {
		return fmt.Errorf("states are not equal")
	}
	if err := IsTimeWithinDuration(aTime, bTime, time.Second); err != nil {
		return fmt.Errorf("wrong updatedAt: %w", err)
	}
	return nil
}
