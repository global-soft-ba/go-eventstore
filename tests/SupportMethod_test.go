package tests

import (
	"context"
	"github.com/global-soft-ba/go-eventstore"
	"github.com/global-soft-ba/go-eventstore/eventstore"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/domain/model/projection"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/port/persistence"
	"github.com/stretchr/testify/assert"
	"math"
	"reflect"
	"testing"
	"time"
)

func testGetAggregateStates(t *testing.T, eventStoreFactory func() event.EventStore, cleanUp func()) {
	cleanUp()
	type args struct {
		ctx            context.Context
		aggregateType  string
		aggregateID    string
		tenantID       string
		projectionTime time.Time
		eventStore     func() event.EventStore
	}
	tests := []struct {
		name      string
		args      args
		wantState event.AggregateState
		wantErr   bool
	}{
		{
			name: "single aggregate",
			args: args{
				ctx:            context.Background(),
				aggregateType:  "forTestConcreteAggregate",
				aggregateID:    "1",
				tenantID:       "0000-0000-0000",
				projectionTime: time.Date(2021, 1, 1, 1, 1, 1, 0, time.UTC),
				eventStore: func() event.EventStore {
					_, store := ForTestGetFilledRepo(eventStoreFactory(), "0000-0000-0000")
					return store
				},
			},
			wantState: event.AggregateState{
				TenantID:            "0000-0000-0000",
				AggregateType:       "forTestConcreteAggregate",
				AggregateID:         "1",
				CurrentVersion:      5,
				LastTransactionTime: time.Date(2021, 1, 1, 1, 1, 1, 4, time.UTC),
				LatestValidTime:     time.Date(2050, 1, 1, 1, 1, 1, 0, time.UTC),
				CreateTime:          time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC),
				CloseTime:           time.Time{},
			},
			wantErr: false,
		},
		{
			name: "multiple aggregates",
			args: args{
				ctx:            context.Background(),
				aggregateType:  "forTestConcreteAggregate",
				aggregateID:    "1",
				tenantID:       "0000-0000-0000",
				projectionTime: time.Date(2021, 1, 1, 1, 1, 1, 0, time.UTC),
				eventStore: func() event.EventStore {
					_, store := ForTestGetFilledRepoWithMultipleAggregates(eventStoreFactory(), "0000-0000-0000")
					return store
				},
			},
			wantState: event.AggregateState{
				TenantID:            "0000-0000-0000",
				AggregateType:       "forTestConcreteAggregate",
				AggregateID:         "1",
				CurrentVersion:      1,
				LastTransactionTime: time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC),
				LatestValidTime:     time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC),
				CreateTime:          time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC),
				CloseTime:           time.Time{},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			eventStore := tt.args.eventStore()
			defer cleanUp()
			gotState, err := eventStore.GetAggregateState(tt.args.ctx, tt.args.tenantID, tt.args.aggregateType, tt.args.aggregateID)
			if (err != nil) != tt.wantErr {
				t.Errorf("LoadAggregateAsAt() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !reflect.DeepEqual(gotState, tt.wantState) {
				t.Errorf("GetAggregateState() gotState = %v, want %v", gotState, tt.wantState)
			}
		})
	}
}

func testGetAggregateStatesForAggregateType(t *testing.T, eventStoreFactory func() event.EventStore, cleanUp func()) {
	cleanUp()
	eventStore := func() event.EventStore {
		es := eventStoreFactory()
		_, err := es.SaveAll(context.Background(), "0000-0000-0000", []event.PersistenceEvents{
			{
				Events: []event.PersistenceEvent{
					resetVersionForSave(ForTestEvent0()),
					resetVersionForSave(ForTestEvent1()),
					resetVersionForSave(ForTestEvent2()),
					resetVersionForSave(ForTestEventPatch()),
					resetVersionForSave(ForTestEventFuturePatch()),
				},
				Version: 0,
			},
			{
				Events: []event.PersistenceEvent{
					resetVersionForSave(ForTestEvent0_Aggregate2()),
				},
				Version: 0,
			},
		})
		if err != nil {
			panic(err)
		}
		return es
	}

	type args struct {
		ctx           context.Context
		aggregateType string
		tenantID      string
		eventStore    func() event.EventStore
	}
	tests := []struct {
		name       string
		args       args
		wantStates []event.AggregateState
		wantErr    bool
	}{
		{
			name: "multiple aggregates",
			args: args{
				ctx:           context.Background(),
				aggregateType: "forTestConcreteAggregate",
				tenantID:      "0000-0000-0000",
				eventStore:    eventStore,
			},
			wantStates: []event.AggregateState{
				{
					TenantID:            "0000-0000-0000",
					AggregateType:       "forTestConcreteAggregate",
					AggregateID:         "1",
					CurrentVersion:      5,
					LastTransactionTime: time.Date(2021, 1, 1, 1, 1, 1, 4, time.UTC),
					LatestValidTime:     time.Date(2050, 1, 1, 1, 1, 1, 0, time.UTC),
					CreateTime:          time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC),
					CloseTime:           time.Time{},
				},
				{
					TenantID:            "0000-0000-0000",
					AggregateType:       "forTestConcreteAggregate",
					AggregateID:         "2",
					CurrentVersion:      1,
					LastTransactionTime: time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC),
					LatestValidTime:     time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC),
					CreateTime:          time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC),
					CloseTime:           time.Time{},
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := tt.args.eventStore()
			defer cleanUp()
			gotState, err := store.GetAggregateStatesForAggregateType(tt.args.ctx, tt.args.tenantID, tt.args.aggregateType)
			if (err != nil) != tt.wantErr {
				t.Errorf("LoadAggregateAsAt() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !reflect.DeepEqual(gotState, tt.wantStates) {
				t.Errorf("GetAggregateState() gotStates = %v, want %v", gotState, tt.wantStates)
			}

		})
	}
}

func testGetAggregateStatesForAggregateTypeTill(t *testing.T, eventStoreFactory func() event.EventStore, cleanUp func()) {
	cleanUp()
	eventStore := func() event.EventStore {
		es := eventStoreFactory()
		_, err := es.SaveAll(context.Background(), "0000-0000-0000", []event.PersistenceEvents{
			{
				Events: []event.PersistenceEvent{
					resetVersionForSave(ForTestEvent0()),
					resetVersionForSave(ForTestEvent1()),
					resetVersionForSave(ForTestEvent2()),
					resetVersionForSave(ForTestEventPatch()),
					resetVersionForSave(ForTestEventFuturePatch()),
				},
				Version: 0,
			},
			{
				Events: []event.PersistenceEvent{
					resetVersionForSave(ForTestEvent0_Aggregate2()),
				},
				Version: 0,
			},
		})
		if err != nil {
			panic(err)
		}
		return es
	}

	type args struct {
		ctx            context.Context
		aggregateType  string
		tenantID       string
		projectionTime time.Time
		eventStore     func() event.EventStore
	}
	tests := []struct {
		name       string
		args       args
		wantStates []event.AggregateState
		wantErr    bool
	}{
		{
			name: "multiple aggregates",
			args: args{
				ctx:            context.Background(),
				aggregateType:  "forTestConcreteAggregate",
				tenantID:       "0000-0000-0000",
				projectionTime: time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC),
				eventStore:     eventStore,
			},
			wantStates: []event.AggregateState{
				{
					TenantID:            "0000-0000-0000",
					AggregateType:       "forTestConcreteAggregate",
					AggregateID:         "1",
					CurrentVersion:      5,
					LastTransactionTime: time.Date(2021, 1, 1, 1, 1, 1, 4, time.UTC),
					LatestValidTime:     time.Date(2050, 1, 1, 1, 1, 1, 0, time.UTC),
					CreateTime:          time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC),
					CloseTime:           time.Time{},
				},
				{
					TenantID:            "0000-0000-0000",
					AggregateType:       "forTestConcreteAggregate",
					AggregateID:         "2",
					CurrentVersion:      1,
					LastTransactionTime: time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC),
					LatestValidTime:     time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC),
					CreateTime:          time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC),
					CloseTime:           time.Time{},
				},
			},
			wantErr: false,
		},
		{
			name: "single result",
			args: args{
				ctx:            context.Background(),
				aggregateType:  "forTestConcreteAggregate",
				tenantID:       "0000-0000-0000",
				projectionTime: time.Date(2021, 1, 1, 1, 1, 1, 0, time.UTC),
				eventStore:     eventStore,
			},
			wantStates: []event.AggregateState{
				{
					TenantID:            "0000-0000-0000",
					AggregateType:       "forTestConcreteAggregate",
					AggregateID:         "1",
					CurrentVersion:      5,
					LastTransactionTime: time.Date(2021, 1, 1, 1, 1, 1, 4, time.UTC),
					LatestValidTime:     time.Date(2050, 1, 1, 1, 1, 1, 0, time.UTC),
					CreateTime:          time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC),
					CloseTime:           time.Time{},
				},
			},
			wantErr: false,
		},
		{
			name: "empty result",
			args: args{
				ctx:            context.Background(),
				aggregateType:  "forTestConcreteAggregate",
				tenantID:       "0000-0000-0000",
				projectionTime: time.Date(2020, 1, 1, 1, 1, 1, 0, time.UTC),
				eventStore:     eventStore,
			},
			wantStates: nil,
			wantErr:    false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := tt.args.eventStore()
			defer cleanUp()
			gotState, err := store.GetAggregateStatesForAggregateTypeTill(tt.args.ctx, tt.args.tenantID, tt.args.aggregateType, tt.args.projectionTime)
			if (err != nil) != tt.wantErr {
				t.Errorf("LoadAggregateAsAt() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			assert.EqualValues(t, gotState, tt.wantStates)
			//{
			//	t.Errorf("GetAggregateState() gotStates = %v, want %v", gotState, tt.wantStates)
			//}

		})
	}
}

func testGetPatchFreePeriodsForInterval(t *testing.T, adapter func() persistence.Port, cleanUp func()) {
	cleanUp()

	type args struct {
		ctx        context.Context
		aggregate  event.AggregateWithEventSourcingSupport
		start      time.Time
		end        time.Time
		eventStore func() event.EventStore
	}
	tests := []struct {
		name        string
		args        args
		wantErr     bool
		wantPeriods []event.TimeInterval
	}{
		{
			name: "instant event only",
			args: args{
				ctx: context.Background(),
				aggregate: newForTestConcreteAggregate("1", "Name", 0, "0000-0000-0000", []event.IEvent{
					ForTestMakeCreateEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 0, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 0, time.UTC)),
					ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
					ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
					ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC)),
					ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 5, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 5, time.UTC)),
					ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 7, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 7, time.UTC)),
				}),
				start: time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC),
				end:   time.Date(2021, 1, 1, 1, 1, 1, 11, time.UTC),
				eventStore: func() event.EventStore {
					store, err, _ := eventstore.New(adapter())
					if err != nil {
						panic("error in test case preparation")
					}
					return store
				},
			},
			wantErr: false,
			wantPeriods: []event.TimeInterval{
				{
					time.Date(2021, 1, 1, 0, 0, 0, 1, time.UTC), //start of search intervall
					time.Date(2021, 1, 1, 1, 1, 1, 10, time.UTC),
				},
			},
		},

		{
			name: "use historical patches only/search window before aggregate create",
			args: args{
				ctx: context.Background(),
				aggregate: newForTestConcreteAggregate("1", "Name", 0, "0000-0000-0000", []event.IEvent{
					ForTestMakeCreateEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 0, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 0, time.UTC)),
					ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 6, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC)),
					ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 7, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 4, time.UTC)),
					ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 8, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 5, time.UTC)),
					ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 12, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 11, time.UTC)),
					ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 13, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 20, time.UTC)),
				}),
				start: time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC),
				end:   time.Date(2021, 1, 1, 1, 1, 1, 11, time.UTC),
				eventStore: func() event.EventStore {
					store, err, _ := eventstore.New(adapter())
					if err != nil {
						panic("error in test case preparation")
					}
					return store
				},
			},
			wantErr: false,
			wantPeriods: []event.TimeInterval{
				{
					time.Date(2021, 1, 1, 0, 0, 0, 1, time.UTC), //start of search intervall
					time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC),
				},
				{
					time.Date(2021, 1, 1, 1, 1, 1, 9, time.UTC),
					time.Date(2021, 1, 1, 1, 1, 1, 10, time.UTC),
				}},
		},
		{
			name: "use historical patches only/search window during aggregate",
			args: args{
				ctx: context.Background(),
				aggregate: newForTestConcreteAggregate("1", "Name", 0, "0000-0000-0000", []event.IEvent{
					ForTestMakeCreateEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 0, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 0, time.UTC)),
					ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 6, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC)),
					ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 7, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 4, time.UTC)),
					ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 8, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 5, time.UTC)),
					ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 12, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 11, time.UTC)),
					ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 13, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 20, time.UTC)),
				}),
				start: time.Date(2021, 1, 1, 1, 1, 1, 4, time.UTC),
				end:   time.Date(2021, 1, 1, 1, 1, 1, 11, time.UTC),
				eventStore: func() event.EventStore {
					store, err, _ := eventstore.New(adapter())
					if err != nil {
						panic("error in test case preparation")
					}
					return store
				},
			},
			wantErr: false,
			wantPeriods: []event.TimeInterval{
				{
					time.Date(2021, 1, 1, 1, 1, 1, 9, time.UTC),
					time.Date(2021, 1, 1, 1, 1, 1, 10, time.UTC),
				}},
		},

		{
			name: "use future patches only/search window during aggregate create",
			args: args{
				ctx: context.Background(),
				aggregate: newForTestConcreteAggregate("1", "Name", 0, "0000-0000-0000", []event.IEvent{
					ForTestMakeCreateEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 0, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 0, time.UTC)),
					ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC)),
					ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 4, time.UTC)),
					ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 5, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 6, time.UTC)),
					ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 8, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 11, time.UTC)),
					ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 20, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 25, time.UTC)),
				}),
				start: time.Date(2021, 1, 1, 1, 1, 1, 5, time.UTC),
				end:   time.Date(2021, 1, 1, 1, 1, 1, 30, time.UTC),
				eventStore: func() event.EventStore {
					store, err, _ := eventstore.New(adapter())
					if err != nil {
						panic("error in test case preparation")
					}
					return store
				},
			},
			wantErr: false,
			wantPeriods: []event.TimeInterval{

				{
					time.Date(2021, 1, 1, 1, 1, 1, 12, time.UTC),
					time.Date(2021, 1, 1, 1, 1, 1, 19, time.UTC),
				},
				{
					time.Date(2021, 1, 1, 1, 1, 1, 26, time.UTC),
					time.Date(2021, 1, 1, 1, 1, 1, 29, time.UTC),
				},
			},
		},
		{
			name: "use future/historical patches only/search window during aggregate create",
			args: args{
				ctx: context.Background(),
				aggregate: newForTestConcreteAggregate("1", "Name", 0, "0000-0000-0000", []event.IEvent{
					ForTestMakeCreateEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 0, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 0, time.UTC)),
					ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
					ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 5, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
					ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 5, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 5, time.UTC)),
					ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 8, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
					ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 9, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 20, time.UTC)),
				}),
				start: time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC),
				end:   time.Date(2021, 1, 1, 1, 1, 1, 30, time.UTC),
				eventStore: func() event.EventStore {
					store, err, _ := eventstore.New(adapter())
					if err != nil {
						panic("error in test case preparation")
					}
					return store
				},
			},
			wantErr: false,
			wantPeriods: []event.TimeInterval{
				{
					time.Date(2021, 1, 1, 1, 1, 1, 21, time.UTC),
					time.Date(2021, 1, 1, 1, 1, 1, 29, time.UTC),
				},
			},
		},
		{
			name: "empty result",
			args: args{
				ctx: context.Background(),
				aggregate: newForTestConcreteAggregate("1", "Name", 0, "0000-0000-0000", []event.IEvent{
					ForTestMakeCreateEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 0, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 0, time.UTC)),
					ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
					ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 5, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
					ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 5, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 5, time.UTC)),
					ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 8, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
					ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 9, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 20, time.UTC)),
				}),
				start: time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC),
				end:   time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC),
				eventStore: func() event.EventStore {
					store, err, _ := eventstore.New(adapter())
					if err != nil {
						panic("error in test case preparation")
					}
					return store
				},
			},
			wantErr:     false,
			wantPeriods: nil,
		},
		{
			name: "no events in search window",
			args: args{
				ctx: context.Background(),
				aggregate: newForTestConcreteAggregate("1", "Name", 0, "0000-0000-0000", []event.IEvent{
					ForTestMakeCreateEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 0, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 0, time.UTC)),
					ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
					ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 5, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
					ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 5, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 5, time.UTC)),
					ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 8, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
					ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 15, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 20, time.UTC)),
				}),
				start: time.Date(2022, 1, 1, 1, 1, 1, 10, time.UTC),
				end:   time.Date(2022, 1, 1, 1, 1, 1, 13, time.UTC),
				eventStore: func() event.EventStore {
					store, err, _ := eventstore.New(adapter())
					if err != nil {
						panic("error in test case preparation")
					}
					return store
				},
			},
			wantErr: false,
			wantPeriods: []event.TimeInterval{
				{
					time.Date(2022, 1, 1, 1, 1, 1, 11, time.UTC),
					time.Date(2022, 1, 1, 1, 1, 1, 12, time.UTC),
				},
			},
		},
		{
			name: "wrong interval equal",
			args: args{
				ctx: context.Background(),
				aggregate: newForTestConcreteAggregate("1", "Name", 0, "0000-0000-0000", []event.IEvent{
					ForTestMakeCreateEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 0, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 0, time.UTC)),
				}),
				start: time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC),
				end:   time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC),
				eventStore: func() event.EventStore {
					store, err, _ := eventstore.New(adapter())
					if err != nil {
						panic("error in test case preparation")
					}
					return store
				},
			},
			wantErr:     true,
			wantPeriods: nil,
		},
		{
			name: "wrong interval to small",
			args: args{
				ctx: context.Background(),
				aggregate: newForTestConcreteAggregate("1", "Name", 0, "0000-0000-0000", []event.IEvent{
					ForTestMakeCreateEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 0, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 0, time.UTC)),
				}),
				start: time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC),
				end:   time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC),
				eventStore: func() event.EventStore {
					store, err, _ := eventstore.New(adapter())
					if err != nil {
						panic("error in test case preparation")
					}
					return store
				},
			},
			wantErr:     true,
			wantPeriods: nil,
		},
		{
			name: "wrong interval end before start",
			args: args{
				ctx: context.Background(),
				aggregate: newForTestConcreteAggregate("1", "Name", 0, "0000-0000-0000", []event.IEvent{
					ForTestMakeCreateEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 0, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 0, time.UTC)),
				}),
				start: time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC),
				end:   time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC),
				eventStore: func() event.EventStore {
					store, err, _ := eventstore.New(adapter())
					if err != nil {
						panic("error in test case preparation")
					}
					return store
				},
			},
			wantErr:     true,
			wantPeriods: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			eventStore := tt.args.eventStore()
			defer cleanUp()

			if _, err := event.SaveAggregate(tt.args.ctx, eventStore, tt.args.aggregate); err != nil {
				t.Errorf("SaveAggregate() error = %v, wantErr %v", err, tt.wantErr)
			}

			gotPeriods, err := eventStore.GetPatchFreePeriodsForInterval(tt.args.ctx, tt.args.aggregate.GetTenantID(), "forTestConcreteAggregate", tt.args.aggregate.GetID(), tt.args.start, tt.args.end)
			if !tt.wantErr && err != nil {
				t.Error(err)
			}
			if !reflect.DeepEqual(gotPeriods, tt.wantPeriods) && !tt.wantErr {
				t.Errorf("GetPatchFreePeriodsForInterval() \n got = %v,\nwant = %v", gotPeriods, tt.wantPeriods)
			}

		})
	}
}

func testGetProjectionStates(t *testing.T, adapter func() persistence.Port, cleanUp func()) {
	cleanUp()
	tenantID := "0000-0000-0000"
	type args struct {
		ctx        context.Context
		aggregates []event.AggregateWithEventSourcingSupport
		eventStore func() (event.EventStore, []event.Projection)
	}
	tests := []struct {
		name      string
		args      args
		wantState map[string]event.ProjectionState
	}{
		{
			name: "single projection with initial options",
			args: args{
				ctx: context.Background(),
				aggregates: []event.AggregateWithEventSourcingSupport{
					newForTestConcreteAggregate("1", "Name", 0, tenantID, []event.IEvent{
						ForTestMakeCreateEvent("1", tenantID, time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
						ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
						ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
					}),
				},
				eventStore: func() (event.EventStore, []event.Projection) {
					projectionTypeOne := newTestProjectionTypeOne("projection_1", tenantID, 0, 1)
					adp := adapter()
					store, err, errCh := eventstore.New(adp,
						eventstore.WithProjection(projectionTypeOne),
					)

					if err != nil {
						panic("error in test case preparation" + err.Error())
					}

					err = <-errCh

					if err != nil {
						panic("error in test case preparation" + err.Error())
					}

					return store, []event.Projection{projectionTypeOne}
				},
			},
			wantState: map[string]event.ProjectionState{
				"projection_1": {
					TenantID:                tenantID,
					ProjectionID:            "projection_1",
					State:                   string(projection.Running),
					UpdatedAt:               time.Now(),
					HPatchStrategy:          event.Error,
					ExecutionTimeOut:        20 * time.Second,
					PreparationTimeOut:      20 * time.Second,
					FinishingTimeOut:        20 * time.Second,
					RebuildExecutionTimeOut: 20 * time.Second,
					InputQueueLength:        100,
					ProjectionType:          event.ECS,
					RetryDurations:          nil,
				},
			},
		},
		{
			name: "single projection with different options",
			args: args{
				ctx: context.Background(),
				aggregates: []event.AggregateWithEventSourcingSupport{
					newForTestConcreteAggregate("1", "Name", 0, tenantID, []event.IEvent{
						ForTestMakeCreateEvent("1", tenantID, time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
						ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
						ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
					}),
				},
				eventStore: func() (event.EventStore, []event.Projection) {
					projectionTypeOne := newTestProjectionTypeOne("projection_1", tenantID, 0, 1)
					adp := adapter()
					store, err, errCh := eventstore.New(adp,
						eventstore.WithProjection(projectionTypeOne),
						eventstore.WithProjectionType("projection_1", event.CCS),
						eventstore.WithHistoricalPatchStrategy("projection_1", event.Projected),
					)
					if err != nil {
						panic("error in test case preparation" + err.Error())
					}

					err = <-errCh
					if err != nil {
						panic("error in test case preparation" + err.Error())
					}

					return store, []event.Projection{projectionTypeOne}
				},
			},
			wantState: map[string]event.ProjectionState{
				"projection_1": {
					TenantID:                tenantID,
					ProjectionID:            "projection_1",
					State:                   string(projection.Running),
					UpdatedAt:               time.Now(),
					HPatchStrategy:          event.Projected,
					ExecutionTimeOut:        20 * time.Second,
					PreparationTimeOut:      20 * time.Second,
					FinishingTimeOut:        20 * time.Second,
					RebuildExecutionTimeOut: 20 * time.Second,
					InputQueueLength:        100,
					ProjectionType:          event.CCS,
					RetryDurations:          nil,
				},
			},
		},
		{
			name: "multiple projection with different options",
			args: args{
				ctx: context.Background(),
				aggregates: []event.AggregateWithEventSourcingSupport{
					newForTestConcreteAggregate("1", "Name", 0, tenantID, []event.IEvent{
						ForTestMakeCreateEvent("1", tenantID, time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
						ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
						ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
					}),
				},
				eventStore: func() (event.EventStore, []event.Projection) {
					projectionTypeOne1 := newTestProjectionTypeOne("projection_1", tenantID, 0, 1)
					projectionTypeOne2 := newTestProjectionTypeOne("projection_2", tenantID, 0, 1)
					adp := adapter()
					store, err, errCh := eventstore.New(adp,
						eventstore.WithProjection(projectionTypeOne1),
						eventstore.WithProjectionType("projection_1", event.CCS),
						eventstore.WithHistoricalPatchStrategy("projection_1", event.Projected),
						eventstore.WithProjection(projectionTypeOne2),
						eventstore.WithProjectionType("projection_2", event.ESS),
						eventstore.WithHistoricalPatchStrategy("projection_2", event.Manual),
					)
					if err != nil {
						panic("error in test case preparation")
					}

					//if _, err = store.StartProjection(context.Background(), tenantID, "projection_1"); err != nil {
					//	if err != nil {
					//		panic("error in test case preparation" + err.Error())
					//	}
					//}

					errCh, err = store.StartProjection(context.Background(), tenantID, "projection_2")

					if err != nil {
						if err != nil {
							panic("error in test case preparation: start" + err.Error())
						}
					}
					err = <-errCh
					if err != nil {
						panic("error in test case preparation: channel" + err.Error())
					}

					if err = store.StopProjection(context.Background(), tenantID, "projection_2"); err != nil {
						if err != nil {
							panic("error in test case preparation: stop" + err.Error())
						}
					}

					err = <-errCh
					if err != nil {
						panic("error in test case preparation")
					}

					return store, []event.Projection{projectionTypeOne1, projectionTypeOne2}
				},
			},
			wantState: map[string]event.ProjectionState{
				"projection_1": {
					TenantID:                tenantID,
					ProjectionID:            "projection_1",
					State:                   string(projection.Running),
					UpdatedAt:               time.Now(),
					HPatchStrategy:          event.Projected,
					ExecutionTimeOut:        20 * time.Second,
					PreparationTimeOut:      20 * time.Second,
					FinishingTimeOut:        20 * time.Second,
					RebuildExecutionTimeOut: 20 * time.Second,
					InputQueueLength:        100,
					ProjectionType:          event.CCS,
					RetryDurations:          nil,
				},
				"projection_2": {
					TenantID:                tenantID,
					ProjectionID:            "projection_2",
					State:                   string(projection.Stopped),
					UpdatedAt:               time.Now(),
					HPatchStrategy:          event.Manual,
					ExecutionTimeOut:        20 * time.Second,
					PreparationTimeOut:      20 * time.Second,
					FinishingTimeOut:        20 * time.Second,
					RebuildExecutionTimeOut: 20 * time.Second,
					InputQueueLength:        100,
					ProjectionType:          event.ESS,
					RetryDurations:          nil,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer cleanUp()
			eventStore, projs := tt.args.eventStore()

			errCh, err := event.SaveAggregates(tt.args.ctx, eventStore, tt.args.aggregates...)
			if err != nil {
				t.Errorf("GetProjectionStates() aggregate save failed gotError = %v, wantErr %v", err, nil)
			}

			<-errCh
			for _, proj := range projs {
				gotState, errIntern := eventStore.GetProjectionStates(tt.args.ctx, tenantID, proj.ID())
				if errIntern != nil {
					t.Errorf("GetProjectionStates() gotErr = %v, wantErr %v", errIntern, nil)
					return
				}

				if err = assertProjectionState(gotState[0], tt.wantState[proj.ID()]); err != nil {
					t.Errorf("wrong state \n gotState = %v\n wantState = %v", gotState[0], tt.wantState[proj.ID()])
				}
			}
		})
	}
}

func testGetAggregatesEventsSortAndSearch(t *testing.T, adapter func() persistence.Port, cleanUp func()) {
	cleanUp()
	tenantID := "0000-0000-0000"

	aggregateRepro := []event.AggregateWithEventSourcingSupport{
		newForTestConcreteAggregate("1", "Name", 0, tenantID, []event.IEvent{
			ForTestMakeCreateEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 0, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 0, time.UTC)),
			ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
			ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
			ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC)),
			ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 5, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 5, time.UTC)),
			ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 7, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 7, time.UTC)),
		}),
		newForTestConcreteAggregate("12", "Name2", 0, tenantID, []event.IEvent{
			ForTestMakeCreateEvent("12", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 0, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 0, time.UTC)),
			ForTestMakeEvent("12", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
			ForTestMakeEvent("12", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
			ForTestMakeEvent("12", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC)),
			ForTestMakeEvent("12", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 7, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 7, time.UTC)),
		}),
		newForTestConcreteAggregate("3", "Name2", 0, tenantID, []event.IEvent{
			ForTestMakeCreateEvent("3", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 0, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 0, time.UTC)),
			ForTestMakeEvent("3", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 5, time.UTC)),
			ForTestMakeEvent("3", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC)),
			ForTestMakeEvent("3", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 5, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
		})}

	type args struct {
		ctx        context.Context
		aggregate  []event.AggregateWithEventSourcingSupport
		eventStore func() event.EventStore
		page       event.PageDTO
	}
	tests := []struct {
		name            string
		args            args
		wantErr         bool
		wantResultCount int
		assertResult    func(rows []event.PersistenceEvent) bool
	}{
		{
			name: "page size 0 / no search / no sort",
			args: args{
				ctx:       context.Background(),
				aggregate: aggregateRepro,
				page:      event.PageDTO{},
				eventStore: func() event.EventStore {
					store, err, _ := eventstore.New(adapter())
					if err != nil {
						panic("error in test case preparation")
					}
					return store
				},
			},
			wantErr:         false,
			wantResultCount: 15,
			assertResult:    func(rows []event.PersistenceEvent) bool { return true },
		},
		{
			name: "page size 0 / search all match / no sort",
			args: args{
				ctx:       context.Background(),
				aggregate: aggregateRepro,
				page: event.PageDTO{
					PageSize:   0,
					SortFields: nil,
					SearchFields: []event.SearchField{
						{
							Name:     event.SearchAggregateType,
							Value:    ".*",
							Operator: event.SearchMatch,
						},
						{
							Name:     event.SearchAggregateID,
							Value:    "12",
							Operator: event.SearchMatch,
						},
						{
							Name:     event.SearchAggregateVersion,
							Value:    ".*",
							Operator: event.SearchMatch,
						},
						{
							Name:     event.SearchAggregateClass,
							Value:    ".*",
							Operator: event.SearchMatch,
						},
						{
							Name:     event.SearchAggregateEventType,
							Value:    ".*",
							Operator: event.SearchMatch,
						},
						{
							Name:     event.SearchValidTime,
							Value:    ".*",
							Operator: event.SearchMatch,
						},
						{
							Name:     event.SearchTransactionTime,
							Value:    ".*",
							Operator: event.SearchMatch,
						},
						{
							Name:     event.SearchValidTime,
							Value:    ".*",
							Operator: event.SearchMatch,
						},
						{
							Name:     event.SearchData,
							Value:    ".*",
							Operator: event.SearchMatch,
						},
					},
					Values:     nil,
					IsBackward: false,
				},
				eventStore: func() event.EventStore {
					store, err, _ := eventstore.New(adapter())
					if err != nil {
						panic("error in test case preparation")
					}
					return store
				},
			},
			wantErr:         false,
			wantResultCount: 5,
			assertResult:    func(rows []event.PersistenceEvent) bool { return true },
		},
		{
			name: "page size 0 / search all operator / no sort",
			args: args{
				ctx:       context.Background(),
				aggregate: aggregateRepro,
				page: event.PageDTO{
					PageSize:   0,
					SortFields: nil,
					SearchFields: []event.SearchField{
						{
							Name:     event.SearchAggregateType,
							Value:    "forTestConcreteAggregate",
							Operator: event.SearchEqual,
						},
						{
							Name:     event.SearchAggregateID,
							Value:    "3",
							Operator: event.SearchGreaterThanOrEqual,
						},
						{
							Name:     event.SearchAggregateVersion,
							Value:    "1",
							Operator: event.SearchGreaterThan,
						},
						{
							Name:     event.SearchAggregateClass,
							Value:    ".*",
							Operator: event.SearchMatch,
						},
						{
							Name:     event.SearchValidTime,
							Value:    time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC).Format(time.RFC3339),
							Operator: event.SearchGreaterThanOrEqual,
						},
						{
							Name:     event.SearchTransactionTime,
							Value:    time.Date(2022, 1, 1, 1, 1, 1, 5, time.UTC).Format(time.RFC3339),
							Operator: event.SearchLessThanOrEqual,
						},
					},
					Values:     nil,
					IsBackward: false,
				},
				eventStore: func() event.EventStore {
					store, err, _ := eventstore.New(adapter())
					if err != nil {
						panic("error in test case preparation")
					}
					return store
				},
			},
			wantErr:         false,
			wantResultCount: 3,
			assertResult:    func(rows []event.PersistenceEvent) bool { return true },
		},
		{
			name: "page size 0 / search all match / no sort",
			args: args{
				ctx:       context.Background(),
				aggregate: aggregateRepro,
				page: event.PageDTO{
					PageSize:   0,
					SortFields: nil,
					SearchFields: []event.SearchField{
						{
							Name:     event.SearchAggregateType,
							Value:    ".*",
							Operator: event.SearchMatch,
						},
						{
							Name:     event.SearchAggregateID,
							Value:    "12",
							Operator: event.SearchMatch,
						},
						{
							Name:     event.SearchAggregateVersion,
							Value:    ".*",
							Operator: event.SearchMatch,
						},
						{
							Name:     event.SearchAggregateClass,
							Value:    ".*",
							Operator: event.SearchMatch,
						},
						{
							Name:     event.SearchAggregateEventType,
							Value:    ".*",
							Operator: event.SearchMatch,
						},
						{
							Name:     event.SearchValidTime,
							Value:    ".*",
							Operator: event.SearchMatch,
						},
						{
							Name:     event.SearchTransactionTime,
							Value:    ".*",
							Operator: event.SearchMatch,
						},
						{
							Name:     event.SearchValidTime,
							Value:    ".*",
							Operator: event.SearchMatch,
						},
						{
							Name:     event.SearchData,
							Value:    ".*",
							Operator: event.SearchMatch,
						},
					},
					Values:     nil,
					IsBackward: false,
				},
				eventStore: func() event.EventStore {
					store, err, _ := eventstore.New(adapter())
					if err != nil {
						panic("error in test case preparation")
					}
					return store
				},
			},
			wantErr:         false,
			wantResultCount: 5,
			assertResult:    func(rows []event.PersistenceEvent) bool { return true },
		},
		{
			name: "page size 0 / no search / all sort fields",
			args: args{
				ctx:       context.Background(),
				aggregate: aggregateRepro,
				page: event.PageDTO{
					PageSize: 0,
					SortFields: []event.SortField{
						{
							Name:   event.SortAggregateType,
							IsDesc: false,
						},
						{
							Name:   event.SortAggregateEventType,
							IsDesc: false,
						},
						{
							Name:   event.SortAggregateClass,
							IsDesc: false,
						},
						{
							Name:   event.SortAggregateID,
							IsDesc: true,
						},
						{
							Name:   event.SortAggregateVersion,
							IsDesc: true,
						},
						{
							Name:   event.SortValidTime,
							IsDesc: true,
						},
						{
							Name:   event.SortTransactionTime,
							IsDesc: true,
						},
					},
					SearchFields: nil,
					Values:       nil,
					IsBackward:   false,
				},
				eventStore: func() event.EventStore {
					store, err, _ := eventstore.New(adapter())
					if err != nil {
						panic("error in test case preparation")
					}
					return store
				},
			},
			wantErr:         false,
			wantResultCount: 15,
			assertResult: func(rows []event.PersistenceEvent) bool {
				return rows[0].AggregateID == "3" && rows[0].Version == 1 &&
					rows[len(rows)-1].AggregateID == "1" && rows[len(rows)-1].Version == 2
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			eventStore := tt.args.eventStore()
			defer cleanUp()

			if _, err := event.SaveAggregates(tt.args.ctx, eventStore, tt.args.aggregate...); err != nil {
				t.Errorf("SaveAggregates() error = %v, wantErr %v", err, tt.wantErr)
			}

			result, _, err := eventStore.GetAggregatesEvents(tt.args.ctx, tenantID, tt.args.page)
			assert.NoError(t, err)
			// assert filter and sorting
			assert.Truef(t, tt.assertResult(result), "wrong first page result in test %s", tt.name)
			assert.Equal(t, tt.wantResultCount, len(result))
		})
	}
}

func testGetAggregatesEventsPaginated(t *testing.T, adapter func() persistence.Port, cleanUp func()) {
	cleanUp()
	tenantID := "0000-0000-0000"

	aggregateRepro := []event.AggregateWithEventSourcingSupport{
		newForTestConcreteAggregate("1", "Name", 0, tenantID, []event.IEvent{
			ForTestMakeCreateEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 0, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 0, time.UTC)),
			ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
			ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
			ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC)),
			ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 5, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 5, time.UTC)),
			ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 7, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 7, time.UTC)),
		}),
		newForTestConcreteAggregate("12", "Name2", 0, tenantID, []event.IEvent{
			ForTestMakeCreateEvent("12", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 0, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 0, time.UTC)),
			ForTestMakeEvent("12", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
			ForTestMakeEvent("12", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
			ForTestMakeEvent("12", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC)),
			ForTestMakeEvent("12", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 7, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 7, time.UTC)),
		}),
		newForTestConcreteAggregate("3", "Name2", 0, tenantID, []event.IEvent{
			ForTestMakeCreateEvent("3", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 0, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 0, time.UTC)),
			ForTestMakeEvent("3", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 5, time.UTC)),
			ForTestMakeEvent("3", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC)),
			ForTestMakeEvent("3", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 5, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
		})}

	type args struct {
		ctx        context.Context
		aggregate  []event.AggregateWithEventSourcingSupport
		eventStore func() event.EventStore
		initPage   event.PageDTO
	}
	tests := []struct {
		name               string
		args               args
		firstPage          int
		wantedCursor       func(pages event.PagesDTO) event.PageDTO
		assertOnFirstPage  func(rows []event.PersistenceEvent) bool
		wantedPage         int
		assertOnWantedPage func(rows []event.PersistenceEvent) bool
	}{
		{
			name: "page size 1 / search / sort / forward",
			args: args{
				ctx:       context.Background(),
				aggregate: aggregateRepro,
				initPage: event.PageDTO{
					PageSize: 1,
					SortFields: []event.SortField{
						{
							Name:   event.SortAggregateVersion,
							IsDesc: true,
						},
					},
					SearchFields: []event.SearchField{
						{
							Name:     event.SearchAggregateID,
							Value:    "3",
							Operator: event.SearchEqual,
						},
					},
					Values:     nil,
					IsBackward: false,
				},
				eventStore: func() event.EventStore {
					store, err, _ := eventstore.New(adapter())
					if err != nil {
						panic("error in test case preparation")
					}
					return store
				},
			},
			firstPage: 0,
			wantedCursor: func(pages event.PagesDTO) event.PageDTO {
				return pages.Next
			},
			assertOnFirstPage: func(rows []event.PersistenceEvent) bool {
				return rows[0].AggregateID == "3" && rows[0].Version == 4
			},
			wantedPage: 3,
			assertOnWantedPage: func(rows []event.PersistenceEvent) bool {
				return rows[0].AggregateID == "3" && rows[0].Version == 1
			},
		},
		{
			name: "page size 2 / search / sort / forward",
			args: args{
				ctx:       context.Background(),
				aggregate: aggregateRepro,
				initPage: event.PageDTO{
					PageSize: 2,
					SortFields: []event.SortField{
						{
							Name:   event.SortAggregateVersion,
							IsDesc: true,
						},
					},
					SearchFields: []event.SearchField{
						{
							Name:     event.SearchAggregateID,
							Value:    "3",
							Operator: event.SearchEqual,
						},
					},
					Values:     nil,
					IsBackward: false,
				},
				eventStore: func() event.EventStore {
					store, err, _ := eventstore.New(adapter())
					if err != nil {
						panic("error in test case preparation")
					}
					return store
				},
			},
			firstPage: 0,
			wantedCursor: func(pages event.PagesDTO) event.PageDTO {
				return pages.Next
			},
			assertOnFirstPage: func(rows []event.PersistenceEvent) bool {
				return rows[0].AggregateID == "3" && rows[0].Version == 4
			},
			wantedPage: 1,
			assertOnWantedPage: func(rows []event.PersistenceEvent) bool {
				return rows[0].AggregateID == "3" && rows[0].Version == 2
			},
		},
		{
			name: "page size 1 / search / sort / backward",
			args: args{
				ctx:       context.Background(),
				aggregate: aggregateRepro,
				initPage: event.PageDTO{
					PageSize: 1,
					SortFields: []event.SortField{
						{
							Name:   event.SortAggregateVersion,
							IsDesc: true,
						},
					},
					SearchFields: []event.SearchField{
						{
							Name:     event.SearchAggregateID,
							Value:    "3",
							Operator: event.SearchEqual,
						},
					},
					Values:     nil,
					IsBackward: false,
				},
				eventStore: func() event.EventStore {
					store, err, _ := eventstore.New(adapter())
					if err != nil {
						panic("error in test case preparation")
					}
					return store
				},
			},
			firstPage: 3,
			wantedCursor: func(pages event.PagesDTO) event.PageDTO {
				return pages.Previous
			},
			assertOnFirstPage: func(rows []event.PersistenceEvent) bool {
				return rows[0].AggregateID == "3" && rows[0].Version == 1
			},
			wantedPage: 0,
			assertOnWantedPage: func(rows []event.PersistenceEvent) bool {
				return rows[0].AggregateID == "3" && rows[0].Version == 4
			},
		},
		{
			name: "page size 2 / search / sort / backward",
			args: args{
				ctx:       context.Background(),
				aggregate: aggregateRepro,
				initPage: event.PageDTO{
					PageSize: 2,
					SortFields: []event.SortField{
						{
							Name:   event.SortAggregateVersion,
							IsDesc: true,
						},
					},
					SearchFields: []event.SearchField{
						{
							Name:     event.SearchAggregateID,
							Value:    "3",
							Operator: event.SearchEqual,
						},
					},
					Values:     nil,
					IsBackward: false,
				},
				eventStore: func() event.EventStore {
					store, err, _ := eventstore.New(adapter())
					if err != nil {
						panic("error in test case preparation")
					}
					return store
				},
			},
			firstPage: 1,
			wantedCursor: func(pages event.PagesDTO) event.PageDTO {
				return pages.Previous
			},
			assertOnFirstPage: func(rows []event.PersistenceEvent) bool {
				return rows[0].AggregateID == "3" && rows[0].Version == 2
			},
			wantedPage: 0,
			assertOnWantedPage: func(rows []event.PersistenceEvent) bool {
				return rows[0].AggregateID == "3" && rows[0].Version == 4
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			eventStore := tt.args.eventStore()
			defer cleanUp()

			if _, err := event.SaveAggregates(tt.args.ctx, eventStore, tt.args.aggregate...); err != nil {
				assert.NoErrorf(t, err, "SaveAggregates() error = %v, wantErr %v", err, nil)
			}

			result, pages, err := eventStore.GetAggregatesEvents(tt.args.ctx, tenantID, tt.args.initPage)
			assert.NoErrorf(t, err, "GetAggregatesEvents() error = %v, wantErr %v", err, nil)

			// navigate to start page
			nextPage := pages.Next
			for i := 0; i < tt.firstPage; i++ {
				result, pages, err = eventStore.GetAggregatesEvents(tt.args.ctx, tenantID, nextPage)
				nextPage = pages.Next
			}
			assert.Truef(t, tt.assertOnFirstPage(result), "wrong first page result in test %s", tt.name)

			// paginate and navigate to wanted page
			maxPageCount := int(math.Abs(float64(tt.wantedPage - tt.firstPage)))
			for i := 0; i < maxPageCount; i++ {
				nextPage = tt.wantedCursor(pages)
				result, pages, err = eventStore.GetAggregatesEvents(tt.args.ctx, tenantID, nextPage)
			}
			assert.Truef(t, tt.assertOnWantedPage(result), "wrong wanted page result in test %s", tt.name)
		})
	}
}
