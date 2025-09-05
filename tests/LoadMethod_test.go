package tests

import (
	"context"
	"github.com/global-soft-ba/go-eventstore"
	"github.com/global-soft-ba/go-eventstore/tests/testdata"
	"sort"
	"testing"
	"time"
)

func testLoadAggregateAsAt(t *testing.T, eventStoreFactory func() event.EventStore, cleanUp func()) {

	var tenantID = "0000-0000-0000"

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
		name            string
		args            args
		wantEventStream []event.IEvent
		wantVersion     int
		wantErr         bool
	}{
		{
			name: "GetOrCreate only first event",
			args: args{
				ctx:            context.Background(),
				aggregateType:  "forTestConcreteAggregate",
				aggregateID:    "1",
				tenantID:       tenantID,
				projectionTime: time.Date(2021, 1, 1, 1, 1, 1, 0, time.UTC),
				eventStore: func() event.EventStore {
					_, store := ForTestGetFilledRepo(eventStoreFactory(), tenantID)
					return store
				},
			},
			wantEventStream: []event.IEvent{
				ForTestMakeCreateEvent("1", tenantID, time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
			},
			wantVersion: 5,
			wantErr:     false,
		},
		{
			name: "GetOrCreate first three events - no patch applied",
			args: args{
				ctx:            context.Background(),
				aggregateType:  "forTestConcreteAggregate",
				aggregateID:    "1",
				tenantID:       tenantID,
				projectionTime: time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC),
				eventStore: func() event.EventStore {
					_, store := ForTestGetFilledRepo(eventStoreFactory(), tenantID)
					return store
				}},
			wantEventStream: []event.IEvent{
				ForTestMakeCreateEvent("1", tenantID, time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
				ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
				ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
			},
			wantVersion: 5,
			wantErr:     false,
		},
		{
			name: "all four events - patch is applied, but not future patch",
			args: args{
				ctx:            context.Background(),
				aggregateType:  "forTestConcreteAggregate",
				aggregateID:    "1",
				tenantID:       tenantID,
				projectionTime: time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC),
				eventStore: func() event.EventStore {
					_, store := ForTestGetFilledRepo(eventStoreFactory(), tenantID)
					return store
				}},
			wantEventStream: []event.IEvent{
				ForTestMakeCreateEvent("1", tenantID, time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
				ForTestMakePatchEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 0, time.UTC)),
				ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
				ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
			},
			wantVersion: 5,
			wantErr:     false,
		},
		{
			name: "aggregate does not exist",
			args: args{
				ctx:            context.Background(),
				aggregateType:  "forTestConcreteAggregate",
				aggregateID:    "111111",
				tenantID:       tenantID,
				projectionTime: time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC),
				eventStore: func() event.EventStore {
					_, store := ForTestGetFilledRepo(eventStoreFactory(), tenantID)
					return store
				}},
			wantEventStream: nil,
			wantVersion:     0,
			wantErr:         true,
		},
		{
			name: "legacy aggregate is loaded correctly",
			args: args{
				ctx:            context.Background(),
				aggregateType:  "forTestConcreteAggregate",
				aggregateID:    "1",
				tenantID:       tenantID,
				projectionTime: time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC),
				eventStore: func() event.EventStore {
					_, store := ForTestGetFilledRepoWithLegacy(eventStoreFactory(), tenantID)
					return store
				}},
			wantEventStream: []event.IEvent{
				ForTestMakeCreateEvent3("1", tenantID, time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), "foobar"),
			},
			wantVersion: 1,
			wantErr:     false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			eventStore := tt.args.eventStore()
			defer cleanUp()
			gotEventStream, gotVersion, err := event.LoadAggregateAsAt(tt.args.ctx, tt.args.tenantID, tt.args.aggregateType, tt.args.aggregateID, tt.args.projectionTime, eventStore)
			if (err != nil) != tt.wantErr {
				t.Errorf("LoadAggregateAsAt() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			testdata.AssertEqualStream(t, gotEventStream, tt.wantEventStream)
			if gotVersion != tt.wantVersion {
				t.Errorf("LoadAggregateAsAt() gotVersion = %v, want %v", gotVersion, tt.wantVersion)
			}
		})
	}
}

func testLoadAggregateAsOf(t *testing.T, eventStoreFactory func() event.EventStore, cleanUp func()) {
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
		name            string
		args            args
		wantEventStream []event.IEvent
		wantVersion     int
		wantErr         bool
	}{
		{
			name: "GetOrCreate only first event",
			args: args{
				ctx:            context.Background(),
				aggregateType:  "forTestConcreteAggregate",
				aggregateID:    "1",
				tenantID:       "0000-0000-0000",
				projectionTime: time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC),
				eventStore: func() event.EventStore {
					_, store := ForTestGetFilledRepo(eventStoreFactory(), "0000-0000-0000")
					return store
				}},
			wantEventStream: []event.IEvent{
				ForTestMakeCreateEvent("1", "0000-0000-0000", time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
			},
			wantVersion: 5,
			wantErr:     false,
		},
		{
			name: "GetOrCreate first event and patch",
			args: args{
				ctx:            context.Background(),
				aggregateType:  "forTestConcreteAggregate",
				aggregateID:    "1",
				tenantID:       "0000-0000-0000",
				projectionTime: time.Date(2021, 1, 1, 1, 1, 1, 0, time.UTC),
				eventStore: func() event.EventStore {
					_, store := ForTestGetFilledRepo(eventStoreFactory(), "0000-0000-0000")
					return store
				}},
			wantEventStream: []event.IEvent{
				ForTestMakeCreateEvent("1", "0000-0000-0000", time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
				ForTestMakePatchEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 0, time.UTC)),
			},
			wantVersion: 5,
			wantErr:     false,
		},
		{
			name: "all four events - patch is applied, but not future patch",
			args: args{
				ctx:            context.Background(),
				aggregateType:  "forTestConcreteAggregate",
				aggregateID:    "1",
				tenantID:       "0000-0000-0000",
				projectionTime: time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC),
				eventStore: func() event.EventStore {
					_, store := ForTestGetFilledRepo(eventStoreFactory(), "0000-0000-0000")
					return store
				}},
			wantEventStream: []event.IEvent{
				ForTestMakeCreateEvent("1", "0000-0000-0000", time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
				ForTestMakePatchEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 0, time.UTC)),
				ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
				ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
			},
			wantVersion: 5,
			wantErr:     false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			eventStore := tt.args.eventStore()
			defer cleanUp()
			gotEventStream, gotVersion, err := event.LoadAggregateAsOf(tt.args.ctx, tt.args.tenantID, tt.args.aggregateType, tt.args.aggregateID, tt.args.projectionTime, eventStore)
			if (err != nil) != tt.wantErr {
				t.Errorf("LoadAggregateAsAt() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			testdata.AssertEqualStream(t, gotEventStream, tt.wantEventStream)
			if gotVersion != tt.wantVersion {
				t.Errorf("LoadAggregateAsAt() gotVersion = %v, want %v", gotVersion, tt.wantVersion)
			}
		})
	}
}

func testLoadAggregateAsOfTill(t *testing.T, eventStoreFactory func() event.EventStore, cleanUp func()) {
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
		aggregateID    string
		tenantID       string
		projectionTime time.Time
		reportTime     time.Time
		eventStore     func() event.EventStore
	}
	tests := []struct {
		name            string
		args            args
		wantEventStream []event.IEvent
		wantVersion     int
		wantErr         bool
	}{
		{
			name: "first event only - patch wasn't known at that time, second event is known but not applied",
			args: args{
				ctx:            context.Background(),
				aggregateType:  "forTestConcreteAggregate",
				aggregateID:    "1",
				tenantID:       "0000-0000-0000",
				projectionTime: time.Date(2021, 1, 1, 1, 1, 1, 0, time.UTC),
				reportTime:     time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC),
				eventStore:     eventStore,
			},
			wantEventStream: []event.IEvent{
				ForTestMakeCreateEvent("1", "0000-0000-0000", time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
			},
			wantVersion: 5,
			wantErr:     false,
		},
		{
			name: "GetOrCreate first event and patch - patch was known at report time",
			args: args{
				ctx:            context.Background(),
				aggregateType:  "forTestConcreteAggregate",
				aggregateID:    "1",
				tenantID:       "0000-0000-0000",
				projectionTime: time.Date(2021, 1, 1, 1, 1, 1, 0, time.UTC),
				reportTime:     time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC),
				eventStore:     eventStore,
			},
			wantEventStream: []event.IEvent{
				ForTestMakeCreateEvent("1", "0000-0000-0000", time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
				ForTestMakePatchEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 0, time.UTC)),
			},
			wantVersion: 5,
			wantErr:     false,
		},
		{
			name: "all four events - patch is applied, but not future patch",
			args: args{
				ctx:            context.Background(),
				aggregateType:  "forTestConcreteAggregate",
				aggregateID:    "1",
				tenantID:       "0000-0000-0000",
				projectionTime: time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC),
				reportTime:     time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC),
				eventStore:     eventStore,
			},
			wantEventStream: []event.IEvent{
				ForTestMakeCreateEvent("1", "0000-0000-0000", time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
				ForTestMakePatchEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 0, time.UTC)),
				ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
				ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
			},
			wantVersion: 5,
			wantErr:     false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			eventstore := tt.args.eventStore()
			defer cleanUp()
			gotEventStream, gotVersion, err := event.LoadAggregateAsOfTill(tt.args.ctx, tt.args.tenantID, tt.args.aggregateType, tt.args.aggregateID, tt.args.projectionTime, tt.args.reportTime, eventstore)
			if (err != nil) != tt.wantErr {
				t.Errorf("LoadAggregateAsAt() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			testdata.AssertEqualStream(t, gotEventStream, tt.wantEventStream)
			if gotVersion != tt.wantVersion {
				t.Errorf("LoadAggregateAsAt() gotVersion = %v, want %v", gotVersion, tt.wantVersion)
			}
		})
	}
}

func testLoadAllAggregatesOfAggregateAsAt(t *testing.T, eventStoreFactory func() event.EventStore, cleanUp func()) {
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
		name             string
		args             args
		wantEventStreams []event.EventStream
		wantErr          bool
	}{
		{
			name: "GetOrCreate first two nanoseconds - no patches",
			args: args{
				ctx:            context.Background(),
				aggregateType:  "forTestConcreteAggregate",
				tenantID:       "0000-0000-0000",
				projectionTime: time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC),
				eventStore:     eventStore,
			},
			wantEventStreams: []event.EventStream{
				{
					Stream: []event.IEvent{
						ForTestMakeCreateEvent("1", "0000-0000-0000", time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
						ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
						ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
					},
					Version: 5,
				},
				{
					Stream: []event.IEvent{
						ForTestMakeCreateEvent("2", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
					},
					Version: 1,
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := tt.args.eventStore()
			defer cleanUp()
			gotEventStreams, err := event.LoadAllOfAggregateTypeAsAt(tt.args.ctx, tt.args.tenantID, tt.args.aggregateType, tt.args.projectionTime, store)
			if (err != nil) != tt.wantErr {
				t.Errorf("LoadAllOfAggregateTypeAsAt() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			sort.Slice(gotEventStreams, func(i, j int) bool {
				return gotEventStreams[i].Stream[0].GetValidTime().Before(gotEventStreams[j].Stream[0].GetValidTime())
			})
			testdata.AssertEqualEventStreams(t, gotEventStreams, tt.wantEventStreams)
		})
	}
}

func testLoadAllAggregatesOfAggregateAsOf(t *testing.T, eventStoreFactory func() event.EventStore, cleanUp func()) {
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
		name             string
		args             args
		wantEventStreams []event.EventStream
		wantErr          bool
	}{
		{
			name: "GetOrCreate first two nanoseconds - with patches",
			args: args{
				ctx:            context.Background(),
				aggregateType:  "forTestConcreteAggregate",
				tenantID:       "0000-0000-0000",
				projectionTime: time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC),
				eventStore:     eventStore,
			},
			wantEventStreams: []event.EventStream{
				{
					Stream: []event.IEvent{
						ForTestMakeCreateEvent("1", "0000-0000-0000", time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
						ForTestMakePatchEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 0, time.UTC)),
						ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
						ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
					},
					Version: 5,
				},
				{
					Stream: []event.IEvent{
						ForTestMakeCreateEvent("2", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
					},
					Version: 1,
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := tt.args.eventStore()
			defer cleanUp()
			gotEventStreams, err := event.LoadAllOfAggregateTypeAsOf(tt.args.ctx, tt.args.tenantID, tt.args.aggregateType, tt.args.projectionTime, store)
			if (err != nil) != tt.wantErr {
				t.Errorf("LoadAllOfAggregateTypeAsOf() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			sort.Slice(gotEventStreams, func(i, j int) bool {
				return gotEventStreams[i].Stream[0].GetValidTime().Before(gotEventStreams[j].Stream[0].GetValidTime())
			})
			testdata.AssertEqualEventStreams(t, gotEventStreams, tt.wantEventStreams)

		})
	}
}

func testLoadAllAggregatesOfAggregateAsOfTill(t *testing.T, eventStoreFactory func() event.EventStore, cleanUp func()) {
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
		reportTime     time.Time
		eventStore     func() event.EventStore
	}
	tests := []struct {
		name             string
		args             args
		wantEventStreams []event.EventStream
		wantErr          bool
	}{
		{
			name: "GetOrCreate first two nanoseconds - patches not known yet",
			args: args{
				ctx:            context.Background(),
				aggregateType:  "forTestConcreteAggregate",
				tenantID:       "0000-0000-0000",
				projectionTime: time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC),
				reportTime:     time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC),
				eventStore:     eventStore,
			},
			wantEventStreams: []event.EventStream{
				{
					Stream: []event.IEvent{
						ForTestMakeCreateEvent("1", "0000-0000-0000", time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
						ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
					},
					Version: 5,
				},
				{
					Stream: []event.IEvent{
						ForTestMakeCreateEvent("2", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
					},
					Version: 1,
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := tt.args.eventStore()
			defer cleanUp()
			gotEventStreams, err := event.LoadAllOfAggregateTypeAsOfTill(tt.args.ctx, tt.args.tenantID, tt.args.aggregateType, tt.args.projectionTime, tt.args.reportTime, store)
			if (err != nil) != tt.wantErr {
				t.Errorf("LoadAllOfAggregateTypeAsOfTill() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			sort.Slice(gotEventStreams, func(i, j int) bool {
				return gotEventStreams[i].Stream[0].GetValidTime().Before(gotEventStreams[j].Stream[0].GetValidTime())
			})

			testdata.AssertEqualEventStreams(t, gotEventStreams, tt.wantEventStreams)

		})
	}
}

func testLoadAllAggregatesAsAt(t *testing.T, eventStoreFactory func() event.EventStore, cleanUp func()) {
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
		return es
	}
	type args struct {
		ctx            context.Context
		tenantID       string
		projectionTime time.Time
		eventStore     func() event.EventStore
	}
	tests := []struct {
		name             string
		args             args
		wantEventStreams []event.EventStream
		wantErr          bool
	}{
		{
			name: "GetOrCreate first two nanoseconds - no patches",
			args: args{
				ctx:            context.Background(),
				tenantID:       "0000-0000-0000",
				projectionTime: time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC),
				eventStore:     eventStore,
			},
			wantEventStreams: []event.EventStream{
				{
					Stream: []event.IEvent{
						ForTestMakeCreateEvent("1", "0000-0000-0000", time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
						ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
						ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
					},
					Version: 5,
				},
				{
					Stream: []event.IEvent{
						ForTestMakeCreateEvent("2", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
					},
					Version: 1,
				},
				{
					Stream: []event.IEvent{
						ForTestMakeCreateEvent2("3", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
					},
					Version: 1,
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := tt.args.eventStore()
			defer cleanUp()
			gotEventStreams, err := event.LoadAllAggregatesAsAt(tt.args.ctx, tt.args.tenantID, tt.args.projectionTime, store)
			if (err != nil) != tt.wantErr {
				t.Errorf("LoadAllAggregatesAsAt() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			sort.Slice(gotEventStreams, func(i, j int) bool {
				return gotEventStreams[i].Stream[0].GetValidTime().Before(gotEventStreams[j].Stream[0].GetValidTime())
			})
			testdata.AssertEqualEventStreams(t, gotEventStreams, tt.wantEventStreams)
		})
	}
}

func testLoadAllAggregatesAsOf(t *testing.T, eventStoreFactory func() event.EventStore, cleanUp func()) {
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
		return es
	}

	type args struct {
		ctx            context.Context
		tenantID       string
		projectionTime time.Time
		eventStore     func() event.EventStore
	}
	tests := []struct {
		name             string
		args             args
		wantEventStreams []event.EventStream
		wantErr          bool
	}{
		{
			name: "GetOrCreate first two nanoseconds - patch applied",
			args: args{
				ctx:            context.Background(),
				tenantID:       "0000-0000-0000",
				projectionTime: time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC),
				eventStore:     eventStore,
			},
			wantEventStreams: []event.EventStream{
				{
					Stream: []event.IEvent{
						ForTestMakeCreateEvent("1", "0000-0000-0000", time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
						ForTestMakePatchEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 0, time.UTC)),
						ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
						ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
					},
					Version: 5,
				},
				{
					Stream: []event.IEvent{
						ForTestMakeCreateEvent("2", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
					},
					Version: 1,
				},
				{
					Stream: []event.IEvent{
						ForTestMakeCreateEvent2("3", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
					},
					Version: 1,
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := tt.args.eventStore()
			defer cleanUp()
			gotEventStreams, err := event.LoadAllAggregatesAsOf(tt.args.ctx, tt.args.tenantID, tt.args.projectionTime, store)
			if (err != nil) != tt.wantErr {
				t.Errorf("LoadAllAggregatesAsAt() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			sort.Slice(gotEventStreams, func(i, j int) bool {
				return gotEventStreams[i].Stream[0].GetValidTime().Before(gotEventStreams[j].Stream[0].GetValidTime())
			})
			testdata.AssertEqualEventStreams(t, gotEventStreams, tt.wantEventStreams)
		})
	}
}

func testLoadAllAggregatesAsOfTill(t *testing.T, eventStoreFactory func() event.EventStore, cleanUp func()) {
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
		return es
	}

	type args struct {
		ctx            context.Context
		tenantID       string
		projectionTime time.Time
		reportTime     time.Time
		eventStore     func() event.EventStore
	}
	tests := []struct {
		name             string
		args             args
		wantEventStreams []event.EventStream
		wantErr          bool
	}{
		{
			name: "GetOrCreate first two nanoseconds - no patch and no third aggregate",
			args: args{
				ctx:            context.Background(),
				tenantID:       "0000-0000-0000",
				projectionTime: time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC),
				reportTime:     time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC),
				eventStore:     eventStore,
			},
			wantEventStreams: []event.EventStream{
				{
					Stream: []event.IEvent{
						ForTestMakeCreateEvent("1", "0000-0000-0000", time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
						ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
					},
					Version: 5,
				},
				{
					Stream: []event.IEvent{
						ForTestMakeCreateEvent("2", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
					},
					Version: 1,
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := tt.args.eventStore()
			defer cleanUp()
			gotEventStreams, err := event.LoadAllAggregatesAsOfTill(tt.args.ctx, tt.args.tenantID, tt.args.projectionTime, tt.args.reportTime, store)
			if (err != nil) != tt.wantErr {
				t.Errorf("LoadAllAggregatesAsAt() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			sort.Slice(gotEventStreams, func(i, j int) bool {
				return gotEventStreams[i].Stream[0].GetValidTime().Before(gotEventStreams[j].Stream[0].GetValidTime())
			})
			testdata.AssertEqualEventStreams(t, gotEventStreams, tt.wantEventStreams)
		})
	}
}
