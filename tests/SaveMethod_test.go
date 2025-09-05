package tests

import (
	"context"
	"errors"
	"github.com/global-soft-ba/go-eventstore"
	"github.com/global-soft-ba/go-eventstore/eventstore"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/port/persistence"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/port/persistence/transactor"
	"github.com/global-soft-ba/go-eventstore/eventstore/infrastructure/persistence/postgres"
	"github.com/global-soft-ba/go-eventstore/tests/testdata"
	"reflect"
	"sync"
	"testing"
	"time"
)

func testSaveAggregate(t *testing.T, trans transactor.Port, eventStoreFactory func(txCtx context.Context) event.EventStore, cleanUp func()) {
	cleanUp()
	//	t.Parallel()
	type args struct {
		ctx        context.Context
		aggregate  event.AggregateWithEventSourcingSupport
		eventStore func(txCtx context.Context) event.EventStore
	}
	tests := []struct {
		name            string
		args            args
		wantErr         bool
		wantEventStream []event.IEvent
		wantVersion     int
	}{
		{
			name: "save single event to blank aggregate",
			args: args{
				ctx: context.Background(),
				aggregate: newForTestConcreteAggregate("1", "Name", 0, "0000-0000-0000", []event.IEvent{
					ForTestMakeCreateEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
				}),
				eventStore: func(txCtx context.Context) event.EventStore {
					return eventStoreFactory(txCtx)
				},
			},
			wantErr: false,
			wantEventStream: []event.IEvent{
				ForTestMakeCreateEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
			},
			wantVersion: 1,
		},
		{
			name: "save single event with userID to blank aggregate",
			args: args{
				ctx: context.WithValue(context.Background(), event.CtxKeyUserID, "user"),
				aggregate: newForTestConcreteAggregate("1", "Name", 0, "0000-0000-0000", []event.IEvent{
					ForTestMakeCreateEvent3("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), "foobar"),
				}),
				eventStore: func(txCtx context.Context) event.EventStore { return eventStoreFactory(txCtx) },
			},
			wantErr: false,
			wantEventStream: []event.IEvent{
				ForTestMakeCreateEventWithUserID("1", "0000-0000-0000", "user", time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), "foobar"),
			},
			wantVersion: 1,
		},
		{
			name: "save single event with wrong class to blank aggregate",
			args: args{
				ctx: context.Background(),
				aggregate: newForTestConcreteAggregate("1", "Name", 0, "0000-0000-0000", []event.IEvent{
					ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
				}),
				eventStore: func(txCtx context.Context) event.EventStore { return eventStoreFactory(txCtx) },
			},
			wantErr:         true,
			wantEventStream: nil,
			wantVersion:     0,
		},
		{
			name: "save empty aggregate",
			args: args{
				ctx:        context.Background(),
				aggregate:  event.AggregateWithEventSourcingSupport(nil),
				eventStore: func(txCtx context.Context) event.EventStore { return eventStoreFactory(txCtx) },
			},
			wantErr:         false,
			wantEventStream: []event.IEvent{},
			wantVersion:     0,
		},
		{
			name: "save multiple event to blank aggregate",
			args: args{
				ctx: context.Background(),
				aggregate: newForTestConcreteAggregate("1", "Name", 0, "0000-0000-0000", []event.IEvent{
					ForTestMakeCreateEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
					ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
					ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC)),
					ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 4, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 4, time.UTC)),
					ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 5, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 5, time.UTC)),
					ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 6, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 6, time.UTC)),
					ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 7, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 7, time.UTC)),
					ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 8, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 8, time.UTC)),
					ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 9, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 9, time.UTC)),
				}),
				eventStore: func(txCtx context.Context) event.EventStore { return eventStoreFactory(txCtx) },
			},
			wantErr: false,
			wantEventStream: []event.IEvent{
				ForTestMakeCreateEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
				ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
				ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC)),
				ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 4, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 4, time.UTC)),
				ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 5, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 5, time.UTC)),
				ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 6, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 6, time.UTC)),
				ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 7, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 7, time.UTC)),
				ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 8, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 8, time.UTC)),
				ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 9, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 9, time.UTC)),
			},
			wantVersion: 9,
		},
		{
			name: "save events with in empty repo with 10 events and multi line string",
			args: args{
				ctx: context.Background(),
				aggregate: newForTestConcreteAggregate("1", "Name", 0, "0000-0000-0000", []event.IEvent{
					ForTestMakeCreateEvent3("1", "0000-0000-0000", time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC),
						`additionalProperty1`),
					ForTestMakeEvent3("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC),
						`additionalProperty1`),
					ForTestMakeEvent3("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC),
						`additionalProperty1`),
					ForTestMakeEvent3("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC),
						`additionalProperty1`),
					ForTestMakeEvent3("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 4, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 4, time.UTC),
						`additionalProperty1`),
					ForTestMakeEvent3("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 5, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 5, time.UTC),
						`additionalProperty1`),
					ForTestMakeEvent3("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 11, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 11, time.UTC),
						`additionalProperty1`),
				}),
				eventStore: func(txCtx context.Context) event.EventStore { return eventStoreFactory(txCtx) },
			},
			wantErr: false,
			wantEventStream: []event.IEvent{ForTestMakeCreateEvent3("1", "0000-0000-0000", time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC),
				`additionalProperty1`),
				ForTestMakeEvent3("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC),
					`additionalProperty1`),
				ForTestMakeEvent3("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC),
					`additionalProperty1`),
				ForTestMakeEvent3("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC),
					`additionalProperty1`),
				ForTestMakeEvent3("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 4, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 4, time.UTC),
					`additionalProperty1`),
				ForTestMakeEvent3("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 5, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 5, time.UTC),
					`additionalProperty1`),
				ForTestMakeEvent3("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 11, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 11, time.UTC),
					`additionalProperty1`),
			},
			wantVersion: 7,
		},
		{
			name: "save create event twice to blank aggregate",
			args: args{
				ctx: context.Background(),
				aggregate: newForTestConcreteAggregate("1", "Name", 0, "0000-0000-0000", []event.IEvent{
					ForTestMakeCreateEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
					ForTestMakeCreateEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
				}),
				eventStore: func(txCtx context.Context) event.EventStore { return eventStoreFactory(txCtx) },
			},
			wantErr:         true,
			wantEventStream: nil,
			wantVersion:     0,
		},
		{
			name: "save close event to blank aggregate",
			args: args{
				ctx: context.Background(),
				aggregate: newForTestConcreteAggregate("1", "Name", 0, "0000-0000-0000", []event.IEvent{
					ForTestMakeCreateEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
					ForTestMakeCloseEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
				}),
				eventStore: func(txCtx context.Context) event.EventStore { return eventStoreFactory(txCtx) },
			},
			wantErr: false,
			wantEventStream: []event.IEvent{
				ForTestMakeCreateEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
				ForTestMakeCloseEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
			},
			wantVersion: 2,
		},
		{
			name: "save event after close event",
			args: args{
				ctx: context.Background(),
				aggregate: newForTestConcreteAggregate("1", "Name", 0, "0000-0000-0000", []event.IEvent{
					ForTestMakeCreateEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
					ForTestMakeCloseEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
					ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC)),
				}),
				eventStore: func(txCtx context.Context) event.EventStore { return eventStoreFactory(txCtx) },
			},
			wantErr:         true,
			wantEventStream: nil,
			wantVersion:     0,
		},
		{
			name: "save patch before a close event",
			args: args{
				ctx: context.Background(),
				aggregate: newForTestConcreteAggregate("1", "Name", 0, "0000-0000-0000", []event.IEvent{
					ForTestMakeCreateEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
					ForTestMakeCloseEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC)),
					ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 4, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
				}),
				eventStore: func(txCtx context.Context) event.EventStore { return eventStoreFactory(txCtx) },
			},
			wantErr: false,
			wantEventStream: []event.IEvent{
				ForTestMakeCreateEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
				ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 4, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
				ForTestMakeCloseEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC)),
			},
			wantVersion: 3,
		},
		{
			name: "save close event before future patch valid time (error case)",
			args: args{
				ctx: context.Background(),
				aggregate: newForTestConcreteAggregate("1", "Name", 0, "0000-0000-0000", []event.IEvent{
					ForTestMakeCreateEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
					ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 4, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 4, time.UTC)),
					ForTestMakeCloseEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC)),
				}),
				eventStore: func(txCtx context.Context) event.EventStore { return eventStoreFactory(txCtx) },
			},
			wantErr:         true,
			wantEventStream: nil,
			wantVersion:     0,
		},
		{
			name: "save scheduled close event",
			args: args{
				ctx: context.Background(),
				aggregate: newForTestConcreteAggregate("1", "Name", 0, "0000-0000-0000", []event.IEvent{
					ForTestMakeCreateEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
					ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
					ForTestMakeCloseEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 4, time.UTC)),
				}),
				eventStore: func(txCtx context.Context) event.EventStore { return eventStoreFactory(txCtx) },
			},
			wantErr: false,
			wantEventStream: []event.IEvent{
				ForTestMakeCreateEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
				ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
				ForTestMakeCloseEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 4, time.UTC)),
			},
			wantVersion: 3,
		},
		{
			name: "save scheduled close event before future patch valid time (error case)",
			args: args{
				ctx: context.Background(),
				aggregate: newForTestConcreteAggregate("1", "Name", 0, "0000-0000-0000", []event.IEvent{
					ForTestMakeCreateEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
					ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 5, time.UTC)),
					ForTestMakeCloseEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 4, time.UTC)),
				}),
				eventStore: func(txCtx context.Context) event.EventStore { return eventStoreFactory(txCtx) },
			},
			wantErr:         true,
			wantEventStream: nil,
			wantVersion:     0,
		},
		{
			name: "save scheduled close event without migration",
			args: args{
				ctx: context.Background(),
				aggregate: newForTestConcreteAggregate("1", "Name", 0, "0000-0000-0000", []event.IEvent{
					ForTestMakeCreateEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
					ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
					ForTestMakeCloseEventWithoutMigration("1", "0000-0000-0000", time.Now().Add(24*time.Hour)),
				}),
				eventStore: func(txCtx context.Context) event.EventStore { return eventStoreFactory(txCtx) },
			},
			wantErr: false,
			wantEventStream: []event.IEvent{
				ForTestMakeCreateEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
				ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
			},
			wantVersion: 3,
		},
		{
			name: "save scheduled close event without migration before future patch valid time (error case)",
			args: args{
				ctx: context.Background(),
				aggregate: newForTestConcreteAggregate("1", "Name", 0, "0000-0000-0000", []event.IEvent{
					ForTestMakeCreateEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
					ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Now().Add(25*time.Hour)),
					ForTestMakeCloseEventWithoutMigration("1", "0000-0000-0000", time.Now().Add(24*time.Hour)),
				}),
				eventStore: func(txCtx context.Context) event.EventStore { return eventStoreFactory(txCtx) },
			},
			wantErr:         true,
			wantEventStream: nil,
			wantVersion:     0,
		},
		{
			name: "save scheduled close event as hpatch (error case)",
			args: args{
				ctx: context.Background(),
				aggregate: newForTestConcreteAggregate("1", "Name", 0, "0000-0000-0000", []event.IEvent{
					ForTestMakeCreateEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
					ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC)),
					ForTestMakeCloseEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 4, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
				}),
				eventStore: func(txCtx context.Context) event.EventStore { return eventStoreFactory(txCtx) },
			},
			wantErr:         true,
			wantEventStream: nil,
			wantVersion:     0,
		},
		{
			name: "save double earlier close event (error case)",
			args: args{
				ctx: context.Background(),
				aggregate: newForTestConcreteAggregate("1", "Name", 0, "0000-0000-0000", []event.IEvent{
					ForTestMakeCreateEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
					ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
					ForTestMakeCloseEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 4, time.UTC)),
					ForTestMakeCloseEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 4, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC)),
				}),
				eventStore: func(txCtx context.Context) event.EventStore { return eventStoreFactory(txCtx) },
			},
			wantErr:         true,
			wantEventStream: nil,
			wantVersion:     0,
		},
		{
			name: "save events from different aggregates",
			args: args{
				ctx: context.Background(),
				aggregate: newForTestConcreteAggregate("1", "Name", 0, "0000-0000-0000", []event.IEvent{
					ForTestMakeCreateEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
					ForTestMakeEvent("2", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
				}),
				eventStore: func(txCtx context.Context) event.EventStore { return eventStoreFactory(txCtx) },
			},
			wantErr:         true,
			wantEventStream: nil,
			wantVersion:     0,
		},
		{
			name: "save event on top of existing aggregate",
			args: args{
				ctx: context.Background(),
				aggregate: newForTestConcreteAggregate("1", "Name", 5, "0000-0000-0000", []event.IEvent{
					ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 5, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 5, time.UTC)),
				}),
				eventStore: func(txCtx context.Context) event.EventStore {
					store := eventStoreFactory(txCtx)
					_, filledStore := ForTestGetFilledRepoWitCtx(txCtx, store, "0000-0000-0000")
					return filledStore
				},
			},
			wantErr: false,
			wantEventStream: []event.IEvent{
				ForTestMakeCreateEvent("1", "0000-0000-0000", time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
				ForTestMakePatchEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 0, time.UTC)),
				ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
				ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
				ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 5, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 5, time.UTC)),
			},
			wantVersion: 6,
		},
		{
			name: "save event that is valid before the initial event",
			args: args{
				ctx: context.Background(),
				aggregate: newForTestConcreteAggregate("1", "Name", 5, "0000-0000-0000", []event.IEvent{
					ForTestMakePatchEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 5, time.UTC), time.Date(2000, 1, 1, 1, 1, 1, 0, time.UTC)),
				}),
				eventStore: func(txCtx context.Context) event.EventStore {
					store := eventStoreFactory(txCtx)
					resCh, filledStore := ForTestGetFilledRepoWitCtx(txCtx, store, "0000-0000-0000")
					err := <-resCh
					if err != nil {
						panic("error in test case preparation")
					}
					return filledStore
				},
			},
			wantErr: true,
			wantEventStream: []event.IEvent{
				ForTestMakeCreateEvent("1", "0000-0000-0000", time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
				ForTestMakePatchEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 0, time.UTC)),
				ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
				ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
			},
			wantVersion: 5,
		},
		{
			name: "save patch event in empty repro",
			args: args{
				ctx: context.Background(),
				aggregate: newForTestConcreteAggregate("1", "Name", 0, "0000-0000-0000", []event.IEvent{
					ForTestMakeCreateEvent("1", "0000-0000-0000", time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
					ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
					ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
					ForTestMakePatchEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 0, time.UTC)),
				}),
				eventStore: func(txCtx context.Context) event.EventStore { return eventStoreFactory(txCtx) },
			},
			wantErr: false,
			wantEventStream: []event.IEvent{
				ForTestMakeCreateEvent("1", "0000-0000-0000", time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
				ForTestMakePatchEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 0, time.UTC)),
				ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
				ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
			},
			wantVersion: 4,
		},
		{
			name: "save event before last synced event in existing repro",
			args: args{
				ctx: context.Background(),
				aggregate: newForTestConcreteAggregate("1", "Name", 5, "0000-0000-0000", []event.IEvent{
					ForTestMakePatchEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 0, time.UTC)),
				}),
				eventStore: func(txCtx context.Context) event.EventStore {
					store := eventStoreFactory(txCtx)
					errCh, filledStore := ForTestGetFilledRepoWitCtx(txCtx, store, "0000-0000-0000")
					err := <-errCh
					if err != nil {
						panic("error in testpreparation")
					}
					return filledStore
				},
			},
			wantErr: true,
			wantEventStream: []event.IEvent{
				ForTestMakeCreateEvent("1", "0000-0000-0000", time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
				ForTestMakePatchEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 0, time.UTC)),
				ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
				ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
			},
			wantVersion: 5,
		},
		{
			name: "save empty event stream into existing stream/repro",
			args: args{
				ctx:       context.Background(),
				aggregate: newForTestConcreteAggregate("1", "Name", 5, "0000-0000-0000", []event.IEvent{}),
				eventStore: func(txCtx context.Context) event.EventStore {
					store := eventStoreFactory(txCtx)
					errCh, filledStore := ForTestGetFilledRepoWitCtx(txCtx, store, "0000-0000-0000")
					err := <-errCh
					if err != nil {
						panic("error in testpreparation")
					}
					return filledStore
				},
			},
			wantErr: false,
			wantEventStream: []event.IEvent{
				ForTestMakeCreateEvent("1", "0000-0000-0000", time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
				ForTestMakePatchEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 0, time.UTC)),
				ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
				ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
			},
			wantVersion: 5,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_ = trans.WithinTX(tt.args.ctx, func(txCtx context.Context) error {
				eventStore := tt.args.eventStore(txCtx)

				_, err := event.SaveAggregate(txCtx, eventStore, tt.args.aggregate)
				if (err != nil) != tt.wantErr {
					t.Errorf("SaveAggregate() error = %v, wantErr %v", err, tt.wantErr)
				}

				//nil case in save
				if tt.args.aggregate == nil {
					return nil
				}

				gotEventStream, gotVersion, err := event.LoadAggregateAsAt(txCtx, tt.args.aggregate.GetTenantID(), "forTestConcreteAggregate", tt.args.aggregate.GetID(), time.Now(), eventStore)
				if !tt.wantErr && err != nil {
					t.Error(err)
				}

				testdata.AssertEqualStream(t, gotEventStream, tt.wantEventStream)

				if gotVersion != tt.wantVersion {
					t.Errorf("SaveAggregate() gotVersion = %v, want %v", gotVersion, tt.wantVersion)
				}

				return postgres.Rollback
			})
		})
	}
}

func testSaveAggregateWithValidTime(t *testing.T, trans transactor.Port, eventStoreFactory func(txCtx context.Context) event.EventStore, cleanUp func()) {
	cleanUp()
	testTime := time.Now()
	type args struct {
		ctx        context.Context
		aggregate  event.AggregateWithEventSourcingSupport
		eventStore func(txCtx context.Context) event.EventStore
	}
	tests := []struct {
		name            string
		args            args
		wantTime        time.Time
		wantErr         bool
		wantEventStream []event.IEvent
		wantVersion     int
	}{
		{
			name: "create aggregate now",
			args: args{
				ctx: context.Background(),
				aggregate: newForTestConcreteAggregate("1", "Name", 0, "0000-0000-0000", []event.IEvent{
					ForTestMakeCreateEventWithoutMigration("1", "0000-0000-0000", testTime.UTC()),
				}),
				eventStore: func(txCtx context.Context) event.EventStore {
					return eventStoreFactory(txCtx)
				},
			},
			wantTime: testTime,
			wantErr:  false,
			wantEventStream: []event.IEvent{
				ForTestMakeCreateEventWithoutMigration("1", "0000-0000-0000", testTime.UTC()),
			},
			wantVersion: 1,
		},
		{
			name: "create aggregate historical",
			args: args{
				ctx: context.Background(),
				aggregate: newForTestConcreteAggregate("1", "Name", 0, "0000-0000-0000", []event.IEvent{
					ForTestMakeCreateEventWithoutMigration("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
				}),
				eventStore: func(txCtx context.Context) event.EventStore {
					return eventStoreFactory(txCtx)
				},
			},
			wantTime: testTime,
			wantErr:  false,
			wantEventStream: []event.IEvent{
				ForTestMakeCreateEventWithoutMigration("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
			},
			wantVersion: 1,
		},
		{
			name: "create aggregate historical and add events",
			args: args{
				ctx: context.Background(),
				aggregate: newForTestConcreteAggregate("1", "Name", 0, "0000-0000-0000", []event.IEvent{
					ForTestMakeCreateEventWithoutMigration("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
					ForTestMakeEventWithValidTime("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
				}),
				eventStore: func(txCtx context.Context) event.EventStore {
					return eventStoreFactory(txCtx)
				},
			},
			wantTime: testTime,
			wantErr:  false,
			wantEventStream: []event.IEvent{
				ForTestMakeCreateEventWithoutMigration("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
				ForTestMakeEventWithValidTime("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
			},
			wantVersion: 2,
		},
		{
			name: "create future aggregate",
			args: args{
				ctx: context.Background(),
				aggregate: newForTestConcreteAggregate("1", "Name", 0, "0000-0000-0000", []event.IEvent{
					ForTestMakeCreateEventWithoutMigration("1", "0000-0000-0000", testTime.Add(24*time.Hour).UTC()),
				}),
				eventStore: func(txCtx context.Context) event.EventStore {
					return eventStoreFactory(txCtx)
				},
			},
			wantTime: testTime.Add(25 * time.Hour),
			wantErr:  false,
			wantEventStream: []event.IEvent{
				ForTestMakeCreateEventWithoutMigration("1", "0000-0000-0000", testTime.Add(24*time.Hour).UTC()),
			},
			wantVersion: 1,
		},
		{
			name: "create future aggregate and add events",
			args: args{
				ctx: context.Background(),
				aggregate: newForTestConcreteAggregate("1", "Name", 0, "0000-0000-0000", []event.IEvent{
					ForTestMakeCreateEventWithoutMigration("1", "0000-0000-0000", testTime.Add(24*time.Hour).UTC()),
					ForTestMakeEventWithValidTime("1", "0000-0000-0000", testTime.Add(25*time.Hour).UTC()),
				}),
				eventStore: func(txCtx context.Context) event.EventStore {
					return eventStoreFactory(txCtx)
				},
			},
			wantTime: testTime.Add(25 * time.Hour),
			wantErr:  false,
			wantEventStream: []event.IEvent{
				ForTestMakeCreateEventWithoutMigration("1", "0000-0000-0000", testTime.Add(24*time.Hour).UTC()),
				ForTestMakeEventWithValidTime("1", "0000-0000-0000", testTime.Add(25*time.Hour).UTC()),
			},
			wantVersion: 2,
		},
		{
			name: "create aggregate historical and add events with transaction time before creation (error case)",
			args: args{
				ctx: context.Background(),
				aggregate: newForTestConcreteAggregate("1", "Name", 0, "0000-0000-0000", []event.IEvent{
					ForTestMakeCreateEventWithoutMigration("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
					ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
				}),
				eventStore: func(txCtx context.Context) event.EventStore {
					return eventStoreFactory(txCtx)
				},
			},
			wantTime:        testTime,
			wantErr:         true,
			wantEventStream: []event.IEvent{},
			wantVersion:     0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_ = trans.WithinTX(tt.args.ctx, func(txCtx context.Context) error {
				eventStore := tt.args.eventStore(txCtx)

				_, err := event.SaveAggregate(txCtx, eventStore, tt.args.aggregate)
				if (err != nil) != tt.wantErr {
					t.Errorf("SaveAggregate() error = %v, wantErr %v", err, tt.wantErr)
				}

				//nil case in save
				if tt.args.aggregate == nil {
					return nil
				}

				gotEventStream, gotVersion, err := event.LoadAggregateAsAt(txCtx, tt.args.aggregate.GetTenantID(), "forTestConcreteAggregate", tt.args.aggregate.GetID(), tt.wantTime.Add(time.Hour), eventStore)
				if !tt.wantErr && err != nil {
					t.Error(err)
				}

				testdata.AssertEqualStream(t, gotEventStream, tt.wantEventStream,
					testdata.CheckAggregateID,
					testdata.CheckTenantID,
					testdata.CheckClass,
					testdata.CheckMigration,
					testdata.CheckValidTime) //no test for transaction time
				if gotVersion != tt.wantVersion {
					t.Errorf("SaveAggregate() gotVersion = %v, want %v", gotVersion, tt.wantVersion)
				}

				return postgres.Rollback
			})
		})
	}
}

func testSaveAggregateWithSnapShot(t *testing.T, adapter func() persistence.Port, cleanUp func()) {
	cleanUp()

	type args struct {
		ctx            context.Context
		aggregate      event.AggregateWithEventSourcingSupport
		eventStore     func() event.EventStore
		projectionTime time.Time
	}
	tests := []struct {
		name            string
		args            args
		wantErr         bool
		wantEventStream []event.IEvent
		wantVersion     int
	}{
		{
			name: "save single snapshot in existing repro without patches ",
			args: args{
				ctx: context.Background(),
				aggregate: newForTestConcreteAggregate("1", "Name", 3, "0000-0000-0000", []event.IEvent{
					ForTestMakeSnapshot("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 4, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 4, time.UTC)),
				}),
				projectionTime: time.Now(),
				eventStore: func() event.EventStore {
					store, err, _ := eventstore.New(adapter())
					if err != nil {
						panic("error in test case preparation")
					}

					_, filledStore := ForTestGetFilledRepoWithoutPatch(store, "0000-0000-0000")
					return filledStore
				},
			},

			wantErr: false,
			wantEventStream: []event.IEvent{
				ForTestMakeSnapshot("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 4, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 4, time.UTC)),
			},
			wantVersion: 3,
		},
		{
			name: "save multiple snapshot in existing repro without patches ",
			args: args{
				ctx: context.Background(),
				aggregate: newForTestConcreteAggregate("1", "Name", 3, "0000-0000-0000", []event.IEvent{
					ForTestMakeSnapshot("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 4, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 4, time.UTC)),
					ForTestMakeSnapshot("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 5, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 5, time.UTC)),
					ForTestMakeSnapshot("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 6, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 6, time.UTC)),
					ForTestMakeSnapshot("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 7, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 7, time.UTC)),
					ForTestMakeSnapshot("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 8, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 8, time.UTC)),
					ForTestMakeSnapshot("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 9, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 9, time.UTC)),
					ForTestMakeSnapshot("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 10, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 10, time.UTC)),
					ForTestMakeSnapshot("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 11, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 11, time.UTC)),
					ForTestMakeSnapshot("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 12, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 12, time.UTC)),
				}),
				projectionTime: time.Now(),
				eventStore: func() event.EventStore {
					store, err, _ := eventstore.New(adapter())
					if err != nil {
						panic("error in test case preparation")
					}

					_, filledStore := ForTestGetFilledRepoWithoutPatch(store, "0000-0000-0000")
					return filledStore
				},
			},
			wantErr: false,
			wantEventStream: []event.IEvent{
				ForTestMakeSnapshot("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 12, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 12, time.UTC)),
			},
			wantVersion: 3,
		},
		{
			name: "save single historical snapshot in existing repro without patches ",
			args: args{
				ctx: context.Background(),
				aggregate: newForTestConcreteAggregate("1", "Name", 3, "0000-0000-0000", []event.IEvent{
					ForTestMakeHistoricalSnapshot("1", "0000-0000-0000", time.Date(2022, 1, 1, 1, 1, 1, 4, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 4, time.UTC)),
				}),
				projectionTime: time.Now(),
				eventStore: func() event.EventStore {
					store, err, _ := eventstore.New(adapter())
					if err != nil {
						panic("error in test case preparation")
					}
					_, filledStore := ForTestGetFilledRepoWithoutPatch(store, "0000-0000-0000")
					return filledStore
				},
			},
			wantErr: false,
			wantEventStream: []event.IEvent{
				ForTestMakeHistoricalSnapshot("1", "0000-0000-0000", time.Date(2022, 1, 1, 1, 1, 1, 4, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 4, time.UTC)),
			},
			wantVersion: 3,
		},
		{
			name: "save single snapshot in empty repro without patches",
			args: args{
				ctx: context.Background(),
				aggregate: newForTestConcreteAggregate("1", "Name", 0, "0000-0000-0000", []event.IEvent{
					ForTestMakeCreateEvent("1", "0000-0000-0000", time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
					ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
					ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
					ForTestMakeSnapshot("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
					ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 4, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 4, time.UTC)),
				}),
				projectionTime: time.Now(),
				eventStore: func() event.EventStore {
					store, err, _ := eventstore.New(adapter())
					if err != nil {
						panic("error in test case preparation")
					}
					return store
				},
			},
			wantErr: false,
			wantEventStream: []event.IEvent{
				ForTestMakeSnapshot("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
				ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 4, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 4, time.UTC)),
			},
			wantVersion: 4,
		},
		{
			name: "save single snapshot during an event stream in empty repro without patches",
			args: args{
				ctx: context.Background(),
				aggregate: newForTestConcreteAggregate("1", "Name", 0, "0000-0000-0000", []event.IEvent{
					ForTestMakeCreateEvent("1", "0000-0000-0000", time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
					ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
					ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
					ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 4, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 4, time.UTC)),
					ForTestMakeSnapshot("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 5, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC)),
				}),
				projectionTime: time.Now(),
				eventStore: func() event.EventStore {
					store, err, _ := eventstore.New(adapter())
					if err != nil {
						panic("error in test case preparation")
					}
					return store
				},
			},
			wantErr: false,
			wantEventStream: []event.IEvent{
				ForTestMakeSnapshot("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 5, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC)),
				ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 4, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 4, time.UTC)),
			},
			wantVersion: 4,
		},
		{
			name: "save two snapshot in empty repro without patches",
			args: args{
				ctx: context.Background(),
				aggregate: newForTestConcreteAggregate("1", "Name", 0, "0000-0000-0000", []event.IEvent{
					ForTestMakeCreateEvent("1", "0000-0000-0000", time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
					ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
					ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
					ForTestMakeSnapshot("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC)),
					ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 4, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 4, time.UTC)),
					ForTestMakeSnapshot("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 5, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 5, time.UTC)),
				}),
				projectionTime: time.Now(),
				eventStore: func() event.EventStore {
					store, err, _ := eventstore.New(adapter())
					if err != nil {
						panic("error in test case preparation")
					}
					return store
				},
			},
			wantErr: false,
			wantEventStream: []event.IEvent{
				ForTestMakeSnapshot("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 5, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 5, time.UTC)),
			},
			wantVersion: 4,
		},
		{
			name: "save event on top of snapshots in existing repro without patches",
			args: args{
				ctx: context.Background(),
				aggregate: newForTestConcreteAggregate("1", "Name", 3, "0000-0000-0000", []event.IEvent{
					ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC)),
				}),
				projectionTime: time.Now(),
				eventStore: func() event.EventStore {
					store, err, _ := eventstore.New(adapter())
					if err != nil {
						panic("error in test case preparation")
					}
					_, filledStore := ForTestGetFilledReproWithSnapshot(store, "0000-0000-0000")
					return filledStore
				},
			},
			wantErr: false,
			wantEventStream: []event.IEvent{
				ForTestMakeSnapshot("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
				ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC)),
			},
			wantVersion: 4,
		},
		{
			name: "save snapshot during an event stream in existing repro without patches",
			args: args{
				ctx: context.Background(),
				aggregate: newForTestConcreteAggregate("1", "Name", 3, "0000-0000-0000", []event.IEvent{
					ForTestMakeSnapshot("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
				}),
				projectionTime: time.Now(),
				eventStore: func() event.EventStore {
					store, err, _ := eventstore.New(adapter())
					if err != nil {
						panic("error in test case preparation")
					}

					errCh, filledStore := ForTestGetFilledRepoWithoutPatch(store, "0000-0000-0000")
					err = <-errCh
					if err != nil {
						panic("error in test preparation")
					}
					return filledStore
				},
			},
			wantErr: false,
			wantEventStream: []event.IEvent{
				ForTestMakeSnapshot("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
				ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
			},
			wantVersion: 3,
		},
		{
			name: "save patch and snapshot within this patch interval in existing repro",
			args: args{
				ctx: context.Background(),
				aggregate: newForTestConcreteAggregate("1", "Name", 3, "0000-0000-0000", []event.IEvent{
					ForTestMakePatchEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 0, time.UTC))}),
				projectionTime: time.Now(),
				eventStore: func() event.EventStore {
					store, err, _ := eventstore.New(adapter())
					if err != nil {
						panic("error in test case preparation")
					}

					_, err = event.SaveAggregate(context.Background(), store,
						newForTestConcreteAggregate("1", "Name", 0, "0000-0000-0000", []event.IEvent{
							ForTestMakeCreateEvent("1", "0000-0000-0000", time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
							ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
							ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
							ForTestMakeSnapshot("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC))}))
					if err != nil {
						panic("error in test preparation")
					}
					return store
				},
			},
			wantErr: false,
			wantEventStream: []event.IEvent{
				ForTestMakeCreateEvent("1", "0000-0000-0000", time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
				ForTestMakePatchEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 0, time.UTC)),
				ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
				ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
			},
			wantVersion: 4,
		},
		{
			name: "save snapshot within a patch interval in existing repro",
			args: args{
				ctx: context.Background(),
				aggregate: newForTestConcreteAggregate("1", "Name", 4, "0000-0000-0000", []event.IEvent{
					ForTestMakeSnapshot("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC))}),
				projectionTime: time.Now(),
				eventStore: func() event.EventStore {
					store, err, _ := eventstore.New(adapter())
					if err != nil {
						panic("error in test case preparation")
					}

					_, err = event.SaveAggregate(context.Background(), store,
						newForTestConcreteAggregate("1", "Name", 0, "0000-0000-0000", []event.IEvent{
							ForTestMakeCreateEvent("1", "0000-0000-0000", time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
							ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
							ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
							ForTestMakePatchEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 0, time.UTC))}))
					if err != nil {
						panic("error in test preparation")
					}
					return store
				},
			},
			wantErr: true,
			wantEventStream: []event.IEvent{
				ForTestMakeCreateEvent("1", "0000-0000-0000", time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
				ForTestMakePatchEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 0, time.UTC)),
				ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
				ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
			},
			wantVersion: 4,
		},
		{
			name: "save snapshot and patch within an event stream in empty repro (failing db)",
			args: args{
				ctx: context.Background(),
				aggregate: newForTestConcreteAggregate("1", "Name", 0, "0000-0000-0000", []event.IEvent{
					ForTestMakeCreateEvent("1", "0000-0000-0000", time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
					ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
					ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
					ForTestMakeSnapshot("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC)),
					ForTestMakePatchEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 0, time.UTC))}),
				projectionTime: time.Now(),
				eventStore: func() event.EventStore {
					store, err, _ := eventstore.New(adapter())
					if err != nil {
						panic("error in test case preparation")
					}
					return store
				},
			},
			wantErr: true,
		},
		{
			name: "save patch and snapshot within an event stream in empty repro (failing core)",
			args: args{
				ctx: context.Background(),
				aggregate: newForTestConcreteAggregate("1", "Name", 0, "0000-0000-0000", []event.IEvent{
					ForTestMakeCreateEvent("1", "0000-0000-0000", time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
					ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
					ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
					ForTestMakePatchEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 0, time.UTC)),
					ForTestMakeSnapshot("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC))}),
				projectionTime: time.Now(),
				eventStore: func() event.EventStore {
					store, err, _ := eventstore.New(adapter())
					if err != nil {
						panic("error in test case preparation")
					}
					return store
				},
			},
			wantErr: true,
		},
		{
			name: "save patch to invalid snapshots in existing repro",
			args: args{
				ctx: context.Background(),
				aggregate: newForTestConcreteAggregate("1", "Name", 3, "0000-0000-0000", []event.IEvent{
					ForTestMakePatchEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 0, time.UTC)),
				}),
				projectionTime: time.Now(),
				eventStore: func() event.EventStore {
					store, err, _ := eventstore.New(adapter())
					if err != nil {
						panic("error in test case preparation")
					}
					_, filledStore := ForTestGetFilledReproWithSnapshot(store, "0000-0000-0000")
					return filledStore
				},
			},
			wantErr: false,
			wantEventStream: []event.IEvent{
				ForTestMakeCreateEvent("1", "0000-0000-0000", time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
				ForTestMakePatchEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 0, time.UTC)),
				ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
				ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
			},
			wantVersion: 4,
		},
		{
			name: "save future patch on top of existing snapshots in existing repro",
			args: args{
				ctx: context.Background(),
				aggregate: newForTestConcreteAggregate("1", "Name", 3, "0000-0000-0000", []event.IEvent{
					ForTestMakePatchEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 4, time.UTC), time.Date(2050, 1, 1, 1, 1, 1, 0, time.UTC)),
				}),
				projectionTime: time.Now(),
				eventStore: func() event.EventStore {
					store, err, _ := eventstore.New(adapter())
					if err != nil {
						panic("error in test case preparation")
					}
					_, filledStore := ForTestGetFilledReproWithSnapshot(store, "0000-0000-0000")
					return filledStore
				},
			},
			wantErr: false,
			wantEventStream: []event.IEvent{
				ForTestMakeSnapshot("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
			},
			wantVersion: 4,
		},
		{
			name: "save snapshot within patch interval in existing repro",
			args: args{
				ctx: context.Background(),
				aggregate: newForTestConcreteAggregate("1", "Name", 5, "0000-0000-0000", []event.IEvent{
					ForTestMakeSnapshot("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
				}),
				projectionTime: time.Now(),
				eventStore: func() event.EventStore {
					store, err, _ := eventstore.New(adapter())
					if err != nil {
						panic("error in test case preparation")
					}
					_, filledStore := ForTestGetFilledRepo(store, "0000-0000-0000")
					return filledStore
				},
			},
			wantErr: true,
			wantEventStream: []event.IEvent{
				ForTestMakeCreateEvent("1", "0000-0000-0000", time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
				ForTestMakePatchEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 0, time.UTC)),
				ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
				ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
			},
			wantVersion: 5,
		},
		{
			name: "save snapshot within future patch interval in existing repro",
			args: args{
				ctx: context.Background(),
				aggregate: newForTestConcreteAggregate("1", "Name", 5, "0000-0000-0000", []event.IEvent{
					ForTestMakeSnapshot("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 4, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 4, time.UTC)),
				}),
				projectionTime: time.Now(),
				eventStore: func() event.EventStore {
					store, err, _ := eventstore.New(adapter())
					if err != nil {
						panic("error in test case preparation")
					}
					_, filledStore := ForTestGetFilledRepo(store, "0000-0000-0000")
					return filledStore
				},
			},
			wantErr: true,
			wantEventStream: []event.IEvent{
				ForTestMakeCreateEvent("1", "0000-0000-0000", time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
				ForTestMakePatchEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 0, time.UTC)),
				ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
				ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
			},
			wantVersion: 5,
		},
		{
			name: "save snapshot not on latest version in existing repro",
			args: args{
				ctx: context.Background(),
				aggregate: newForTestConcreteAggregate("1", "Name", 2, "0000-0000-0000", []event.IEvent{
					ForTestMakeSnapshot("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 4, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 4, time.UTC)),
				}),
				projectionTime: time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC),
				eventStore: func() event.EventStore {
					store, err, _ := eventstore.New(adapter(),
						eventstore.WithConcurrentModificationStrategy("forTestConcreteAggregate", event.Ignore))
					if err != nil {
						panic("error in test case preparation")
					}

					aggregate := newForTestConcreteAggregate("1", "Name", 0, "0000-0000-0000", []event.IEvent{
						ForTestMakeCreateEvent("1", "0000-0000-0000", time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
						ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
						ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
						ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC)),
					})

					if _, err := event.SaveAggregate(context.Background(), store, aggregate); err != nil {
						t.Errorf("error in test preparation %v", err)
					}

					return store
				},
			},
			wantErr: true,
			wantEventStream: []event.IEvent{
				ForTestMakeCreateEvent("1", "0000-0000-0000", time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
				ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
			},
			wantVersion: 4,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			eventStore := tt.args.eventStore()
			defer cleanUp()

			if _, err := event.SaveAggregate(tt.args.ctx, eventStore, tt.args.aggregate); (err != nil) != tt.wantErr {
				t.Errorf("SaveAggregate() error = %v, wantErr %v", err, tt.wantErr)
			}
			gotEventStream, gotVersion, err := event.LoadAggregateAsAt(tt.args.ctx, tt.args.aggregate.GetTenantID(), "forTestConcreteAggregate", tt.args.aggregate.GetID(), tt.args.projectionTime, eventStore)
			if !tt.wantErr && err != nil {
				t.Error(err)
			}

			testdata.AssertEqualStream(t, gotEventStream, tt.wantEventStream)

			if gotVersion != tt.wantVersion {
				t.Errorf("SaveAggregate() gotVersion = %v, want %v", gotVersion, tt.wantVersion)
			}

		})
	}
}

func testAggregateWithSnapShotValidity(t *testing.T, eventStoreFactory func() event.EventStore, cleanUp func()) {
	cleanUp()
	type args struct {
		ctx        context.Context
		aggregate  event.AggregateWithEventSourcingSupport
		eventStore func() event.EventStore
		sinceTime  time.Time
	}
	tests := []struct {
		name             string
		args             args
		wantEventStream  []event.IEvent
		wantErr          bool
		want2EventStream []event.IEvent
		wantVersion      int
	}{
		{
			name: "Disable snapshot with effect",
			args: args{
				ctx: context.Background(),
				aggregate: newForTestConcreteAggregate("1", "Name", 0, "0000-0000-0000", []event.IEvent{
					ForTestMakeCreateEvent("1", "0000-0000-0000", time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
					ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 0, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 0, time.UTC)),
					ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
					ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
					ForTestMakeSnapshot("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC)),
				}),
				eventStore: func() event.EventStore { return eventStoreFactory() },
				sinceTime:  time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC),
			},
			wantErr: false,
			wantEventStream: []event.IEvent{
				ForTestMakeSnapshot("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC)),
			},
			want2EventStream: []event.IEvent{
				ForTestMakeCreateEvent("1", "0000-0000-0000", time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
				ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 0, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 0, time.UTC)),
				ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
				ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
			},
			wantVersion: 4,
		},
		{
			name: "Disable snapshot before creation time does not effect init snapshot of the aggregate",
			args: args{
				ctx: context.Background(),
				aggregate: newForTestConcreteAggregate("1", "Name", 0, "0000-0000-0000", []event.IEvent{
					ForTestMakeCreateEvent("1", "0000-0000-0000", time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
					ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 0, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 0, time.UTC)),
					ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
					ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
					ForTestMakeSnapshot("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC)),
				}),

				eventStore: func() event.EventStore { return eventStoreFactory() },
				sinceTime:  time.Date(2019, 1, 1, 1, 1, 1, 2, time.UTC),
			},
			wantErr: false,
			wantEventStream: []event.IEvent{
				ForTestMakeSnapshot("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC)),
			},
			want2EventStream: []event.IEvent{
				ForTestMakeCreateEvent("1", "0000-0000-0000", time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
				ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 0, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 0, time.UTC)),
				ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
				ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
			},
			wantVersion: 4,
		},
		{
			name: "Disable snapshot after last snapshot does not effect snapshot",
			args: args{
				ctx: context.Background(),
				aggregate: newForTestConcreteAggregate("1", "Name", 0, "0000-0000-0000", []event.IEvent{
					ForTestMakeCreateEvent("1", "0000-0000-0000", time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
					ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 0, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 0, time.UTC)),
					ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
					ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
					ForTestMakeSnapshot("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC)),
				}),

				eventStore: func() event.EventStore { return eventStoreFactory() },
				sinceTime:  time.Now(),
			},
			wantErr: false,
			wantEventStream: []event.IEvent{
				ForTestMakeSnapshot("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC)),
			},
			want2EventStream: []event.IEvent{
				ForTestMakeSnapshot("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC)),
			},
			wantVersion: 4,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			eventStore := tt.args.eventStore()
			defer cleanUp()

			if _, err := event.SaveAggregate(tt.args.ctx, eventStore, tt.args.aggregate); (err != nil) != tt.wantErr {
				t.Errorf("Disable Snapshot() error = %v, wantErr %v", err, tt.wantErr)
			}

			gotEventStream, gotVersion, err := event.LoadAggregateAsAt(tt.args.ctx, tt.args.aggregate.GetTenantID(), "forTestConcreteAggregate", tt.args.aggregate.GetID(), time.Now(), eventStore)
			if !tt.wantErr && err != nil {
				t.Error(err)
			}

			testdata.AssertEqualStream(t, gotEventStream, tt.wantEventStream)

			if gotVersion != tt.wantVersion {
				t.Errorf("Disable Snapshot() gotVersion = %v, want %v", gotVersion, tt.wantVersion)
			}

			if err := eventStore.DeleteSnapShots(tt.args.ctx, tt.args.aggregate.GetTenantID(), "forTestConcreteAggregate", tt.args.aggregate.GetID(), tt.args.sinceTime); (err != nil) != tt.wantErr {
				t.Errorf("Disable Snapshot() error = %v, wantErr %v", err, tt.wantErr)
			}

			gotEventStream2, gotVersion, err := event.LoadAggregateAsAt(tt.args.ctx, tt.args.aggregate.GetTenantID(), "forTestConcreteAggregate", tt.args.aggregate.GetID(), time.Now(), eventStore)
			if !tt.wantErr && err != nil {
				t.Error(err)
			}

			testdata.AssertEqualStream(t, gotEventStream2, tt.want2EventStream)

			if gotVersion != tt.wantVersion {
				t.Errorf("Disable Snapshot() gotVersion = %v, want %v", gotVersion, tt.wantVersion)
			}

		})
	}
}

func testSaveAggregates(t *testing.T, adapter func() persistence.Port, cleanUp func()) {
	cleanUp()
	type args struct {
		ctx        context.Context
		eventStore func() event.EventStore
		aggregates []event.AggregateWithEventSourcingSupport
	}
	tests := []struct {
		name     string
		args     args
		wantErr  bool
		expected []event.PersistenceEvents
	}{
		{
			name: "happy path save multiple aggregates of one tenant to empty repo",
			args: args{
				ctx: context.Background(),
				eventStore: func() event.EventStore {
					emptyRetryEventStore, err, _ := eventstore.New(adapter(),
						eventstore.WithConcurrentModificationStrategy("forTestConcreteAggregate", event.Ignore),
					)
					if err != nil {
						t.Errorf("test case preparation %v failed: %v", t.Name(), err)
					}
					return emptyRetryEventStore
				},
				aggregates: []event.AggregateWithEventSourcingSupport{
					newForTestConcreteAggregate("1", "Name", 0, "0000-0000-0000", []event.IEvent{
						ForTestMakeCreateEvent("1", "0000-0000-0000", time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
						ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
						ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
						ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC)),
						ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 4, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 4, time.UTC)),
					}),
					newForTestConcreteAggregate("2", "Name", 0, "0000-0000-0000", []event.IEvent{
						ForTestMakeCreateEvent("2", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
					}),
				},
			},
			wantErr: false,
			expected: []event.PersistenceEvents{
				{Events: []event.PersistenceEvent{
					ForTestEvent0(),
					ForTestEvent1(),
					ForTestEvent2(),
					ForTestEvent3(),
					ForTestEvent4(),
				},
					Version: 5,
				},
				{Events: []event.PersistenceEvent{
					ForTestEvent1b(),
				},
					Version: 1},
			},
		},
		{
			name: "save nil aggregate slice",
			args: args{
				ctx: context.Background(),
				eventStore: func() event.EventStore {
					emptyRetryEventStore, err, _ := eventstore.New(adapter(),
						eventstore.WithConcurrentModificationStrategy("forTestConcreteAggregate", event.Ignore),
					)
					if err != nil {
						t.Errorf("test case preparation %v failed: %v", t.Name(), err)
					}
					return emptyRetryEventStore
				},
				aggregates: nil,
			},
			wantErr:  false,
			expected: []event.PersistenceEvents{},
		},
		{
			name: "save multiple aggregates with same aggregateID  of one tenant to empty repo",
			args: args{
				ctx: context.Background(),
				eventStore: func() event.EventStore {
					emptyRetryEventStore, err, _ := eventstore.New(adapter(),
						eventstore.WithConcurrentModificationStrategy("forTestConcreteAggregate", event.Ignore),
					)
					if err != nil {
						t.Errorf("test case preparation %v failed: %v", t.Name(), err)
					}
					return emptyRetryEventStore
				},
				aggregates: []event.AggregateWithEventSourcingSupport{
					newForTestConcreteAggregate("1", "Name", 0, "0000-0000-0000", []event.IEvent{
						ForTestMakeCreateEvent("1", "0000-0000-0000", time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
						ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
						ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
						ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC)),
						ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 4, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 4, time.UTC)),
					}),
					newForTestConcreteAggregate("2", "Name", 0, "0000-0000-0000", []event.IEvent{
						ForTestMakeCreateEvent("2", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
					}),
					newForTestConcreteAggregate("1", "Name", 0, "0000-0000-0000", []event.IEvent{
						ForTestMakeCreateEvent("1", "0000-0000-0000", time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
						ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
						ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
						ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC)),
						ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 4, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 4, time.UTC)),
					}),
				},
			},
			wantErr:  true,
			expected: nil,
		},
		{
			name: "sad path save multiple aggregates of one tenant with corrupted tenantIDs in empty repo",
			args: args{
				ctx: context.Background(),
				eventStore: func() event.EventStore {
					emptyRetryEventStore, err, _ := eventstore.New(adapter(),
						eventstore.WithConcurrentModificationStrategy("forTestConcreteAggregate", event.Ignore),
					)
					if err != nil {
						t.Errorf("test case preparation %v failed: %v", t.Name(), err)
					}
					return emptyRetryEventStore
				},
				aggregates: []event.AggregateWithEventSourcingSupport{
					newForTestConcreteAggregate("1", "Name", 0, "0000-0000-0000", []event.IEvent{
						ForTestMakeCreateEvent("1", "0000-0000-0001", time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
					}),
					newForTestConcreteAggregate("2", "Name", 0, "0000-0000-0001", []event.IEvent{
						ForTestMakeCreateEvent("2", "0000-0000-0001", time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
						ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
					}),
				},
			},
			wantErr:  true,
			expected: nil,
		},
		{
			name: "happy path save single aggregate to empty repo but with multiple event streams with ignore version strategy",
			args: args{
				ctx: context.Background(),
				eventStore: func() event.EventStore {
					store, err, _ := eventstore.New(adapter(),
						eventstore.WithConcurrentModificationStrategy("forTestConcreteAggregate", event.Ignore),
					)
					if err != nil {
						t.Errorf("test case preparation %v failed: %v", t.Name(), err)
					}
					return store
				},
				aggregates: []event.AggregateWithEventSourcingSupport{
					newForTestConcreteAggregate("1", "Name", 0, "0000-0000-0000", []event.IEvent{
						ForTestMakeCreateEvent("1", "0000-0000-0000", time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
						ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
						ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
					}),
					newForTestConcreteAggregate("1", "Name", 0, "0000-0000-0000", []event.IEvent{
						ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC)),
						ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 4, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 4, time.UTC)),
					}),
				},
			},
			wantErr: false,
			expected: []event.PersistenceEvents{
				{Events: []event.PersistenceEvent{
					ForTestEvent0(),
					ForTestEvent1(),
					ForTestEvent2(),
					ForTestEvent3(),
					ForTestEvent4(),
				},
					Version: 5},
			},
		},
		{
			name: "save multiple aggregates with incorrect init version",
			args: args{
				ctx: context.Background(),
				eventStore: func() event.EventStore {
					store, err, _ := eventstore.New(adapter())
					if err != nil {
						t.Errorf("test case preparation %v failed: %v", t.Name(), err)
					}
					return store
				},
				aggregates: []event.AggregateWithEventSourcingSupport{
					newForTestConcreteAggregate("10", "Name", 1, "0000-0000-0000", []event.IEvent{
						ForTestMakeEvent("10", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
					}),
				},
			},
			wantErr:  true,
			expected: nil,
		},
		{
			name: "save multiple aggregates in prefilled store",
			args: args{
				ctx: context.Background(),
				eventStore: func() event.EventStore {
					store, err, _ := eventstore.New(adapter())
					if err != nil {
						t.Errorf("test case preparation %v failed: %v", t.Name(), err)
					}
					_, filledStore := ForTestGetFilledRepo(store, "0000-0000-0000")
					return filledStore
				},
				aggregates: []event.AggregateWithEventSourcingSupport{
					newForTestConcreteAggregate("1", "Name", 5, "0000-0000-0000", []event.IEvent{
						ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 5, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 5, time.UTC)),
					}),
					newForTestConcreteAggregate("2", "Name", 0, "0000-0000-0000", []event.IEvent{
						ForTestMakeCreateEvent("2", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
					}),
				},
			},
			wantErr: false,
			expected: []event.PersistenceEvents{
				{Events: []event.PersistenceEvent{
					ForTestEvent0(),
					ForTestEventPatch(),
					ForTestEvent1(),
					ForTestEvent2(),
					ForTestEvent5(),
				}, Version: 6},
				{Events: []event.PersistenceEvent{
					ForTestEvent0_Aggregate2(),
				}, Version: 1},
			},
		},
		{
			name: "retry for a single aggregate",
			args: args{
				ctx: context.Background(),
				eventStore: func() event.EventStore {
					store, err, _ := eventstore.New(adapter(),
						eventstore.WithConcurrentModificationStrategy("forTestConcreteAggregate", event.Ignore))
					if err != nil {
						t.Errorf("test case preparation %v failed: %v", t.Name(), err)
					}
					_, err = store.Save(context.Background(), "0000-0000-0000", []event.PersistenceEvent{resetVersionForSave(ForTestEvent0())}, 0)
					if err != nil {
						panic("error in test preparation")
					}
					return store
				},
				aggregates: []event.AggregateWithEventSourcingSupport{
					newForTestConcreteAggregate("1", "Name", 0, "0000-0000-0000", []event.IEvent{
						ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 5, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 5, time.UTC)),
					}),
					newForTestConcreteAggregate("2", "Name", 0, "0000-0000-0000", []event.IEvent{
						ForTestMakeCreateEvent("2", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
					}),
				},
			},
			wantErr: false,
			expected: []event.PersistenceEvents{
				{Events: []event.PersistenceEvent{
					ForTestEvent0(),
					ForTestEvent5b(),
				}, Version: 2},
				{Events: []event.PersistenceEvent{
					ForTestEvent0_Aggregate2(),
				}, Version: 1},
			},
		},
		{
			name: "happy path save aggregate and patch it",
			args: args{
				ctx: context.Background(),
				eventStore: func() event.EventStore {
					adp := adapter()
					store, err, _ := eventstore.New(adp,
						eventstore.WithConcurrentModificationStrategy("forTestConcreteAggregate", event.Ignore))
					if err != nil {
						t.Errorf("test case preparation %v failed: %v", t.Name(), err)
					}

					return store
				},

				aggregates: []event.AggregateWithEventSourcingSupport{
					newForTestConcreteAggregate("1", "Name", 0, "0000-0000-0000", []event.IEvent{
						ForTestMakeCreateEvent("1", "0000-0000-0000", time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
						ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
						ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
						ForTestMakePatchEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 0, time.UTC)),
					}),
				},
			},
			wantErr: false,
			expected: []event.PersistenceEvents{
				{Events: []event.PersistenceEvent{
					ForTestEvent0(),
					ForTestEventPatch(),
					ForTestEvent1(),
					ForTestEvent2(),
				}, Version: 4},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			eventStore := tt.args.eventStore()
			defer cleanUp()

			resCh, err := event.SaveAggregates(tt.args.ctx, eventStore, tt.args.aggregates...)
			if (err != nil) != tt.wantErr {
				t.Errorf("SaveAggregates() error = %v, wantErr %v", err, tt.wantErr)
			}
			if resCh != nil {
				<-resCh
			}

			//nil case in save
			if tt.args.aggregates == nil {
				return
			}

			eventStream, err := eventStore.LoadAllAsAt(tt.args.ctx, tt.args.aggregates[0].GetTenantID(), time.Now())
			if !tt.wantErr && err != nil {
				t.Error(err)
			}
			//hack for UUID
			for sId, stream := range eventStream {
				for eId, pEvent := range stream.Events {
					tt.expected[sId].Events[eId].ID = pEvent.ID
				}
			}

			if !reflect.DeepEqual(eventStream, tt.expected) {
				t.Errorf("SaveAggregates()\ngot:  %+v\nwant: %+v", eventStream, tt.expected)
			}
		})
	}
}

func testSaveAggregatesWithSnapShot(t *testing.T, eventStoreFactory func() event.EventStore, cleanUp func()) {
	cleanUp()
	type args struct {
		ctx        context.Context
		aggregates []event.AggregateWithEventSourcingSupport
		eventStore func() event.EventStore
	}
	tests := []struct {
		name            string
		args            args
		wantErr         bool
		wantEventStream []event.EventStream
		wantVersion     int
		wantSnapshot    bool
	}{
		{
			name: "save snapshots with aggregates stream in empty repro without patches ",
			args: args{
				ctx: context.Background(),
				aggregates: []event.AggregateWithEventSourcingSupport{
					newForTestConcreteAggregate("1", "Name", 0, "0000-0000-0000", []event.IEvent{
						ForTestMakeCreateEvent("1", "0000-0000-0000", time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
						ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
						ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
						ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC)),
						ForTestMakeSnapshot("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 4, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 4, time.UTC)),
					}),
					newForTestConcreteAggregate("2", "Name", 0, "0000-0000-0000", []event.IEvent{
						ForTestMakeCreateEvent("2", "0000-0000-0000", time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
						ForTestMakeEvent("2", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
						ForTestMakeEvent("2", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
						ForTestMakeSnapshot("2", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC)),
					}),
				},
				eventStore: func() event.EventStore { return eventStoreFactory() },
			},
			wantErr: false,
			wantEventStream: []event.EventStream{
				{Stream: []event.IEvent{ForTestMakeSnapshot("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 4, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 4, time.UTC))},
					Version: 4},
				{Stream: []event.IEvent{ForTestMakeSnapshot("2", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC))},
					Version: 3},
			},
		},
		{
			name: "save snapshots splitt up in aggregates stream in empty repro without patches ",
			args: args{
				ctx: context.Background(),
				aggregates: []event.AggregateWithEventSourcingSupport{
					newForTestConcreteAggregate("1", "Name", 0, "0000-0000-0000", []event.IEvent{
						ForTestMakeCreateEvent("1", "0000-0000-0000", time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
						ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
						ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
						ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC)),
					}),
					newForTestConcreteAggregate("2", "Name", 0, "0000-0000-0000", []event.IEvent{
						ForTestMakeCreateEvent("2", "0000-0000-0000", time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
						ForTestMakeEvent("2", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
						ForTestMakeEvent("2", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
					}),
					newForTestConcreteAggregate("1", "Name", 4, "0000-0000-0000", []event.IEvent{
						ForTestMakeSnapshot("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 4, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 4, time.UTC)),
					}),
					newForTestConcreteAggregate("2", "Name", 3, "0000-0000-0000", []event.IEvent{
						ForTestMakeSnapshot("2", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC)),
					}),
				},
				eventStore: func() event.EventStore { return eventStoreFactory() },
			},
			wantErr: false,
			wantEventStream: []event.EventStream{
				{Stream: []event.IEvent{ForTestMakeSnapshot("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 4, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 4, time.UTC))},
					Version: 4},
				{Stream: []event.IEvent{ForTestMakeSnapshot("2", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC))},
					Version: 3},
			},
		},
		{
			name: "save snapshots and invalid by patch splitt up in aggregates stream in empty repro",
			args: args{
				ctx: context.Background(),
				aggregates: []event.AggregateWithEventSourcingSupport{
					newForTestConcreteAggregate("1", "Name", 4, "0000-0000-0000", []event.IEvent{
						ForTestMakePatchEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 5, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
					}),
				},
				eventStore: func() event.EventStore {
					store := eventStoreFactory()
					aggregates := []event.AggregateWithEventSourcingSupport{
						newForTestConcreteAggregate("1", "Name", 0, "0000-0000-0000", []event.IEvent{
							ForTestMakeCreateEvent("1", "0000-0000-0000", time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
							ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
							ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
							ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC)),
						}),
						newForTestConcreteAggregate("2", "Name", 0, "0000-0000-0000", []event.IEvent{
							ForTestMakeCreateEvent("2", "0000-0000-0000", time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
							ForTestMakeEvent("2", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
							ForTestMakeEvent("2", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
						}),
						newForTestConcreteAggregate("1", "Name", 4, "0000-0000-0000", []event.IEvent{
							ForTestMakeSnapshot("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 4, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 4, time.UTC)),
						}),
						newForTestConcreteAggregate("2", "Name", 3, "0000-0000-0000", []event.IEvent{
							ForTestMakeSnapshot("2", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC)),
						}),
					}
					_, err := event.SaveAggregates(context.Background(), store, aggregates...)

					if err != nil {
						panic("error in test case preparation")
					}
					return store
				},
			},
			wantErr: false,
			wantEventStream: []event.EventStream{
				{Stream: []event.IEvent{
					ForTestMakeCreateEvent("1", "0000-0000-0000", time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
					ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
					ForTestMakePatchEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 5, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
					ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
					ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC))},
					Version: 5},
				{Stream: []event.IEvent{
					ForTestMakeSnapshot("2", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC))},
					Version: 3},
			},
		},
		{
			name: "save patch and invalid snapshot splitt up in aggregates stream in empty repro",
			args: args{
				ctx: context.Background(),
				aggregates: []event.AggregateWithEventSourcingSupport{
					newForTestConcreteAggregate("1", "Name", 0, "0000-0000-0000", []event.IEvent{
						ForTestMakeCreateEvent("1", "0000-0000-0000", time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
						ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
						ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
						ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC)),
					}),
					newForTestConcreteAggregate("2", "Name", 0, "0000-0000-0000", []event.IEvent{
						ForTestMakeCreateEvent("2", "0000-0000-0000", time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
						ForTestMakeEvent("2", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
						ForTestMakeEvent("2", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
					}),
					newForTestConcreteAggregate("1", "Name", 4, "0000-0000-0000", []event.IEvent{
						ForTestMakePatchEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
					}),
					newForTestConcreteAggregate("1", "Name", 5, "0000-0000-0000", []event.IEvent{
						ForTestMakeSnapshot("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 4, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 4, time.UTC)),
					}),
					newForTestConcreteAggregate("2", "Name", 3, "0000-0000-0000", []event.IEvent{
						ForTestMakeSnapshot("2", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC)),
					}),
				},
				eventStore: func() event.EventStore { return eventStoreFactory() },
			},
			wantErr: false,
			wantEventStream: []event.EventStream{
				{Stream: []event.IEvent{
					ForTestMakeCreateEvent("1", "0000-0000-0000", time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
					ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
					ForTestMakePatchEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
					ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
					ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC))},
					Version: 5},
				{Stream: []event.IEvent{
					ForTestMakeSnapshot("2", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC))},
					Version: 3},
			},
		},
		{
			name: "save snapshots in aggregates stream in prefilled repro without patches ",
			args: args{
				ctx: context.Background(),
				aggregates: []event.AggregateWithEventSourcingSupport{newForTestConcreteAggregate("1", "Name", 3, "0000-0000-0000", []event.IEvent{
					ForTestMakeSnapshot("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 4, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 4, time.UTC)),
				}),
					newForTestConcreteAggregate("2", "Name", 0, "0000-0000-0000", []event.IEvent{
						ForTestMakeCreateEvent("2", "0000-0000-0000", time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
						ForTestMakeEvent("2", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
						ForTestMakeEvent("2", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
						ForTestMakeEvent("2", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC)),
						ForTestMakeSnapshot("2", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 4, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 4, time.UTC)),
					}),
				},
				eventStore: func() event.EventStore {
					store := eventStoreFactory()
					_, filledStore := ForTestGetFilledRepoWithoutPatch(store, "0000-0000-0000")
					return filledStore
				},
			},
			wantErr: false,
			wantEventStream: []event.EventStream{
				{Stream: []event.IEvent{ForTestMakeSnapshot("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 4, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 4, time.UTC))},
					Version: 3},
				{Stream: []event.IEvent{ForTestMakeSnapshot("2", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 4, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 4, time.UTC))},
					Version: 4},
			},
		},
		{
			name: "save snapshots and invalid by patch split up in aggregates stream in empty repo with 10 snapshot events",
			args: args{
				ctx: context.Background(),
				aggregates: []event.AggregateWithEventSourcingSupport{
					newForTestConcreteAggregate("1", "Name", 0, "0000-0000-0000", []event.IEvent{
						ForTestMakeCreateEvent("1", "0000-0000-0000", time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
						ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
						ForTestMakeSnapshot("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
						ForTestMakeSnapshot("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC)),
						ForTestMakeSnapshot("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 4, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 4, time.UTC)),
						ForTestMakeSnapshot("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 5, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 5, time.UTC)),
						ForTestMakeSnapshot("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 6, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 6, time.UTC)),
						ForTestMakeSnapshot("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 7, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 7, time.UTC)),
						ForTestMakeSnapshot("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 8, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 8, time.UTC)),
						ForTestMakeSnapshot("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 9, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 9, time.UTC)),
						ForTestMakeSnapshot("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 10, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 10, time.UTC)),
						ForTestMakeSnapshot("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 11, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 11, time.UTC)),
					}),
				},
				eventStore: func() event.EventStore { return eventStoreFactory() },
			},
			wantErr: false,
			wantEventStream: []event.EventStream{
				{Stream: []event.IEvent{
					ForTestMakeSnapshot("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 11, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 11, time.UTC))},
					Version: 2},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			eventStore := tt.args.eventStore()
			defer cleanUp()

			resCh, err := event.SaveAggregates(tt.args.ctx, eventStore, tt.args.aggregates...)
			if (err != nil) != tt.wantErr {
				t.Errorf("SaveAggregates() error = %v, wantErr %v", err, tt.wantErr)
			}
			<-resCh
			gotEventStream, err := event.LoadAllOfAggregateTypeAsAt(tt.args.ctx, tt.args.aggregates[0].GetTenantID(), "forTestConcreteAggregate", time.Now(), eventStore)
			if !tt.wantErr && err != nil {
				t.Error(err)
			}

			testdata.AssertEqualEventStreams(t, gotEventStream, tt.wantEventStream)

		})
	}
}

func testSaveAggregateWithConcurrentModificationException(t *testing.T, adapter func() persistence.Port, cleanUp func()) {
	cleanUp()
	type args struct {
		ctx        context.Context
		aggregate1 event.AggregateWithEventSourcingSupport
		aggregate2 event.AggregateWithEventSourcingSupport
		eventStore func() event.EventStore
	}
	tests := []struct {
		name            string
		args            args
		wantErr1Assert  func(err error) bool
		wantErr2Assert  func(err error) bool
		wantEventStream []event.IEvent
		wantVersion     int
	}{
		{
			name: "No strategy - concurrent modification exception",
			args: args{
				ctx: context.Background(),
				aggregate1: newForTestConcreteAggregate("1", "Name", 0, "0000-0000-0000", []event.IEvent{
					ForTestMakeCreateEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 0, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 0, time.UTC)),
				}),
				aggregate2: newForTestConcreteAggregate("1", "Name", 0, "0000-0000-0000", []event.IEvent{
					ForTestMakeCreateEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
				}),
				eventStore: func() event.EventStore {
					store, err, _ := eventstore.New(adapter(),
						eventstore.WithConcurrentModificationStrategy("forTestConcreteAggregate", event.Fail),
					)
					if err != nil {
						t.Errorf("test case preparation %v failed:%v", t.Name(), err)
					}
					return store
				},
			},
			wantErr1Assert: func(err error) bool {
				return err == nil
			},
			wantErr2Assert: func(err error) bool {
				var wantErr *event.ErrorConcurrentModification
				return errors.As(err, &wantErr)
			},
			wantEventStream: []event.IEvent{
				ForTestMakeCreateEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 0, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 0, time.UTC)),
			},
			wantVersion: 1,
		},
		{
			name: "retry strategy - both are saved",
			args: args{
				ctx: context.Background(),
				aggregate1: newForTestConcreteAggregate("1", "Name", 0, "0000-0000-0000", []event.IEvent{
					ForTestMakeCreateEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 0, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 0, time.UTC)),
				}),
				aggregate2: newForTestConcreteAggregate("1", "Name", 0, "0000-0000-0000", []event.IEvent{
					ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
				}),

				eventStore: func() event.EventStore {
					store, err, _ := eventstore.New(adapter(),
						eventstore.WithConcurrentModificationStrategy("forTestConcreteAggregate", event.Ignore),
					)
					if err != nil {
						t.Errorf("test case preparation %v failed: %v", t.Name(), err)
					}
					return store
				},
			},
			wantErr1Assert: func(err error) bool {
				return err == nil
			},
			wantErr2Assert: func(err error) bool {
				return err == nil
			},
			wantEventStream: []event.IEvent{
				ForTestMakeCreateEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 0, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 0, time.UTC)),
				ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
			},
			wantVersion: 2,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			eventStore := tt.args.eventStore()
			defer cleanUp()

			if _, err := event.SaveAggregate(tt.args.ctx, eventStore, tt.args.aggregate1); !tt.wantErr1Assert(err) {
				t.Errorf("SaveAggregate() error = %v, wantErr %v", err, tt.wantErr1Assert(err))
			}
			if _, err := event.SaveAggregate(tt.args.ctx, eventStore, tt.args.aggregate2); !tt.wantErr2Assert(err) {
				t.Errorf("SaveAggregate() error = %v, wantErr %v", err, tt.wantErr2Assert(err))
			}
			gotEventStream, gotVersion, _ := event.LoadAggregateAsAt(tt.args.ctx, tt.args.aggregate1.GetTenantID(), "forTestConcreteAggregate", tt.args.aggregate1.GetID(), time.Now(), eventStore)
			testdata.AssertEqualStream(t, gotEventStream, tt.wantEventStream)

			if gotVersion != tt.wantVersion {
				t.Errorf("SaveAggregate() gotVersion = %v, want %v", gotVersion, tt.wantVersion)
			}
		})
	}
}

func testSaveAggregateWithEphemeralEventTypes(t *testing.T, adapter func() persistence.Port, cleanUp func()) {
	cleanUp()
	type args struct {
		ctx        context.Context
		aggregate  event.AggregateWithEventSourcingSupport
		eventStore func() event.EventStore
	}
	tests := []struct {
		name            string
		args            args
		wantErr         bool
		wantEventStream []event.IEvent
		wantVersion     int
	}{
		{
			name: "multiple event type without projection",
			args: args{
				ctx: context.Background(),
				aggregate: newForTestConcreteAggregate("1", "Name", 0, "0000-0000-0000", []event.IEvent{
					ForTestMakeCreateEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 0, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 0, time.UTC)),
					ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
					ForTestMakeEvent2("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
					ForTestMakeEvent3("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), ""),
				}),
				eventStore: func() event.EventStore {
					store, err, _ := eventstore.New(adapter(),
						eventstore.WithEphemeralEventTypes("forTestConcreteAggregate",
							[]string{
								"github.com/global-soft-ba/go-eventstore/tests/forTestEvent2",
								"github.com/global-soft-ba/go-eventstore/tests/forTestEvent3",
							}),
					)

					if err != nil {
						t.Errorf("test case preparation %v failed:%v", t.Name(), err)
					}
					return store
				},
			},
			wantErr: false,
			wantEventStream: []event.IEvent{
				ForTestMakeCreateEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 0, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 0, time.UTC)),
				ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
			},
			wantVersion: 4,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			eventStore := tt.args.eventStore()
			defer cleanUp()

			if _, err := event.SaveAggregate(tt.args.ctx, eventStore, tt.args.aggregate); (err != nil) != tt.wantErr {
				t.Errorf("SaveAggregate() error = %v, wantErr %v", err, tt.wantErr)
			}

			gotEventStream, gotVersion, _ := event.LoadAggregateAsAt(tt.args.ctx, tt.args.aggregate.GetTenantID(), "forTestConcreteAggregate", tt.args.aggregate.GetID(), time.Now(), eventStore)
			testdata.AssertEqualStream(t, gotEventStream, tt.wantEventStream)

			if gotVersion != tt.wantVersion {
				t.Errorf("SaveAggregate() gotVersion = %v, want %v", gotVersion, tt.wantVersion)
			}
		})
	}
}

func testSaveAggregatesConcurrently(t *testing.T, adapter func() persistence.Port, cleanUp func()) {
	cleanUp()
	type args struct {
		ctx              context.Context
		aggregatesFirst  []event.AggregateWithEventSourcingSupport
		aggregatesSecond []event.AggregateWithEventSourcingSupport
		eventStore       func() event.EventStore
	}
	tests := []struct {
		name          string
		args          args
		waitTime      time.Duration
		wantErr       bool
		wantErrAssert func(err error) bool
		wantErrorType any
	}{
		{
			name: "concurrent aggregate access error",
			args: args{
				ctx: context.Background(),
				aggregatesFirst: []event.AggregateWithEventSourcingSupport{
					newForTestConcreteAggregate("1", "Name", 0, "0000-0000-0000", []event.IEvent{
						ForTestMakeCreateEvent("1", "0000-0000-0000", time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
					}),
					newForTestConcreteAggregate("2", "Name", 0, "0000-0000-0000", []event.IEvent{
						ForTestMakeCreateEvent("2", "0000-0000-0000", time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
					}),
				},
				aggregatesSecond: []event.AggregateWithEventSourcingSupport{
					newForTestConcreteAggregate("1", "Name", 0, "0000-0000-0000", []event.IEvent{
						ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
					}),
				},

				eventStore: func() event.EventStore {
					store, err, _ := eventstore.New(adapter(),
						eventstore.WithConcurrentModificationStrategy("forTestConcreteAggregate", event.Ignore),
						eventstore.WithSaveRetryDurations([]time.Duration{5}),
					)
					if err != nil {
						t.Errorf("test case preparation %v failed:%v", t.Name(), err)
					}
					return store
				},
			},
			waitTime: 16 * time.Millisecond,
			wantErr:  true,
			wantErrAssert: func(err error) bool {
				var wantErr *event.ErrorConcurrentAggregateAccess
				return errors.As(err, &wantErr)
			},
			wantErrorType: &event.ErrorConcurrentAggregateAccess{},
		},
		{
			name: "no concurrent aggregate access error",
			args: args{
				ctx: context.Background(),
				aggregatesFirst: []event.AggregateWithEventSourcingSupport{
					newForTestConcreteAggregate("1", "Name", 0, "0000-0000-0000", []event.IEvent{
						ForTestMakeCreateEvent("1", "0000-0000-0000", time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
					}),
					newForTestConcreteAggregate("2", "Name", 0, "0000-0000-0000", []event.IEvent{
						ForTestMakeCreateEvent("2", "0000-0000-0000", time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
					}),
				},
				aggregatesSecond: []event.AggregateWithEventSourcingSupport{
					newForTestConcreteAggregate("1", "Name", 0, "0000-0000-0000", []event.IEvent{
						ForTestMakeEvent("1", "0000-0000-0000", time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
					}),
				},
				eventStore: func() event.EventStore {
					store, err, _ := eventstore.New(adapter(),
						eventstore.WithConcurrentModificationStrategy("forTestConcreteAggregate", event.Ignore),
					)
					if err != nil {
						t.Errorf("test case preparation %v failed:%v", t.Name(), err)
					}
					return store
				},
			},
			waitTime: 200 * time.Millisecond,
			wantErr:  false,
			wantErrAssert: func(err error) bool {
				return err == nil
			},
			wantErrorType: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			eventStore := tt.args.eventStore()
			defer func() {
				cleanUp()
			}()

			//We need a wait group to make sure that the subroutine is finished before the next test case started
			wg := sync.WaitGroup{}
			wg.Add(1)
			go func() {
				_, errIntern := event.SaveAggregates(context.Background(), eventStore, tt.args.aggregatesFirst...)
				if errIntern != nil {
					t.Errorf("SaveAggregates() subroutine error = %v, wantErr %v", errIntern, false)
				}
				wg.Done()
			}()
			time.Sleep(tt.waitTime)
			_, err := event.SaveAggregates(tt.args.ctx, eventStore, tt.args.aggregatesSecond...)
			wg.Wait()

			if (err != nil) != tt.wantErr {
				t.Errorf("Concurrent SaveAggregates()  error = %v, wantErr %v", err, tt.wantErr)
			}

			if (err != nil && tt.wantErrAssert(err)) != tt.wantErr {
				t.Errorf("Concurrent SaveAggregates() wrong error type got error %v want error type %v", err, reflect.TypeOf(tt.wantErrorType))
			}
		})
	}
}
