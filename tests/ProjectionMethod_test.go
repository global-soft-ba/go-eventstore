package tests

import (
	"context"
	"errors"
	"fmt"
	"github.com/global-soft-ba/go-eventstore"
	"github.com/global-soft-ba/go-eventstore/eventstore"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/domain/model/projection"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/port/persistence"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/shared"
	"github.com/global-soft-ba/go-eventstore/tests/testdata"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"strconv"
	"sync"
	"testing"
	"time"
)

func testSaveAggregateWithProjection(t *testing.T, testType testType, projType event.ProjectionType, adapter func() persistence.Port, cleanUp func()) {
	cleanUp()
	tenantID := "0000-0000-0000"

	type args struct {
		excludeFromUnitTest bool
		ctx                 context.Context
		aggregate           event.AggregateWithEventSourcingSupport
		eventStore          func() (event.EventStore, event.Projection, persistence.Port)
	}
	tests := []struct {
		name                string
		args                args
		wantErr             bool
		wantProjectionError bool
		projAsserts         projectionAsserts
	}{
		{
			name: string(projType) + "---" + "execute projection with one projected event type in empty repro",
			args: args{
				excludeFromUnitTest: false,
				ctx:                 context.Background(),
				aggregate: newForTestConcreteAggregate("1", "Name", 0, tenantID, []event.IEvent{
					ForTestMakeCreateEvent("1", tenantID, time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
					ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
					ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
				}),
				eventStore: func() (event.EventStore, event.Projection, persistence.Port) {
					projectionTypeOne := newTestProjectionTypeOne("projection_1", tenantID, 0, 1)
					adp := adapter()
					store, err, errCh := eventstore.New(adp,
						eventstore.WithProjection(projectionTypeOne),
					)
					if err != nil {
						panic("error in test case preparation")
					}

					err = <-errCh
					if err != nil {
						panic("error in test case preparation")
					}

					return store, projectionTypeOne, adp
				},
			},
			wantErr:             false,
			wantProjectionError: false,
			projAsserts: projectionAsserts{
				id:                 shared.NewProjectionID(tenantID, "projection_1"),
				wantPrepCounter:    0,
				wantExecuteCounter: 3,
				wantFinishCounter:  0,
				wantState:          projection.Running,
				wantUpdatedAt:      true,
				wantEventStream: []event.IEvent{
					ForTestMakeCreateEvent("1", tenantID, time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
					ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
					ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
				},
			},
		},
		{
			name: string(projType) + "---" + "execute projection with one projected event type and close event in empty repro",
			args: args{
				excludeFromUnitTest: false,
				ctx:                 context.Background(),
				aggregate: newForTestConcreteAggregate("1", "Name", 0, tenantID, []event.IEvent{
					ForTestMakeCreateEvent("1", tenantID, time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
					ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
					ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
					ForTestMakeCloseEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC)),
				}),
				eventStore: func() (event.EventStore, event.Projection, persistence.Port) {
					projectionTypeOne := newTestProjectionTypeOne("projection_1", tenantID, 0, 1)
					adp := adapter()
					store, err, errCh := eventstore.New(adp,
						eventstore.WithProjection(projectionTypeOne),
					)
					if err != nil {
						panic("error in test case preparation")
					}

					err = <-errCh
					if err != nil {
						panic("error in test case preparation")
					}

					return store, projectionTypeOne, adp
				},
			},
			wantErr:             false,
			wantProjectionError: false,
			projAsserts: projectionAsserts{
				id:                 shared.NewProjectionID(tenantID, "projection_1"),
				wantPrepCounter:    0,
				wantExecuteCounter: 4,
				wantFinishCounter:  0,
				wantState:          projection.Running,
				wantUpdatedAt:      true,
				wantEventStream: []event.IEvent{
					ForTestMakeCreateEvent("1", tenantID, time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
					ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
					ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
					ForTestMakeCloseEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC))},
			},
		},
		{
			name: string(projType) + "---" + "execute projection with different projected event types in empty repro",
			args: args{
				excludeFromUnitTest: false,
				ctx:                 context.Background(),
				aggregate: newForTestConcreteAggregate("1", "Name", 0, tenantID, []event.IEvent{
					ForTestMakeCreateEvent("1", tenantID, time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
					ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
					ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
					ForTestMakeEvent2("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC)),
				}),
				eventStore: func() (event.EventStore, event.Projection, persistence.Port) {
					projectionTypeOne := newTestProjectionTwoTypes("projection_1", tenantID, 0, 1)
					adp := adapter()
					store, _, _ := eventstore.New(adp,
						eventstore.WithProjection(projectionTypeOne),
						eventstore.WithProjectionType(projectionTypeOne.ID(), projType),
					)
					evtStore := store

					return evtStore, projectionTypeOne, adp
				},
			},
			wantErr:             false,
			wantProjectionError: false,
			projAsserts: projectionAsserts{
				id:                 shared.NewProjectionID(tenantID, "projection_1"),
				wantPrepCounter:    0,
				wantExecuteCounter: 4,
				wantFinishCounter:  0,
				wantState:          projection.Running,
				wantUpdatedAt:      true,
				wantEventStream: []event.IEvent{
					ForTestMakeCreateEvent("1", tenantID, time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
					ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
					ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
					ForTestMakeEvent2("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC))},
			},
		},
		{
			name: string(projType) + "---" + "execute projection with different projected event types in empty repro",
			args: args{
				excludeFromUnitTest: false,
				ctx:                 context.Background(),
				aggregate: newForTestConcreteAggregate("1", "Name", 0, tenantID, []event.IEvent{
					ForTestMakeCreateEvent("1", tenantID, time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
					ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
					ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
					ForTestMakeEvent2("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC)),
				}),
				eventStore: func() (event.EventStore, event.Projection, persistence.Port) {
					projectionTypeOne := newTestProjectionTwoTypes("projection_1", tenantID, 0, 1)
					adp := adapter()
					store, _, _ := eventstore.New(adp,
						eventstore.WithProjection(projectionTypeOne),
						eventstore.WithProjectionType(projectionTypeOne.ID(), projType),
					)
					evtStore := store

					return evtStore, projectionTypeOne, adp
				},
			},
			wantErr:             false,
			wantProjectionError: false,
			projAsserts: projectionAsserts{
				id:                 shared.NewProjectionID(tenantID, "projection_1"),
				wantPrepCounter:    0,
				wantExecuteCounter: 4,
				wantFinishCounter:  0,
				wantState:          projection.Running,
				wantUpdatedAt:      true,
				wantEventStream: []event.IEvent{
					ForTestMakeCreateEvent("1", tenantID, time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
					ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
					ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
					ForTestMakeEvent2("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC))},
			},
		},
		{
			name: string(projType) + "---" + "execute multiple projection with different projected event types in empty repro (projection 1)",
			args: args{
				excludeFromUnitTest: false,
				ctx:                 context.Background(),
				aggregate: newForTestConcreteAggregate("1", "Name", 0, tenantID, []event.IEvent{
					ForTestMakeCreateEvent("1", tenantID, time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
					ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
					ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
					ForTestMakeEvent2("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC)),
				}),
				eventStore: func() (event.EventStore, event.Projection, persistence.Port) {
					projectionTypeOne := newTestProjectionTypeOne("projection_1", tenantID, 0, 1)
					testProjectionTypeTwo := newTestProjectionTypeTwo("projection_2", tenantID, 0, 1)
					adp := adapter()
					store, err, _ := eventstore.New(adp,
						eventstore.WithProjection(projectionTypeOne),
						eventstore.WithProjection(testProjectionTypeTwo),
						eventstore.WithProjectionType(projectionTypeOne.ID(), projType),
						eventstore.WithProjectionType(testProjectionTypeTwo.ID(), event.ESS),
					)

					if err != nil {
						panic("error in test preparation")
					}

					return store, projectionTypeOne, adp
				},
			},
			wantErr:             false,
			wantProjectionError: false,
			projAsserts: projectionAsserts{
				id:                 shared.NewProjectionID(tenantID, "projection_1"),
				wantPrepCounter:    0,
				wantExecuteCounter: 3,
				wantFinishCounter:  0,
				wantState:          projection.Running,
				wantUpdatedAt:      true,
				wantEventStream: []event.IEvent{
					ForTestMakeCreateEvent("1", tenantID, time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
					ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
					ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC))},
			},
		},
		{
			name: string(projType) + "---" + "execute multiple projection with different projected event types in empty repro (projection 2)",
			args: args{
				excludeFromUnitTest: false,
				ctx:                 context.Background(),
				aggregate: newForTestConcreteAggregate("1", "Name", 0, tenantID, []event.IEvent{
					ForTestMakeCreateEvent("1", tenantID, time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
					ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
					ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
					ForTestMakeEvent2("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC)),
				}),
				eventStore: func() (event.EventStore, event.Projection, persistence.Port) {
					projectionTypeOne := newTestProjectionTypeOne("projection_1", tenantID, 0, 1)
					testProjectionTypeTwo := newTestProjectionTypeTwo("projection_2", tenantID, 0, 1)
					adp := adapter()
					store, _, _ := eventstore.New(adp,
						eventstore.WithProjection(projectionTypeOne),
						eventstore.WithProjection(testProjectionTypeTwo),
						eventstore.WithProjectionType(projectionTypeOne.ID(), projType),
						eventstore.WithProjectionType(testProjectionTypeTwo.ID(), event.ESS),
					)
					evtStore := store

					return evtStore, testProjectionTypeTwo, adp
				},
			},
			wantErr:             false,
			wantProjectionError: false,
			projAsserts: projectionAsserts{
				id:                 shared.NewProjectionID(tenantID, "projection_1"),
				wantPrepCounter:    0,
				wantExecuteCounter: 1,
				wantFinishCounter:  0,
				wantState:          projection.Running,
				wantUpdatedAt:      true,
				wantEventStream: []event.IEvent{
					ForTestMakeEvent2("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC))},
			},
		},
		{
			name: string(projType) + "---" + "execute projection of non-projected event type in empty repro",
			args: args{
				excludeFromUnitTest: false,
				ctx:                 context.Background(),
				aggregate: newForTestConcreteAggregate("1", "Name", 0, tenantID, []event.IEvent{
					ForTestMakeCreateEvent2("1", tenantID, time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
					ForTestMakeEvent2("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
					ForTestMakeEvent2("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
				}),
				eventStore: func() (event.EventStore, event.Projection, persistence.Port) {
					projectionTypeOne := newTestProjectionTypeOne("projection_1", tenantID, 0, 1)
					adp := adapter()
					store, _, _ := eventstore.New(adp,
						eventstore.WithProjection(projectionTypeOne),
						eventstore.WithProjectionType(projectionTypeOne.ID(), projType),
					)
					evtStore := store

					return evtStore, projectionTypeOne, adp
				},
			},
			wantErr:             false,
			wantProjectionError: false,
			projAsserts: projectionAsserts{
				wantEventStream: []event.IEvent{},
			},
		},
		{
			name: string(projType) + "---" + "execute projection of non-projected/projected event type in empty repro",
			args: args{
				excludeFromUnitTest: false,
				ctx:                 context.Background(),
				aggregate: newForTestConcreteAggregate("1", "Name", 0, tenantID, []event.IEvent{
					ForTestMakeCreateEvent2("1", tenantID, time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
					ForTestMakeEvent2("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
					ForTestMakeEvent2("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
					ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC)),
				}),
				eventStore: func() (event.EventStore, event.Projection, persistence.Port) {
					projectionTypeOne := newTestProjectionTypeOne("projection_1", tenantID, 0, 1)
					adp := adapter()
					store, _, _ := eventstore.New(adp,
						eventstore.WithProjection(projectionTypeOne),
						eventstore.WithProjectionType(projectionTypeOne.ID(), projType),
					)
					evtStore := store

					return evtStore, projectionTypeOne, adp
				},
			},
			wantErr:             false,
			wantProjectionError: false,
			projAsserts: projectionAsserts{
				id:                 shared.NewProjectionID(tenantID, "projection_1"),
				wantPrepCounter:    0,
				wantExecuteCounter: 1,
				wantFinishCounter:  0,
				wantState:          projection.Running,
				wantUpdatedAt:      true,
				wantEventStream: []event.IEvent{
					ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC))},
			},
		},
		{
			name: string(projType) + "---" + "execute projection of one projected event type in existing repro with projection options",
			args: args{
				excludeFromUnitTest: false,
				ctx:                 context.Background(),
				aggregate: newForTestConcreteAggregate("1", "Name", 5, tenantID, []event.IEvent{
					ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 5, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 5, time.UTC)),
				}),

				eventStore: func() (event.EventStore, event.Projection, persistence.Port) {
					projectionTypeOne := newTestProjectionTypeOne("projection_1", tenantID, 0, 1)
					adp := adapter()
					store, err, chErr := eventstore.New(adp,
						eventstore.WithProjection(projectionTypeOne),
						eventstore.WithProjectionWorkerQueueLength(projectionTypeOne.ID(), 30),
						eventstore.WithProjectionTimeOut(projectionTypeOne.ID(), 20*time.Second),
						eventstore.WithHistoricalPatchStrategy(projectionTypeOne.ID(), event.Projected),
						eventstore.WithProjectionType(projectionTypeOne.ID(), projType),
					)
					if err != nil {
						panic("error in test case preparation")
					}

					err = <-chErr
					if err != nil {
						panic("error in test case preparation")
					}

					chErr, filledStore := ForTestGetFilledRepo(store, tenantID)

					err = <-chErr
					if err != nil {
						panic("error in test case preparation")
					}
					projectionTypeOne.(*forTestProjection).ForTestResetAll()
					return filledStore, projectionTypeOne, adp
				},
			},
			wantErr:             false,
			wantProjectionError: false,
			projAsserts: projectionAsserts{
				id:                 shared.NewProjectionID(tenantID, "projection_1"),
				wantPrepCounter:    0,
				wantExecuteCounter: 1,
				wantFinishCounter:  0,
				wantState:          projection.Running,
				wantUpdatedAt:      true,
				wantEventStream: []event.IEvent{
					ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 5, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 5, time.UTC))},
			},
		},
		{
			name: string(projType) + "---" + "execute projection of one projected event type in existing repro with historical patch (manual strategy)",
			args: args{
				excludeFromUnitTest: false,
				ctx:                 context.Background(),
				aggregate: newForTestConcreteAggregate("1", "Name", 5, tenantID, []event.IEvent{
					ForTestMakePatchEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 6, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC)),
					ForTestMakePatchEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 7, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
				}),
				eventStore: func() (event.EventStore, event.Projection, persistence.Port) {
					projectionTypeOne := newTestProjectionTypeOne("projection_1", tenantID, 0, 100)
					adp := adapter()
					store, err, _ := eventstore.New(adp,
						eventstore.WithProjection(projectionTypeOne),
						eventstore.WithHistoricalPatchStrategy(projectionTypeOne.ID(), event.Manual),
						eventstore.WithProjectionType(projectionTypeOne.ID(), projType),
					)
					if err != nil {
						panic("error in test case preparation")
					}

					events := newForTestConcreteAggregate("1", "Name", 0, tenantID, []event.IEvent{
						ForTestMakeCreateEvent("1", tenantID, time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
						ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
						ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
						ForTestMakePatchEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 0, time.UTC)),
						ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 5, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 5, time.UTC)),
					})

					errCh, err := event.SaveAggregate(context.Background(), store, events)
					if err != nil {
						panic("error in test case preparation")
					}

					err = <-errCh
					if err != nil {
						panic("error in test case preparation")
					}

					projectionTypeOne.(*forTestProjection).ForTestResetAll()
					return store, projectionTypeOne, adp
				},
			},
			wantErr:             false,
			wantProjectionError: false,
			projAsserts: projectionAsserts{
				id:                 shared.NewProjectionID(tenantID, "projection_1"),
				wantPrepCounter:    0,
				wantExecuteCounter: 0,
				wantFinishCounter:  0,
				wantState:          projection.Running,
				wantUpdatedAt:      true,
				wantEventStream:    []event.IEvent{},
			},
		},
		{
			name: string(event.ECS) + "---" + "execute projection of one projected event type in empty repro with historical patch (error default strategy)",
			args: args{
				excludeFromUnitTest: false,
				ctx:                 context.Background(),
				aggregate: newForTestConcreteAggregate("1", "Name", 0, tenantID, []event.IEvent{
					ForTestMakeCreateEvent("1", tenantID, time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
					ForTestMakePatchEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 7, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
				}),
				eventStore: func() (event.EventStore, event.Projection, persistence.Port) {
					projectionTypeOne := newTestProjectionTypeOne("projection_1", tenantID, 0, 100)
					adp := adapter()
					store, err, _ := eventstore.New(adp,
						eventstore.WithProjection(projectionTypeOne),
						eventstore.WithProjectionType(projectionTypeOne.ID(), event.ECS),
					)
					if err != nil {
						panic("error in test case preparation")
					}

					return store, projectionTypeOne, adp
				},
			},
			wantErr:             false,
			wantProjectionError: true,
			projAsserts: projectionAsserts{
				id:                 shared.NewProjectionID(tenantID, "projection_1"),
				wantPrepCounter:    0,
				wantExecuteCounter: 0,
				wantFinishCounter:  0,
				wantState:          projection.Running,
				wantUpdatedAt:      true,
				wantEventStream:    []event.IEvent{},
			},
		},
		{
			name: string(event.CCS) + "---" + "execute projection of one projected event type in empty repro with historical patch (error default strategy)",
			args: args{
				excludeFromUnitTest: false,
				ctx:                 context.Background(),
				aggregate: newForTestConcreteAggregate("1", "Name", 0, tenantID, []event.IEvent{
					ForTestMakeCreateEvent("1", tenantID, time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
					ForTestMakePatchEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 7, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
				}),
				eventStore: func() (event.EventStore, event.Projection, persistence.Port) {
					projectionTypeOne := newTestProjectionTypeOne("projection_1", tenantID, 0, 100)
					adp := adapter()
					store, err, _ := eventstore.New(adp,
						eventstore.WithProjection(projectionTypeOne),
						eventstore.WithProjectionType(projectionTypeOne.ID(), event.CCS),
					)

					if err != nil {
						panic("error in test case preparation")
					}

					//Because of the testcases saving of aggregate fails.However,
					//the first (successfully) save initials corresponding projections for a tenant.
					//To make the later assertion fit for the projection, it must be created initially.
					if _, err = store.StartProjection(context.Background(), tenantID, projectionTypeOne.ID()); err != nil {
						panic("error in test case preparation")
					}

					return store, projectionTypeOne, adp
				},
			},
			wantErr:             true,
			wantProjectionError: false,
			projAsserts: projectionAsserts{
				id:                 shared.NewProjectionID(tenantID, "projection_1"),
				wantPrepCounter:    0,
				wantExecuteCounter: 0,
				wantFinishCounter:  0,
				wantState:          projection.Running,
				wantUpdatedAt:      true,
				wantEventStream:    []event.IEvent{},
			},
		},
		{
			name: string(projType) + "---" + "execute projection of one projected event type in existing repro with historical patch in several chunks (rebuild strategy)",
			args: args{
				excludeFromUnitTest: false,
				ctx:                 context.Background(),
				aggregate: newForTestConcreteAggregate("1", "Name", 4, tenantID, []event.IEvent{
					ForTestMakePatchEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 6, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC)),
					ForTestMakePatchEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 7, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
				}),
				eventStore: func() (event.EventStore, event.Projection, persistence.Port) {
					projectionTypeOne := newTestProjectionTypeOne("projection_1", tenantID, 0, 1)
					adp := adapter()
					store, err, _ := eventstore.New(adp,
						eventstore.WithProjection(projectionTypeOne),
						eventstore.WithHistoricalPatchStrategy(projectionTypeOne.ID(), event.Rebuild),
						eventstore.WithProjectionType(projectionTypeOne.ID(), projType),
					)
					if err != nil {
						panic("error in test case preparation")
					}

					events := newForTestConcreteAggregate("1", "Name", 0, tenantID, []event.IEvent{
						ForTestMakeCreateEvent("1", tenantID, time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
						ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
						ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
						ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 5, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 5, time.UTC)),
					})

					errCh, err := event.SaveAggregate(context.Background(), store, events)
					if err != nil {
						msg, _ := fmt.Printf("error in test case preparation %q", err)
						panic(msg)
					}

					err = <-errCh
					if err != nil {
						panic("error in test case preparation")
					}

					projectionTypeOne.(*forTestProjection).ForTestResetAll()
					return store, projectionTypeOne, adp
				},
			},
			wantErr:             false,
			wantProjectionError: false,
			projAsserts: projectionAsserts{
				id:                 shared.NewProjectionID(tenantID, "projection_1"),
				wantPrepCounter:    1,
				wantExecuteCounter: 6,
				wantFinishCounter:  1,
				wantState:          projection.Running,
				wantUpdatedAt:      true,
				wantEventStream: []event.IEvent{
					ForTestMakeCreateEvent("1", tenantID, time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
					ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
					ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
					ForTestMakePatchEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 7, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
					ForTestMakePatchEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 6, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC)),
					ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 5, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 5, time.UTC)),
				},
			},
		},
		{
			name: string(event.ECS) + "---" + "fail execute projection of one projected event type in existing repro with historical patch in several chunks (rebuild strategy)",
			args: args{
				excludeFromUnitTest: false,
				ctx:                 context.Background(),
				aggregate: newForTestConcreteAggregate("1", "Name", 4, tenantID, []event.IEvent{
					ForTestMakePatchEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 6, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC)),
					ForTestMakePatchEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 7, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
				}),
				eventStore: func() (event.EventStore, event.Projection, persistence.Port) {
					projectionTypeOne := newTestProjectionTypeOneWithExecuteFail("projection_1", tenantID, 1, true, 5) //5th projection execution fails
					adp := adapter()
					store, err, _ := eventstore.New(adp,
						eventstore.WithProjection(projectionTypeOne),
						eventstore.WithHistoricalPatchStrategy(projectionTypeOne.ID(), event.Rebuild),
						eventstore.WithProjectionType(projectionTypeOne.ID(), event.ECS),
					)
					if err != nil {
						panic("error in test case preparation")
					}

					events := newForTestConcreteAggregate("1", "Name", 0, tenantID, []event.IEvent{
						ForTestMakeCreateEvent("1", tenantID, time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
						ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
						ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
						ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 5, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 5, time.UTC)),
					})

					errCh, err := event.SaveAggregate(context.Background(), store, events)
					if err != nil {
						msg, _ := fmt.Printf("error in test case preparation %q", err)
						panic(msg)
					}

					err = <-errCh
					if err != nil {
						panic("error in test case preparation")
					}

					projectionTypeOne.(*forTestProjection).ForTestResetEventsOnly() //we wann see if the projection was trying to execute the events

					return store, projectionTypeOne, adp
				},
			},
			wantErr:             false,
			wantProjectionError: true,
			projAsserts: projectionAsserts{
				id:                 shared.NewProjectionID(tenantID, "projection_1"),
				wantPrepCounter:    1,
				wantExecuteCounter: 5,
				wantFinishCounter:  0,
				wantState:          projection.Erroneous,
				wantUpdatedAt:      true,
				wantEventStream:    []event.IEvent{},
			},
		},
		{
			name: string(event.CCS) + "---" + "fail execute projection of one projected event type in existing repro with historical patch in several chunks (rebuild strategy)",
			args: args{
				excludeFromUnitTest: true, //would need a second transaction to change the state (memDb does not support this)
				ctx:                 context.Background(),
				aggregate: newForTestConcreteAggregate("1", "Name", 4, tenantID, []event.IEvent{
					ForTestMakePatchEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 6, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC)),
					ForTestMakePatchEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 7, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
				}),
				eventStore: func() (event.EventStore, event.Projection, persistence.Port) {
					projectionTypeOne := newTestProjectionTypeOneWithExecuteFail("projection_1", tenantID, 1, true, 5) // 5th projection execution fails
					adp := adapter()
					store, err, _ := eventstore.New(adp,
						eventstore.WithProjection(projectionTypeOne),
						eventstore.WithHistoricalPatchStrategy(projectionTypeOne.ID(), event.Rebuild),
						eventstore.WithProjectionType(projectionTypeOne.ID(), event.CCS),
					)
					if err != nil {
						panic("error in test case preparation")
					}

					events := newForTestConcreteAggregate("1", "Name", 0, tenantID, []event.IEvent{
						ForTestMakeCreateEvent("1", tenantID, time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
						ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
						ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
						ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 5, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 5, time.UTC)),
					})

					errCh, err := event.SaveAggregate(context.Background(), store, events)
					if err != nil {
						msg, _ := fmt.Printf("error in test case preparation %q", err)
						panic(msg)
					}

					err = <-errCh
					if err != nil {
						panic("error in test case preparation")
					}

					projectionTypeOne.(*forTestProjection).ForTestResetEventsOnly() //we wann see if the projection was trying to execute the events

					return store, projectionTypeOne, adp
				},
			},
			wantErr:             true,
			wantProjectionError: false,
			projAsserts: projectionAsserts{
				id:                 shared.NewProjectionID(tenantID, "projection_1"),
				wantPrepCounter:    1,
				wantExecuteCounter: 5,
				wantFinishCounter:  0,
				wantState:          projection.Erroneous,
				wantUpdatedAt:      true,
				wantEventStream:    []event.IEvent{},
			},
		},
		{
			name: string(projType) + "---" + "execute projection of one projected event type in existing repro with historical patch in one chunk (rebuild strategy)",
			args: args{
				excludeFromUnitTest: false,
				ctx:                 context.Background(),
				aggregate: newForTestConcreteAggregate("1", "Name", 4, tenantID, []event.IEvent{
					ForTestMakePatchEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 6, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC)),
					ForTestMakePatchEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 7, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
				}),
				eventStore: func() (event.EventStore, event.Projection, persistence.Port) {
					projectionTypeOne := newTestProjectionTypeOne("projection_1", tenantID, 0, 100)
					adp := adapter()
					store, err, _ := eventstore.New(adp,
						eventstore.WithProjection(projectionTypeOne),
						eventstore.WithProjectionType(projectionTypeOne.ID(), projType),
						eventstore.WithHistoricalPatchStrategy(projectionTypeOne.ID(), event.Rebuild),
					)
					if err != nil {
						panic("error in test case preparation")
					}

					events := newForTestConcreteAggregate("1", "Name", 0, tenantID, []event.IEvent{
						ForTestMakeCreateEvent("1", tenantID, time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
						ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
						ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
						ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 5, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 5, time.UTC)),
					})

					errCh, err := event.SaveAggregate(context.Background(), store, events)
					if err != nil {
						panic("error in test case preparation")
					}

					err = <-errCh
					if err != nil {
						panic("error in test case preparation")
					}

					projectionTypeOne.(*forTestProjection).ForTestResetAll()
					return store, projectionTypeOne, adp
				},
			},
			wantErr:             false,
			wantProjectionError: false,
			projAsserts: projectionAsserts{
				id:                 shared.NewProjectionID(tenantID, "projection_1"),
				wantPrepCounter:    1,
				wantExecuteCounter: 1,
				wantFinishCounter:  1,
				wantState:          projection.Running,
				wantUpdatedAt:      true,
				wantEventStream: []event.IEvent{
					ForTestMakeCreateEvent("1", tenantID, time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
					ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
					ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
					ForTestMakePatchEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 7, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
					ForTestMakePatchEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 6, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC)),
					ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 5, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 5, time.UTC))},
			},
		},
		{
			name: string(projType) + "---" + "execute projection of one projected event type in existing repro with historical patch (rebuild since strategy)",
			args: args{
				excludeFromUnitTest: false,
				ctx:                 context.Background(),
				aggregate: newForTestConcreteAggregate("1", "Name", 4, tenantID, []event.IEvent{
					ForTestMakePatchEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 6, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC)),
					ForTestMakePatchEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 7, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
				}),
				eventStore: func() (event.EventStore, event.Projection, persistence.Port) {
					projectionTypeOne := newTestProjectionTypeOne("projection_1", tenantID, 0, 1)
					adp := adapter()
					store, err, _ := eventstore.New(adp,
						eventstore.WithProjection(projectionTypeOne),
						eventstore.WithProjectionType(projectionTypeOne.ID(), projType),
						eventstore.WithHistoricalPatchStrategy(projectionTypeOne.ID(), event.RebuildSince),
					)
					if err != nil {
						panic("error in test case preparation")
					}

					events := newForTestConcreteAggregate("1", "Name", 0, tenantID, []event.IEvent{
						ForTestMakeCreateEvent("1", tenantID, time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
						ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
						ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
						ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 5, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 5, time.UTC)),
					})

					errCh, err := event.SaveAggregate(context.Background(), store, events)
					if err != nil {
						panic("error in test case preparation")
					}

					err = <-errCh
					if err != nil {
						panic("error in test case preparation")
					}

					projectionTypeOne.(*forTestProjection).ForTestResetAll()
					return store, projectionTypeOne, adp
				},
			},
			wantErr:             false,
			wantProjectionError: false,
			projAsserts: projectionAsserts{
				id:                 shared.NewProjectionID(tenantID, "projection_1"),
				wantPrepCounter:    1,
				wantExecuteCounter: 4,
				wantFinishCounter:  1,
				wantState:          projection.Running,
				wantUpdatedAt:      true,
				wantEventStream: []event.IEvent{
					ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
					ForTestMakePatchEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 7, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
					ForTestMakePatchEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 6, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC)),
					ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 5, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 5, time.UTC))},
			},
		},
		{
			name: string(event.ECS) + "---" + "execute projection of one projected event type in existing repro with future patches",
			args: args{
				excludeFromUnitTest: false,
				ctx:                 context.Background(),
				aggregate: newForTestConcreteAggregate("1", "Name", 5, tenantID, []event.IEvent{
					ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 5, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 5, time.UTC)),
					ForTestMakePatchEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 6, time.UTC), time.Date(2221, 1, 1, 1, 1, 1, 5, time.UTC)),
				}),
				eventStore: func() (event.EventStore, event.Projection, persistence.Port) {
					projectionTypeOne := newTestProjectionTypeOne("projection_1", tenantID, 0, 1)
					adp := adapter()
					store, err, chErr := eventstore.New(adp,
						eventstore.WithProjection(projectionTypeOne),
						eventstore.WithProjectionType(projectionTypeOne.ID(), event.ECS),
						eventstore.WithHistoricalPatchStrategy(projectionTypeOne.ID(), event.Projected),
					)
					if err != nil {
						panic("error in test case preparation")
					}
					<-chErr

					chErr, filledStore := ForTestGetFilledRepo(store, tenantID)
					<-chErr

					projectionTypeOne.(*forTestProjection).ForTestResetAll()
					return filledStore, projectionTypeOne, adp
				},
			},
			wantErr:             false,
			wantProjectionError: false,
			projAsserts: projectionAsserts{
				id:                 shared.NewProjectionID(tenantID, "projection_1"),
				wantPrepCounter:    0,
				wantExecuteCounter: 1,
				wantFinishCounter:  0,
				wantState:          projection.Running,
				wantUpdatedAt:      true,
				wantEventStream: []event.IEvent{
					ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 5, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 5, time.UTC))},
			},
		},
		{
			name: string(event.CCS) + "---" + "execute projection of one projected event type in existing repro with future patches",
			args: args{
				excludeFromUnitTest: false,
				ctx:                 context.Background(),
				aggregate: newForTestConcreteAggregate("1", "Name", 5, tenantID, []event.IEvent{
					ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 5, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 5, time.UTC)),
					ForTestMakePatchEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 6, time.UTC), time.Date(2221, 1, 1, 1, 1, 1, 5, time.UTC)),
				}),
				eventStore: func() (event.EventStore, event.Projection, persistence.Port) {
					projectionTypeOne := newTestProjectionTypeOne("projection_1", tenantID, 0, 1)
					adp := adapter()
					store, err, chErr := eventstore.New(adp,
						eventstore.WithProjection(projectionTypeOne),
						eventstore.WithProjectionType(projectionTypeOne.ID(), event.CCS),
						eventstore.WithHistoricalPatchStrategy(projectionTypeOne.ID(), event.Projected),
					)
					if err != nil {
						panic("error in test case preparation")
					}
					<-chErr

					chErr, filledStore := ForTestGetFilledRepo(store, tenantID)
					<-chErr

					projectionTypeOne.(*forTestProjection).ForTestResetAll()
					return filledStore, projectionTypeOne, adp
				},
			},
			wantErr:             false,
			wantProjectionError: false,
			projAsserts: projectionAsserts{
				id:                 shared.NewProjectionID(tenantID, "projection_1"),
				wantPrepCounter:    0,
				wantExecuteCounter: 2,
				wantFinishCounter:  0,
				wantState:          projection.Running,
				wantUpdatedAt:      true,
				wantEventStream: []event.IEvent{
					ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 5, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 5, time.UTC)),
					ForTestMakePatchEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 6, time.UTC), time.Date(2221, 1, 1, 1, 1, 1, 5, time.UTC))},
			},
		},
		{
			name: string(projType) + "---" + "execute projection with batch insert/update events",
			args: args{
				excludeFromUnitTest: false,
				ctx:                 context.Background(),
				aggregate: newForTestConcreteAggregate("1", "Name", 9, tenantID, []event.IEvent{
					ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 10, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 10, time.UTC)),
					ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 11, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 11, time.UTC)),
					ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 12, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 12, time.UTC)),
					ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 13, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 13, time.UTC)),
					ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 14, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 14, time.UTC)),
					ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 15, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 15, time.UTC)),
					ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 16, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 16, time.UTC)),
					ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 17, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 17, time.UTC)),
					ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 18, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 18, time.UTC)),
					ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 19, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 19, time.UTC)),
					ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 20, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 20, time.UTC)),
				}),
				eventStore: func() (event.EventStore, event.Projection, persistence.Port) {
					projectionTypeOne := newTestProjectionTypeOne("projection_1", tenantID, 0*time.Second, 100)
					adp := adapter()
					store, err, chErr := eventstore.New(adp,
						eventstore.WithProjection(projectionTypeOne),
						eventstore.WithProjectionType(projectionTypeOne.ID(), projType),
					)
					if err != nil {
						panic("error in test case preparation")
					}
					<-chErr

					aggregate := newForTestConcreteAggregate("1", "Name", 0, tenantID, []event.IEvent{
						ForTestMakeCreateEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
						ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
						ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC)),
						ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 4, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 4, time.UTC)),
						ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 5, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 5, time.UTC)),
						ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 6, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 6, time.UTC)),
						ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 7, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 7, time.UTC)),
						ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 8, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 8, time.UTC)),
						ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 9, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 9, time.UTC)),
					})
					resCh, err := event.SaveAggregate(context.Background(), store, aggregate)
					if err != nil {
						panic("error in test case preparation")
					}
					<-resCh
					projectionTypeOne.(*forTestProjection).ForTestResetAll()
					return store, projectionTypeOne, adp
				},
			},
			wantErr:             false,
			wantProjectionError: false,
			projAsserts: projectionAsserts{
				id:                 shared.NewProjectionID(tenantID, "projection_1"),
				wantPrepCounter:    0,
				wantExecuteCounter: 1,
				wantFinishCounter:  0,
				wantState:          projection.Running,
				wantUpdatedAt:      true,
				wantEventStream: []event.IEvent{
					ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 10, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 10, time.UTC)),
					ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 11, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 11, time.UTC)),
					ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 12, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 12, time.UTC)),
					ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 13, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 13, time.UTC)),
					ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 14, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 14, time.UTC)),
					ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 15, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 15, time.UTC)),
					ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 16, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 16, time.UTC)),
					ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 17, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 17, time.UTC)),
					ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 18, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 18, time.UTC)),
					ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 19, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 19, time.UTC)),
					ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 20, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 20, time.UTC))},
			},
		},
		{
			name: string(event.ECS) + "---" + "execute projection with stopped state",
			args: args{
				excludeFromUnitTest: false,
				ctx:                 context.Background(),
				aggregate: newForTestConcreteAggregate("1", "Name", 0, tenantID, []event.IEvent{
					ForTestMakeCreateEvent("1", tenantID, time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
				}),

				eventStore: func() (event.EventStore, event.Projection, persistence.Port) {
					projectionTypeOne := newTestProjectionTypeOne("projection_1", tenantID, 0, 1)
					adp := adapter()
					store, _, _ := eventstore.New(adp,
						eventstore.WithProjection(projectionTypeOne),
						eventstore.WithProjectionType(projectionTypeOne.ID(), event.ECS),
					)

					chErr, err := store.StartProjection(context.Background(), tenantID, "projection_1")
					if err != nil {
						panic("error in test case preparation: start projection: " + err.Error())
					}
					<-chErr

					projectionTypeOne.(*forTestProjection).ForTestResetAll()
					err = store.StopProjection(context.Background(), tenantID, "projection_1")
					if err != nil {
						panic("error in test case preparation: stop projection: " + err.Error())
					}

					return store, projectionTypeOne, adp
				},
			},
			wantErr:             false,
			wantProjectionError: true,
			projAsserts: projectionAsserts{
				id:                 shared.NewProjectionID(tenantID, "projection_1"),
				wantPrepCounter:    0,
				wantExecuteCounter: 0,
				wantFinishCounter:  0,
				wantState:          projection.Stopped,
				wantUpdatedAt:      true,
				wantEventStream:    []event.IEvent{},
			},
		},
		{
			name: string(event.CCS) + "---" + "execute projection with stopped state",
			args: args{
				excludeFromUnitTest: true,
				ctx:                 context.Background(),
				aggregate: newForTestConcreteAggregate("1", "Name", 0, tenantID, []event.IEvent{
					ForTestMakeCreateEvent("1", tenantID, time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
				}),

				eventStore: func() (event.EventStore, event.Projection, persistence.Port) {
					projectionTypeOne := newTestProjectionTypeOne("projection_1", tenantID, 0, 1)
					adp := adapter()
					store, _, _ := eventstore.New(adp,
						eventstore.WithProjection(projectionTypeOne),
						eventstore.WithProjectionType(projectionTypeOne.ID(), event.CCS),
					)

					errCh, err := store.StartProjection(context.Background(), tenantID, "projection_1")
					if err != nil {
						panic("error in test case preparation: start projection: " + err.Error())
					}
					<-errCh
					projectionTypeOne.(*forTestProjection).ForTestResetAll()

					err = store.StopProjection(context.Background(), tenantID, "projection_1")
					if err != nil {
						panic("error in test case preparation: stop projection: " + err.Error())
					}

					return store, projectionTypeOne, adp
				},
			},
			wantErr:             true,
			wantProjectionError: false,
			projAsserts: projectionAsserts{
				id:                 shared.NewProjectionID(tenantID, "projection_1"),
				wantPrepCounter:    0,
				wantExecuteCounter: 0,
				wantFinishCounter:  0,
				wantState:          projection.Stopped,
				wantUpdatedAt:      true,
				wantEventStream:    []event.IEvent{},
			},
		},
		{
			name: string(projType) + "---" + "restart projection after stopped",
			args: args{
				excludeFromUnitTest: false,
				ctx:                 context.Background(),
				aggregate: newForTestConcreteAggregate("1", "Name", 0, tenantID, []event.IEvent{
					ForTestMakeCreateEvent("1", tenantID, time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
					ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
					ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
				}),

				eventStore: func() (event.EventStore, event.Projection, persistence.Port) {
					projectionTypeOne := newTestProjectionTypeOne("projection_1", tenantID, 0, 1)
					adp := adapter()
					store, _, _ := eventstore.New(adp,
						eventstore.WithProjection(projectionTypeOne),
						eventstore.WithProjectionType(projectionTypeOne.ID(), projType),
					)

					_ = store.StopProjection(context.Background(), tenantID, "projection_1")

					resCh, err := store.StartProjection(context.Background(), tenantID, "projection_1")
					if err != nil {
						panic(err)
					}
					<-resCh

					return store, projectionTypeOne, adp
				},
			},
			wantErr:             false,
			wantProjectionError: false,
			projAsserts: projectionAsserts{
				id:                 shared.NewProjectionID(tenantID, "projection_1"),
				wantPrepCounter:    0,
				wantExecuteCounter: 3,
				wantFinishCounter:  0,
				wantState:          projection.Running,
				wantUpdatedAt:      true,
				wantEventStream: []event.IEvent{
					ForTestMakeCreateEvent("1", tenantID, time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
					ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
					ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC))},
			},
		},
		{
			name: string(event.ECS) + "---" + "execute projection with timeout projection in empty repro",
			args: args{
				excludeFromUnitTest: false,
				ctx:                 context.Background(),
				aggregate: newForTestConcreteAggregate("1", "Name", 0, tenantID, []event.IEvent{
					ForTestMakeCreateEvent("1", tenantID, time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
					ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
					ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
				}),

				eventStore: func() (event.EventStore, event.Projection, persistence.Port) {
					projectionTypeOne := newTestProjectionTypeOne("projection_1", tenantID, 100*time.Millisecond, 3)
					adp := adapter()
					store, _, _ := eventstore.New(adp,
						eventstore.WithProjection(projectionTypeOne),
						eventstore.WithProjectionType(projectionTypeOne.ID(), event.ECS),
						eventstore.WithProjectionWorkerQueueLength(projectionTypeOne.ID(), 10),
						eventstore.WithProjectionTimeOut(projectionTypeOne.ID(), 10*time.Millisecond),
					)
					evtStore := store
					return evtStore, projectionTypeOne, adp
				},
			},
			wantErr:             false,
			wantProjectionError: true,
			projAsserts: projectionAsserts{
				id:                 shared.NewProjectionID(tenantID, "projection_1"),
				wantPrepCounter:    0,
				wantExecuteCounter: 1,
				wantFinishCounter:  0,
				wantState:          projection.Running,
				wantUpdatedAt:      true,
				wantEventStream:    []event.IEvent{},
			},
		},
		{
			name: string(event.CCS) + "---" + "execute projection with timeout projection in empty repro",
			args: args{
				excludeFromUnitTest: false,
				ctx:                 context.Background(),
				aggregate: newForTestConcreteAggregate("1", "Name", 0, tenantID, []event.IEvent{
					ForTestMakeCreateEvent("1", tenantID, time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
					ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
					ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
				}),

				eventStore: func() (event.EventStore, event.Projection, persistence.Port) {
					projectionTypeOne := newTestProjectionTypeOne("projection_1", tenantID, 100*time.Millisecond, 3)
					adp := adapter()
					store, _, _ := eventstore.New(adp,
						eventstore.WithProjection(projectionTypeOne),
						eventstore.WithProjectionType(projectionTypeOne.ID(), event.CCS),
						eventstore.WithProjectionWorkerQueueLength(projectionTypeOne.ID(), 10),
						eventstore.WithProjectionTimeOut(projectionTypeOne.ID(), 10*time.Millisecond),
					)

					evtStore := store

					_, err := store.StartProjection(context.Background(), tenantID, "projection_1")
					if err != nil {
						panic("error in test case preparation")
					}
					return evtStore, projectionTypeOne, adp
				},
			},
			wantErr:             true,
			wantProjectionError: false,
			projAsserts: projectionAsserts{
				id:                 shared.NewProjectionID(tenantID, "projection_1"),
				wantPrepCounter:    0,
				wantExecuteCounter: 1,
				wantFinishCounter:  0,
				wantState:          projection.Running,
				wantUpdatedAt:      true,
				wantEventStream:    []event.IEvent{},
			},
		},
		{
			name: "execute projection with ephemeral projected event types in empty repro",
			args: args{
				ctx: context.Background(),
				aggregate: newForTestConcreteAggregate("1", "Name", 0, "0000-0000-0000", []event.IEvent{
					ForTestMakeCreateEvent("1", "0000-0000-0000", time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
					ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
					ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
					ForTestMakeEvent2("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC)),
					ForTestMakeEvent3("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), ""),
				}),
				eventStore: func() (event.EventStore, event.Projection, persistence.Port) {
					projectionTwoTypes := newTestProjectionTwoTypes("projection_1", tenantID, 0, 1)
					adp := adapter()
					store, _, _ := eventstore.New(adp,
						eventstore.WithProjection(projectionTwoTypes),
						eventstore.WithEphemeralEventTypes("forTestConcreteAggregate",
							[]string{
								"github.com/global-soft-ba/go-eventstore/tests/forTestEvent2",
								"github.com/global-soft-ba/go-eventstore/tests/forTestEvent3",
							}),
					)
					evtStore := store
					return evtStore, projectionTwoTypes, adp
				},
			},
			wantErr:             false,
			wantProjectionError: false,
			projAsserts: projectionAsserts{
				id:                 shared.NewProjectionID(tenantID, "projection_1"),
				wantPrepCounter:    0,
				wantExecuteCounter: 4,
				wantFinishCounter:  0,
				wantState:          projection.Running,
				wantUpdatedAt:      true,
				wantEventStream: []event.IEvent{
					ForTestMakeCreateEvent("1", "0000-0000-0000", time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
					ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
					ForTestMakeEvent("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
					ForTestMakeEvent2("1", "0000-0000-0000", time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC))},
			},
		},
	}
	for _, tt := range tests {
		if tt.args.excludeFromUnitTest && testType == UnitTest {
			t.Skipf("skipped %s for test type %s (memeDB does not support multiple writer/transactions)", tt.name, testType)
		}

		t.Run(tt.name, func(t *testing.T) {
			store, proj, _ := tt.args.eventStore()
			defer cleanUp()

			res, err := event.SaveAggregate(tt.args.ctx, store, tt.args.aggregate)
			if err != nil != tt.wantErr {
				t.Errorf("SaveAggregate() gotError = %v, wantErr %v", err, tt.wantErr)
			}

			if err = assertProjectionError(res, tt.wantProjectionError); err != nil {
				t.Errorf("SaveAggregate() projection assertion failed: %v", err)
			}

			if err = assertProjection(tt.args.ctx, tt.projAsserts, store, proj); err != nil {
				t.Errorf("SaveAggregate() projection assertion failed: %v", err)
			}
		})
	}
}

func testSaveAggregatesWithProjection(t *testing.T, projType event.ProjectionType, adapter func() persistence.Port, cleanUp func()) {
	cleanUp()
	tenantID := "0000-0000-0000"

	type args struct {
		ctx        context.Context
		aggregates []event.AggregateWithEventSourcingSupport
		eventStore func() (event.EventStore, event.Projection, persistence.Port)
	}
	tests := []struct {
		name                string
		args                args
		wantErr             bool
		wantProjectionError bool
		projAsserts         projectionAsserts
	}{
		{
			name: string(projType) + "---" + "execute projection with one projected event type in empty repro",
			args: args{
				ctx: context.Background(),
				aggregates: []event.AggregateWithEventSourcingSupport{
					newForTestConcreteAggregate("1", "Name", 0, tenantID, []event.IEvent{
						ForTestMakeCreateEvent("1", tenantID, time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
						ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
						ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
					}),
					newForTestConcreteAggregate("2", "Name", 0, tenantID, []event.IEvent{
						ForTestMakeCreateEvent("2", tenantID, time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
						ForTestMakeEvent("2", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
						ForTestMakeEvent("2", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
					}),
				},
				eventStore: func() (event.EventStore, event.Projection, persistence.Port) {
					adp := adapter()
					projectionTypeOne := newTestProjectionTypeOne("projection_1", tenantID, 0, 1)
					store, _, _ := eventstore.New(adp,
						eventstore.WithProjection(projectionTypeOne),
						eventstore.WithProjectionType(projectionTypeOne.ID(), projType),
					)
					evtStore := store

					return evtStore, projectionTypeOne, adp
				},
			},
			wantErr:             false,
			wantProjectionError: false,
			projAsserts: projectionAsserts{
				id:                 shared.NewProjectionID(tenantID, "projection_1"),
				wantPrepCounter:    0,
				wantExecuteCounter: 6,
				wantFinishCounter:  0,
				wantState:          projection.Running,
				wantUpdatedAt:      true,
				wantEventStream: []event.IEvent{
					ForTestMakeCreateEvent("1", tenantID, time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
					ForTestMakeCreateEvent("2", tenantID, time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
					ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
					ForTestMakeEvent("2", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
					ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
					ForTestMakeEvent("2", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC))},
			},
		},
		{
			name: string(projType) + "---" + "execute projection with one projected event type an mixed aggregate events in empty repro",
			args: args{
				ctx: context.Background(),
				aggregates: []event.AggregateWithEventSourcingSupport{
					newForTestConcreteAggregate("1", "Name", 0, tenantID, []event.IEvent{
						ForTestMakeCreateEvent("1", tenantID, time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
					}),
					newForTestConcreteAggregate("2", "Name", 0, tenantID, []event.IEvent{
						ForTestMakeCreateEvent("2", tenantID, time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
						ForTestMakeEvent("2", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
						ForTestMakeEvent("2", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
					}),
					newForTestConcreteAggregate("1", "Name", 1, tenantID, []event.IEvent{
						ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
						ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
					}),
				},
				eventStore: func() (event.EventStore, event.Projection, persistence.Port) {
					adp := adapter()
					projectionTypeOne := newTestProjectionTypeOne("projection_1", tenantID, 0, 1)
					store, _, _ := eventstore.New(adp,
						eventstore.WithProjection(projectionTypeOne),
						eventstore.WithProjectionType(projectionTypeOne.ID(), projType),
					)
					evtStore := store
					return evtStore, projectionTypeOne, adp
				},
			},
			wantErr:             false,
			wantProjectionError: false,
			projAsserts: projectionAsserts{
				id:                 shared.NewProjectionID(tenantID, "projection_1"),
				wantPrepCounter:    0,
				wantExecuteCounter: 6,
				wantFinishCounter:  0,
				wantState:          projection.Running,
				wantUpdatedAt:      true,
				wantEventStream: []event.IEvent{
					ForTestMakeCreateEvent("1", tenantID, time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
					ForTestMakeCreateEvent("2", tenantID, time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
					ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
					ForTestMakeEvent("2", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
					ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
					ForTestMakeEvent("2", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC))},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store, proj, _ := tt.args.eventStore()
			defer cleanUp()

			res, err := event.SaveAggregates(tt.args.ctx, store, tt.args.aggregates...)
			if err != nil != tt.wantErr {
				t.Errorf("SaveAggregates() gotError = %v, wantErr %v", err, tt.wantErr)
			}

			if err = assertProjectionError(res, tt.wantProjectionError); err != nil {
				t.Errorf("SaveAggregates() projection assertion error failed %v", err)
			}

			if err = assertProjection(tt.args.ctx, tt.projAsserts, store, proj); err != nil {
				t.Errorf("SaveAggregates()  projection assertion failed %v", err)
			}
		})
	}

}

func testSaveAggregatesConcurrentWithProjection(t *testing.T, adapter func() persistence.Port, cleanUp func()) {
	cleanUp()
	defer cleanUp()

	ctx := context.Background()
	tenantID := "0000-0000-0000"
	proj1 := newTestProjectionTypeOne("proj_1", tenantID, 0*time.Second, 1)
	proj2 := newTestProjectionTypeTwo("proj_2", tenantID, 0*time.Second, 1)
	proj12 := newTestProjectionTwoTypes("proj_1_2", tenantID, 0*time.Second, 1)

	eventStore, _, _ := eventstore.New(adapter(),
		eventstore.WithConcurrentModificationStrategy("forTestConcreteAggregate", event.Ignore),
		eventstore.WithConcurrentModificationStrategy("forTestConcreteAggregate2", event.Ignore),
		eventstore.WithProjection(proj1),
		eventstore.WithProjectionWorkerQueueLength(proj1.ID(), 5000),
		eventstore.WithProjectionTimeOut(proj1.ID(), 30*time.Second),
		eventstore.WithProjectionWorkerQueueLength(proj2.ID(), 5000),
		eventstore.WithProjectionTimeOut(proj2.ID(), 30*time.Second),
		eventstore.WithProjection(proj2),
		eventstore.WithProjectionTimeOut(proj12.ID(), 30*time.Second),
		eventstore.WithProjection(proj12),
		eventstore.WithProjectionWorkerQueueLength(proj12.ID(), 5000),
	)
	//limited by maxConnections in postgres
	aggregateUpdateCount := 3
	aggregateCreationCount := 5

	eventCount := 0
	wg := sync.WaitGroup{}
	mut := sync.RWMutex{}
	for j := 0; j < aggregateUpdateCount; j++ {
		for i := 0; i < aggregateCreationCount; i++ {
			wg.Add(1)
			id := strconv.Itoa(i)
			var aggregate forTestConcreteAggregate
			if j == 0 {
				aggregate = newForTestConcreteAggregate(id, "Name", 0, tenantID, []event.IEvent{
					ForTestMakeCreateEventNow(id, tenantID),
					ForTestMakeEventNow(id, tenantID),
					ForTestMakeEvent2Now(id, tenantID),
				})

			} else {
				aggregate = newForTestConcreteAggregate(id, "Name", 0, tenantID, []event.IEvent{
					ForTestMakeEventNow(id, tenantID),
					ForTestMakeEventNow(id, tenantID),
					ForTestMakeEvent2Now(id, tenantID),
				})
			}

			eventCount = eventCount + len(aggregate.GetUnsavedChanges())

			//to make sure that all aggregateIDs are inserted before we update then
			if j == 0 {
				func(i int) {
					resCh, err := event.SaveAggregates(ctx, eventStore, aggregate)
					if err != nil {
						panic("error in test preparation " + err.Error())
					}
					err = <-resCh
					if err != nil {
						panic("error in test preparation " + err.Error())
					}
					wg.Done()
				}(i)
			} else {
				go func(i int) {
					mut.Lock()
					resCh, err2 := event.SaveAggregates(ctx, eventStore, aggregate)
					mut.Unlock()

					wg.Done()
					if err2 != nil {
						fmt.Println(err2)
						return
					}
					err2 = <-resCh
					if err2 != nil {
						fmt.Println(err2)
						return
					}
				}(i)
			}
		}
	}
	wg.Wait()
	time.Sleep(1 * time.Second)

	//fmt.Println("-----Event Counter---")
	//fmt.Println(proj1.ID(), ":", proj1.(*forTestProjection).eventCounter.Load())
	//fmt.Println(proj2.ID(), ":", proj2.(*forTestProjection).eventCounter.Load())
	//fmt.Println(proj12.ID(), ":", proj12.(*forTestProjection).eventCounter.Load())
	//fmt.Println("-----Call Counter---")
	//fmt.Println(proj1.ID(), ":", proj1.(*forTestProjection).executeCounter.Load())
	//fmt.Println(proj2.ID(), ":", proj2.(*forTestProjection).executeCounter.Load())
	//fmt.Println(proj12.ID(), ":", proj12.(*forTestProjection).executeCounter.Load())

	if len(proj1.(*forTestProjection).ForTestGetEvents()) != 2*aggregateCreationCount*aggregateUpdateCount {
		events, ids := checkForLostEvents(proj1.(*forTestProjection).ForTestGetEvents(), 2, 2*aggregateCreationCount*aggregateUpdateCount)
		fmt.Println(proj1.ID(), " - lost events:", events, "and missing id:", ids)
		fmt.Println(proj1.ID(), "eventCounter:", proj1.(*forTestProjection).eventCounter.Load(), " array:", len(proj1.(*forTestProjection).ForTestGetEvents()))
		t.Errorf("SaveAggregate Concurrent With Projection() %v: gotEvents=%v, want %v", proj1.ID(), len(proj1.(*forTestProjection).ForTestGetEvents()), 2*aggregateCreationCount*aggregateUpdateCount)
	}

	if len(proj2.(*forTestProjection).ForTestGetEvents()) != 1*aggregateCreationCount*aggregateUpdateCount {
		events, ids := checkForLostEvents(proj2.(*forTestProjection).ForTestGetEvents(), 1, 1*aggregateCreationCount*aggregateUpdateCount)
		fmt.Println(proj2.ID(), " - lost events:", events, "and missing id:", ids)
		fmt.Println(proj2.ID(), "eventCounter:", proj2.(*forTestProjection).eventCounter.Load(), " array:", len(proj2.(*forTestProjection).ForTestGetEvents()))
		t.Errorf("SaveAggregate Concurrent With Projection() %v:  gotCalls = %v, want %v", proj2.ID(), len(proj2.(*forTestProjection).ForTestGetEvents()), 1*aggregateCreationCount*aggregateUpdateCount)
	}

	if len(proj12.(*forTestProjection).ForTestGetEvents()) != 3*aggregateCreationCount*aggregateUpdateCount {
		events, ids := checkForLostEvents(proj12.(*forTestProjection).ForTestGetEvents(), 3, 3*aggregateCreationCount*aggregateUpdateCount)
		fmt.Println(proj12.ID(), " - lost events:", events, "and missing id:", ids)
		fmt.Println(proj12.ID(), "eventCounter:", proj12.(*forTestProjection).eventCounter.Load(), " array:", len(proj12.(*forTestProjection).ForTestGetEvents()))
		t.Errorf("SaveAggregate Concurrent With Projection() %v:  gotCalls = %v, want %v", proj12.ID(), len(proj12.(*forTestProjection).ForTestGetEvents()), 3*aggregateCreationCount*aggregateUpdateCount)
	}

}

func checkForLostEvents(evts []event.IEvent, eventCountPerAggregate int, aggregateCount int) (result map[string][]event.IEvent, missingIds []string) {
	var aggregates = make(map[string]bool)
	for i := 0; i < aggregateCount; i++ {
		aggregates[strconv.Itoa(i)] = false
	}
	//Check completeness of event count
	var projResult = make(map[string][]event.IEvent)
	for _, evt := range evts {
		projResult[evt.GetAggregateID()] = append(projResult[evt.GetAggregateID()], evt)
		aggregates[evt.GetAggregateID()] = true
	}
	result = make(map[string][]event.IEvent)
	for key, events := range projResult {
		if len(events) != eventCountPerAggregate {
			result[key] = events
		}
	}
	for id, value := range aggregates {
		if value == false {
			missingIds = append(missingIds, id)
		}
	}

	return result, missingIds
}

func testSaveAggregatesWithConcurrentProjections(t *testing.T, adapter func() persistence.Port, cleanUp func()) {
	cleanUp()
	tenantID := "0000-0000-0000"

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
		wantErrType   any
	}{
		{
			name: "concurrent access error for consistent projection",
			args: args{
				ctx: context.Background(),
				aggregatesFirst: []event.AggregateWithEventSourcingSupport{
					newForTestConcreteAggregate("1", "Name", 0, tenantID, []event.IEvent{
						ForTestMakeCreateEvent("1", tenantID, time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
					}),
					newForTestConcreteAggregate("1", "Name", 1, tenantID, []event.IEvent{
						ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
					}),
				},
				aggregatesSecond: []event.AggregateWithEventSourcingSupport{
					newForTestConcreteAggregate("2", "Name", 0, tenantID, []event.IEvent{
						ForTestMakeCreateEvent("2", tenantID, time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
					}),
				},

				eventStore: func() event.EventStore {
					projection := newTestProjectionTypeOne("projection_1", tenantID, 10*time.Millisecond, 10)
					store, err, errCh := eventstore.New(adapter(),
						eventstore.WithConcurrentModificationStrategy("forTestConcreteAggregate", event.Fail),
						eventstore.WithProjection(projection),
						eventstore.WithProjectionType(projection.ID(), event.CSS),
						eventstore.WithSaveRetryDurations([]time.Duration{0}),
					)
					if err != nil {
						t.Errorf("test case preparation %v failed:%v", t.Name(), err)
					}
					<-errCh
					return store
				},
			},
			waitTime: 20 * time.Millisecond,
			wantErr:  true,
			wantErrAssert: func(err error) bool {
				var wantErr *event.ErrorConcurrentProjectionAccess
				return errors.As(err, &wantErr)
			},
			wantErrType: event.ErrorConcurrentProjectionAccess{},
		},

		// For eventual consistent projections the error does not happen within a single pod/instance. In this case
		// we use a worker (channel) and all requests are serialized. A simultaneous attempt to execute a projection
		// cannot work, i.e. will be an entry in the channel queue and serialized. Only in the multi-pod scenarios
		// this error will occur for eventual consistent projections.

	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			eventStore := tt.args.eventStore()
			defer cleanUp()

			wg := sync.WaitGroup{}
			wg.Add(1)
			go func() {
				resCh, errIntern := event.SaveAggregates(context.Background(), eventStore, tt.args.aggregatesFirst...)
				if errIntern != nil {
					t.Errorf("SaveAggregates() subroutine error = %v, wantErr %v", errIntern, false)
				}
				err := <-resCh
				if err != nil {
					t.Errorf("error in test case preparation")
				}
				wg.Done()
			}()

			time.Sleep(tt.waitTime)
			_, err := event.SaveAggregates(tt.args.ctx, eventStore, tt.args.aggregatesSecond...)

			wg.Wait()
			if (err != nil) != tt.wantErrAssert(err) {
				t.Errorf("Concurrent SaveAggregates() got error = %v, wantErr %v", err, !tt.wantErr)
			}
		})
	}
}

func testInitAdapterWithExistingProjections(t *testing.T, adapter func() persistence.Port, cleanUp func()) {
	cleanUp()
	tenantID := "0000-0000-0000"
	type args struct {
		ctx        context.Context
		aggregates []event.AggregateWithEventSourcingSupport
		eventStore func() (event.EventStore, event.Projection)
	}
	tests := []struct {
		name            string
		args            args
		waitTime        time.Duration
		wantErr         bool
		callCounter     int32
		wantEventStream []event.IEvent
	}{
		{
			name: "init adapter with existing projections",
			args: args{
				ctx: context.Background(),
				aggregates: []event.AggregateWithEventSourcingSupport{
					newForTestConcreteAggregate("1", "Name", 0, tenantID, []event.IEvent{
						ForTestMakeCreateEvent("1", tenantID, time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
					}),
					newForTestConcreteAggregate("1", "Name", 1, tenantID, []event.IEvent{
						ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
					}),
				},
				eventStore: func() (event.EventStore, event.Projection) {
					projection := newTestProjectionTypeOne("projection_1", tenantID, 10*time.Millisecond, 1)
					store, err, errCh := eventstore.New(adapter(),
						eventstore.WithConcurrentModificationStrategy("forTestConcreteAggregate", event.Fail),
						eventstore.WithProjection(projection),
						eventstore.WithSaveRetryDurations([]time.Duration{0}),
					)
					if err != nil {
						t.Errorf("test case preparation %v failed:%v", t.Name(), err)
					}
					err = <-errCh
					if err != nil {
						t.Errorf("test case preparation %v failed:%v", t.Name(), err)
					}

					return store, projection
				},
			},
			wantErr:     false,
			callCounter: 2,
			wantEventStream: []event.IEvent{
				ForTestMakeCreateEvent("1", tenantID, time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
				ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			eventStore, projection := tt.args.eventStore()
			defer cleanUp()

			//create projections entry in DB
			errCh, err := event.SaveAggregates(tt.args.ctx, eventStore, tt.args.aggregates...)
			if (err != nil) != tt.wantErr {
				t.Errorf("init adapter failed %v failed:%v", t.Name(), err)
			}

			err = <-errCh
			if err != nil {
				t.Errorf("init adapter failed %v failed:%v", t.Name(), err)
			}

			cleanRegistries()
			//second new store(pod) must pick up the projections entry in DB
			_, err, errStartCh := eventstore.New(adapter(),
				eventstore.WithConcurrentModificationStrategy("forTestConcreteAggregate", event.Fail),
				eventstore.WithProjection(projection),
				eventstore.WithSaveRetryDurations([]time.Duration{0}),
			)
			if (err != nil) != tt.wantErr {
				t.Errorf("init adapter failed %v failed:%v", t.Name(), err)
			}

			err = <-errStartCh
			if err != nil {
				t.Errorf("init adapter failed %v failed:%v", t.Name(), err)
			}

			gotEventStream := projection.(*forTestProjection).ForTestGetEvents()
			testdata.AssertEqualStream(t, gotEventStream, tt.wantEventStream)

			gotCalls := projection.(*forTestProjection).executeCounter.Load()
			if gotCalls != tt.callCounter {
				t.Errorf("SaveAggregate With Projection() gotProjectionCalls = %v, want %v", gotCalls, tt.callCounter)
			}

			gotPrepare := projection.(*forTestProjection).prepareCounter.Load()
			if gotPrepare != 0 {
				t.Errorf("SaveAggregate With Projection() gotProjectionPreparationCalls = %v, want %v", gotPrepare, 0)
			}
		})
	}
}

func testInitAdapterWithInitialTenants(t *testing.T, adapter func() persistence.Port, cleanUp func()) {
	cleanUp()
	tenantID := "0000-0000-0000"

	type args struct {
		ctx        context.Context
		aggregates []event.AggregateWithEventSourcingSupport
		eventStore func() (event.EventStore, event.Projection, event.Projection)
	}
	tests := []struct {
		name            string
		args            args
		waitTime        time.Duration
		wantErr         bool
		callCounter     int32
		wantEventStream []event.IEvent
	}{
		{
			name: "init adapter with existing projections",
			args: args{
				ctx: context.Background(),
				aggregates: []event.AggregateWithEventSourcingSupport{
					newForTestConcreteAggregate("1", "Name", 0, tenantID, []event.IEvent{
						ForTestMakeCreateEvent("1", tenantID, time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
					}),
					newForTestConcreteAggregate("1", "Name", 1, tenantID, []event.IEvent{
						ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
					}),
					newForTestConcreteAggregate("2", "Name", 0, tenantID, []event.IEvent{
						ForTestMakeCreateEvent("2", tenantID, time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
					}),
					newForTestConcreteAggregate("2", "Name", 1, tenantID, []event.IEvent{
						ForTestMakeEvent2("2", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
					}),
				},
				eventStore: func() (event.EventStore, event.Projection, event.Projection) {
					projection := newTestProjectionTypeOne("projection_1", tenantID, 10*time.Millisecond, 1)
					projection2 := newTestProjectionTypeTwo("projection_2", tenantID, 10*time.Millisecond, 1)
					store, err, errCh := eventstore.New(adapter(),
						eventstore.WithConcurrentModificationStrategy("forTestConcreteAggregate", event.Fail),
						eventstore.WithProjection(projection),
						eventstore.WithProjection(projection2),
						eventstore.WithSaveRetryDurations([]time.Duration{0}),
					)
					if err != nil {
						t.Errorf("test case preparation %v failed:%v", t.Name(), err)
					}
					err = <-errCh
					if err != nil {
						t.Errorf("test case preparation %v failed:%v", t.Name(), err)
					}

					return store, projection, projection2
				},
			},
			wantErr:     false,
			callCounter: 3,
			wantEventStream: []event.IEvent{
				ForTestMakeCreateEvent("1", tenantID, time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
				ForTestMakeCreateEvent("2", tenantID, time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
				ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			eventStore, projection1, projection2 := tt.args.eventStore()
			defer cleanUp()

			//create projections entry in DB
			errCh, err := event.SaveAggregates(tt.args.ctx, eventStore, tt.args.aggregates...)
			if (err != nil) != tt.wantErr {
				t.Errorf("init adapter failed %v failed:%v", t.Name(), err)
			}

			err = <-errCh
			if err != nil {
				t.Errorf("init adapter failed %v failed:%v", t.Name(), err)
			}

			cleanRegistries()
			//second new store(pod) must pick up the projections entry in DB
			_, err, errStartCh := eventstore.New(adapter(),
				eventstore.WithConcurrentModificationStrategy("forTestConcreteAggregate", event.Fail),
				eventstore.WithProjection(projection1),
				eventstore.WithProjection(projection2),
				eventstore.WithSaveRetryDurations([]time.Duration{0}))

			if (err != nil) != tt.wantErr {
				t.Errorf("init adapter failed %v failed:%v", t.Name(), err)
			}

			err = <-errStartCh
			if err != nil {
				t.Errorf("init adapter failed %v failed:%v", t.Name(), err)
			}

			gotEventStream := projection1.(*forTestProjection).ForTestGetEvents()
			testdata.AssertEqualStream(t, gotEventStream, tt.wantEventStream)

			gotCalls := projection1.(*forTestProjection).executeCounter.Load()
			if gotCalls != tt.callCounter {
				t.Errorf("SaveAggregate With Projection() gotProjectionCalls = %v, want %v", gotCalls, tt.callCounter)
			}

			gotPrepare := projection1.(*forTestProjection).prepareCounter.Load()
			if gotPrepare != 0 {
				t.Errorf("SaveAggregate With Projection() gotProjectionPreparationCalls = %v, want %v", gotPrepare, 0)
			}

			gotCalls = projection2.(*forTestProjection).executeCounter.Load()
			if gotCalls != 1 {
				t.Errorf("SaveAggregate With Projection() gotProjectionCalls = %v, want %v", gotCalls, tt.callCounter)
			}
		})
	}
}

func testInitAdapterWithNewProjection(t *testing.T, adapter func() persistence.Port, cleanUp func()) {
	cleanUp()
	tenantID := "0000-0000-0000"

	type args struct {
		ctx        context.Context
		eventStore func() error
	}
	tests := []struct {
		name     string
		args     args
		waitTime time.Duration
		wantErr  bool
	}{
		{
			name: "init adapter with existing projections and a new projection",
			args: args{
				ctx: context.Background(),
				eventStore: func() error {
					ctx := context.Background()
					projection1 := newTestProjectionTypeOne("projection_1", tenantID, 10*time.Millisecond, 1)
					store, err, errCh := eventstore.New(adapter(),
						eventstore.WithConcurrentModificationStrategy("forTestConcreteAggregate", event.Fail),
						eventstore.WithProjection(projection1),
						eventstore.WithSaveRetryDurations([]time.Duration{0}),
					)
					if err != nil {
						t.Errorf("test case preparation %v failed:%v", t.Name(), err)
					}
					err = <-errCh
					if err != nil {
						t.Errorf("test case preparation %v failed:%v", t.Name(), err)
					}

					agg := newForTestConcreteAggregate("1", "Name", 0, tenantID, []event.IEvent{
						ForTestMakeCreateEvent("1", tenantID, time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
						ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
						ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
					})

					errCh, err = event.SaveAggregates(ctx, store, agg)
					err = <-errCh
					if err != nil {
						t.Errorf("test case preparation %v failed:%v", t.Name(), err)
					}

					return nil
				},
			},
			wantErr: false,
		},
		{
			name: "init adapter with existing projections and a new projection for different tenant",
			args: args{
				ctx: context.Background(),
				eventStore: func() error {
					ctx := context.Background()
					projection1 := newTestProjectionTypeOne("projection_1", "0000-0000-0001", 0, 1)
					store, err, errCh := eventstore.New(adapter(),
						eventstore.WithConcurrentModificationStrategy("forTestConcreteAggregate", event.Fail),
						eventstore.WithProjection(projection1),
						eventstore.WithSaveRetryDurations([]time.Duration{0}),
					)
					if err != nil {
						t.Errorf("test case preparation %v failed:%v", t.Name(), err)
					}
					err = <-errCh
					if err != nil {
						t.Errorf("test case preparation %v failed:%v", t.Name(), err)
					}

					agg := newForTestConcreteAggregate("1", "Name", 0, "0000-0000-0001", []event.IEvent{
						ForTestMakeCreateEvent("1", "0000-0000-0001", time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
						ForTestMakeEvent("1", "0000-0000-0001", time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
						ForTestMakeEvent("1", "0000-0000-0001", time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
					})

					errCh, err = event.SaveAggregates(ctx, store, agg)
					if err != nil {
						t.Errorf("test case preparation %v failed:%v", t.Name(), err)
					}
					err = <-errCh
					if err != nil {
						t.Errorf("test case preparation %v failed:%v", t.Name(), err)
					}

					return nil
				},
			},
			wantErr: false,
		},
		{
			name: "init adapter with new projection",
			args: args{
				ctx: context.Background(),
				eventStore: func() error {
					ctx := context.Background()
					store, err, errCh := eventstore.New(adapter(),
						eventstore.WithConcurrentModificationStrategy("forTestConcreteAggregate", event.Fail),
						eventstore.WithSaveRetryDurations([]time.Duration{0}),
					)
					if err != nil {
						t.Errorf("test case preparation %v failed:%v", t.Name(), err)
					}
					err = <-errCh
					if err != nil {
						t.Errorf("test case preparation %v failed:%v", t.Name(), err)
					}

					agg := newForTestConcreteAggregate("1", "Name", 0, tenantID, []event.IEvent{
						ForTestMakeCreateEvent("1", tenantID, time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
						ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
						ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
					})

					errCh, err = event.SaveAggregates(ctx, store, agg)
					err = <-errCh
					if err != nil {
						t.Errorf("test case preparation %v failed:%v", t.Name(), err)
					}

					return nil
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.args.eventStore(); err != nil {
				t.Errorf("init test case failed %v failed:%v", t.Name(), err)
			}

			projection1 := newTestProjectionTypeOne("projection_1", tenantID, 10*time.Millisecond, 1)
			projection2 := newTestProjectionTypeOne("projection_2", tenantID, 10*time.Millisecond, 1)

			store, err, errCh := eventstore.New(adapter(),
				eventstore.WithConcurrentModificationStrategy("forTestConcreteAggregate", event.Fail),
				eventstore.WithProjection(projection1),
				eventstore.WithProjection(projection2),
				eventstore.WithSaveRetryDurations([]time.Duration{0}),
			)
			if err != nil {
				t.Errorf("test case preparation %v failed:%v", t.Name(), err)
			}
			if err = assertProjectionError(errCh, false); err != nil {
				t.Errorf("test case preparation %v failed:%v", t.Name(), err)
			}

			agg := newForTestConcreteAggregate("2", "Name", 0, tenantID, []event.IEvent{
				ForTestMakeCreateEvent("2", tenantID, time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
				ForTestMakeEvent("2", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
				ForTestMakeEvent("2", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
			})

			errCh, err = event.SaveAggregates(tt.args.ctx, store, agg)
			if err != nil {
				t.Errorf("init adapter failed %v failed:%v", t.Name(), err)
				return
			}

			if err = assertProjectionError(errCh, false); err != nil {
				t.Errorf("init adapter failed %v failed:%v", t.Name(), err)
			}

			cleanUp()
		})
	}
}

func testExecuteAllExistingProjections(t *testing.T, adapter func() persistence.Port, cleanUp func()) {
	cleanUp()
	tenantID := uuid.New().String()

	waitingTime := 50 * time.Millisecond

	type args struct {
		ctx         context.Context
		aggregates  func(savepoint time.Time) []event.AggregateWithEventSourcingSupport
		eventStore  func() (event.EventStore, event.Projection)
		projections []string
	}
	tests := []struct {
		name            string
		args            args
		wantErr         bool
		callCounter     int32
		wantEventStream func(savePoint time.Time) []event.IEvent
	}{
		{
			name: "execute all existing projections without projection selection",
			args: args{
				ctx: context.Background(),
				aggregates: func(savePoint time.Time) []event.AggregateWithEventSourcingSupport {
					return []event.AggregateWithEventSourcingSupport{
						newForTestConcreteAggregate("1", "Name", 0, tenantID, []event.IEvent{
							ForTestMakeCreateEvent("1", tenantID, savePoint, savePoint),
						}),
						newForTestConcreteAggregate("1", "Name", 1, tenantID, []event.IEvent{
							ForTestMakeEvent("1", tenantID, savePoint.Add(waitingTime+1*time.Nanosecond), savePoint.Add(waitingTime+1*time.Nanosecond)),
						}),
					}
				},
				eventStore: func() (event.EventStore, event.Projection) {
					proj := newTestProjectionTypeOne("projection_1", tenantID, 0*time.Millisecond, 1)
					store, err, errCh := eventstore.New(adapter(),
						eventstore.WithConcurrentModificationStrategy("forTestConcreteAggregate", event.Fail),
						eventstore.WithProjection(proj),
						eventstore.WithSaveRetryDurations([]time.Duration{0}),
					)
					if err != nil {
						t.Errorf("test case preparation %v failed:%v", t.Name(), err)
					}
					err = <-errCh
					if err != nil {
						t.Errorf("test case preparation %v failed:%v", t.Name(), err)
					}

					return store, proj
				},
				projections: []string{}, //executes all projections
			},
			wantErr:     false,
			callCounter: 1,
			wantEventStream: func(savePoint time.Time) []event.IEvent {
				return []event.IEvent{
					ForTestMakeEvent("1", tenantID, savePoint.Add(waitingTime+1*time.Nanosecond), savePoint.Add(waitingTime+1*time.Nanosecond)),
				}
			},
		},
		{
			name: "execute all existing projections with projection selection",
			args: args{
				ctx: context.Background(),
				aggregates: func(savePoint time.Time) []event.AggregateWithEventSourcingSupport {
					return []event.AggregateWithEventSourcingSupport{
						newForTestConcreteAggregate("1", "Name", 0, tenantID, []event.IEvent{
							ForTestMakeCreateEvent("1", tenantID, savePoint, savePoint),
						}),
						newForTestConcreteAggregate("1", "Name", 1, tenantID, []event.IEvent{
							ForTestMakeEvent("1", tenantID, savePoint.Add(waitingTime+1*time.Nanosecond), savePoint.Add(waitingTime+1*time.Nanosecond)),
						}),
					}
				},
				eventStore: func() (event.EventStore, event.Projection) {
					proj := newTestProjectionTypeOne("projection_1", tenantID, 0*time.Millisecond, 1)
					store, err, errCh := eventstore.New(adapter(),
						eventstore.WithConcurrentModificationStrategy("forTestConcreteAggregate", event.Fail),
						eventstore.WithProjection(proj),
						eventstore.WithSaveRetryDurations([]time.Duration{0}),
					)
					if err != nil {
						t.Errorf("test case preparation %v failed:%v", t.Name(), err)
					}
					err = <-errCh
					if err != nil {
						t.Errorf("test case preparation %v failed:%v", t.Name(), err)
					}

					return store, proj
				},
				projections: []string{"projection_1"}, //executes all projections
			},
			wantErr:     false,
			callCounter: 1,
			wantEventStream: func(savePoint time.Time) []event.IEvent {
				return []event.IEvent{
					ForTestMakeEvent("1", tenantID, savePoint.Add(waitingTime+1*time.Nanosecond), savePoint.Add(waitingTime+1*time.Nanosecond)),
				}
			},
		},
		{
			name: "execute all existing projections with projection selection (negativ-case)",
			args: args{
				ctx: context.Background(),
				aggregates: func(savePoint time.Time) []event.AggregateWithEventSourcingSupport {
					return []event.AggregateWithEventSourcingSupport{
						newForTestConcreteAggregate("1", "Name", 0, tenantID, []event.IEvent{
							ForTestMakeCreateEvent("1", tenantID, savePoint, savePoint),
						}),
						newForTestConcreteAggregate("1", "Name", 1, tenantID, []event.IEvent{
							ForTestMakeEvent("1", tenantID, savePoint.Add(waitingTime+1*time.Nanosecond), savePoint.Add(waitingTime+1*time.Nanosecond)),
						}),
					}
				},
				eventStore: func() (event.EventStore, event.Projection) {
					proj := newTestProjectionTypeOne("projection_1", tenantID, 0*time.Millisecond, 1)
					store, err, errCh := eventstore.New(adapter(),
						eventstore.WithConcurrentModificationStrategy("forTestConcreteAggregate", event.Fail),
						eventstore.WithProjection(proj),
						eventstore.WithSaveRetryDurations([]time.Duration{0}),
					)
					if err != nil {
						t.Errorf("test case preparation %v failed:%v", t.Name(), err)
					}
					err = <-errCh
					if err != nil {
						t.Errorf("test case preparation %v failed:%v", t.Name(), err)
					}

					return store, proj
				},
				projections: []string{"projection_2"}, //executes all projections
			},
			wantErr:     false,
			callCounter: 0,
			wantEventStream: func(savePoint time.Time) []event.IEvent {
				return []event.IEvent{}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			eventStore, proj := tt.args.eventStore()
			defer cleanUp()

			savePoint := time.Now().UTC()
			//create projections entry in DB
			errCh, err := event.SaveAggregates(tt.args.ctx, eventStore, tt.args.aggregates(savePoint)...)
			if (err != nil) != tt.wantErr {
				t.Errorf("init adapter failed %v failed:%v", t.Name(), err)
			}

			err = <-errCh
			if err != nil {
				t.Errorf("init adapter failed %v failed:%v", t.Name(), err)
			}

			// reset projections execution due to save aggregates
			proj.(*forTestProjection).ForTestResetAll()

			//wait until future patches are valid
			time.Sleep(waitingTime)
			err = eventStore.ExecuteAllProjections(tt.args.ctx, tt.args.projections...)
			if (err != nil) != tt.wantErr {
				t.Errorf("init adapter failed %v failed:%v", t.Name(), err)
			}

			gotEventStream := proj.(*forTestProjection).ForTestGetEvents()
			testdata.AssertEqualStream(t, gotEventStream, tt.wantEventStream(savePoint))

			gotCalls := proj.(*forTestProjection).executeCounter.Load()
			if gotCalls != tt.callCounter {
				t.Errorf("SaveAggregate With Projection() gotProjectionCalls = %v, want %v", gotCalls, tt.callCounter)
			}

		})
	}
}

func testRemoveProjection(t *testing.T, adapter func() persistence.Port, cleanUp func()) {
	cleanUp()
	tenantID := "0000-0000-0000"
	tenantID2 := "0000-0000-0001"

	projectionID1 := "projection_1"
	projectionID2 := "projection_2"
	timePoint := time.Now().UTC()

	//prepare initial store
	initStore := func() {
		proj11 := newTestProjectionTypeOne(projectionID1, tenantID, 0*time.Millisecond, 1)
		proj21 := newTestProjectionTypeOne(projectionID2, tenantID, 0*time.Millisecond, 1)

		store, err, _ := eventstore.New(adapter(),
			eventstore.WithConcurrentModificationStrategy("forTestConcreteAggregate", event.Ignore),
			eventstore.WithProjection(proj11),
			eventstore.WithProjection(proj21),
			eventstore.WithSaveRetryDurations([]time.Duration{0}),
		)
		if err != nil {
			t.Errorf("test case preparation %v failed:%v", t.Name(), err)
		}

		err = store.StopProjection(context.Background(), tenantID, projectionID1) //to keep events in the queue
		assert.NoError(t, err)
		err = store.StopProjection(context.Background(), tenantID, projectionID2) //to keep events in the queue
		assert.NoError(t, err)

		events := []event.AggregateWithEventSourcingSupport{
			newForTestConcreteAggregate("1", "Name", 0, tenantID, []event.IEvent{
				ForTestMakeCreateEvent("1", tenantID, timePoint, timePoint.Add(-10*time.Nanosecond)),
			}),
			newForTestConcreteAggregate("1", "Name", 1, tenantID, []event.IEvent{
				ForTestMakeEvent("1", tenantID, timePoint, timePoint.Add(-2*time.Nanosecond)),
			}),
		}

		_, err = event.SaveAggregates(context.Background(), store, events...)
		assert.NoError(t, err)

		events2 := []event.AggregateWithEventSourcingSupport{
			newForTestConcreteAggregate("1", "Name", 0, tenantID2, []event.IEvent{
				ForTestMakeCreateEvent("1", tenantID2, timePoint, timePoint.Add(-10*time.Nanosecond)),
			}),
			newForTestConcreteAggregate("1", "Name", 1, tenantID2, []event.IEvent{
				ForTestMakeEvent("1", tenantID2, timePoint, timePoint.Add(-2*time.Nanosecond)),
			}),
		}
		_, err = event.SaveAggregates(context.Background(), store, events2...)
		assert.NoError(t, err)

	}

	type args struct {
		ctx        context.Context
		eventStore func() event.EventStore
	}
	tests := []struct {
		name                    string
		args                    args
		removeProjectionID      string
		wantRemainingProjection []string
		wantErr                 assert.ErrorAssertionFunc
	}{
		{
			name: "remove non-registered projections (happy path)",
			args: args{
				ctx: context.Background(),
				eventStore: func() event.EventStore {
					proj := newTestProjectionTypeOne(projectionID2, tenantID, 0*time.Millisecond, 1)
					store, err, errCh := eventstore.New(adapter(),
						eventstore.WithConcurrentModificationStrategy("forTestConcreteAggregate", event.Ignore),
						eventstore.WithProjection(proj),
						eventstore.WithSaveRetryDurations([]time.Duration{0}),
					)
					if err != nil {
						t.Errorf("test case preparation %v failed:%v", t.Name(), err)
					}

					<-errCh //wait till init is done

					return store
				},
			},
			removeProjectionID:      projectionID1,
			wantRemainingProjection: []string{projectionID2},
			wantErr:                 assert.NoError,
		},
		{
			name: "remove registered projections (error case)",
			args: args{
				ctx: context.Background(),
				eventStore: func() event.EventStore {
					proj1 := newTestProjectionTypeOne(projectionID1, tenantID, 0*time.Millisecond, 1)
					proj2 := newTestProjectionTypeOne(projectionID2, tenantID, 0*time.Millisecond, 1)

					store, err, errCh := eventstore.New(adapter(),
						eventstore.WithConcurrentModificationStrategy("forTestConcreteAggregate", event.Ignore),
						eventstore.WithProjection(proj1),
						eventstore.WithProjection(proj2),
						eventstore.WithSaveRetryDurations([]time.Duration{0}),
					)
					if err != nil {
						t.Errorf("test case preparation %v failed:%v", t.Name(), err)
					}

					<-errCh //wait till init is done

					return store
				},
			},
			removeProjectionID:      projectionID1,
			wantRemainingProjection: []string{projectionID1, projectionID2},
			wantErr:                 assert.Error,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			initStore()
			eventStore := tt.args.eventStore()
			defer cleanUp()

			err := eventStore.RemoveProjection(tt.args.ctx, tt.removeProjectionID)
			tt.wantErr(t, err)

			states, err := eventStore.GetAllProjectionStates(tt.args.ctx, tenantID)
			assert.NoError(t, err)
			assertProjectionIDs(t, states, tt.wantRemainingProjection)
		})
	}
}
