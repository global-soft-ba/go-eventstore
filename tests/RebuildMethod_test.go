package tests

import (
	"context"
	"github.com/global-soft-ba/go-eventstore"
	"github.com/global-soft-ba/go-eventstore/eventstore"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/domain/model/projection"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/port/persistence"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/shared"
	"testing"
	"time"
)

func testRebuildProjection(t *testing.T, adapter func() persistence.Port, cleanUp func()) {
	cleanUp()
	tenantID := "0000-0000-0000"
	type args struct {
		ctx                      context.Context
		aggregates               []event.AggregateWithEventSourcingSupport
		projection               event.Projection
		eventStore               func() event.EventStore
		resetProjectionAfterSave func(proj event.Projection)
	}
	tests := []struct {
		name           string
		args           args
		wantRebuildErr bool
		projAsserts    projectionAsserts
	}{
		{
			name: "rebuild projection in with single aggregate and without snapshot",
			args: args{
				ctx: context.Background(),
				aggregates: []event.AggregateWithEventSourcingSupport{
					newForTestConcreteAggregate("1", "Name", 0, tenantID, []event.IEvent{
						ForTestMakeCreateEvent("1", tenantID, time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
						ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
						ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
						ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC)),
						ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 5, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 5, time.UTC)),
					}),
				},
				projection:               newTestProjectionTypeOne("projection_1", tenantID, 0*time.Second, 1),
				resetProjectionAfterSave: func(proj event.Projection) { proj.(*forTestProjection).ForTestResetAll() },
			},
			wantRebuildErr: false,
			projAsserts: projectionAsserts{
				id:                 shared.NewProjectionID(tenantID, "projection_1"),
				wantPrepCounter:    1,
				wantExecuteCounter: 5,
				wantFinishCounter:  1,
				wantState:          projection.Running,
				wantUpdatedAt:      true,
				wantEventStream: []event.IEvent{
					ForTestMakeCreateEvent("1", tenantID, time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
					ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
					ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
					ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC)),
					ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 5, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 5, time.UTC)),
				},
			},
		},
		{
			name: "rebuild projection in with multiple aggregate and without snapshot",
			args: args{
				ctx: context.Background(),
				aggregates: []event.AggregateWithEventSourcingSupport{
					newForTestConcreteAggregate("1", "Name", 0, tenantID, []event.IEvent{
						ForTestMakeCreateEvent("1", tenantID, time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
						ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
						ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
						ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC)),
						ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 5, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 5, time.UTC)),
					}),
					newForTestConcreteAggregate("2", "Name", 0, tenantID, []event.IEvent{
						ForTestMakeCreateEvent("2", tenantID, time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
						ForTestMakeEvent("2", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC)),
					}),
				},
				projection:               newTestProjectionTypeOne("projection_1", tenantID, 0*time.Second, 100),
				resetProjectionAfterSave: func(proj event.Projection) { proj.(*forTestProjection).ForTestResetAll() },
			},
			wantRebuildErr: false,
			projAsserts: projectionAsserts{
				id:                 shared.NewProjectionID(tenantID, "projection_1"),
				wantPrepCounter:    1,
				wantExecuteCounter: 1,
				wantFinishCounter:  1,
				wantState:          projection.Running,
				wantUpdatedAt:      true,
				wantEventStream: []event.IEvent{ //InitStream is a snapshot -> that's why the stream starts with both creation events aka snapshots
					ForTestMakeCreateEvent("1", tenantID, time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
					ForTestMakeCreateEvent("2", tenantID, time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
					ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
					ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
					ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC)),
					ForTestMakeEvent("2", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC)),
					ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 5, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 5, time.UTC))},
			},
		},
		{
			name: "rebuild projection in with single event type and intermediate snapshot",
			args: args{
				ctx: context.Background(),
				aggregates: []event.AggregateWithEventSourcingSupport{
					newForTestConcreteAggregate("1", "Name", 0, tenantID, []event.IEvent{
						ForTestMakeCreateEvent("1", tenantID, time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
						ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
						ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
						ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC)),
						ForTestMakeSnapshot("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 4, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 4, time.UTC)),
						ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 5, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 5, time.UTC)),
					}),
					newForTestConcreteAggregate("2", "Name", 0, tenantID, []event.IEvent{
						ForTestMakeCreateEvent("2", tenantID, time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
						ForTestMakeSnapshot("2", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
						ForTestMakeEvent("2", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC)),
					}),
				},
				projection:               newTestProjectionTypeOne("projection_1", tenantID, 0*time.Second, 2),
				resetProjectionAfterSave: func(proj event.Projection) { proj.(*forTestProjection).ForTestResetAll() },
			},
			wantRebuildErr: false,
			projAsserts: projectionAsserts{
				id:                 shared.NewProjectionID(tenantID, "projection_1"),
				wantPrepCounter:    1,
				wantExecuteCounter: 2,
				wantFinishCounter:  1,
				wantState:          projection.Running,
				wantUpdatedAt:      true,
				wantEventStream: []event.IEvent{
					ForTestMakeSnapshot("2", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
					ForTestMakeEvent("2", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC)),
					ForTestMakeSnapshot("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 4, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 4, time.UTC)),
					ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 5, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 5, time.UTC))},
			},
		},
		{
			name: "rebuild projection with different event type",
			args: args{
				ctx: context.Background(),
				aggregates: []event.AggregateWithEventSourcingSupport{
					newForTestConcreteAggregate("1", "Name", 0, tenantID, []event.IEvent{
						ForTestMakeCreateEvent("1", tenantID, time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
						ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
						ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
						ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC)),
						ForTestMakeSnapshot("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 4, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 4, time.UTC)),
						ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 5, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 5, time.UTC)),
					}),
					newForTestConcreteAggregate("2", "Name", 0, tenantID, []event.IEvent{
						ForTestMakeCreateEvent2("2", tenantID, time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
						ForTestMakeSnapshot2("2", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
						ForTestMakeEvent2("2", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC)),
					}),
				},
				projection:               newTestProjectionTypeOne("projection_1", tenantID, 0*time.Second, 1),
				resetProjectionAfterSave: func(proj event.Projection) { proj.(*forTestProjection).ForTestResetAll() },
			},
			wantRebuildErr: false,
			projAsserts: projectionAsserts{
				id:                 shared.NewProjectionID(tenantID, "projection_1"),
				wantPrepCounter:    1,
				wantExecuteCounter: 2,
				wantFinishCounter:  1,
				wantState:          projection.Running,
				wantUpdatedAt:      true,
				wantEventStream: []event.IEvent{
					ForTestMakeSnapshot("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 4, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 4, time.UTC)),
					ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 5, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 5, time.UTC))},
			},
		},
		{
			name: "rebuild projection in with single aggregate and timeout in preparation",
			args: args{
				ctx: context.Background(),
				aggregates: []event.AggregateWithEventSourcingSupport{
					newForTestConcreteAggregate("1", "Name", 0, tenantID, []event.IEvent{
						ForTestMakeCreateEvent("1", tenantID, time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
						ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
						ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
						ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC)),
						ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 5, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 5, time.UTC)),
					}),
				},
				projection:               newTestProjectionTypeOneWithDelays("projection_1", tenantID, 100*time.Millisecond, 0, 0, 1, 1),
				resetProjectionAfterSave: func(proj event.Projection) { proj.(*forTestProjection).ForTestReset([]event.IEvent{}, 0, 0, 0) }, //because of initial save we have to reset the counters
			},
			wantRebuildErr: true,
			projAsserts: projectionAsserts{
				id:                 shared.NewProjectionID(tenantID, "projection_1"),
				wantPrepCounter:    1,
				wantExecuteCounter: 0,
				wantFinishCounter:  0,
				wantState:          projection.Erroneous,
				wantUpdatedAt:      true,
				wantEventStream:    []event.IEvent{},
			},
		},
		{
			name: "rebuild projection in with single aggregate and timeout in execution",
			args: args{
				ctx: context.Background(),
				aggregates: []event.AggregateWithEventSourcingSupport{
					newForTestConcreteAggregate("1", "Name", 0, tenantID, []event.IEvent{
						ForTestMakeCreateEvent("1", tenantID, time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
						ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
						ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
						ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC)),
						ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 5, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 5, time.UTC)),
					}),
				},
				projection: newTestProjectionTypeOneWithDelays("projection_1", tenantID, 0, 100*time.Millisecond, 0, 6, 1),
				//we must avoid a timeout in the initial save,that's why we reset back to 4 that the first rebuild projection execution calls will fail (see timeOutExecutionCount = 5)
				resetProjectionAfterSave: func(proj event.Projection) { proj.(*forTestProjection).ForTestReset([]event.IEvent{}, 0, 5, 0) },
			},
			wantRebuildErr: true,
			projAsserts: projectionAsserts{
				id:                 shared.NewProjectionID(tenantID, "projection_1"),
				wantPrepCounter:    1,
				wantExecuteCounter: 6,
				wantFinishCounter:  0,
				wantState:          projection.Erroneous,
				wantUpdatedAt:      true,
				wantEventStream:    []event.IEvent{},
			},
		},
		{
			name: "rebuild projection in with single aggregate and timeout in finishing",
			args: args{
				ctx: context.Background(),
				aggregates: []event.AggregateWithEventSourcingSupport{
					newForTestConcreteAggregate("1", "Name", 0, tenantID, []event.IEvent{
						ForTestMakeCreateEvent("1", tenantID, time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
						ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
						ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
						ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC)),
						ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 5, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 5, time.UTC)),
					}),
				},
				projection:               newTestProjectionTypeOneWithDelays("projection_1", tenantID, 0, 0, 100*time.Millisecond, 1, 1),
				resetProjectionAfterSave: func(proj event.Projection) { proj.(*forTestProjection).ForTestReset([]event.IEvent{}, 0, 0, 0) },
			},
			wantRebuildErr: true,
			projAsserts: projectionAsserts{
				id:                 shared.NewProjectionID(tenantID, "projection_1"),
				wantPrepCounter:    1,
				wantExecuteCounter: 5,
				wantFinishCounter:  1,
				wantState:          projection.Erroneous,
				wantUpdatedAt:      true,
				wantEventStream: []event.IEvent{
					//these events were received by the projection - but failed to finished
					ForTestMakeCreateEvent("1", tenantID, time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
					ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
					ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
					ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC)),
					ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 5, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 5, time.UTC))},
			},
		},
		{
			name: "rebuild projection with error in execution",
			args: args{
				ctx: context.Background(),
				aggregates: []event.AggregateWithEventSourcingSupport{
					newForTestConcreteAggregate("1", "Name", 0, tenantID, []event.IEvent{
						ForTestMakeCreateEvent("1", tenantID, time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
						ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
						ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
						ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC)),
						ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 5, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 5, time.UTC)),
					}),
				},
				//we must avoid an execution failure in the initial save,that's why we reset to 5 that the first rebuild projection execution calls will fail (see failExecuteCounter = 6)
				projection:               newTestProjectionTypeOneWithExecuteFail("projection_1", tenantID, 1, true, 6),
				resetProjectionAfterSave: func(proj event.Projection) { proj.(*forTestProjection).ForTestReset([]event.IEvent{}, 0, 5, 0) },
			},
			wantRebuildErr: true,
			projAsserts: projectionAsserts{
				id:                 shared.NewProjectionID(tenantID, "projection_1"),
				wantPrepCounter:    1,
				wantExecuteCounter: 6,
				wantFinishCounter:  0,
				wantState:          projection.Erroneous,
				wantUpdatedAt:      true,
				wantEventStream:    []event.IEvent{},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			adp := adapter()
			eventStore, err, _ := eventstore.New(adp,
				eventstore.WithProjection(tt.args.projection),
				eventstore.WithProjectionTimeOut(tt.args.projection.ID(), 50*time.Millisecond),
				eventstore.WithRebuildTimeOut(tt.args.projection.ID(), 50*time.Millisecond),
				eventstore.WithPreparationTimeOut(tt.args.projection.ID(), 50*time.Millisecond),
				eventstore.WithFinishTimeOut(tt.args.projection.ID(), 50*time.Millisecond),
			)
			defer cleanUp()
			if err != nil {
				t.Errorf("RebuildProjection() error = %v, wantErr %v", err, nil)
			}
			resSaveCh, err := event.SaveAggregates(tt.args.ctx, eventStore, tt.args.aggregates...)
			if err != nil {
				t.Errorf("RebuildProjection() error = %v, wantErr %v", err, nil)
			}

			if err = assertProjectionError(resSaveCh, false); err != nil {
				t.Errorf("RebuildProjection() projection assertion error failed %v", err)
			}

			//reset projection data after saving aggregates
			tt.args.resetProjectionAfterSave(tt.args.projection)

			resChan := eventStore.RebuildProjection(tt.args.ctx, tenantID, tt.args.projection.ID())
			if err = assertProjectionError(resChan, tt.wantRebuildErr); err != nil {
				t.Errorf("RebuildProjection() error = %v, wantErr %v", err, tt.wantRebuildErr)
			}

			if err = assertProjection(tt.args.ctx, tt.projAsserts, eventStore, tt.args.projection); err != nil {
				t.Errorf("RebuildProjection() projection assertion failed: %v", err)
			}
		})
	}

}

func testRebuildProjectionSince(t *testing.T, adapter func() persistence.Port, cleanUp func()) {
	cleanUp()
	tenantID := "0000-0000-0000"
	type args struct {
		ctx        context.Context
		aggregates []event.AggregateWithEventSourcingSupport
		projection event.Projection
		since      time.Time
		eventStore func() event.EventStore
	}
	tests := []struct {
		name        string
		args        args
		wantErr     bool
		projAsserts projectionAsserts
	}{
		{
			name: "rebuild projection in with single aggregate and without snapshot",
			args: args{
				ctx: context.Background(),
				aggregates: []event.AggregateWithEventSourcingSupport{
					newForTestConcreteAggregate("1", "Name", 0, tenantID, []event.IEvent{
						ForTestMakeCreateEvent("1", tenantID, time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
						ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
						ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
						ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC)),
						ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 5, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 5, time.UTC)),
					}),
				},
				since:      time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC),
				projection: newTestProjectionTypeOne("projection_1", tenantID, 0*time.Second, 1),
			},
			wantErr: false,
			projAsserts: projectionAsserts{
				id:                 shared.NewProjectionID(tenantID, "projection_1"),
				wantPrepCounter:    1,
				wantExecuteCounter: 3,
				wantFinishCounter:  1,
				wantState:          projection.Running,
				wantUpdatedAt:      true,
				wantEventStream: []event.IEvent{
					ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
					ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC)),
					ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 5, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 5, time.UTC))},
			},
		},
		{
			name: "rebuild projection in with multiple aggregate and without snapshot",
			args: args{
				ctx: context.Background(),
				aggregates: []event.AggregateWithEventSourcingSupport{
					newForTestConcreteAggregate("1", "Name", 0, tenantID, []event.IEvent{
						ForTestMakeCreateEvent("1", tenantID, time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
						ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
						ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
						ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC)),
						ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 5, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 5, time.UTC)),
					}),
					newForTestConcreteAggregate("2", "Name", 0, tenantID, []event.IEvent{
						ForTestMakeCreateEvent("2", tenantID, time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
						ForTestMakeEvent("2", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC)),
					}),
				},
				since:      time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC),
				projection: newTestProjectionTypeOne("projection_1", tenantID, 0*time.Second, 100),
			},
			wantErr: false,
			projAsserts: projectionAsserts{
				id:                 shared.NewProjectionID(tenantID, "projection_1"),
				wantPrepCounter:    1,
				wantExecuteCounter: 1,
				wantFinishCounter:  1,
				wantState:          projection.Running,
				wantUpdatedAt:      true,
				wantEventStream: []event.IEvent{
					ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
					ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC)),
					ForTestMakeEvent("2", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC)),
					ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 5, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 5, time.UTC))},
			},
		},
		{
			name: "rebuild projection in with single event type and intermediate snapshot",
			args: args{
				ctx: context.Background(),
				aggregates: []event.AggregateWithEventSourcingSupport{
					newForTestConcreteAggregate("1", "Name", 0, tenantID, []event.IEvent{
						ForTestMakeCreateEvent("1", tenantID, time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
						ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
						ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
						ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC)),
						ForTestMakeSnapshot("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 4, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 4, time.UTC)),
						ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 5, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 5, time.UTC)),
					}),
					newForTestConcreteAggregate("2", "Name", 0, tenantID, []event.IEvent{
						ForTestMakeCreateEvent("2", tenantID, time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
						ForTestMakeSnapshot("2", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
						ForTestMakeEvent("2", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC)),
					}),
				},
				since:      time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC),
				projection: newTestProjectionTypeOne("projection_1", tenantID, 0*time.Second, 2),
			},
			wantErr: false,
			projAsserts: projectionAsserts{
				id:                 shared.NewProjectionID(tenantID, "projection_1"),
				wantPrepCounter:    1,
				wantExecuteCounter: 2,
				wantFinishCounter:  1,
				wantState:          projection.Running,
				wantUpdatedAt:      true,
				wantEventStream: []event.IEvent{
					ForTestMakeEvent("2", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC)),
					ForTestMakeSnapshot("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 4, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 4, time.UTC)),
					ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 5, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 5, time.UTC))},
			},
		},
		{
			name: "rebuild projection with different event type",
			args: args{
				ctx: context.Background(),
				aggregates: []event.AggregateWithEventSourcingSupport{
					newForTestConcreteAggregate("1", "Name", 0, tenantID, []event.IEvent{
						ForTestMakeCreateEvent("1", tenantID, time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
						ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
						ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
						ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC)),
						ForTestMakeSnapshot("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 4, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 4, time.UTC)),
						ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 5, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 5, time.UTC)),
					}),
					newForTestConcreteAggregate("2", "Name", 0, tenantID, []event.IEvent{
						ForTestMakeCreateEvent2("2", tenantID, time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
						ForTestMakeSnapshot2("2", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
						ForTestMakeEvent2("2", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC)),
					}),
				},
				since:      time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC),
				projection: newTestProjectionTypeOne("projection_1", tenantID, 0*time.Second, 1),
			},
			wantErr: false,
			projAsserts: projectionAsserts{
				id:                 shared.NewProjectionID(tenantID, "projection_1"),
				wantPrepCounter:    1,
				wantExecuteCounter: 2,
				wantFinishCounter:  1,
				wantState:          projection.Running,
				wantUpdatedAt:      true,
				wantEventStream: []event.IEvent{
					ForTestMakeSnapshot("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 4, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 4, time.UTC)),
					ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 5, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 5, time.UTC))},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer cleanUp()
			adp := adapter()
			eventStore, err, _ := eventstore.New(adp,
				eventstore.WithProjection(tt.args.projection),
			)
			if err != nil {
				t.Errorf("RebuildProjection() error = %v, wantErr %v", err, nil)
			}
			resSaveCh, err := event.SaveAggregates(tt.args.ctx, eventStore, tt.args.aggregates...)
			if err != nil {
				t.Errorf("RebuildProjectionSince() save error = %v, wantErr %v", err, nil)
			}

			if err = assertProjectionError(resSaveCh, false); err != nil {
				t.Errorf("RebuildProjectionSince() projection error = %v, wantErr %v", err, nil)
			}

			tt.args.projection.(*forTestProjection).ForTestResetAll()
			res := eventStore.RebuildProjectionSince(tt.args.ctx, tenantID, tt.args.projection.ID(), tt.args.since)
			if err = assertProjectionError(res, tt.wantErr); err != nil {
				t.Errorf("RebuildProjectionSince() error = %v, wantErr %v", err, tt.wantErr)
			}

			if err = assertProjection(tt.args.ctx, tt.projAsserts, eventStore, tt.args.projection); err != nil {
				t.Errorf("RebuildProjectionSince() projection assertion failed: %v", err)
			}
		})
	}

}

func testRebuildAllProjection(t *testing.T, adapter func() persistence.Port, cleanUp func()) {
	cleanUp()
	tenantID := "0000-0000-0000"
	type args struct {
		ctx        context.Context
		aggregates []event.AggregateWithEventSourcingSupport
		eventStore func() event.EventStore
	}

	type projDefinition struct {
		proj        event.Projection
		projReset   func(proj event.Projection)
		projAsserts projectionAsserts
	}

	tests := []struct {
		name      string
		args      args
		sinceTime time.Time
		wantErr   bool
		proj1     projDefinition
		proj2     projDefinition
	}{
		{
			name: "rebuild all projection without since time",
			args: args{
				ctx: context.Background(),
				aggregates: []event.AggregateWithEventSourcingSupport{
					newForTestConcreteAggregate("1", "Name", 0, tenantID, []event.IEvent{
						ForTestMakeCreateEvent("1", tenantID, time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
						ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
					}),
					newForTestConcreteAggregate("2", "Name", 0, tenantID, []event.IEvent{
						ForTestMakeCreateEvent2("2", tenantID, time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
						ForTestMakeSnapshot2("2", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
						ForTestMakeEvent2("2", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC)),
					}),
				},
			},
			wantErr:   false,
			sinceTime: time.Time{},
			proj1: projDefinition{
				proj:      newTestProjectionTypeOne("projection_1", tenantID, 0*time.Second, 1),
				projReset: func(proj event.Projection) { proj.(*forTestProjection).ForTestResetAll() },
				projAsserts: projectionAsserts{
					id:                 shared.NewProjectionID(tenantID, "projection_1"),
					wantPrepCounter:    1,
					wantExecuteCounter: 2, //chunksize is 1
					wantFinishCounter:  1,
					wantState:          projection.Running,
					wantUpdatedAt:      true,
					wantEventStream: []event.IEvent{
						ForTestMakeCreateEvent("1", tenantID, time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
						ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC))},
				},
			},
			proj2: projDefinition{
				proj:      newTestProjectionTypeTwo("projection_2", tenantID, 0*time.Second, 100),
				projReset: func(proj event.Projection) { proj.(*forTestProjection).ForTestResetAll() },
				projAsserts: projectionAsserts{
					id:                 shared.NewProjectionID(tenantID, "projection_2"),
					wantPrepCounter:    1,
					wantExecuteCounter: 1, //chunksize is 100
					wantFinishCounter:  1,
					wantState:          projection.Running,
					wantUpdatedAt:      true,
					wantEventStream: []event.IEvent{
						ForTestMakeSnapshot2("2", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
						ForTestMakeEvent2("2", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC))}},
			},
		},
		{
			name: "rebuild all projection without since time and failing proj_1",
			args: args{
				ctx: context.Background(),
				aggregates: []event.AggregateWithEventSourcingSupport{
					newForTestConcreteAggregate("1", "Name", 0, tenantID, []event.IEvent{
						ForTestMakeCreateEvent("1", tenantID, time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
						ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
					}),
					newForTestConcreteAggregate("2", "Name", 0, tenantID, []event.IEvent{
						ForTestMakeCreateEvent2("2", tenantID, time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
						ForTestMakeSnapshot2("2", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
						ForTestMakeEvent2("2", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC)),
					}),
				},
			},
			wantErr:   true,
			sinceTime: time.Time{},
			proj1: projDefinition{
				proj:      newTestProjectionTypeOneWithExecuteFail("projection_1", tenantID, 100, true, 2),
				projReset: func(proj event.Projection) { proj.(*forTestProjection).ForTestReset([]event.IEvent{}, 0, 1, 0) },
				projAsserts: projectionAsserts{
					id:                 shared.NewProjectionID(tenantID, "projection_1"),
					wantPrepCounter:    1,
					wantExecuteCounter: 2,
					wantFinishCounter:  0,
					wantState:          projection.Erroneous,
					wantUpdatedAt:      true,
					wantEventStream:    []event.IEvent{},
				},
			},
			proj2: projDefinition{
				proj:      newTestProjectionTypeTwo("projection_2", tenantID, 0*time.Second, 100),
				projReset: func(proj event.Projection) { proj.(*forTestProjection).ForTestResetAll() },
				projAsserts: projectionAsserts{
					id:                 shared.NewProjectionID(tenantID, "projection_2"),
					wantPrepCounter:    1,
					wantExecuteCounter: 1,
					wantFinishCounter:  1,
					wantState:          projection.Running,
					wantUpdatedAt:      true,
					wantEventStream: []event.IEvent{
						ForTestMakeSnapshot2("2", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
						ForTestMakeEvent2("2", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC))}},
			},
		},
		{
			name: "rebuild all projection with since time",
			args: args{
				ctx: context.Background(),
				aggregates: []event.AggregateWithEventSourcingSupport{
					newForTestConcreteAggregate("1", "Name", 0, tenantID, []event.IEvent{
						ForTestMakeCreateEvent("1", tenantID, time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
						ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
					}),
					newForTestConcreteAggregate("2", "Name", 0, tenantID, []event.IEvent{
						ForTestMakeCreateEvent2("2", tenantID, time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC), time.Date(2020, 12, 31, 23, 59, 59, 999999999, time.UTC)),
						ForTestMakeSnapshot2("2", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
						ForTestMakeEvent2("2", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC)),
					}),
				},
			},
			sinceTime: time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC),
			wantErr:   false,
			proj1: projDefinition{
				proj:      newTestProjectionTypeOne("projection_1", tenantID, 0*time.Second, 100),
				projReset: func(proj event.Projection) { proj.(*forTestProjection).ForTestResetAll() },
				projAsserts: projectionAsserts{
					id:                 shared.NewProjectionID(tenantID, "projection_1"),
					wantPrepCounter:    1,
					wantExecuteCounter: 1,
					wantFinishCounter:  1,
					wantState:          projection.Running,
					wantUpdatedAt:      true,
					wantEventStream: []event.IEvent{
						ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC))},
				},
			},
			proj2: projDefinition{
				proj:      newTestProjectionTypeTwo("projection_2", tenantID, 0*time.Second, 100),
				projReset: func(proj event.Projection) { proj.(*forTestProjection).ForTestResetAll() },
				projAsserts: projectionAsserts{
					id:                 shared.NewProjectionID(tenantID, "projection_2"),
					wantPrepCounter:    1,
					wantExecuteCounter: 1,
					wantFinishCounter:  1,
					wantState:          projection.Running,
					wantUpdatedAt:      true,
					wantEventStream: []event.IEvent{
						ForTestMakeSnapshot2("2", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
						ForTestMakeEvent2("2", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC))}},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			adp := adapter()
			eventStore, err, _ := eventstore.New(adp,
				eventstore.WithProjection(tt.proj1.proj),
				eventstore.WithProjection(tt.proj2.proj),
			)
			defer cleanUp()
			if err != nil {
				t.Errorf("RebuildAllProjection() error = %v, wantErr %v", err, nil)
			}
			resSaveCh, err := event.SaveAggregates(tt.args.ctx, eventStore, tt.args.aggregates...)
			if err != nil {
				t.Errorf("RebuildProjection() error = %v, wantErr %v", err, nil)
			}
			if err = assertProjectionError(resSaveCh, false); err != nil {
				t.Errorf("RebuildProjection() projection error = %v, wantErr %v", err, nil)
			}

			tt.proj1.projReset(tt.proj1.proj)
			tt.proj2.projReset(tt.proj2.proj)

			var errCh chan error
			if tt.sinceTime.IsZero() {
				errCh = eventStore.RebuildAllProjection(tt.args.ctx, tenantID)
			} else {
				errCh = eventStore.RebuildAllProjectionSince(tt.args.ctx, tenantID, tt.sinceTime)
			}

			if err = assertProjectionError(errCh, tt.wantErr); err != nil {
				t.Errorf("RebuildAllProjection() error = %v, wantErr %v", err, tt.wantErr)
			}

			if err = assertProjection(tt.args.ctx, tt.proj1.projAsserts, eventStore, tt.proj1.proj); err != nil {
				t.Errorf("RebuildAllProjection() projection assertion for projection_1 failed: %v", err)
			}

			if err = assertProjection(tt.args.ctx, tt.proj2.projAsserts, eventStore, tt.proj2.proj); err != nil {
				t.Errorf("RebuildAllProjection() projection assertion for projection_2 failed: %v", err)
			}
		})
	}
}
