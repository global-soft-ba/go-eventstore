package tests

import (
	"context"
	"github.com/global-soft-ba/go-eventstore"
	"github.com/global-soft-ba/go-eventstore/eventstore"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/domain/model/projection"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/port/persistence"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/shared"
	"github.com/global-soft-ba/go-eventstore/eventstore/infrastructure/persistence/memory"
	"github.com/global-soft-ba/go-eventstore/tests/testdata"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"strconv"
	"testing"
	"time"
)

func testDeleteEvents(t *testing.T, adapter func() persistence.Port, cleanUp func()) {
	tenantID := uuid.NewString()
	t.Parallel()
	type args struct {
		ctx             context.Context
		aggregate       event.AggregateWithEventSourcingSupport
		aggregateID     string
		aggregateType   string
		versionToDelete int
		strategy        event.DeleteStrategy
		store           func(adp persistence.Port) (event.EventStore, event.Projection, error)
	}
	tests := []struct {
		name                 string
		args                 args
		wantErr              assert.ErrorAssertionFunc
		wantEventStream      []event.IEvent
		wantVersion          int
		wantProjectionAssert projectionAsserts
	}{
		{
			name: "delete event without projection (hard delete)",
			args: args{
				ctx: context.Background(),
				aggregate: newForTestConcreteAggregate("1", "Name", 0, tenantID, []event.IEvent{
					ForTestMakeCreateEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
					ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
					ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC)),
				}),
				aggregateID:     "1",
				aggregateType:   "forTestConcreteAggregate",
				versionToDelete: 3,
				store: func(adp persistence.Port) (event.EventStore, event.Projection, error) {
					store, err, _ := eventstore.New(adp,
						eventstore.WithDeleteStrategy("forTestConcreteAggregate", event.HardDelete))
					return store, nil, err
				},
			},

			wantErr: assert.NoError,
			wantEventStream: []event.IEvent{
				ForTestMakeCreateEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
				ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
			},
			wantVersion:          3,
			wantProjectionAssert: projectionAsserts{},
		},
		{
			name: "delete event without projection (soft delete)",
			args: args{
				ctx: context.Background(),
				aggregate: newForTestConcreteAggregate("1", "Name", 0, tenantID, []event.IEvent{
					ForTestMakeCreateEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
					ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
					ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC)),
				}),
				aggregateID:     "1",
				aggregateType:   "forTestConcreteAggregate",
				versionToDelete: 3,
				store: func(adp persistence.Port) (event.EventStore, event.Projection, error) {
					store, err, _ := eventstore.New(adp,
						eventstore.WithDeleteStrategy("forTestConcreteAggregate", event.SoftDelete))
					return store, nil, err
				},
			},

			wantErr: assert.NoError,
			wantEventStream: []event.IEvent{
				ForTestMakeCreateEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
				ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
			},
			wantVersion:          3,
			wantProjectionAssert: projectionAsserts{},
		},
		{
			name: "delete event still in projection queue (hard delete)",
			args: args{
				ctx: context.Background(),
				aggregate: newForTestConcreteAggregate("1", "Name", 0, tenantID, []event.IEvent{
					ForTestMakeCreateEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
					ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
					ForTestMakePatchEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 5, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC)),
				}),
				aggregateID:     "1",
				aggregateType:   "forTestConcreteAggregate",
				versionToDelete: 3,
				store: func(adp persistence.Port) (event.EventStore, event.Projection, error) {
					projectionTypeOne := newTestProjectionTypeOne("projection_1", tenantID, 0, 1)
					store, err, _ := eventstore.New(adp,
						eventstore.WithDeleteStrategy("forTestConcreteAggregate", event.HardDelete),
						eventstore.WithProjection(projectionTypeOne),
						eventstore.WithProjectionType(projectionTypeOne.ID(), event.ECS),
						// due to historical patch event (see above) it will kept in the queue
						eventstore.WithHistoricalPatchStrategy(projectionTypeOne.ID(), event.Manual),
					)
					return store, projectionTypeOne, err
				},
			},
			wantErr: assert.NoError,
			wantEventStream: []event.IEvent{
				ForTestMakeCreateEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
				ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
			},
			wantVersion: 3,
			wantProjectionAssert: projectionAsserts{
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
			name: "delete event still in projection queue (soft delete)",
			args: args{
				ctx: context.Background(),
				aggregate: newForTestConcreteAggregate("1", "Name", 0, tenantID, []event.IEvent{
					ForTestMakeCreateEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
					ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
					ForTestMakePatchEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 5, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC)),
				}),
				aggregateID:     "1",
				aggregateType:   "forTestConcreteAggregate",
				versionToDelete: 3,
				store: func(adp persistence.Port) (event.EventStore, event.Projection, error) {
					projectionTypeOne := newTestProjectionTypeOne("projection_1", tenantID, 0, 1)
					store, err, _ := eventstore.New(adp,
						eventstore.WithDeleteStrategy("forTestConcreteAggregate", event.SoftDelete),
						eventstore.WithProjection(projectionTypeOne),
						eventstore.WithProjectionType(projectionTypeOne.ID(), event.ECS),
						// due to historical patch event (see above) it will kept in the queue
						eventstore.WithHistoricalPatchStrategy(projectionTypeOne.ID(), event.Manual),
					)
					return store, projectionTypeOne, err
				},
			},
			wantErr: assert.NoError,
			wantEventStream: []event.IEvent{
				ForTestMakeCreateEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
				ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
			},
			wantVersion: 3,
			wantProjectionAssert: projectionAsserts{
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
			name: "delete event and send to projection (hard delete)",
			args: args{
				ctx: context.Background(),
				aggregate: newForTestConcreteAggregate("1", "Name", 0, tenantID, []event.IEvent{
					ForTestMakeCreateEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
					ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
					ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC)),
				}),
				aggregateID:     "1",
				aggregateType:   "forTestConcreteAggregate",
				versionToDelete: 3,
				store: func(adp persistence.Port) (event.EventStore, event.Projection, error) {
					projectionTypeOne := newTestProjectionTypeOne("projection_1", tenantID, 0, 1)
					store, err, _ := eventstore.New(adp,
						eventstore.WithDeleteStrategy("forTestConcreteAggregate", event.HardDelete),
						eventstore.WithProjection(projectionTypeOne),
						eventstore.WithProjectionType(projectionTypeOne.ID(), event.CCS),
						eventstore.WithDeletePatchStrategy(projectionTypeOne.ID(), event.Projected),
					)
					return store, projectionTypeOne, err
				},
			},
			wantErr: assert.NoError,
			wantEventStream: []event.IEvent{
				ForTestMakeCreateEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
				ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
			},
			wantVersion: 3,
			wantProjectionAssert: projectionAsserts{
				id:                 shared.NewProjectionID(tenantID, "projection_1"),
				wantPrepCounter:    0,
				wantExecuteCounter: 1,
				wantFinishCounter:  0,
				wantState:          projection.Running,
				wantUpdatedAt:      true,
				wantEventStream: []event.IEvent{
					ForTestMakeDeleteEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC)),
				},
			},
		},
		{
			name: "delete event and send to projection (soft delete)",
			args: args{
				ctx: context.Background(),
				aggregate: newForTestConcreteAggregate("1", "Name", 0, tenantID, []event.IEvent{
					ForTestMakeCreateEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
					ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
					ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC)),
				}),
				aggregateID:     "1",
				aggregateType:   "forTestConcreteAggregate",
				versionToDelete: 3,
				store: func(adp persistence.Port) (event.EventStore, event.Projection, error) {
					projectionTypeOne := newTestProjectionTypeOne("projection_1", tenantID, 0, 1)
					store, err, _ := eventstore.New(adp,
						eventstore.WithDeleteStrategy("forTestConcreteAggregate", event.SoftDelete),
						eventstore.WithProjection(projectionTypeOne),
						eventstore.WithProjectionType(projectionTypeOne.ID(), event.CCS),
						eventstore.WithDeletePatchStrategy(projectionTypeOne.ID(), event.Projected),
					)
					return store, projectionTypeOne, err
				},
			},
			wantErr: assert.NoError,
			wantEventStream: []event.IEvent{
				ForTestMakeCreateEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
				ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
			},
			wantVersion: 3,
			wantProjectionAssert: projectionAsserts{
				id:                 shared.NewProjectionID(tenantID, "projection_1"),
				wantPrepCounter:    0,
				wantExecuteCounter: 1,
				wantFinishCounter:  0,
				wantState:          projection.Running,
				wantUpdatedAt:      true,
				wantEventStream: []event.IEvent{
					ForTestMakeDeleteEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC)),
				},
			},
		},
		{
			name: "delete event and rebuild projection (hard delete)",
			args: args{
				ctx: context.Background(),
				aggregate: newForTestConcreteAggregate("1", "Name", 0, tenantID, []event.IEvent{
					ForTestMakeCreateEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
					ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
					ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC)),
				}),
				aggregateID:     "1",
				aggregateType:   "forTestConcreteAggregate",
				versionToDelete: 3,
				store: func(adp persistence.Port) (event.EventStore, event.Projection, error) {
					projectionTypeOne := newTestProjectionTypeOne("projection_1", tenantID, 0, 1)
					store, err, _ := eventstore.New(adp,
						eventstore.WithDeleteStrategy("forTestConcreteAggregate", event.HardDelete),
						eventstore.WithProjection(projectionTypeOne),
						eventstore.WithProjectionType(projectionTypeOne.ID(), event.CCS),
						eventstore.WithDeletePatchStrategy(projectionTypeOne.ID(), event.Rebuild),
					)
					return store, projectionTypeOne, err
				},
			},
			wantErr: assert.NoError,
			wantEventStream: []event.IEvent{
				ForTestMakeCreateEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
				ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
			},
			wantVersion: 3,
			wantProjectionAssert: projectionAsserts{
				id:                 shared.NewProjectionID(tenantID, "projection_1"),
				wantPrepCounter:    1,
				wantExecuteCounter: 2,
				wantFinishCounter:  1,
				wantState:          projection.Running,
				wantUpdatedAt:      true,
				wantEventStream: []event.IEvent{
					ForTestMakeCreateEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
					ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
				},
			},
		},
		{
			name: "delete event and rebuild projection (soft delete)",
			args: args{
				ctx: context.Background(),
				aggregate: newForTestConcreteAggregate("1", "Name", 0, tenantID, []event.IEvent{
					ForTestMakeCreateEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
					ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
					ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC)),
				}),
				aggregateID:     "1",
				aggregateType:   "forTestConcreteAggregate",
				versionToDelete: 3,
				store: func(adp persistence.Port) (event.EventStore, event.Projection, error) {
					projectionTypeOne := newTestProjectionTypeOne("projection_1", tenantID, 0, 1)
					store, err, _ := eventstore.New(adp,
						eventstore.WithDeleteStrategy("forTestConcreteAggregate", event.SoftDelete),
						eventstore.WithProjection(projectionTypeOne),
						eventstore.WithProjectionType(projectionTypeOne.ID(), event.CCS),
						eventstore.WithDeletePatchStrategy(projectionTypeOne.ID(), event.Rebuild),
					)
					return store, projectionTypeOne, err
				},
			},
			wantErr: assert.NoError,
			wantEventStream: []event.IEvent{
				ForTestMakeCreateEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
				ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
			},
			wantVersion: 3,
			wantProjectionAssert: projectionAsserts{
				id:                 shared.NewProjectionID(tenantID, "projection_1"),
				wantPrepCounter:    1,
				wantExecuteCounter: 2,
				wantFinishCounter:  1,
				wantState:          projection.Running,
				wantUpdatedAt:      true,
				wantEventStream: []event.IEvent{
					ForTestMakeCreateEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
					ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
				},
			},
		},
		{
			name: "error/no delete strategy",
			args: args{
				ctx: context.Background(),
				aggregate: newForTestConcreteAggregate("1", "Name", 0, tenantID, []event.IEvent{
					ForTestMakeCreateEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
					ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
				}),
				aggregateID:     "1",
				aggregateType:   "forTestConcreteAggregate",
				versionToDelete: 2,
				store: func(adp persistence.Port) (event.EventStore, event.Projection, error) {
					store, err, _ := eventstore.New(adp,
						eventstore.WithDeleteStrategy("forTestConcreteAggregate", event.NoDelete))

					return store, nil, err
				},
			},
			wantErr: assert.Error,
			wantEventStream: []event.IEvent{
				ForTestMakeCreateEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
				ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
			},
			wantVersion: 2,
		},
		{
			name: "error/delete create stream",
			args: args{
				ctx: context.Background(),
				aggregate: newForTestConcreteAggregate("1", "Name", 0, tenantID, []event.IEvent{
					ForTestMakeCreateEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
					ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
				}),
				aggregateID:     "1",
				aggregateType:   "forTestConcreteAggregate",
				versionToDelete: 1,
				store: func(adp persistence.Port) (event.EventStore, event.Projection, error) {
					store, err, _ := eventstore.New(adp,
						eventstore.WithDeleteStrategy("forTestConcreteAggregate", event.HardDelete))

					return store, nil, err
				},
			},
			wantErr: assert.Error,
			wantEventStream: []event.IEvent{
				ForTestMakeCreateEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
				ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
			},
			wantVersion: 2,
		},
		{
			name: "delete historical close event (hard delete)",
			args: args{
				ctx: context.Background(),
				aggregate: newForTestConcreteAggregate("1", "Name", 0, tenantID, []event.IEvent{
					ForTestMakeCreateEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
					ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
					ForTestMakeCloseEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC)),
				}),
				aggregateID:     "1",
				aggregateType:   "forTestConcreteAggregate",
				versionToDelete: 3,
				store: func(adp persistence.Port) (event.EventStore, event.Projection, error) {
					store, err, _ := eventstore.New(adp,
						eventstore.WithDeleteStrategy("forTestConcreteAggregate", event.HardDelete))
					return store, nil, err
				},
			},
			wantErr: assert.NoError,
			wantEventStream: []event.IEvent{
				ForTestMakeCreateEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
				ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
			},
			wantVersion: 3,
		},
		{
			name: "delete future close event (hard delete)",
			args: args{
				ctx: context.Background(),
				aggregate: newForTestConcreteAggregate("1", "Name", 0, tenantID, []event.IEvent{
					ForTestMakeCreateEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
					ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
					ForTestMakeCloseEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Now().Add(time.Hour)),
				}),
				aggregateID:     "1",
				aggregateType:   "forTestConcreteAggregate",
				versionToDelete: 3,
				store: func(adp persistence.Port) (event.EventStore, event.Projection, error) {
					store, err, _ := eventstore.New(adp,
						eventstore.WithDeleteStrategy("forTestConcreteAggregate", event.HardDelete))
					return store, nil, err
				},
			},
			wantErr: assert.NoError,
			wantEventStream: []event.IEvent{
				ForTestMakeCreateEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
				ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
			},
			wantVersion: 3,
		},
		{
			name: "delete historical close event (soft delete)",
			args: args{
				ctx: context.Background(),
				aggregate: newForTestConcreteAggregate("1", "Name", 0, tenantID, []event.IEvent{
					ForTestMakeCreateEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
					ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
					ForTestMakeCloseEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC)),
				}),
				aggregateID:     "1",
				aggregateType:   "forTestConcreteAggregate",
				versionToDelete: 3,
				store: func(adp persistence.Port) (event.EventStore, event.Projection, error) {
					store, err, _ := eventstore.New(adp,
						eventstore.WithDeleteStrategy("forTestConcreteAggregate", event.SoftDelete))
					return store, nil, err
				},
			},
			wantErr: assert.NoError,
			wantEventStream: []event.IEvent{
				ForTestMakeCreateEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
				ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
			},
			wantVersion: 3,
		},
		{
			name: "delete future close event (soft delete)",
			args: args{
				ctx: context.Background(),
				aggregate: newForTestConcreteAggregate("1", "Name", 0, tenantID, []event.IEvent{
					ForTestMakeCreateEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
					ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
					ForTestMakeCloseEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC), time.Now().Add(time.Hour)),
				}),
				aggregateID:     "1",
				aggregateType:   "forTestConcreteAggregate",
				versionToDelete: 3,
				store: func(adp persistence.Port) (event.EventStore, event.Projection, error) {
					store, err, _ := eventstore.New(adp,
						eventstore.WithDeleteStrategy("forTestConcreteAggregate", event.SoftDelete))
					return store, nil, err
				},
			},
			wantErr: assert.NoError,
			wantEventStream: []event.IEvent{
				ForTestMakeCreateEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
				ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
			},
			wantVersion: 3,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			adp := adapter()
			defer cleanUp()

			// prepare
			if tt.name == "delete event and rebuild projection (hard delete)" || tt.name == "delete event and rebuild projection (soft delete)" {
				_, ok := adp.(memory.Adapter)
				if ok {
					t.Skip("Skipping test for MemeDB adapter")
				}
			}

			store, proj, err := tt.args.store(adp)
			if err != nil {
				t.Errorf("store creation failed: %s", err)
				return
			}

			errCh, err := event.SaveAggregate(ctx, store, tt.args.aggregate)
			if err != nil {
				t.Errorf("save aggregate failed: %s", err)
			}
			<-errCh

			// reset before execution got triggered by delete
			if proj != nil {
				proj.(*forTestProjection).ForTestReset(nil, 0, 0, 0)
			}

			// execute
			deleteEvent := _getEventID(t, store, tenantID, tt.args.aggregateID, tt.args.versionToDelete)
			err = store.DeleteEvent(ctx, tenantID, tt.args.aggregateType, tt.args.aggregateID, deleteEvent)
			tt.wantErr(t, err, "DeletePatch() error = %v, wantErr %v", err, tt.wantErr)

			// assert
			gotEventStream, version, err := event.LoadAggregateAsAt(ctx, tt.args.aggregate.GetTenantID(), "forTestConcreteAggregate", tt.args.aggregate.GetID(), time.Now(), store)
			if err != nil {
				t.Errorf("retrieving aggregate failed: %s", err)
			}

			testdata.AssertEqualStream(t, gotEventStream, tt.wantEventStream)
			assert.Equalf(t, version, tt.wantVersion, "version not as expected")

			if proj != nil {
				if err = assertProjectionCounter(tt.wantProjectionAssert, proj); err != nil {
					t.Errorf("projection counter assertion failed: %v", err)
				}

				if err = assertExecutionState(ctx, tt.wantProjectionAssert.id, tt.wantProjectionAssert.wantState, tt.wantProjectionAssert.wantUpdatedAt, store); err != nil {
					t.Errorf("projection state assertion failed: %v", err)
				}

				testdata.AssertEqualStream(t, proj.(*forTestProjection).ForTestGetEvents(), tt.wantProjectionAssert.wantEventStream)
			}

			// assert correct load for projections
			if proj != nil {
				proj.(*forTestProjection).ForTestReset(nil, 0, 0, 0)
			}

		})
	}
}

func testProjectionsAfterDeleteEvents(t *testing.T, adapter func() persistence.Port, cleanUp func()) {
	tenantID := uuid.NewString()
	type args struct {
		ctx             context.Context
		aggregate       event.AggregateWithEventSourcingSupport
		aggregateID     string
		aggregateType   string
		versionToDelete int
		strategy        event.DeleteStrategy
		store           func(adp persistence.Port) (event.EventStore, event.Projection, error)
	}
	tests := []struct {
		name                 string
		args                 args
		wantErr              assert.ErrorAssertionFunc
		wantVersion          int
		wantProjectionAssert projectionAsserts
	}{

		{
			name: "projection execution after delete (hard delete)",
			args: args{
				ctx: context.Background(),
				aggregate: newForTestConcreteAggregate("1", "Name", 0, tenantID, []event.IEvent{
					ForTestMakeCreateEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
					ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
					ForTestMakePatchEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 5, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC)),
				}),
				aggregateID:     "1",
				aggregateType:   "forTestConcreteAggregate",
				versionToDelete: 3,
				store: func(adp persistence.Port) (event.EventStore, event.Projection, error) {
					projectionTypeOne := newTestProjectionTypeOne("projection_1", tenantID, 0, 1)
					store, err, _ := eventstore.New(adp,
						eventstore.WithDeleteStrategy("forTestConcreteAggregate", event.HardDelete),
						eventstore.WithProjection(projectionTypeOne),
						eventstore.WithProjectionType(projectionTypeOne.ID(), event.ECS),
						// due to historical patch event (see above) it will kept in the queue
						eventstore.WithHistoricalPatchStrategy(projectionTypeOne.ID(), event.Manual),
					)
					return store, projectionTypeOne, err
				},
			},
			wantErr:     assert.NoError,
			wantVersion: 3,
			wantProjectionAssert: projectionAsserts{
				id:                 shared.NewProjectionID(tenantID, "projection_1"),
				wantPrepCounter:    1,
				wantExecuteCounter: 2,
				wantFinishCounter:  1,
				wantState:          projection.Running,
				wantUpdatedAt:      true,
				wantEventStream: []event.IEvent{
					ForTestMakeCreateEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
					ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
				},
			},
		},
		{
			name: "projection execution after delete (soft delete)",
			args: args{
				ctx: context.Background(),
				aggregate: newForTestConcreteAggregate("1", "Name", 0, tenantID, []event.IEvent{
					ForTestMakeCreateEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
					ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
					ForTestMakePatchEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 5, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 3, time.UTC)),
				}),
				aggregateID:     "1",
				aggregateType:   "forTestConcreteAggregate",
				versionToDelete: 3,
				store: func(adp persistence.Port) (event.EventStore, event.Projection, error) {
					projectionTypeOne := newTestProjectionTypeOne("projection_1", tenantID, 0, 1)
					store, err, _ := eventstore.New(adp,
						eventstore.WithDeleteStrategy("forTestConcreteAggregate", event.SoftDelete),
						eventstore.WithProjection(projectionTypeOne),
						eventstore.WithProjectionType(projectionTypeOne.ID(), event.ECS),
						eventstore.WithHistoricalPatchStrategy(projectionTypeOne.ID(), event.Manual),
					)
					return store, projectionTypeOne, err
				},
			},
			wantErr:     assert.NoError,
			wantVersion: 3,
			wantProjectionAssert: projectionAsserts{
				id:                 shared.NewProjectionID(tenantID, "projection_1"),
				wantPrepCounter:    1,
				wantExecuteCounter: 2,
				wantFinishCounter:  1,
				wantState:          projection.Running,
				wantUpdatedAt:      true,
				wantEventStream: []event.IEvent{
					ForTestMakeCreateEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 1, time.UTC)),
					ForTestMakeEvent("1", tenantID, time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC), time.Date(2021, 1, 1, 1, 1, 1, 2, time.UTC)),
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			adp := adapter()
			defer cleanUp()

			store, proj, err := tt.args.store(adp)
			if err != nil {
				t.Errorf("store creation failed: %s", err)
				return
			}

			errCh, err := event.SaveAggregate(ctx, store, tt.args.aggregate)
			if err != nil {
				t.Errorf("save aggregate failed: %s", err)
			}
			err1 := <-errCh
			assert.NoError(t, err1)

			deleteEvent := _getEventID(t, store, tenantID, tt.args.aggregateID, tt.args.versionToDelete)
			err = store.DeleteEvent(ctx, tenantID, tt.args.aggregateType, tt.args.aggregateID, deleteEvent)
			tt.wantErr(t, err, "DeletePatch() error = %v, wantErr %v", err, tt.wantErr)

			// reset before execution got triggered by rebuild
			if proj != nil {
				proj.(*forTestProjection).ForTestReset(nil, 0, 0, 0)
			}

			// execute
			ch := store.RebuildProjection(ctx, tenantID, proj.ID())
			err2 := <-ch
			assert.NoError(t, err2)
			// assert correct load for projections
			if proj != nil {
				if err = assertProjectionCounter(tt.wantProjectionAssert, proj); err != nil {
					t.Errorf("projection counter assertion failed: %v", err)
				}

				if err = assertExecutionState(ctx, tt.wantProjectionAssert.id, tt.wantProjectionAssert.wantState, tt.wantProjectionAssert.wantUpdatedAt, store); err != nil {
					t.Errorf("projection state assertion failed: %v", err)
				}

				testdata.AssertEqualStream(t, proj.(*forTestProjection).ForTestGetEvents(), tt.wantProjectionAssert.wantEventStream)
			}

		})
	}
}

func _getEventID(t *testing.T, store event.EventStore, tenantID, aggregateID string, version int) string {
	evtToDelete, _, err := store.GetAggregatesEvents(context.Background(), tenantID, event.PageDTO{
		PageSize:   1,
		SortFields: nil,
		SearchFields: []event.SearchField{
			{Name: event.SearchAggregateID, Value: aggregateID, Operator: event.SearchEqual},
			{Name: event.SearchAggregateVersion, Value: strconv.Itoa(version), Operator: event.SearchEqual},
		},
		Values:     nil,
		IsBackward: false,
	})
	if err != nil || len(evtToDelete) == 0 {
		t.Errorf("Error in test case preparation while retrieving events to delete: %v", err)
		return ""
	}
	return evtToDelete[0].ID
}
