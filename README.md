# Go EventStore

A library for implementing an event store in Go applications.

-----

## Features

- **Event Sourcing Made Simple**: Provides a clean and intuitive API for managing event streams.
- **Pluggable Persistence Layer**: Supports PostgreSQL and in-memory storage.
- **Stream Isolation**: Strong isolation via composite keys, partitions, and versioning per aggregate.
- **Snapshot Support**: Built-in snapshotting for fast aggregate state rehydration.
- **Projections**: Supports both single-stream and cross-stream projections with configurable consistency options.
- **Event Replay**: Enables selective replay for rebuilding projections.
- **Concurrency Control**: Optimistic concurrency with multiple strategies.
- **Bi-Temporal Events**: Fully supports **bi-temporal modeling**, allowing events to have both transaction time (recorded time) and valid time (domain-valid time). This enables advanced use cases like historical corrections, future-dated events, and precise event ordering.
- **Event Migration**: Allows soft/hard deletes and migration events.
- **Subscriptions**: Subscribes to predefined event types per projection.
- **Patch Support**: Supports bi-temporal patches with various strategies.

### Bi-Temporal Events

Bi-temporal events are a key feature of the `go-eventstore` library, enabling advanced event modeling by supporting two distinct timestamps for each event:

1. **Transaction Time**: The time when the event was recorded in the system.
2. **Valid Time**: The time when the event is considered valid in the domain.

This dual-timestamp approach allows for:
- **Future-Dated Events**: Scheduling events to take effect at a specific valid time in the future.
- **Precise Event Ordering**: Ensuring events are processed in the correct sequence based on their valid time.

#### Patch Strategies

To handle bi-temporal events, the library provides configurable strategies for managing patches:

- **Error**: Throws an error if a patch is detected. This is the default for historical patches.
- **Manual**: Queues the patch until its valid time is reached, requiring manual projection execution.
- **Projected**: Immediately passes the patch to the projection, which must handle temporal details.
- **Rebuild**: Sends the patch to the projection and triggers a full rebuild afterward.
- **Rebuild Since**: Similar to **Rebuild**, but only rebuilds from the patch's valid time.

#### Configuration Example

```go
eventstore.WithHistoricalPatchStrategy(myProjection.ID(), event.Error)
eventstore.WithHistoricalPatchStrategy(myProjection2.ID(), event.Manual)
eventstore.WithHistoricalPatchStrategy(myProjection3.ID(), event.Projected)
eventstore.WithHistoricalPatchStrategy(myProjection4.ID(), event.Rebuild)
eventstore.WithHistoricalPatchStrategy(myProjection5.ID(), event.RebuildSince)
```

-----

## Installation

To install the library, use:

```bash
go get github.com/global-soft-ba/go-eventstore
```

-----

## Initialization

Set up the EventStore with PostgreSQL as the persistence adapter:

```go
import (
    "github.com/global-soft-ba/go-eventstore"
    "github.com/global-soft-ba/go-eventstore/eventstore"
    eventStorePostgres "github.com/global-soft-ba/go-eventstore/eventstore/infrastructure/persistence/postgres"
)

func initializeEventStore(ctx context.Context, postgresPool *pgxpool.Pool) (*eventstore.EventStore, chan error, error) {
    psqlEventStore, err := eventStorePostgres.New(postgresPool, eventStorePostgres.Options{AutoMigrate: true})
    if err != nil {
        return nil, nil, fmt.Errorf("could not create postgres event store: %w", err)
    }

    eventStore, err, errChan := eventstore.New(psqlEventStore)
    if err != nil {
        return nil, nil, fmt.Errorf("could not initialize event store: %w", err)
    }

    return eventStore, errChan, nil
}
```

-----

## Projections

Projections in event sourcing are read models that process events to build a queryable state. They listen to events from the event store and update their state accordingly. Projections are typically used to create views optimized for querying, such as aggregations, denormalized tables, or other derived data.

### Key Components

1. **Event Types**: Specifies which event types the projection listens to.
2. **Execution Logic**: Processes events and updates the read model.
3. **Chunk Size**: Determines how many events are processed in a single batch.
4. **Rebuild Support**: Allows recreating the state by replaying all events.

### Example

### 1. Projection Initialization

The `Projection` struct is initialized with:
- `chunkSize`: Number of events to process in a batch.
- `persistence`: A write model interface for updating the read model.

```go
type Projection struct {
    chunkSize   int
    persistence writeModel.Port
}

func NewItemProjection(ctx context.Context, chunkSize int, persistence writeModel.Port) *Projection {
    return &Projection{persistence: persistence, chunkSize: chunkSize}
}
```

### 2. Event Types

The EventTypes method defines the events the projection listens to. For example:

```go
func (p *Projection) EventTypes() []string {
    return []string{
        event.EventType(itemModel.Created{}),
        event.EventType(itemModel.Deleted{}),
        event.EventType(itemModel.Renamed{}),
    }
}
```

### 3. Processing Events

The `Execute` method processes a batch of events. Events are grouped by tenant, and each tenant's events are processed in a transaction.

```go
func (p *Projection) Execute(ctx context.Context, events []event.IEvent) error {
    eventsPerTenant, _ := helper.PartitionPerTenant(events)
    for tenantID, eventsOfTenant := range eventsPerTenant {
        err := p.execute(ctx, tenantID, eventsOfTenant)
        if err != nil {
            return err
        }
    }
    return nil
}
```

The `execute` method ensures that all events for a tenant are processed within a single transaction. This guarantees consistency when updating the read model.

```go
func (p *Projection) execute(ctx context.Context, tenantID string, eventsOfTenant []event.IEvent) error {
    return p.persistence.ExecWithinTransaction(ctx, tenantID, func(txCtx context.Context) error {
        for _, iEvent := range eventsOfTenant {
            if err := p.processEvent(txCtx, iEvent); err != nil {
                return err
            }
        }
        return nil
    })
}
```

### 4. Event Processing

The `processEvent` method handles each event type and delegates the processing to specific handlers. Unsupported event types return an error.

```go
func (p *Projection) processEvent(ctx context.Context, evt event.IEvent) error {
    switch event.EventType(evt) {
    case event.EventType(itemModel.Created{}):
        return p.handleItemCreated(ctx, evt.(*itemModel.Created))
    case event.EventType(itemModel.Deleted{}):
        return p.handleItemDeleted(ctx, evt.(*itemModel.Deleted))
    case event.EventType(itemModel.Renamed{}):
        return p.handleItemRenamed(ctx, evt.(*itemModel.Renamed))
    }
    return fmt.Errorf("unsupported event type %q", event.EventType(evt))
}
```

### 5. Rebuild Support

The PrepareRebuild and FinishRebuild methods handle the lifecycle of rebuilding a projection.

```go
func (p *Projection) PrepareRebuild(ctx context.Context, tenantID string) error {
    return p.persistence.PrepareRebuild(ctx, tenantID)
}

func (p *Projection) FinishRebuild(ctx context.Context, tenantID string) error {
    return p.persistence.FinishRebuild(ctx, tenantID)
}
```

-----

## Registering `ItemProjection` into`EventStore`

The `ItemProjection` can be registered into `EventStore` during its initialization. This enables the projection to process events and update the read model.

```go
itemWriteModel, err := writeModelItem.NewAdapter(ctx, postgresPool)
if err != nil {
    log.Fatalf("could not create item writemodel: %v", err)
}

// Initialize the ItemProjection
itemProj := projectionItem.NewItemProjection(ctx, env.Config().ProjectionChunkSize, itemWriteModel)

// Register the ItemProjection with the EventStore
eventStore, err, errChan := eventstore.New(psqlEventStore,
    eventstore.WithConcurrentModificationStrategy(reflect.TypeOf(item.Item{}).Name(), event.Ignore),
    eventstore.WithProjection(itemProj),
    eventstore.WithProjectionType(itemProj.ID(), event.ESS),
    eventstore.WithProjectionTimeOut(itemProj.ID(), env.Config().ProjectionTimeout),
    eventstore.WithRebuildTimeOut(itemProj.ID(), env.Config().ProjectionRebuildTimeout),
)
if err != nil {
    log.Fatalf("could not create eventstore: %v", err)
}
```

-----

## App Example
Application demonstrating the usage of [go-eventstore](https://github.com/global-soft-ba/go-eventstore/tree/main/example).