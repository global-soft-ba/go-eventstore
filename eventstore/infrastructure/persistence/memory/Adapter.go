package memory

import (
	"github.com/global-soft-ba/go-eventstore/eventstore/core/port/persistence"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/port/persistence/aggregate"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/port/persistence/projection"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/port/persistence/transactor"
	"github.com/global-soft-ba/go-eventstore/eventstore/infrastructure/persistence/memory/internal"
	"github.com/global-soft-ba/go-eventstore/eventstore/infrastructure/persistence/memory/internal/tests"
)

// NewTXPassThrough creates a new PostgreSQL event store supporting single transaction operations.
// It relies on a single transaction for all operations and expects an injected transaction or database instance from an external source.
// This implementation doesn't initiate a new transaction, except when creating the table schema with migrations (if AutoMigrate is set to true).
// The PassThrough EventStore is specifically designed for testing purposes.
func NewTXPassThrough() persistence.Port {
	trans := tests.NewTxPassThroughTransactor(internal.CtxStorageKey)
	aggRepro := internal.NewAggregates(trans)
	projRepro := internal.NewProjecter(trans)

	return Adapter{aggregates: aggRepro, projections: projRepro, transactor: trans}
}

// NewTXStored creates a new PostgreSQL event store supporting single transaction operations.
// It relies on a single transaction for all operations and get initialized with a transaction or database instance.
// This implementation doesn't initiate a new transaction, except when creating the table schema with migrations (if AutoMigrate is set to true).
// The PassThrough EventStore is specifically designed for testing purposes.
func NewTXStored() persistence.Port {
	trans := tests.NewTxPassThroughTransactor(internal.CtxStorageKey)
	aggRepro := internal.NewAggregates(trans)
	projRepro := internal.NewProjecter(trans)

	return Adapter{aggregates: aggRepro, projections: projRepro, transactor: trans}
}

func New() persistence.Port {
	trans := internal.NewTransactor()
	aggRepro := internal.NewAggregates(trans)
	projRepro := internal.NewProjecter(trans)

	return Adapter{aggregates: aggRepro, projections: projRepro, transactor: trans}
}

type Adapter struct {
	aggregates  aggregate.Port
	projections projection.Port
	transactor  transactor.Port
}

func (a Adapter) ProjectionPort() projection.Port {
	return a.projections
}

func (a Adapter) AggregatePort() aggregate.Port {
	return a.aggregates
}

func (a Adapter) Transactor() transactor.Port {
	return a.transactor
}
