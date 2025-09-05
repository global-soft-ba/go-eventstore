package persistence

import (
	"github.com/global-soft-ba/go-eventstore/eventstore/core/port/persistence/aggregate"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/port/persistence/projection"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/port/persistence/transactor"
)

type Port interface {
	AggregatePort() aggregate.Port
	ProjectionPort() projection.Port
	Transactor() transactor.Port
}
