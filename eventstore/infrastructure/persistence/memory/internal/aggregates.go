package internal

import (
	"github.com/global-soft-ba/go-eventstore/eventstore/core/port/persistence/aggregate"
	trans "github.com/global-soft-ba/go-eventstore/eventstore/core/port/persistence/transactor"
)

func NewAggregates(trans trans.Port) aggregate.Port {
	return &aggregates{
		saver:  newSaver(trans),
		loader: newLoader(trans),
	}
}

type aggregates struct {
	saver
	loader
}
