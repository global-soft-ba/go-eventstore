package internal

import (
	sq "github.com/Masterminds/squirrel"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/port/persistence/aggregate"
	trans "github.com/global-soft-ba/go-eventstore/eventstore/core/port/persistence/transactor"
)

func NewAggregates(dataBaseSchema string, placeholder sq.PlaceholderFormat, trans trans.Port) aggregate.Port {
	return &aggregates{
		loader: newLoader(dataBaseSchema, placeholder, trans),
		saver:  newSaver(dataBaseSchema, placeholder, trans),
	}
}

type aggregates struct {
	saver
	loader
}
