package internal

import (
	"context"
	sq "github.com/Masterminds/squirrel"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/port/persistence/aggregate"
	trans "github.com/global-soft-ba/go-eventstore/eventstore/core/port/persistence/transactor"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/shared"
	"time"
)

func NewSlowAggregatePort(dataBaseSchema string, placeholder sq.PlaceholderFormat, trans trans.Port) aggregate.Port {
	return &slowAggregates{
		loader: newLoader(dataBaseSchema, placeholder, trans),
		saver:  newSaver(dataBaseSchema, placeholder, trans),
	}
}

type slowAggregates struct {
	saver
	loader
}

// Lock The idea is that only set-valued IDs are stored with a delay. This is used in the concurrent test cases,
// by having the first store with two Ids (slowed down) and the second store with one id (not slowed down).
// Thus, we can test the concurrent behavior.
func (s slowAggregates) Lock(ctx context.Context, ids ...shared.AggregateID) (err error) {
	err = s.saver.Lock(ctx, ids...)
	if len(ids) > 1 {
		time.Sleep(100 * time.Millisecond)
	}

	return err
}
