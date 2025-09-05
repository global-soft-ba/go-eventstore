package internal

import (
	"context"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/port/persistence/aggregate"
	trans "github.com/global-soft-ba/go-eventstore/eventstore/core/port/persistence/transactor"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/shared"
	"time"
)

func NewSlowAggregatePort(trans trans.Port) aggregate.Port {
	aggRepro := slowAggregatePort{
		saver:  newSaver(trans),
		loader: newLoader(trans),
	}
	return &aggRepro
}

type slowAggregatePort struct {
	saver
	loader
}

func (s slowAggregatePort) Lock(ctx context.Context, ids ...shared.AggregateID) (err error) {
	err = s.saver.Lock(ctx, ids...)

	if len(ids) > 1 {
		waitTime := 100 * time.Millisecond
		time.Sleep(waitTime)
	}

	return err
}
