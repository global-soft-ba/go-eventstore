package memory

import (
	"fmt"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/port/persistence"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/port/persistence/transactor"
	"github.com/global-soft-ba/go-eventstore/eventstore/infrastructure/persistence/memory/internal"
)

var Rollback = fmt.Errorf("rollback error")
var Commit error = nil

func NewSlowAdapterForTest() persistence.Port {
	trans := internal.NewTransactor()
	aggRepro := internal.NewSlowAggregatePort(trans)
	projRepro := internal.NewProjecter(trans)

	return Adapter{aggRepro, projRepro, trans}
}

func NewTransactor() transactor.Port {
	return internal.NewTransactor()
}
