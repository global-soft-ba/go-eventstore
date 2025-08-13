package event

import (
	"fmt"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/shared"
)

// ErrorInsertBeforeCreateEventStream used for event streams that should be written into but that have not been created
type ErrorInsertBeforeCreateEventStream struct {
	TenantID      string
	AggregateID   string
	AggregateType string
}

func (c *ErrorInsertBeforeCreateEventStream) Error() string {
	return fmt.Sprintf("event stream %q was not create before inserting any other events", shared.NewAggregateID(c.TenantID, c.AggregateType, c.AggregateID))
}
