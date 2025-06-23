package event

import "fmt"

// ErrorEmptyEventStream used for aggregates without event streams
type ErrorEmptyEventStream struct {
	AggregateID   string
	TenantID      string
	AggregateType string
	Err           error
}

func (c *ErrorEmptyEventStream) Error() string {
	return fmt.Sprintf("empty event stream detected for aggregateID %q of aggregateType %q and tenant %q: %v", c.AggregateID, c.AggregateType, c.TenantID, c.Err)
}

func (c *ErrorEmptyEventStream) Unwrap() error {
	return c.Err
}
