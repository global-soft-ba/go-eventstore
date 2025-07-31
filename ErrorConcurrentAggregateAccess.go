package event

import "fmt"

// ErrorConcurrentAggregateAccess used for aggregates which are already in manipulation
type ErrorConcurrentAggregateAccess struct {
	TenantID      string
	AggregateType string
	AggregateID   string
}

func (c *ErrorConcurrentAggregateAccess) Error() string {
	return fmt.Sprintf("concurrent access detected for aggregateID %q of aggregateType %q and tenant %v", c.AggregateID, c.AggregateType, c.TenantID)
}
