package event

import "fmt"

type ErrorConcurrentModification struct {
	AggregateID   string
	TenantID      string
	AggregateType string
	Version       int
	multiResult   string
}

func (c *ErrorConcurrentModification) Add(aggregateID string, tenantID string, aggregateType string, version int) {
	c.multiResult += fmt.Sprintf("aggregateID %q and version %d for tenant %q and type %q", aggregateID, version, tenantID, aggregateType) + "\n"
}

func (c *ErrorConcurrentModification) Error() string {
	if c.multiResult != "" {
		return fmt.Sprintf("concurrent modifications in one/all of the following aggregates detected:\n%v", c.multiResult)
	}

	return fmt.Sprintf("concurrent modification detected for aggregateID %q and version %d", c.AggregateID, c.Version)
}
