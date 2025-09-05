package projection

import "github.com/global-soft-ba/go-eventstore"

// PartitionPerTenant takes a slice of events and partitions them by tenant ID.
func PartitionPerTenant(data []event.IEvent) (map[string][]event.IEvent, map[string][]string) {
	tenants := make(map[string][]event.IEvent)
	aggIDs := make(map[string][]string)
	for _, evt := range data {
		tenantID := evt.GetTenantID()
		tenants[tenantID] = append(tenants[tenantID], evt)
		aggIDs[tenantID] = append(aggIDs[tenantID], evt.GetAggregateID())
	}
	return tenants, aggIDs
}
