package shared

func NewAggregateID(tenantID, aggregateType, aggregateID string) AggregateID {
	return AggregateID{
		TenantID:      tenantID,
		AggregateType: aggregateType,
		AggregateID:   aggregateID,
	}
}

type AggregateID struct {
	TenantID      string
	AggregateType string
	AggregateID   string
}

func (s AggregateID) Equal(b AggregateID) bool {
	if s.TenantID == b.TenantID && s.AggregateType == b.AggregateType && s.AggregateID == b.AggregateID {
		return true
	}

	return false
}
