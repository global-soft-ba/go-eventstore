package tables

var AggregateSnapsShotTable = AggregatePersistentEventsTableSchema{
	Name:            "aggregates_snapshots",
	ID:              "id",
	TenantID:        "tenant_id",
	AggregateType:   "aggregate_type",
	AggregateID:     "aggregate_id",
	Version:         "version",
	Type:            "type",
	Class:           "class",
	TransactionTime: "transaction_time",
	ValidTime:       "valid_time",
	FromMigration:   "from_migration",
	Data:            "data",
}
