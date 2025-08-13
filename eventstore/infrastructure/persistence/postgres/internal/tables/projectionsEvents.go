package tables

type ProjectionsEventRow struct {
	AggregateEventRow
	ProjectionID string `db:"projection_id"`
}

type ProjectionsEventsTableSchema struct {
	Name            string
	ProjectionID    string
	ID              string
	TenantID        string
	AggregateType   string
	AggregateID     string
	Version         string
	Type            string
	Class           string
	TransactionTime string
	ValidTime       string
	FromMigration   string
	Data            string
}

// AllColumns if you change order of columns you must adjust the function ...ToArrayOfValues in mapper as well.
func (a ProjectionsEventsTableSchema) AllColumns() []string {
	return []string{a.ProjectionID, a.ID, a.TenantID, a.AggregateType, a.AggregateID, a.Version, a.Type, a.Class, a.TransactionTime, a.ValidTime, a.FromMigration, a.Data}
}

var ProjectionsEventsTable = ProjectionsEventsTableSchema{
	Name:            "projections_events",
	ProjectionID:    "projection_id",
	ID:              "id",
	TenantID:        "tenant_id",
	AggregateType:   "aggregate_type",
	AggregateID:     "aggregate_id",
	Version:         "version",
	Type:            "type",
	Class:           "class",
	ValidTime:       "valid_time",
	TransactionTime: "transaction_time",
	FromMigration:   "from_migration",
	Data:            "data",
}

type ProjectionsEventsLoadRow struct {
	ProjectionsEventRow
	State string `db:"state"` //must be equal to the field state in projections table
}
