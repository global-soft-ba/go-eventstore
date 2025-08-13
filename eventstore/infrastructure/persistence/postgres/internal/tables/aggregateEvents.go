package tables

import (
	"encoding/json"
)

type AggregateEventRow struct {
	ID              string          `db:"id"`
	TenantID        string          `db:"tenant_id"`
	AggregateType   string          `db:"aggregate_type"`
	AggregateID     string          `db:"aggregate_id"`
	Version         int64           `db:"version"`
	Type            string          `db:"type"`
	Class           string          `db:"class"`
	TransactionTime int64           `db:"transaction_time" `
	ValidTime       int64           `db:"valid_time"`
	FromMigration   bool            `db:"from_migration"`
	Data            json.RawMessage `db:"data"`
}

type AggregatePersistentEventLoadRow struct {
	AggregateEventRow
	CurrentVersion int64 `db:"current_version"` //must be equal to the field currentVersion in aggregate table
}

type AggregatePersistentEventsTableSchema struct {
	Name            string
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
func (a AggregatePersistentEventsTableSchema) AllColumns() []string {
	return []string{a.ID, a.TenantID, a.AggregateType, a.AggregateID, a.Version, a.Type, a.Class, a.TransactionTime, a.ValidTime, a.FromMigration, a.Data}
}

var AggregateEventTable = AggregatePersistentEventsTableSchema{
	Name:            "aggregates_events",
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
