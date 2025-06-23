package tables

var AggregateTable = AggregateTableSchema{
	Name:                "aggregates",
	TenantID:            "tenant_id",
	AggregateType:       "aggregate_type",
	AggregateID:         "aggregate_id",
	CurrentVersion:      "current_version",
	LastTransactionTime: "last_transaction_time",
	LatestValidTime:     "latest_valid_time",
	CreateTime:          "create_time",
	CloseTime:           "close_time",
}

type AggregateTableSchema struct {
	Name                string
	TenantID            string
	AggregateType       string
	AggregateID         string
	CurrentVersion      string
	LastTransactionTime string
	LatestValidTime     string
	CreateTime          string
	CloseTime           string
}

// AllColumns if you change order of columns you must adjust the function ...ToArrayOfValues in mapper as well.
func (a AggregateTableSchema) AllColumns() []string {
	return []string{a.TenantID, a.AggregateType, a.AggregateID, a.CurrentVersion, a.LastTransactionTime, a.LatestValidTime, a.CreateTime, a.CloseTime}
}

type AggregateRow struct {
	TenantID            string `db:"tenant_id"`
	AggregateType       string `db:"aggregate_type"`
	AggregateID         string `db:"aggregate_id"`
	CurrentVersion      int64  `db:"current_version"`
	LastTransactionTime int64  `db:"last_transaction_time"`
	LatestValidTime     int64  `db:"latest_valid_time"`
	CreateTime          int64  `db:"create_time"`
	CloseTime           int64  `db:"close_time"`
}
