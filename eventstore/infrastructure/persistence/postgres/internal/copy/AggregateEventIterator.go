package copy

import (
	"github.com/global-soft-ba/go-eventstore/eventstore/infrastructure/persistence/postgres/internal/tables"
	"github.com/jackc/pgx/v5"
)

func NewAggregateEventIterator(rows []tables.AggregateEventRow) pgx.CopyFromSource {
	return &AggregateEventIterator{
		rows: rows,
	}
}

// AggregateEventIterator implements pgx.CopyFromSource.
type AggregateEventIterator struct {
	rows                 []tables.AggregateEventRow
	skippedFirstNextCall bool
}

func (r *AggregateEventIterator) Next() bool {
	if len(r.rows) == 0 {
		return false
	}
	if !r.skippedFirstNextCall {
		r.skippedFirstNextCall = true
		return true
	}
	r.rows = r.rows[1:]
	return len(r.rows) > 0
}

func (r *AggregateEventIterator) Values() ([]interface{}, error) {
	//order of columns must be same as in create table
	return []interface{}{
		r.rows[0].ID,
		r.rows[0].TenantID,
		r.rows[0].AggregateType,
		r.rows[0].AggregateID,
		r.rows[0].Version,
		r.rows[0].Type,
		r.rows[0].Class,
		r.rows[0].TransactionTime,
		r.rows[0].ValidTime,
		r.rows[0].FromMigration,
		r.rows[0].Data,
	}, nil
}

func (r *AggregateEventIterator) Err() error {
	return nil
}
