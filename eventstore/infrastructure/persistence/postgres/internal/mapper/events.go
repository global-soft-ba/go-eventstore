package mapper

import (
	"github.com/global-soft-ba/go-eventstore"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/shared"
	"github.com/global-soft-ba/go-eventstore/eventstore/infrastructure/persistence/postgres/internal/tables"
)

func ToAggregateEventRows(rows ...event.PersistenceEvent) []tables.AggregateEventRow {
	var result []tables.AggregateEventRow
	for _, row := range rows {
		mappedRow := ToAggregateEventRow(row)
		result = append(result, mappedRow)
	}

	return result
}

func ToAggregateEventRow(row event.PersistenceEvent) tables.AggregateEventRow {

	return tables.AggregateEventRow{
		ID:              row.ID,
		AggregateID:     row.AggregateID,
		TenantID:        row.TenantID,
		AggregateType:   row.AggregateType,
		Version:         int64(row.Version),
		Type:            row.Type,
		Class:           string(row.Class),
		TransactionTime: MapToNanoseconds(row.TransactionTime),
		ValidTime:       MapToNanoseconds(row.ValidTime),
		FromMigration:   row.FromMigration,
		Data:            row.Data,
	}
}

func ToPersistenceEventArray(rows []tables.AggregateEventRow) []event.PersistenceEvent {
	var result []event.PersistenceEvent
	for _, row := range rows {
		result = append(result, ToPersistenceEvent(row))
	}
	return result
}

func ToPersistenceEvent(row tables.AggregateEventRow) event.PersistenceEvent {
	return event.PersistenceEvent{
		ID:              row.ID,
		AggregateID:     row.AggregateID,
		TenantID:        row.TenantID,
		AggregateType:   row.AggregateType,
		Version:         int(row.Version),
		Type:            row.Type,
		Class:           event.Class(row.Class),
		TransactionTime: MapToTimeStampTZ(row.TransactionTime),
		ValidTime:       MapToTimeStampTZ(row.ValidTime),
		FromMigration:   row.FromMigration,
		Data:            row.Data,
	}
}

func ToPersistenceEvents(rows []tables.AggregatePersistentEventLoadRow) ([]event.PersistenceEvents, error) {
	var result []event.PersistenceEvents
	var currentAggID shared.AggregateID
	var currentPEvent event.PersistenceEvents

	if len(rows) == 0 {
		return nil, nil
	}

	for id, row := range rows {
		if id == 0 || !currentAggID.Equal(shared.NewAggregateID(row.TenantID, row.AggregateType, row.AggregateID)) {
			if id > 0 {
				result = append(result, currentPEvent)
			}
			currentAggID = shared.NewAggregateID(row.TenantID, row.AggregateType, row.AggregateID)
			currentPEvent = event.PersistenceEvents{
				Events:  nil,
				Version: int(row.Version),
			}
		}

		if row.CurrentVersion > int64(currentPEvent.Version) {
			currentPEvent.Version = int(row.CurrentVersion)
		}
		currentPEvent.Events = append(currentPEvent.Events, ToPersistenceEvent(row.AggregateEventRow))
	}

	//last run in for loop
	result = append(result, currentPEvent)

	return result, nil
}

func AggregateEventRowToArrayOfValues(rows ...tables.AggregateEventRow) []interface{} {
	//order of columns must be same as in function AllColumns()
	var result []interface{}
	for _, row := range rows {
		result = append(result,
			row.ID,
			row.TenantID,
			row.AggregateType,
			row.AggregateID,
			row.Version,
			row.Type,
			row.Class,
			row.TransactionTime,
			row.ValidTime,
			row.FromMigration,
			row.Data,
		)
	}
	return result
}
