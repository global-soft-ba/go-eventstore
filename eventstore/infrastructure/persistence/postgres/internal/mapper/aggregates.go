package mapper

import (
	"github.com/global-soft-ba/go-eventstore"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/port/persistence/aggregate"
	"github.com/global-soft-ba/go-eventstore/eventstore/infrastructure/persistence/postgres/internal/tables"
)

func ToAggregateRows(rows ...aggregate.DTO) []tables.AggregateRow {
	var result []tables.AggregateRow
	for _, row := range rows {
		result = append(result, ToAggregateRow(row))
	}

	return result
}

func ToAggregateRow(row aggregate.DTO) tables.AggregateRow {
	return tables.AggregateRow{
		TenantID:            row.TenantID,
		AggregateType:       row.AggregateType,
		AggregateID:         row.AggregateID,
		CurrentVersion:      row.CurrentVersion,
		LastTransactionTime: MapToNanoseconds(row.LastTransactionTime),
		LatestValidTime:     MapToNanoseconds(row.LatestValidTime),
		CreateTime:          MapToNanoseconds(row.CreateTime),
		CloseTime:           MapToNanoseconds(row.CloseTime),
	}
}

func ToAggregate(rows ...tables.AggregateRow) []aggregate.DTO {
	var result []aggregate.DTO
	for _, row := range rows {
		result = append(result, toAggregate(row))
	}

	return result
}

func ToAggregateStates(rows []tables.AggregateRow) (states []event.AggregateState) {
	for _, row := range rows {
		states = append(states, event.AggregateState{
			TenantID:            row.TenantID,
			AggregateType:       row.AggregateType,
			AggregateID:         row.AggregateID,
			CurrentVersion:      row.CurrentVersion,
			LastTransactionTime: MapToTimeStampTZ(row.LastTransactionTime),
			LatestValidTime:     MapToTimeStampTZ(row.LatestValidTime),
			CreateTime:          MapToTimeStampTZ(row.CreateTime),
			CloseTime:           MapToTimeStampTZ(row.CloseTime),
		})

	}
	return states
}

func ToAggregateState(row tables.AggregateRow) event.AggregateState {
	return event.AggregateState{
		TenantID:            row.TenantID,
		AggregateType:       row.AggregateType,
		AggregateID:         row.AggregateID,
		CurrentVersion:      row.CurrentVersion,
		LastTransactionTime: MapToTimeStampTZ(row.LastTransactionTime),
		LatestValidTime:     MapToTimeStampTZ(row.LatestValidTime),
		CreateTime:          MapToTimeStampTZ(row.CreateTime),
		CloseTime:           MapToTimeStampTZ(row.CloseTime),
	}
}

func toAggregate(row tables.AggregateRow) aggregate.DTO {
	return aggregate.DTO{
		TenantID:            row.TenantID,
		AggregateType:       row.AggregateType,
		AggregateID:         row.AggregateID,
		CurrentVersion:      row.CurrentVersion,
		LastTransactionTime: MapToTimeStampTZ(row.LastTransactionTime),
		LatestValidTime:     MapToTimeStampTZ(row.LatestValidTime),
		CreateTime:          MapToTimeStampTZ(row.CreateTime),
		CloseTime:           MapToTimeStampTZ(row.CloseTime),
	}
}

func AggregateRowToArrayOfValues(rows ...tables.AggregateRow) []interface{} {
	//order of columns must be same as in function AllColumns()
	var result []interface{}
	for _, row := range rows {
		result = append(result,
			row.TenantID,
			row.AggregateType,
			row.AggregateID,
			row.CurrentVersion,
			row.LastTransactionTime,
			row.LatestValidTime,
			row.CreateTime,
			row.CloseTime,
		)
	}

	return result
}
