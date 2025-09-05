package mapper

import (
	"github.com/global-soft-ba/go-eventstore"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/port/persistence/projection"
	"github.com/global-soft-ba/go-eventstore/eventstore/infrastructure/persistence/postgres/internal/tables"
)

func ToProjections(rows ...tables.ProjectionsRow) []projection.DTO {
	var result []projection.DTO
	for _, row := range rows {
		result = append(result, projection.DTO{
			TenantID:     row.TenantID,
			ProjectionID: row.ProjectionID,
			State:        row.State,
			UpdatedAt:    row.UpdatedAt,
			Events:       nil,
		})
	}

	return result
}

func ToProjectionsEventRows(projections ...projection.DTO) []tables.ProjectionsEventRow {
	var result []tables.ProjectionsEventRow
	for _, proj := range projections {
		for _, evt := range proj.Events {
			result = append(result, ToProjectionsEventRow(proj.ProjectionID, evt))
		}
	}

	return result
}

func ToProjectionsEventRow(projectionID string, row event.PersistenceEvent) tables.ProjectionsEventRow {
	return tables.ProjectionsEventRow{
		AggregateEventRow: ToAggregateEventRow(row),
		ProjectionID:      projectionID,
	}
}

func ToProjection(sinceTimeStamp int64, rows ...tables.ProjectionsEventsLoadRow) projection.DTO {
	var (
		result                  projection.DTO
		latestExecutedValidTime int64
	)
	for _, row := range rows {
		if row.ValidTime < sinceTimeStamp && row.ValidTime > latestExecutedValidTime {
			latestExecutedValidTime = row.ValidTime
		}
		result.ProjectionID = row.ProjectionID
		result.TenantID = row.TenantID
		result.State = row.State
		result.UpdatedAt = MapToTimeStampTZ(latestExecutedValidTime)
		result.Events = append(result.Events, ToPersistenceEvent(row.AggregateEventRow))
	}

	return result
}

func ProjectionEventRowsToArrayOfValues(rows ...tables.ProjectionsEventRow) []interface{} {
	//order of columns must be same as in function AllColumns()
	var result []interface{}
	for _, row := range rows {
		result = append(result,
			row.ProjectionID,
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
