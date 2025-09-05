package mapper

import (
	"fmt"
	"github.com/global-soft-ba/go-eventstore"
	"github.com/global-soft-ba/go-eventstore/eventstore/infrastructure/persistence/postgres/internal/pagination"
	"github.com/global-soft-ba/go-eventstore/eventstore/infrastructure/persistence/postgres/internal/tables"
)

func ToPageCursor(IdFieldName string, dto event.PageDTO) (pagination.PageCursor, error) {

	sortColumns, err := sortFieldsToTableAndColumNames(dto.SortFields)
	if err != nil {
		return pagination.PageCursor{}, fmt.Errorf("could not map sort fields: %w", err)
	}

	return pagination.NewPageCursor(dto.PageSize, IdFieldName, sortColumns, dto.Values, dto.IsBackward)

}

func sortFieldsToTableAndColumNames(fields []event.SortField) ([]pagination.SortField, error) {
	var out []pagination.SortField

	for _, field := range fields {
		switch field.Name {
		case event.SortAggregateID:
			out = append(out, pagination.SortField{Name: tables.AggregateEventTable.AggregateID, Desc: field.IsDesc})
		case event.SortAggregateType:
			out = append(out, pagination.SortField{Name: tables.AggregateEventTable.AggregateType, Desc: field.IsDesc})
		case event.SortAggregateVersion:
			out = append(out, pagination.SortField{Name: tables.AggregateEventTable.Version, Desc: field.IsDesc})
		case event.SortAggregateEventType:
			out = append(out, pagination.SortField{Name: tables.AggregateEventTable.Type, Desc: field.IsDesc})
		case event.SortAggregateClass:
			out = append(out, pagination.SortField{Name: tables.AggregateEventTable.Class, Desc: field.IsDesc})
		case event.SortValidTime:
			out = append(out, pagination.SortField{Name: tables.AggregateEventTable.ValidTime, Desc: field.IsDesc})
		case event.SortTransactionTime:
			out = append(out, pagination.SortField{Name: tables.AggregateEventTable.TransactionTime, Desc: field.IsDesc})
		default:
			return nil, fmt.Errorf("unknown sort field to column maping for field: %s", field.Name)
		}
	}

	return out, nil
}

func ToNewPage(page event.PageDTO, newValues []any, backward bool) event.PageDTO {
	if len(newValues) == 0 || page.PageSize == 0 {
		return event.PageDTO{}
	}

	newPage := page
	newPage.Values = newValues
	newPage.IsBackward = backward

	return newPage
}

func MapAggregateEventRowToSortValues(row tables.AggregateEventRow, fields []event.SortField) (values []any) {
	for _, field := range fields {
		switch field.Name {
		case event.SortAggregateType:
			values = append(values, row.AggregateType)
		case event.SortAggregateID:
			values = append(values, row.AggregateID)
		case event.SortAggregateVersion:
			values = append(values, row.Version)
		case event.SortAggregateEventType:
			values = append(values, row.Type)
		case event.SortAggregateClass:
			values = append(values, row.Class)
		case event.SortValidTime:
			values = append(values, row.ValidTime)
		case event.SortTransactionTime:
			values = append(values, row.TransactionTime)
		default:
			continue
		}
	}

	//add internal id
	values = append(values, row.ID)

	return values
}
