package mapper

import (
	"example/core/port/readmodel/item"
	"example/infrastructure/persistence/postgres/tables"
	"fmt"
	"github.com/global-soft-ba/go-eventstore"
	"github.com/global-soft-ba/go-eventstore/transactor/postgres/queries/pagination"
)

func ToItemDTOs(rows []tables.ItemRow) []item.DTO {
	var dtos []item.DTO
	for _, row := range rows {
		dtos = append(dtos, ToItemDTO(row))
	}
	return dtos
}

func ToItemDTO(row tables.ItemRow) item.DTO {
	return item.DTO{
		TenantID: row.TenantID,
		ItemID:   row.ItemID,
		ItemName: row.ItemName,
	}
}

func ToPageCursor(dto event.PageDTO) (pagination.PageCursor, error) {
	if dto.PageSize == 0 {
		return pagination.PageCursor{}, nil
	}

	sortColumns, err := sortFieldsToColumNames(dto.SortFields)
	if err != nil {
		return pagination.PageCursor{}, fmt.Errorf("could not map sort fields: %w", err)
	}
	return pagination.NewPageCursor(dto.PageSize, tables.ItemsTable.ID, sortColumns, dto.Values, dto.IsBackward)
}

func sortFieldsToColumNames(fields []event.SortField) ([]pagination.SortField, error) {
	var out []pagination.SortField

	for _, field := range fields {
		switch field.Name {
		case item.SortID:
			out = append(out, pagination.SortField{Name: tables.ItemsTable.ID, Desc: field.IsDesc})
		case item.SortItemID:
			out = append(out, pagination.SortField{Name: tables.ItemsTable.ItemID, Desc: field.IsDesc})
		case item.SortItemName:
			out = append(out, pagination.SortField{Name: tables.ItemsTable.ItemName, Desc: field.IsDesc})
		default:
			return nil, fmt.Errorf("unknown sort field to column maping for items: %s", field.Name)
		}
	}

	return out, nil
}

func ToForwardBackwardCursors(dto event.PageDTO, rows []tables.ItemRow) (event.PageDTO, event.PageDTO) {
	if len(rows) == 0 || dto.PageSize == 0 {
		return event.PageDTO{}, event.PageDTO{}
	}
	previousPage := dto
	previousPage.Values = mapRowToCursorValue(rows[0], dto.SortFields)
	previousPage.IsBackward = true
	nextPage := dto
	nextPage.Values = mapRowToCursorValue(rows[len(rows)-1], dto.SortFields)
	nextPage.IsBackward = false
	return previousPage, nextPage
}

func mapRowToCursorValue(row tables.ItemRow, fields []event.SortField) (values []interface{}) {
	for _, field := range fields {
		switch field.Name {
		case item.SortID:
			values = append(values, row.ID)
		case item.SortItemID:
			values = append(values, row.ItemID)
		case item.SortItemName:
			values = append(values, row.ItemName)
		default:
			continue
		}
	}
	//add internal id
	values = append(values, row.ID)
	return values
}
