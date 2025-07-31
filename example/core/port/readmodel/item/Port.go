package item

import (
	"context"
	"example/presentation/paginator"
	"github.com/global-soft-ba/go-eventstore"
)

const (
	MaxPageSize uint = 1000
)

const (
	SortID       string = "ID"
	SortItemID   string = "ItemID"
	SortItemName string = "ItemName"

	SortDefaultField = SortID
)

const (
	SearchItemID   string = "SearchItemID"
	SearchItemName string = "SearchItemName"
)

var (
	SortMap = map[string]string{
		"ID":       SortID,
		"ItemID":   SortItemID,
		"ItemName": SortItemName,
	}

	SearchMap = map[string]string{
		"ItemID":   SearchItemID,
		"ItemName": SearchItemName,
	}
)

type Port interface {
	FindItems(ctx context.Context, tenantID string, cursor event.PageDTO) ([]DTO, event.PagesDTO, error)
	FindItemByID(ctx context.Context, tenantID, itemID string) (DTO, error)
}

type DTO struct {
	TenantID string
	ItemID   string
	ItemName string
}

func FromDTO(page paginator.PageDTO) event.PageDTO {
	pageDTO := event.PageDTO{
		PageSize:   page.PageSize,
		Values:     page.Values,
		IsBackward: page.IsBackward,
	}
	for _, field := range page.SortFields {
		if field.Name == "name" {
			field.Name = SortDefaultField
			field.Desc = true
		}
		pageDTO.SortFields = append(pageDTO.SortFields, event.SortField{
			Name:   field.FieldID,
			IsDesc: field.Desc,
		})
	}
	for _, field := range page.SearchFields {
		pageDTO.SearchFields = append(pageDTO.SearchFields, event.SearchField{
			Name:     field.FieldID,
			Value:    field.Value,
			Operator: event.SearchOperator(field.Operator),
		})
	}
	return pageDTO
}
