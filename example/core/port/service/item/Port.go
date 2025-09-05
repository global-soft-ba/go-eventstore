package item

import (
	"context"
	"github.com/global-soft-ba/go-eventstore"
	"time"
)

type Port interface {
	CreateItem(ctx context.Context, dto DTO) error
	FindItems(ctx context.Context, tenantID string, page event.PageDTO) ([]DTO, event.PagesDTO, error)
	FindItemByID(ctx context.Context, tenantID, itemID string) (DTO, error)
	RenameItem(ctx context.Context, tenantID, itemID, name string, start time.Time) error
	DeleteItem(ctx context.Context, tenantID, itemID string) error
}

type DTO struct {
	TenantID string `json:"tenantID"`
	ItemID   string `json:"itemID"`
	ItemName string `json:"itemName"`
}

type CreateDTO struct {
	ItemID   string `json:"itemID"`
	ItemName string `json:"itemName"`
}

type RenameDTO struct {
	Name string `json:"name"`
}
