package item

import (
	"context"
)

type Port interface {
	ExecWithinTransaction(ctx context.Context, tenantID string, tFunc func(ctx context.Context) error, options ...func(opt any) error) error

	CreateItem(txCtx context.Context, dto DTO) error
	RenameItem(txCtx context.Context, tenantID, itemID, name string) error
	DeleteItem(txCtx context.Context, tenantID, itemID string) error

	PrepareRebuild(txCtx context.Context, tenantID string) error
	FinishRebuild(txCtx context.Context, tenantID string) error
}

type DTO struct {
	TenantID string
	ItemID   string
	ItemName string
}
