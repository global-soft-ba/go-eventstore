package repository

import (
	"context"
	domainModel "example/core/domain/model/item"
	"time"
)

type IReadRepository interface {
	GetAllItems(ctx context.Context, tenantID string) ([]domainModel.Item, error)
	GetItem(ctx context.Context, tenantID, itemID string, start time.Time) (domainModel.Item, error)
}
