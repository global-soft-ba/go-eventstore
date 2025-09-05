package repository

import (
	"context"
	domainModel "example/core/domain/model/item"
)

type IWriteRepository interface {
	SaveItem(ctx context.Context, item domainModel.Item) error
	SaveItems(ctx context.Context, items ...domainModel.Item) error
}
