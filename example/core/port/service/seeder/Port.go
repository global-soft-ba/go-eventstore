package seeder

import "context"

const (
	DefaultSeedItemsCount = "1000"
)

type Port interface {
	SeedItems(ctx context.Context, tenantID string, count int) error
}
