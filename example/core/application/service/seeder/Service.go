package seeder

import (
	"context"
	"example/core/application/repository"
	"example/core/domain/model/item"
	"fmt"
	"github.com/global-soft-ba/go-eventstore/instrumentation/port/logger"
)

type Service struct {
	writePort repository.IWriteRepository
}

func NewSeederService(wp repository.IWriteRepository) *Service {
	return &Service{
		writePort: wp,
	}
}

func (s *Service) SeedItems(ctx context.Context, tenantID string, count int) error {
	items, err := item.SeedItems(tenantID, count)
	if err != nil {
		return fmt.Errorf("seeding Items error %w, abort seeding", err)
	}
	if err = s.writePort.SaveItems(ctx, items...); err != nil {
		return fmt.Errorf("seeding Items error %w, abort seeding", err)
	}

	logger.Info("Seeded %d items for tenant %q", len(items), tenantID)
	return nil
}
