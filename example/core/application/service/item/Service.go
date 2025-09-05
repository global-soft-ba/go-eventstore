package item

import (
	"context"
	"example/core/application/repository"
	"example/core/application/service/item/mapper"
	"example/core/domain/model/item"
	readModel "example/core/port/readmodel/item"
	service "example/core/port/service/item"
	"fmt"
	"github.com/global-soft-ba/go-eventstore"
	"github.com/global-soft-ba/go-eventstore/instrumentation/port/logger"
	"github.com/global-soft-ba/go-eventstore/instrumentation/port/metrics"
	"time"
)

type Service struct {
	readPort  repository.IReadRepository
	writePort repository.IWriteRepository

	readModel readModel.Port
}

func NewItemService(rp repository.IReadRepository, wp repository.IWriteRepository, rm readModel.Port) *Service {
	return &Service{
		readPort:  rp,
		writePort: wp,
		readModel: rm,
	}
}

func (s *Service) CreateItem(ctx context.Context, dto service.DTO) error {
	ctx, endSpan := metrics.StartSpan(ctx, "create-item", map[string]interface{}{"tenantID": dto.TenantID, "itemID": dto.ItemID})
	defer endSpan()

	i, err := item.CreateItem(dto.ItemID, dto.TenantID)
	if err != nil {
		return fmt.Errorf("could not create item %q: %w", dto.ItemID, err)
	}
	i, err = i.Rename(dto.ItemName, time.Time{})
	if err != nil {
		return fmt.Errorf("could not create item %q: %w", dto.ItemID, err)
	}
	err = s.writePort.SaveItem(ctx, i)
	if err != nil {
		return fmt.Errorf("could not create item %q: %w", dto.ItemID, err)
	}

	logger.Info("Item created successfully: %+v", i)
	return nil
}

func (s *Service) FindItems(ctx context.Context, tenantID string, page event.PageDTO) ([]service.DTO, event.PagesDTO, error) {
	ctx, endSpan := metrics.StartSpan(ctx, "find-items", map[string]interface{}{"tenantID": tenantID})
	defer endSpan()

	items, pagesDTO, err := s.readModel.FindItems(ctx, tenantID, page)
	if err != nil {
		return nil, event.PagesDTO{}, err
	}

	logger.Info("Items found successfully: %d", len(items))

	return mapper.FromReadModelToServiceDTOs(items), pagesDTO, nil
}

func (s *Service) FindItemByID(ctx context.Context, tenantID, itemID string) (service.DTO, error) {
	ctx, endSpan := metrics.StartSpan(ctx, "find-item-by-id", map[string]interface{}{"tenantID": tenantID, "itemID": itemID})
	defer endSpan()

	i, err := s.readModel.FindItemByID(ctx, tenantID, itemID)
	if err != nil {
		return service.DTO{}, err
	}

	logger.Info("Item found successfully: %+v", i)

	return mapper.FromReadModelToServiceDTO(i), nil
}

func (s *Service) RenameItem(ctx context.Context, tenantID, itemID, name string, start time.Time) error {
	ctx, endSpan := metrics.StartSpan(ctx, "rename-item", map[string]interface{}{"tenantID": tenantID, "itemID": itemID, "start": start})
	defer endSpan()

	i, err := s.readPort.GetItem(ctx, tenantID, itemID, start)
	if err != nil {
		return fmt.Errorf("could not rename item %q: %w", itemID, err)
	}
	i, err = i.Rename(name, start)
	if err != nil {
		return fmt.Errorf("could not rename item %q: %w", itemID, err)
	}
	err = s.writePort.SaveItem(ctx, i)
	if err != nil {
		return fmt.Errorf("could not rename item %q: %w", itemID, err)
	}

	logger.Info("Item renamed successfully: %+v", i)
	return nil
}

func (s *Service) DeleteItem(ctx context.Context, tenantID, itemID string) error {
	ctx, endSpan := metrics.StartSpan(ctx, "delete-item", map[string]interface{}{"tenantID": tenantID, "itemID": itemID})
	defer endSpan()

	i, err := s.readPort.GetItem(ctx, tenantID, itemID, time.Time{})
	if err != nil {
		return fmt.Errorf("could not delete item %q: %w", itemID, err)
	}
	i, err = i.Delete()
	if err != nil {
		return fmt.Errorf("could not delete item %q: %w", itemID, err)
	}
	err = s.writePort.SaveItem(ctx, i)
	if err != nil {
		return fmt.Errorf("could not delete item %q: %w", itemID, err)
	}

	logger.Info("Item deleted successfully: %+v", i)
	return nil
}
