package item

import (
	"context"
	"errors"
	itemModel "example/core/domain/model/item"
	writeModel "example/core/port/writemodel/item"
	"fmt"
	"github.com/global-soft-ba/go-eventstore"
	helper "github.com/global-soft-ba/go-eventstore/helper"
	"github.com/global-soft-ba/go-eventstore/instrumentation/port/logger"
	"github.com/global-soft-ba/go-eventstore/instrumentation/port/metrics"
	"github.com/global-soft-ba/go-eventstore/transactor"
)

type Projection struct {
	chunkSize   int
	persistence writeModel.Port
}

func NewItemProjection(ctx context.Context, chunkSize int, persistence writeModel.Port) *Projection {
	ctx, endSpan := metrics.StartSpan(ctx, "init-item-projection", nil)
	defer endSpan()

	return &Projection{persistence: persistence, chunkSize: chunkSize}
}

func (p *Projection) ID() string {
	return "item"
}

func (p *Projection) EventTypes() []string {
	return []string{
		event.EventType(itemModel.Created{}),
		event.EventType(itemModel.Deleted{}),
		event.EventType(itemModel.Renamed{}),
	}
}

func (p *Projection) ChunkSize() int {
	return p.chunkSize
}

func (p *Projection) Execute(ctx context.Context, events []event.IEvent) (err error) {
	ctx, endSpan := metrics.StartSpan(ctx, "execute-item-projection", map[string]interface{}{"numberOfEvents": len(events)})
	defer endSpan()

	eventsPerTenant, _ := helper.PartitionPerTenant(events)
	for tenantID, eventsOfTenant := range eventsPerTenant {
		err = p.execute(ctx, tenantID, eventsOfTenant)
		var tenantError *transactor.ErrorTenantWasNotCreated
		if err != nil {
			if errors.As(err, &tenantError) {
				return p.execute(ctx, tenantID, eventsOfTenant)
			}
			return err
		}
	}
	return nil
}

func (p *Projection) execute(ctx context.Context, tenantID string, eventsOfTenant []event.IEvent) (err error) {
	errTx := p.persistence.ExecWithinTransaction(ctx, tenantID, func(txCtx context.Context) error {
		txCtx, endSpan := metrics.StartSpan(txCtx, "execute-item-projection-within-transaction", map[string]interface{}{"tenantID": tenantID, "numberOfEvents": len(eventsOfTenant)})
		defer endSpan()
		for _, iEvent := range eventsOfTenant {
			processErr := p.processEvent(txCtx, iEvent)
			if processErr != nil {
				return fmt.Errorf("could not process event %q: %w", event.EventType(iEvent), processErr)
			}
		}
		return nil
	})
	if errTx != nil {
		return fmt.Errorf("could not execute projection %q of tenant %s: %w", p.ID(), tenantID, errTx)
	}

	logger.Info("Projection %q executed successfully for tenant %q with %d events", p.ID(), tenantID, len(eventsOfTenant))
	return nil
}

func (p *Projection) processEvent(ctx context.Context, evt event.IEvent) error {
	switch event.EventType(evt) {
	case event.EventType(itemModel.Created{}):
		return p.handleItemCreated(ctx, evt.(*itemModel.Created))
	case event.EventType(itemModel.Deleted{}):
		return p.handleItemDeleted(ctx, evt.(*itemModel.Deleted))
	case event.EventType(itemModel.Renamed{}):
		return p.handleItemRenamed(ctx, evt.(*itemModel.Renamed))
	}
	return fmt.Errorf("unsupported event type %q in projection %q", event.EventType(evt), p.ID())
}

func (p *Projection) handleItemCreated(ctx context.Context, evt *itemModel.Created) error {
	return p.persistence.CreateItem(ctx, writeModel.DTO{
		TenantID: evt.GetTenantID(),
		ItemID:   evt.GetAggregateID(),
	})
}

func (p *Projection) handleItemDeleted(ctx context.Context, evt *itemModel.Deleted) error {
	return p.persistence.DeleteItem(ctx, evt.GetTenantID(), evt.GetAggregateID())
}

func (p *Projection) handleItemRenamed(ctx context.Context, evt *itemModel.Renamed) error {
	return p.persistence.RenameItem(ctx, evt.GetTenantID(), evt.GetAggregateID(), evt.Name)
}

func (p *Projection) PrepareRebuild(ctx context.Context, tenantID string) error {
	ctx, endSpan := metrics.StartSpan(ctx, "prepare-rebuild-of-item-projection", map[string]interface{}{"tenantID": tenantID})
	defer endSpan()
	err := p.persistence.PrepareRebuild(ctx, tenantID)
	if err != nil {
		return fmt.Errorf("could not prepare rebuild of %q projection: %w", p.ID(), err)
	}

	logger.Info("Prepared rebuild of projection %q for tenant %q", p.ID(), tenantID)
	return nil
}

func (p *Projection) FinishRebuild(ctx context.Context, tenantID string) error {
	ctx, endSpan := metrics.StartSpan(ctx, "finish-rebuild-of-item-projection", map[string]interface{}{"tenantID": tenantID})
	defer endSpan()
	err := p.persistence.FinishRebuild(ctx, tenantID)
	if err != nil {
		return fmt.Errorf("could not finish rebuild of %q projection: %w", p.ID(), err)
	}

	logger.Info("Finished rebuild of projection %q for tenant %q", p.ID(), tenantID)
	return nil
}
