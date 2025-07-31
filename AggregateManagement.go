package event

import (
	"context"
	"time"
)

type AggregateState struct {
	TenantID            string
	AggregateType       string
	AggregateID         string
	CurrentVersion      int64
	LastTransactionTime time.Time
	LatestValidTime     time.Time
	CreateTime          time.Time
	CloseTime           time.Time
}

type AggregateManagement interface {
	// GetAggregatesEvents returns all events paginated  defined by the page object, i.e., it searchs and sorts fields
	GetAggregatesEvents(ctx context.Context, tenantID string, page PageDTO) ([]PersistenceEvent, PagesDTO, error)

	GetAggregateState(ctx context.Context, tenantID, aggregateType, aggregateID string) (AggregateState, error)
	GetAggregateStatesForAggregateType(ctx context.Context, tenantID string, aggregateType string) ([]AggregateState, error)
	GetAggregateStatesForAggregateTypeTill(ctx context.Context, tenantID string, aggregateType string, until time.Time) ([]AggregateState, error)
}
