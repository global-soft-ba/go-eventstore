package postgres

import (
	"context"
	iProj "example/core/application/projection/item"
	wModelPort "example/core/port/writemodel/item"
	"example/infrastructure/persistence/postgres/tables"
	"example/infrastructure/writemodel/item/postgres/queries"
	"example/testdata"
	"fmt"
	sq "github.com/Masterminds/squirrel"
	"github.com/global-soft-ba/go-eventstore"
	baseAdapter "github.com/global-soft-ba/go-eventstore/transactor/postgres"
	"github.com/global-soft-ba/go-eventstore/transactor/postgres/transactorForTest"
	"github.com/jackc/pgx/v5/pgxpool"
	"testing"
)

func NewAdapterWithWriterForTest(db *pgxpool.Pool) (wModelPort.Port, testdata.IWriter[event.IEvent, string]) {
	adp, err := NewAdapter(context.Background(), db)
	if err != nil {
		panic(fmt.Errorf("error creating adapter: %w", err))
	}
	return adp, writer[event.IEvent, string]{
		adp:  adp,
		proj: iProj.NewItemProjection(context.Background(), 100, adp)}
}

func NewTxPassThroughAdapterWithWriterForTest() (wModelPort.Port, testdata.IWriter[event.IEvent, string]) {
	builder, err := queries.NewSqlBuilder(tables.DatabaseSchema, sq.Dollar)
	if err != nil {
		panic(fmt.Errorf("error in NewTxStoredAdapterWithWriterForTest: %w", err))
	}
	a := baseAdapter.NewAdapterFromTransactorAndBuilder(transactorForTest.NewTxPassThroughTransactor(), builder.Builder)
	adp := &adapter{a, builder}
	return adp, writer[event.IEvent, string]{adp: adp, proj: iProj.NewItemProjection(context.Background(), 100, adp)}
}

type writer[T event.IEvent, V string] struct {
	adp  wModelPort.Port
	proj *iProj.Projection
}

func (s writer[T, V]) Write(t *testing.T, txCtx context.Context, data ...event.IEvent) error {
	err := s.proj.Execute(txCtx, s.filterEventType(data, s.proj.EventTypes()))
	if err != nil {
		return fmt.Errorf("error in writer: %w", err)
	}
	return err
}

func (s writer[T, V]) filterEventType(data []event.IEvent, eventTypes []string) []event.IEvent {
	var events []event.IEvent
	for _, evt := range data {
		for _, eventType := range eventTypes {
			if event.EventType(evt) == eventType {
				events = append(events, evt)
			}
		}
	}
	return events
}

func (s writer[T, V]) Execute(t *testing.T, ctx context.Context, executeFunc string, data ...string) error {
	switch executeFunc {
	case "PrepareRebuild":
		return s.proj.PrepareRebuild(ctx, data[0])
	case "FinishRebuild":
		return s.proj.FinishRebuild(ctx, data[0])
	case "CreateTenant":
		return s.adp.(*adapter).CreateNewTenantIfNeededIntern(ctx, data[0], tables.ItemTableName)
	case "DropTenant":
		return s.adp.(*adapter).DropTenantIntern(ctx, data[0], tables.ItemTableName)
	default:
		return fmt.Errorf("executeFunc %s not found", executeFunc)
	}
}
