package postgres

import (
	"context"
	itemRead "example/core/port/readmodel/item"
	"example/infrastructure/persistence/postgres/tables"
	"example/infrastructure/readmodel/item/postgres/mapper"
	"example/infrastructure/readmodel/item/postgres/queries"
	"fmt"
	sq "github.com/Masterminds/squirrel"
	"github.com/georgysavva/scany/v2/pgxscan"
	"github.com/global-soft-ba/go-eventstore"
	"github.com/global-soft-ba/go-eventstore/instrumentation/port/logger"
	"github.com/global-soft-ba/go-eventstore/instrumentation/port/metrics"
	transactorPort "github.com/global-soft-ba/go-eventstore/transactor"
	"github.com/global-soft-ba/go-eventstore/transactor/postgres/transactor"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Adapter struct {
	trans transactorPort.Port
	sql   queries.Builder
}

func NewAdapter(_ context.Context, pool *pgxpool.Pool) *Adapter {
	out := &Adapter{
		trans: transactor.NewTransactor(pool),
		sql:   queries.NewSqlBuilder(tables.DatabaseSchema, sq.Dollar),
	}
	return out
}

func (a *Adapter) GetTx(ctx context.Context) (transactor.DBTX, error) {
	valAny, err := a.trans.GetTransaction(ctx)
	if err != nil {
		return nil, err
	}

	tx, ok := valAny.(transactor.DBTX)
	if !ok {
		return nil, fmt.Errorf("could not get DBTX: wrong type %T", valAny)
	}

	return tx, nil
}

func (a *Adapter) FindItems(ctx context.Context, tenantID string, cursor event.PageDTO) ([]itemRead.DTO, event.PagesDTO, error) {
	ctx, endSpan := metrics.StartSpan(ctx, "find-items(postgres)", map[string]interface{}{"tenantID": tenantID})
	defer endSpan()

	if tenantID == "" {
		return nil, event.PagesDTO{}, fmt.Errorf("tenantID must not be empty")
	}

	if cursor.PageSize > itemRead.MaxPageSize {
		return nil, event.PagesDTO{}, fmt.Errorf("page size must not be greater than %d", itemRead.MaxPageSize)
	}

	var rows []tables.ItemRow
	errTx := a.trans.ExecWithoutTransaction(ctx, func(txCtx context.Context) error {
		p, err := mapper.ToPageCursor(cursor)
		if err != nil {
			return err
		}
		stmt, args, err := a.sql.FindItems(tenantID, p, cursor.SearchFields)
		if err != nil {
			return err
		}
		tx, err := a.GetTx(txCtx)
		if err != nil {
			return err
		}
		err = pgxscan.Select(txCtx, tx, &rows, stmt, args...)
		if err != nil {
			return err
		}
		return nil
	})
	if errTx != nil {
		return nil, event.PagesDTO{}, fmt.Errorf("could not get items for tenant %q: %w", tenantID, errTx)
	}
	previous, next := mapper.ToForwardBackwardCursors(cursor, rows)

	logger.Info("Retrieving items from postgres projection: %d items found", len(rows))

	return mapper.ToItemDTOs(rows), event.PagesDTO{Previous: previous, Next: next}, nil
}

func (a *Adapter) FindItemByID(ctx context.Context, tenantID, itemID string) (itemRead.DTO, error) {
	ctx, endSpan := metrics.StartSpan(ctx, "find-item-by-id(postgres)", map[string]interface{}{"tenantID": tenantID, "itemID": itemID})
	defer endSpan()

	if tenantID == "" || itemID == "" {
		return itemRead.DTO{}, fmt.Errorf("tenantID and itemID must not be empty")
	}

	var row tables.ItemRow
	errTx := a.trans.ExecWithoutTransaction(ctx, func(txCtx context.Context) error {
		stmt, args, err := a.sql.FindItemByID(tenantID, itemID)
		if err != nil {
			return err
		}
		tx, err := a.GetTx(txCtx)
		if err != nil {
			return err
		}
		err = pgxscan.Get(txCtx, tx, &row, stmt, args...)
		if err != nil {
			return fmt.Errorf("could not find item by ID: %w", err)
		}
		return nil
	})
	if errTx != nil {
		return itemRead.DTO{}, fmt.Errorf("could not get item for tenant %q and itemID %q: %w", tenantID, itemID, errTx)
	}

	logger.Info("Retrieving item from postgres projection: item found with ID %q", itemID)

	return mapper.ToItemDTO(row), nil
}
