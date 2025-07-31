package postgres

import (
	"context"
	"example/core/port/writemodel/item"
	"example/infrastructure/persistence/postgres/tables"
	"example/infrastructure/writemodel/item/postgres/mapper"
	"example/infrastructure/writemodel/item/postgres/queries"
	"fmt"
	sq "github.com/Masterminds/squirrel"
	"github.com/global-soft-ba/go-eventstore/instrumentation/port/logger"
	"github.com/global-soft-ba/go-eventstore/instrumentation/port/metrics"
	transPort "github.com/global-soft-ba/go-eventstore/transactor"
	baseAdapter "github.com/global-soft-ba/go-eventstore/transactor/postgres"
	"github.com/global-soft-ba/go-eventstore/transactor/postgres/transactor"
	"github.com/jackc/pgx/v5/pgxpool"
)

type adapter struct {
	*baseAdapter.Adapter
	sql *queries.Builder
}

func NewAdapter(_ context.Context, db *pgxpool.Pool) (item.Port, error) {
	builder, err := queries.NewSqlBuilder(tables.DatabaseSchema, sq.Dollar)
	if err != nil {
		return nil, fmt.Errorf("could not create sql builder: %w", err)
	}
	a := baseAdapter.NewAdapterFromTransactorAndBuilder(transactor.NewTransactor(db), builder.Builder)
	return &adapter{a, builder}, nil
}

func (a *adapter) executeSqlQueryInTx(txCtx context.Context, stmt string, args []interface{}) error {
	tx, err := a.GetTx(txCtx)
	if err != nil {
		return fmt.Errorf("could not execute sql query in tx: %w", err)
	}

	_, err = tx.Exec(txCtx, stmt, args...)
	if err != nil {
		return fmt.Errorf("could not execute sql query in tx: %w", err)
	}
	return nil
}

func (a *adapter) CreateItem(txCtx context.Context, dto item.DTO) error {
	txCtx, endSpan := metrics.StartSpan(txCtx, "create-item(postgres)", map[string]interface{}{"tenantID": dto.TenantID, "itemID": dto.ItemID})
	defer endSpan()

	stmt, args, err := a.sql.CreateItem(mapper.ToItemRow(dto))
	if err != nil {
		return fmt.Errorf("could not create item: %w", err)
	}
	err = a.executeSqlQueryInTx(txCtx, stmt, args)
	if err != nil {
		subCTX := context.WithValue(txCtx, transactor.CtxStorageKey, nil)
		errTenant := a.CreateNewTenantIfNeededIntern(subCTX, dto.TenantID, tables.ItemTableName)
		if errTenant != nil {
			return fmt.Errorf("could not create item, unable to create new tenant: %w", err)
		}
		return transPort.NewErrorTenantWasNotCreated(dto.TenantID, err)
	}

	logger.Info("Item created successfully in postgres projection for tenantID: %q, itemID: %q", dto.TenantID, dto.ItemID)
	return nil
}

func (a *adapter) DeleteItem(txCtx context.Context, tenantID, itemID string) error {
	txCtx, endSpan := metrics.StartSpan(txCtx, "delete-item(postgres)", map[string]interface{}{"tenantID": tenantID, "itemID": itemID})
	defer endSpan()

	stmt, args, err := a.sql.DeleteItem(tenantID, itemID)
	if err != nil {
		return fmt.Errorf("could not delete item: %w", err)
	}
	err = a.executeSqlQueryInTx(txCtx, stmt, args)
	if err != nil {
		return fmt.Errorf("could not delete item: %w", err)
	}

	logger.Info("Item deleted successfully in postgres projection for tenantID %q, itemID %q", tenantID, itemID)
	return nil
}

func (a *adapter) RenameItem(txCtx context.Context, tenantID, itemID, name string) error {
	txCtx, endSpan := metrics.StartSpan(txCtx, "rename-item(postgres)", map[string]interface{}{"tenantID": tenantID, "itemID": itemID})
	defer endSpan()

	stmt, args, err := a.sql.RenameItem(tenantID, itemID, name)
	if err != nil {
		return fmt.Errorf("could not rename item: %w", err)
	}
	err = a.executeSqlQueryInTx(txCtx, stmt, args)
	if err != nil {
		return fmt.Errorf("could not rename item: %w", err)
	}

	logger.Info("Item renamed successfully in postgres projection for tenantID %q, itemID %q", tenantID, itemID)
	return nil
}

func (a *adapter) PrepareRebuild(txCtx context.Context, tenantID string) error {
	err := a.PrepareRebuildIntern(txCtx, tenantID, tables.ItemTableName)
	if err != nil {
		return fmt.Errorf("could not prepare rebuild of item postgres projection: %w", err)
	}

	logger.Info("Prepared rebuild of item postgres projection for tenantID %q", tenantID)
	return nil
}

func (a *adapter) FinishRebuild(txCtx context.Context, tenantID string) error {
	err := a.FinishRebuildIntern(txCtx, tenantID, tables.ItemTableName)
	if err != nil {
		return fmt.Errorf("could not finish rebuild of item postgres projection: %w", err)
	}

	logger.Info("Finished rebuild of item postgres projection for tenantID %q", tenantID)
	return nil
}
