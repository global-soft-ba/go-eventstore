package postgres

import (
	"context"
	"fmt"
	sq "github.com/Masterminds/squirrel"
	"github.com/georgysavva/scany/v2/pgxscan"
	transPort "github.com/global-soft-ba/go-eventstore/transactor"
	queries "github.com/global-soft-ba/go-eventstore/transactor/postgres/queries/partition"
	"github.com/global-soft-ba/go-eventstore/transactor/postgres/transactor"
	"github.com/jackc/pgx/v5/pgxpool"
	"reflect"
	"slices"
	"strings"
)

type Adapter struct {
	transactor transPort.Port
	sql        *queries.Builder
}

func NewAdapterFromTransactorAndBuilder(transactor transPort.Port, builder *queries.Builder) *Adapter {
	return &Adapter{
		transactor: transactor,
		sql:        builder,
	}
}

func NewAdapter(_ context.Context, schema string, db *pgxpool.Pool) (*Adapter, error) {
	builder, err := queries.NewSqlBuilder(schema, sq.Dollar)
	if err != nil {
		return nil, fmt.Errorf("could not create sql builder: %w", err)
	}
	return &Adapter{
		transactor: transactor.NewTransactor(db),
		sql:        builder,
	}, nil
}

// transaction handler
func (a *Adapter) ExecWithinTransaction(ctx context.Context, _ string, tFunc func(ctx context.Context) error, options ...func(opt any) error) error {
	return a.transactor.ExecWithinTransaction(ctx, tFunc, options...)
}

func (a *Adapter) Transactor() transPort.Port {
	return a.transactor
}

func (a *Adapter) Builder() *queries.Builder {
	return a.sql
}

func (a *Adapter) GetTx(ctx context.Context) (transactor.DBTX, error) {
	dbtx, err := a.transactor.GetTransaction(ctx)
	if err != nil {
		return nil, err
	}
	result, ok := dbtx.(transactor.DBTX)
	if !ok {
		return nil, fmt.Errorf("could not find correct transaction type in context: found type %s", reflect.TypeOf(result).String())
	}
	return result, nil
}

// projection handler
func (a *Adapter) PrepareRebuildIntern(ctx context.Context, tenantID, parentTable string) error {
	return a.ExecWithinTransaction(ctx, tenantID, func(ctx context.Context) error {
		tx, err := a.GetTx(ctx)
		if err != nil {
			return err
		}
		if err = a.executeRebuildPreparation(ctx, tx, tenantID, parentTable); err != nil {
			return fmt.Errorf("could not execute rebuild preparation for %s: %w", parentTable, err)
		}
		return nil
	})
}

func (a *Adapter) executeRebuildPreparation(ctx context.Context, tx transactor.DBTX, tenantID, parentTable string) error {
	// What tables are already there
	tablesOfTenant, err := a.getAllTablesOfTenantForParentTable(ctx, tx, tenantID, parentTable)
	if err != nil {
		return fmt.Errorf("could not execute rebuild preparation for table %q: %w", parentTable, err)
	}
	// Create Tenant Table and Partition if not there yet
	if err = a.createNewTenantIfNeeded(ctx, tx, tenantID, parentTable); err != nil {
		return fmt.Errorf("could not execute rebuild preparation for table %q: %w", parentTable, err)
	}
	// (Optional) Rename tenant table to rebuild table if not there yet
	if !slices.Contains(tablesOfTenant, a.sql.GetTempTableName(tenantID, parentTable)) {
		if err = a.renameTenantTable(ctx, tx, tenantID, parentTable); err != nil {
			return fmt.Errorf("could not execute rebuild preparation for table %q: %w", parentTable, err)
		}
	}
	// Create rebuild table if not exists
	if err = a.createRebuildTable(ctx, tx, tenantID, parentTable); err != nil {
		return fmt.Errorf("could not execute rebuild preparation for table %q: %w", parentTable, err)
	}
	// Truncate rebuild table
	if err = a.truncateRebuildTable(ctx, tx, tenantID, parentTable); err != nil {
		return fmt.Errorf("could not execute rebuild preparation for table %q: %w", parentTable, err)
	}
	return nil
}

func (a *Adapter) getAllTablesOfTenantForParentTable(ctx context.Context, tx transactor.DBTX, tenantID, parentTable string) (t []string, err error) {
	stmt, val, err := a.sql.GetAllTablesOfTenant(tenantID, parentTable)
	if err != nil {
		return nil, fmt.Errorf("could not build sql statement for check if tenant tables exists: %w", err)
	}
	if err = pgxscan.Select(ctx, tx, &t, stmt, val...); err != nil {
		return nil, fmt.Errorf("could not execute sql statement for check tenant tables exists: %w", err)
	}
	return t, nil
}

func (a *Adapter) CreateNewTenantIfNeededIntern(ctx context.Context, tenantID, parentTable string) error {
	return a.transactor.ExecWithinTransaction(ctx, func(txCtx context.Context) error {
		tx, err := a.GetTx(txCtx)
		if err != nil {
			return fmt.Errorf("could not get transactor: %w", err)
		}
		err = a.createNewTenantIfNeeded(txCtx, tx, tenantID, parentTable)
		if err != nil {
			return fmt.Errorf("could not execute sql statement for create agrement: %w", err)
		}
		return nil
	})
}

func (a *Adapter) createNewTenantIfNeeded(ctx context.Context, tx transactor.DBTX, tenantID, parentTable string) error {
	// query 1: What partitions are already there
	partitionsOfTenant, err := a.getAllPartitionsOfTenantForParentTable(ctx, tx, tenantID, parentTable)
	if err != nil {
		return fmt.Errorf("could not execute rebuild preparation for table %q: %w", parentTable, err)
	}
	// query 2: create tenant
	if err = a.createTenantTable(ctx, tx, tenantID, parentTable); err != nil {
		return fmt.Errorf("could not create tenant table for tenant %q and parent table %q: %w", tenantID, parentTable, err)
	}
	// query 3: attach tenant table partition if not there yet
	if !slices.Contains(partitionsOfTenant, a.sql.GetTenantTableName(tenantID, parentTable)) && !slices.Contains(partitionsOfTenant, a.sql.GetTempTableName(tenantID, parentTable)) {
		if err = a.attachTenantTablePartition(ctx, tx, tenantID, parentTable); err != nil {
			return fmt.Errorf("could not attach tenant table partition for tenant %q and parent table %q: %w", tenantID, parentTable, err)
		}
	}
	return nil
}

func (a *Adapter) getAllPartitionsOfTenantForParentTable(ctx context.Context, tx transactor.DBTX, tenantID, parentTable string) ([]string, error) {
	stmt, err := a.sql.GetAllPartitionsOfTenant(tenantID, parentTable)
	if err != nil {
		return nil, fmt.Errorf("could not build sql statement for check if tenant partitions exists: %w", err)
	}
	var partitions []string
	err = pgxscan.Select(ctx, tx, &partitions, stmt)
	if err != nil {
		return nil, fmt.Errorf("could not execute sql statement for check tenant partitions exists: %w", err)
	}
	for i, partition := range partitions {
		partitions[i] = strings.ReplaceAll(partition, "\"", "")
	}
	return partitions, nil
}

func (a *Adapter) createTenantTable(ctx context.Context, tx transactor.DBTX, tenantID, parentTable string) error {
	stmt, err := a.sql.CreateNewTenantTable(tenantID, parentTable)
	if err != nil {
		return fmt.Errorf("could not build sql statement to create tenant table: %w", err)
	}
	if _, err = tx.Exec(ctx, stmt); err != nil {
		return fmt.Errorf("could not execute sql statement to create tenant table: %w", err)
	}
	return nil
}

func (a *Adapter) createRebuildTable(ctx context.Context, tx transactor.DBTX, tenantID, parentTable string) error {
	// prepare rebuild tables
	stmt, err := a.sql.CreateNewRebuildTable(tenantID, parentTable)
	if err != nil {
		return fmt.Errorf("could not build sql statement to create rebuild table: %w", err)
	}
	if _, err = tx.Exec(ctx, stmt); err != nil {
		return fmt.Errorf("could not execute sql statement to create rebuild table: %w", err)
	}
	return nil
}

func (a *Adapter) attachTenantTablePartition(ctx context.Context, tx transactor.DBTX, tenantID, parentTable string) error {
	stmt, err := a.sql.AttachTenantPartition(tenantID, parentTable)
	if err != nil {
		return fmt.Errorf("could not build sql statement to attach tenant table partition: %w", err)
	}
	if _, err = tx.Exec(ctx, stmt); err != nil {
		return fmt.Errorf("could not execute sql statement to attach tenant table partition: %w", err)
	}
	return nil
}

func (a *Adapter) renameTenantTable(ctx context.Context, tx transactor.DBTX, tenantID, parentTable string) error {
	stmt, err := a.sql.RenameTenantTableToRebuildTable(tenantID, parentTable)
	if err != nil {
		return fmt.Errorf("could not build sql statement to rename tenant table: %w", err)
	}
	if _, err = tx.Exec(ctx, stmt); err != nil {
		return fmt.Errorf("could not execute sql statement to rename tenant table: %w", err)
	}
	return nil
}

func (a *Adapter) truncateRebuildTable(ctx context.Context, tx transactor.DBTX, tenantID, parentTable string) error {
	stmt, err := a.sql.TruncateTenantTable(tenantID, parentTable)
	if err != nil {
		return fmt.Errorf("could not build sql statement to rename tenant table: %w", err)
	}
	if _, err = tx.Exec(ctx, stmt); err != nil {
		return fmt.Errorf("could not execute sql statement to rename tenant table: %w", err)
	}
	return nil
}

func (a *Adapter) detachTempTablePartition(ctx context.Context, tx transactor.DBTX, tenantID, parentTableName string) error {
	stmt, err := a.sql.DetachTempTablePartition(tenantID, parentTableName)
	if err != nil {
		return fmt.Errorf("could not build sql statement to detach tenant table: %w", err)
	}
	if _, err = tx.Exec(ctx, stmt); err != nil {
		return fmt.Errorf("could not execute sql statement to detach tenant table: %w", err)
	}
	return nil
}

func (a *Adapter) dropRebuildTable(ctx context.Context, tx transactor.DBTX, tenantID, parentTableName string) error {
	stmt, err := a.sql.DropRebuildTable(tenantID, parentTableName)
	if err != nil {
		return fmt.Errorf("could not build sql statement to drop rebuild table: %w", err)
	}
	if _, err = tx.Exec(ctx, stmt); err != nil {
		return fmt.Errorf("could not execute sql statement to drop rebuild table: %w", err)
	}
	return nil
}

func (a *Adapter) FinishRebuildIntern(ctx context.Context, tenantID, parentTable string) error {
	return a.ExecWithinTransaction(ctx, tenantID, func(ctx context.Context) error {
		tx, err := a.GetTx(ctx)
		if err != nil {
			return err
		}

		if err = a.executeFinishRebuild(ctx, tx, tenantID, parentTable); err != nil {
			return fmt.Errorf("could not execute finish rebuild: %w", err)
		}
		return nil
	})
}

func (a *Adapter) executeFinishRebuild(ctx context.Context, tx transactor.DBTX, tenantID, parentTable string) error {
	// query 1: What partitions are already there?
	partitionsOfTenant, err := a.getAllPartitionsOfTenantForParentTable(ctx, tx, tenantID, parentTable)
	if err != nil {
		return fmt.Errorf("could not execute rebuild finalization for table %q: %w", parentTable, err)
	}
	// query 2: (Optional) Detach old tenant partition if it was there
	if slices.Contains(partitionsOfTenant, a.sql.GetTempTableName(tenantID, parentTable)) {
		if err = a.detachTempTablePartition(ctx, tx, tenantID, parentTable); err != nil {
			return fmt.Errorf("could not execute rebuild finalization for table %q: %w", parentTable, err)
		}
	}
	// query 3: (Optional) Attach rebuild partition if not there yet
	if !slices.Contains(partitionsOfTenant, a.sql.GetTenantTableName(tenantID, parentTable)) {
		if err = a.attachTenantTablePartition(ctx, tx, tenantID, parentTable); err != nil {
			return fmt.Errorf("could not execute rebuild finalization for table %q: %w", parentTable, err)
		}
	}
	// query 4: Drop old tenant table
	if err = a.dropRebuildTable(ctx, tx, tenantID, parentTable); err != nil {
		return fmt.Errorf("could not execute rebuild finalization for table %q: %w", parentTable, err)
	}
	return nil
}

// ------------------------------------FOR TESTS-------------------------------------------------
func (a *Adapter) DropTenantIntern(ctx context.Context, tenantID, parentTable string) error {
	errTX := a.transactor.ExecWithinTransaction(ctx, func(txCtx context.Context) error {
		tx, err := a.GetTx(txCtx)
		if err != nil {
			return err
		}

		//drop tenant
		stmt, val, err := a.sql.DropTenant(tenantID, parentTable)
		if err != nil {
			return fmt.Errorf("could not build sql statement for drop tenant: %w", err)
		}

		_, err = tx.Exec(ctx, stmt, val...)
		if err != nil {
			return fmt.Errorf("could not execute sql statement for drop tenant: %w", err)
		}

		return nil
	})
	return errTX

}
