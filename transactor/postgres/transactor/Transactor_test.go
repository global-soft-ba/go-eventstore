//go:build integration

package transactor

import (
	"context"
	"errors"
	"fmt"
	"github.com/georgysavva/scany/v2/pgxscan"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	transactor2 "github.com/global-soft-ba/go-eventstore/transactor"
	"github.com/global-soft-ba/go-eventstore/transactor/postgres/test"
	"reflect"
	"testing"
)

const (
	PostgresHost           = "localhost"
	PostgresPort           = "5433"
	PostgresUsername       = "postgres"
	PostgresPassword       = "docker"
	PostgresSchema         = "eventstore_testing"
	PostgresMaxConnections = 10

	CreateTestTableStatement = `CREATE TABLE IF NOT EXISTS for_test(test_id text not null, test_field text not null)`
	DropTestTableStatement   = `DROP TABLE for_test`

	InsertStatement             = `INSERT INTO for_test (test_id, test_field) VALUES ($1, $2)`
	ErrorInsertStatement        = `INSERT INTO non_existing_table (test_id, test_field) VALUES ($1, $2)`
	GetTestFieldsStatement      = `SELECT test_field FROM for_test WHERE test_id = $1`
	ErrorGetTestFieldsStatement = `SELECT test_field FROM non_existing_table WHERE test_id = $1`
)

type TestTableRow struct {
	TestID    string `db:"test_id"`
	TestField string `db:"test_field"`
}

func Test_TxExecution(t *testing.T) {
	dbPool := test.InitPersistenceForTest(test.PostgresClientOptions{
		Host:           PostgresHost,
		Port:           PostgresPort,
		Username:       PostgresUsername,
		Password:       PostgresPassword,
		Schema:         PostgresSchema,
		MaxConnections: PostgresMaxConnections,
	})
	if err := createTestTable(dbPool); err != nil {
		panic(fmt.Errorf("unable to create test table: %w", err))
	}

	contextWithCancelCommit, cancelFuncCommit := context.WithCancel(context.Background())
	contextWithCancelRollback, cancelFuncRollback := context.WithCancel(context.Background())

	defer dbPool.Close()

	tests := []struct {
		name          string
		ctx           context.Context
		exec          func(t *testing.T, ctx context.Context, tx DBTX) error
		options       func(ctx context.Context, adp transactor2.Port) []func(opt any) error
		wantedRecords []string
		wantErr       func(t *testing.T, err error)
	}{
		{
			name: "empty transaction",
			ctx:  context.Background(),
			exec: func(t *testing.T, ctx context.Context, tx DBTX) error {
				return nil
			},
			options: func(ctx context.Context, adp transactor2.Port) (out []func(opt any) error) {
				return nil
			},
			wantedRecords: []string{},
			wantErr:       func(t *testing.T, err error) { assert.NoError(t, err) },
		},
		{
			name: "write single transaction",
			ctx:  context.Background(),
			exec: func(t *testing.T, ctx context.Context, tx DBTX) error {
				_, err := tx.Exec(ctx, InsertStatement, "write single transaction", "test")
				return err
			},
			options: func(ctx context.Context, adp transactor2.Port) (out []func(opt any) error) {
				return nil
			},
			wantedRecords: []string{"test"},
			wantErr:       func(t *testing.T, err error) { assert.NoError(t, err) },
		},
		{
			name: "write multiple transaction",
			ctx:  context.Background(),
			exec: func(t *testing.T, ctx context.Context, tx DBTX) error {
				for i := 0; i < 10; i++ {
					_, err := tx.Exec(ctx, InsertStatement, "write multiple transaction", fmt.Sprintf("test.%d", i))
					if err != nil {
						return err
					}
				}
				return nil
			},
			options: func(ctx context.Context, adp transactor2.Port) (out []func(opt any) error) {
				return nil
			},
			wantedRecords: []string{"test.0", "test.1", "test.2", "test.3", "test.4", "test.5", "test.6", "test.7", "test.8", "test.9"},
			wantErr:       func(t *testing.T, err error) { assert.NoError(t, err) },
		},
		{
			name: "transaction with error in executing function (rollback)",
			ctx:  context.Background(),
			exec: func(t *testing.T, ctx context.Context, tx DBTX) error {
				_, err := tx.Exec(ctx, ErrorInsertStatement, "transaction with error in executing function (rollback)", "test")
				if err != nil {
					return fmt.Errorf("error in pipeline")
				}
				return nil
			},
			options: func(ctx context.Context, adp transactor2.Port) (out []func(opt any) error) {
				return nil
			},
			wantedRecords: []string{},
			wantErr:       func(t *testing.T, err error) { assert.Error(t, err) },
		},
		{
			name: "transaction with error in executing commit (no rollback)",
			ctx:  contextWithCancelCommit,
			exec: func(t *testing.T, ctx context.Context, tx DBTX) error {
				_, err := tx.Exec(ctx, InsertStatement, "transaction with error in executing commit (no rollback)", "test")
				if err != nil {
					return err
				}
				cancelFuncCommit()
				return nil
			},
			options: func(ctx context.Context, adp transactor2.Port) (out []func(opt any) error) {
				return nil
			},
			wantedRecords: []string{"test"},
			wantErr: func(t *testing.T, err error) {
				var target *transactor2.ErrorCommitFailed
				if errors.Is(err, target) {
					return
				}
				t.Errorf("wrong error type want:%s got:%s ", reflect.TypeOf(target), reflect.TypeOf(err))
			},
		},
		{
			name: "transaction with error in executing rollback",
			ctx:  contextWithCancelRollback,
			exec: func(t *testing.T, ctx context.Context, tx DBTX) error {
				cancelFuncRollback()
				return fmt.Errorf("execution function fails")
			},
			options: func(ctx context.Context, adp transactor2.Port) (out []func(opt any) error) {
				return nil
			},
			wantedRecords: []string{},
			wantErr: func(t *testing.T, err error) {
				var target *transactor2.ErrorRollbackFailed
				if errors.Is(err, target) {
					return
				}
				t.Errorf("wrong error type want:%s got:%s ", reflect.TypeOf(target), reflect.TypeOf(err))
			},
		},
		{
			name: "transaction with preparation and finish function (no-error)",
			ctx:  context.Background(),
			exec: func(t *testing.T, ctx context.Context, tx DBTX) error {
				for i := 0; i < 10; i++ {
					_, err := tx.Exec(ctx, InsertStatement, "transaction with preparation and finish function (no-error)", fmt.Sprintf("test.%d", i))
					if err != nil {
						return err
					}
				}
				return nil
			},
			options: func(ctx context.Context, adp transactor2.Port) (out []func(opt any) error) {
				preparationFunc := func(ctx context.Context) error {
					_, err := dbPool.Exec(ctx, InsertStatement, "test_prep_func", "preparationFuncFired")
					if err != nil {
						return err
					}
					return nil
				}

				complFunc := func(ctx context.Context) error {
					var result string
					err := dbPool.QueryRow(ctx, GetTestFieldsStatement, "test_prep_func").Scan(&result)
					if err != nil {
						return fmt.Errorf("completion function error: %w", err)
					}
					if result != "preparationFuncFired" {
						return fmt.Errorf("completion function error: no preperation function record found")
					}
					return nil
				}

				return append(out, WithTXPreparation(preparationFunc), WithTXCompletion(complFunc))
			},
			wantedRecords: []string{"test.0", "test.1", "test.2", "test.3", "test.4", "test.5", "test.6", "test.7", "test.8", "test.9"},
			wantErr:       func(t *testing.T, err error) { assert.NoError(t, err) },
		},
		{
			name: "transaction with preparation and finish function (preparation function error)",
			ctx:  context.Background(),
			exec: func(t *testing.T, ctx context.Context, tx DBTX) error {
				for i := 0; i < 10; i++ {
					_, err := tx.Exec(ctx, InsertStatement, "transaction with preparation and finish function (preparation function error)", fmt.Sprintf("test.%d", i))
					if err != nil {
						return err
					}
				}
				return nil
			},
			options: func(ctx context.Context, adp transactor2.Port) (out []func(opt any) error) {
				preparationFunc := func(ctx context.Context) error {
					_, err := dbPool.Exec(ctx, ErrorInsertStatement, "test_prep_func_2", "preparationFuncFired")
					if err != nil {
						return fmt.Errorf("preparation function error: %w", err)
					}
					return nil
				}

				complFunc := func(ctx context.Context) error {
					var result string
					err := dbPool.QueryRow(ctx, GetTestFieldsStatement, "test_prep_func_2").Scan(&result)
					if err != nil {
						return fmt.Errorf("completion function error: %w", err)
					}
					if result != "preparationFuncFired" {
						return fmt.Errorf("completion function error: no preperation function record found")
					}
					return nil
				}

				return append(out, WithTXPreparation(preparationFunc), WithTXCompletion(complFunc))
			},
			wantedRecords: []string{"test.0", "test.1", "test.2", "test.3", "test.4", "test.5", "test.6", "test.7", "test.8", "test.9"},
			wantErr:       func(t *testing.T, err error) { assert.Error(t, err) },
		},
		{
			name: "transaction with preparation and finish function (completion function error)",
			ctx:  context.Background(),
			exec: func(t *testing.T, ctx context.Context, tx DBTX) error {
				for i := 0; i < 10; i++ {
					_, err := tx.Exec(ctx, InsertStatement, "transaction with preparation and finish function (completion function error)", fmt.Sprintf("test.%d", i))
					if err != nil {
						return err
					}
				}
				return nil
			},
			options: func(ctx context.Context, adp transactor2.Port) (out []func(opt any) error) {
				preparationFunc := func(ctx context.Context) error {
					_, err := dbPool.Exec(ctx, InsertStatement, "test_prep_func_3", "preparationFuncFired")
					if err != nil {
						return err
					}
					return nil
				}

				complFunc := func(ctx context.Context) error {
					var result string
					err := dbPool.QueryRow(ctx, ErrorGetTestFieldsStatement, "test_prep_func_3").Scan(&result)
					if err != nil {
						return fmt.Errorf("completion function error: %w", err)
					}
					if result != "preparationFuncFired" {
						return fmt.Errorf("completion function error: no preperation function record found")
					}
					return nil
				}

				return append(out, WithTXPreparation(preparationFunc), WithTXCompletion(complFunc))
			},
			wantedRecords: []string{"test.0", "test.1", "test.2", "test.3", "test.4", "test.5", "test.6", "test.7", "test.8", "test.9"},
			wantErr:       func(t *testing.T, err error) { assert.Error(t, err) },
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			adp := NewTransactor(dbPool)

			errTx := adp.ExecWithinTransaction(tt.ctx, func(txCtx context.Context) error {
				anyVal, err := adp.GetTransaction(txCtx)
				tx := anyVal.(DBTX)
				err = tt.exec(t, tt.ctx, tx)
				return err
			}, tt.options(tt.ctx, adp)...)
			tt.wantErr(t, errTx)
			if errTx == nil {
				_ = adp.ExecWithoutTransaction(tt.ctx, func(txCtx context.Context) error {
					anyVal := adp.GetClient()
					tx, ok := anyVal.(DBTX)
					if !ok {
						assert.Fail(t, "could not get client in ExecWithoutTransaction()")
					}
					records, err := forTestGetTestTableRows(tt.ctx, tx, tt.name)
					assert.NoError(t, err)
					assertRecords(t, tt.wantedRecords, recordsToSliceOfString(records))
					return nil
				})
			}
		})

	}
	err := dropTestTable(dbPool)
	assert.NoError(t, err)
}

func assertRecords(t *testing.T, got, want []string) {
	if len(got) != len(want) {
		assert.Fail(t, "number of got records and wanted is not the same, want: %d, got: %d", len(want), len(got))
	}
	count := 0
	for _, g := range got {
		for _, w := range want {
			if g == w {
				count++
			}
		}
	}
	if count != len(want) {
		assert.Fail(t, "did not find all wanted records, number of found: %d", count)
	}
}

func recordsToSliceOfString(rows []TestTableRow) []string {
	var testFields []string
	for _, r := range rows {
		testFields = append(testFields, r.TestField)
	}
	return testFields
}

func createTestTable(dbPool *pgxpool.Pool) error {
	_, err := dbPool.Exec(context.Background(), CreateTestTableStatement)
	return err
}

func dropTestTable(dbPool *pgxpool.Pool) error {
	_, err := dbPool.Exec(context.Background(), DropTestTableStatement)
	return err
}

func forTestGetTestTableRows(txCtx context.Context, tx DBTX, testID string) ([]TestTableRow, error) {
	var rows []TestTableRow

	err := pgxscan.Select(txCtx, tx, &rows, GetTestFieldsStatement, testID)
	if err != nil {
		return nil, err
	}

	return rows, nil
}
