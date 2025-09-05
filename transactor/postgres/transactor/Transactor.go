package transactor

import (
	"context"
	"fmt"
	"github.com/global-soft-ba/go-eventstore/instrumentation/port/metrics"
	transPort "github.com/global-soft-ba/go-eventstore/transactor"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"reflect"
)

const CtxStorageKey = "ctxSQLStorageKey"

type transactor struct {
	db *pgxpool.Pool
}

func NewTransactor(db *pgxpool.Pool) transPort.Port {
	return &transactor{db: db}
}

func (t *transactor) GetTransaction(txCtx context.Context) (any, error) {
	dbtx, err := t.extractTXFromCTX(txCtx, CtxStorageKey)
	if err != nil {
		return nil, err
	}

	return dbtx, nil
}

func (t *transactor) GetClient() any {
	return t.db
}

func (t *transactor) ExecWithinTransaction(ctx context.Context, tFunc func(ctx context.Context) error, options ...func(opt any) error) error {
	ctx, endSpan := metrics.StartSpan(ctx, "transactor-within-pipeline", nil)
	defer endSpan()

	// only for test of existing db/tx in context
	if _, err := t.extractTXFromCTX(ctx, CtxStorageKey); err == nil {
		return fmt.Errorf("could not execute transaction: existing db/tx in context")
	}

	opt, err := t.getTxOptions(options...)
	if err != nil {
		return fmt.Errorf("could not get options for transaction: %w", err)
	}

	//execute preparation function
	err = opt.preparationFunc(ctx)
	if err != nil {
		return fmt.Errorf("could not execute transaction preparation function: %w", err)
	}

	// execute transaction
	txn, err := t.createConnection(ctx, opt)
	if err != nil {
		return fmt.Errorf("could not create connection for transaction: %w", err)
	}

	txCtx := t.injectTXIntoCTX(ctx, CtxStorageKey, txn)

	err = tFunc(txCtx)
	if err != nil {
		return t.rollback(txCtx, txn, err)
	}
	if err = t.commit(txCtx, txn, err); err != nil {
		return err
	}

	// execute completion function
	// we need to create a sub-context for the completion function,
	// to avoid the transaction injected by main function
	err = opt.completionFunc(context.WithValue(ctx, CtxStorageKey, nil))
	if err != nil {
		return fmt.Errorf("could not execute transaction completion function: %w", err)
	}

	return err
}

func (t *transactor) ExecWithoutTransaction(ctx context.Context, tFunc func(txCtx context.Context) error, options ...func(opt any) error) error {
	ctx, endSpan := metrics.StartSpan(ctx, "transactor-without-pipeline", nil)
	defer endSpan()

	// only for test of existing db/tx in context
	if _, err := t.extractTXFromCTX(ctx, CtxStorageKey); err == nil {
		return fmt.Errorf("could not execute transaction: existing db/tx in context")
	}

	opt, err := t.getTxOptions(options...)
	if err != nil {
		return err
	}

	// execute preparation function
	err = opt.preparationFunc(ctx)
	if err != nil {
		return fmt.Errorf("could not execute transaction preparation function: %w", err)
	}

	// execute transaction
	client, err := t.db.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("could not get connection: %w", err)
	}
	defer client.Release()

	txCtx := t.injectTXIntoCTX(ctx, CtxStorageKey, client)

	err = tFunc(txCtx)
	if err != nil {
		return fmt.Errorf("could not execute transaction: %w", err)
	}

	// execute completion function
	// we need to create a sub-context for the completion function,
	// to avoid the transaction injected by main function
	err = opt.completionFunc(context.WithValue(ctx, CtxStorageKey, nil))
	if err != nil {
		return fmt.Errorf("could not execute transaction completion function: %w", err)
	}

	return err
}

func (t *transactor) injectTXIntoCTX(ctx context.Context, key string, value any) context.Context {
	return context.WithValue(ctx, key, value)
}

func (t *transactor) extractTXFromCTX(ctx context.Context, key string) (DBTX, error) {
	valAny := ctx.Value(key)
	if valAny == nil {
		return nil, fmt.Errorf("could not find key for transaction in given context")
	}

	result, ok := valAny.(DBTX)
	if !ok {
		return nil, fmt.Errorf("could not found correct transaction type in context: found type %s", reflect.TypeOf(valAny).String())
	}
	return result, nil
}

func (t *transactor) createConnection(ctx context.Context, option *PostgresOptions) (pgx.Tx, error) {
	ctx, endSpan := metrics.StartSpan(ctx, "createConnection (transactor)", nil)
	defer endSpan()

	txn, err := t.db.BeginTx(ctx, option.txOptions)
	if err != nil {
		return nil, fmt.Errorf("could not start transaction connection: %w", err)
	}

	return txn, nil
}

func (t *transactor) rollback(ctx context.Context, txn pgx.Tx, errTrans error) error {
	ctx, endSpan := metrics.StartSpan(ctx, "rollback (transactor)", nil)
	defer endSpan()

	if err := txn.Rollback(ctx); err != nil {
		return transPort.NewErrorRollbackFailed(errTrans)
	}
	return errTrans
}

func (t *transactor) commit(ctx context.Context, txn pgx.Tx, errTrans error) error {
	ctx, endSpan := metrics.StartSpan(ctx, "commit (transactor)", nil)
	defer endSpan()

	if err := txn.Commit(ctx); err != nil {
		return transPort.NewErrorCommitFailed(err)
	}
	return errTrans
}

func (t *transactor) getTxOptions(options ...func(opt any) error) (*PostgresOptions, error) {
	result := defaultTxOptions
	for _, opt := range options {
		err := opt(&result)
		if err != nil {
			return nil, fmt.Errorf("could not configure transaction: %w", err)
		}
	}

	return &result, nil
}
