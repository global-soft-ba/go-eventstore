package internal

import (
	"context"
	"fmt"
	transactor2 "github.com/global-soft-ba/go-eventstore/eventstore/core/port/persistence/transactor"
	"github.com/global-soft-ba/go-eventstore/instrumentation/port/metrics"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"reflect"
)

const CtxStorageKey = "ctxSQLStorageKey"

func NewTransactor(db *pgxpool.Pool) transactor2.Port {
	return &transactor{db: db}
}

type transactor struct {
	db *pgxpool.Pool
}

func (t *transactor) injectTXIntoCTX(ctx context.Context, key string, value any) context.Context {
	return context.WithValue(ctx, key, value)
}

func (t *transactor) extractTXFromCTX(ctx context.Context, key string) (any, error) {
	valAny := ctx.Value(key)
	if valAny == nil {
		return nil, fmt.Errorf("could not find key for aggregates in given context")
	}
	return valAny, nil
}

func (t *transactor) GetTX(txCtx context.Context) (any, error) {
	valAny, err := t.extractTXFromCTX(txCtx, CtxStorageKey)
	if err != nil {
		return nil, err
	}

	return valAny, nil
}

func (t *transactor) WithinTX(ctx context.Context, tFunc func(ctx context.Context) error, options ...func(tx interface{}) error) error {
	ctx, endSpan := metrics.StartSpan(ctx, "WithinTX (transactor)", nil)
	defer endSpan()

	if _, err := t.extractTXFromCTX(ctx, CtxStorageKey); err == nil {
		return fmt.Errorf("could not execute transaction: existing db/tx in context")
	}

	txn, err := t.createConnection(ctx, options)
	if err != nil {
		return fmt.Errorf("could not create connection for transaction: %w", err)
	}

	txCtx := t.injectTXIntoCTX(ctx, CtxStorageKey, txn)

	err = tFunc(txCtx)
	if err != nil {
		return t.rollback(txCtx, txn, err)

	} else {
		return t.commit(txCtx, txn, err)
	}
}

func (t *transactor) createConnection(ctx context.Context, options []func(tx interface{}) error) (pgx.Tx, error) {
	ctx, endSpan := metrics.StartSpan(ctx, "createConnection (transactor)", nil)
	defer endSpan()

	opt, err := t.getTxOptions(options...)
	if err != nil {
		return nil, err
	}

	txn, err := t.db.BeginTx(ctx, opt)
	if err != nil {
		return nil, fmt.Errorf("could not start transation connection: %w", err)
	}

	return txn, nil
}

func (t *transactor) rollback(ctx context.Context, txn pgx.Tx, errTrans error) error {
	ctx, endSpan := metrics.StartSpan(ctx, "rollback (transactor)", nil)
	defer endSpan()

	if err := txn.Rollback(ctx); err != nil {
		return transactor2.NewErrorRollbackFailed(errTrans)
	}
	return errTrans
}

func (t *transactor) commit(ctx context.Context, txn pgx.Tx, errTrans error) error {
	ctx, endSpan := metrics.StartSpan(ctx, "commit (transactor)", nil)
	defer endSpan()

	if err := txn.Commit(ctx); err != nil {
		return transactor2.NewErrorCommitFailed(errTrans)
	}
	return errTrans
}

func (t *transactor) WithoutTX(ctx context.Context, tFunc func(ctx context.Context) error, _ ...func(tx interface{}) error) error {
	ctx, endSpan := metrics.StartSpan(ctx, "WithoutTX (transactor)", nil)
	defer endSpan()

	if _, err := t.extractTXFromCTX(ctx, CtxStorageKey); err == nil {
		return fmt.Errorf("could not execute transaction: existing db/tx in context")
	}

	client, err := t.db.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("could not get connection: %w", err)
	}
	defer client.Release()

	txCtx := t.injectTXIntoCTX(ctx, CtxStorageKey, client)

	err = tFunc(txCtx)
	return err
}

func (t *transactor) getTxOptions(options ...func(tx interface{}) error) (pgx.TxOptions, error) {
	result := pgx.TxOptions{}
	result.AccessMode = pgx.ReadWrite

	for _, opt := range options {
		err := opt(&result)
		if err != nil {
			return pgx.TxOptions{}, fmt.Errorf("could not configure transaction: %w", err)
		}
	}

	return result, nil
}

func (t *transactor) WithTxIsolationLevels(level transactor2.TxIsoLevel) func(tx interface{}) error {
	return func(tx interface{}) error {
		txOpt, ok := tx.(*pgx.TxOptions)
		if !ok {
			return fmt.Errorf("wrong type for options; want %q got %q", reflect.TypeOf(pgx.TxOptions{}), reflect.TypeOf(tx))
		}
		txOpt.IsoLevel = pgx.TxIsoLevel(level)
		return nil
	}
}
func (t *transactor) WithTxDeferrableMode(mode transactor2.TxDeferrableMode) func(tx interface{}) error {
	return func(tx interface{}) error {
		txOpt, ok := tx.(*pgx.TxOptions)
		if !ok {
			return fmt.Errorf("wrong type for options; want %q got %q", reflect.TypeOf(pgx.TxOptions{}), reflect.TypeOf(tx))
		}
		txOpt.DeferrableMode = pgx.TxDeferrableMode(mode)
		return nil
	}
}
