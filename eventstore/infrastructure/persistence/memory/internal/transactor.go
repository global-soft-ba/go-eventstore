package internal

import (
	"context"
	"fmt"
	transactor2 "github.com/global-soft-ba/go-eventstore/eventstore/core/port/persistence/transactor"
	db2 "github.com/global-soft-ba/go-eventstore/eventstore/infrastructure/persistence/memory/internal/db"
	"go.uber.org/atomic"
)

const CtxStorageKey = "ctxMemDBStorageKey"

func NewTransactor() transactor2.Port {
	return &transactor{
		db:                       db2.NewInMemoryDB(),
		activeRunningTransaction: atomic.NewBool(false),
	}
}

type transactor struct {
	transactor2.Port
	db *db2.MemDB

	activeRunningTransaction *atomic.Bool // used to check if there is already a transaction running
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
	if _, err := t.extractTXFromCTX(ctx, CtxStorageKey); err == nil {
		return fmt.Errorf("could not execute transaction: existing db/tx in context")
	}

	if t.activeRunningTransaction.Load() {
		return fmt.Errorf("could not execute transaction: parallel running transaction")
	} else {
		t.activeRunningTransaction.Swap(true)
		defer func() {
			t.activeRunningTransaction.Swap(false)
		}()
	}

	txn := t.db.DBTX(true)
	txCtx := t.injectTXIntoCTX(ctx, CtxStorageKey, txn)

	err := tFunc(txCtx)

	if err != nil {
		return t.abort(txn, err)
	} else {
		return t.commit(txn, err)
	}
}

func (t *transactor) abort(txn *db2.MemDBTX, errTrans error) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = transactor2.NewErrorRollbackFailed(errTrans)
		}
	}()
	txn.Abort()
	return errTrans
}

func (t *transactor) commit(txn *db2.MemDBTX, errTrans error) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = transactor2.NewErrorCommitFailed(errTrans)
		}
	}()
	txn.Commit()
	return err
}

func (t *transactor) WithoutTX(ctx context.Context, tFunc func(ctx context.Context) error, options ...func(tx interface{}) error) (err error) {
	// InitStream a read only transaction
	txn := t.db.DBTX(false)
	txCtx := t.injectTXIntoCTX(ctx, CtxStorageKey, txn)
	err = tFunc(txCtx)
	if err != nil {
		txn.Abort()
		return err
	} else {
		txn.Commit()
	}
	return err
}

func (t *transactor) WithTxIsolationLevels(level transactor2.TxIsoLevel) func(tx interface{}) error {
	return nil
}
func (t *transactor) WithTxDeferrableMode(mode transactor2.TxDeferrableMode) func(tx interface{}) error {
	return nil
}
