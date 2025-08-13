package transactorForTest

import (
	"context"
	"fmt"
	trans "github.com/global-soft-ba/go-eventstore/transactor"
	trans2 "github.com/global-soft-ba/go-eventstore/transactor/postgres/transactor"
	"reflect"
)

func NewTxPassThroughTransactor() trans.Port {
	return &passThroughTransactor{storageKey: trans2.CtxStorageKey}
}

type passThroughTransactor struct {
	storageKey string
}

func (t *passThroughTransactor) extractTXFromCTX(ctx context.Context, key string) (trans2.DBTX, error) {
	valAny := ctx.Value(key)
	if valAny == nil {
		return nil, fmt.Errorf("could not find key for transaction in given context")
	}

	result, ok := valAny.(trans2.DBTX)
	if !ok {
		return nil, fmt.Errorf("could not found correct transaction type in context: found type %s", reflect.TypeOf(valAny).String())
	}

	return result, nil
}

func (t *passThroughTransactor) GetClient() any {
	return nil
}

func (t *passThroughTransactor) GetTransaction(txCtx context.Context) (any, error) {
	dbtx, err := t.extractTXFromCTX(txCtx, t.storageKey)
	if err != nil {
		return nil, err
	}

	return dbtx, nil
}

func (t *passThroughTransactor) ExecWithinTransaction(txCtx context.Context, tFunc func(ctx context.Context) error, options ...func(opt any) error) error {
	if _, err := t.extractTXFromCTX(txCtx, t.storageKey); err != nil {
		return fmt.Errorf("could not execute single test transaction: no existing db/tx in context")
	}

	return tFunc(txCtx)
}

func (t *passThroughTransactor) ExecWithoutTransaction(txCtx context.Context, tFunc func(ctx context.Context) error, options ...func(opt any) error) error {
	if _, err := t.extractTXFromCTX(txCtx, trans2.CtxStorageKey); err != nil {
		return fmt.Errorf("could not execute single test transaction: no existing db/tx in context")
	}

	return tFunc(txCtx)
}
