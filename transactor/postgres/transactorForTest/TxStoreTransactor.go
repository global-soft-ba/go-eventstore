package transactorForTest

import (
	"context"
	"fmt"
	trans "github.com/global-soft-ba/go-eventstore/transactor"
	trans2 "github.com/global-soft-ba/go-eventstore/transactor/postgres/transactor"
)

func NewTxStoreTransactor(txCtx context.Context) trans.Port {
	t := storetransactor{key: trans2.CtxStorageKey}

	tx, err := t.extractTXFromCTX(txCtx, t.key)
	if err != nil {
		panic(fmt.Errorf("could not init test stored transactor: no existing db/tx in context"))
	}
	t.tx = tx

	return &t
}

type storetransactor struct {
	key string
	tx  trans2.DBTX
}

func (t *storetransactor) injectTXIntoCTX(ctx context.Context, key string, value any) context.Context {
	return context.WithValue(ctx, key, value)
}

func (t *storetransactor) extractTXFromCTX(ctx context.Context, key string) (trans2.DBTX, error) {
	valAny := ctx.Value(key)
	if valAny == nil {
		return nil, fmt.Errorf("could not find key for transaction in given context")
	}
	return valAny.(trans2.DBTX), nil
}

func (t *storetransactor) GetClient() any {
	return nil
}

func (t *storetransactor) GetTransaction(ctx context.Context) (any, error) {
	return t.tx, nil
}

func (t *storetransactor) ExecWithinTransaction(ctx context.Context, tFunc func(ctx context.Context) error, _ ...func(opt any) error) error {
	if _, err := t.extractTXFromCTX(ctx, t.key); err == nil {
		return fmt.Errorf("could not execute test stored transactor: existing db/tx in context")
	}

	return tFunc(ctx)
}

func (t *storetransactor) ExecWithoutTransaction(ctx context.Context, tFunc func(ctx context.Context) error, _ ...func(opt any) error) error {
	if _, err := t.extractTXFromCTX(ctx, t.key); err == nil {
		return fmt.Errorf("could not execute test stored transactor: existing db/tx in context")
	}

	return tFunc(ctx)
}
