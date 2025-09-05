package tests

import (
	"context"
	"fmt"
	trans "github.com/global-soft-ba/go-eventstore/eventstore/core/port/persistence/transactor"
	"github.com/global-soft-ba/go-eventstore/eventstore/infrastructure/persistence/memory/internal/db"
)

func NewTxStoreTransactor(txCtx context.Context, storageKey string) trans.Port {
	t := storeTransactor{key: storageKey}

	tx, err := t.extractTXFromCTX(txCtx, t.key)
	if err != nil {
		panic(fmt.Errorf("ould not init test stored passThroughTransactor: no existing db/tx in context"))
	}
	t.tx = tx

	return &t
}

type storeTransactor struct {
	key string
	tx  *db.MemDBTX
}

func (t *storeTransactor) injectTXIntoCTX(ctx context.Context) context.Context {
	return context.WithValue(ctx, t.key, t.tx)
}

func (t *storeTransactor) extractTXFromCTX(ctx context.Context, key string) (*db.MemDBTX, error) {
	valAny := ctx.Value(key)
	if valAny == nil {
		return nil, fmt.Errorf("could not find key for transaction in given context")
	}
	return valAny.(*db.MemDBTX), nil
}

func (t *storeTransactor) GetTX(ctx context.Context) (any, error) {
	return t.tx, nil
}

func (t *storeTransactor) WithinTX(ctx context.Context, tFunc func(ctx context.Context) error, options ...func(tx interface{}) error) error {
	if _, err := t.extractTXFromCTX(ctx, t.key); err == nil {
		return fmt.Errorf("could not execute test stored passThroughTransactor: existing db/tx in context")
	}

	return tFunc(ctx)
}

func (t *storeTransactor) WithoutTX(ctx context.Context, tFunc func(ctx context.Context) error, options ...func(tx interface{}) error) error {
	if _, err := t.extractTXFromCTX(ctx, t.key); err == nil {
		return fmt.Errorf("could not execute test stored passThroughTransactor: existing db/tx in context")
	}

	return tFunc(ctx)
}

func (t *storeTransactor) WithTxIsolationLevels(level trans.TxIsoLevel) func(tx interface{}) error {
	return func(tx interface{}) error {
		return nil
	}
}
func (t *storeTransactor) WithTxDeferrableMode(mode trans.TxDeferrableMode) func(tx interface{}) error {
	return func(tx interface{}) error {
		return nil
	}
}
