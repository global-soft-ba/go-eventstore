package tests

import (
	"context"
	"fmt"
	trans "github.com/global-soft-ba/go-eventstore/eventstore/core/port/persistence/transactor"
)

func NewTxPassThroughTransactor(storageKey string) trans.Port {
	return &passThroughTransactor{storageKey: storageKey}
}

type passThroughTransactor struct {
	storageKey string
}

func (t *passThroughTransactor) extractTXFromCTX(ctx context.Context, key string) (any, error) {
	valAny := ctx.Value(key)
	if valAny == nil {
		return nil, fmt.Errorf("could not find key for transaction in given context")
	}
	return valAny, nil
}

func (t *passThroughTransactor) GetTX(txCtx context.Context) (any, error) {
	valAny, err := t.extractTXFromCTX(txCtx, t.storageKey)
	if err != nil {
		return nil, err
	}

	return valAny, nil
}

func (t *passThroughTransactor) WithinTX(ctx context.Context, tFunc func(ctx context.Context) error, options ...func(tx interface{}) error) error {
	if _, err := t.extractTXFromCTX(ctx, t.storageKey); err != nil {
		return fmt.Errorf("could not execute single test transaction: no existing db/tx in context")
	}

	return tFunc(ctx)
}

func (t *passThroughTransactor) WithoutTX(ctx context.Context, tFunc func(ctx context.Context) error, options ...func(tx interface{}) error) error {
	if _, err := t.extractTXFromCTX(ctx, t.storageKey); err != nil {
		return fmt.Errorf("could not execute single test transaction: no existing db/tx in context")
	}

	return tFunc(ctx)
}

func (t *passThroughTransactor) WithTxIsolationLevels(level trans.TxIsoLevel) func(tx interface{}) error {
	return func(tx interface{}) error {
		return nil
	}
}
func (t *passThroughTransactor) WithTxDeferrableMode(mode trans.TxDeferrableMode) func(tx interface{}) error {
	return func(tx interface{}) error {
		return nil
	}
}
