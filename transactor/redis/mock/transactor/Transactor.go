package transactor

import (
	"context"
	transactor2 "github.com/global-soft-ba/go-eventstore/transactor"
)

func NewTransactor() transactor2.Port {
	return &transactor{}
}

type transactor struct {
}

func (t transactor) GetClient() any {
	return nil
}

func (t transactor) GetTransaction(txCtx context.Context) (any, error) {
	return nil, nil
}

func (t transactor) ExecWithinTransaction(ctx context.Context, tFunc func(ctx context.Context) error, options ...func(opt any) error) error {
	return tFunc(ctx)
}

func (t transactor) ExecWithoutTransaction(ctx context.Context, tFunc func(ctx context.Context) error, options ...func(opt any) error) error {
	return tFunc(ctx)
}
