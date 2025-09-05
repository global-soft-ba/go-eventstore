package transactor

import (
	"context"
)

type Port interface {
	GetTransaction(txCtx context.Context) (any, error)
	GetClient() any

	ExecWithinTransaction(ctx context.Context, tFunc func(txCtx context.Context) error, options ...func(opt any) error) error
	ExecWithoutTransaction(ctx context.Context, tFunc func(txCtx context.Context) error, options ...func(opt any) error) error
}
