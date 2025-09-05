package transactor

import (
	"context"
)

// TxIsoLevel is the transaction isolation level (serializable, repeatable read, read committed or read uncommitted)
type TxIsoLevel string

// Transaction isolation levels
const (
	Serializable    TxIsoLevel = "serializable"
	RepeatableRead  TxIsoLevel = "repeatable read"
	ReadCommitted   TxIsoLevel = "read committed"
	ReadUncommitted TxIsoLevel = "read uncommitted"
)

// TxDeferrableMode is the transaction deferrable mode (deferrable or not deferrable)
type TxDeferrableMode string

// Transaction deferrable modes
const (
	Deferrable    TxDeferrableMode = "deferrable"
	NotDeferrable TxDeferrableMode = "not deferrable"
)

type Port interface {
	GetTX(txCtx context.Context) (any, error)

	WithinTX(ctx context.Context, tFunc func(ctx context.Context) error, options ...func(tx interface{}) error) error
	WithoutTX(ctx context.Context, tFunc func(ctx context.Context) error, options ...func(tx interface{}) error) error
	WithTxIsolationLevels(level TxIsoLevel) func(tx interface{}) error
	WithTxDeferrableMode(mode TxDeferrableMode) func(tx interface{}) error
}
