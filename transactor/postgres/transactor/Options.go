package transactor

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v5"
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

var defaultTxOptions = PostgresOptions{
	preparationFunc: func(ctx context.Context) error {
		return nil
	},
	completionFunc: func(ctx context.Context) error {
		return nil
	},
	txOptions: pgx.TxOptions{AccessMode: pgx.ReadWrite},
}

type PostgresOptions struct {
	txOptions       pgx.TxOptions
	preparationFunc func(ctx context.Context) error
	completionFunc  func(ctx context.Context) error
}

func WithTXPreparation(pFunc func(ctx context.Context) error) func(opt any) error {
	return func(opt any) error {
		options, ok := opt.(*PostgresOptions)
		if !ok {
			return fmt.Errorf("preparation function already set")
		}
		options.preparationFunc = pFunc
		return nil
	}
}

func WithTXCompletion(cFunc func(ctx context.Context) error) func(opt any) error {
	return func(opt any) error {
		options, ok := opt.(*PostgresOptions)
		if !ok {
			return fmt.Errorf("completion function already set")
		}
		options.completionFunc = cFunc
		return nil
	}
}

func WithTxDeferrableMode(mode TxDeferrableMode) func(opt *PostgresOptions) error {
	return func(opt *PostgresOptions) error {
		opt.txOptions.DeferrableMode = pgx.TxDeferrableMode(mode)
		return nil
	}
}

func WithTxIsolationLevels(level TxIsoLevel) func(opt *PostgresOptions) error {
	return func(opt *PostgresOptions) error {
		opt.txOptions.IsoLevel = pgx.TxIsoLevel(level)
		return nil
	}
}
