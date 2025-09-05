package transactor

import (
	"context"
	"fmt"
)

var defaultTxOptions = RedisOptions{
	WatchKeys:       []string{},
	NonExistentKeys: false,
	PreparationFunc: func(ctx context.Context) error {
		return nil
	},
	CompletionFunc: func(ctx context.Context) error {
		return nil
	},
}

type RedisOptions struct {
	WatchKeys       []string
	NonExistentKeys bool
	PreparationFunc func(ctx context.Context) error
	CompletionFunc  func(ctx context.Context) error
}

func getTxOptions(options ...func(opt any) error) (*RedisOptions, error) {
	result := defaultTxOptions

	for _, opt := range options {
		err := opt(&result)
		if err != nil {
			return nil, fmt.Errorf("could not configure transaction: %w", err)
		}
	}

	return &result, nil
}

func WithTXPreparation(prepareFunc func(ctx context.Context) error) func(opt any) error {
	return func(opt any) error {
		options, ok := opt.(*RedisOptions)
		if !ok {
			return fmt.Errorf("preparation function already set")
		}

		options.PreparationFunc = prepareFunc
		return nil
	}
}

func WithTXCompletion(completeFunc func(ctx context.Context) error) func(opt any) error {
	return func(opt any) error {
		options, ok := opt.(*RedisOptions)
		if !ok {
			return fmt.Errorf("preparation function already set")
		}

		options.CompletionFunc = completeFunc
		return nil
	}
}

// WithTXWatchKeys This function monitors all specified keys in the Redis DB for changes. However, only
// The client's DB is monitored. Changes to the database within a pipe with a select command, for example, cannot be
// monitored
func WithTXWatchKeys(keys ...string) func(opt any) error {
	return func(opt any) error {
		options, ok := opt.(*RedisOptions)
		if !ok {
			return fmt.Errorf("preparation function already set")
		}

		options.WatchKeys = keys
		return nil
	}
}

func WithTXNonExistingKeyAccess() func(opt any) error {
	return func(opt any) error {
		options, ok := opt.(*RedisOptions)
		if !ok {
			return fmt.Errorf("preparation function already set")
		}

		options.NonExistentKeys = true
		return nil
	}
}
