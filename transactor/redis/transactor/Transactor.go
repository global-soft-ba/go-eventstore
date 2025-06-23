package transactor

import (
	"context"
	"errors"
	"fmt"
	"github.com/global-soft-ba/go-eventstore/instrumentation/port/metrics"
	transPort "github.com/global-soft-ba/go-eventstore/transactor"
	"github.com/redis/go-redis/v9"
	"reflect"
)

const CtxStorageKey = "ctxREDISStorageKey"

func NewTransactor(redis *redis.Client) transPort.Port {
	return &transactor{redis: redis}
}

type transactor struct {
	redis *redis.Client
}

func (t *transactor) GetTransaction(txCtx context.Context) (any, error) {
	return t.extractTXFromCTX(txCtx, CtxStorageKey)
}

func (t *transactor) GetClient() any {
	return t.redis
}

func (t *transactor) ExecWithinTransaction(ctx context.Context, tFunc func(ctx context.Context) error, options ...func(opt any) error) error {
	ctx, endSpan := metrics.StartSpan(ctx, "transactor-within-pipeline", nil)
	defer endSpan()

	if t.doesCtxContainsTransaction(ctx) {
		return fmt.Errorf("could not execute transaction: existing db/pipeline in context")
	}

	opt, err := getTxOptions(options...)
	if err != nil {
		return fmt.Errorf("could not get options for transaction: %w", err)
	}

	// inject in context and execute transaction
	err = t.executeTxPipelined(ctx, tFunc, opt)
	if err != nil {
		return err
	}

	return err
}

func (t *transactor) executeTxPipelined(ctx context.Context, tFunc func(ctx context.Context) error, opt *RedisOptions) error {

	errTx := t.redis.Watch(ctx, func(tx *redis.Tx) error {
		cmds, err := tx.TxPipelined(ctx,
			t.executeWithPreparationAndFinishFunction(ctx, opt, tFunc))
		if err != nil {
			if err == redis.Nil && opt.NonExistentKeys {
				return nil
			}
			// check if cmds has been executed in pipeline
			if len(cmds) != 0 {
				return transPort.NewErrorCommitFailed(fmt.Errorf("error while executing transaction (cannot rollback): %w", err))
			}
			return err
		}
		return err
	}, opt.WatchKeys...)
	if errors.Is(errTx, redis.TxFailedErr) {
		return transPort.NewErrorLockFailed(fmt.Errorf("could not execute transaction: optimistic lock on keys %s failed: %s", errTx, opt.WatchKeys))
	}

	var errCommit *transPort.ErrorCommitFailed
	if errors.Is(errTx, errCommit) {
		return errTx
	}

	if errTx != nil {
		return fmt.Errorf("could not execute transaction: %w", errTx)
	}

	return errTx
}

func (t *transactor) executeWithPreparationAndFinishFunction(ctx context.Context, opt *RedisOptions, tFunc func(ctx context.Context) error) func(pipe redis.Pipeliner) error {
	return func(pipe redis.Pipeliner) error {
		// inject in context
		txCtx := t.injectTXIntoCTX(ctx, CtxStorageKey, pipe)

		//execute preparation function
		err := opt.PreparationFunc(txCtx)
		if err != nil {
			return fmt.Errorf("could not execute transaction preparation function: %w", err)
		}

		err = tFunc(txCtx)
		// if we have an error during filling the pipeline, we do a rollback
		if err != nil {
			pipe.Discard()
			return fmt.Errorf("could not execute transaction function: %w", err)
		}

		//execute completion function
		err = opt.CompletionFunc(txCtx)
		if err != nil {
			return fmt.Errorf("could not execute transaction completion function: %w", err)
		}

		return err
	}
}

func (t *transactor) ExecWithoutTransaction(ctx context.Context, tFunc func(ctx context.Context) error, options ...func(opt any) error) error {
	ctx, endSpan := metrics.StartSpan(ctx, "transactor-without-pipeline", nil)
	defer endSpan()

	if t.doesCtxContainsTransaction(ctx) {
		return fmt.Errorf("could not execute transaction: existing db/pipeline in context")
	}

	opt, err := getTxOptions(options...)
	if err != nil {
		return fmt.Errorf("could not get options for transaction: %w", err)
	}

	//execute preparation function
	err = opt.PreparationFunc(ctx)
	if err != nil {
		return fmt.Errorf("could not execute transaction preparation function: %w", err)
	}

	// execute main function
	err = t.executeNonTxPipeline(ctx, tFunc, opt)
	if err != nil {
		return fmt.Errorf("could not execute transaction: %w", err)
	}

	//execute completion function
	// we need to create a sub-context for the completion function,
	// to avoid the transaction injected by main function
	err = opt.CompletionFunc(context.WithValue(ctx, CtxStorageKey, nil))
	if err != nil {
		return fmt.Errorf("could not execute transaction completion function: %w", err)
	}

	return err
}

func (t *transactor) executeNonTxPipeline(ctx context.Context, tFunc func(ctx context.Context) error, opt *RedisOptions) error {
	cmds, err := t.redis.Pipelined(ctx, t.executeWithPreparationAndFinishFunction(ctx, opt, tFunc))
	if err != nil {
		if err == redis.Nil && opt.NonExistentKeys {
			return nil
		}
		// check if any cmds has been executed in pipeline
		if len(cmds) != 0 {
			return transPort.NewErrorCommitFailed(fmt.Errorf("error while executing query due to partially execution: %w", err))
		}
		return err
	}
	return nil
}

func (t *transactor) injectTXIntoCTX(ctx context.Context, key string, value redis.Pipeliner) context.Context {
	return context.WithValue(ctx, key, value)
}

func (t *transactor) extractTXFromCTX(ctx context.Context, key string) (redis.Pipeliner, error) {
	valAny := ctx.Value(key)
	if valAny == nil {
		return nil, fmt.Errorf("could not find key for transaction in given context")
	}
	result, ok := valAny.(redis.Pipeliner)
	if !ok {
		return nil, fmt.Errorf("could not found correct transaction type in context: found type %s", reflect.TypeOf(valAny).String())
	}
	return result, nil
}

func (t *transactor) doesCtxContainsTransaction(ctx context.Context) bool {
	_, err := t.extractTXFromCTX(ctx, CtxStorageKey)
	return err == nil
}
