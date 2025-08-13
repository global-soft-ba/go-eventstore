//go:build integration

package transactor

import (
	"context"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	transactor2 "github.com/global-soft-ba/go-eventstore/transactor"
	test2 "github.com/global-soft-ba/go-eventstore/transactor/redis/test"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"
)

var redisClient *redis.Client

const (
	RedisURL      = "localhost:6379"
	RedisDB       = 0
	RedisUsername = ""
	RedisPassword = ""
	writeDB       = 5
)

func TestMain(m *testing.M) {
	redisClient = test2.InitPersistenceForTest(redis.Options{
		Addr:     RedisURL,
		DB:       RedisDB,
		Username: RedisUsername,
		Password: RedisPassword,
	}, writeDB)
	exitVal := m.Run()
	os.Exit(exitVal)
}

func _TestPersistent() transactor2.Port {
	return NewTransactor(redisClient)
}

func Test_TxExecution(t *testing.T) {
	tID := uuid.NewString()

	tests := []struct {
		name    string
		ctx     context.Context
		exec    func(t *testing.T, ctx context.Context, pipe redis.Pipeliner) (keys []string, vals []string, err error)
		options func(ctx context.Context, adp transactor2.Port) []func(opt any) error
		wantErr func(t *testing.T, err error)
	}{

		{
			name: "empty transaction",
			ctx:  context.Background(),
			exec: func(t *testing.T, ctx context.Context, pipe redis.Pipeliner) (keys []string, vals []string, err error) {
				return nil, nil, nil
			},
			options: func(ctx context.Context, adp transactor2.Port) (out []func(opt any) error) {
				return nil
			},
			wantErr: func(t *testing.T, err error) { assert.NoError(t, err) },
		},
		{
			name: "write single transaction",
			ctx:  context.Background(),
			exec: func(t *testing.T, ctx context.Context, pipe redis.Pipeliner) (keys []string, vals []string, err error) {
				pipe.Set(ctx, "test", "12345", time.Hour)
				return []string{"test"}, []string{"12345"}, nil
			},
			options: func(ctx context.Context, adp transactor2.Port) (out []func(opt any) error) {
				return nil
			},
			wantErr: func(t *testing.T, err error) { assert.NoError(t, err) },
		},
		{
			name: "write multiple transaction",
			ctx:  context.Background(),
			exec: func(t *testing.T, ctx context.Context, pipe redis.Pipeliner) (keys []string, vals []string, err error) {
				for i := 0; i < 10; i++ {
					key := fmt.Sprintf("key%d", i)
					pipe.Set(ctx, key, fmt.Sprintf("%d", i), 0)
					keys = append(keys, key)
					vals = append(vals, fmt.Sprintf("%d", i))
				}
				return keys, vals, nil
			},
			options: func(ctx context.Context, adp transactor2.Port) (out []func(opt any) error) {
				return nil
			},
			wantErr: func(t *testing.T, err error) { assert.NoError(t, err) },
		},
		{
			name: "transaction with key not found error in pipeline (error due missing options)",
			ctx:  context.Background(),
			exec: func(t *testing.T, ctx context.Context, pipe redis.Pipeliner) (keys []string, vals []string, err error) {
				pipe.Get(ctx, "non-existent-key")
				return keys, vals, nil
			},
			options: func(ctx context.Context, adp transactor2.Port) (out []func(opt any) error) {
				return nil
			},
			wantErr: func(t *testing.T, err error) { assert.Error(t, err) },
		},
		{
			name: "transaction with key not found error in pipeline (no-error)",
			ctx:  context.Background(),
			exec: func(t *testing.T, ctx context.Context, pipe redis.Pipeliner) (keys []string, vals []string, err error) {
				pipe.Get(ctx, "non-existent-key")
				return keys, vals, nil
			},
			wantErr: func(t *testing.T, err error) { assert.NoError(t, err) },
			options: func(ctx context.Context, adp transactor2.Port) (out []func(opt any) error) {
				return append(out, WithTXNonExistingKeyAccess())
			},
		},
		{
			name: "transaction with error in filling pipeline (rollback)",
			ctx:  context.Background(),
			exec: func(t *testing.T, ctx context.Context, pipe redis.Pipeliner) (keys []string, vals []string, err error) {
				for i := 0; i < 10; i++ {
					key := fmt.Sprintf("key%d", i)
					pipe.Set(ctx, key, fmt.Sprintf("%d", i), 0)
					keys = append(keys, key)
					vals = append(vals, fmt.Sprintf("%d", i))
				}
				return keys, vals, fmt.Errorf("error in pipeline")
			},
			options: func(ctx context.Context, adp transactor2.Port) (out []func(opt any) error) {
				return nil
			},
			wantErr: func(t *testing.T, err error) { assert.Error(t, err) },
		},
		{
			name: "transaction with error in execution pipeline (no rollback)",
			ctx:  context.Background(),
			exec: func(t *testing.T, ctx context.Context, pipe redis.Pipeliner) (keys []string, vals []string, err error) {
				for i := 0; i < 10; i++ {
					key := fmt.Sprintf("key%d", i)
					pipe.Set(ctx, key, fmt.Sprintf("%d", i), 0)
					keys = append(keys, key)
					vals = append(vals, fmt.Sprintf("%d", i))
				}
				pipe.Set(ctx, "errorkey", "error", 0)
				pipe.Incr(ctx, "errorkey") //Cannot increment a string value => error in execute pipeline
				return keys, vals, nil
			},
			options: func(ctx context.Context, adp transactor2.Port) (out []func(opt any) error) {
				return nil
			},
			wantErr: func(t *testing.T, err error) {
				var target *transactor2.ErrorCommitFailed
				if errors.Is(err, target) {
					return
				}
				t.Errorf("wrong error type want:%s got:%s ", reflect.TypeOf(target), reflect.TypeOf(err))
			},
		},
		{
			name: "transaction with preparation and finish function (no-error)",
			ctx:  context.Background(),
			exec: func(t *testing.T, ctx context.Context, pipe redis.Pipeliner) (keys []string, vals []string, err error) {
				for i := 0; i < 10; i++ {
					key := fmt.Sprintf("key%d", i)
					pipe.Set(ctx, key, fmt.Sprintf("%d", i), 0)
					keys = append(keys, key)
					vals = append(vals, fmt.Sprintf("%d", i))
				}
				return keys, vals, nil
			},
			options: func(ctx context.Context, adp transactor2.Port) (out []func(opt any) error) {
				preparationFunc := func(ctx context.Context) error {
					anyVal, err := adp.GetTransaction(ctx)
					assert.NoError(t, err)

					pipe := anyVal.(redis.Pipeliner)
					pipe.Set(ctx, tID, "preparation", 0)
					return nil
				}

				complFunc := func(ctx context.Context) error {
					anyVal, err := adp.GetTransaction(ctx)
					assert.NoError(t, err)

					pipe := anyVal.(redis.Pipeliner)
					pipe.Get(ctx, tID) // if the key would not exists, the test would fail
					return nil
				}

				return append(out, WithTXPreparation(preparationFunc), WithTXCompletion(complFunc))
			},
			wantErr: func(t *testing.T, err error) { assert.NoError(t, err) },
		},
		{
			name: "transaction with preparation and finish function (error)",
			ctx:  context.Background(),
			exec: func(t *testing.T, ctx context.Context, pipe redis.Pipeliner) (keys []string, vals []string, err error) {
				for i := 0; i < 10; i++ {
					key := fmt.Sprintf("key%d", i)
					pipe.Set(ctx, key, fmt.Sprintf("%d", i), 0)
					keys = append(keys, key)
					vals = append(vals, fmt.Sprintf("%d", i))
				}
				return keys, vals, nil
			},
			options: func(ctx context.Context, adp transactor2.Port) (out []func(opt any) error) {
				preparationFunc := func(ctx context.Context) error {
					anyVal, err := adp.GetTransaction(ctx)
					assert.NoError(t, err)

					pipe := anyVal.(redis.Pipeliner)
					pipe.Set(ctx, tID, "preparation", 0)
					return nil
				}

				complFunc := func(ctx context.Context) error {
					anyVal, err := adp.GetTransaction(ctx)
					assert.NoError(t, err)

					pipe := anyVal.(redis.Pipeliner)
					pipe.Get(ctx, tID+"1") //  key does not exists => error
					return nil
				}

				return append(out, WithTXPreparation(preparationFunc), WithTXCompletion(complFunc))
			},
			wantErr: func(t *testing.T, err error) { assert.Error(t, err) },
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			adp := _TestPersistent()
			var keys, vals []string
			errTx := adp.ExecWithinTransaction(tt.ctx, func(txCtx context.Context) error {
				anyVal, err := adp.GetTransaction(txCtx)
				pipe := anyVal.(redis.Pipeliner)

				pipe.Select(tt.ctx, writeDB)
				keys, vals, err = tt.exec(t, tt.ctx, pipe)
				return err
			}, tt.options(tt.ctx, adp)...)
			tt.wantErr(t, errTx)

			if errTx == nil {
				assertKeys(t, tt.ctx, keys, vals)
			}
			test2.FlushDB(t, tt.ctx, redisClient)
		})

	}
}

func Test_TxExecutionWithWatch(t *testing.T) {
	ctx := context.Background()
	adp := _TestPersistent()

	createKeyAndValuesWithOffset := func(offset int64) (keys []string, vals []string) {
		for i := 0; i < 5; i++ {
			key := fmt.Sprintf("key%d", i)
			keys = append(keys, key)
			vals = append(vals, fmt.Sprintf("%d", int64(i)*offset))
		}
		return keys, vals
	}

	// transaction function
	fillPipe := func(t *testing.T, ctx context.Context, pipe redis.Pipeliner, keys, vals []string) {
		for i, key := range keys {
			pipe.Set(ctx, key, vals[i], 0)
		}
	}

	// create same keys and different values
	key1, val1 := createKeyAndValuesWithOffset(0)  // written by the first transaction
	key2, val2 := createKeyAndValuesWithOffset(10) // written by the second transaction

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		errTx := adp.ExecWithinTransaction(ctx, func(txCtx context.Context) error {
			anyVal, err := adp.GetTransaction(txCtx)
			assert.NoError(t, err)
			pipe := anyVal.(redis.Pipeliner)
			fillPipe(t, txCtx, pipe, key1, val1)

			// wait for the second transaction to finish
			wg.Done()
			time.Sleep(10 * time.Millisecond)
			return nil
		}, WithTXWatchKeys(key1...))

		//values of key1 has changed due to the second transaction, so the transaction should fail
		if errTx == nil {
			t.Error("expected error on optimistic lock")
		}
		var wantErr *transactor2.ErrorLockFailed
		if !assert.ErrorAs(t, errTx, &wantErr) {
			t.Error(fmt.Errorf("wrong expected error type want:%s got:%s ", reflect.TypeOf(wantErr), reflect.TypeOf(errTx)))
		}

		wg.Done()
	}()

	// wait here until the first transaction finished filling its pipeline
	wg.Wait()
	// start second transaction with changed key values incl. key1
	wg.Add(1)

	errTx := adp.ExecWithinTransaction(ctx, func(txCtx context.Context) error {
		anyVal, err := adp.GetTransaction(txCtx)
		assert.NoError(t, err)

		pipe := anyVal.(redis.Pipeliner)
		fillPipe(t, txCtx, pipe, key2, val2)
		return nil
	})
	assert.NoError(t, errTx)
	assertKeys(t, ctx, key1, val2)
	// wait here until the first transaction finished
	wg.Wait()
}

func assertKeys(t *testing.T, ctx context.Context, keys []string, value []string) {
	adp := _TestPersistent()

	var result []*redis.StringCmd
	errTx := adp.ExecWithoutTransaction(ctx, func(txCtx context.Context) error {
		anyVal, err := adp.GetTransaction(txCtx)
		assert.NoError(t, err)
		pipe := anyVal.(redis.Pipeliner)

		for _, key := range keys {
			getCmd := pipe.Get(ctx, key)
			result = append(result, getCmd)
		}
		return nil
	})
	assert.NoError(t, errTx)
	for i, cmd := range result {
		assert.Equal(t, value[i], cmd.Val())
	}
}
