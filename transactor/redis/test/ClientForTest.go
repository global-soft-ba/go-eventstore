package test

import (
	"context"
	"github.com/redis/go-redis/v9"
)

func ConnectionParamsForTest(options redis.Options, db ...int) redis.Options {
	if len(db) > 0 {
		return redis.Options{
			Addr:     options.Addr,
			DB:       db[0],
			Username: options.Username,
			Password: options.Password,
		}
	}

	return redis.Options{
		Addr:     options.Addr,
		DB:       options.DB,
		Username: options.Username,
		Password: options.Password,
	}
}

func InitPersistenceForTest(options redis.Options, db ...int) *redis.Client {
	ctx := context.Background()

	rdsOptions := ConnectionParamsForTest(options, db...)
	rds := redis.NewClient(&rdsOptions)
	_, err := rds.Ping(ctx).Result()
	if err != nil {
		panic("could not ping redis")
	}

	return rds
}
