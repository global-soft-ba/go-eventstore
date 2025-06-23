package test

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PostgresClientOptions struct {
	Host           string
	Port           string
	Username       string
	Password       string
	Schema         string
	MaxConnections int
}

func InitPersistenceForTest(options PostgresClientOptions) *pgxpool.Pool {
	ctx := context.Background()

	dbURI := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		options.Host,
		options.Port,
		options.Username,
		options.Password,
		options.Schema)
	conf, err := pgxpool.ParseConfig(dbURI)
	if err != nil {
		panic("ParseConfig fails, could not connect to postgres")
	}
	conf.MaxConns = int32(options.MaxConnections)

	pool, err := pgxpool.NewWithConfig(ctx, conf)
	if err != nil {
		panic("ConnectConfig fails, could not connect to postgres")
	}

	return pool
}
