package persistence

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"github.com/global-soft-ba/go-eventstore/instrumentation/port/metrics"
	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/pgx/v5"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed postgres/migration/*
var migration embed.FS

type PgxClient struct {
	pool *pgxpool.Pool
}

type Options struct {
	Host           string
	Port           string
	Username       string
	Password       string
	Schema         string
	MaxConnections int
	AutoMigrate    bool
}

func NewPgxClient(ctx context.Context, options Options) (*pgxpool.Pool, error) {
	ctx, endSpan := metrics.StartSpan(ctx, "init-pgx-client", nil)
	defer endSpan()

	dbURI := getDatabaseConnectionString(options)
	conf, err := pgxpool.ParseConfig(dbURI)
	if err != nil {
		return nil, err
	}
	conf.MaxConns = int32(options.MaxConnections)
	if options.AutoMigrate {
		if err = applyMigrations(ctx, options); err != nil {
			return nil, err
		}
	}
	pool, err := pgxpool.NewWithConfig(ctx, conf)
	if err != nil {
		return nil, err
	}
	return pool, nil
}

func getDatabaseConnectionString(options Options) string {
	return fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		options.Host,
		options.Port,
		options.Username,
		options.Password,
		options.Schema)
}

func applyMigrations(ctx context.Context, options Options) (err error) {
	_, endSpan := metrics.StartSpan(ctx, "apply-migrations(postgres)", nil)
	defer endSpan()
	d, err := iofs.New(migration, "postgres/migration")
	if err != nil {
		return fmt.Errorf("could not apply migrations: %w", err)
	}

	dbURI := fmt.Sprintf("pgx5://%s:%s@%s:%s/%s",
		options.Username,
		options.Password,
		options.Host,
		options.Port,
		options.Schema)

	m, err := migrate.NewWithSourceInstance("iofs", d, dbURI)
	if err != nil {
		return fmt.Errorf("could not apply migrations: %w", err)
	}

	if err = m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return fmt.Errorf("could not apply migrations: %w", err)
	}
	return nil
}
