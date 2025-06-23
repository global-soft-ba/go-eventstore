package postgres

import (
	"context"
	"embed"
	"errors"
	"fmt"
	sq "github.com/Masterminds/squirrel"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/port/persistence"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/port/persistence/aggregate"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/port/persistence/projection"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/port/persistence/transactor"
	"github.com/global-soft-ba/go-eventstore/eventstore/infrastructure/persistence/postgres/internal"
	"github.com/global-soft-ba/go-eventstore/eventstore/infrastructure/persistence/postgres/internal/tests"
	"github.com/global-soft-ba/go-eventstore/instrumentation/port/metrics"
	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/pgx/v5"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	"github.com/jackc/pgx/v5/pgxpool"
	"strconv"
	"strings"
)

const dataBaseSchema = "eventstore"

type Options struct {
	AutoMigrate bool
}

//go:embed internal/migration/*
var migration embed.FS

// NewTxPassThrough creates a new PostgreSQL event store supporting single transaction operations.
// It relies on a single transaction for all operations and expects an injected transaction or database instance from an external source.
// This implementation doesn't initiate a new transaction, except when creating the table schema with migrations (if AutoMigrate is set to true).
// The PassThrough EventStore is specifically designed for testing purposes.
func NewTxPassThrough(db *pgxpool.Pool, opt Options) (persistence.Port, error) {
	trans := tests.NewTxPassThroughTransactor(internal.CtxStorageKey)
	aggRepro := internal.NewAggregates(dataBaseSchema, sq.Dollar, trans)
	projRepro := internal.NewProjecter(dataBaseSchema, sq.Dollar, trans)

	if opt.AutoMigrate {
		if err := applyMigration(context.Background(), dataBaseSchema, db); err != nil {
			return nil, err
		}
	}

	return Adapter{aggregates: aggRepro, projections: projRepro, transactor: trans}, nil
}

// NewTXStored creates a new PostgreSQL event store supporting single transaction operations.
// It relies on a single transaction for all operations and get initialized with a transaction or database instance.
// This implementation doesn't initiate a new transaction, except when creating the table schema with migrations (if AutoMigrate is set to true).
// The PassThrough EventStore is specifically designed for testing purposes.
func NewTXStored(txCtx context.Context, db *pgxpool.Pool, opt Options) (persistence.Port, error) {
	trans := tests.NewTxStoreTransactor(txCtx, internal.CtxStorageKey)
	aggRepro := internal.NewAggregates(dataBaseSchema, sq.Dollar, trans)
	projRepro := internal.NewProjecter(dataBaseSchema, sq.Dollar, trans)

	if opt.AutoMigrate {
		if err := applyMigration(context.Background(), dataBaseSchema, db); err != nil {
			return nil, err
		}
	}

	return Adapter{aggregates: aggRepro, projections: projRepro, transactor: trans}, nil
}

func New(db *pgxpool.Pool, opt Options) (persistence.Port, error) {
	trans := internal.NewTransactor(db)
	aggRepro := internal.NewAggregates(dataBaseSchema, sq.Dollar, trans)
	projRepro := internal.NewProjecter(dataBaseSchema, sq.Dollar, trans)

	if opt.AutoMigrate {
		if err := applyMigration(context.Background(), dataBaseSchema, db); err != nil {
			return nil, err
		}
	}

	return Adapter{aggregates: aggRepro, projections: projRepro, transactor: trans}, nil
}

func applyMigration(ctx context.Context, dataBaseSchema string, db *pgxpool.Pool) (err error) {
	ctx, endSpan := metrics.StartSpan(ctx, "migrations(postgres)", nil)
	defer endSpan()

	_, err = db.Exec(ctx, fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s;", dataBaseSchema))
	if err != nil {
		return fmt.Errorf("could not apply migrations: %w", replacePasswordInError(err, db.Config().ConnConfig.Password))
	}

	d, err := iofs.New(migration, "internal/migration")
	if err != nil {
		return fmt.Errorf("could not apply migrations: %w", replacePasswordInError(err, db.Config().ConnConfig.Password))
	}

	dbURI := fmt.Sprintf("pgx5://%s:%s@%s:%s/%s?x-migrations-table=%s&x-migrations-table-quoted=true",
		db.Config().ConnConfig.User,
		db.Config().ConnConfig.Password,
		db.Config().ConnConfig.Host,
		strconv.FormatUint(uint64(db.Config().ConnConfig.Port), 10),
		db.Config().ConnConfig.Database,
		fmt.Sprintf(`"%s"."schema_migrations"`, dataBaseSchema),
	)

	m, err := migrate.NewWithSourceInstance("iofs", d, dbURI)
	if err != nil {
		return fmt.Errorf("could not apply migrations: %w", replacePasswordInError(err, db.Config().ConnConfig.Password))
	}

	if err = m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return fmt.Errorf("could not apply migrations: %w", replacePasswordInError(err, db.Config().ConnConfig.Password))
	}

	return nil
}

func replacePasswordInError(err error, password string) error {
	return fmt.Errorf("%s", strings.ReplaceAll(err.Error(), password, "*****"))
}

type Adapter struct {
	aggregates  aggregate.Port
	projections projection.Port
	transactor  transactor.Port
}

func (a Adapter) ProjectionPort() projection.Port {
	return a.projections
}

func (a Adapter) AggregatePort() aggregate.Port {
	return a.aggregates
}

func (a Adapter) Transactor() transactor.Port {
	return a.transactor
}
