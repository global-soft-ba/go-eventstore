package postgres

import (
	"context"
	"fmt"
	sq "github.com/Masterminds/squirrel"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/port/persistence"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/port/persistence/transactor"
	"github.com/global-soft-ba/go-eventstore/eventstore/infrastructure/persistence/postgres/internal"
	"github.com/jackc/pgx/v5/pgxpool"
)

var Rollback = fmt.Errorf("rollback error")
var Commit error = nil

func NewSlowAdapter(db *pgxpool.Pool) (persistence.Port, error) {
	trans := internal.NewTransactor(db)
	aggRepro := internal.NewSlowAggregatePort(dataBaseSchema, sq.Dollar, trans)
	projRepro := internal.NewProjecter(dataBaseSchema, sq.Dollar, trans)

	if err := applyMigration(context.Background(), dataBaseSchema, db); err != nil {
		return nil, err
	}

	return Adapter{aggregates: aggRepro, projections: projRepro, transactor: trans}, nil
}

func NewTransactor(dbPool *pgxpool.Pool) transactor.Port {
	return internal.NewTransactor(dbPool)
}
