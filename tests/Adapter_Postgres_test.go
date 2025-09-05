//go:build integration

package tests

import (
	"context"
	"errors"
	"fmt"
	"github.com/global-soft-ba/go-eventstore"
	hexStore "github.com/global-soft-ba/go-eventstore/eventstore"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/port/persistence"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/port/persistence/transactor"
	"github.com/global-soft-ba/go-eventstore/eventstore/infrastructure/persistence/postgres"
	"github.com/global-soft-ba/go-eventstore/instrumentation/port/logger"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"testing"
)

const (
	Host               = "localhost"
	Port               = "5433"
	Username           = "postgres"
	Password           = "docker"
	DB                 = "eventstore_testing"
	MaxOpenConnections = 5
)

func init() {
	var err error

	pool, err = NewDbPool()
	if err != nil {
		panic("error in init function of integration")
	}

	// To migrate before testcases starts
	_, err = postgres.New(pool, postgres.Options{AutoMigrate: true})
	if err != nil {
		logger.Error(err)
		panic("error in init function of test package sql adapter")
	}

}

var pool *pgxpool.Pool

func NewDbPool() (*pgxpool.Pool, error) {
	ctx := context.Background()
	var conf *pgxpool.Config
	var err error
	dbURI := fmt.Sprintf("host=%s port=%s user=%s dbname=%s sslmode=disable password=%s",
		Host,
		Port,
		Username,
		DB,
		Password)
	conf, err = pgxpool.ParseConfig(dbURI)
	if err != nil {
		panic(err)
	}
	conf.MaxConns = int32(MaxOpenConnections)

	return pgxpool.NewWithConfig(ctx, conf)

}

func _PostgresTransactor() transactor.Port {
	return postgres.NewTransactor(pool)
}

func NewTestPassThroughSQLAdapter(pool *pgxpool.Pool) persistence.Port {
	// do not try to migrate before each test
	// take much longer and waste db-connections, because it cannot use the db-pool
	var opt = postgres.Options{AutoMigrate: false}
	adp, err := postgres.NewTxPassThrough(pool, opt)
	if err != nil {
		panic("error in start up sql adapter")
	}

	return adp
}

func NewTestTxStoredSQLAdapter(txCtx context.Context, pool *pgxpool.Pool) persistence.Port {
	// do not try to migrate before each test
	// take much longer and waste db-connections, because it cannot use the db-pool
	var opt = postgres.Options{AutoMigrate: false}
	adp, err := postgres.NewTXStored(txCtx, pool, opt)
	if err != nil {
		panic("error in start up sql adapter")
	}

	return adp
}

func NewTestSQLAdapter(pool *pgxpool.Pool) persistence.Port {
	// do not try to migrate before each test
	// take much longer and waste db-connections, because it cannot use the db-pool
	var opt = postgres.Options{AutoMigrate: false}
	adp, err := postgres.New(pool, opt)
	if err != nil {
		panic("error in start up sql adapter")
	}

	return adp
}

func NewTestSlowSQLAdapter(pool *pgxpool.Pool) persistence.Port {
	adp, err := postgres.NewSlowAdapter(pool)
	if err != nil {
		panic("error in start up sql adapter")
	}

	return adp
}

func NewEventStoreSQLWithTxCTX(txCtx context.Context, adapter persistence.Port) event.EventStore {
	evt, err, resCh := hexStore.NewForTestWithTxCTX(txCtx, adapter)
	if err != nil {
		panic("error in test to init event store")
	}

	for elem := range resCh {
		if elem != nil {
			panic("error in test to init event store")
		}
	}

	return evt
}

func NewEventStoreSQL(_ context.Context, adapter persistence.Port) event.EventStore {
	evt, err, resCh := hexStore.New(adapter)
	if err != nil {
		panic("error in test to init event store")
	}

	for elem := range resCh {
		if elem != nil {
			panic("error in test to init event store")
		}
	}

	return evt
}

func cleanUp(pool *pgxpool.Pool) {
	cleanUpDb(pool)
	cleanRegistries()
}

func cleanUpDb(pool *pgxpool.Pool) {
	_, err := pool.Exec(context.Background(),
		"BEGIN;  "+
			"TRUNCATE eventstore.aggregates ;"+
			"TRUNCATE eventstore.aggregates_snapshots ;"+
			"TRUNCATE eventstore.aggregates_events ;"+
			"TRUNCATE eventstore.projections ;"+
			"TRUNCATE eventstore.projections_events ;"+
			"select pg_advisory_unlock_all();"+
			"COMMIT;",
	)
	var pgError *pgconn.PgError
	if err != nil {
		switch {
		case errors.As(err, &pgError):
			if pgError.Code == "3F000" { //tables does not exists (init case)
			}
		default:
			panic(err)
		}
	}
}

func TestSaveAggregateSQL(t *testing.T) {
	testSaveAggregate(t,
		_PostgresTransactor(),
		func(txCtx context.Context) event.EventStore {
			store := NewEventStoreSQLWithTxCTX(txCtx, NewTestTxStoredSQLAdapter(txCtx, pool))
			return store
		}, func() { cleanUp(pool) })
}

func TestSaveAggregateWithValidTimeSQL(t *testing.T) {
	testSaveAggregateWithValidTime(t,
		_PostgresTransactor(),
		func(txCtx context.Context) event.EventStore {
			store := NewEventStoreSQLWithTxCTX(txCtx, NewTestTxStoredSQLAdapter(txCtx, pool))
			return store
		}, func() { cleanUp(pool) })
}

func TestSaveAggregatesSQL(t *testing.T) {
	testSaveAggregates(t, func() persistence.Port { return NewTestSQLAdapter(pool) }, func() { cleanUp(pool) })
}

func TestSaveAggregatesConcurrentlySQL(t *testing.T) {
	testSaveAggregatesConcurrently(t, func() persistence.Port {
		return NewTestSlowSQLAdapter(pool)
	}, func() { cleanUpDb(pool) })
}

func TestSaveAggregateWithConcurrentModificationExceptionSQL(t *testing.T) {
	testSaveAggregateWithConcurrentModificationException(t, func() persistence.Port { return NewTestSQLAdapter(pool) }, func() { cleanUp(pool) })
}

func TestSaveAggregateWithEphemeralEventTypesSQL(t *testing.T) {
	testSaveAggregateWithEphemeralEventTypes(t, func() persistence.Port { return NewTestSQLAdapter(pool) }, func() { cleanUp(pool) })
}

func TestSaveAggregateWithSnapshotsSQL(t *testing.T) {
	testSaveAggregateWithSnapShot(t, func() persistence.Port { return NewTestSQLAdapter(pool) }, func() { cleanUp(pool) })
}

func TestSaveAggregatesWithSnapshotsSQL(t *testing.T) {
	testSaveAggregatesWithSnapShot(t, func() event.EventStore { return NewEventStoreSQL(context.Background(), NewTestSQLAdapter(pool)) }, func() { cleanUp(pool) })
}

func TestValidityOfSnapsShotsSQL(t *testing.T) {
	testAggregateWithSnapShotValidity(t, func() event.EventStore { return NewEventStoreSQL(context.Background(), NewTestSQLAdapter(pool)) }, func() { cleanUp(pool) })
}

func TestSaveAggregateWitProjectionSQL(t *testing.T) {
	testSaveAggregateWithProjection(t, IntegrationTest, event.ECS, func() persistence.Port { return NewTestSQLAdapter(pool) }, func() { cleanUp(pool) })
	testSaveAggregateWithProjection(t, IntegrationTest, event.CSS, func() persistence.Port { return NewTestSQLAdapter(pool) }, func() { cleanUp(pool) })
}

func TestSaveAggregatesWithProjectionSQL(t *testing.T) {
	testSaveAggregatesWithProjection(t, event.ECS, func() persistence.Port { return NewTestSQLAdapter(pool) }, func() { cleanUp(pool) })
	testSaveAggregatesWithProjection(t, event.CCS, func() persistence.Port { return NewTestSQLAdapter(pool) }, func() { cleanUp(pool) })
}

func TestSaveAggregatesWithConcurrentProjectionSQL(t *testing.T) {
	testSaveAggregatesWithConcurrentProjections(t, func() persistence.Port { return NewTestSQLAdapter(pool) }, func() { cleanUp(pool) })
}

func TestRebuildProjectionSQL(t *testing.T) {
	testRebuildProjection(t, func() persistence.Port { return NewTestSQLAdapter(pool) }, func() { cleanUp(pool) })
}

func TestRebuildProjectionSinceSQL(t *testing.T) {
	testRebuildProjectionSince(t, func() persistence.Port {
		return NewTestSQLAdapter(pool)
	}, func() { cleanUp(pool) })
}

func TestRebuildAllProjectionSQL(t *testing.T) {
	testRebuildAllProjection(t, func() persistence.Port {
		return NewTestSQLAdapter(pool)
	}, func() { cleanUp(pool) })
}

func TestSaveAggregatesConcurrentWithProjectionSQL(t *testing.T) {
	testSaveAggregatesConcurrentWithProjection(t, func() persistence.Port { return NewTestSQLAdapter(pool) }, func() { cleanUp(pool) })
}

func TestInitAdapterWithExistingProjectionsSQL(t *testing.T) {
	testInitAdapterWithExistingProjections(t, func() persistence.Port { return NewTestSQLAdapter(pool) }, func() { cleanUp(pool) })
}

func TestInitAdapterWithInitialTenantsSQL(t *testing.T) {
	testInitAdapterWithInitialTenants(t, func() persistence.Port { return NewTestSQLAdapter(pool) }, func() { cleanUp(pool) })
}

func TestInitAdapterWithNewProjectionSQL(t *testing.T) {
	testInitAdapterWithNewProjection(t, func() persistence.Port { return NewTestSQLAdapter(pool) }, func() { cleanUp(pool) })
}

func TestExecuteAllExistingProjectionsSQL(t *testing.T) {
	testExecuteAllExistingProjections(t, func() persistence.Port { return NewTestSQLAdapter(pool) }, func() { cleanUp(pool) })
}

func TestLoadAggregateAsAtSQL(t *testing.T) {
	testLoadAggregateAsAt(t, func() event.EventStore { return NewEventStoreSQL(context.Background(), NewTestSQLAdapter(pool)) }, func() { cleanUp(pool) })
}

func TestLoadAggregateAsOfSQL(t *testing.T) {
	testLoadAggregateAsOf(t, func() event.EventStore { return NewEventStoreSQL(context.Background(), NewTestSQLAdapter(pool)) }, func() { cleanUp(pool) })
}

func TestLoadAggregateAsOfTillSQL(t *testing.T) {
	testLoadAggregateAsOfTill(t, func() event.EventStore {
		return NewEventStoreSQL(context.Background(), NewTestSQLAdapter(pool))
	}, func() { cleanUp(pool) })
}

func TestLoadAllAggregatesOfAggregateAsAtSQL(t *testing.T) {
	testLoadAllAggregatesOfAggregateAsAt(t, func() event.EventStore {
		return NewEventStoreSQL(context.Background(), NewTestSQLAdapter(pool))
	}, func() { cleanUp(pool) })
}

func TestLoadAllAggregatesOfAggregateAsOfSQL(t *testing.T) {
	testLoadAllAggregatesOfAggregateAsOf(t, func() event.EventStore {
		return NewEventStoreSQL(context.Background(), NewTestSQLAdapter(pool))
	}, func() { cleanUp(pool) })
}

func TestLoadAllAggregatesOfAggregateAsOfTillSQL(t *testing.T) {
	testLoadAllAggregatesOfAggregateAsOfTill(t, func() event.EventStore {
		return NewEventStoreSQL(context.Background(), NewTestSQLAdapter(pool))
	}, func() { cleanUp(pool) })
}

func TestLoadAllAggregatesAsAtSQL(t *testing.T) {
	testLoadAllAggregatesAsAt(t, func() event.EventStore {
		return NewEventStoreSQL(context.Background(), NewTestSQLAdapter(pool))
	}, func() { cleanUp(pool) })
}

func TestLoadAllAggregatesAsOfSQL(t *testing.T) {
	testLoadAllAggregatesAsOf(t, func() event.EventStore {
		return NewEventStoreSQL(context.Background(), NewTestSQLAdapter(pool))
	}, func() { cleanUp(pool) })
}

func TestLoadAllAggregatesAsOfTillSQL(t *testing.T) {
	testLoadAllAggregatesAsOfTill(t, func() event.EventStore {
		return NewEventStoreSQL(context.Background(), NewTestSQLAdapter(pool))
	}, func() { cleanUp(pool) })
}

func TestGetAggregateStateSQL(t *testing.T) {
	testGetAggregateStates(t, func() event.EventStore {
		return NewEventStoreSQL(context.Background(), NewTestSQLAdapter(pool))
	}, func() { cleanUp(pool) })
}

func TestGetAggregateStatesForAggregateTypeSQL(t *testing.T) {
	testGetAggregateStatesForAggregateType(t, func() event.EventStore {
		return NewEventStoreSQL(context.Background(), NewTestSQLAdapter(pool))
	}, func() { cleanUp(pool) })
}

func TestGetAggregateStatesForAggregateTypeTillSQL(t *testing.T) {
	testGetAggregateStatesForAggregateTypeTill(t, func() event.EventStore {
		return NewEventStoreSQL(context.Background(), NewTestSQLAdapter(pool))
	}, func() { cleanUp(pool) })
}

func TestGetPatchFreePeriodsForIntervalSQL(t *testing.T) {
	testGetPatchFreePeriodsForInterval(t, func() persistence.Port {
		return NewTestSQLAdapter(pool)
	}, func() { cleanUp(pool) })
}

func TestGetProjectionStatesSQL(t *testing.T) {
	testGetProjectionStates(t, func() persistence.Port {
		return NewTestSQLAdapter(pool)
	}, func() { cleanUp(pool) })
}

func TestGetAggregatesEventsSQL(t *testing.T) {
	testGetAggregatesEventsSortAndSearch(t, func() persistence.Port { return NewTestSQLAdapter(pool) }, func() { cleanUp(pool) })
	testGetAggregatesEventsPaginated(t, func() persistence.Port { return NewTestSQLAdapter(pool) }, func() { cleanUp(pool) })
}

func TestRemoveProjectionSQL(t *testing.T) {
	testRemoveProjection(t, func() persistence.Port { return NewTestSQLAdapter(pool) }, func() { cleanUp(pool) })
}

func TestDeleteEventSQL(t *testing.T) {
	testDeleteEvents(t, func() persistence.Port { return NewTestSQLAdapter(pool) }, func() { cleanUp(pool) })
	testProjectionsAfterDeleteEvents(t, func() persistence.Port { return NewTestSQLAdapter(pool) }, func() { cleanUp(pool) })
}
