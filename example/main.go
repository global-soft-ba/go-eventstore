package main

import (
	"context"
	projectionItem "example/core/application/projection/item"
	eventStoreRepository "example/core/application/repository/eventstore"
	serviceEvent "example/core/application/service/event"
	serviceItem "example/core/application/service/item"
	serviceProjection "example/core/application/service/projection"
	serviceSeeder "example/core/application/service/seeder"
	"example/core/domain/model/item"
	env "example/core/port/environment"
	"example/infrastructure/environment"
	"example/infrastructure/persistence"
	readModelItem "example/infrastructure/readmodel/item/postgres"
	writeModelItem "example/infrastructure/writemodel/item/postgres"
	"example/presentation/rest"
	"fmt"
	"github.com/global-soft-ba/go-eventstore"
	"github.com/global-soft-ba/go-eventstore/eventstore"
	eventStorePostgres "github.com/global-soft-ba/go-eventstore/eventstore/infrastructure/persistence/postgres"
	stdoutLogger "github.com/global-soft-ba/go-eventstore/instrumentation/adapter/logger/stdout"
	noopMetrics "github.com/global-soft-ba/go-eventstore/instrumentation/adapter/metrics/noop"
	"github.com/global-soft-ba/go-eventstore/instrumentation/port/logger"
	"github.com/global-soft-ba/go-eventstore/instrumentation/port/metrics"
	"reflect"
	"time"
)

func main() {
	ctx := context.Background()
	environment.InitEnvironment(env.Local)
	logger.SetLogger(stdoutLogger.Adapter{})
	metrics.SetMetrics(noopMetrics.Adapter{})

	postgresPool, err := persistence.NewPgxClient(ctx, persistence.Options{
		Host:           env.Config().DatabaseHost,
		Port:           env.Config().DatabasePort,
		Username:       env.Config().DatabaseUser,
		Password:       env.Config().DatabasePassword,
		Schema:         env.Config().DatabaseSchema,
		MaxConnections: env.Config().DatabaseMaxNumberOfConnections,
		AutoMigrate:    true,
	})
	if err != nil {
		logger.Error(fmt.Errorf("could not create postgres client: %w", err))
		return
	}
	defer postgresPool.Close()

	psqlEventStore, err := eventStorePostgres.New(postgresPool, eventStorePostgres.Options{AutoMigrate: true})
	if err != nil {
		logger.Error(err)
	}

	itemReadModel := readModelItem.NewAdapter(ctx, postgresPool)
	itemWriteModel, err := writeModelItem.NewAdapter(ctx, postgresPool)
	if err != nil {
		logger.Error(fmt.Errorf("could not create item writemodel: %v", err))
		return
	}
	itemProj := projectionItem.NewItemProjection(ctx, env.Config().ProjectionChunkSize, itemWriteModel)

	eventStore, err, errChan := eventstore.New(psqlEventStore,
		eventstore.WithConcurrentModificationStrategy(reflect.TypeOf(item.Item{}).Name(), event.Ignore),
		eventstore.WithDeleteStrategy(reflect.TypeOf(item.Item{}).Name(), event.HardDelete),

		eventstore.WithProjection(itemProj),
		eventstore.WithProjectionType(itemProj.ID(), event.ESS),
		eventstore.WithProjectionTimeOut(itemProj.ID(), env.Config().ProjectionTimeout),
		eventstore.WithRebuildTimeOut(itemProj.ID(), env.Config().ProjectionRebuildTimeout),
	)
	if err != nil {
		logger.Error(fmt.Errorf("could not create evenstore: %v", err))
		return
	}
	go logErrorChannel(errChan)

	writeRepository := eventStoreRepository.NewWriteRepository(ctx, eventStore)
	readRepository := eventStoreRepository.NewReadRepository(ctx, eventStore)

	itemService := serviceItem.NewItemService(readRepository, writeRepository, itemReadModel)
	projectionService := serviceProjection.NewProjectionService(eventStore, itemProj)
	seederService := serviceSeeder.NewSeederService(writeRepository)
	eventService := serviceEvent.NewEventService(eventStore)

	restController := rest.NewController(ctx, rest.Options{
		Port:            env.Config().HttpServerPort,
		ShutdownTimeout: 10 * time.Second,
	}, itemService, projectionService, seederService, eventService)
	err = restController.Run(ctx)
	if err != nil {
		logger.Fatal("shutting down server: %+v", err)
	}

	for {
		select {
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func logErrorChannel(errChan chan error) {
	for err := range errChan {
		if err != nil {
			logger.Error(err)
		}
	}
}
