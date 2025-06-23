package projection

import (
	"context"
	"fmt"
	"github.com/global-soft-ba/go-eventstore"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/application/local/commandBus"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/application/local/commandBus/commands"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/application/local/eventBus"
	domainEvents2 "github.com/global-soft-ba/go-eventstore/eventstore/core/application/local/eventBus/domainEvents"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/application/registry"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/application/repository"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/application/services/projection/executors"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/domain/model/projection"
	transactor2 "github.com/global-soft-ba/go-eventstore/eventstore/core/port/persistence/transactor"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/port/scheduler"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/shared"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/shared/rateWorker"
	scheduler2 "github.com/global-soft-ba/go-eventstore/eventstore/infrastructure/scheduler"
	"github.com/global-soft-ba/go-eventstore/instrumentation/port/logger"
	"github.com/global-soft-ba/go-eventstore/instrumentation/port/metrics"
	"time"
)

func NewProjectionService(projRepro repository.ProjectionRepositoryInterface, transactor transactor2.Port, evtBus *eventBus.EventPublisher, cmdBus *commandBus.CommandPublisher, registries *registry.Registries) ProjectionService {
	srv := ProjectionService{
		projectionRepository: projRepro,
		transactor:           transactor,
		scheduler:            scheduler2.NewAdapter(),
		registries:           registries,
	}

	evtBus.Subscribe(&srv, domainEvents2.ProjectionSaved{}, domainEvents2.StoreStarted{}, domainEvents2.FuturePatchSaved{})
	cmdBus.Subscribe(&srv, commands.CreateTenant{}, commands.ExecuteProjection{}, commands.DeleteEvent{})
	return srv
}

type ProjectionService struct {
	projectionRepository repository.ProjectionRepositoryInterface
	scheduler            scheduler.Port
	transactor           transactor2.Port
	registries           *registry.Registries

	retryAfterMilliseconds []time.Duration
}

// --------------------------------------Inter Service Communication---------------------------------------------------

func (p *ProjectionService) Execute(ctx context.Context, cmd commandBus.Command) error {
	switch actCMD := cmd.(type) {
	case commands.ExecuteProjection:
		return p.ConsistentProjection(ctx, actCMD.Streams())
	case commands.CreateTenant:
		return p.InitProjectionServiceForNewTenant(ctx, actCMD.ID())
	case commands.DeleteEvent:
		return p.deleteEvent(ctx, actCMD.Streams())
	default:
		return fmt.Errorf("command %q not found", cmd.Name())
	}
}

func (p *ProjectionService) Receive(ctx context.Context, evt eventBus.Event) chan error {
	switch actEvent := evt.(type) {
	case domainEvents2.ProjectionSaved:
		return p.EventualConsistentProjection(ctx, actEvent.Id(), actEvent.EarliestHPatch())
	case domainEvents2.StoreStarted:
		// starting of the eventstore
		return p.RestartProjectionService(ctx)
	case domainEvents2.FuturePatchSaved:
		return p.AddTaskToScheduler(ctx, actEvent.Task())
	default:
		return nil
	}
}

// ---------------------------------------Tenant Management------------------------------------------------------------

// InitProjectionServiceForNewTenant initializes the projection service for a new tenant.
// Due to the fact that we allow new TenantIDs to be added dynamically at runtime, it is not possible to initialize
// all tenants and associated projections upfront during the start of the service. Instead, while saving an
// aggregate (see SaverService), we always check if the given tenant is already known, if not, it will be initialized
// with this function.
//
// All projections already contained in the database are initialized during startup via RestartProjectionService.
func (p *ProjectionService) InitProjectionServiceForNewTenant(ctx context.Context, tenantID string) error {
	if err := p.registerTenant(ctx, tenantID); err != nil {
		return fmt.Errorf("register tenant failed:%w", err)
	}

	projIDs, err := p.initProjectionWorkers(tenantID)
	if err != nil {
		return fmt.Errorf("init projection service for workers failed:%w", err)
	}

	if err = p.createProjection(ctx, projIDs...); err != nil {
		return fmt.Errorf("init projections service for projections failed:%w", err)
	}

	return nil
}

func (p *ProjectionService) registerTenant(_ context.Context, tenantID string) error {
	if p.registries.TenantRegistry.Exists(tenantID) {
		return nil // tenant already registered
	}

	if err := p.registries.TenantRegistry.Register(tenantID); err != nil {
		return fmt.Errorf("register tenant %q failed:%w", tenantID, err)
	}

	return nil
}

func (p *ProjectionService) initProjectionWorkers(tenantID string) ([]shared.ProjectionID, error) {
	var projIDs []shared.ProjectionID
	for _, proj := range p.registries.ProjectionRegistry.All() {
		var sharedID shared.ProjectionID
		var err error
		if sharedID, err = p.initProjectionWorker(tenantID, proj.ID()); err != nil {
			return nil, err
		}
		projIDs = append(projIDs, sharedID)
	}
	return projIDs, nil
}

func (p *ProjectionService) initProjectionWorker(tenantID string, projectionID string) (projID shared.ProjectionID, err error) {
	sharedProjID := shared.ProjectionID{TenantID: tenantID, ProjectionID: projectionID}

	if err = p.registries.WorkerRegistry.CreateAndStart(sharedProjID, p.registries.ProjectionRegistry.Options(projectionID).InputQueueLength); err != nil {
		return shared.ProjectionID{}, fmt.Errorf("start worker failed for %v: %w", sharedProjID, err)
	}

	return sharedProjID, nil
}

// rateLimitedProjectionExecution: used an internal a rate limit worker with a dedicated queue to execute the projection.
// If we have multiple projection request during a (executing) projection and more then the given rate limit (defined in worker registry),
// the worker will ignore such additional requests.
// For example,
// if we have 10 parallel request and the queue has length=2, we will execute the first (1) and last (10) projection request, only.
// If after finishing the first request and before finishing the last request, we get a new request, the worker will execute them
// after he finished the last request.
func (p *ProjectionService) rateLimitedProjectionExecution(ctx context.Context, id shared.ProjectionID, execute func(ctx context.Context, id shared.ProjectionID) error) chan error {
	errCh := make(chan error, 1)
	queue, err := p.registries.WorkerRegistry.Queue(id)
	if err != nil {
		errCh <- err
		close(errCh)
		return errCh
	}

	executionParams := rateWorker.ExecutionParams[error]{
		Ctx:      ctx,
		ResultCh: errCh,
		Execute:  func(ctx context.Context) error { return execute(ctx, id) },
	}
	queue <- executionParams
	return errCh
}

func (p *ProjectionService) createProjection(ctx context.Context, projIDs ...shared.ProjectionID) error {
	errTx := p.transactor.WithinTX(ctx, func(txCtx context.Context) (err error) {
		// in init case, due to concurrency, we have to lock the projectionsIDs,
		//so that we don't overwrite yet parallel running inits/creates
		if err = p.projectionRepository.Lock(txCtx, projIDs...); err != nil {
			return fmt.Errorf("lock of projections failed: %w", err)
		}

		defer func() {
			if errUnLock := p.projectionRepository.UnLock(txCtx, projIDs...); errUnLock != nil {
				logger.Error(fmt.Errorf("unlocking of projections failed: %w", errUnLock))
			}
		}()

		// we have to look first if the projection was already created in database due to other pods
		// if so, we don't have to create it again
		var toInitProjections []shared.ProjectionID
		toInitProjections, err = p.projectionRepository.GetNotInitialized(txCtx, projIDs...)
		if err != nil {
			return fmt.Errorf("get not-initialized projections failed: %w", err)
		}

		// projection already exists (or was created in between)
		if len(toInitProjections) == 0 {
			return nil
		}

		if _, err = p.projectionRepository.Create(txCtx, toInitProjections...); err != nil {
			return fmt.Errorf("init of projections failed: %w", err)
		}

		return nil
	})

	return errTx
}

///--------------------------------------------- Projections-------------------------------------------------------

func (p *ProjectionService) ConsistentProjection(txCtx context.Context, streams []projection.Stream) error {
	txCtx, endSpan := metrics.StartSpan(txCtx, "ConsistentProjection", map[string]interface{}{"numberOfStreams": len(streams)})
	defer endSpan()

	for _, stream := range streams {
		err := p.executeProjection(txCtx, executors.NewConsistentProjectionExecutor(p.transactor, p.projectionRepository, stream))
		if err != nil {
			return fmt.Errorf("consistent execution of projection %q failed: %w", stream.ID(), err)
		}
	}
	return nil
}

func (p *ProjectionService) EventualConsistentProjection(ctx context.Context, id shared.ProjectionID, hPatch time.Time) chan error {
	ctx, endSpan := metrics.StartSpan(ctx, "EventualConsistentProjection", map[string]interface{}{"tenantID": id.TenantID, "projectionID": id.ProjectionID, "hPatch": hPatch})
	defer endSpan()

	return p.rateLimitedProjectionExecution(ctx, id, func(ctx context.Context, id shared.ProjectionID) error {
		opt := p.registries.ProjectionRegistry.Options(id.ProjectionID)

		return p.executeProjection(ctx, executors.NewEventualConsistentProjectionExecutor(p.transactor, p.projectionRepository, id, opt, hPatch))
	})
}

func (p *ProjectionService) executeProjection(ctx context.Context, executor executors.IExecuter) error {
	if !executor.HasHPatch() {
		return executor.Run(ctx)
	} else {
		switch executor.GetOptions().HPatchStrategy {
		case event.Rebuild:
			return executor.Rebuild(ctx)
		case event.RebuildSince:
			return executor.RebuildSince(ctx, executor.GetHPatch())
		case event.Manual:
			return nil
		case event.Projected:
			return executor.Projected(ctx)
		case event.Error:
			return fmt.Errorf("historical patch found: historical patch strategy %q is used", executor.GetOptions().HPatchStrategy)
		default:
			return fmt.Errorf("historical patch strategy %q not found", executor.GetOptions().HPatchStrategy)
		}
	}
}

///--------------------------------------------------- Rebuild ---------------------------------------------------------

// Rebuild rebuilds a projection in chunks until the projection is finished.
//
//	Consistent projections and eventual consistent projections are treated the same, BUT:
//	In case of eventual consistent projection, it is still possible to store new events during the rebuild in the event store.
//
//	In case of consistent projection, it is not possible to store any new events during the rebuild in the store until
//	the rebuild is successfully finished (save would fail with error).

func (p *ProjectionService) Rebuild(ctx context.Context, id shared.ProjectionID, since time.Time) chan error {
	ctx, endSpan := metrics.StartSpan(ctx, "Rebuild", map[string]interface{}{"tenantID": id.TenantID, "projectionID": id.ProjectionID, "sinceTime": since})
	defer endSpan()

	executor := executors.NewEventualConsistentProjectionExecutor(p.transactor, p.projectionRepository, id, p.registries.ProjectionRegistry.Options(id.ProjectionID), time.Time{})
	if err := executor.RebuildSince(ctx, since); err != nil {
		errCh := make(chan error, 1)
		errCh <- fmt.Errorf("rebuild failed for projection %s of tenant %s: %w", id.ProjectionID, id.TenantID, err)
		close(errCh)
		return errCh
	}

	// start projection (new events might have been added during rebuilding of an eventual consistent projections)
	// consistent projection will not have any new events (see above) - so they will not be executed and no error will be returned
	return p.EventualConsistentProjection(ctx, id, time.Time{})
}

/// -------------------------------------------------HardDelete Event-------------------------------------------------------

// deleteEvent deletes an event from the projection. This is always done in a consistent projection execution.
func (p *ProjectionService) deleteEvent(ctx context.Context, streams []projection.Stream) error {
	for _, stream := range streams {
		if err := p.executeProjectionsToDeleteEvent(ctx, executors.NewConsistentProjectionExecutor(p.transactor, p.projectionRepository, stream)); err != nil {
			return fmt.Errorf("error deleting event: %w", err)
		}
	}

	return nil
}

func (p *ProjectionService) executeProjectionsToDeleteEvent(ctx context.Context, executor executors.IExecuter) error {
	switch executor.GetOptions().DPatchStrategy {
	case event.Rebuild:
		return executor.Rebuild(ctx)
	case event.RebuildSince:
		return executor.RebuildSince(ctx, executor.GetDPatch())
	case event.Manual:
		return nil
	case event.Projected:
		return executor.Projected(ctx)
	case event.Error:
		return fmt.Errorf("delete patch found: delete patch strategy %q is used", executor.GetOptions().DPatchStrategy)
	default:
		return fmt.Errorf("delete patch strategy %q not found", executor.GetOptions().DPatchStrategy)
	}
}

/// ---------------------------------------------------- Scheduler -----------------------------------------------------

func (p *ProjectionService) AddTaskToScheduler(ctx context.Context, task scheduler.ScheduledProjectionTask) chan error {
	resCh := make(chan error, 1)
	go func(resCh chan error) {
		if err := p.scheduler.AddTasks(ctx, task); err != nil {
			resCh <- fmt.Errorf("AddTaskToScheduler() failed for %q:%w", task, err)
		}
		close(resCh)
	}(resCh)
	return resCh
}
