package projection

import (
	"context"
	"fmt"
	"github.com/global-soft-ba/go-eventstore"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/application/mapper"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/domain/model/projection"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/shared"
	"github.com/global-soft-ba/go-eventstore/instrumentation/port/logger"
	"github.com/samber/lo"
	"slices"
	"sync"
	"time"
)

// ----------- Projection-Management -----------------------------------------------------------------------------------

func (p *ProjectionService) Start(ctx context.Context, id shared.ProjectionID) (chan error, error) {
	// We need to cover the empty database case at this point. This means that the projection has not been initialized
	// yet because the specified TenantID is new. Thus, a projection should be able to start  even if no event has been
	// saved with the tenantID, yet.
	if exists := p.registries.TenantRegistry.Exists(id.TenantID); !exists {
		if err := p.InitProjectionServiceForNewTenant(ctx, id.TenantID); err != nil {
			return nil, fmt.Errorf("init projection service failed:%w", err)
		}
	}

	if err := p.upDateProjectionStateByIDWithinTX(ctx, id, projection.Running); err != nil {
		return nil, err
	}

	return p.EventualConsistentProjection(ctx, id, time.Time{}), nil
}

func (p *ProjectionService) Stop(ctx context.Context, id shared.ProjectionID) error {
	// We need to cover the empty database case at this point. This means that the projection has not been initialized
	// yet because the specified TenantID is new. Thus, a projection should be able to stopped even if no event has been
	// saved with the tenantID, yet.
	if exists := p.registries.TenantRegistry.Exists(id.TenantID); !exists {
		if err := p.InitProjectionServiceForNewTenant(ctx, id.TenantID); err != nil {
			return fmt.Errorf("init projection service failed:%w", err)
		}
	}
	return p.upDateProjectionStateByIDWithinTX(ctx, id, projection.Stopped)
}

// upDateProjectionStateByIDWithinTX updates the state of the projection stream within a separate transaction.
// Because we just have the ID, we have to lock the projection before we retrieve the stream.
func (p *ProjectionService) upDateProjectionStateByIDWithinTX(ctx context.Context, id shared.ProjectionID, state projection.State) (err error) {
	errTx := p.transactor.WithinTX(ctx, func(txCtx context.Context) (err error) {
		defer func() {
			if errUnlock := p.projectionRepository.UnLock(txCtx, id); errUnlock != nil {
				logger.Error(fmt.Errorf("unlock of projection %q of tenant %q failed: %w", id.ProjectionID, id.TenantID, err))
			}
		}()
		if err = p.projectionRepository.Lock(txCtx, id); err != nil {
			return fmt.Errorf("lock of projection failed: %w", err)
		}

		stream, err := p.projectionRepository.Get(txCtx, id)
		if err != nil {
			return fmt.Errorf("retrieval of projection failed: %w", err)
		}
		return p.upDateProjectionStreamState(txCtx, stream, state)
	})

	if errTx != nil {
		return fmt.Errorf("upDateProjectionStateByIDWithinTX() failed:%w", errTx)
	}
	return errTx
}

// upDateProjectionStreamState updates the state of the projection stream within a given transaction.
// Because we already have the stream, we dont have to retrieve it and must therefore also not be locekd
func (p *ProjectionService) upDateProjectionStreamState(txCtx context.Context, stream projection.Stream, states ...projection.State) error {
	for _, state := range states {
		if err := stream.UpdateState(state); err != nil {
			return fmt.Errorf("update of projection state failed: %w", err)
		}
	}

	if err := p.projectionRepository.SaveStates(txCtx, stream); err != nil {
		return fmt.Errorf("save of projection state failed: %w", err)
	}

	return nil
}

// upDateProjectionStreamStateWithinTXWithoutLock updates the state of the projection stream within a separate transaction.
// This function is mostly used to change the state of a projection even if the surrounding transactions failed (or will fail).
// But because of the surrounding transaction we cannot lock the projection (because it is alread looked).
func (p *ProjectionService) upDateProjectionStreamStateWithinTXWithoutLock(ctx context.Context, stream projection.Stream, states ...projection.State) error {
	errTx := p.transactor.WithinTX(ctx, func(newTxCtx context.Context) (err error) {
		return p.upDateProjectionStreamState(newTxCtx, stream, states...)
	})
	if errTx != nil {
		return fmt.Errorf("upDateProjectionStreamStateWithinTX() failed:%w", errTx)
	}

	return nil
}

// GetProjectionStates returns the projection state for a projection
func (p *ProjectionService) GetProjectionStates(ctx context.Context, tenantID string, projectionIDs ...string) (projectionState []event.ProjectionState, err error) {
	errTX := p.transactor.WithoutTX(ctx, func(txCtx context.Context) error {
		streams, errIntern := p.projectionRepository.GetProjections(txCtx, shared.NewProjectionIDs(tenantID, projectionIDs...)...)
		projectionState = mapper.MapStreamsToProjectionStates(streams)
		return errIntern
	})

	if errTX != nil {
		return nil, fmt.Errorf("GetProjectionState failed for projections %q:%w", projectionIDs, errTX)
	}

	return projectionState, errTX
}

// GetAllProjectionStates returns the projection states for all projections of a tenant
func (p *ProjectionService) GetAllProjectionStates(ctx context.Context, tenantID string) (projectionState []event.ProjectionState, err error) {
	errTX := p.transactor.WithoutTX(ctx, func(txCtx context.Context) error {
		streams, errIntern := p.projectionRepository.GetAllForTenant(txCtx, tenantID)
		projectionState = mapper.MapStreamsToProjectionStates(streams)
		return errIntern
	})

	if errTX != nil {
		return nil, fmt.Errorf("GetAllProjectionStates failed for tenant %q:%w", tenantID, errTX)
	}

	return projectionState, errTX
}

func (p *ProjectionService) RemoveProjection(ctx context.Context, projectionID string) error {
	// check if projection is still registered
	proj, err := p.registries.ProjectionRegistry.Projection(projectionID)
	if proj != nil {
		if err == nil {
			return fmt.Errorf("projection %q cannot be removed: projection is still registered", projectionID)
		}
		return fmt.Errorf("projection %q cannot be removed: error in projection registry:%w", projectionID, err)
	}

	errTX := p.transactor.WithinTX(ctx, func(txCtx context.Context) error {
		return p.projectionRepository.RemoveProjection(txCtx, projectionID)
	})
	return errTX

}

// RestartProjectionService restarts the projection service for all stored projections of all tenants. The functions is
// only responsible for already stored projections in the database and starts new projections for existing tenants.
func (p *ProjectionService) RestartProjectionService(ctx context.Context) (errCh chan error) {
	errCh = make(chan error, 100)

	// get all already stored (already initialized) projections
	storedProjections, err := p.getAllStoredProjections(ctx)
	if err != nil {
		errCh <- fmt.Errorf("retrieval of existent projections failed:%w", err)
		close(errCh)
		return errCh
	}

	storedProjectionsMap := lo.GroupBy(storedProjections, func(i projection.Stream) string {
		return i.ID().TenantID
	})

	// iterate over all registered projections and look for each tenant if the projection must be created or just restarted
	for _, proj := range p.registries.ProjectionRegistry.All() {
		for tenantID, storedProj := range storedProjectionsMap {
			// does the projection already exist in db?
			if _, ok := lo.Find(storedProj, func(i projection.Stream) bool { return i.ID().ProjectionID == proj.ID() }); !ok {
				// create projection
				createErr := p.createProjection(ctx, shared.NewProjectionID(tenantID, proj.ID()))
				if createErr != nil {
					errCh <- fmt.Errorf("initial create of projection %v failed:%w", shared.NewProjectionID(tenantID, proj.ID()), createErr)
					continue
				}
			}
			// register tenant if not already done (in a loop before)
			registerErr := p.registerTenant(ctx, tenantID)
			if registerErr != nil {
				errCh <- fmt.Errorf("register tenant %v failed:%w", tenantID, registerErr)
				continue
			}
			// init worker for projection
			if projID, initErr := p.initProjectionWorker(tenantID, proj.ID()); initErr != nil {
				errCh <- fmt.Errorf("start worker for projection %v failed:%w", projID, initErr)
			}
		}
	}
	// initial execute all projection
	if err = p.ExecuteAllProjections(ctx); err != nil {
		errCh <- fmt.Errorf("initial execute of projections failed:%w", err)
	}
	close(errCh)
	return errCh
}

func (p *ProjectionService) ExecuteAllProjections(ctx context.Context, projectionsID ...string) (err error) {
	storedProjectionsOfAllTenants, err := p.getAllStoredProjections(ctx)
	if err != nil {
		return fmt.Errorf("execute all projections failed:%w", err)
	}

	if len(projectionsID) > 0 {
		storedProjectionsOfAllTenants = slices.DeleteFunc(storedProjectionsOfAllTenants, func(i projection.Stream) bool {
			for _, id := range projectionsID {
				if i.ID().ProjectionID == id {
					return false
				}
			}
			return true
		})
	}

	return p.executeProjections(ctx, storedProjectionsOfAllTenants)
}

func (p *ProjectionService) getAllStoredProjections(ctx context.Context) (allProjections []projection.Stream, err error) {
	errTx := p.transactor.WithoutTX(ctx, func(txCtx context.Context) (err error) {
		allProjections, err = p.projectionRepository.GetAllForAllTenants(txCtx)
		if err != nil {
			return fmt.Errorf("GetAllForAllTenants() failed:%w", err)
		}
		return nil
	})

	return allProjections, errTx
}

func (p *ProjectionService) executeProjections(ctx context.Context, streams []projection.Stream) (err error) {
	// We use a WaitGroup to keep track of the number of active goroutines
	var wg sync.WaitGroup
	errCh := make(chan error, len(streams))

	// Starting the goroutines
	for _, proj := range streams {
		wg.Add(1)
		go func(proj projection.Stream) {
			defer wg.Done() // We make sure that wg.Done() is always called

			ch := p.EventualConsistentProjection(ctx, proj.ID(), time.Time{})
			for errIntern := range ch {
				// Error handling within the goroutine
				if errIntern != nil {
					errCh <- errIntern
				}
			}
		}(proj)
	}

	// End goroutines and wait
	go func() {
		wg.Wait()
		close(errCh) // Close the channel when all goroutines are finished
	}()

	for {
		select {
		case errFromCh, ok := <-errCh:
			if !ok {
				return err // Exit the function when the channel is closed
			}
			// Errors from the goroutines were collected in errCh
			if errFromCh != nil {
				err = fmt.Errorf(":%w", errFromCh)
			}
		case <-ctx.Done():
			return fmt.Errorf("execute all projection failed: execution deadline exceeded")
		}
	}
}
