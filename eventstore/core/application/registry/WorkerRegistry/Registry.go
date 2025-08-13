package WorkerRegistry

import (
	"fmt"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/shared"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/shared/kvTable"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/shared/rateWorker"
)

const (
	// In case of simultaneous requests to a single (identical) asynchron projection, at most as many projections as specified
	// in defaultProjectionRateLimit will be executed, e.g. with rateLimit=2, 2 out of 10 simultaneous requests only 2 will be executed
	// (like first and last), but all 10 will be informed via channel if the projection is done.
	defaultProjectionRateLimit = 2
)

func NewRegistry() *Registry {
	return &Registry{
		workers: kvTable.NewKeyValuesTable[chan rateWorker.ExecutionParams[error]](),
	}
}

type Registry struct {
	workers kvTable.IKVTable[chan rateWorker.ExecutionParams[error]] //key[tenantID][projectionID]
}

func (r Registry) Delete(id shared.ProjectionID) error {
	err := kvTable.Del(r.workers, kvTable.NewKey(id.TenantID, id.ProjectionID))
	if err != nil {
		return fmt.Errorf("could not delete worker: %w", err)
	}
	return err
}

func (r Registry) Queue(id shared.ProjectionID) (chan rateWorker.ExecutionParams[error], error) {
	resCh, err := kvTable.GetFirst(r.workers, kvTable.NewKey(id.TenantID, id.ProjectionID))
	if err != nil {
		if kvTable.IsKeyNotFound(err) {
			return nil, err
		}
		return nil, fmt.Errorf("could not found input queue for projections %q: %w", id, err)
	}
	return resCh, err
}

func (r Registry) CreateAndStart(id shared.ProjectionID, inputQueueLength int) error {
	//one worker per tenant and projection
	if r.Exists(id) {
		return fmt.Errorf("worker already exists for projection %q and tenant %q", id.ProjectionID, id.TenantID)
	}

	inputCh := make(chan rateWorker.ExecutionParams[error], inputQueueLength)
	rateLimitedWorker := rateWorker.NewRateLimitedWorker[error](defaultProjectionRateLimit, inputCh)

	//save input queue
	if err := kvTable.Set(r.workers, kvTable.NewKey(id.TenantID, id.ProjectionID), inputCh); err != nil {
		return fmt.Errorf("saving failed for worker %q: %w", id, err)
	}

	go rateLimitedWorker.Start()
	return nil
}

func (r Registry) Shutdown(id shared.ProjectionID) error {
	if r.Exists(id) {
		return fmt.Errorf("worker for potjection %q doesn't exists", id)
	}
	existingCh, err := r.Queue(id)
	if err != nil {
		return err
	}
	close(existingCh)

	err = r.Delete(id)
	if err != nil {
		return fmt.Errorf("could not delete worker queue for porjection %q: %w", id, err)
	}
	return err
}

func (r Registry) Exists(id shared.ProjectionID) bool {
	ch, _ := r.Queue(id)
	if ch != nil {
		return true
	}
	return false
}
