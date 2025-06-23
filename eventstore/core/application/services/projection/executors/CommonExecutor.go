package executors

import (
	"context"
	"fmt"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/application/repository"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/domain/model/projection"
	transactor2 "github.com/global-soft-ba/go-eventstore/eventstore/core/port/persistence/transactor"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/shared"
	"github.com/global-soft-ba/go-eventstore/instrumentation/port/logger"
	"time"
)

type IExecuter interface {
	HasHPatch() bool
	GetHPatch() time.Time
	GetDPatch() time.Time

	GetOptions() projection.Options

	Run(ctx context.Context) error
	Projected(ctx context.Context) error

	Rebuild(ctx context.Context) error
	RebuildSince(ctx context.Context, since time.Time) error
}

type commonExecutor struct {
	transactor           transactor2.Port
	projectionRepository repository.ProjectionRepositoryInterface
}

func (e commonExecutor) execute(txCtx context.Context, stream projection.Stream, timeout time.Duration, initState projection.State) (int, error) {
	streamWithNewEvents, err := e.projectionRepository.GetWithNewEventsSinceLastRun(txCtx, stream.ID())
	if err != nil {
		return 0, fmt.Errorf("get new events since last failed for projection %q failed: %w", stream.ID(), err)
	}

	executed, err := streamWithNewEvents.ExecuteWithTimeOut(txCtx, timeout, initState)
	if err != nil {
		return executed, fmt.Errorf("execution of projection %q failed: %w", stream.ID(), err)
	}
	return executed, err
}

func (e commonExecutor) prepareRebuild(txCtx context.Context, stream projection.Stream, since time.Time) error {
	if err := e.projectionRepository.Reset(txCtx, stream.ID(), stream.MinimumProjectionSinceTime(since), stream.EventTypes()...); err != nil {
		return fmt.Errorf("reset of projection %q id failed: %w", stream.ID(), err)
	}

	return stream.Prepare(txCtx, since, stream.Options().PreparationTimeOut)
}

func (e commonExecutor) finishRebuild(txCtx context.Context, stream projection.Stream) error {
	return stream.Finish(txCtx, stream.Options().FinishingTimeOut)
}

func (e commonExecutor) updateStreamState(txCtx context.Context, stream projection.Stream, newStates ...projection.State) error {
	for _, newState := range newStates {
		// we want to check if the state and its order is valid
		if err := stream.UpdateState(newState); err != nil {
			return fmt.Errorf("update state of projection stream %q failed:%w", stream.ID(), err)
		}
	}

	if err := e.projectionRepository.SaveStates(txCtx, stream); err != nil {
		return fmt.Errorf("update state of projection %q failed:%w", stream.ID(), err)

	}

	return nil
}

// updateStreamStateWithTx This function is mainly used to cover error cases. That's why we are using a new transaction to update the state,
// because the original transaction will fail due to the error.
func (e commonExecutor) updateStreamStateWithTx(stream projection.Stream, newStates ...projection.State) error {
	errTx := e.transactor.WithinTX(context.Background(), func(ctx context.Context) (err error) {
		return e.updateStreamState(ctx, stream, newStates...)
	})
	if errTx != nil {
		return fmt.Errorf("update state of projection %q failed:%w", stream.ID(), errTx)
	}

	return nil
}

func (e commonExecutor) lockProjectionWithTX(id shared.ProjectionID) error {
	errTx := e.transactor.WithinTX(context.Background(), func(txCtx context.Context) (err error) {
		if err = e.projectionRepository.Lock(txCtx, id); err != nil {
			return fmt.Errorf("lock of projection %q of tenant %q failed: %w", id.ProjectionID, id.TenantID, err)
		}
		return nil
	})
	return errTx
}

func (e commonExecutor) unLockProjectionWithTX(id shared.ProjectionID) error {
	errTx := e.transactor.WithinTX(context.Background(), func(txCtx context.Context) (err error) {
		if errUnlock := e.projectionRepository.UnLock(txCtx, id); errUnlock != nil {
			logger.Error(fmt.Errorf("unlock of projection %q of tenant %q failed: %w", id.ProjectionID, id.TenantID, errUnlock))
		}
		return nil
	})
	return errTx
}
