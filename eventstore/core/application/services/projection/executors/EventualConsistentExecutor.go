package executors

import (
	"context"
	"errors"
	"fmt"
	"github.com/global-soft-ba/go-eventstore"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/application/repository"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/domain/model/projection"
	transactor2 "github.com/global-soft-ba/go-eventstore/eventstore/core/port/persistence/transactor"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/shared"
	helper "github.com/global-soft-ba/go-eventstore/helper"
	"github.com/global-soft-ba/go-eventstore/instrumentation/port/logger"
	"time"
)

func NewEventualConsistentProjectionExecutor(trans transactor2.Port, repro repository.ProjectionRepositoryInterface, id shared.ProjectionID, opt projection.Options, earliestHPatch time.Time) EventualConsistentProjectionExecutor {
	return EventualConsistentProjectionExecutor{commonExecutor{trans, repro}, id, opt, earliestHPatch}
}

type EventualConsistentProjectionExecutor struct {
	commonExecutor
	id             shared.ProjectionID
	opt            projection.Options
	earliestHPatch time.Time
}

func (e EventualConsistentProjectionExecutor) HasHPatch() bool {
	return !e.earliestHPatch.IsZero()
}

func (e EventualConsistentProjectionExecutor) GetHPatch() time.Time {
	return e.earliestHPatch
}

func (e EventualConsistentProjectionExecutor) GetDPatch() time.Time {
	//Dpatches are not executed in eventual consistent projections
	logger.Error(fmt.Errorf("DPatches are not allowed to executed in eventual consistent projections"))
	return time.Time{}
}

func (e EventualConsistentProjectionExecutor) GetOptions() projection.Options {
	return e.opt
}

func (e EventualConsistentProjectionExecutor) Run(ctx context.Context) error {
	var stream projection.Stream
	errTrans := e.transactor.WithoutTX(ctx, func(txCtx context.Context) (err error) {
		stream, err = e.projectionRepository.Get(txCtx, e.id)
		return err
	})

	if errTrans != nil {
		return fmt.Errorf("retrieval of stream for projection %q failed: %w", e.id.ProjectionID, errTrans)
	}

	return helper.ExecuteFunctionChunkWise(func() (bool, error) {
		return e.run(ctx, stream)
	})
}

func (e EventualConsistentProjectionExecutor) run(ctx context.Context, stream projection.Stream) (bool, error) {
	var executed int
	errTx := e.transactor.WithinTX(ctx, func(txCtx context.Context) (err error) {
		// We lock/unlock the projection during each single chunks execution and not for the whole projection execution.
		// The reason is, if the projection finishes fatal or the main/pod crashes, the lock will automatically be released
		// (advisory lock in postgres) and do not block other instances/pods to proceed with the projection.
		if err = e.projectionRepository.Lock(txCtx, e.id); err != nil {
			return err
		}

		defer func() {
			if errUnlock := e.projectionRepository.UnLock(txCtx, e.id); errUnlock != nil {
				logger.Error(errUnlock)
			}
		}()

		// execute the projection
		executed, err = e.execute(txCtx, stream, stream.Options().ExecutionTimeOut, projection.Running)
		if err != nil {
			return e.handleErrorsDuringProjection(stream, err)
		}
		return
	})

	return executed == stream.ChunkSize(), errTx
}

func (e EventualConsistentProjectionExecutor) Projected(ctx context.Context) error {
	return e.Run(ctx)
}

func (e EventualConsistentProjectionExecutor) Rebuild(ctx context.Context) error {
	return e.RebuildSince(ctx, time.Time{})
}

func (e EventualConsistentProjectionExecutor) RebuildSince(ctx context.Context, since time.Time) error {
	// We lock the projection over the entire period of the rebuild (all three steps) by setting the state to "rebuild".
	// So no other rebuild/start request (from any pod) will be accepted.

	stream, err := e.prepareRebuildWithTX(ctx, since)
	if err != nil {
		return fmt.Errorf("prepare rebuild of projection %q failed: %w", e.id, err)
	}

	err = e.executeRebuildWithTX(ctx, stream)
	if err != nil {
		return fmt.Errorf("execute rebuild of projection %q failed: %w", e.id, err)
	}

	if err = e.finishRebuildingWithTX(ctx, stream); err != nil {
		return fmt.Errorf("finish rebuild of projection %q failed: %w", e.id, err)
	}

	return nil

}

func (e EventualConsistentProjectionExecutor) prepareRebuildWithTX(ctx context.Context, since time.Time) (projection.Stream, error) {
	var stream projection.Stream
	errTx := e.transactor.WithinTX(ctx, func(txCtx context.Context) (err error) {
		defer func() {
			if errUnlock := e.projectionRepository.UnLock(txCtx, e.id); errUnlock != nil {
				logger.Error(fmt.Errorf("unlock of projection %q of tenant %q failed: %w", e.id.ProjectionID, e.id.TenantID, errUnlock))
			}
		}()
		if err = e.projectionRepository.Lock(txCtx, e.id); err != nil {
			return fmt.Errorf("lock of projection %q of tenant %q failed: %w", e.id.ProjectionID, e.id.TenantID, err)
		}

		if stream, err = e.projectionRepository.Get(txCtx, e.id); err != nil {
			return fmt.Errorf("retrieval of stream for projection %q of tenant %q failed: %w", e.id.ProjectionID, e.id.TenantID, err)
		}

		if err = e.updateStreamState(txCtx, stream, projection.Rebuilding); err != nil {
			return err
		}

		// prepare the projection rebuild
		return e.commonExecutor.prepareRebuild(txCtx, stream, since)
	})

	return stream, e.handleErrorsDuringRebuilding(stream, errTx)
}

func (e EventualConsistentProjectionExecutor) executeRebuildWithTX(ctx context.Context, stream projection.Stream) error {
	errTx := e.transactor.WithinTX(ctx, func(txCtx context.Context) error {
		defer func() {
			if errUnlock := e.projectionRepository.UnLock(txCtx, stream.ID()); errUnlock != nil {
				logger.Error(fmt.Errorf("unlock of projection %q of tenant %q failed: %w", stream.ID().ProjectionID, stream.ID().TenantID, errUnlock))
			}
		}()
		if err := e.projectionRepository.Lock(txCtx, stream.ID()); err != nil {
			return fmt.Errorf("lock of projection %q of tenant %q failed: %w", stream.ID().ProjectionID, stream.ID().TenantID, err)
		}

		// execute projection in chunks
		if err := helper.ExecuteFunctionChunkWise(func() (bool, error) {
			executed, err := e.commonExecutor.execute(txCtx, stream, stream.Options().RebuildExecutionTimeOut, projection.Rebuilding)
			return stream.ChunkSize() == executed, err
		}); err != nil {
			return err
		}

		return nil
	})
	return e.handleErrorsDuringRebuilding(stream, errTx)
}

func (e EventualConsistentProjectionExecutor) finishRebuildingWithTX(ctx context.Context, stream projection.Stream) error {
	errTx := e.transactor.WithinTX(ctx, func(txCtx context.Context) (err error) {
		defer func() {
			if errUnlock := e.projectionRepository.UnLock(txCtx, stream.ID()); errUnlock != nil {
				logger.Error(fmt.Errorf("unlock of projection %q of tenant %q failed: %w", stream.ID().ProjectionID, stream.ID().TenantID, errUnlock))
			}
		}()
		if err = e.projectionRepository.Lock(txCtx, stream.ID()); err != nil {
			return fmt.Errorf("lock of projection %q of tenant %q failed: %w", stream.ID().ProjectionID, stream.ID().TenantID, err)
		}

		// finish the projection rebuild
		if err = e.commonExecutor.finishRebuild(txCtx, stream); err != nil {
			return err
		}

		return e.updateStreamState(txCtx, stream, projection.Stopped, projection.Running)
	})

	return e.handleErrorsDuringRebuilding(stream, errTx)
}

func (e EventualConsistentProjectionExecutor) handleErrorsDuringProjection(stream projection.Stream, err error) error {
	if err == nil {
		return nil
	}

	// transaction errors
	var roll *transactor2.ErrorRollbackFailed
	var commit *transactor2.ErrorCommitFailed

	// projection errors
	var concurrentProjectionAccess *event.ErrorConcurrentProjectionAccess
	var wrongState *event.ErrorProjectionInWrongState
	var timeOut *event.ErrorProjectionTimeOut
	var executeFail *event.ErrorProjectionExecutionFailed
	var outOfSync *event.ErrorProjectionOutOfSync

	switch {
	case errors.As(err, &roll):
		// rollback error
		err = event.NewErrorProjectionOutOfSync(err, e.id)
		logger.Error(err)
	case errors.As(err, &commit):
		// commit error
		err = event.NewErrorProjectionOutOfSync(err, e.id)
		logger.Error(err)
	case errors.As(err, &wrongState):
		// wrong state
		logger.Info(wrongState.Error())
	case errors.As(err, &concurrentProjectionAccess):
		// concurrent access
		logger.Info(concurrentProjectionAccess.Error())
	case errors.As(err, &timeOut):
		// time out error
		logger.Error(err)
	case errors.As(err, &executeFail):
		// execution failed
		logger.Error(err)
	case errors.As(err, &outOfSync):
		if errTx := e.updateStreamStateWithTx(stream, projection.Erroneous); err != nil {
			logger.Error(errTx)
		}
	default:
		logger.Error(err)
	}

	return err
}

func (e EventualConsistentProjectionExecutor) handleErrorsDuringRebuilding(stream projection.Stream, err error) error {
	if err == nil {
		return nil
	}

	//transaction errors
	var roll *transactor2.ErrorRollbackFailed
	var commit *transactor2.ErrorCommitFailed

	//projection errors
	var concurrentProjectionAccess *event.ErrorConcurrentProjectionAccess
	var wrongState *event.ErrorProjectionInWrongState
	var timeOut *event.ErrorProjectionTimeOut
	var executeFail *event.ErrorProjectionExecutionFailed

	switch {
	case errors.As(err, &roll):
		// rollback error
		err = event.NewErrorProjectionOutOfSync(err, e.id)
		logger.Error(err)
	case errors.As(err, &commit):
		// commit error
		err = event.NewErrorProjectionOutOfSync(err, e.id)
		logger.Error(err)
	case errors.As(err, &wrongState):
		// wrong state
		logger.Info(wrongState.Error())
	case errors.As(err, &concurrentProjectionAccess):
		// concurrent access
		logger.Info(concurrentProjectionAccess.Error())
	case errors.As(err, &timeOut):
		// time out error
		err = event.NewErrorProjectionOutOfSync(err, e.id)
		logger.Error(err)
	case errors.As(err, &executeFail):
		// execution failed
		err = event.NewErrorProjectionOutOfSync(err, e.id)
		logger.Error(err)
	default:
		err = event.NewErrorProjectionOutOfSync(err, e.id)
		logger.Error(err)
	}

	var outOfSync *event.ErrorProjectionOutOfSync
	//change state of stream to erroneous
	switch {
	case errors.As(err, &outOfSync):
		if errTx := e.updateStreamStateWithTx(stream, projection.Erroneous); err != nil {
			logger.Error(errTx)
		}
	}

	return err
}
