package executors

import (
	"context"
	"errors"
	"github.com/global-soft-ba/go-eventstore"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/application/repository"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/domain/model/projection"
	transactor2 "github.com/global-soft-ba/go-eventstore/eventstore/core/port/persistence/transactor"
	helper "github.com/global-soft-ba/go-eventstore/helper"
	"github.com/global-soft-ba/go-eventstore/instrumentation/port/logger"
	"time"
)

func NewConsistentProjectionExecutor(trans transactor2.Port, repro repository.ProjectionRepositoryInterface, stream projection.Stream) ConsistentProjectionExecutor {
	return ConsistentProjectionExecutor{commonExecutor{trans, repro}, stream}
}

type ConsistentProjectionExecutor struct {
	commonExecutor
	stream projection.Stream
}

func (c ConsistentProjectionExecutor) HasHPatch() bool {
	return !c.stream.EarliestHPatch().IsZero()
}

func (c ConsistentProjectionExecutor) GetHPatch() time.Time {
	return c.stream.EarliestHPatch()
}

func (c ConsistentProjectionExecutor) GetDPatch() time.Time {
	return c.stream.EarliestDeleteEventInCurrentStream()
}

func (c ConsistentProjectionExecutor) GetOptions() projection.Options {
	return c.stream.Options()
}

func (c ConsistentProjectionExecutor) Run(txCtx context.Context) error {
	// executes directly on the given projection stream
	// Within consistent executeProjectionInChunks the chunks size is handles within the projection aggregate itself.
	// No need for a chunked execution as it is done in eventual consistent projections.
	// The projection is already locked in the save transaction.

	// In consistent projection we have to sort explicitly to get the "natural" order of the events.
	// For eventual consistent projection this is done during the retrieval events from the projection queue
	c.stream.SortByValidTimeByAggregateIdByVersion()

	_, err := c.stream.ExecuteWithTimeOut(txCtx, c.stream.Options().ExecutionTimeOut, projection.Running)
	return c.handleErrorsDuringProjection(err)
}

func (c ConsistentProjectionExecutor) Projected(ctx context.Context) error {
	return c.Run(ctx)
}

// Rebuild within ConsistentProjectionExecutor it is assumed that he was triggered within a transaction, like it happens to rebuild in case
// of historical patch. That means or rather we assume:
//
//			1.)
//	     		We have locked the stream, already. Only this original save transaction with a historical patch
//	     		can unlock	the projection. Because of that, we use a separate transaction to switch the projection to "rebuild" state".
//		        This is necessary to ensure that no other rebuild request (from any pod) will be accepted.
//
//			2.) we can access to the new events (with historical patch) only in the original transaction context. Because
//	            of that, we have to use this transaction context to execute the rebuilding.
//
//	 It is not possible to store any new events during the rebuild in the store until it is successfully finished.
//	 The same is true, if the rebuild fails. In this case the projection is set to erroneous and will not accept any new save request.
func (c ConsistentProjectionExecutor) Rebuild(txCtx context.Context) error {
	return c.RebuildSince(txCtx, time.Time{})
}

func (c ConsistentProjectionExecutor) RebuildSince(txCtx context.Context, since time.Time) error {
	// We lock the projection over the entire period of the rebuild (all three steps)
	// by setting the state to "rebuild". So no other rebuild request (from any pod) will be accepted

	// 1.) switch projection to rebuild in separate transaction to avoid multiple rebuild trigger from other transactions
	if err := c.updateStreamStateWithTx(c.stream, projection.Rebuilding); err != nil {
		return err
	}

	// 2.) use old txCTX to execute all steps
	if err := c.rebuild(txCtx, since); err != nil {
		return c.handleErrorsDuringRebuilding(err)
	}

	// 3.) re-switch to stopped / running again in a separate transaction
	if err := c.updateStreamStateWithTx(c.stream, projection.Stopped, projection.Running); err != nil {
		return err
	}

	return nil
}

func (c ConsistentProjectionExecutor) rebuild(txCtx context.Context, since time.Time) error {
	// prepare rebuild
	if err := c.commonExecutor.prepareRebuild(txCtx, c.stream, since); err != nil {
		return err
	}

	// execute projection in chunks
	if err := helper.ExecuteFunctionChunkWise(func() (bool, error) {
		executed, err := c.commonExecutor.execute(txCtx, c.stream, c.stream.Options().RebuildExecutionTimeOut, projection.Rebuilding)
		return c.stream.ChunkSize() == executed, err
	}); err != nil {
		return err
	}

	// finish rebuild
	if err := c.commonExecutor.finishRebuild(txCtx, c.stream); err != nil {
		return err
	}

	return nil
}

// handleErrors handles/changes projection stream state in case of projection errors.
// But if the input error is nil its returns nil as well.
// In consistent projection if the execution fails the corresponding save request will fail as well. Even if a save would
// fail, the projection is still valid and can be used for reads (because the state of the persisted aggregate is the same
// as in the projection).
func (c ConsistentProjectionExecutor) handleErrorsDuringProjection(err error) error {
	if err == nil {
		return err
	}

	var concurrentProjectionAccess *event.ErrorConcurrentProjectionAccess
	var wrongState *event.ErrorProjectionInWrongState
	var timeOut *event.ErrorProjectionTimeOut
	var executeFail *event.ErrorProjectionExecutionFailed
	var outOfSync *event.ErrorProjectionOutOfSync

	switch {
	case errors.As(err, &wrongState):
		// wrong state
		logger.Warn(wrongState.Error())
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
		if errTx := c.updateStreamStateWithTx(c.stream, projection.Erroneous); err != nil {
			logger.Error(errTx)
		}
	default:
		logger.Error(err)
	}

	return err
}

// handlePossibleErrorsInConsistentProjectionsForHistoricalPatch handles/changes projection stream state in case of projection errors.
// But if the input error is nil its returns nil as well.
// In consistent projection if the execution fails the corresponding save request will fail as well. Even if a save would
// fail, the projection is still valid and can be used for reads (because the state of the persisted aggregate is the same
// as in the projection).
func (c ConsistentProjectionExecutor) handleErrorsDuringRebuilding(err error) error {
	if err == nil {
		return nil
	}

	var concurrentProjectionAccess *event.ErrorConcurrentProjectionAccess
	var wrongState *event.ErrorProjectionInWrongState
	var timeOut *event.ErrorProjectionTimeOut
	var executeFail *event.ErrorProjectionExecutionFailed

	switch {
	case errors.As(err, &wrongState):
		// wrong state
		logger.Warn(wrongState.Error())
	case errors.As(err, &concurrentProjectionAccess):
		// concurrent access
		logger.Info(concurrentProjectionAccess.Error())
	case errors.As(err, &timeOut):
		// time out error - we do not know if the projection is okay or not.
		err = event.NewErrorProjectionOutOfSync(err, c.stream.ID())
		logger.Error(err)
	case errors.As(err, &executeFail):
		// execution failed
		err = event.NewErrorProjectionOutOfSync(err, c.stream.ID())
		logger.Error(err)
	default:
		err = event.NewErrorProjectionOutOfSync(err, c.stream.ID())
		logger.Error(err)
	}

	var outOfSync *event.ErrorProjectionOutOfSync
	//change state of stream to erroneous
	switch {
	case errors.As(err, &outOfSync):
		if errTx := c.updateStreamStateWithTx(c.stream, projection.Erroneous); err != nil {
			logger.Error(errTx)
		}
	}

	return err
}
