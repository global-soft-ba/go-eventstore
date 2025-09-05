package mapper

import (
	"github.com/global-soft-ba/go-eventstore"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/domain/model/projection"
)

func MapStreamsToProjectionStates(streams []projection.Stream) []event.ProjectionState {
	var projectionStates []event.ProjectionState
	for _, stream := range streams {
		projectionStates = append(projectionStates, MapStreamToProjectionState(stream))
	}
	return projectionStates
}

func MapStreamToProjectionState(stream projection.Stream) event.ProjectionState {
	return event.ProjectionState{
		TenantID:                stream.ID().TenantID,
		ProjectionID:            stream.ID().ProjectionID,
		State:                   string(stream.State()),
		UpdatedAt:               stream.UpdatedAt(),
		HPatchStrategy:          stream.Options().HPatchStrategy,
		ExecutionTimeOut:        stream.Options().ExecutionTimeOut,
		PreparationTimeOut:      stream.Options().PreparationTimeOut,
		FinishingTimeOut:        stream.Options().FinishingTimeOut,
		RebuildExecutionTimeOut: stream.Options().RebuildExecutionTimeOut,
		InputQueueLength:        stream.Options().InputQueueLength,
		ProjectionType:          stream.Options().ProjectionType,
		RetryDurations:          stream.Options().RetryDurations,
	}

}
