package projection

import (
	"github.com/global-soft-ba/go-eventstore"
	"time"
)

type Options struct {
	HPatchStrategy          event.ProjectionPatchStrategy
	DPatchStrategy          event.ProjectionPatchStrategy
	ExecutionTimeOut        time.Duration
	PreparationTimeOut      time.Duration
	FinishingTimeOut        time.Duration
	RebuildExecutionTimeOut time.Duration
	InputQueueLength        int
	ProjectionType          event.ProjectionType
	RetryDurations          []time.Duration
}
