package domainEvents

import (
	"github.com/global-soft-ba/go-eventstore/eventstore/core/port/scheduler"
)

func EventFuturePatchSaved(fPatch scheduler.ScheduledProjectionTask) FuturePatchSaved {
	return FuturePatchSaved{
		fPatch: fPatch,
	}
}

type FuturePatchSaved struct {
	fPatch scheduler.ScheduledProjectionTask
}

func (e FuturePatchSaved) Name() string {
	return "event.projection.patch.future.saved"
}

func (e FuturePatchSaved) Task() scheduler.ScheduledProjectionTask {
	return e.fPatch
}
