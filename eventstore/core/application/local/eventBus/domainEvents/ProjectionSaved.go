package domainEvents

import (
	"github.com/global-soft-ba/go-eventstore/eventstore/core/shared"
	"time"
)

func EventProjectionSaved(id shared.ProjectionID, earliestHPatch time.Time) ProjectionSaved {
	return ProjectionSaved{
		id:             id,
		earliestHPatch: earliestHPatch,
	}
}

type ProjectionSaved struct {
	id             shared.ProjectionID
	earliestHPatch time.Time
}

func (e ProjectionSaved) Name() string {
	return "event.projection.saved"
}

func (e ProjectionSaved) Id() shared.ProjectionID {
	return e.id
}

func (e ProjectionSaved) EarliestHPatch() time.Time {
	return e.earliestHPatch
}
