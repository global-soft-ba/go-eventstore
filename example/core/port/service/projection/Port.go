package projection

import (
	"context"
	"github.com/global-soft-ba/go-eventstore"
)

type Port interface {
	GetProjectionStates(ctx context.Context, tenantID string, projectionID ...string) ([]StateResponseDTO, error)
	DeleteProjection(ctx context.Context, tenantID, projectionID string) error
	RebuildProjections(ctx context.Context, tenantID string, projectionIDs ...string) error
	StartProjections(ctx context.Context, tenantID string, projectionIDs ...string) (err error)
	StopProjections(ctx context.Context, tenantID string, projectionIDs ...string) (err error)
}

type StateResponseDTO struct {
	TenantID                string   `json:"tenantID"`
	ProjectionID            string   `json:"projectionID"`
	State                   string   `json:"state"`
	HPatchStrategy          string   `json:"hPatchStrategy"`
	ExecutionTimeOut        string   `json:"executionTimeOut"`
	PreparationTimeOut      string   `json:"preparationTimeOut"`
	FinishingTimeOut        string   `json:"finishingTimeOut"`
	RebuildExecutionTimeOut string   `json:"rebuildExecutionTimeOut"`
	InputQueueLength        int      `json:"inputQueueLength"`
	ProjectionType          string   `json:"projectionType"`
	RetryDurations          []string `json:"retryDurations"`
}

func FromEventStoreProjectionStates(states []event.ProjectionState) (dtos []StateResponseDTO) {
	for _, state := range states {
		dtos = append(dtos, FromEventStoreProjectionState(state))
	}
	return dtos
}

func FromEventStoreProjectionState(state event.ProjectionState) StateResponseDTO {
	durations := make([]string, len(state.RetryDurations))
	for i, d := range state.RetryDurations {
		durations[i] = d.String()
	}
	return StateResponseDTO{
		TenantID:                state.TenantID,
		ProjectionID:            state.ProjectionID,
		State:                   state.State,
		HPatchStrategy:          string(state.HPatchStrategy),
		ExecutionTimeOut:        state.ExecutionTimeOut.String(),
		PreparationTimeOut:      state.PreparationTimeOut.String(),
		FinishingTimeOut:        state.FinishingTimeOut.String(),
		RebuildExecutionTimeOut: state.RebuildExecutionTimeOut.String(),
		InputQueueLength:        state.InputQueueLength,
		ProjectionType:          string(state.ProjectionType),
		RetryDurations:          durations,
	}
}
