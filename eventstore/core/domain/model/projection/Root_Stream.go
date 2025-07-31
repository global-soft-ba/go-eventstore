package projection

import (
	"context"
	"fmt"
	"github.com/global-soft-ba/go-eventstore"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/port/persistence/projection"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/shared"
	"github.com/global-soft-ba/go-eventstore/instrumentation/port/logger"
	"github.com/global-soft-ba/go-eventstore/instrumentation/port/metrics"
	"sort"
	"time"
)

func LoadFromDTO(dto projection.DTO, proj event.Projection, opt Options) Stream {
	return Stream{
		id:         shared.NewProjectionID(dto.TenantID, dto.ProjectionID),
		projection: proj,
		options:    opt,
		state:      State(dto.State),
		updatedAt:  dto.UpdatedAt,
		events:     dto.Events,
	}
}

func CreateStream(projectionID shared.ProjectionID, state State, updatedAt time.Time, projection event.Projection, options Options) Stream {
	return Stream{
		id:         projectionID,
		state:      state,
		updatedAt:  updatedAt,
		projection: projection,
		options:    options,
	}
}

type Stream struct {
	id         shared.ProjectionID
	projection event.Projection
	options    Options
	state      State
	updatedAt  time.Time

	events []event.PersistenceEvent
}

func (s *Stream) Events() []event.PersistenceEvent {
	return s.events
}

func (s *Stream) FuturePatches() []event.PersistenceEvent {
	var future []event.PersistenceEvent
	for _, evt := range s.events {
		if evt.Class == event.FuturePatch {
			future = append(future, evt)
		}
	}
	return future
}

func (s *Stream) EarliestHPatch() time.Time {
	earliestHPatch := time.Time{}
	for _, evt := range s.events {
		if evt.Class == event.HistoricalPatch && (evt.ValidTime.Before(earliestHPatch) || earliestHPatch.IsZero()) {
			earliestHPatch = evt.ValidTime
		}
	}
	return earliestHPatch
}

func (s *Stream) EarliestDeleteEventInCurrentStream() time.Time {
	var earliestDeleteEvent time.Time
	for _, evt := range s.events {
		if evt.Class == event.DeletePatch && (earliestDeleteEvent.IsZero() || evt.ValidTime.Before(earliestDeleteEvent)) {
			earliestDeleteEvent = evt.ValidTime
		}
	}
	return earliestDeleteEvent
}

func (s *Stream) ID() shared.ProjectionID {
	return s.id
}

func (s *Stream) State() State {
	return s.state
}

func (s *Stream) UpdatedAt() time.Time {
	return s.updatedAt
}

func (s *Stream) Options() Options {
	return s.options
}

func (s *Stream) EventTypes() []string {
	return s.projection.EventTypes()
}

func (s *Stream) ChunkSize() int {
	return s.projection.ChunkSize()
}

func (s *Stream) MinimumProjectionSinceTime(sinceTime time.Time) time.Time {
	_, ok := s.projection.(event.ProjectionSince)
	if ok {
		return sinceTime
	}

	return time.Time{}
}

func (s *Stream) Prepare(_ context.Context, sinceTime time.Time, timeOut time.Duration) error {
	//Create new context in order to avoid leaking of transaction
	ctxNew, cancel := context.WithTimeout(context.Background(), timeOut)
	defer cancel()

	// we use a non-blocking implementation, to make sure that a blocking projection in domain does not
	// block the events store
	resCh := make(chan error)
	go func(chan error) {
		var errIntern error
		if sinceTime.IsZero() {
			errIntern = s.projection.PrepareRebuild(ctxNew, s.ID().TenantID)
		} else {
			projSince, ok := s.projection.(event.ProjectionSince)
			if ok {
				errIntern = projSince.PrepareRebuildSince(ctxNew, s.ID().TenantID, sinceTime)
			} else {
				errIntern = s.projection.PrepareRebuild(ctxNew, s.ID().TenantID)
			}
		}

		if errIntern != nil {
			errIntern = event.NewErrorProjectionExecutionFailed(fmt.Errorf("preparation failed: %w", errIntern), s.id)
		}

		resCh <- errIntern
	}(resCh)

	var err error
	select {
	case err = <-resCh: // executed in time
	case <-ctxNew.Done(): // timeout
		err = event.NewErrorProjectionTimeOut(fmt.Errorf("deadline exceeded for projection preparation"), s.id)
		logger.Error(err)
	}

	return err
}

func (s *Stream) validateState(state State) (bool, error) {
	if s.State() != state {
		switch s.Options().ProjectionType {
		case event.ECS, event.ESS:
			return false, nil
		default:
			return false, fmt.Errorf("wrong state %q of consistent projection (want %q)", s.State(), state)
		}
	}

	return true, nil
}

// ExecuteWithTimeOut We pass timeout explicitly even if it is included in the stream. This makes it possible that
// other timeouts can be injected, e.g. during the rebuild.
func (s *Stream) ExecuteWithTimeOut(ctx context.Context, timeOut time.Duration, state State) (count int, err error) {
	ctx, endSpan := metrics.StartSpan(ctx, "execute projection", map[string]interface{}{"tenantID": s.id.TenantID, "projectionID": s.id.ProjectionID, "numberOfEvents": len(s.events)})
	defer endSpan()

	// no events for proj -> don't call it
	if len(s.events) == 0 {
		return 0, nil
	}

	if execute, errState := s.validateState(state); errState != nil || !execute {
		if errState != nil {
			return 0, errState
		} else {
			return 0, event.NewErrorProjectionInWrongState(nil, string(s.state), string(state), s.id)
		}

	}

	iEvents, err := mapToIEvents(s.events)
	if err != nil {
		return 0, err
	}

	ctxNew, cancel := context.WithTimeout(context.Background(), timeOut)
	defer cancel()
	// we use a non-blocking implementation, to make sure that a blocking projection in domain does not
	// block the events store
	resCh := make(chan error)
	go func(chan error) {
		var errIntern error
		// we used a chunked execution because we cannot guarantee that the stream contains the exact chunk size
		// especially in the case of consistent projections execution
		for _, chunk := range chunkSlice(iEvents, s.ChunkSize()) {
			errIntern = s.projection.Execute(ctxNew, chunk)
			if errIntern != nil {
				errIntern = event.NewErrorProjectionExecutionFailed(fmt.Errorf("execution failed: %w", errIntern), s.id)
				break
			}
		}
		resCh <- errIntern
	}(resCh)

	select {
	case err = <-resCh: // executed in time
	case <-ctxNew.Done(): // timeout
		err = event.NewErrorProjectionTimeOut(fmt.Errorf("deadline exceeded for projection execution"), s.id)
		logger.Error(err)
	}

	return len(s.events), err
}

func (s *Stream) Finish(_ context.Context, timeOut time.Duration) error {
	//Create new context in order to avoid leaking of transaction
	ctxNew, cancel := context.WithTimeout(context.Background(), timeOut)
	defer cancel()

	// we use a non-blocking implementation, to make sure that a blocking projection in domain does not
	// block the events store
	resCh := make(chan error)
	go func(chan error) {
		var errIntern error
		if errIntern = s.projection.FinishRebuild(ctxNew, s.ID().TenantID); errIntern != nil {
			errIntern = event.NewErrorProjectionExecutionFailed(fmt.Errorf("finishing failed: %w", errIntern), s.id)
			resCh <- errIntern
		} else {
			// The events have been transferred to the domain and can be marked as delivered or removed in the stream.
			s.DeleteEvents()
		}

		resCh <- errIntern
	}(resCh)

	var err error
	select {
	case err = <-resCh: // executed in time
	case <-ctxNew.Done(): // timeout
		err = event.NewErrorProjectionTimeOut(fmt.Errorf("deadline exceeded for projection finishing"), s.id)
		logger.Error(err)
	}

	return err
}

// SortByValidTimeByAggregateIdByVersion sorts the events by valid time, aggregate id and version.
// This is the general sort of criteria for all events in projections.
func (s *Stream) SortByValidTimeByAggregateIdByVersion() {
	sort.SliceStable(s.events, func(i, j int) bool {
		if s.events[i].ValidTime.Before(s.events[j].ValidTime) {
			return true
		} else if s.events[i].ValidTime.Equal(s.events[j].ValidTime) {
			if s.events[i].AggregateID < s.events[j].AggregateID {
				return true
			} else if s.events[i].AggregateID == s.events[j].AggregateID {
				if s.events[i].Version < s.events[j].Version {
					return true
				}
			}
		}
		return false
	})
}

func mapToIEvents(events []event.PersistenceEvent) ([]event.IEvent, error) {
	var iEvents []event.IEvent

	for _, evt := range events {
		iEvent, err := event.DeserializeEvent(evt)
		if err != nil {
			return nil, err
		}

		iEvents = append(iEvents, iEvent)
	}

	return iEvents, nil
}

func chunkSlice[T any](events []T, chunkSize int) (chunks [][]T) {
	for i := 0; i < len(events); i += chunkSize {
		end := i + chunkSize

		if end > len(events) {
			end = len(events)
		}

		chunks = append(chunks, events[i:end])
	}
	return chunks
}
