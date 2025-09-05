package internal

import (
	"context"
	"fmt"
	"github.com/global-soft-ba/go-eventstore"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/port/persistence/aggregate"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/shared"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/shared/timespan"
	"github.com/global-soft-ba/go-eventstore/eventstore/infrastructure/persistence/memory/internal/db"
	"github.com/global-soft-ba/go-eventstore/eventstore/infrastructure/persistence/memory/internal/mapper"
	"time"
)

func (l loader) GetAggregateState(ctx context.Context, tenantID, aggregateType, aggregateID string) (event.AggregateState, error) {
	state, err := l.retrieveAggregates(ctx, db.IdxSetOfId, tenantID, aggregateType, aggregateID)
	if err != nil {
		return event.AggregateState{}, err
	}

	if len(state) == 0 {
		return event.AggregateState{}, &aggregate.NotFoundError{
			ID: shared.AggregateID{
				TenantID:      tenantID,
				AggregateType: aggregateType,
				AggregateID:   aggregateID,
			}}
	}

	return event.AggregateState{
		TenantID:            state[0].TenantID,
		AggregateType:       state[0].AggregateType,
		AggregateID:         state[0].AggregateID,
		CurrentVersion:      state[0].CurrentVersion,
		LastTransactionTime: state[0].LastTransactionTime,
		LatestValidTime:     state[0].LatestValidTime,
		CreateTime:          state[0].CreateTime,
		CloseTime:           state[0].CloseTime,
	}, err
}

func (l loader) GetAggregateStatesForAggregateType(ctx context.Context, tenantID string, aggregateType string) ([]event.AggregateState, error) {
	states, err := l.retrieveAggregates(ctx, db.IdxTenantIdAggregateType, tenantID, aggregateType)
	if err != nil {
		return nil, err
	}

	if len(states) == 0 {
		return nil, &aggregate.NotFoundError{
			ID: shared.AggregateID{
				TenantID:      tenantID,
				AggregateType: aggregateType,
				AggregateID:   "",
			}}
	}

	var out []event.AggregateState
	for _, state := range states {
		out = append(out, event.AggregateState{
			TenantID:            state.TenantID,
			AggregateType:       state.AggregateType,
			AggregateID:         state.AggregateID,
			CurrentVersion:      state.CurrentVersion,
			LastTransactionTime: state.LastTransactionTime,
			LatestValidTime:     state.LatestValidTime,
			CreateTime:          state.CreateTime,
			CloseTime:           state.CloseTime,
		})
	}

	return out, err
}

func (l loader) GetAggregateStatesForAggregateTypeTill(ctx context.Context, tenantID string, aggregateType string, until time.Time) ([]event.AggregateState, error) {
	allAggregates, err := l.GetAggregateStatesForAggregateType(ctx, tenantID, aggregateType)
	if err != nil {
		return nil, err
	}

	var out []event.AggregateState
	for _, agg := range allAggregates {
		if agg.CreateTime.Before(until) {
			out = append(out, agg)
		}
	}

	return out, err
}

func (l loader) GetPatchFreePeriodsForInterval(ctx context.Context, tenantID, aggregateType, aggregateID string, start time.Time, end time.Time) ([]event.TimeInterval, error) {
	entireStream, err := l.GetTx(ctx).Get(db.TableEvent, db.IdxSetOfId, tenantID, aggregateType, aggregateID)
	if err != nil || entireStream == nil {
		return nil, nil
	}

	var eventsInInterval []event.PersistenceEvent
	for obj := entireStream.Next(); obj != nil; obj = entireStream.Next() {
		incEvt, ok := obj.(db.AutoIncrementEvent)
		if !ok {
			return nil, fmt.Errorf("type cast failed for value %q", obj)
		}
		evt := incEvt.Event

		// only events within search interval are needed downstream
		if (evt.TransactionTime.After(start) || evt.ValidTime.After(start)) &&
			(evt.TransactionTime.Before(end)) || evt.ValidTime.Before(end) {
			eventsInInterval = append(eventsInInterval, evt)
		}
	}

	//no events in time interval
	if len(eventsInInterval) == 0 {
		return nil, nil
	}

	patchRanges, err := l.findPatcheEvents(eventsInInterval, err)
	if err != nil {
		return nil, err
	}

	// if we don't have find patches in the stream, the entire
	// search interval is a patch free period
	if len(patchRanges) == 0 {
		return []event.TimeInterval{{
			Start: start,
			End:   end,
		}}, nil
	}

	// find all points (or rather intervals) within the search interval, not contained in any of the patch intervals(s)
	// (search / []patches)
	searchIntervall, err := timespan.New(start, end)
	if err != nil {
		return nil, fmt.Errorf("could not create time interval:%w", err)
	}
	resultSpans := searchIntervall.Excepts(timespan.NewSpans(patchRanges...))

	return mapper.ToTimeInterval(resultSpans), nil
}

func (l loader) findPatcheEvents(eventsInInterval []event.PersistenceEvent, err error) ([]timespan.Span, error) {
	var patchRanges []timespan.Span

	for _, evt := range eventsInInterval {
		var span timespan.Span
		//we ignore instant events
		if evt.ValidTime.After(evt.TransactionTime) { //switch for future Patch semantic; start <= end for patchRanges
			span, err = timespan.New(evt.TransactionTime, evt.ValidTime)
		} else if evt.ValidTime.Before(evt.TransactionTime) {
			span, err = timespan.New(evt.ValidTime, evt.TransactionTime)
		}
		if err != nil {
			return nil, fmt.Errorf("could not create time interval:%w", err)
		}

		patchRanges = append(patchRanges, span)
	}

	return patchRanges, err
}

func (l loader) GetAggregatesEvents(ctx context.Context, tenantID string, page event.PageDTO) (events []event.PersistenceEvent, pages event.PagesDTO, err error) {
	entireStream, err := l.GetTx(ctx).Get(db.TableEvent, db.IdxTenantId, tenantID)
	if err != nil || entireStream == nil {
		return nil, event.PagesDTO{}, nil
	}

	var allEvents []event.PersistenceEvent
	for obj := entireStream.Next(); obj != nil; obj = entireStream.Next() {
		incEvt, ok := obj.(db.AutoIncrementEvent)
		if !ok {
			return nil, event.PagesDTO{}, fmt.Errorf("type cast failed for value %q", obj)
		}
		evt := incEvt.Event
		allEvents = append(allEvents, evt)
	}

	// filtering
	filteredEvents := filterEvents(allEvents, page.SearchFields)

	// sorting
	sortEvents(filteredEvents, page.SortFields)

	// pagination
	paginatedEvents, first, last := paginateEvents(filteredEvents, page)

	return paginatedEvents, event.PagesDTO{Previous: first, Next: last}, nil
}
