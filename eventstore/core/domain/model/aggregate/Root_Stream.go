package aggregate

import (
	"fmt"
	"github.com/global-soft-ba/go-eventstore"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/port/persistence/aggregate"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/shared"
	"time"
)

const initVersion = 0

func LoadStreamFromDTO(dto aggregate.DTO, opt Options) Stream {
	return Stream{
		id:                  shared.NewAggregateID(dto.TenantID, dto.AggregateType, dto.AggregateID),
		currentVersion:      dto.CurrentVersion,
		lastTransactionTime: dto.LastTransactionTime,
		latestValidTime:     dto.LatestValidTime,
		createTime:          dto.CreateTime,
		closeTime:           dto.CloseTime,
		events:              nil,
		snapShots:           nil,
		options:             opt,
	}
}

func CreateEmptyStream(aggregateID shared.AggregateID, opt Options) Stream {
	return Stream{
		id:                  aggregateID,
		currentVersion:      initVersion,
		lastTransactionTime: time.Time{},
		latestValidTime:     time.Time{},
		createTime:          time.Time{},
		closeTime:           time.Time{},
		events:              nil,
		snapShots:           nil,
		options:             opt,
	}
}

type Stream struct {
	id shared.AggregateID

	currentVersion int64
	//The flag signals whether we have the latest version. Due to the Ignore-ConcurrentModificationStrategy it is possible
	//that we don't have the latest version of the aggregate. In such cases, for example,  snapshots are not allowed.
	latestVersion bool

	lastTransactionTime time.Time
	latestValidTime     time.Time

	createTime time.Time
	closeTime  time.Time

	options Options

	events    []event.PersistenceEvent
	snapShots []event.PersistenceEvent
}

func (s *Stream) ID() shared.AggregateID {
	return s.id
}

func (s *Stream) CurrentVersion() int64 {
	return s.currentVersion
}

func (s *Stream) LastTransactionTime() time.Time {
	return s.lastTransactionTime
}

func (s *Stream) LatestValidTime() time.Time {
	return s.latestValidTime
}

func (s *Stream) CreateTime() time.Time {
	return s.createTime
}

func (s *Stream) CloseTime() time.Time {
	return s.closeTime
}

func (s *Stream) Options() Options {
	return s.options
}

func (s *Stream) Events() []event.PersistenceEvent {
	return s.events
}

func (s *Stream) SnapShots() []event.PersistenceEvent {
	return s.snapShots
}

func (s *Stream) EarliestPatchInCurrentStream() time.Time {
	var earliestPatch time.Time
	for _, evt := range s.events {
		switch evt.Class {
		case event.HistoricalPatch:

			if earliestPatch.IsZero() {
				earliestPatch = evt.ValidTime
			} else if evt.ValidTime.Before(earliestPatch) {
				earliestPatch = evt.ValidTime
			}
		case event.FuturePatch:
			if earliestPatch.IsZero() {
				earliestPatch = evt.ValidTime
			} else if evt.TransactionTime.Before(earliestPatch) {
				earliestPatch = evt.TransactionTime
			}
		}
	}
	return earliestPatch
}

func (s *Stream) applyStreamConsistencyChecks(evt event.PersistenceEvent) (err error) {
	switch {
	case !s.id.Equal(shared.NewAggregateID(evt.TenantID, evt.AggregateType, evt.AggregateID)):
		err = fmt.Errorf("event for stream %q does not belong to stream %q", shared.NewAggregateID(evt.TenantID, evt.AggregateType, evt.AggregateID), s.id)
	case !s.closeTime.IsZero():
		if s.closeTime.Before(evt.ValidTime) {
			err = fmt.Errorf("cannot write to a closed stream %q", s.ID())
		}
	case evt.ValidTime.Before(s.createTime):
		err = fmt.Errorf("valid time %v is before creation time %v", evt.ValidTime, s.createTime)
	case evt.TransactionTime.Before(s.lastTransactionTime):
		err = fmt.Errorf("transaction time %v is before last stored transaction time %v", evt.TransactionTime, s.lastTransactionTime)
	case evt.Class != event.CreateStreamEvent && s.currentVersion == initVersion:
		err = &event.ErrorInsertBeforeCreateEventStream{
			TenantID:      evt.TenantID,
			AggregateID:   evt.AggregateID,
			AggregateType: evt.AggregateType,
		}
	case evt.Class == event.CloseStreamEvent && !s.closeTime.IsZero():
		err = fmt.Errorf("stream %q has already a closed event at %s", s.ID(), s.CloseTime())
	case evt.Class == event.CloseStreamEvent && !(evt.ValidTime.Equal(evt.TransactionTime) || evt.ValidTime.After(evt.TransactionTime)):
		err = fmt.Errorf("close event for aggregate %q has a valid time %v before its transaction time %v", evt.AggregateID, evt.ValidTime, evt.TransactionTime)
	case evt.Class == event.CloseStreamEvent && s.latestValidTime.After(evt.ValidTime):
		err = fmt.Errorf("stream %q cannot have latest valid time %v greater then close time %v", s.ID(), s.latestValidTime, evt.ValidTime)
	}
	if err != nil {
		return fmt.Errorf("consistency check failed: %w", err)
	}

	return nil
}

func (s *Stream) addTimeStamps(evt event.PersistenceEvent, time time.Time) event.PersistenceEvent {
	//NewMigrationEvent has already defined timestamps, which should not be changed
	if evt.FromMigration {
		return evt
	}
	switch evt.Class {
	case event.CreateStreamEvent, event.CloseStreamEvent:
		if evt.ValidTime.IsZero() {
			evt.ValidTime = time
		}
		evt.TransactionTime = time

	case event.InstantEvent, event.SnapShot:
		evt.ValidTime = time
		evt.TransactionTime = time
	case event.HistoricalPatch, event.FuturePatch, event.HistoricalSnapShot:
		evt.TransactionTime = time
	}

	return evt
}

func (s *Stream) addVersion(evt event.PersistenceEvent, currentVersion int64) (event.PersistenceEvent, int64) {
	switch evt.Class {
	case event.CreateStreamEvent, event.CloseStreamEvent, event.InstantEvent, event.HistoricalPatch, event.FuturePatch:
		currentVersion++
		evt.Version = int(currentVersion)
	case event.SnapShot, event.HistoricalSnapShot:
		evt.Version = int(currentVersion)
	}

	return evt, currentVersion
}

func (s *Stream) upDateStreamMetaData(evt event.PersistenceEvent, newVersion int64) {
	s.currentVersion = newVersion
	s.lastTransactionTime = evt.TransactionTime
	if evt.ValidTime.After(s.latestValidTime) {
		s.latestValidTime = evt.ValidTime
	}
}
