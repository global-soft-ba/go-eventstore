package aggregate

import (
	"github.com/global-soft-ba/go-eventstore"
	"time"
)

func (s *Stream) AddEvent(evt event.PersistenceEvent, time time.Time) (event.PersistenceEvent, error) {
	//Init case of a stream
	if evt.Class == event.CreateStreamEvent {
		err := s.InitStream(evt, time)
		if err != nil {
			return event.PersistenceEvent{}, err
		}
	}

	versionedEvt, err := s.prepareEventForInsertInStream(evt, time)
	if err != nil {
		return evt, err
	}

	//check if event is ephemeral and if, do not add it to the stream
	if ephemeral, _ := s.Options().EphemeralEvents[versionedEvt.Type]; ephemeral {
		return versionedEvt, nil
	}

	switch versionedEvt.Class {
	case event.SnapShot, event.HistoricalSnapShot:
		if err = s.addSnapShots(versionedEvt); err != nil {
			return event.PersistenceEvent{}, err
		}
	default:
		s.events = append(s.events, versionedEvt)
		if versionedEvt.Class == event.CloseStreamEvent {
			if err = s.CloseStream(versionedEvt, time); err != nil {
				return event.PersistenceEvent{}, err
			}
		}
	}

	return versionedEvt, nil
}

func (s *Stream) prepareEventForInsertInStream(evt event.PersistenceEvent, time time.Time) (event.PersistenceEvent, error) {
	evt = s.addTimeStamps(evt, time)
	evt, newVersion := s.addVersion(evt, s.currentVersion)
	if err := s.applyStreamConsistencyChecks(evt); err != nil {
		return event.PersistenceEvent{}, err
	}
	s.upDateStreamMetaData(evt, newVersion)
	return evt, nil
}
