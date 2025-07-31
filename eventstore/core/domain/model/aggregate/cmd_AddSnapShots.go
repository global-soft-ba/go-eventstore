package aggregate

import (
	"fmt"
	event2 "github.com/global-soft-ba/go-eventstore"
	"time"
)

func during(startInterval, endInterval, time time.Time) bool {
	if (startInterval.Before(time) || startInterval.Equal(time)) && endInterval.After(time) || endInterval.Equal(time) {
		return true
	}

	return false
}

func (s *Stream) addSnapShots(snap event2.PersistenceEvent) error {
	if !s.latestVersion && snap.Class == event2.SnapShot {
		return event2.NewErrorSnapShotNotAllowed(
			nil, s.ID())
	}

	for _, e := range s.events {
		switch e.Class {
		case event2.HistoricalPatch:
			if during(e.ValidTime, e.TransactionTime, snap.ValidTime) {
				return fmt.Errorf("snapshot event of aggregate %s is within a historical patch interval", s.id)
			}

		case event2.FuturePatch:
			if during(e.TransactionTime, e.ValidTime, snap.ValidTime) {
				return fmt.Errorf("snapshot event of aggregate %s is within a future patch interval", s.id)
			}
		}
	}

	s.snapShots = append(s.snapShots, snap)
	return nil
}
