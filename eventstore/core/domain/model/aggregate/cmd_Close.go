package aggregate

import (
	"fmt"
	"github.com/global-soft-ba/go-eventstore"
	"time"
)

func (s *Stream) CloseStream(evt event.PersistenceEvent, closed time.Time) error {
	if evt.Class != event.CloseStreamEvent {
		return fmt.Errorf("wrong event class for close")
	}

	if !s.closeTime.IsZero() {
		return fmt.Errorf("stream already closed")
	}
	if evt.FromMigration || !evt.ValidTime.IsZero() {
		closed = evt.ValidTime
	}

	s.closeTime = closed
	return nil
}
