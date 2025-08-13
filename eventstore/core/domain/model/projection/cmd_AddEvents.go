package projection

import (
	"github.com/global-soft-ba/go-eventstore"
)

func (s *Stream) AddEvents(events ...event.PersistenceEvent) error {
	s.events = append(s.Events(), events...)
	return nil
}
