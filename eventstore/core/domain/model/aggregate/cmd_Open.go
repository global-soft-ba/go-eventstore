package aggregate

import "github.com/global-soft-ba/go-eventstore"

func (s *Stream) Open(version int) error {
	if int64(version) != s.currentVersion {
		switch s.options.ConcurrentModificationStrategy {
		case event.Fail:
			return &event.ErrorConcurrentModification{
				TenantID:      s.ID().TenantID,
				AggregateType: s.ID().AggregateType,
				AggregateID:   s.ID().AggregateID,
				Version:       int(s.currentVersion),
			}
		case event.Ignore:
			s.latestVersion = false
			return nil
		}
	}

	s.latestVersion = true
	return nil
}
