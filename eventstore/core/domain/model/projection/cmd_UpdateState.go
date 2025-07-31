package projection

import (
	"github.com/global-soft-ba/go-eventstore/instrumentation/port/logger"
)

func (s *Stream) UpdateState(state State) error {
	if err := s.state.isValidProjectionStateChange(state); err != nil {
		return err
	}
	logger.Info("state updated for projection %q: old %q, new %q", s.projection.ID(), s.state, state)

	s.state = state
	return nil
}
