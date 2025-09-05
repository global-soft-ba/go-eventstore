package projection

func (s *Stream) DeleteEvents() {
	s.events = nil
}
