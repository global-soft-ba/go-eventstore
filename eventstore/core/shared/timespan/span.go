package timespan

import (
	externalSpan "github.com/go-follow/time-interval"
	"time"
)

func newForTest(start, end time.Time) Span {
	s, _ := New(start, end)
	return s
}

// New initialization of a new time interval.
func New(start, end time.Time) (Span, error) {
	span, err := externalSpan.New(start, end)
	if err != nil {
		return Span{}, err
	}
	return Span{span}, err
}

type Span struct {
	span externalSpan.Span
}

// Start returning start time interval
func (s *Span) Start() time.Time {
	return s.span.Start()
}

// End returning end time interval
func (s *Span) End() time.Time {
	return s.span.End()
}

// String implementation interface stringer for Span
func (s *Span) String() string {
	return s.span.String()
}

// IsEmpty  defines empty spacing
func (s *Span) IsEmpty() bool {
	return s.span.IsEmpty()
}

// offset - possible deviation from the time interval
func (s *Span) Equal(input Span, offset ...time.Duration) bool {
	return s.span.Equal(input.span, offset...)
}

// Union of two time intervals. (OR)
func (s *Span) Union(input Span) Spans {
	res := s.span.Union(input.span)
	return bridgeFromLibToSpans(res.Spans()...)
}

// Intersection of two time intervals (AND)
func (s *Span) Intersection(input Span) Span {
	res := s.span.Intersection(input.span)
	return Span{res}
}

// Except  difference in time intervals - from input (s \ input).
func (s *Span) Except(input Span) Spans {
	res := s.span.Except(input.span)
	return bridgeFromLibToSpans(res.Spans()...)
}

// Excepts  difference in time intervals - from input (s \ input).
func (s *Span) Excepts(input Spans) Spans {
	sps := NewSpans(*s)
	return sps.Except(input.Spans()...)
}
