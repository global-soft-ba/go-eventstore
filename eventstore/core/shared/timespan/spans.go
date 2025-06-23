package timespan

import (
	externalSpan "github.com/go-follow/time-interval"
	"time"
)

// NewMany initialization for multiple time intervals.
func NewSpans(spans ...Span) Spans {
	return Spans{bridgeFromSpansToLib(spans...)}
}

func bridgeFromLibToSpans(s ...externalSpan.Span) Spans {
	return NewSpans(bridgeFromLibToSpanArray(s...)...)
}

func bridgeFromLibToSpanArray(s ...externalSpan.Span) []Span {
	var out []Span
	for _, span := range s {
		out = append(out, Span{span})
	}

	return out
}

func bridgeFromSpansToLib(s ...Span) externalSpan.SpanMany {
	var out []externalSpan.Span
	for _, span := range s {
		out = append(out, span.span)
	}

	return externalSpan.NewMany(out...)
}

func bridgeFromArraySpansToLib(s ...Spans) []externalSpan.SpanMany {
	var out []externalSpan.SpanMany
	for _, span := range s {
		out = append(out, bridgeFromSpansToLib(span.Spans()...))
	}

	return out
}

func bridgeFromSpanArrayToLib(s ...Span) []externalSpan.Span {
	var out []externalSpan.Span
	for _, span := range s {
		out = append(out, span.span)
	}

	return out
}

type Spans struct {
	spans externalSpan.SpanMany
}

func (s *Spans) String() string {
	return s.spans.String()
}

// Spans get an array of intervals.
func (s *Spans) Spans() []Span {
	return bridgeFromLibToSpanArray(s.spans.Spans()...)
}

// AddSpans adding several time slots at once to the existing one Spans.
func (s *Spans) AddSpans(spans ...Span) {
	s.spans.AddMany(bridgeFromSpanArrayToLib(spans...)...)
}

// ExceptionIfIntersection excludes (entire) spans from the SpanMany if there is an intersection with another SpanMany.
// offset - possible deviation from the time interval
func (s *Spans) ExceptionIfIntersection(input Spans, offset ...time.Duration) Spans {
	res := s.spans.ExceptionIfIntersection(bridgeFromSpansToLib(input.Spans()...), offset...)
	return bridgeFromLibToSpans(res.Spans()...)
}

// Except difference between each array element SpanMany and input Span  (s[i] \ input).
// Returns the elements of SpanMany, where the time interval remains after the Except operation with input.
// Before returning the final result is sorted and merged.
func (s *Spans) Except(input ...Span) Spans {
	res := *s
	for _, span := range input {
		newSpans := NewSpans()
		for _, sp := range res.Spans() {
			exc := sp.Except(span)
			newSpans.AddSpans(exc.Spans()...)
		}
		res = newSpans
	}

	if !(len(res.Spans()) > 0) {
		return Spans{}
	}

	return res
}

// Union concatenation SpanMany of array SpanMany.
func (s *Spans) Union(input ...Spans) Spans {
	res := s.spans.Union(bridgeFromArraySpansToLib(input...)...)
	return Spans{res}
}
