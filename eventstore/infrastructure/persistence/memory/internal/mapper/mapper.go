package mapper

import (
	"github.com/global-soft-ba/go-eventstore"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/shared/timespan"
)

func ToTimeInterval(spans timespan.Spans) []event.TimeInterval {
	var out []event.TimeInterval
	for _, span := range spans.Spans() {
		out = append(out, event.TimeInterval{
			Start: span.Start(),
			End:   span.End(),
		})
	}

	return out
}
