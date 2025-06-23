package mapper

import (
	"github.com/global-soft-ba/go-eventstore"
)

func ToTimeIntervalls(rows []struct {
	StartRange int64
	EndRange   int64
}) (out []event.TimeInterval) {
	for _, row := range rows {
		out = append(out, toTimeIntervall(row))
	}

	return out
}

func toTimeIntervall(row struct {
	StartRange int64
	EndRange   int64
}) event.TimeInterval {
	return event.TimeInterval{
		Start: MapToTimeStampTZ(row.StartRange),
		End:   MapToTimeStampTZ(row.EndRange)}
}
