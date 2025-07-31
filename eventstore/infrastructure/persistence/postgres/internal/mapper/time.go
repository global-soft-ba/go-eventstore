package mapper

import (
	"time"
)

func MapToTimeStampTZ(nanoseconds int64) time.Time {
	if nanoseconds == 0 {
		return time.Time{}
	}
	return time.Unix(0, nanoseconds).UTC()
}

func MapToNanoseconds(timestampTZ time.Time) int64 {
	if timestampTZ.IsZero() {
		return 0
	}
	return timestampTZ.UTC().UnixNano()
}
