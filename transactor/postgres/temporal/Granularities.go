package temporal

import (
	"fmt"
	"time"
)

type Granularity string

// Granularity the values are compatible with postgres date_trunc function
const (
	Microsecond Granularity = "microseconds"
	Millisecond Granularity = "milliseconds"
	Second      Granularity = "second"
	Minute      Granularity = "minute"
	Hour        Granularity = "hour"
	Day         Granularity = "day"
	Week        Granularity = "week"
	Month       Granularity = "month"
	Quarter     Granularity = "quarter"
	Year        Granularity = "year"
	Decade      Granularity = "decade"
	Millennium  Granularity = "millennium"
)

// TruncateDate shortens a date to the specified precision (similar to postgres)
func TruncateDate(date time.Time, granularity Granularity) (time.Time, error) {
	switch granularity {
	case Microsecond:
		return date.Truncate(time.Microsecond), nil
	case Millisecond:
		return date.Truncate(time.Millisecond), nil
	case Second:
		return date.Truncate(time.Second), nil
	case Minute:
		return date.Truncate(time.Minute), nil
	case Hour:
		return date.Truncate(time.Hour), nil
	case Day:
		return time.Date(date.Year(), date.Month(), date.Day(), 0, 0, 0, 0, date.Location()), nil
	case Week:
		weekday := int(date.Weekday())
		startOfWeek := date.AddDate(0, 0, -weekday)
		return time.Date(startOfWeek.Year(), startOfWeek.Month(), startOfWeek.Day(), 0, 0, 0, 0, date.Location()), nil
	case Month:
		return time.Date(date.Year(), date.Month(), 1, 0, 0, 0, 0, date.Location()), nil
	case Quarter:
		quarter := (int(date.Month()) - 1) / 3
		startOfQuarter := time.Date(date.Year(), time.Month(quarter*3+1), 1, 0, 0, 0, 0, date.Location())
		return startOfQuarter, nil
	case Year:
		return time.Date(date.Year(), time.January, 1, 0, 0, 0, 0, date.Location()), nil
	case Decade:
		decadeYear := date.Year() / 10 * 10
		return time.Date(decadeYear, time.January, 1, 0, 0, 0, 0, date.Location()), nil
	case Millennium:
		millenniumYear := date.Year() / 1000 * 1000
		return time.Date(millenniumYear, time.January, 1, 0, 0, 0, 0, date.Location()), nil
	default:
		return time.Time{}, fmt.Errorf("unknown granularity: %v", granularity)
	}
}

// ForTestDateTrunc This function does not return an error and panics instead. Should be used for test cases, only.
func ForTestDateTrunc(time time.Time, gran Granularity) time.Time {
	res, err := TruncateDate(time, gran)
	if err != nil {
		panic("could not truncate date")
	}
	return res
}

func (g Granularity) IsValid() bool {
	switch g {
	case Microsecond, Millisecond, Second, Minute, Hour, Day, Week, Month, Quarter, Year, Decade, Millennium:
		return true
	default:
		return false
	}
}
