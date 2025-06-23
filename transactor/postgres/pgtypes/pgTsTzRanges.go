package pgtypes

import (
	"github.com/jackc/pgx/v5/pgtype"
	"time"
)

func PgTimeStampRanges(lower time.Time, upper time.Time, lowerType, upperType pgtype.BoundType) pgtype.Range[time.Time] {
	var result pgtype.Range[time.Time]

	switch {
	case lower.IsZero() && upper.IsZero():
		result.LowerType = pgtype.Unbounded
		result.UpperType = pgtype.Unbounded
		result.Valid = true
	case lower.IsZero() && !upper.IsZero():
		result.LowerType = pgtype.Unbounded
		result.Upper = upper
		result.UpperType = upperType
		result.Valid = true
	case !lower.IsZero() && upper.IsZero():
		result.Lower = lower
		result.LowerType = lowerType
		result.UpperType = pgtype.Unbounded
		result.Valid = true
	default:
		result.Lower = lower
		result.LowerType = lowerType
		result.Upper = upper
		result.UpperType = upperType
		result.Valid = true
	}

	return result

}
