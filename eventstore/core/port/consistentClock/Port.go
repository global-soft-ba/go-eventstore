package consistentClock

import "time"

// We use time as a unique order for events. This also applies to cross aggregate stream
// projections. With regard to multiple pods, the synchronicity of the time between the pods must be ensured.
// This is done by this consistent clock.

type Port interface {
	Now() time.Time
}
