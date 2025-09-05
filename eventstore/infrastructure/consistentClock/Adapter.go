package consistentClock

import (
	"github.com/global-soft-ba/go-eventstore/eventstore/core/port/consistentClock"
	"time"
)

func New() consistentClock.Port {
	return &Adapter{}
}

type Adapter struct {
}

func (a Adapter) Now() time.Time {
	//TODO Time synchronizing
	// - we should keep in mind that the time between different pods are "equal enough"
	// - within and pod we should have a protection, that a wrong started pod /time destroy temporal order
	return time.Now()
}
