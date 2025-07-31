package aggregate

import "github.com/global-soft-ba/go-eventstore"

type Options struct {
	ConcurrentModificationStrategy event.ConcurrentModificationStrategy
	EphemeralEvents                map[string]bool
	DeleteStrategy                 event.DeleteStrategy
}
