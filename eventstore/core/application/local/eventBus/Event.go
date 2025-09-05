package eventBus

// Event interface for describing Domain Event
type Event interface {
	Name() string
}
