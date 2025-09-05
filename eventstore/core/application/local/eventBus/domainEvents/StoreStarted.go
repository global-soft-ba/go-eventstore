package domainEvents

func EventStoreStarted() StoreStarted {
	return StoreStarted{}
}

type StoreStarted struct {
}

func (e StoreStarted) Name() string {
	return "event.store.started"
}
