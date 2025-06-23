package commands

import (
	"github.com/global-soft-ba/go-eventstore"
	"github.com/global-soft-ba/go-eventstore/eventstore/core/domain/model/projection"
)

func CmdDeleteEvent(streams []projection.Stream) DeleteEvent {
	return DeleteEvent{
		streams: streams,
	}
}

type DeleteEvent struct {
	event   event.PersistenceEvent
	streams []projection.Stream
}

func (e DeleteEvent) Name() string {
	return "cmd.aggregate.delete.event"
}

func (e DeleteEvent) Streams() []projection.Stream {
	return e.streams
}
