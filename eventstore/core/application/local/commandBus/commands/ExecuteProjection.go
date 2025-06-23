package commands

import "github.com/global-soft-ba/go-eventstore/eventstore/core/domain/model/projection"

func CmdExecuteProjections(stream []projection.Stream) ExecuteProjection {
	return ExecuteProjection{
		stream: stream,
	}
}

type ExecuteProjection struct {
	stream []projection.Stream
}

func (e ExecuteProjection) Name() string {
	return "cmd.projection.execute"
}

func (e ExecuteProjection) Streams() []projection.Stream {
	return e.stream
}
