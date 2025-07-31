package eventBus

import "context"

type EventHandler interface {
	Receive(ctx context.Context, evt Event) chan error
}
