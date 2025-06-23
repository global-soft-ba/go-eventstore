package commandBus

import "context"

type CommandHandler interface {
	Execute(ctx context.Context, cmd Command) error
}
