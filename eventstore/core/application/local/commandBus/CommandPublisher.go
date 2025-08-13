package commandBus

import (
	"context"
	"fmt"
	"sync"
)

func NewCommandPublisher() *CommandPublisher {
	return &CommandPublisher{handlers: make(map[string][]CommandHandler)}
}

type CommandPublisher struct {
	sync.RWMutex
	handlers map[string][]CommandHandler
}

func (b *CommandPublisher) Execute(ctx context.Context, cmd Command) error {
	b.RLock()
	defer b.RUnlock()

	for _, handler := range b.handlers[cmd.Name()] {
		err := handler.Execute(ctx, cmd)
		if err != nil {
			return fmt.Errorf("command %q failed:%w", cmd.Name(), err)
		}
	}
	return nil
}

func (b *CommandPublisher) Subscribe(handler CommandHandler, cmds ...Command) {
	b.Lock()
	defer b.Unlock()

	for _, cmd := range cmds {
		handlers := b.handlers[cmd.Name()]
		handlers = append(handlers, handler)
		b.handlers[cmd.Name()] = handlers
	}
}
