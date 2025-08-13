package eventBus

import (
	"context"
	"fmt"
	"github.com/global-soft-ba/go-eventstore/instrumentation/port/metrics"
	"sync"
)

func NewEventPublisher() *EventPublisher {
	return &EventPublisher{handlers: make(map[string][]EventHandler)}
}

type EventPublisher struct {
	sync.RWMutex
	handlers map[string][]EventHandler
}

func (p *EventPublisher) Publish(ctx context.Context, events ...Event) chan error {
	ctx, endSpan := metrics.StartSpan(ctx, "publish events (service)", map[string]interface{}{"amount of events": len(events)})
	defer endSpan()

	errCh := make(chan error, 100)

	go func(returnCh chan error) {
		defer close(returnCh)

		p.RLock()
		defer p.RUnlock()

		for _, event := range events {
			for _, handler := range p.handlers[event.Name()] {
				ch := handler.Receive(ctx, event)

				select {
				case err, ok := <-ch:
					if !ok {
						// Channel was closed, no more errors
						break
					}
					if err != nil {
						returnCh <- err
					}
				case <-ctx.Done():
					returnCh <- fmt.Errorf("publish event %q failed: execution deadline exceeded", event.Name())
					return // Exit the goroutine at timeout
				}
			}
		}
	}(errCh)

	return errCh
}

func (p *EventPublisher) Subscribe(handler EventHandler, events ...Event) {
	p.Lock()
	defer p.Unlock()

	for _, evt := range events {
		handlers := p.handlers[evt.Name()]
		handlers = append(handlers, handler)
		p.handlers[evt.Name()] = handlers
	}
}
