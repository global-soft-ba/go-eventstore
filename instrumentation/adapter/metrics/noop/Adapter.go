package noop

import "context"

type Adapter struct{}

func (a Adapter) StartSpan(ctx context.Context, name string, tags map[string]interface{}) (context.Context, func()) {
	return ctx, func() {}
}
