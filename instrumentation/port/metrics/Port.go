package metrics

import (
	"context"
)

var (
	metrics Port
)

type Port interface {
	StartSpan(ctx context.Context, name string, tags map[string]interface{}) (context.Context, func())
}

func SetMetrics(m Port) {
	metrics = m
}

func IsMetricsSet() bool {
	return metrics != nil
}

func StartSpan(ctx context.Context, name string, attributes map[string]interface{}) (context.Context, func()) {
	if metrics != nil {
		return metrics.StartSpan(ctx, name, attributes)
	}
	return ctx, func() {}
}
