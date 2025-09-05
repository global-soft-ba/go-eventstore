package testdata

import (
	"context"
	"testing"
)

type IWriter[T, V any] interface {
	Write(t *testing.T, ctx context.Context, data ...T) error
	Execute(t *testing.T, ctx context.Context, executeFunc string, args ...V) error
}
