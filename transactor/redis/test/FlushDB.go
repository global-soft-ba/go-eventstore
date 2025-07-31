package test

import (
	"context"
	"github.com/redis/go-redis/v9"
	"testing"
)

func FlushDB(t *testing.T, ctx context.Context, rds redis.UniversalClient) {
	if err := rds.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("could not flush db: %s", err)
	}
}
