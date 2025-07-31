package rateWorker

import (
	"context"
	"go.uber.org/atomic"
	"math/rand"
	"sync"
	"testing"
	"time"
)

func TestWorkerRun(t *testing.T) {
	type result struct {
		Err error
		ID  int
	}
	inCh := make(chan ExecutionParams[result], 2000)
	w := NewRateLimitedWorker[result](2, inCh)
	go w.Start()

	counter := 2

	gotCounter := atomic.NewInt32(0)

	wg := sync.WaitGroup{}
	for i := 0; i < counter; i++ {
		j := i
		wg.Add(1)

		go func() {
			resCh := make(chan result)

			sl := int64(rand.Intn(10))
			time.Sleep(time.Duration(sl) * time.Millisecond)

			inCh <- ExecutionParams[result]{
				Ctx:      context.Background(),
				ResultCh: resCh,
				Execute: func(ctx context.Context) result {
					time.Sleep(10 * time.Millisecond)
					return result{
						Err: nil,
						ID:  j,
					}
				},
			}

			<-resCh
			wg.Done()
			gotCounter.Add(1)
		}()

	}

	wg.Wait()
	if gotCounter.Load() != 2 {
		t.Errorf("RateLimiter Worker failed want %v, got %v", 2, gotCounter.Load())
	}
}
