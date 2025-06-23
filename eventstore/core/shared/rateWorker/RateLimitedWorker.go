package rateWorker

import (
	"context"
	"github.com/google/uuid"
	"sync"
)

func NewRateLimitedWorker[T any](rate int, inputCh chan ExecutionParams[T]) RateLimitedWorker[T] {
	return RateLimitedWorker[T]{
		inputCh:           inputCh,
		stopCh:            make(chan struct{}),
		msgCh:             make(chan ExecutionParams[T], rate),
		currentSubscriber: make(map[string][]chan T),
		currentRun:        uuid.NewString(),
	}
}

type ExecutionParams[T any] struct {
	Ctx      context.Context
	ResultCh chan T
	Execute  func(ctx context.Context) T
}

type RateLimitedWorker[T any] struct {
	sync.RWMutex
	inputCh           chan ExecutionParams[T]
	msgCh             chan ExecutionParams[T]
	stopCh            chan struct{}
	currentSubscriber map[string][]chan T
	currentRun        string
}

func (p *RateLimitedWorker[T]) Input() chan ExecutionParams[T] {
	return p.inputCh
}

func (p *RateLimitedWorker[T]) startInputReceiver() {
	for {
		select {
		case in, openCh := <-p.inputCh:
			if !openCh {
				goto end
			}

			select {
			case p.msgCh <- in:
			default:
				p.subscribe(in.ResultCh)
			}

		}
	}

end:
	p.Stop()
}

func (p *RateLimitedWorker[T]) Start() {
	//start of input queue
	go p.startInputReceiver()

	//execution loop
	for {
		select {
		case <-p.stopCh:
			break
		case param := <-p.msgCh:
			p.subscribe(param.ResultCh)
			runID := p.getCurrentRunAndSetNextRun()
			//execute injectedFunction func(ctx) error
			result := param.Execute(param.Ctx)
			//send executionResult to all subscriber of this run
			p.publish(runID, result)
			//delete the subscriber of this run
			p.unsubscribe(runID)
		}
	}
}

func (p *RateLimitedWorker[T]) Stop() {
	close(p.stopCh)
}

func (p *RateLimitedWorker[T]) getCurrentRunAndSetNextRun() (currentRunID string) {
	p.Lock()
	nextRun := uuid.NewString()
	currentRunID = p.currentRun
	p.currentRun = nextRun
	p.Unlock()

	return currentRunID

}

func (p *RateLimitedWorker[T]) subscribe(resCh chan T) {
	p.Lock()
	run := p.currentRun
	p.currentSubscriber[run] = append(p.currentSubscriber[p.currentRun], resCh)
	p.Unlock()
}

func (p *RateLimitedWorker[T]) unsubscribe(runID string) {
	p.Lock()
	delete(p.currentSubscriber, runID)
	p.Unlock()
}

func (p *RateLimitedWorker[T]) publish(runID string, result T) {
	p.RLock()
	currSubs := p.currentSubscriber[runID]
	defer p.RUnlock()

	for _, msgCh := range currSubs {
		// msgCh is buffered, use non-blocking send to protect the broker:
		select {
		case msgCh <- result:
		default:
		}
		//close as broadcast that th run is done
		close(msgCh)
	}
}
