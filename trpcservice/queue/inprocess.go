package queue

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"

	"github.com/cyl6/trpc-agent-service/trpcservice/delivery"
	"github.com/cyl6/trpc-agent-service/trpcservice/worker"
)

var (
	ErrFull   = errors.New("worker queue is full")
	ErrClosed = errors.New("worker queue is closed")
)

type Dispatcher interface {
	Submit(context.Context, worker.Task) error
}

// ReadinessProbe is implemented by dispatchers that can verify whether they
// can currently accept work. The HTTP readiness endpoint uses it without
// expanding the basic Dispatcher contract used by tests and integrations.
type ReadinessProbe interface {
	Ready(context.Context) error
}

type Processor interface {
	Process(context.Context, worker.Task) (worker.Result, error)
}

type InProcess struct {
	processor Processor
	tasks     chan worker.Task
	onError   func(error)
	mu        sync.RWMutex
	closed    bool
	wg        sync.WaitGroup
}

func NewInProcess(processor Processor, size, workers int, onError func(error)) *InProcess {
	if size <= 0 {
		size = 256
	}
	if workers <= 0 {
		workers = 4
	}
	q := &InProcess{processor: processor, tasks: make(chan worker.Task, size), onError: onError}
	q.wg.Add(workers)
	for i := 0; i < workers; i++ {
		go q.consume()
	}
	return q
}

func (q *InProcess) Submit(ctx context.Context, task worker.Task) error {
	q.mu.RLock()
	defer q.mu.RUnlock()
	if q.closed {
		return ErrClosed
	}
	select {
	case q.tasks <- task:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("enqueue task: %w", ctx.Err())
	default:
		return ErrFull
	}
}

func (q *InProcess) Ready(context.Context) error {
	q.mu.RLock()
	defer q.mu.RUnlock()
	if q.closed {
		return ErrClosed
	}
	if len(q.tasks) >= cap(q.tasks) {
		return ErrFull
	}
	return nil
}

func (q *InProcess) consume() {
	defer q.wg.Done()
	for task := range q.tasks {
		ctx := otel.GetTextMapPropagator().Extract(context.Background(), propagation.MapCarrier(task.TraceCarrier))
		if q.processor == nil {
			continue
		}
		var err error
		for attempt := 0; attempt < 3; attempt++ {
			if _, err = q.processor.Process(ctx, task); err == nil {
				break
			}
			var deliveryFailure *delivery.FailureError
			if errors.As(err, &deliveryFailure) {
				if deliveryFailure.Outcome != delivery.RetryableNotSent || !deliveryFailure.RetryWholeTask {
					// Unknown may already have produced a provider side effect, while
					// a later-part failure cannot replay already confirmed parts.
					break
				}
			}
			if attempt < 2 {
				time.Sleep(time.Duration(attempt+1) * 100 * time.Millisecond)
			}
		}
		if err != nil && q.onError != nil {
			q.onError(err)
		}
	}
}

func (q *InProcess) Close() error {
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return nil
	}
	q.closed = true
	close(q.tasks)
	q.mu.Unlock()
	q.wg.Wait()
	return nil
}
