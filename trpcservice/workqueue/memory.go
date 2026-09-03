package workqueue

import (
	"context"
	"errors"
	"sync"
)

// MemoryQueue is an in-process at-least-once queue for tests and tutorials.
type MemoryQueue struct {
	messages chan AgentTask
	closeCtx context.Context
	cancel   context.CancelFunc
	once     sync.Once
}

func NewMemoryQueue(buffer int) *MemoryQueue {
	if buffer <= 0 {
		buffer = 128
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &MemoryQueue{messages: make(chan AgentTask, buffer), closeCtx: ctx, cancel: cancel}
}

func (q *MemoryQueue) Publish(ctx context.Context, task AgentTask) error {
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case q.messages <- task:
		return nil
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-q.closeCtx.Done():
		return errors.New("memory work queue is closed")
	}
}

func (q *MemoryQueue) Receive(ctx context.Context) (Delivery, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case task := <-q.messages:
		return &memoryDelivery{queue: q, task: task}, nil
	case <-ctx.Done():
		return nil, context.Cause(ctx)
	case <-q.closeCtx.Done():
		return nil, errors.New("memory work queue is closed")
	}
}

func (q *MemoryQueue) Ready(ctx context.Context) error {
	if ctx != nil && ctx.Err() != nil {
		return context.Cause(ctx)
	}
	select {
	case <-q.closeCtx.Done():
		return errors.New("memory work queue is closed")
	default:
		return nil
	}
}

func (q *MemoryQueue) Close() error {
	if q == nil {
		return nil
	}
	q.once.Do(q.cancel)
	return nil
}

type memoryDelivery struct {
	queue *MemoryQueue
	task  AgentTask
	once  sync.Once
	err   error
}

func (d *memoryDelivery) Task() AgentTask { return d.task }

func (d *memoryDelivery) Ack(context.Context) error {
	d.once.Do(func() {})
	return d.err
}

func (d *memoryDelivery) Retry(ctx context.Context) error {
	d.once.Do(func() {
		task := d.task
		task.Attempt++
		d.err = d.queue.Publish(ctx, task)
	})
	return d.err
}

var _ Queue = (*MemoryQueue)(nil)
var _ Delivery = (*memoryDelivery)(nil)
