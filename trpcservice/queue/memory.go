package queue

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/DocJlm/trpc-agent-service/trpcservice/store"
)

type MemoryQueue struct {
	items  chan Delivery
	dead   chan Delivery
	closed atomic.Bool
	nextID atomic.Uint64
}

func NewMemoryQueue(size int) *MemoryQueue {
	if size < 1 {
		size = 128
	}
	return &MemoryQueue{items: make(chan Delivery, size), dead: make(chan Delivery, size)}
}

func (q *MemoryQueue) Publish(ctx context.Context, task store.DispatchTask) error {
	id := fmt.Sprintf("memory-%d", q.nextID.Add(1))
	select {
	case q.items <- Delivery{ID: id, Task: task}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (q *MemoryQueue) Receive(ctx context.Context, _ string, _ time.Duration) (Delivery, error) {
	select {
	case item, ok := <-q.items:
		if !ok {
			return Delivery{}, context.Canceled
		}
		return item, nil
	case <-ctx.Done():
		return Delivery{}, ctx.Err()
	}
}

func (q *MemoryQueue) Ack(context.Context, Delivery) error { return nil }

func (q *MemoryQueue) Retry(ctx context.Context, delivery Delivery) error {
	delivery.Task.Attempts++
	select {
	case q.items <- delivery:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (q *MemoryQueue) Dead(ctx context.Context, delivery Delivery, _ error) error {
	select {
	case q.dead <- delivery:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (q *MemoryQueue) Close() error {
	if q.closed.CompareAndSwap(false, true) {
		close(q.items)
		close(q.dead)
	}
	return nil
}

type memoryLockState struct {
	mu    sync.Mutex
	fence int64
}

type MemoryLocker struct {
	mu    sync.Mutex
	locks map[string]*memoryLockState
}

func NewMemoryLocker() *MemoryLocker { return &MemoryLocker{locks: make(map[string]*memoryLockState)} }

func (l *MemoryLocker) Acquire(ctx context.Context, key string, _ time.Duration) (Lease, error) {
	l.mu.Lock()
	state := l.locks[key]
	if state == nil {
		state = &memoryLockState{}
		l.locks[key] = state
	}
	l.mu.Unlock()

	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		if state.mu.TryLock() {
			state.fence++
			return &memoryLease{state: state, fence: state.fence}, nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ticker.C:
		}
	}
}

type memoryLease struct {
	state *memoryLockState
	fence int64
	once  sync.Once
}

func (l *memoryLease) Fence() int64                               { return l.fence }
func (l *memoryLease) Renew(context.Context, time.Duration) error { return nil }
func (l *memoryLease) Release(context.Context) error {
	l.once.Do(func() {
		l.state.mu.Unlock()
	})
	return nil
}
