package audit

import (
	"context"
	"errors"
	"sync"
)

type MemoryWriter struct {
	mu     sync.Mutex
	closed bool
	events []Event
}

func NewMemoryWriter() *MemoryWriter { return &MemoryWriter{} }

func (w *MemoryWriter) Record(ctx context.Context, event Event) error {
	if ctx != nil && ctx.Err() != nil {
		return context.Cause(ctx)
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return errors.New("audit writer is closed")
	}
	w.events = append(w.events, sanitizeEvent(event))
	return nil
}

func (w *MemoryWriter) Events() []Event {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]Event(nil), w.events...)
}

func (w *MemoryWriter) Ready(ctx context.Context) error {
	if ctx != nil && ctx.Err() != nil {
		return context.Cause(ctx)
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return errors.New("audit writer is closed")
	}
	return nil
}

func (w *MemoryWriter) Close() error {
	if w == nil {
		return nil
	}
	w.mu.Lock()
	w.closed = true
	w.mu.Unlock()
	return nil
}

var _ Writer = (*MemoryWriter)(nil)
