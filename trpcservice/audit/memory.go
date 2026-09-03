package audit

import (
	"context"
	"errors"
	"sort"
	"sync"
)

type MemoryWriter struct {
	mu     sync.Mutex
	closed bool
	events []Event
}

func (w *MemoryWriter) Query(ctx context.Context, query Query) ([]Event, error) {
	if ctx != nil && ctx.Err() != nil {
		return nil, context.Cause(ctx)
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	limit := query.Limit
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	result := make([]Event, 0, limit)
	for _, event := range w.events {
		if event.TenantID != query.TenantID ||
			(query.Decision != "" && event.Decision != query.Decision) ||
			(query.TraceID != "" && event.TraceID != query.TraceID) {
			continue
		}
		result = append(result, event)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].OccurredAt.After(result[j].OccurredAt) })
	if len(result) > limit {
		result = result[:limit]
	}
	return result, nil
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
var _ Reader = (*MemoryWriter)(nil)
