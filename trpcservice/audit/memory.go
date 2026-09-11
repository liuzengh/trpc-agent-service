package audit

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"sort"
	"sync"
	"time"
)

type MemoryWriter struct {
	mu         sync.Mutex
	closed     bool
	events     []Event
	identities map[string][]byte
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
		appID, _ := event.Details["app_id"].(string)
		if query.AppID != "" && appID != query.AppID || query.RequestID != "" && event.RequestID != query.RequestID {
			continue
		}
		if query.ReleaseOnly && event.Decision != "admin_revision_published" && event.Decision != "admin_draft_published" && event.Decision != "admin_rollout_policy_updated" {
			continue
		}
		if !query.BeforeTime.IsZero() && (event.OccurredAt.After(query.BeforeTime) || event.OccurredAt.Equal(query.BeforeTime) && event.ID >= query.BeforeID) {
			continue
		}
		if event.TenantID != query.TenantID ||
			(query.Decision != "" && event.Decision != query.Decision) ||
			(query.TraceID != "" && event.TraceID != query.TraceID) {
			continue
		}
		event.Details = redactMap(event.Details)
		result = append(result, event)
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].OccurredAt.Equal(result[j].OccurredAt) {
			return result[i].ID > result[j].ID
		}
		return result[i].OccurredAt.After(result[j].OccurredAt)
	})
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
	event = sanitizeEvent(event)
	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}
	if w.identities == nil {
		w.identities = map[string][]byte{}
	}
	if old, exists := w.identities[event.ID]; exists {
		if !bytes.Equal(old, payload) {
			return ErrEventConflict
		}
		return nil
	}
	w.identities[event.ID] = payload
	w.events = append(w.events, event)
	return nil
}

func (w *MemoryWriter) Prune(ctx context.Context, tenantID string, before time.Time, limit int) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	count := 0
	kept := w.events[:0]
	for _, event := range w.events {
		if event.TenantID == tenantID && event.OccurredAt.Before(before) && count < limit {
			delete(w.identities, event.ID)
			count++
			continue
		}
		kept = append(kept, event)
	}
	w.events = kept
	return count, nil
}

func (w *MemoryWriter) Events() []Event {
	w.mu.Lock()
	defer w.mu.Unlock()
	result := append([]Event(nil), w.events...)
	for i := range result {
		result[i].Details = redactMap(result[i].Details)
	}
	return result
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
