package storage

import (
	"context"
	"errors"
	"sync"
)

// MemoryEventStore is an in-process EventStore for live-path tests.
type MemoryEventStore struct {
	mu     sync.Mutex
	nextID int64
	events []UserEvent
}

// NewMemoryEventStore constructs an empty durable event log.
func NewMemoryEventStore() *MemoryEventStore {
	return &MemoryEventStore{}
}

// AppendUserEvent implements EventStore.
func (s *MemoryEventStore) AppendUserEvent(_ context.Context, event UserEvent) error {
	if event.SessionID == "" || event.TenantID == "" || event.AppID == "" ||
		event.Channel == "" || event.MsgID == "" || event.SenderID == "" {
		return errors.New("user event session, tenant, app, channel, message, and sender IDs are required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, existing := range s.events {
		if existing.TenantID == event.TenantID && existing.Channel == event.Channel && existing.MsgID == event.MsgID {
			return ErrDuplicateEvent
		}
	}
	s.nextID++
	event.ID = s.nextID
	s.events = append(s.events, event)
	return nil
}

// PendingUserEvents implements EventStore.
func (s *MemoryEventStore) PendingUserEvents(_ context.Context, sessionID string, afterID int64) ([]UserEvent, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var result []UserEvent
	for _, event := range s.events {
		if event.SessionID == sessionID && event.ID > afterID && !event.Consumed {
			result = append(result, event)
		}
	}
	return result, nil
}

// MarkConsumed implements EventStore.
func (s *MemoryEventStore) MarkConsumed(_ context.Context, ids []int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	marked := make(map[int64]struct{}, len(ids))
	for _, id := range ids {
		marked[id] = struct{}{}
	}
	for index := range s.events {
		if _, ok := marked[s.events[index].ID]; ok {
			s.events[index].Consumed = true
		}
	}
	return nil
}
