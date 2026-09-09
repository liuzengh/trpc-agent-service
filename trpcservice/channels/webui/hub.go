package webui

import (
	"errors"
	"sync"

	"github.com/Violet2314/trpc-agent-service/trpcservice/reply"
)

var (
	errStreamNotFound  = errors.New("WebUI stream not found")
	errStreamForbidden = errors.New("WebUI stream owner mismatch")
)

const maxHistoryEvents = 512

// StreamHub is the cross-request event transport used by WebUI.
type StreamHub interface {
	Ensure(sessionID, owner string) error
	Subscribe(sessionID, owner string) (<-chan reply.Event, func(), error)
	Publish(sessionID, owner string, event reply.Event) error
}

type topic struct {
	owner       string
	history     []reply.Event
	subscribers map[uint64]chan reply.Event
	done        bool
}

// Hub fans Worker events out to browser SSE subscribers.
type Hub struct {
	mu     sync.Mutex
	nextID uint64
	topics map[string]*topic
}

// NewHub constructs an empty event hub.
func NewHub() *Hub {
	return &Hub{topics: make(map[string]*topic)}
}

// Ensure records session ownership before a browser opens its SSE stream.
func (h *Hub) Ensure(sessionID, owner string) error {
	if sessionID == "" || owner == "" {
		return errors.New("WebUI session and owner are required")
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if existing, ok := h.topics[sessionID]; ok {
		if existing.owner != owner {
			return errStreamForbidden
		}
		return nil
	}
	h.topics[sessionID] = &topic{
		owner:       owner,
		subscribers: make(map[uint64]chan reply.Event),
	}
	return nil
}

// Subscribe replays history and follows future events for the session owner.
func (h *Hub) Subscribe(sessionID, owner string) (<-chan reply.Event, func(), error) {
	h.mu.Lock()
	current, ok := h.topics[sessionID]
	if !ok {
		h.mu.Unlock()
		return nil, nil, errStreamNotFound
	}
	if current.owner != owner {
		h.mu.Unlock()
		return nil, nil, errStreamForbidden
	}
	buffer := len(current.history) + 128
	output := make(chan reply.Event, buffer)
	for _, event := range current.history {
		output <- event
	}
	if current.done {
		close(output)
		h.mu.Unlock()
		return output, func() {}, nil
	}
	h.nextID++
	id := h.nextID
	current.subscribers[id] = output
	h.mu.Unlock()

	var once sync.Once
	cancel := func() {
		once.Do(func() {
			h.mu.Lock()
			if active, exists := h.topics[sessionID]; exists {
				if subscriber, exists := active.subscribers[id]; exists {
					delete(active.subscribers, id)
					close(subscriber)
				}
			}
			h.mu.Unlock()
		})
	}
	return output, cancel, nil
}

// Publish appends history and sends without blocking Agent execution.
func (h *Hub) Publish(sessionID, owner string, event reply.Event) error {
	if err := h.Ensure(sessionID, owner); err != nil {
		return err
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	current := h.topics[sessionID]
	if current.done {
		return nil
	}
	current.history = append(current.history, event)
	if len(current.history) > maxHistoryEvents {
		current.history = append([]reply.Event(nil), current.history[len(current.history)-maxHistoryEvents:]...)
	}
	for id, subscriber := range current.subscribers {
		select {
		case subscriber <- event:
		default:
			delete(current.subscribers, id)
			close(subscriber)
		}
	}
	if event.Type == "done" {
		current.done = true
		for id, subscriber := range current.subscribers {
			delete(current.subscribers, id)
			close(subscriber)
		}
	}
	return nil
}
