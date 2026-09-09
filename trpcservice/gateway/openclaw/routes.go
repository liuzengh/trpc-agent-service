package openclaw

import (
	"crypto/subtle"
	"errors"
	"sync"

	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

// Route is a trusted binding resolved from server-owned credentials.
type Route struct {
	TenantID, AppID, BindingID string
	ChannelType                tenant.ChannelType
	ConfigVersion              tenant.ConfigVersion
	Credential                 string
}

// Routes resolves a credential to one tenant binding.
type Routes interface {
	Resolve(string, string) (Route, error)
}

// StaticRoutes is an immutable local registry suitable for tests and Compose.
type StaticRoutes struct{ routes map[string][]Route }

// NewStaticRoutes validates and copies routes.
func NewStaticRoutes(routes ...Route) (*StaticRoutes, error) {
	registry := &StaticRoutes{routes: make(map[string][]Route, len(routes))}
	for _, route := range routes {
		if route.TenantID == "" || route.AppID == "" || route.BindingID == "" || route.ChannelType == "" || route.ConfigVersion == 0 || route.Credential == "" {
			return nil, errors.New("openclaw: route scope, version, channel, and credential are required")
		}
		for _, existing := range registry.routes[route.BindingID] {
			if len(existing.Credential) == len(route.Credential) && subtle.ConstantTimeCompare([]byte(existing.Credential), []byte(route.Credential)) == 1 {
				return nil, errors.New("openclaw: ambiguous binding credential")
			}
		}
		registry.routes[route.BindingID] = append(registry.routes[route.BindingID], route)
	}
	return registry, nil
}

// Resolve performs a constant-time credential comparison.
func (routes *StaticRoutes) Resolve(bindingID, credential string) (Route, error) {
	for _, route := range routes.routes[bindingID] {
		if len(credential) == len(route.Credential) && subtle.ConstantTimeCompare([]byte(credential), []byte(route.Credential)) == 1 {
			return route, nil
		}
	}
	return Route{}, errors.New("openclaw: invalid binding credential")
}

// Hub fans execution events out to an optional streaming HTTP client.
type Hub struct {
	mu   sync.Mutex
	subs map[string]map[chan StreamEvent]struct{}
}

// EventBus supports local or distributed request-scoped event delivery.
type EventBus interface {
	gateway.EventPublisher
	Subscribe(string) (<-chan StreamEvent, func())
	Unsubscribe(string, <-chan StreamEvent)
}

func NewHub() *Hub { return &Hub{subs: make(map[string]map[chan StreamEvent]struct{})} }

func (hub *Hub) Subscribe(requestID string) (<-chan StreamEvent, func()) {
	stream := make(chan StreamEvent, 32)
	hub.mu.Lock()
	if hub.subs[requestID] == nil {
		hub.subs[requestID] = make(map[chan StreamEvent]struct{})
	}
	hub.subs[requestID][stream] = struct{}{}
	hub.mu.Unlock()
	var once sync.Once
	return stream, func() {
		once.Do(func() {
			hub.mu.Lock()
			delete(hub.subs[requestID], stream)
			if len(hub.subs[requestID]) == 0 {
				delete(hub.subs, requestID)
			}
			hub.mu.Unlock()
		})
	}
}

// Unsubscribe removes a stream after disconnect or terminal delivery.
func (hub *Hub) Unsubscribe(requestID string, target <-chan StreamEvent) {
	if hub == nil || target == nil {
		return
	}
	hub.mu.Lock()
	defer hub.mu.Unlock()
	for stream := range hub.subs[requestID] {
		if (<-chan StreamEvent)(stream) == target {
			delete(hub.subs[requestID], stream)
		}
	}
	if len(hub.subs[requestID]) == 0 {
		delete(hub.subs, requestID)
	}
}

// Publish implements gateway.EventPublisher. Slow clients cannot block Workers.
func (hub *Hub) Publish(event gateway.RunEvent) {
	projected := StreamEvent{Type: event.Type, RequestID: event.RequestID, SessionID: event.SessionID, TraceID: event.TraceID, Delta: event.Delta, Reply: event.Message, Stage: event.Stage, ToolName: event.ToolName, ToolCallID: event.ToolCallID, ToolStatus: event.ToolStatus, Error: event.Error, Terminal: event.Terminal}
	if event.TotalTokens != 0 {
		projected.Usage = &Usage{PromptTokens: event.PromptTokens, CompletionTokens: event.CompletionTokens, TotalTokens: event.TotalTokens}
	}
	hub.mu.Lock()
	defer hub.mu.Unlock()
	for stream := range hub.subs[event.RequestID] {
		select {
		case stream <- projected:
		default:
			if event.Terminal {
				select {
				case <-stream:
				default:
				}
				select {
				case stream <- projected:
				default:
				}
			}
		}
		if event.Terminal {
			delete(hub.subs[event.RequestID], stream)
		}
	}
	if event.Terminal && len(hub.subs[event.RequestID]) == 0 {
		delete(hub.subs, event.RequestID)
	}
}
