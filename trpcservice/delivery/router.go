package delivery

import (
	"context"
	"errors"
	"fmt"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
)

// Route binds one immutable tenant channel to its outbound sender.
type Route struct {
	Binding BindingKey
	Sender  channels.TextSender
}

// RouteResolver supplies the currently claimable bindings and resolves the
// sender pinned by an Outbox message. Production uses a control-plane-backed
// resolver; Router remains the immutable fixture used by tests.
type RouteResolver interface {
	Keys() []BindingKey
	Resolve(gateway.OutboundMessage) (channels.TextSender, error)
}

// ContextRouteResolver is implemented by production route tables whose
// control-plane lookup must be cancellable during shutdown or DB incidents.
type ContextRouteResolver interface {
	ResolveContext(context.Context, gateway.OutboundMessage) (channels.TextSender, error)
}

// Router resolves only explicitly registered tenant bindings.
type Router struct {
	senders map[BindingKey]channels.TextSender
	keys    []BindingKey
}

// NewRouter constructs an immutable delivery route table.
func NewRouter(routes ...Route) (*Router, error) {
	if len(routes) == 0 {
		return nil, errors.New("delivery: at least one sender route is required")
	}
	router := &Router{senders: make(map[BindingKey]channels.TextSender, len(routes)), keys: make([]BindingKey, 0, len(routes))}
	for _, route := range routes {
		if route.Binding.TenantID == "" || route.Binding.BindingID == "" || route.Sender == nil {
			return nil, errors.New("delivery: complete route binding and sender are required")
		}
		if _, exists := router.senders[route.Binding]; exists {
			return nil, fmt.Errorf("delivery: duplicate route %q/%q", route.Binding.TenantID, route.Binding.BindingID)
		}
		router.senders[route.Binding] = route.Sender
		router.keys = append(router.keys, route.Binding)
	}
	return router, nil
}

// Keys returns a copy of the bindings this process is allowed to claim.
func (router *Router) Keys() []BindingKey {
	if router == nil {
		return nil
	}
	return append([]BindingKey(nil), router.keys...)
}

// Resolve selects a sender without trusting payload-owned tenant scope.
func (router *Router) Resolve(message gateway.OutboundMessage) (channels.TextSender, error) {
	return router.ResolveContext(context.Background(), message)
}

// ResolveContext selects a sender while honoring the caller's deadline.
func (router *Router) ResolveContext(ctx context.Context, message gateway.OutboundMessage) (channels.TextSender, error) {
	if router == nil {
		return nil, errors.New("delivery: nil router")
	}
	if ctx == nil {
		return nil, errors.New("delivery: nil context")
	}
	sender := router.senders[BindingKey{TenantID: message.TenantID, BindingID: message.BindingID}]
	if sender == nil {
		return nil, fmt.Errorf("delivery: no sender for tenant binding %q/%q", message.TenantID, message.BindingID)
	}
	return sender, nil
}

var _ RouteResolver = (*Router)(nil)
var _ ContextRouteResolver = (*Router)(nil)
