package modelops

import (
	"context"
	"errors"
	"net"
	"net/http"
	"sync"

	"github.com/openai/openai-go/option"
	"trpc.group/trpc-go/trpc-agent-go/model"
)

// ErrUnavailable carries no provider URL, credentials or raw response body.
var ErrUnavailable = errors.New("model dependency temporarily unavailable")

type availabilityKey struct{}
type Availability struct {
	mu                    sync.Mutex
	unavailable, progress bool
	notSent               bool
	parent                *Availability
}

func ObserveAvailability(ctx context.Context) (context.Context, *Availability) {
	a := &Availability{}
	a.parent, _ = ctx.Value(availabilityKey{}).(*Availability)
	return context.WithValue(ctx, availabilityKey{}, a), a
}

func (a *Availability) DefinitelyNotSent() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.notSent && !a.progress
}
func (a *Availability) CanWait() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.unavailable && !a.progress
}

// AvailabilityMiddleware uses tRPC-Agent-Go's WithOpenAIOptions extension.
// HTTP requests, model execution and Events remain owned by the framework.
func AvailabilityMiddleware(req *http.Request, next option.MiddlewareNext) (*http.Response, error) {
	res, err := next(req)
	if a, ok := req.Context().Value(availabilityKey{}).(*Availability); ok {
		var network net.Error
		var operation *net.OpError
		notSent := err != nil && errors.As(err, &operation) && operation.Op == "dial"
		temporary := err != nil && !errors.Is(err, context.Canceled) && errors.As(err, &network)
		if res != nil {
			temporary = res.StatusCode == 408 || res.StatusCode == 429 || res.StatusCode >= 500
		}
		for current := a; current != nil; current = current.parent {
			current.mu.Lock()
			current.unavailable, current.notSent = temporary, notSent
			current.mu.Unlock()
		}
	}
	return res, err
}

// Framework callbacks rule out delayed whole-turn retries after model progress.
func AddAvailabilityCallbacks(callbacks *model.Callbacks) *model.Callbacks {
	if callbacks == nil {
		callbacks = model.NewCallbacks()
	}
	observe := func(ctx context.Context, args *model.AfterModelArgs) (*model.AfterModelResult, error) {
		if a, ok := ctx.Value(availabilityKey{}).(*Availability); ok && args != nil && args.Response != nil && len(args.Response.Choices) > 0 {
			a.mu.Lock()
			a.progress = true
			a.mu.Unlock()
		}
		return nil, nil
	}
	// Run before output guardrails, which may return a replacement response and
	// deliberately short-circuit the remaining framework callback chain.
	callbacks.AfterModel = append([]model.AfterModelCallbackStructured{observe}, callbacks.AfterModel...)
	return callbacks
}
