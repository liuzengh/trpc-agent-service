package sessioncoord

import (
	"context"
	"errors"
	"fmt"

	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/session"
)

type leaseContextKey struct{}

// WithLease binds trusted lease ownership to all Runner persistence calls.
func WithLease(ctx context.Context, lease Lease) context.Context {
	return context.WithValue(ctx, leaseContextKey{}, lease)
}

// FenceValidator verifies exact session ownership.
type FenceValidator interface {
	ValidateFence(context.Context, gateway.SessionKey, uint64) error
	WithFence(context.Context, gateway.SessionKey, uint64, func(context.Context) error) error
}

// FencedSessionService rejects every mutating upstream Session operation from stale Workers.
type FencedSessionService struct {
	delegate  session.Service
	validator FenceValidator
}

// NewFencedSessionService wraps a public tRPC-Agent-Go Session service.
func NewFencedSessionService(delegate session.Service, validator FenceValidator) (*FencedSessionService, error) {
	if delegate == nil || validator == nil {
		return nil, errors.New("sessioncoord: session service and validator are required")
	}
	return &FencedSessionService{delegate: delegate, validator: validator}, nil
}

func (service *FencedSessionService) leaseFor(ctx context.Context, appName, userID, sessionID string) (Lease, error) {
	lease, ok := ctx.Value(leaseContextKey{}).(Lease)
	if !ok {
		return Lease{}, errors.New("sessioncoord: lease missing from context")
	}
	wantApp, err := tenant.CanonicalAppName(lease.Key.TenantID, lease.Key.AppID)
	if err != nil {
		return Lease{}, err
	}
	if appName != wantApp || (userID != "" && userID != lease.Key.UserID) || (sessionID != "" && sessionID != lease.Key.SessionID) {
		return Lease{}, fmt.Errorf("sessioncoord: write scope does not match lease")
	}
	return lease, nil
}

func (service *FencedSessionService) mutate(ctx context.Context, appName, userID, sessionID string, operation func(context.Context) error) error {
	lease, err := service.leaseFor(ctx, appName, userID, sessionID)
	if err != nil {
		return err
	}
	return service.validator.WithFence(ctx, lease.Key, lease.Token, operation)
}

func (service *FencedSessionService) CreateSession(ctx context.Context, key session.Key, state session.StateMap, opts ...session.Option) (*session.Session, error) {
	var created *session.Session
	err := service.mutate(ctx, key.AppName, key.UserID, key.SessionID, func(fencedCtx context.Context) error {
		var err error
		created, err = service.delegate.CreateSession(fencedCtx, key, state, opts...)
		return err
	})
	return created, err
}
func (service *FencedSessionService) GetSession(ctx context.Context, key session.Key, opts ...session.Option) (*session.Session, error) {
	return service.delegate.GetSession(ctx, key, opts...)
}
func (service *FencedSessionService) ListSessions(ctx context.Context, key session.UserKey, opts ...session.Option) ([]*session.Session, error) {
	return service.delegate.ListSessions(ctx, key, opts...)
}
func (service *FencedSessionService) DeleteSession(ctx context.Context, key session.Key, opts ...session.Option) error {
	return service.mutate(ctx, key.AppName, key.UserID, key.SessionID, func(fencedCtx context.Context) error {
		return service.delegate.DeleteSession(fencedCtx, key, opts...)
	})
}
func (service *FencedSessionService) UpdateAppState(context.Context, string, session.StateMap) error {
	return errors.New("sessioncoord: app state writes require an app-scoped lease")
}
func (service *FencedSessionService) DeleteAppState(context.Context, string, string) error {
	return errors.New("sessioncoord: app state writes require an app-scoped lease")
}
func (service *FencedSessionService) ListAppStates(ctx context.Context, appName string) (session.StateMap, error) {
	return service.delegate.ListAppStates(ctx, appName)
}
func (service *FencedSessionService) UpdateUserState(context.Context, session.UserKey, session.StateMap) error {
	return errors.New("sessioncoord: user state writes require a user-scoped lease")
}
func (service *FencedSessionService) ListUserStates(ctx context.Context, key session.UserKey) (session.StateMap, error) {
	return service.delegate.ListUserStates(ctx, key)
}
func (service *FencedSessionService) DeleteUserState(context.Context, session.UserKey, string) error {
	return errors.New("sessioncoord: user state writes require a user-scoped lease")
}
func (service *FencedSessionService) UpdateSessionState(ctx context.Context, key session.Key, state session.StateMap) error {
	return service.mutate(ctx, key.AppName, key.UserID, key.SessionID, func(fencedCtx context.Context) error {
		return service.delegate.UpdateSessionState(fencedCtx, key, state)
	})
}
func (service *FencedSessionService) AppendEvent(ctx context.Context, sess *session.Session, evt *event.Event, opts ...session.Option) error {
	if sess == nil {
		return errors.New("sessioncoord: nil session")
	}
	return service.mutate(ctx, sess.AppName, sess.UserID, sess.ID, func(fencedCtx context.Context) error {
		return service.delegate.AppendEvent(fencedCtx, sess, evt, opts...)
	})
}
func (service *FencedSessionService) CreateSessionSummary(ctx context.Context, sess *session.Session, filter string, force bool) error {
	if sess == nil {
		return errors.New("sessioncoord: nil session")
	}
	return service.mutate(ctx, sess.AppName, sess.UserID, sess.ID, func(fencedCtx context.Context) error {
		return service.delegate.CreateSessionSummary(fencedCtx, sess, filter, force)
	})
}
func (service *FencedSessionService) EnqueueSummaryJob(ctx context.Context, sess *session.Session, filter string, force bool) error {
	if sess == nil {
		return errors.New("sessioncoord: nil session")
	}
	return service.mutate(ctx, sess.AppName, sess.UserID, sess.ID, func(fencedCtx context.Context) error {
		return service.delegate.EnqueueSummaryJob(fencedCtx, sess, filter, force)
	})
}
func (service *FencedSessionService) GetSessionSummaryText(ctx context.Context, sess *session.Session, opts ...session.SummaryOption) (string, bool) {
	return service.delegate.GetSessionSummaryText(ctx, sess, opts...)
}
func (service *FencedSessionService) Close() error { return service.delegate.Close() }
