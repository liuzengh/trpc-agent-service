package storage

import (
	"context"
	"fmt"

	"trpc.group/trpc-go/trpc-agent-go/session"
)

// Sessions wraps a session service with tenant-scoped access, mapping the
// tenant id to the framework's AppName isolation boundary.
type Sessions struct {
	svc session.Service
}

// NewSessions builds a session service for the given backend (see
// backends.go for the backend table).
func NewSessions(cfg SessionConfig) (*Sessions, error) {
	b, ok := backendTable[cfg.Backend]
	if !ok || b.session == nil {
		return nil, fmt.Errorf("storage: unknown session backend %q (supported: %v)", cfg.Backend, SupportedBackends())
	}
	svc, err := b.session(cfg)
	if err != nil {
		return nil, err
	}
	return &Sessions{svc: svc}, nil
}

// Create creates a session with the given initial state for the tenant,
// user, and session ids. The tenant id is the isolation boundary.
func (s *Sessions) Create(ctx context.Context, tenantID, userID, sessionID string, state session.StateMap) (*session.Session, error) {
	key := session.Key{AppName: tenantID, UserID: userID, SessionID: sessionID}
	return s.svc.CreateSession(ctx, key, state)
}

// Get returns the session for the given ids. A missing session is lazily
// created by the backend, so callers compare State/Events for isolation.
func (s *Sessions) Get(ctx context.Context, tenantID, userID, sessionID string) (*session.Session, error) {
	key := session.Key{AppName: tenantID, UserID: userID, SessionID: sessionID}
	return s.svc.GetSession(ctx, key)
}

// Delete removes the session for the given ids.
func (s *Sessions) Delete(ctx context.Context, tenantID, userID, sessionID string) error {
	key := session.Key{AppName: tenantID, UserID: userID, SessionID: sessionID}
	return s.svc.DeleteSession(ctx, key)
}

// Service exposes the underlying framework session service, e.g. for wiring
// into runner.Runner (WithSessionService).
func (s *Sessions) Service() session.Service {
	return s.svc
}
