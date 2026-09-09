// Tencent is pleased to support the open source community by making
// trpc-agent-go available.
//
// Copyright (C) 2025 Tencent. All rights reserved.
//
// trpc-agent-go is licensed under the Apache License Version 2.0.

// Package fence contains the small, shared contract used to carry an
// execution authority from the control plane into Session and Memory writes.
// Keeping this contract independent from either package avoids an import cycle
// and makes unsupported backends explicit at composition time.
package fence

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
)

var (
	// ErrTokenRequired means a fenced service was called without an execution
	// authority. Production services fail closed on this condition.
	ErrTokenRequired = errors.New("execution fence token is required")
	// ErrMismatch means the authority no longer owns the session generation.
	ErrMismatch = errors.New("execution fence token is not current")
	// ErrInvalidToken means a caller supplied an incomplete or malformed token.
	ErrInvalidToken = errors.New("execution fence token is invalid")
	// ErrScopeMismatch means a fenced operation attempted to address a
	// tenant/app/user/session outside the admitted execution scope.
	ErrScopeMismatch = errors.New("execution fence operation is outside token scope")
	// ErrLeaseLost means the execution authority could not be renewed or was
	// no longer current at the end of an operation. Callers must not publish a
	// successful result after observing it.
	ErrLeaseLost = errors.New("execution fence lease was lost")
)

// Token identifies one admitted execution attempt for one logical session.
// ExecutionID and Generation are both checked: the generation prevents an old
// attempt from becoming valid again after reconciliation, while the ID/token
// pair proves the exact attempt currently owns the guard.
type Token struct {
	TenantID   string
	AgentAppID string
	// AgentAppName is the immutable logical app name selected by the control
	// plane. AgentAppID remains the database identity used by the fence guard.
	AgentAppName string
	// ScopedAppName is the canonical tenant-scoped app name used as the key in
	// Session and Memory backends. Production fenced services require this
	// field and compare it with every caller-supplied key; keeping the logical
	// name above preserves useful control-plane identity in audit/debug data.
	ScopedAppName string
	// UserID is the admitted provider identity used by Session and audit.
	UserID string
	// MemoryUserID is the provider-scoped storage actor derived at the trusted
	// Worker boundary. It does not change Session identity or provider routing.
	// Empty is retained for explicitly composed in-process adapters; production
	// Worker processing always installs a separate Memory identity.
	MemoryUserID string
	// SessionOwnerID binds Session operations to the Runner user key. It is
	// distinct from UserID for group conversations, where all actors share one
	// Session while Memory remains scoped to the individual actor. An empty
	// value is accepted for rolling upgrades and falls back to UserID.
	SessionOwnerID string
	SessionID      string
	ExecutionID    int64
	Generation     int64
	Value          string
}

// Validate checks the fields that are independent of a particular backend.
func (t Token) Validate() error {
	if strings.TrimSpace(t.TenantID) == "" ||
		strings.TrimSpace(t.AgentAppID) == "" ||
		strings.TrimSpace(t.SessionID) == "" ||
		t.ExecutionID <= 0 || t.Generation <= 0 ||
		strings.TrimSpace(t.Value) == "" ||
		strings.ContainsAny(t.Value, "\x00\r\n") {
		return ErrInvalidToken
	}
	for _, value := range []string{t.AgentAppName, t.ScopedAppName, t.UserID, t.MemoryUserID, t.SessionOwnerID} {
		if strings.ContainsAny(value, "\x00\r\n") {
			return ErrInvalidToken
		}
	}
	return nil
}

// Scope returns the canonical lock scope. Length prefixes make the encoding
// injective even when identifiers contain the separator byte.
func (t Token) Scope() string {
	return ScopeFor(t.TenantID, t.AgentAppID, t.SessionID)
}

// ScopeFor is the canonical lock scope used by both admission and service
// operations. It is exported so the control plane can lock before it creates
// an execution token without manufacturing a partially valid Token value.
func ScopeFor(tenantID, agentAppID, sessionID string) string {
	return fmt.Sprintf("%d:%s|%d:%s|%d:%s",
		len(tenantID), tenantID,
		len(agentAppID), agentAppID,
		len(sessionID), sessionID)
}

// Authorizer acquires a backend-native fence for the duration of one service
// operation. The release function must be called exactly once; its error is
// part of the operation result because leaking a lock is an availability bug.
type Authorizer interface {
	Acquire(context.Context, Token) (release func() error, err error)
}

// State carries errors from upstream interfaces that cannot return an error
// themselves (notably SessionService.GetSessionSummaryText). The Runner keeps
// the context value across its internal derived contexts; the Worker checks
// the state before publishing a response.
type State struct {
	mu  sync.Mutex
	err error
}

type stateContextKey struct{}

// WithState installs a mutable fence state in ctx.
func WithState(ctx context.Context) (context.Context, *State) {
	if ctx == nil {
		ctx = context.Background()
	}
	state := &State{}
	return context.WithValue(ctx, stateContextKey{}, state), state
}

// RecordError records the first fence error in the context state.
func RecordError(ctx context.Context, err error) {
	if err == nil || ctx == nil {
		return
	}
	state, _ := ctx.Value(stateContextKey{}).(*State)
	if state == nil {
		return
	}
	state.mu.Lock()
	if state.err == nil {
		state.err = err
	}
	state.mu.Unlock()
}

// Error returns the first recorded fence error, if any.
func (s *State) Error() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.err
}

type tokenContextKey struct{}

// WithToken attaches an execution authority to a context. Context values are
// preserved by context.WithoutCancel, which lets queued memory jobs retain the
// authority while still being rejected after it becomes stale.
func WithToken(ctx context.Context, token Token) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, tokenContextKey{}, token)
}

// TokenFromContext returns the execution authority attached to ctx.
func TokenFromContext(ctx context.Context) (Token, error) {
	if ctx == nil {
		return Token{}, ErrTokenRequired
	}
	token, ok := ctx.Value(tokenContextKey{}).(Token)
	if !ok {
		return Token{}, ErrTokenRequired
	}
	if err := token.Validate(); err != nil {
		return Token{}, err
	}
	return token, nil
}
