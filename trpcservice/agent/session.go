package agent

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"

	"github.com/XnLemon/trpc-agent-service/trpcservice/tenant"
	trpcevent "trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/session"
)

var (
	// ErrTenantSessionScope identifies a session operation outside its fixed Tenant.
	ErrTenantSessionScope = errors.New("tenant session scope violation")
)

// TenantSessionService binds every upstream Session operation to one validated
// active Tenant. The delegate is used as a capability and never receives an
// unscoped app or user key.
type TenantSessionService struct {
	prefix   string
	delegate session.Service
}

// NewTenantSessionService creates a Tenant-scoped adapter around delegate.
// The adapter borrows delegate; the caller owns its lifecycle.
func NewTenantSessionService(root tenant.Tenant, delegate session.Service) (*TenantSessionService, error) {
	if err := root.Validate(); err != nil || !root.CanAcceptExecution() {
		return nil, fmt.Errorf("%w: tenant must be a valid active root", ErrTenantSessionScope)
	}
	if delegate == nil {
		return nil, fmt.Errorf("%w: session service is required", ErrTenantSessionScope)
	}
	prefix := "tenant:" + base64.RawURLEncoding.EncodeToString([]byte(root.TenantID)) + ":"
	return &TenantSessionService{prefix: prefix, delegate: delegate}, nil
}

var _ session.Service = (*TenantSessionService)(nil)

// CreateSession creates a session in the fixed Tenant namespace.
func (service *TenantSessionService) CreateSession(ctx context.Context, key session.Key, state session.StateMap, options ...session.Option) (*session.Session, error) {
	scoped, err := service.scopeKey(key, false)
	if err != nil {
		return nil, err
	}
	return service.delegate.CreateSession(ctx, scoped, state, options...)
}

// GetSession reads a session in the fixed Tenant namespace.
func (service *TenantSessionService) GetSession(ctx context.Context, key session.Key, options ...session.Option) (*session.Session, error) {
	scoped, err := service.scopeKey(key, true)
	if err != nil {
		return nil, err
	}
	return service.delegate.GetSession(ctx, scoped, options...)
}

// ListSessions lists sessions for one raw user ID in the fixed Tenant namespace.
func (service *TenantSessionService) ListSessions(ctx context.Context, userKey session.UserKey, options ...session.Option) ([]*session.Session, error) {
	scoped, err := service.scopeUserKey(userKey)
	if err != nil {
		return nil, err
	}
	return service.delegate.ListSessions(ctx, scoped, options...)
}

// DeleteSession deletes a session in the fixed Tenant namespace.
func (service *TenantSessionService) DeleteSession(ctx context.Context, key session.Key, options ...session.Option) error {
	scoped, err := service.scopeKey(key, true)
	if err != nil {
		return err
	}
	return service.delegate.DeleteSession(ctx, scoped, options...)
}

// UpdateAppState updates state for an app in the fixed Tenant namespace.
func (service *TenantSessionService) UpdateAppState(ctx context.Context, appName string, state session.StateMap) error {
	scoped, err := service.scopeIdentifier(appName)
	if err != nil {
		return err
	}
	return service.delegate.UpdateAppState(ctx, scoped, state)
}

// DeleteAppState deletes state for an app in the fixed Tenant namespace.
func (service *TenantSessionService) DeleteAppState(ctx context.Context, appName string, key string) error {
	scoped, err := service.scopeIdentifier(appName)
	if err != nil {
		return err
	}
	return service.delegate.DeleteAppState(ctx, scoped, key)
}

// ListAppStates lists state for an app in the fixed Tenant namespace.
func (service *TenantSessionService) ListAppStates(ctx context.Context, appName string) (session.StateMap, error) {
	scoped, err := service.scopeIdentifier(appName)
	if err != nil {
		return nil, err
	}
	return service.delegate.ListAppStates(ctx, scoped)
}

// UpdateUserState updates state for a user in the fixed Tenant namespace.
func (service *TenantSessionService) UpdateUserState(ctx context.Context, userKey session.UserKey, state session.StateMap) error {
	scoped, err := service.scopeUserKey(userKey)
	if err != nil {
		return err
	}
	return service.delegate.UpdateUserState(ctx, scoped, state)
}

// ListUserStates lists state for a user in the fixed Tenant namespace.
func (service *TenantSessionService) ListUserStates(ctx context.Context, userKey session.UserKey) (session.StateMap, error) {
	scoped, err := service.scopeUserKey(userKey)
	if err != nil {
		return nil, err
	}
	return service.delegate.ListUserStates(ctx, scoped)
}

// DeleteUserState deletes state for a user in the fixed Tenant namespace.
func (service *TenantSessionService) DeleteUserState(ctx context.Context, userKey session.UserKey, key string) error {
	scoped, err := service.scopeUserKey(userKey)
	if err != nil {
		return err
	}
	return service.delegate.DeleteUserState(ctx, scoped, key)
}

// UpdateSessionState updates state for a session in the fixed Tenant namespace.
func (service *TenantSessionService) UpdateSessionState(ctx context.Context, key session.Key, state session.StateMap) error {
	scoped, err := service.scopeKey(key, true)
	if err != nil {
		return err
	}
	return service.delegate.UpdateSessionState(ctx, scoped, state)
}

// AppendEvent appends an event only to a Session returned by this adapter.
func (service *TenantSessionService) AppendEvent(ctx context.Context, sess *session.Session, evt *trpcevent.Event, options ...session.Option) error {
	if err := service.validateSession(sess); err != nil {
		return err
	}
	return service.delegate.AppendEvent(ctx, sess, evt, options...)
}

// CreateSessionSummary delegates summary creation for a scoped Session.
func (service *TenantSessionService) CreateSessionSummary(ctx context.Context, sess *session.Session, filterKey string, force bool) error {
	if err := service.validateSession(sess); err != nil {
		return err
	}
	return service.delegate.CreateSessionSummary(ctx, sess, filterKey, force)
}

// EnqueueSummaryJob delegates summary enqueueing for a scoped Session.
func (service *TenantSessionService) EnqueueSummaryJob(ctx context.Context, sess *session.Session, filterKey string, force bool) error {
	if err := service.validateSession(sess); err != nil {
		return err
	}
	return service.delegate.EnqueueSummaryJob(ctx, sess, filterKey, force)
}

// GetSessionSummaryText reads a summary for a scoped Session.
func (service *TenantSessionService) GetSessionSummaryText(ctx context.Context, sess *session.Session, options ...session.SummaryOption) (string, bool) {
	if service.validateSession(sess) != nil {
		return "", false
	}
	return service.delegate.GetSessionSummaryText(ctx, sess, options...)
}

// Close closes the borrowed delegate when the caller explicitly closes the adapter.
func (service *TenantSessionService) Close() error { return service.delegate.Close() }

func (service *TenantSessionService) scopeKey(key session.Key, requireSessionID bool) (session.Key, error) {
	if err := key.CheckUserKey(); err != nil {
		return session.Key{}, err
	}
	if requireSessionID && key.SessionID == "" {
		return session.Key{}, session.ErrSessionIDRequired
	}
	if service.isScoped(key.AppName) || service.isScoped(key.UserID) {
		if service.isScoped(key.AppName) && service.isScoped(key.UserID) {
			return key, nil
		}
		return session.Key{}, ErrTenantSessionScope
	}
	if hasTenantScopePrefix(key.AppName) || hasTenantScopePrefix(key.UserID) {
		return session.Key{}, ErrTenantSessionScope
	}
	return session.Key{AppName: service.scopeIdentifierUnchecked(key.AppName), UserID: service.scopeIdentifierUnchecked(key.UserID), SessionID: key.SessionID}, nil
}

func (service *TenantSessionService) scopeUserKey(key session.UserKey) (session.UserKey, error) {
	if err := key.CheckUserKey(); err != nil {
		return session.UserKey{}, err
	}
	if service.isScoped(key.AppName) || service.isScoped(key.UserID) {
		if service.isScoped(key.AppName) && service.isScoped(key.UserID) {
			return key, nil
		}
		return session.UserKey{}, ErrTenantSessionScope
	}
	if hasTenantScopePrefix(key.AppName) || hasTenantScopePrefix(key.UserID) {
		return session.UserKey{}, ErrTenantSessionScope
	}
	return session.UserKey{AppName: service.scopeIdentifierUnchecked(key.AppName), UserID: service.scopeIdentifierUnchecked(key.UserID)}, nil
}

func (service *TenantSessionService) scopeIdentifier(value string) (string, error) {
	if value == "" {
		return "", session.ErrAppNameRequired
	}
	if service.isScoped(value) {
		return value, nil
	}
	if hasTenantScopePrefix(value) {
		return "", ErrTenantSessionScope
	}
	return service.scopeIdentifierUnchecked(value), nil
}

func (service *TenantSessionService) scopeIdentifierUnchecked(value string) string {
	return service.prefix + base64.RawURLEncoding.EncodeToString([]byte(value))
}

func (service *TenantSessionService) isScoped(value string) bool {
	return strings.HasPrefix(value, service.prefix)
}

func hasTenantScopePrefix(value string) bool {
	return strings.HasPrefix(value, "tenant:")
}

func (service *TenantSessionService) validateSession(sess *session.Session) error {
	if sess == nil || !service.isScoped(sess.AppName) || !service.isScoped(sess.UserID) || sess.ID == "" {
		return ErrTenantSessionScope
	}
	return nil
}
