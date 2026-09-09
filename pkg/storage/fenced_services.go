package storage

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"

	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/fence"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/telemetry"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/tenant"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/memory"
	"trpc.group/trpc-go/trpc-agent-go/session"
	"trpc.group/trpc-go/trpc-agent-go/tool"
)

// FencedSessionService serializes every operation through a backend-native
// execution fence. The upstream tRPC interface has no generation parameter;
// this adapter is the seam that prevents an old Runner from silently writing
// after its admission generation has been replaced.
type FencedSessionService struct {
	inner      session.Service
	authorizer fence.Authorizer
	scope      fencedScope
}

func NewFencedSessionService(inner session.Service, authorizer fence.Authorizer) *FencedSessionService {
	return &FencedSessionService{inner: inner, authorizer: authorizer}
}

// NewStrictFencedSessionService creates the production form of the wrapper.
// It binds the service to one tenant and requires every backend key to match
// the canonical app/user/session scope carried by the execution token.
func NewStrictFencedSessionService(inner session.Service, authorizer fence.Authorizer, tenantID string) (*FencedSessionService, error) {
	if isNilStorageService(inner) {
		return nil, errors.New("strict fenced session service requires an inner service")
	}
	if isNilStorageService(authorizer) {
		return nil, fmt.Errorf("strict fenced session service requires an authorizer")
	}
	scope, err := newFencedScope(tenantID)
	if err != nil {
		return nil, err
	}
	return &FencedSessionService{inner: inner, authorizer: authorizer, scope: scope}, nil
}

func (s *FencedSessionService) runChecked(ctx context.Context, operation telemetry.Operation, check func(fence.Token) error, fn func(context.Context) error) (err error) {
	ctx, span := telemetry.StartOperation(ctx, operation)
	defer func() {
		if recovered := recover(); recovered != nil {
			telemetry.EndOperation(span, errors.New("fenced session operation panicked"))
			panic(recovered)
		}
		telemetry.EndOperation(span, err)
	}()
	if s == nil || isNilStorageService(s.inner) {
		return errors.New("fenced session service is not configured")
	}
	release, err := acquireScopedFence(ctx, s.authorizer, s.scope, check)
	if err != nil {
		return err
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			_ = release()
			panic(recovered)
		}
		err = errors.Join(err, release())
	}()
	return fn(ctx)
}

func (s *FencedSessionService) valueChecked(ctx context.Context, operation telemetry.Operation, check func(fence.Token) error, fn func(context.Context) (*session.Session, error)) (value *session.Session, err error) {
	ctx, span := telemetry.StartOperation(ctx, operation)
	defer func() {
		if recovered := recover(); recovered != nil {
			telemetry.EndOperation(span, errors.New("fenced session operation panicked"))
			panic(recovered)
		}
		telemetry.EndOperation(span, err)
	}()
	if s == nil || isNilStorageService(s.inner) {
		return nil, errors.New("fenced session service is not configured")
	}
	release, err := acquireScopedFence(ctx, s.authorizer, s.scope, check)
	if err != nil {
		return nil, err
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			_ = release()
			panic(recovered)
		}
		err = errors.Join(err, release())
	}()
	return fn(ctx)
}

func (s *FencedSessionService) listChecked(ctx context.Context, operation telemetry.Operation, check func(fence.Token) error, fn func(context.Context) ([]*session.Session, error)) (value []*session.Session, err error) {
	ctx, span := telemetry.StartOperation(ctx, operation)
	defer func() {
		if recovered := recover(); recovered != nil {
			telemetry.EndOperation(span, errors.New("fenced session operation panicked"))
			panic(recovered)
		}
		telemetry.EndOperation(span, err)
	}()
	if s == nil || isNilStorageService(s.inner) {
		return nil, errors.New("fenced session service is not configured")
	}
	release, err := acquireScopedFence(ctx, s.authorizer, s.scope, check)
	if err != nil {
		return nil, err
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			_ = release()
			panic(recovered)
		}
		err = errors.Join(err, release())
	}()
	return fn(ctx)
}

func (s *FencedSessionService) CreateSession(ctx context.Context, key session.Key, state session.StateMap, opts ...session.Option) (*session.Session, error) {
	return s.valueChecked(ctx, telemetry.OperationSessionWrite, func(token fence.Token) error {
		return s.scope.validateSessionKey(token, key)
	}, func(ctx context.Context) (*session.Session, error) {
		if s.scope.strict && protectedSessionState(state) != nil {
			return nil, ErrSessionIncarnation
		}
		return s.createSessionWithIncarnation(ctx, key, state, opts...)
	})
}

func (s *FencedSessionService) GetSession(ctx context.Context, key session.Key, opts ...session.Option) (*session.Session, error) {
	return s.valueChecked(ctx, telemetry.OperationSessionRead, func(token fence.Token) error {
		return s.scope.validateSessionKey(token, key)
	}, func(ctx context.Context) (*session.Session, error) {
		value, err := s.inner.GetSession(ctx, key, opts...)
		if err != nil {
			return nil, err
		}
		if value == nil {
			return nil, nil
		}
		if err := s.validateReturnedSession(ctx, value); err != nil {
			return nil, err
		}
		return s.bindSessionIncarnation(ctx, key, value)
	})
}

func (s *FencedSessionService) ListSessions(ctx context.Context, key session.UserKey, opts ...session.Option) ([]*session.Session, error) {
	return s.listChecked(ctx, telemetry.OperationSessionRead, func(token fence.Token) error {
		return s.scope.validateSessionUserKey(token, key.AppName, key.UserID)
	}, func(ctx context.Context) ([]*session.Session, error) {
		values, err := s.inner.ListSessions(ctx, key, opts...)
		if err != nil {
			return nil, err
		}
		if err := s.validateReturnedSessions(ctx, values); err != nil {
			return nil, err
		}
		return values, nil
	})
}

func (s *FencedSessionService) DeleteSession(ctx context.Context, key session.Key, opts ...session.Option) error {
	return s.runChecked(ctx, telemetry.OperationSessionWrite, func(token fence.Token) error {
		return s.scope.validateSessionKey(token, key)
	}, func(ctx context.Context) error { return s.inner.DeleteSession(ctx, key, opts...) })
}

func (s *FencedSessionService) UpdateAppState(ctx context.Context, appName string, state session.StateMap) error {
	return s.runChecked(ctx, telemetry.OperationSessionWrite, func(token fence.Token) error {
		return s.scope.validateAppName(token, appName)
	}, func(ctx context.Context) error { return s.inner.UpdateAppState(ctx, appName, state) })
}

func (s *FencedSessionService) DeleteAppState(ctx context.Context, appName string, key string) error {
	return s.runChecked(ctx, telemetry.OperationSessionWrite, func(token fence.Token) error {
		return s.scope.validateAppName(token, appName)
	}, func(ctx context.Context) error { return s.inner.DeleteAppState(ctx, appName, key) })
}

func (s *FencedSessionService) ListAppStates(ctx context.Context, appName string) (value session.StateMap, err error) {
	ctx, span := telemetry.StartOperation(ctx, telemetry.OperationSessionRead)
	defer func() {
		if recovered := recover(); recovered != nil {
			telemetry.EndOperation(span, errors.New("fenced session operation panicked"))
			panic(recovered)
		}
		telemetry.EndOperation(span, err)
	}()
	if s == nil || isNilStorageService(s.inner) {
		return nil, errors.New("fenced session service is not configured")
	}
	release, err := acquireScopedFence(ctx, s.authorizer, s.scope, func(token fence.Token) error {
		return s.scope.validateAppName(token, appName)
	})
	if err != nil {
		return nil, err
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			_ = release()
			panic(recovered)
		}
		err = errors.Join(err, release())
	}()
	return s.inner.ListAppStates(ctx, appName)
}

func (s *FencedSessionService) UpdateUserState(ctx context.Context, key session.UserKey, state session.StateMap) error {
	return s.runChecked(ctx, telemetry.OperationSessionWrite, func(token fence.Token) error {
		return s.scope.validateSessionUserKey(token, key.AppName, key.UserID)
	}, func(ctx context.Context) error { return s.inner.UpdateUserState(ctx, key, state) })
}

func (s *FencedSessionService) ListUserStates(ctx context.Context, key session.UserKey) (value session.StateMap, err error) {
	ctx, span := telemetry.StartOperation(ctx, telemetry.OperationSessionRead)
	defer func() {
		if recovered := recover(); recovered != nil {
			telemetry.EndOperation(span, errors.New("fenced session operation panicked"))
			panic(recovered)
		}
		telemetry.EndOperation(span, err)
	}()
	if s == nil || isNilStorageService(s.inner) {
		return nil, errors.New("fenced session service is not configured")
	}
	release, err := acquireScopedFence(ctx, s.authorizer, s.scope, func(token fence.Token) error {
		return s.scope.validateSessionUserKey(token, key.AppName, key.UserID)
	})
	if err != nil {
		return nil, err
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			_ = release()
			panic(recovered)
		}
		err = errors.Join(err, release())
	}()
	return s.inner.ListUserStates(ctx, key)
}

func (s *FencedSessionService) DeleteUserState(ctx context.Context, key session.UserKey, stateKey string) error {
	return s.runChecked(ctx, telemetry.OperationSessionWrite, func(token fence.Token) error {
		return s.scope.validateSessionUserKey(token, key.AppName, key.UserID)
	}, func(ctx context.Context) error { return s.inner.DeleteUserState(ctx, key, stateKey) })
}

func (s *FencedSessionService) UpdateSessionState(ctx context.Context, key session.Key, state session.StateMap) error {
	return s.runChecked(ctx, telemetry.OperationSessionWrite, func(token fence.Token) error {
		return s.scope.validateSessionKey(token, key)
	}, func(ctx context.Context) error {
		if s.scope.strict && protectedSessionState(state) != nil {
			return ErrSessionIncarnation
		}
		return s.inner.UpdateSessionState(ctx, key, state)
	})
}

func (s *FencedSessionService) AppendEvent(ctx context.Context, sess *session.Session, evt *event.Event, opts ...session.Option) error {
	return s.runChecked(ctx, telemetry.OperationSessionWrite, func(token fence.Token) error {
		return s.scope.validateSession(token, sess)
	}, func(ctx context.Context) error {
		if s.scope.strict && evt != nil && protectedSessionState(evt.StateDelta) != nil {
			return ErrSessionIncarnation
		}
		return s.inner.AppendEvent(ctx, sess, evt, opts...)
	})
}

func (s *FencedSessionService) CreateSessionSummary(ctx context.Context, sess *session.Session, filterKey string, force bool) error {
	return s.runChecked(ctx, telemetry.OperationSessionWrite, func(token fence.Token) error {
		return s.scope.validateSession(token, sess)
	}, func(ctx context.Context) error { return s.inner.CreateSessionSummary(ctx, sess, filterKey, force) })
}

func (s *FencedSessionService) EnqueueSummaryJob(ctx context.Context, sess *session.Session, filterKey string, force bool) error {
	return s.runChecked(ctx, telemetry.OperationSessionWrite, func(token fence.Token) error {
		return s.scope.validateSession(token, sess)
	}, func(ctx context.Context) error { return s.inner.EnqueueSummaryJob(ctx, sess, filterKey, force) })
}

func (s *FencedSessionService) GetSessionSummaryText(ctx context.Context, sess *session.Session, opts ...session.SummaryOption) (value string, found bool) {
	ctx, span := telemetry.StartOperation(ctx, telemetry.OperationSessionRead)
	var spanErr error
	defer func() {
		if recovered := recover(); recovered != nil {
			telemetry.EndOperation(span, errors.New("fenced session operation panicked"))
			panic(recovered)
		}
		telemetry.EndOperation(span, spanErr)
	}()
	if s == nil || isNilStorageService(s.inner) {
		spanErr = errors.New("fenced session service is not configured")
		fence.RecordError(ctx, spanErr)
		return "", false
	}
	release, err := acquireScopedFence(ctx, s.authorizer, s.scope, func(token fence.Token) error {
		return s.scope.validateSession(token, sess)
	})
	if err != nil {
		spanErr = err
		fence.RecordError(ctx, spanErr)
		return "", false
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			_ = release()
			panic(recovered)
		}
		if releaseErr := release(); releaseErr != nil {
			spanErr = releaseErr
			fence.RecordError(ctx, spanErr)
		}
	}()
	return s.inner.GetSessionSummaryText(ctx, sess, opts...)
}

func (s *FencedSessionService) Close() error {
	if s == nil || isNilStorageService(s.inner) {
		return nil
	}
	return s.inner.Close()
}

func (s *FencedSessionService) validateReturnedSession(ctx context.Context, value *session.Session) error {
	if s == nil || !s.scope.strict {
		return nil
	}
	token, err := fence.TokenFromContext(ctx)
	if err != nil {
		return err
	}
	if err := s.scope.validateSession(token, value); err != nil {
		return fmt.Errorf("validate returned session scope: %w", err)
	}
	return nil
}

func (s *FencedSessionService) validateReturnedSessions(ctx context.Context, values []*session.Session) error {
	if s == nil || !s.scope.strict {
		return nil
	}
	for _, value := range values {
		if err := s.validateReturnedSession(ctx, value); err != nil {
			return err
		}
	}
	return nil
}

var _ session.Service = (*FencedSessionService)(nil)

// ErrHealthCheckUnsupported is returned only when the wrapped tRPC service
// exposes neither HealthCheck nor PingContext. The adapter treats this as an
// explicit capability absence, while preserving real errors from backends
// that do expose one of those probes.
var ErrHealthCheckUnsupported = errors.New("wrapped storage service does not expose a health probe")

func (s *FencedSessionService) HealthCheck(ctx context.Context) error {
	if s == nil || isNilStorageService(s.inner) {
		return errors.New("fenced session service is not configured")
	}
	return delegateHealthCheck(ctx, s.inner)
}

func (s *FencedSessionService) PingContext(ctx context.Context) error {
	if s == nil || isNilStorageService(s.inner) {
		return errors.New("fenced session service is not configured")
	}
	if pinger, ok := s.inner.(interface{ PingContext(context.Context) error }); ok {
		return pinger.PingContext(ctx)
	}
	return ErrHealthCheckUnsupported
}

// FencedMemoryService applies the same authority to memory reads, writes and
// asynchronous extraction enqueueing. Tools are immutable service metadata and
// do not require a per-call token.
type FencedMemoryService struct {
	inner      memory.Service
	authorizer fence.Authorizer
	scope      fencedScope
}

func NewFencedMemoryService(inner memory.Service, authorizer fence.Authorizer) *FencedMemoryService {
	return &FencedMemoryService{inner: inner, authorizer: authorizer}
}

// NewStrictFencedMemoryService creates the production form of the memory
// wrapper. It shares the same tenant and canonical app/user scope contract as
// FencedSessionService.
func NewStrictFencedMemoryService(inner memory.Service, authorizer fence.Authorizer, tenantID string) (*FencedMemoryService, error) {
	if isNilStorageService(inner) {
		return nil, errors.New("strict fenced memory service requires an inner service")
	}
	if isNilStorageService(authorizer) {
		return nil, fmt.Errorf("strict fenced memory service requires an authorizer")
	}
	scope, err := newFencedScope(tenantID)
	if err != nil {
		return nil, err
	}
	return &FencedMemoryService{inner: inner, authorizer: authorizer, scope: scope}, nil
}

func (s *FencedMemoryService) runChecked(ctx context.Context, operation telemetry.Operation, check func(fence.Token) error, fn func(context.Context) error) (err error) {
	ctx, span := telemetry.StartOperation(ctx, operation)
	defer func() {
		if recovered := recover(); recovered != nil {
			telemetry.EndOperation(span, errors.New("fenced memory operation panicked"))
			panic(recovered)
		}
		telemetry.EndOperation(span, err)
	}()
	if s == nil || isNilStorageService(s.inner) {
		return errors.New("fenced memory service is not configured")
	}
	release, err := acquireScopedFence(ctx, s.authorizer, s.scope, check)
	if err != nil {
		return err
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			_ = release()
			panic(recovered)
		}
		err = errors.Join(err, release())
	}()
	return fn(ctx)
}

func (s *FencedMemoryService) entriesChecked(ctx context.Context, operation telemetry.Operation, check func(fence.Token) error, fn func(context.Context) ([]*memory.Entry, error)) (value []*memory.Entry, err error) {
	ctx, span := telemetry.StartOperation(ctx, operation)
	defer func() {
		if recovered := recover(); recovered != nil {
			telemetry.EndOperation(span, errors.New("fenced memory operation panicked"))
			panic(recovered)
		}
		telemetry.EndOperation(span, err)
	}()
	if s == nil || isNilStorageService(s.inner) {
		return nil, errors.New("fenced memory service is not configured")
	}
	release, err := acquireScopedFence(ctx, s.authorizer, s.scope, check)
	if err != nil {
		return nil, err
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			_ = release()
			panic(recovered)
		}
		err = errors.Join(err, release())
	}()
	return fn(ctx)
}

func (s *FencedMemoryService) ReadMemories(ctx context.Context, key memory.UserKey, limit int) ([]*memory.Entry, error) {
	return s.entriesChecked(ctx, telemetry.OperationMemoryRead, func(token fence.Token) error {
		return s.scope.validateUserKey(token, key.AppName, key.UserID)
	}, func(ctx context.Context) ([]*memory.Entry, error) {
		values, err := s.inner.ReadMemories(ctx, key, limit)
		if err != nil {
			return nil, err
		}
		if err := s.validateReturnedEntries(ctx, values); err != nil {
			return nil, err
		}
		return values, nil
	})
}

func (s *FencedMemoryService) SearchMemories(ctx context.Context, key memory.UserKey, query string, opts ...memory.SearchOption) ([]*memory.Entry, error) {
	return s.entriesChecked(ctx, telemetry.OperationMemoryRead, func(token fence.Token) error {
		return s.scope.validateUserKey(token, key.AppName, key.UserID)
	}, func(ctx context.Context) ([]*memory.Entry, error) {
		values, err := s.inner.SearchMemories(ctx, key, query, opts...)
		if err != nil {
			return nil, err
		}
		if err := s.validateReturnedEntries(ctx, values); err != nil {
			return nil, err
		}
		return values, nil
	})
}

func (s *FencedMemoryService) AddMemory(ctx context.Context, key memory.UserKey, text string, topics []string, opts ...memory.AddOption) error {
	return s.runChecked(ctx, telemetry.OperationMemoryWrite, func(token fence.Token) error {
		return s.scope.validateUserKey(token, key.AppName, key.UserID)
	}, func(ctx context.Context) error { return s.inner.AddMemory(ctx, key, text, topics, opts...) })
}

func (s *FencedMemoryService) UpdateMemory(ctx context.Context, key memory.Key, text string, topics []string, opts ...memory.UpdateOption) error {
	return s.runChecked(ctx, telemetry.OperationMemoryWrite, func(token fence.Token) error {
		return s.scope.validateMemoryKey(token, key)
	}, func(ctx context.Context) error { return s.inner.UpdateMemory(ctx, key, text, topics, opts...) })
}

func (s *FencedMemoryService) DeleteMemory(ctx context.Context, key memory.Key) error {
	return s.runChecked(ctx, telemetry.OperationMemoryWrite, func(token fence.Token) error {
		return s.scope.validateMemoryKey(token, key)
	}, func(ctx context.Context) error { return s.inner.DeleteMemory(ctx, key) })
}

func (s *FencedMemoryService) ClearMemories(ctx context.Context, key memory.UserKey) error {
	return s.runChecked(ctx, telemetry.OperationMemoryWrite, func(token fence.Token) error {
		return s.scope.validateUserKey(token, key.AppName, key.UserID)
	}, func(ctx context.Context) error { return s.inner.ClearMemories(ctx, key) })
}

func (s *FencedMemoryService) Tools() []tool.Tool {
	if s == nil || isNilStorageService(s.inner) {
		return nil
	}
	tools := s.inner.Tools()
	return append([]tool.Tool(nil), tools...)
}

func (s *FencedMemoryService) EnqueueAutoMemoryJob(ctx context.Context, sess *session.Session) error {
	return s.runChecked(ctx, telemetry.OperationMemoryWrite, func(token fence.Token) error {
		if sess == nil || (s.scope.strict && sess.ID != token.SessionID) {
			return fence.ErrScopeMismatch
		}
		// The Worker memory wrapper translates the direct-session owner to its
		// provider-scoped Memory actor before handing extraction to this service.
		return s.scope.validateUserKey(token, sess.AppName, sess.UserID)
	}, func(ctx context.Context) error { return s.inner.EnqueueAutoMemoryJob(ctx, sess) })
}

func (s *FencedMemoryService) Close() error {
	if s == nil || isNilStorageService(s.inner) {
		return nil
	}
	return s.inner.Close()
}

var _ memory.Service = (*FencedMemoryService)(nil)

func (s *FencedMemoryService) HealthCheck(ctx context.Context) error {
	if s == nil || isNilStorageService(s.inner) {
		return errors.New("fenced memory service is not configured")
	}
	return delegateHealthCheck(ctx, s.inner)
}

func (s *FencedMemoryService) PingContext(ctx context.Context) error {
	if s == nil || isNilStorageService(s.inner) {
		return errors.New("fenced memory service is not configured")
	}
	if pinger, ok := s.inner.(interface{ PingContext(context.Context) error }); ok {
		return pinger.PingContext(ctx)
	}
	return ErrHealthCheckUnsupported
}

func (s *FencedMemoryService) validateReturnedEntries(ctx context.Context, values []*memory.Entry) error {
	if s == nil || !s.scope.strict {
		return nil
	}
	token, err := fence.TokenFromContext(ctx)
	if err != nil {
		return err
	}
	for _, value := range values {
		if value == nil {
			return fmt.Errorf("%w: memory backend returned a nil entry", fence.ErrScopeMismatch)
		}
		if err := s.scope.validateUserKey(token, value.AppName, value.UserID); err != nil {
			return fmt.Errorf("validate returned memory scope: %w", err)
		}
		if value.Memory == nil || value.ID == "" {
			return fmt.Errorf("%w: memory backend returned an incomplete entry", fence.ErrScopeMismatch)
		}
	}
	return nil
}

// fencedScope is deliberately opt-in. The legacy constructors remain useful
// for callers that only need generation fencing and have no tenant router;
// production composition uses the strict constructors below and fails closed
// on every mismatched key.
type fencedScope struct {
	tenantID string
	strict   bool
}

func newFencedScope(tenantID string) (fencedScope, error) {
	if err := tenant.ValidateTenantID(tenantID); err != nil {
		return fencedScope{}, fmt.Errorf("fenced service tenant ID is invalid: %w", err)
	}
	return fencedScope{tenantID: tenantID, strict: true}, nil
}

func (s fencedScope) validateToken(token fence.Token) error {
	if !s.strict {
		return nil
	}
	if token.TenantID != s.tenantID ||
		strings.TrimSpace(token.ScopedAppName) == "" ||
		strings.TrimSpace(token.UserID) == "" {
		return fmt.Errorf("%w: token tenant or execution scope is not admitted", fence.ErrScopeMismatch)
	}
	expected, err := TenantScopedAppName(&tenant.Tenant{ID: token.TenantID}, token.AgentAppName)
	if err != nil || token.ScopedAppName != expected {
		return fmt.Errorf("%w: token canonical app scope is invalid", fence.ErrScopeMismatch)
	}
	return nil
}

func (s fencedScope) validateAppName(token fence.Token, appName string) error {
	if err := s.validateToken(token); err != nil {
		return err
	}
	if s.strict && appName != token.ScopedAppName {
		return fmt.Errorf("%w: app name is outside execution scope", fence.ErrScopeMismatch)
	}
	return nil
}

func (s fencedScope) validateUserKey(token fence.Token, appName, userID string) error {
	if err := s.validateAppName(token, appName); err != nil {
		return err
	}
	memoryUserID := token.MemoryUserID
	if memoryUserID == "" {
		memoryUserID = token.UserID
	}
	if s.strict && userID != memoryUserID {
		return fmt.Errorf("%w: user is outside execution scope", fence.ErrScopeMismatch)
	}
	return nil
}

func (s fencedScope) validateSessionUserKey(token fence.Token, appName, userID string) error {
	if err := s.validateAppName(token, appName); err != nil {
		return err
	}
	if s.strict && userID != sessionOwnerID(token) {
		return fmt.Errorf("%w: session owner is outside execution scope", fence.ErrScopeMismatch)
	}
	return nil
}

func (s fencedScope) validateSessionKey(token fence.Token, key session.Key) error {
	if err := s.validateAppName(token, key.AppName); err != nil {
		return err
	}
	if s.strict && key.UserID != sessionOwnerID(token) {
		return fmt.Errorf("%w: session owner is outside execution scope", fence.ErrScopeMismatch)
	}
	if s.strict && key.SessionID != token.SessionID {
		return fmt.Errorf("%w: session is outside execution scope", fence.ErrScopeMismatch)
	}
	return nil
}

// sessionOwnerID keeps Session authorization compatible with tokens issued
// before group Session identity was separated from the actor identity.
func sessionOwnerID(token fence.Token) string {
	if strings.TrimSpace(token.SessionOwnerID) != "" {
		return token.SessionOwnerID
	}
	return token.UserID
}

func (s fencedScope) validateMemoryKey(token fence.Token, key memory.Key) error {
	if err := s.validateUserKey(token, key.AppName, key.UserID); err != nil {
		return err
	}
	if s.strict && strings.TrimSpace(key.MemoryID) == "" {
		return fmt.Errorf("%w: memory key is incomplete", fence.ErrScopeMismatch)
	}
	return nil
}

func (s fencedScope) validateSession(token fence.Token, sess *session.Session) error {
	if sess == nil {
		return fmt.Errorf("%w: session is required", fence.ErrScopeMismatch)
	}
	return s.validateSessionKey(token, session.Key{
		AppName:   sess.AppName,
		UserID:    sess.UserID,
		SessionID: sess.ID,
	})
}

func acquireScopedFence(
	ctx context.Context,
	authorizer fence.Authorizer,
	scope fencedScope,
	check func(fence.Token) error,
) (func() error, error) {
	if isNilStorageService(authorizer) {
		return nil, fmt.Errorf("%w: authorizer is unavailable", fence.ErrMismatch)
	}
	token, err := fence.TokenFromContext(ctx)
	if err != nil {
		return nil, err
	}
	if err := scope.validateToken(token); err != nil {
		return nil, err
	}
	if scope.strict && check != nil {
		if err := check(token); err != nil {
			return nil, err
		}
	}
	return authorizer.Acquire(ctx, token)
}

func isNilStorageService(value interface{}) bool {
	if value == nil {
		return true
	}
	v := reflect.ValueOf(value)
	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Ptr, reflect.Slice:
		return v.IsNil()
	default:
		return false
	}
}

func delegateHealthCheck(ctx context.Context, service interface{}) error {
	if checker, ok := service.(interface{ HealthCheck(context.Context) error }); ok {
		if err := checker.HealthCheck(ctx); !errors.Is(err, ErrHealthCheckUnsupported) {
			return err
		}
	}
	if pinger, ok := service.(interface{ PingContext(context.Context) error }); ok {
		return pinger.PingContext(ctx)
	}
	return ErrHealthCheckUnsupported
}
