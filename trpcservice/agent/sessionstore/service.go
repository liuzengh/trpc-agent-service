// Package sessionstore adapts tenant-scoped session persistence to the
// upstream session.Service capability used by Runner.
package sessionstore

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/XnLemon/trpc-agent-service/trpcservice/metrics"
	"github.com/XnLemon/trpc-agent-service/trpcservice/observability"
	sessionstorage "github.com/XnLemon/trpc-agent-service/trpcservice/storage/session"
	trpcevent "trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/session"
)

// Persistence is the storage capability required by the session adapter. It
// intentionally excludes inbound message and reply delivery concerns.
type Persistence interface {
	sessionstorage.SessionStateStore
	sessionstorage.EventHistoryStore
}

type sessionPersistence = Persistence

// Service keeps the upstream session behavior for summaries, tracks, and
// transient state while making Session metadata, state versions, and events
// durable through tenant-scoped Persistence.
type Service struct {
	tenantID  string
	delegate  session.Service
	store     sessionPersistence
	mu        sync.Mutex
	versions  map[string]int64
	telemetry observability.Provider
	metrics   metrics.Catalog
	backend   string
}

// New creates a fixed-tenant session capability. The delegate is borrowed;
// callers remain responsible for closing it.
func New(tenantID string, delegate session.Service, store Persistence) (*Service, error) {
	return NewWithObservability(tenantID, delegate, store, nil)
}

// NewWithObservability creates a fixed-tenant session capability and records
// actual persistence operation latency under the supplied provider. The
// optional backend name is normalized to the bounded metric provider bucket.
func NewWithObservability(tenantID string, delegate session.Service, store Persistence, telemetry observability.Provider, backendName ...string) (*Service, error) {
	if sessionstorage.ValidateTenant(tenantID) != nil || delegate == nil || store == nil {
		return nil, sessionstorage.ErrInvalid
	}
	if telemetry == nil {
		telemetry = observability.NewNoopProvider()
	}
	backend := "other"
	if len(backendName) > 0 {
		backend = backendName[0]
	}
	return &Service{tenantID: tenantID, delegate: delegate, store: store, versions: map[string]int64{}, telemetry: telemetry, metrics: metrics.New(telemetry), backend: backend}, nil
}

// CreateSession creates and durably records a tenant-scoped session.
func (s *Service) CreateSession(ctx context.Context, key session.Key, state session.StateMap, options ...session.Option) (*session.Session, error) {
	if err := validateKey(key); err != nil {
		return nil, err
	}
	created, err := s.delegate.CreateSession(ctx, key, state, options...)
	if err != nil {
		return nil, err
	}
	if _, err := observeStore(s, ctx, func(operationCtx context.Context) (sessionstorage.Session, error) {
		return s.store.CreateSession(operationCtx, s.tenantID, key.SessionID, stateToAny(created.State))
	}); err != nil && !errors.Is(err, sessionstorage.ErrDuplicate) {
		_ = s.delegate.DeleteSession(ctx, key)
		return nil, err
	}
	s.setVersion(key.SessionID, 1)
	return created, nil
}

// GetSession restores a tenant-scoped session and its durable event history.
func (s *Service) GetSession(ctx context.Context, key session.Key, options ...session.Option) (*session.Session, error) {
	if err := validateKey(key); err != nil {
		return nil, err
	}
	persisted, storeErr := observeStore(s, ctx, func(operationCtx context.Context) (sessionstorage.Session, error) {
		return s.store.GetSession(operationCtx, s.tenantID, key.SessionID)
	})
	if storeErr != nil {
		if errors.Is(storeErr, sessionstorage.ErrNotFound) {
			return s.delegate.GetSession(ctx, key, options...)
		}
		return nil, storeErr
	}
	state := anyToState(persisted.State)
	value, err := s.delegate.GetSession(ctx, key, options...)
	if err == nil && value != nil {
		if err := s.delegate.UpdateSessionState(ctx, key, state); err != nil {
			return nil, err
		}
		if refreshed, refreshErr := s.delegate.GetSession(ctx, key, options...); refreshErr == nil && refreshed != nil {
			value = refreshed
		}
		value.State = cloneState(state)
		if err := s.restoreHistory(ctx, value, key.SessionID, options...); err != nil {
			return nil, err
		}
		value.State = cloneState(state)
		s.setVersion(key.SessionID, persisted.Version)
		return value, nil
	}
	value, err = s.delegate.CreateSession(ctx, key, state)
	if err != nil {
		return nil, err
	}
	if err := s.restoreHistory(ctx, value, key.SessionID, options...); err != nil {
		return nil, err
	}
	value.State = cloneState(state)
	s.setVersion(key.SessionID, persisted.Version)
	return value, err
}

func (s *Service) restoreHistory(ctx context.Context, value *session.Session, sessionID string, options ...session.Option) error {
	history, err := observeStore(s, ctx, func(operationCtx context.Context) ([]sessionstorage.EventPayload, error) {
		return s.store.ListEventPayloads(operationCtx, s.tenantID, sessionID)
	})
	if err != nil {
		return err
	}
	existing := make(map[string]struct{}, value.GetEventCount())
	for _, item := range value.GetEvents() {
		existing[item.ID] = struct{}{}
	}
	for _, item := range history {
		if _, ok := existing[item.EventID]; ok {
			continue
		}
		var historical trpcevent.Event
		if err := json.Unmarshal(item.Payload, &historical); err != nil {
			return sessionstorage.ErrStorage
		}
		if err := s.delegate.AppendEvent(ctx, value, &historical, options...); err != nil {
			return err
		}
	}
	return nil
}

// UpdateSessionState persists a session state change before updating the delegate.
func (s *Service) UpdateSessionState(ctx context.Context, key session.Key, state session.StateMap) error {
	if err := validateKey(key); err != nil {
		return err
	}
	persisted, err := observeStore(s, ctx, func(operationCtx context.Context) (sessionstorage.Session, error) {
		return s.store.GetSession(operationCtx, s.tenantID, key.SessionID)
	})
	if err != nil {
		return err
	}
	updated, err := observeStore(s, ctx, func(operationCtx context.Context) (sessionstorage.Session, error) {
		return s.store.UpdateSessionState(operationCtx, s.tenantID, key.SessionID, persisted.Version, stateToAny(state))
	})
	if err != nil {
		return err
	}
	s.setVersion(key.SessionID, updated.Version)
	return s.delegate.UpdateSessionState(ctx, key, state)
}

// AppendEvent durably records an event before forwarding it to the delegate.
func (s *Service) AppendEvent(ctx context.Context, sess *session.Session, value *trpcevent.Event, options ...session.Option) error {
	if sess == nil || value == nil {
		return session.ErrNilSession
	}
	if value.ID == "" {
		return sessionstorage.ErrInvalid
	}
	payload, err := json.Marshal(value)
	if err != nil {
		return sessionstorage.ErrInvalid
	}
	// Inbound message_event rows are created by the trusted Channel/Gateway
	// boundary, where binding_id and external_message_id are available. Runner
	// event history is a separate session-scoped immutable log.
	if _, err := observeStore(s, ctx, func(operationCtx context.Context) (sessionstorage.EventPayload, error) {
		return s.store.AppendEventPayload(operationCtx, sessionstorage.EventPayload{
			TenantID: s.tenantID, SessionID: sess.ID, EventID: value.ID, Payload: payload,
		})
	}); err != nil {
		return err
	}
	return s.delegate.AppendEvent(ctx, sess, value, options...)
}

// ListSessions preserves the upstream delegate's session listing semantics.
func (s *Service) ListSessions(ctx context.Context, key session.UserKey, options ...session.Option) ([]*session.Session, error) {
	started := time.Now()
	operationCtx, _, finish := observability.StartOperation(ctx, s.telemetry, observability.OperationStorageOperation, "storage")
	_ = s.metrics.Request(operationCtx, map[string]string{"component": "storage", "operation": observability.OperationStorageOperation, "provider": s.backend, "status": "started"})
	values, err := s.delegate.ListSessions(operationCtx, key, options...)
	finish(err)
	_ = s.metrics.Operation(operationCtx, started, map[string]string{"component": "storage", "operation": observability.OperationStorageOperation, "provider": s.backend}, err)
	return values, err
}

// DeleteSession removes durable state and then deletes the delegate session.
func (s *Service) DeleteSession(ctx context.Context, key session.Key, options ...session.Option) error {
	if err := validateKey(key); err != nil {
		return err
	}
	if err := observeStoreError(s, ctx, func(operationCtx context.Context) error {
		return s.store.DeleteSession(operationCtx, s.tenantID, key.SessionID)
	}); err != nil && !errors.Is(err, sessionstorage.ErrNotFound) {
		return err
	}
	if err := s.delegate.DeleteSession(ctx, key, options...); err != nil {
		return err
	}
	s.mu.Lock()
	delete(s.versions, key.SessionID)
	s.mu.Unlock()
	return nil
}

// UpdateAppState delegates application state persistence.
func (s *Service) UpdateAppState(ctx context.Context, app string, state session.StateMap) error {
	return s.delegate.UpdateAppState(ctx, app, state)
}

// DeleteAppState delegates application state deletion.
func (s *Service) DeleteAppState(ctx context.Context, app, key string) error {
	return s.delegate.DeleteAppState(ctx, app, key)
}

// ListAppStates delegates application state listing.
func (s *Service) ListAppStates(ctx context.Context, app string) (session.StateMap, error) {
	return s.delegate.ListAppStates(ctx, app)
}

// UpdateUserState delegates user state persistence.
func (s *Service) UpdateUserState(ctx context.Context, key session.UserKey, state session.StateMap) error {
	return s.delegate.UpdateUserState(ctx, key, state)
}

// ListUserStates delegates user state listing.
func (s *Service) ListUserStates(ctx context.Context, key session.UserKey) (session.StateMap, error) {
	return s.delegate.ListUserStates(ctx, key)
}

// DeleteUserState delegates user state deletion.
func (s *Service) DeleteUserState(ctx context.Context, key session.UserKey, field string) error {
	return s.delegate.DeleteUserState(ctx, key, field)
}

// CreateSessionSummary delegates summary creation.
func (s *Service) CreateSessionSummary(ctx context.Context, value *session.Session, filter string, force bool) error {
	return s.delegate.CreateSessionSummary(ctx, value, filter, force)
}

// EnqueueSummaryJob delegates asynchronous summary creation.
func (s *Service) EnqueueSummaryJob(ctx context.Context, value *session.Session, filter string, force bool) error {
	return s.delegate.EnqueueSummaryJob(ctx, value, filter, force)
}

// GetSessionSummaryText delegates summary retrieval.
func (s *Service) GetSessionSummaryText(ctx context.Context, value *session.Session, options ...session.SummaryOption) (string, bool) {
	return s.delegate.GetSessionSummaryText(ctx, value, options...)
}

// Close releases only Service-owned state. The upstream session service is
// borrowed from the caller and must remain available to other tenant runners;
// its owner closes it separately during bootstrap shutdown.
func (s *Service) Close() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	clear(s.versions)
	s.mu.Unlock()
	return nil
}

func validateKey(key session.Key) error {
	if key.AppName == "" {
		return session.ErrAppNameRequired
	}
	if key.UserID == "" {
		return session.ErrUserIDRequired
	}
	if key.SessionID == "" {
		return session.ErrSessionIDRequired
	}
	return nil
}

func observeStore[T any](service *Service, ctx context.Context, operation func(context.Context) (T, error)) (T, error) {
	var zero T
	if service == nil || operation == nil {
		return zero, sessionstorage.ErrInvalid
	}
	started := time.Now()
	operationCtx, _, finish := observability.StartOperation(ctx, service.telemetry, observability.OperationStorageOperation, "storage")
	_ = service.metrics.Request(operationCtx, map[string]string{"component": "storage", "operation": observability.OperationStorageOperation, "provider": service.backend, "status": "started"})
	value, err := operation(operationCtx)
	finish(err)
	_ = service.metrics.Operation(operationCtx, started, map[string]string{"component": "storage", "operation": observability.OperationStorageOperation, "provider": service.backend}, err)
	status := "success"
	if err != nil {
		status = observability.ErrorClass(err)
		if status == "" {
			status = "error"
		}
	}
	_ = service.metrics.BackendDuration(operationCtx, observability.DurationMilliseconds(started), map[string]string{"component": "storage", "provider": service.backend, "status": status, "error_class": observability.ErrorClass(err)})
	return value, err
}

func observeStoreError(service *Service, ctx context.Context, operation func(context.Context) error) error {
	_, err := observeStore(service, ctx, func(operationCtx context.Context) (struct{}, error) {
		return struct{}{}, operation(operationCtx)
	})
	return err
}

func (s *Service) setVersion(id string, version int64) {
	s.mu.Lock()
	s.versions[id] = version
	s.mu.Unlock()
}
func cloneState(value session.StateMap) session.StateMap {
	result := make(session.StateMap, len(value))
	for key, data := range value {
		result[key] = append([]byte(nil), data...)
	}
	return result
}
func stateToAny(value session.StateMap) map[string]any {
	result := make(map[string]any, len(value))
	for key, data := range value {
		result[key] = append([]byte(nil), data...)
	}
	return result
}
func anyToState(value map[string]any) session.StateMap {
	result := make(session.StateMap, len(value))
	for key, data := range value {
		if raw, ok := data.([]byte); ok {
			result[key] = append([]byte(nil), raw...)
		} else if encoded, ok := data.(string); ok {
			if decoded, err := base64.StdEncoding.DecodeString(encoded); err == nil {
				result[key] = decoded
			} else {
				result[key] = []byte(encoded)
			}
		} else if encoded, err := json.Marshal(data); err == nil {
			result[key] = encoded
		}
	}
	return result
}

var _ session.Service = (*Service)(nil)
