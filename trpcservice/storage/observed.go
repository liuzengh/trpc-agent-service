package storage

import (
	"context"
	"time"

	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/memory"
	"trpc.group/trpc-go/trpc-agent-go/session"
	"trpc.group/trpc-go/trpc-agent-go/tool"
)

// OperationObservation contains only bounded, server-owned metric dimensions.
// Backend errors, keys, payloads, user IDs, and session IDs are excluded.
type OperationObservation struct {
	TenantID, AppID, Domain, Backend, Operation, Status string
	Duration                                            time.Duration
}

type OperationObserver func(context.Context, OperationObservation)

func observe(ctx context.Context, observer OperationObserver, tenantID, appID, domain, backend, operation string, started time.Time, err error) {
	if observer == nil {
		return
	}
	status := "success"
	if err != nil {
		status = "failed"
	}
	observer(ctx, OperationObservation{TenantID: tenantID, AppID: appID, Domain: domain, Backend: backend, Operation: operation, Status: status, Duration: time.Since(started)})
}

// ObservedSession measures actual calls to the configured tRPC-Agent-Go
// Session adapter without exposing the adapter's keys or values.
type ObservedSession struct {
	Delegate        session.Service
	TenantID, AppID string
	Backend         string
	Observe         OperationObserver
}

func (service *ObservedSession) record(ctx context.Context, operation string, started time.Time, err error) {
	observe(ctx, service.Observe, service.TenantID, service.AppID, "session", service.Backend, operation, started, err)
}
func (service *ObservedSession) CreateSession(ctx context.Context, key session.Key, state session.StateMap, opts ...session.Option) (value *session.Session, err error) {
	started := time.Now()
	defer func() { service.record(ctx, "create", started, err) }()
	return service.Delegate.CreateSession(ctx, key, state, opts...)
}
func (service *ObservedSession) GetSession(ctx context.Context, key session.Key, opts ...session.Option) (value *session.Session, err error) {
	started := time.Now()
	defer func() { service.record(ctx, "get", started, err) }()
	return service.Delegate.GetSession(ctx, key, opts...)
}
func (service *ObservedSession) ListSessions(ctx context.Context, key session.UserKey, opts ...session.Option) (value []*session.Session, err error) {
	started := time.Now()
	defer func() { service.record(ctx, "list", started, err) }()
	return service.Delegate.ListSessions(ctx, key, opts...)
}
func (service *ObservedSession) DeleteSession(ctx context.Context, key session.Key, opts ...session.Option) (err error) {
	started := time.Now()
	defer func() { service.record(ctx, "delete", started, err) }()
	return service.Delegate.DeleteSession(ctx, key, opts...)
}
func (service *ObservedSession) UpdateAppState(ctx context.Context, appName string, state session.StateMap) (err error) {
	started := time.Now()
	defer func() { service.record(ctx, "update_app_state", started, err) }()
	return service.Delegate.UpdateAppState(ctx, appName, state)
}
func (service *ObservedSession) DeleteAppState(ctx context.Context, appName, key string) (err error) {
	started := time.Now()
	defer func() { service.record(ctx, "delete_app_state", started, err) }()
	return service.Delegate.DeleteAppState(ctx, appName, key)
}
func (service *ObservedSession) ListAppStates(ctx context.Context, appName string) (value session.StateMap, err error) {
	started := time.Now()
	defer func() { service.record(ctx, "list_app_states", started, err) }()
	return service.Delegate.ListAppStates(ctx, appName)
}
func (service *ObservedSession) UpdateUserState(ctx context.Context, key session.UserKey, state session.StateMap) (err error) {
	started := time.Now()
	defer func() { service.record(ctx, "update_user_state", started, err) }()
	return service.Delegate.UpdateUserState(ctx, key, state)
}
func (service *ObservedSession) ListUserStates(ctx context.Context, key session.UserKey) (value session.StateMap, err error) {
	started := time.Now()
	defer func() { service.record(ctx, "list_user_states", started, err) }()
	return service.Delegate.ListUserStates(ctx, key)
}
func (service *ObservedSession) DeleteUserState(ctx context.Context, key session.UserKey, name string) (err error) {
	started := time.Now()
	defer func() { service.record(ctx, "delete_user_state", started, err) }()
	return service.Delegate.DeleteUserState(ctx, key, name)
}
func (service *ObservedSession) UpdateSessionState(ctx context.Context, key session.Key, state session.StateMap) (err error) {
	started := time.Now()
	defer func() { service.record(ctx, "update_session_state", started, err) }()
	return service.Delegate.UpdateSessionState(ctx, key, state)
}
func (service *ObservedSession) AppendEvent(ctx context.Context, value *session.Session, evt *event.Event, opts ...session.Option) (err error) {
	started := time.Now()
	defer func() { service.record(ctx, "append_event", started, err) }()
	return service.Delegate.AppendEvent(ctx, value, evt, opts...)
}
func (service *ObservedSession) CreateSessionSummary(ctx context.Context, value *session.Session, filter string, force bool) (err error) {
	started := time.Now()
	defer func() { service.record(ctx, "create_summary", started, err) }()
	return service.Delegate.CreateSessionSummary(ctx, value, filter, force)
}
func (service *ObservedSession) EnqueueSummaryJob(ctx context.Context, value *session.Session, filter string, force bool) (err error) {
	started := time.Now()
	defer func() { service.record(ctx, "enqueue_summary", started, err) }()
	return service.Delegate.EnqueueSummaryJob(ctx, value, filter, force)
}
func (service *ObservedSession) GetSessionSummaryText(ctx context.Context, value *session.Session, opts ...session.SummaryOption) (text string, ok bool) {
	started := time.Now()
	defer func() { service.record(ctx, "get_summary", started, nil) }()
	return service.Delegate.GetSessionSummaryText(ctx, value, opts...)
}
func (service *ObservedSession) Close() error { return service.Delegate.Close() }

// ObservedMemory measures actual calls to the configured tRPC-Agent-Go Memory
// adapter and intentionally does not inspect memory content or identifiers.
type ObservedMemory struct {
	Delegate        memory.Service
	TenantID, AppID string
	Backend         string
	Observe         OperationObserver
}

func (service *ObservedMemory) record(ctx context.Context, operation string, started time.Time, err error) {
	observe(ctx, service.Observe, service.TenantID, service.AppID, "memory", service.Backend, operation, started, err)
}
func (service *ObservedMemory) ReadMemories(ctx context.Context, key memory.UserKey, limit int) (value []*memory.Entry, err error) {
	started := time.Now()
	defer func() { service.record(ctx, "read", started, err) }()
	return service.Delegate.ReadMemories(ctx, key, limit)
}
func (service *ObservedMemory) SearchMemories(ctx context.Context, key memory.UserKey, query string, opts ...memory.SearchOption) (value []*memory.Entry, err error) {
	started := time.Now()
	defer func() { service.record(ctx, "search", started, err) }()
	return service.Delegate.SearchMemories(ctx, key, query, opts...)
}
func (service *ObservedMemory) AddMemory(ctx context.Context, key memory.UserKey, value string, topics []string, opts ...memory.AddOption) (err error) {
	started := time.Now()
	defer func() { service.record(ctx, "add", started, err) }()
	return service.Delegate.AddMemory(ctx, key, value, topics, opts...)
}
func (service *ObservedMemory) UpdateMemory(ctx context.Context, key memory.Key, value string, topics []string, opts ...memory.UpdateOption) (err error) {
	started := time.Now()
	defer func() { service.record(ctx, "update", started, err) }()
	return service.Delegate.UpdateMemory(ctx, key, value, topics, opts...)
}
func (service *ObservedMemory) DeleteMemory(ctx context.Context, key memory.Key) (err error) {
	started := time.Now()
	defer func() { service.record(ctx, "delete", started, err) }()
	return service.Delegate.DeleteMemory(ctx, key)
}
func (service *ObservedMemory) ClearMemories(ctx context.Context, key memory.UserKey) (err error) {
	started := time.Now()
	defer func() { service.record(ctx, "clear", started, err) }()
	return service.Delegate.ClearMemories(ctx, key)
}
func (service *ObservedMemory) Tools() []tool.Tool { return service.Delegate.Tools() }
func (service *ObservedMemory) EnqueueAutoMemoryJob(ctx context.Context, value *session.Session) (err error) {
	started := time.Now()
	defer func() { service.record(ctx, "enqueue_auto", started, err) }()
	return service.Delegate.EnqueueAutoMemoryJob(ctx, value)
}
func (service *ObservedMemory) Close() error { return service.Delegate.Close() }

var _ session.Service = (*ObservedSession)(nil)
var _ memory.Service = (*ObservedMemory)(nil)
