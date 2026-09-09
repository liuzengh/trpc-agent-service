package storage

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/attribute"
	otelmetric "go.opentelemetry.io/otel/metric"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/session"

	"github.com/liuzengh/trpc-agent-service/trpcservice/metrics"
)

// MetricsSessionService decorates a session.Service with per-operation
// latency metrics (metrics.SessionStoreDuration, labeled by backend, op and
// result). Each backend is wrapped at creation, so the migration fanout
// (FanoutSessionService) composes already-decorated backends and per-backend
// attribution survives a migration instead of blurring both sides into one
// series.
type MetricsSessionService struct {
	Backend string // "redis" | "postgres"
	Inner   session.Service
}

var _ session.Service = (*MetricsSessionService)(nil)

// UnwrapSessionService strips the metrics decorator, returning the wrapped
// service (or s itself when undecorated). The storage migrator needs the
// concrete backends — it type-asserts *PGSessionService for full-journal
// reads — so main hands it the unwrapped services.
func UnwrapSessionService(s session.Service) session.Service {
	if m, ok := s.(*MetricsSessionService); ok {
		return m.Inner
	}
	return s
}

func (m *MetricsSessionService) CreateSession(ctx context.Context, key session.Key, state session.StateMap, opts ...session.Option) (*session.Session, error) {
	start := time.Now()
	sess, err := m.Inner.CreateSession(ctx, key, state, opts...)
	m.record(ctx, "create_session", start, err)
	return sess, err
}

func (m *MetricsSessionService) GetSession(ctx context.Context, key session.Key, opts ...session.Option) (*session.Session, error) {
	start := time.Now()
	sess, err := m.Inner.GetSession(ctx, key, opts...)
	m.record(ctx, "get_session", start, err)
	return sess, err
}

func (m *MetricsSessionService) ListSessions(ctx context.Context, userKey session.UserKey, opts ...session.Option) ([]*session.Session, error) {
	start := time.Now()
	sessions, err := m.Inner.ListSessions(ctx, userKey, opts...)
	m.record(ctx, "list_sessions", start, err)
	return sessions, err
}

func (m *MetricsSessionService) DeleteSession(ctx context.Context, key session.Key, opts ...session.Option) error {
	start := time.Now()
	err := m.Inner.DeleteSession(ctx, key, opts...)
	m.record(ctx, "delete_session", start, err)
	return err
}

func (m *MetricsSessionService) UpdateAppState(ctx context.Context, appName string, state session.StateMap) error {
	start := time.Now()
	err := m.Inner.UpdateAppState(ctx, appName, state)
	m.record(ctx, "update_app_state", start, err)
	return err
}

func (m *MetricsSessionService) DeleteAppState(ctx context.Context, appName string, key string) error {
	start := time.Now()
	err := m.Inner.DeleteAppState(ctx, appName, key)
	m.record(ctx, "delete_app_state", start, err)
	return err
}

func (m *MetricsSessionService) ListAppStates(ctx context.Context, appName string) (session.StateMap, error) {
	start := time.Now()
	state, err := m.Inner.ListAppStates(ctx, appName)
	m.record(ctx, "list_app_states", start, err)
	return state, err
}

func (m *MetricsSessionService) UpdateUserState(ctx context.Context, userKey session.UserKey, state session.StateMap) error {
	start := time.Now()
	err := m.Inner.UpdateUserState(ctx, userKey, state)
	m.record(ctx, "update_user_state", start, err)
	return err
}

func (m *MetricsSessionService) ListUserStates(ctx context.Context, userKey session.UserKey) (session.StateMap, error) {
	start := time.Now()
	state, err := m.Inner.ListUserStates(ctx, userKey)
	m.record(ctx, "list_user_states", start, err)
	return state, err
}

func (m *MetricsSessionService) DeleteUserState(ctx context.Context, userKey session.UserKey, key string) error {
	start := time.Now()
	err := m.Inner.DeleteUserState(ctx, userKey, key)
	m.record(ctx, "delete_user_state", start, err)
	return err
}

func (m *MetricsSessionService) UpdateSessionState(ctx context.Context, key session.Key, state session.StateMap) error {
	start := time.Now()
	err := m.Inner.UpdateSessionState(ctx, key, state)
	m.record(ctx, "update_session_state", start, err)
	return err
}

func (m *MetricsSessionService) AppendEvent(ctx context.Context, sess *session.Session, e *event.Event, opts ...session.Option) error {
	start := time.Now()
	err := m.Inner.AppendEvent(ctx, sess, e, opts...)
	m.record(ctx, "append_event", start, err)
	return err
}

func (m *MetricsSessionService) CreateSessionSummary(ctx context.Context, sess *session.Session, filterKey string, force bool) error {
	start := time.Now()
	err := m.Inner.CreateSessionSummary(ctx, sess, filterKey, force)
	m.record(ctx, "create_session_summary", start, err)
	return err
}

func (m *MetricsSessionService) EnqueueSummaryJob(ctx context.Context, sess *session.Session, filterKey string, force bool) error {
	start := time.Now()
	err := m.Inner.EnqueueSummaryJob(ctx, sess, filterKey, force)
	m.record(ctx, "enqueue_summary_job", start, err)
	return err
}

// GetSessionSummaryText carries no error, so its result is always "ok".
func (m *MetricsSessionService) GetSessionSummaryText(ctx context.Context, sess *session.Session, opts ...session.SummaryOption) (string, bool) {
	start := time.Now()
	text, ok := m.Inner.GetSessionSummaryText(ctx, sess, opts...)
	m.record(ctx, "get_session_summary_text", start, nil)
	return text, ok
}

// Close delegates to the wrapped service: the decorator owns no resources.
func (m *MetricsSessionService) Close() error {
	start := time.Now()
	err := m.Inner.Close()
	m.record(context.Background(), "close", start, err)
	return err
}

// record reports one operation's latency in milliseconds, tagged with the
// result so an error-rate alert can split failures out of the latency stream.
func (m *MetricsSessionService) record(ctx context.Context, op string, start time.Time, err error) {
	result := "ok"
	if err != nil {
		result = "error"
	}
	metrics.SessionStoreDuration.Record(ctx,
		float64(time.Since(start))/float64(time.Millisecond),
		otelmetric.WithAttributes(
			attribute.String("backend", m.Backend),
			attribute.String("op", op),
			attribute.String("result", result),
		))
}
