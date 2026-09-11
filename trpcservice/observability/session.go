package observability

import (
	"context"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/session"
)

// WrapSession instruments the SPI passed to Runner. Keep the original service
// for transactional/fencing/database-identity capabilities in Worker Runtime.
func WrapSession(inner session.Service, backend, tenant string) session.Service {
	return &observedSession{Service: inner, backend: backend, tenant: tenant}
}

type observedSession struct {
	session.Service
	backend, tenant string
}

func (s *observedSession) CreateSession(ctx context.Context, key session.Key, state session.StateMap, options ...session.Option) (value *session.Session, err error) {
	ctx, finish := StartStorage(ctx, "session.create", s.backend, s.tenant, "")
	defer func() { finish(err) }()
	return s.Service.CreateSession(ctx, key, state, options...)
}

func (s *observedSession) GetSession(ctx context.Context, key session.Key, options ...session.Option) (value *session.Session, err error) {
	ctx, finish := StartStorage(ctx, "session.get", s.backend, s.tenant, "")
	defer func() { finish(err) }()
	return s.Service.GetSession(ctx, key, options...)
}

func (s *observedSession) ListSessions(ctx context.Context, key session.UserKey, options ...session.Option) (value []*session.Session, err error) {
	ctx, finish := StartStorage(ctx, "session.list", s.backend, s.tenant, "")
	defer func() { finish(err) }()
	return s.Service.ListSessions(ctx, key, options...)
}

func (s *observedSession) DeleteSession(ctx context.Context, key session.Key, options ...session.Option) (err error) {
	ctx, finish := StartStorage(ctx, "session.delete", s.backend, s.tenant, "")
	defer func() { finish(err) }()
	return s.Service.DeleteSession(ctx, key, options...)
}

func (s *observedSession) UpdateAppState(ctx context.Context, appName string, state session.StateMap) (err error) {
	ctx, finish := StartStorage(ctx, "session.app_state.update", s.backend, s.tenant, "")
	defer func() { finish(err) }()
	return s.Service.UpdateAppState(ctx, appName, state)
}

func (s *observedSession) DeleteAppState(ctx context.Context, appName, key string) (err error) {
	ctx, finish := StartStorage(ctx, "session.app_state.delete", s.backend, s.tenant, "")
	defer func() { finish(err) }()
	return s.Service.DeleteAppState(ctx, appName, key)
}

func (s *observedSession) ListAppStates(ctx context.Context, appName string) (value session.StateMap, err error) {
	ctx, finish := StartStorage(ctx, "session.app_state.list", s.backend, s.tenant, "")
	defer func() { finish(err) }()
	return s.Service.ListAppStates(ctx, appName)
}

func (s *observedSession) UpdateUserState(ctx context.Context, key session.UserKey, state session.StateMap) (err error) {
	ctx, finish := StartStorage(ctx, "session.user_state.update", s.backend, s.tenant, "")
	defer func() { finish(err) }()
	return s.Service.UpdateUserState(ctx, key, state)
}

func (s *observedSession) ListUserStates(ctx context.Context, key session.UserKey) (value session.StateMap, err error) {
	ctx, finish := StartStorage(ctx, "session.user_state.list", s.backend, s.tenant, "")
	defer func() { finish(err) }()
	return s.Service.ListUserStates(ctx, key)
}

func (s *observedSession) DeleteUserState(ctx context.Context, userKey session.UserKey, key string) (err error) {
	ctx, finish := StartStorage(ctx, "session.user_state.delete", s.backend, s.tenant, "")
	defer func() { finish(err) }()
	return s.Service.DeleteUserState(ctx, userKey, key)
}

func (s *observedSession) UpdateSessionState(ctx context.Context, key session.Key, state session.StateMap) (err error) {
	ctx, finish := StartStorage(ctx, "session.state.update", s.backend, s.tenant, "")
	defer func() { finish(err) }()
	return s.Service.UpdateSessionState(ctx, key, state)
}

func (s *observedSession) AppendEvent(ctx context.Context, sess *session.Session, evt *event.Event, options ...session.Option) (err error) {
	ctx, finish := StartStorage(ctx, "session.event.append", s.backend, s.tenant, "")
	defer func() { finish(err) }()
	return s.Service.AppendEvent(ctx, sess, evt, options...)
}

func (s *observedSession) CreateSessionSummary(ctx context.Context, sess *session.Session, filterKey string, force bool) (err error) {
	ctx, finish := StartStorage(ctx, "summary.create", s.backend, s.tenant, "")
	defer func() { finish(err) }()
	return s.Service.CreateSessionSummary(ctx, sess, filterKey, force)
}

func (s *observedSession) EnqueueSummaryJob(ctx context.Context, sess *session.Session, filterKey string, force bool) (err error) {
	ctx, finish := StartStorage(ctx, "summary.enqueue", s.backend, s.tenant, "")
	defer func() { finish(err) }()
	return s.Service.EnqueueSummaryJob(ctx, sess, filterKey, force)
}

func (s *observedSession) GetSessionSummaryText(ctx context.Context, sess *session.Session, opts ...session.SummaryOption) (string, bool) {
	ctx, finish := StartStorage(ctx, "summary.text", s.backend, s.tenant, "")
	defer finish(nil)
	return s.Service.GetSessionSummaryText(ctx, sess, opts...)
}
