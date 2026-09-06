package storage

import (
	"context"
	"reflect"
	"strings"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/metrics"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtimecontext"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/session"
)

type observedSession struct {
	session.Service
	backend    string
	operations *metrics.Operation
}

func observeSession(service session.Service, backend string) session.Service {
	if _, ok := service.(*observedSession); ok {
		return service
	}
	if backend == "startup" || backend == "" {
		kind := reflect.TypeOf(service)
		if kind.Kind() == reflect.Pointer {
			kind = kind.Elem()
		}
		path := kind.PkgPath()
		backend = "custom"
		for _, candidate := range []string{"inmemory", "redis", "postgres"} {
			if strings.HasSuffix(path, "/"+candidate) {
				backend = candidate
			}
		}
	}
	return &observedSession{service, backend, metrics.NewOperation("storage")}
}
func (s *observedSession) start(ctx context.Context, app, op string) func(error) {
	started := time.Now()
	tenantID, appID, _ := runtimecontext.ParseStorageScope(app)
	return func(err error) { s.operations.Finish(ctx, started, tenantID, appID, "session."+op, s.backend, err) }
}
func (s *observedSession) CreateSession(ctx context.Context, key session.Key, state session.StateMap, opts ...session.Option) (out *session.Session, err error) {
	defer func(done func(error)) { done(err) }(s.start(ctx, key.AppName, "create"))
	return s.Service.CreateSession(ctx, key, state, opts...)
}
func (s *observedSession) GetSession(ctx context.Context, key session.Key, opts ...session.Option) (out *session.Session, err error) {
	defer func(done func(error)) { done(err) }(s.start(ctx, key.AppName, "get"))
	return s.Service.GetSession(ctx, key, opts...)
}
func (s *observedSession) ListSessions(ctx context.Context, key session.UserKey, opts ...session.Option) (out []*session.Session, err error) {
	defer func(done func(error)) { done(err) }(s.start(ctx, key.AppName, "list"))
	return s.Service.ListSessions(ctx, key, opts...)
}
func (s *observedSession) DeleteSession(ctx context.Context, key session.Key, opts ...session.Option) (err error) {
	defer func(done func(error)) { done(err) }(s.start(ctx, key.AppName, "delete"))
	return s.Service.DeleteSession(ctx, key, opts...)
}
func (s *observedSession) AppendEvent(ctx context.Context, sess *session.Session, evt *event.Event, opts ...session.Option) (err error) {
	if sess == nil {
		return session.ErrNilSession
	}
	defer func(done func(error)) { done(err) }(s.start(ctx, sess.AppName, "event.append"))
	return s.Service.AppendEvent(ctx, sess, evt, opts...)
}
func (s *observedSession) UpdateSessionState(ctx context.Context, key session.Key, state session.StateMap) (err error) {
	defer func(done func(error)) { done(err) }(s.start(ctx, key.AppName, "state.update"))
	return s.Service.UpdateSessionState(ctx, key, state)
}
func (s *observedSession) UpdateAppState(ctx context.Context, app string, state session.StateMap) (err error) {
	defer func(done func(error)) { done(err) }(s.start(ctx, app, "app_state.update"))
	return s.Service.UpdateAppState(ctx, app, state)
}
func (s *observedSession) DeleteAppState(ctx context.Context, app, key string) (err error) {
	defer func(done func(error)) { done(err) }(s.start(ctx, app, "app_state.delete"))
	return s.Service.DeleteAppState(ctx, app, key)
}
func (s *observedSession) ListAppStates(ctx context.Context, app string) (out session.StateMap, err error) {
	defer func(done func(error)) { done(err) }(s.start(ctx, app, "app_state.list"))
	return s.Service.ListAppStates(ctx, app)
}
func (s *observedSession) UpdateUserState(ctx context.Context, key session.UserKey, state session.StateMap) (err error) {
	defer func(done func(error)) { done(err) }(s.start(ctx, key.AppName, "user_state.update"))
	return s.Service.UpdateUserState(ctx, key, state)
}
func (s *observedSession) DeleteUserState(ctx context.Context, key session.UserKey, stateKey string) (err error) {
	defer func(done func(error)) { done(err) }(s.start(ctx, key.AppName, "user_state.delete"))
	return s.Service.DeleteUserState(ctx, key, stateKey)
}
func (s *observedSession) ListUserStates(ctx context.Context, key session.UserKey) (out session.StateMap, err error) {
	defer func(done func(error)) { done(err) }(s.start(ctx, key.AppName, "user_state.list"))
	return s.Service.ListUserStates(ctx, key)
}
func (s *observedSession) CreateSessionSummary(ctx context.Context, sess *session.Session, filter string, force bool) (err error) {
	if sess == nil {
		return session.ErrNilSession
	}
	defer func(done func(error)) { done(err) }(s.start(ctx, sess.AppName, "summary.create"))
	return s.Service.CreateSessionSummary(ctx, sess, filter, force)
}
