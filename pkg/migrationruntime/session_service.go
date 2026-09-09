package migrationruntime

import (
	"context"

	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/datamigration"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/fence"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/storage"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/tenant"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/session"
)

func (r *SessionBackends) Decorator(coordinator SessionCoordinator) storage.SessionServiceDecorator {
	return func(t *tenant.Tenant, inner session.Service) (session.Service, error) {
		if r == nil || coordinator == nil || t == nil || inner == nil || tenant.ValidateTenantID(t.ID) != nil {
			return nil, datamigration.ErrMigrationCapability
		}
		config := t.Storage
		config.SessionConfig = make(map[string]string, len(t.Storage.SessionConfig))
		for key, value := range t.Storage.SessionConfig {
			config.SessionConfig[key] = value
		}
		return &liveSessionService{inner: inner, backends: r, coordinator: coordinator, tenantID: t.ID, fallback: t.Storage.SessionProfile, config: config}, nil
	}
}

type liveSessionService struct {
	inner       session.Service
	backends    *SessionBackends
	coordinator SessionCoordinator
	tenantID    string
	fallback    string
	config      tenant.StorageConfig
}

func (s *liveSessionService) service(ctx context.Context, profile string) (session.Service, func(), error) {
	if profile == s.fallback {
		return s.inner, func() {}, nil
	}
	backend, err := s.backends.backend(ctx, s.tenantID, profile, s.config)
	if err != nil {
		return nil, nil, err
	}
	return backend.service, s.backends.releaseBackend(backend), nil
}

func (s *liveSessionService) read(ctx context.Context, fn func(context.Context, session.Service) error) error {
	return s.coordinator.WithRead(ctx, s.tenantID, datamigration.DomainSession, s.fallback, func(ctx context.Context, profile string) error {
		service, release, err := s.service(ctx, profile)
		if err != nil {
			return err
		}
		defer release()
		return fn(ctx, service)
	})
}

func (s *liveSessionService) write(ctx context.Context, keys []string, fn func(context.Context, session.Service) error) error {
	return s.coordinator.WithWrite(ctx, s.tenantID, datamigration.DomainSession, s.fallback, keys, func(ctx context.Context, profile string) error {
		service, release, err := s.service(ctx, profile)
		if err != nil {
			return err
		}
		defer release()
		return fn(ctx, service)
	})
}

func (s *liveSessionService) keys(key session.Key) ([]string, error) {
	if validateSessionApp(s.tenantID, key.AppName) != nil {
		return nil, datamigration.ErrInvalidRecord
	}
	value, err := sessionRecordKey(key)
	if err != nil {
		return nil, err
	}
	return []string{value}, nil
}

func (s *liveSessionService) CreateSession(ctx context.Context, key session.Key, state session.StateMap, opts ...session.Option) (result *session.Session, err error) {
	keys, err := s.keys(key)
	if err != nil {
		return nil, err
	}
	if requireSessionOnlyState(state) != nil {
		keys = nil
	}
	err = s.write(ctx, keys, func(ctx context.Context, service session.Service) error {
		var opErr error
		result, opErr = service.CreateSession(ctx, key, state, opts...)
		return opErr
	})
	return result, err
}

func (s *liveSessionService) GetSession(ctx context.Context, key session.Key, opts ...session.Option) (result *session.Session, err error) {
	if _, err := s.keys(key); err != nil {
		return nil, err
	}
	err = s.read(ctx, func(ctx context.Context, service session.Service) error {
		var opErr error
		result, opErr = service.GetSession(ctx, key, opts...)
		return opErr
	})
	return result, err
}

func (s *liveSessionService) ListSessions(ctx context.Context, key session.UserKey, opts ...session.Option) (result []*session.Session, err error) {
	if validateSessionApp(s.tenantID, key.AppName) != nil {
		return nil, datamigration.ErrInvalidRecord
	}
	err = s.read(ctx, func(ctx context.Context, service session.Service) error {
		var opErr error
		result, opErr = service.ListSessions(ctx, key, opts...)
		return opErr
	})
	return result, err
}

func (s *liveSessionService) DeleteSession(ctx context.Context, key session.Key, opts ...session.Option) error {
	keys, err := s.keys(key)
	if err != nil {
		return err
	}
	return s.write(ctx, keys, func(ctx context.Context, service session.Service) error {
		return service.DeleteSession(ctx, key, opts...)
	})
}

func (s *liveSessionService) UpdateSessionState(ctx context.Context, key session.Key, state session.StateMap) error {
	keys, err := s.keys(key)
	if err != nil {
		return err
	}
	if requireSessionOnlyState(state) != nil {
		keys = nil
	}
	return s.write(ctx, keys, func(ctx context.Context, service session.Service) error {
		return service.UpdateSessionState(ctx, key, state)
	})
}

func (s *liveSessionService) AppendEvent(ctx context.Context, sess *session.Session, evt *event.Event, opts ...session.Option) error {
	if sess == nil || evt == nil {
		return datamigration.ErrInvalidRecord
	}
	keys, err := s.keys(session.Key{AppName: sess.AppName, UserID: sess.UserID, SessionID: sess.ID})
	if err != nil {
		return err
	}
	if requireSessionOnlyState(evt.StateDelta) != nil {
		keys = nil
	}
	return s.write(ctx, keys, func(ctx context.Context, service session.Service) error {
		return service.AppendEvent(ctx, sess, evt, opts...)
	})
}

func (s *liveSessionService) UpdateAppState(ctx context.Context, appName string, state session.StateMap) error {
	if validateSessionApp(s.tenantID, appName) != nil {
		return datamigration.ErrInvalidRecord
	}
	return s.write(ctx, nil, func(ctx context.Context, service session.Service) error {
		return service.UpdateAppState(ctx, appName, state)
	})
}

func (s *liveSessionService) DeleteAppState(ctx context.Context, appName, key string) error {
	if validateSessionApp(s.tenantID, appName) != nil {
		return datamigration.ErrInvalidRecord
	}
	return s.write(ctx, nil, func(ctx context.Context, service session.Service) error {
		return service.DeleteAppState(ctx, appName, key)
	})
}

func (s *liveSessionService) ListAppStates(ctx context.Context, appName string) (result session.StateMap, err error) {
	if validateSessionApp(s.tenantID, appName) != nil {
		return nil, datamigration.ErrInvalidRecord
	}
	err = s.read(ctx, func(ctx context.Context, service session.Service) error {
		var opErr error
		result, opErr = service.ListAppStates(ctx, appName)
		return opErr
	})
	return result, err
}

func (s *liveSessionService) UpdateUserState(ctx context.Context, key session.UserKey, state session.StateMap) error {
	if validateSessionApp(s.tenantID, key.AppName) != nil {
		return datamigration.ErrInvalidRecord
	}
	return s.write(ctx, nil, func(ctx context.Context, service session.Service) error {
		return service.UpdateUserState(ctx, key, state)
	})
}

func (s *liveSessionService) DeleteUserState(ctx context.Context, key session.UserKey, name string) error {
	if validateSessionApp(s.tenantID, key.AppName) != nil {
		return datamigration.ErrInvalidRecord
	}
	return s.write(ctx, nil, func(ctx context.Context, service session.Service) error {
		return service.DeleteUserState(ctx, key, name)
	})
}

func (s *liveSessionService) ListUserStates(ctx context.Context, key session.UserKey) (result session.StateMap, err error) {
	if validateSessionApp(s.tenantID, key.AppName) != nil {
		return nil, datamigration.ErrInvalidRecord
	}
	err = s.read(ctx, func(ctx context.Context, service session.Service) error {
		var opErr error
		result, opErr = service.ListUserStates(ctx, key)
		return opErr
	})
	return result, err
}

func (s *liveSessionService) CreateSessionSummary(ctx context.Context, sess *session.Session, filterKey string, force bool) error {
	if sess == nil || validateSessionApp(s.tenantID, sess.AppName) != nil {
		return datamigration.ErrInvalidRecord
	}
	return s.write(ctx, nil, func(ctx context.Context, service session.Service) error {
		return service.CreateSessionSummary(ctx, sess, filterKey, force)
	})
}

func (s *liveSessionService) EnqueueSummaryJob(ctx context.Context, sess *session.Session, filterKey string, force bool) error {
	if sess == nil || validateSessionApp(s.tenantID, sess.AppName) != nil {
		return datamigration.ErrInvalidRecord
	}
	return s.write(ctx, nil, func(ctx context.Context, service session.Service) error {
		return service.EnqueueSummaryJob(ctx, sess, filterKey, force)
	})
}

func (s *liveSessionService) GetSessionSummaryText(ctx context.Context, sess *session.Session, opts ...session.SummaryOption) (result string, found bool) {
	if sess == nil || validateSessionApp(s.tenantID, sess.AppName) != nil {
		fence.RecordError(ctx, datamigration.ErrInvalidRecord)
		return "", false
	}
	if err := s.read(ctx, func(ctx context.Context, service session.Service) error {
		result, found = service.GetSessionSummaryText(ctx, sess, opts...)
		return nil
	}); err != nil {
		fence.RecordError(ctx, err)
		return "", false
	}
	return result, found
}

func (s *liveSessionService) AppendTrackEvent(ctx context.Context, sess *session.Session, evt *session.TrackEvent, opts ...session.Option) error {
	if sess == nil || evt == nil {
		return datamigration.ErrInvalidRecord
	}
	keys, err := s.keys(session.Key{AppName: sess.AppName, UserID: sess.UserID, SessionID: sess.ID})
	if err != nil {
		return err
	}
	return s.write(ctx, keys, func(ctx context.Context, service session.Service) error {
		tracks, ok := service.(session.TrackService)
		if !ok {
			return datamigration.ErrMigrationCapability
		}
		return tracks.AppendTrackEvent(ctx, sess, evt, opts...)
	})
}

func (s *liveSessionService) GetTrackEvents(ctx context.Context, key session.Key, track session.Track, opts ...session.Option) (result *session.TrackEvents, err error) {
	if _, err := s.keys(key); err != nil {
		return nil, err
	}
	err = s.read(ctx, func(ctx context.Context, service session.Service) error {
		reader, ok := service.(interface {
			GetTrackEvents(context.Context, session.Key, session.Track, ...session.Option) (*session.TrackEvents, error)
		})
		if !ok {
			return datamigration.ErrMigrationCapability
		}
		var opErr error
		result, opErr = reader.GetTrackEvents(ctx, key, track, opts...)
		return opErr
	})
	return result, err
}

func (s *liveSessionService) Close() error { return s.inner.Close() }

func (s *liveSessionService) HealthCheck(ctx context.Context) error {
	if check, ok := s.inner.(interface{ HealthCheck(context.Context) error }); ok {
		return check.HealthCheck(ctx)
	}
	if check, ok := s.inner.(interface{ PingContext(context.Context) error }); ok {
		return check.PingContext(ctx)
	}
	return storage.ErrHealthCheckUnsupported
}

var _ session.Service = (*liveSessionService)(nil)
var _ session.TrackService = (*liveSessionService)(nil)
