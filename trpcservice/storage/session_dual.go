package storage

import (
	"context"
	"fmt"

	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/session"
)

type dualSessionService struct {
	primary          session.Service
	secondary        session.Service
	onSecondaryError func(context.Context, error)
}

func (s *dualSessionService) secondaryError(ctx context.Context, err error) error {
	if s.onSecondaryError != nil {
		s.onSecondaryError(ctx, err)
	}
	return fmt.Errorf("secondary session write: %w", err)
}

func (s *dualSessionService) CreateSession(
	ctx context.Context,
	key session.Key,
	state session.StateMap,
	opts ...session.Option,
) (*session.Session, error) {
	created, err := s.primary.CreateSession(ctx, key, state, opts...)
	if err != nil {
		return nil, err
	}
	if _, err := s.secondary.CreateSession(ctx, key, cloneStateMap(state), opts...); err != nil {
		return nil, s.secondaryError(ctx, err)
	}
	return created, nil
}

func (s *dualSessionService) GetSession(
	ctx context.Context,
	key session.Key,
	opts ...session.Option,
) (*session.Session, error) {
	return s.primary.GetSession(ctx, key, opts...)
}

func (s *dualSessionService) ListSessions(
	ctx context.Context,
	key session.UserKey,
	opts ...session.Option,
) ([]*session.Session, error) {
	return s.primary.ListSessions(ctx, key, opts...)
}

func (s *dualSessionService) DeleteSession(
	ctx context.Context,
	key session.Key,
	opts ...session.Option,
) error {
	if err := s.primary.DeleteSession(ctx, key, opts...); err != nil {
		return err
	}
	if err := s.secondary.DeleteSession(ctx, key, opts...); err != nil {
		return s.secondaryError(ctx, err)
	}
	return nil
}

func (s *dualSessionService) UpdateAppState(
	ctx context.Context,
	appName string,
	state session.StateMap,
) error {
	if err := s.primary.UpdateAppState(ctx, appName, state); err != nil {
		return err
	}
	if err := s.secondary.UpdateAppState(ctx, appName, cloneStateMap(state)); err != nil {
		return s.secondaryError(ctx, err)
	}
	return nil
}

func (s *dualSessionService) DeleteAppState(ctx context.Context, appName string, key string) error {
	if err := s.primary.DeleteAppState(ctx, appName, key); err != nil {
		return err
	}
	if err := s.secondary.DeleteAppState(ctx, appName, key); err != nil {
		return s.secondaryError(ctx, err)
	}
	return nil
}

func (s *dualSessionService) ListAppStates(
	ctx context.Context,
	appName string,
) (session.StateMap, error) {
	return s.primary.ListAppStates(ctx, appName)
}

func (s *dualSessionService) UpdateUserState(
	ctx context.Context,
	key session.UserKey,
	state session.StateMap,
) error {
	if err := s.primary.UpdateUserState(ctx, key, state); err != nil {
		return err
	}
	if err := s.secondary.UpdateUserState(ctx, key, cloneStateMap(state)); err != nil {
		return s.secondaryError(ctx, err)
	}
	return nil
}

func (s *dualSessionService) ListUserStates(
	ctx context.Context,
	key session.UserKey,
) (session.StateMap, error) {
	return s.primary.ListUserStates(ctx, key)
}

func (s *dualSessionService) DeleteUserState(
	ctx context.Context,
	key session.UserKey,
	stateKey string,
) error {
	if err := s.primary.DeleteUserState(ctx, key, stateKey); err != nil {
		return err
	}
	if err := s.secondary.DeleteUserState(ctx, key, stateKey); err != nil {
		return s.secondaryError(ctx, err)
	}
	return nil
}

func (s *dualSessionService) UpdateSessionState(
	ctx context.Context,
	key session.Key,
	state session.StateMap,
) error {
	if err := s.primary.UpdateSessionState(ctx, key, state); err != nil {
		return err
	}
	if _, err := ensureSession(ctx, s.secondary, key, state); err != nil {
		return s.secondaryError(ctx, err)
	}
	if err := s.secondary.UpdateSessionState(ctx, key, cloneStateMap(state)); err != nil {
		return s.secondaryError(ctx, err)
	}
	return nil
}

func (s *dualSessionService) AppendEvent(
	ctx context.Context,
	sess *session.Session,
	item *event.Event,
	opts ...session.Option,
) error {
	if err := s.primary.AppendEvent(ctx, sess, item, opts...); err != nil {
		return err
	}
	key := session.Key{AppName: sess.AppName, UserID: sess.UserID, SessionID: sess.ID}
	secondarySession, err := ensureSession(ctx, s.secondary, key, sess.State)
	if err != nil {
		return s.secondaryError(ctx, err)
	}
	if err := s.secondary.AppendEvent(ctx, secondarySession, item.Clone(), opts...); err != nil {
		return s.secondaryError(ctx, err)
	}
	return nil
}

func (s *dualSessionService) CreateSessionSummary(
	ctx context.Context,
	sess *session.Session,
	filterKey string,
	force bool,
) error {
	if err := s.primary.CreateSessionSummary(ctx, sess, filterKey, force); err != nil {
		return err
	}
	key := session.Key{AppName: sess.AppName, UserID: sess.UserID, SessionID: sess.ID}
	secondarySession, err := ensureSession(ctx, s.secondary, key, sess.State)
	if err != nil {
		return s.secondaryError(ctx, err)
	}
	if err := s.secondary.CreateSessionSummary(ctx, secondarySession, filterKey, force); err != nil {
		return s.secondaryError(ctx, err)
	}
	return nil
}

func (s *dualSessionService) EnqueueSummaryJob(
	ctx context.Context,
	sess *session.Session,
	filterKey string,
	force bool,
) error {
	return s.primary.EnqueueSummaryJob(ctx, sess, filterKey, force)
}

func (s *dualSessionService) GetSessionSummaryText(
	ctx context.Context,
	sess *session.Session,
	opts ...session.SummaryOption,
) (string, bool) {
	return s.primary.GetSessionSummaryText(ctx, sess, opts...)
}

func (s *dualSessionService) Close() error { return nil }

func ensureSession(
	ctx context.Context,
	service session.Service,
	key session.Key,
	state session.StateMap,
) (*session.Session, error) {
	existing, err := service.GetSession(ctx, key)
	if err == nil && existing != nil {
		return existing, nil
	}
	created, createErr := service.CreateSession(ctx, key, cloneStateMap(state))
	if createErr == nil {
		return created, nil
	}
	return service.GetSession(ctx, key)
}

func cloneStateMap(input session.StateMap) session.StateMap {
	result := make(session.StateMap, len(input))
	for key, value := range input {
		result[key] = append([]byte(nil), value...)
	}
	return result
}

var _ session.Service = (*dualSessionService)(nil)
