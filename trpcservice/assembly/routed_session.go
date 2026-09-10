package assembly

import (
	"context"
	"errors"
	"fmt"

	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/session"
)

// routedSessionService reads from one backend and synchronously mirrors every
// mutation to the migration writers. Physical service lifecycle remains owned
// by ManagedSessionProvider.
type routedSessionService struct {
	session.Service
	writers []session.Service
}

func newRoutedSessionService(reader session.Service, writers ...session.Service) session.Service {
	return &routedSessionService{Service: reader, writers: append([]session.Service(nil), writers...)}
}

func (s *routedSessionService) GetEventWindow(ctx context.Context, request session.EventWindowRequest) (*session.EventWindow, error) {
	reader, ok := s.Service.(session.WindowService)
	if !ok {
		return nil, fmt.Errorf("Session migration reader does not support event windows: %w", session.ErrEventPageUnsupported)
	}
	return reader.GetEventWindow(ctx, request)
}

func (s *routedSessionService) CreateSession(ctx context.Context, key session.Key, state session.StateMap, options ...session.Option) (*session.Session, error) {
	var result *session.Session
	for index, writer := range s.writers {
		created, err := writer.CreateSession(ctx, key, cloneSessionState(state), options...)
		if err != nil {
			return nil, err
		}
		if index == 0 {
			result = created
		}
	}
	return result, nil
}

func (s *routedSessionService) DeleteSession(ctx context.Context, key session.Key, options ...session.Option) error {
	return forEachSessionWriter(s.writers, func(writer session.Service) error { return writer.DeleteSession(ctx, key, options...) })
}

func (s *routedSessionService) UpdateAppState(ctx context.Context, appName string, state session.StateMap) error {
	return forEachSessionWriter(s.writers, func(writer session.Service) error {
		return writer.UpdateAppState(ctx, appName, cloneSessionState(state))
	})
}

func (s *routedSessionService) DeleteAppState(ctx context.Context, appName, key string) error {
	return forEachSessionWriter(s.writers, func(writer session.Service) error { return writer.DeleteAppState(ctx, appName, key) })
}

func (s *routedSessionService) UpdateUserState(ctx context.Context, key session.UserKey, state session.StateMap) error {
	return forEachSessionWriter(s.writers, func(writer session.Service) error { return writer.UpdateUserState(ctx, key, cloneSessionState(state)) })
}

func (s *routedSessionService) DeleteUserState(ctx context.Context, key session.UserKey, stateKey string) error {
	return forEachSessionWriter(s.writers, func(writer session.Service) error { return writer.DeleteUserState(ctx, key, stateKey) })
}

func (s *routedSessionService) UpdateSessionState(ctx context.Context, key session.Key, state session.StateMap) error {
	return forEachSessionWriter(s.writers, func(writer session.Service) error {
		return writer.UpdateSessionState(ctx, key, cloneSessionState(state))
	})
}

func (s *routedSessionService) AppendEvent(ctx context.Context, current *session.Session, source *event.Event, options ...session.Option) error {
	return forEachSessionWriter(s.writers, func(writer session.Service) error {
		currentCopy := current.Clone()
		eventCopy := *source
		return writer.AppendEvent(ctx, currentCopy, &eventCopy, options...)
	})
}

func (s *routedSessionService) CreateSessionSummary(ctx context.Context, current *session.Session, filterKey string, force bool) error {
	return forEachSessionWriter(s.writers, func(writer session.Service) error {
		return writer.CreateSessionSummary(ctx, current.Clone(), filterKey, force)
	})
}

func (s *routedSessionService) EnqueueSummaryJob(ctx context.Context, current *session.Session, filterKey string, force bool) error {
	return forEachSessionWriter(s.writers, func(writer session.Service) error {
		return writer.EnqueueSummaryJob(ctx, current.Clone(), filterKey, force)
	})
}

func (s *routedSessionService) Close() error { return nil }

func forEachSessionWriter(writers []session.Service, operation func(session.Service) error) error {
	if len(writers) == 0 {
		return errors.New("Session migration route has no writers")
	}
	for _, writer := range writers {
		if err := operation(writer); err != nil {
			return err
		}
	}
	return nil
}

func cloneSessionState(source session.StateMap) session.StateMap {
	if source == nil {
		return nil
	}
	result := make(session.StateMap, len(source))
	for key, value := range source {
		result[key] = append([]byte(nil), value...)
	}
	return result
}

var _ session.WindowService = (*routedSessionService)(nil)
