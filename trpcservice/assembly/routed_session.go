package assembly

import (
	"context"
	"errors"
	"fmt"

	platformstorage "github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/session"
)

// routedSessionService reads from the migration primary and mirrors durable
// mutations to replicas. A durable dirty intent is written before the primary
// mutation, so replica failures never create an untracked divergence.
type routedSessionService struct {
	session.Service
	primary  session.Service
	replicas []session.Service
	route    platformstorage.SessionMigrationRoute
	repairs  platformstorage.SessionMigrationRepairStore
}

func newRoutedSessionService(
	reader session.Service,
	primary session.Service,
	replicas []session.Service,
	route platformstorage.SessionMigrationRoute,
	repairs platformstorage.SessionMigrationRepairStore,
) session.Service {
	base := &routedSessionService{
		Service: reader, primary: primary, replicas: append([]session.Service(nil), replicas...), route: route, repairs: repairs,
	}
	return wrapRoutedSessionCapabilities(base, reader)
}

func (s *routedSessionService) searchEvents(ctx context.Context, request session.EventSearchRequest) ([]session.EventSearchResult, error) {
	searchable, ok := s.Service.(session.SearchableService)
	if !ok {
		return nil, errors.New("Session migration reader does not support event search")
	}
	return searchable.SearchEvents(ctx, request)
}

func (s *routedSessionService) getEventWindow(ctx context.Context, request session.EventWindowRequest) (*session.EventWindow, error) {
	window, ok := s.Service.(session.WindowService)
	if !ok {
		return nil, fmt.Errorf("Session migration reader does not support event windows: %w", session.ErrEventPageUnsupported)
	}
	return window.GetEventWindow(ctx, request)
}

func (s *routedSessionService) appendTrackEvent(ctx context.Context, current *session.Session, trackEvent *session.TrackEvent, options ...session.Option) error {
	if current == nil {
		return session.ErrNilSession
	}
	key := session.Key{AppName: current.AppName, UserID: current.UserID, SessionID: current.ID}
	return s.mutateSame(ctx, s.sessionRepair(key), func(writer session.Service) error {
		trackWriter, ok := writer.(session.TrackService)
		if !ok {
			return errors.New("Session migration writer does not support track events")
		}
		currentCopy := current.Clone()
		var eventCopy *session.TrackEvent
		if trackEvent != nil {
			copy := *trackEvent
			copy.Payload = append([]byte(nil), trackEvent.Payload...)
			eventCopy = &copy
		}
		return trackWriter.AppendTrackEvent(ctx, currentCopy, eventCopy, options...)
	})
}

func (s *routedSessionService) getTrackEvents(ctx context.Context, key session.Key, track session.Track, options ...session.Option) (*session.TrackEvents, error) {
	reader, ok := s.Service.(sessionTrackEventReader)
	if !ok {
		return nil, errors.New("Session migration reader does not support track reads")
	}
	return reader.GetTrackEvents(ctx, key, track, options...)
}

type routedSearchSessionService struct{ *routedSessionService }

func (s *routedSearchSessionService) SearchEvents(ctx context.Context, request session.EventSearchRequest) ([]session.EventSearchResult, error) {
	return s.searchEvents(ctx, request)
}

type routedWindowSessionService struct{ *routedSessionService }

func (s *routedWindowSessionService) GetEventWindow(ctx context.Context, request session.EventWindowRequest) (*session.EventWindow, error) {
	return s.getEventWindow(ctx, request)
}

type routedTrackSessionService struct{ *routedSessionService }

func (s *routedTrackSessionService) AppendTrackEvent(ctx context.Context, current *session.Session, trackEvent *session.TrackEvent, options ...session.Option) error {
	return s.appendTrackEvent(ctx, current, trackEvent, options...)
}

type routedSearchWindowSessionService struct{ *routedSessionService }

func (s *routedSearchWindowSessionService) SearchEvents(ctx context.Context, request session.EventSearchRequest) ([]session.EventSearchResult, error) {
	return s.searchEvents(ctx, request)
}
func (s *routedSearchWindowSessionService) GetEventWindow(ctx context.Context, request session.EventWindowRequest) (*session.EventWindow, error) {
	return s.getEventWindow(ctx, request)
}

type routedSearchTrackSessionService struct{ *routedSessionService }

func (s *routedSearchTrackSessionService) SearchEvents(ctx context.Context, request session.EventSearchRequest) ([]session.EventSearchResult, error) {
	return s.searchEvents(ctx, request)
}
func (s *routedSearchTrackSessionService) AppendTrackEvent(ctx context.Context, current *session.Session, trackEvent *session.TrackEvent, options ...session.Option) error {
	return s.appendTrackEvent(ctx, current, trackEvent, options...)
}

type routedWindowTrackSessionService struct{ *routedSessionService }

func (s *routedWindowTrackSessionService) GetEventWindow(ctx context.Context, request session.EventWindowRequest) (*session.EventWindow, error) {
	return s.getEventWindow(ctx, request)
}
func (s *routedWindowTrackSessionService) AppendTrackEvent(ctx context.Context, current *session.Session, trackEvent *session.TrackEvent, options ...session.Option) error {
	return s.appendTrackEvent(ctx, current, trackEvent, options...)
}

type routedSearchWindowTrackSessionService struct{ *routedSessionService }

func (s *routedSearchWindowTrackSessionService) SearchEvents(ctx context.Context, request session.EventSearchRequest) ([]session.EventSearchResult, error) {
	return s.searchEvents(ctx, request)
}
func (s *routedSearchWindowTrackSessionService) GetEventWindow(ctx context.Context, request session.EventWindowRequest) (*session.EventWindow, error) {
	return s.getEventWindow(ctx, request)
}
func (s *routedSearchWindowTrackSessionService) AppendTrackEvent(ctx context.Context, current *session.Session, trackEvent *session.TrackEvent, options ...session.Option) error {
	return s.appendTrackEvent(ctx, current, trackEvent, options...)
}

type routedTrackReaderSessionService struct{ *routedTrackSessionService }

func (s *routedTrackReaderSessionService) GetTrackEvents(ctx context.Context, key session.Key, track session.Track, options ...session.Option) (*session.TrackEvents, error) {
	return s.getTrackEvents(ctx, key, track, options...)
}

type routedSearchTrackReaderSessionService struct {
	*routedSearchTrackSessionService
}

func (s *routedSearchTrackReaderSessionService) GetTrackEvents(ctx context.Context, key session.Key, track session.Track, options ...session.Option) (*session.TrackEvents, error) {
	return s.getTrackEvents(ctx, key, track, options...)
}

type routedWindowTrackReaderSessionService struct {
	*routedWindowTrackSessionService
}

func (s *routedWindowTrackReaderSessionService) GetTrackEvents(ctx context.Context, key session.Key, track session.Track, options ...session.Option) (*session.TrackEvents, error) {
	return s.getTrackEvents(ctx, key, track, options...)
}

type routedSearchWindowTrackReaderSessionService struct {
	*routedSearchWindowTrackSessionService
}

func (s *routedSearchWindowTrackReaderSessionService) GetTrackEvents(ctx context.Context, key session.Key, track session.Track, options ...session.Option) (*session.TrackEvents, error) {
	return s.getTrackEvents(ctx, key, track, options...)
}

func wrapRoutedSessionCapabilities(base *routedSessionService, reader session.Service) session.Service {
	_, hasSearch := reader.(session.SearchableService)
	_, hasWindow := reader.(session.WindowService)
	_, hasReader := reader.(sessionTrackEventReader)
	hasTrack := routedWritersSupportTrack(base.primary, base.replicas)
	if hasTrack {
		switch {
		case hasSearch && hasWindow && hasReader:
			return &routedSearchWindowTrackReaderSessionService{routedSearchWindowTrackSessionService: &routedSearchWindowTrackSessionService{routedSessionService: base}}
		case hasSearch && hasWindow:
			return &routedSearchWindowTrackSessionService{routedSessionService: base}
		case hasSearch && hasReader:
			return &routedSearchTrackReaderSessionService{routedSearchTrackSessionService: &routedSearchTrackSessionService{routedSessionService: base}}
		case hasSearch:
			return &routedSearchTrackSessionService{routedSessionService: base}
		case hasWindow && hasReader:
			return &routedWindowTrackReaderSessionService{routedWindowTrackSessionService: &routedWindowTrackSessionService{routedSessionService: base}}
		case hasWindow:
			return &routedWindowTrackSessionService{routedSessionService: base}
		case hasReader:
			return &routedTrackReaderSessionService{routedTrackSessionService: &routedTrackSessionService{routedSessionService: base}}
		default:
			return &routedTrackSessionService{routedSessionService: base}
		}
	}
	switch {
	case hasSearch && hasWindow:
		return &routedSearchWindowSessionService{routedSessionService: base}
	case hasSearch:
		return &routedSearchSessionService{routedSessionService: base}
	case hasWindow:
		return &routedWindowSessionService{routedSessionService: base}
	default:
		return base
	}
}

func routedWritersSupportTrack(primary session.Service, replicas []session.Service) bool {
	if primary == nil {
		return false
	}
	if _, ok := primary.(session.TrackService); !ok {
		return false
	}
	for _, replica := range replicas {
		if _, ok := replica.(session.TrackService); !ok {
			return false
		}
	}
	return true
}

func (s *routedSessionService) CreateSession(ctx context.Context, key session.Key, state session.StateMap, options ...session.Option) (*session.Session, error) {
	var created *session.Session
	err := s.mutate(ctx, s.sessionRepair(key), func(writer session.Service) error {
		var err error
		created, err = writer.CreateSession(ctx, key, cloneSessionState(state), options...)
		return err
	}, func(writer session.Service) error {
		_, err := writer.CreateSession(ctx, key, cloneSessionState(state), options...)
		return err
	})
	return created, err
}

func (s *routedSessionService) DeleteSession(ctx context.Context, key session.Key, options ...session.Option) error {
	return s.mutateSame(ctx, s.sessionRepair(key), func(writer session.Service) error {
		return writer.DeleteSession(ctx, key, options...)
	})
}

func (s *routedSessionService) UpdateAppState(ctx context.Context, appName string, state session.StateMap) error {
	return s.mutateSame(ctx, s.applicationRepair(appName), func(writer session.Service) error {
		return writer.UpdateAppState(ctx, appName, cloneSessionState(state))
	})
}

func (s *routedSessionService) DeleteAppState(ctx context.Context, appName, key string) error {
	return s.mutateSame(ctx, s.applicationRepair(appName), func(writer session.Service) error {
		return writer.DeleteAppState(ctx, appName, key)
	})
}

func (s *routedSessionService) UpdateUserState(ctx context.Context, key session.UserKey, state session.StateMap) error {
	return s.mutateSame(ctx, s.userRepair(key), func(writer session.Service) error {
		return writer.UpdateUserState(ctx, key, cloneSessionState(state))
	})
}

func (s *routedSessionService) DeleteUserState(ctx context.Context, key session.UserKey, stateKey string) error {
	return s.mutateSame(ctx, s.userRepair(key), func(writer session.Service) error {
		return writer.DeleteUserState(ctx, key, stateKey)
	})
}

func (s *routedSessionService) UpdateSessionState(ctx context.Context, key session.Key, state session.StateMap) error {
	return s.mutateSame(ctx, s.sessionRepair(key), func(writer session.Service) error {
		return writer.UpdateSessionState(ctx, key, cloneSessionState(state))
	})
}

func (s *routedSessionService) AppendEvent(ctx context.Context, current *session.Session, source *event.Event, options ...session.Option) error {
	key := session.Key{AppName: current.AppName, UserID: current.UserID, SessionID: current.ID}
	return s.mutateSame(ctx, s.sessionRepair(key), func(writer session.Service) error {
		currentCopy := current.Clone()
		eventCopy := *source
		return writer.AppendEvent(ctx, currentCopy, &eventCopy, options...)
	})
}

// Summaries are derived from authoritative events. They intentionally stay on
// the current primary and are rebuilt naturally after migration cutover.
func (s *routedSessionService) CreateSessionSummary(ctx context.Context, current *session.Session, filterKey string, force bool) error {
	if s.primary == nil {
		return errors.New("Session migration route has no primary writer")
	}
	return s.primary.CreateSessionSummary(ctx, current.Clone(), filterKey, force)
}

func (s *routedSessionService) EnqueueSummaryJob(ctx context.Context, current *session.Session, filterKey string, force bool) error {
	if s.primary == nil {
		return errors.New("Session migration route has no primary writer")
	}
	return s.primary.EnqueueSummaryJob(ctx, current.Clone(), filterKey, force)
}

func (s *routedSessionService) Close() error { return nil }

func (s *routedSessionService) mutateSame(ctx context.Context, repair platformstorage.SessionMigrationRepair, operation func(session.Service) error) error {
	return s.mutate(ctx, repair, operation, operation)
}

func (s *routedSessionService) mutate(
	ctx context.Context,
	repair platformstorage.SessionMigrationRepair,
	primaryOperation func(session.Service) error,
	replicaOperation func(session.Service) error,
) error {
	if s.primary == nil {
		return errors.New("Session migration route has no primary writer")
	}
	if len(s.replicas) == 0 {
		return primaryOperation(s.primary)
	}
	if s.repairs == nil {
		return errors.New("Session migration repair store is required for replica writes")
	}
	dirty, err := s.repairs.UpsertSessionMigrationRepair(ctx, repair)
	if err != nil {
		return fmt.Errorf("record Session migration repair intent: %w", err)
	}
	if err := primaryOperation(s.primary); err != nil {
		return err
	}
	for _, replica := range s.replicas {
		if err := replicaOperation(replica); err != nil {
			// The primary is authoritative and has already committed. The durable
			// repair intent remains for convergence; the user request succeeds.
			return nil
		}
	}
	// Failure to clear the journal must not turn an already committed user
	// mutation into a retry. The repair worker will safely reconverge it.
	_, _ = s.repairs.CompleteSessionMigrationRepair(ctx, dirty, "")
	return nil
}

func (s *routedSessionService) baseRepair(scope, scopeKey, subjectID string) platformstorage.SessionMigrationRepair {
	replicaProfileID := ""
	if len(s.route.ReplicaWriters) > 0 {
		replicaProfileID = s.route.ReplicaWriters[0].ProfileID
	}
	return platformstorage.SessionMigrationRepair{
		MigrationID:      s.route.MigrationID,
		TenantID:         s.route.TenantID,
		AppCode:          s.route.AppCode,
		Scope:            scope,
		ScopeKey:         scopeKey,
		SubjectID:        subjectID,
		RouteGeneration:  s.route.Generation,
		PrimaryProfileID: s.route.PrimaryWriter.ProfileID,
		ReplicaProfileID: replicaProfileID,
	}
}

func (s *routedSessionService) sessionRepair(key session.Key) platformstorage.SessionMigrationRepair {
	return s.baseRepair(platformstorage.SessionRepairScopeSession, key.SessionID, key.UserID)
}

func (s *routedSessionService) userRepair(key session.UserKey) platformstorage.SessionMigrationRepair {
	return s.baseRepair(platformstorage.SessionRepairScopeUser, key.UserID, key.UserID)
}

func (s *routedSessionService) applicationRepair(appName string) platformstorage.SessionMigrationRepair {
	return s.baseRepair(platformstorage.SessionRepairScopeApplication, appName, "")
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
