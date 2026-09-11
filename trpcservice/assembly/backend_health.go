package assembly

import (
	"context"

	"github.com/liuzengh/trpc-agent-service/trpcservice/backendhealth"
	platformstorage "github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/memory"
	"trpc.group/trpc-go/trpc-agent-go/session"
	agenttool "trpc.group/trpc-go/trpc-agent-go/tool"
)

type healthSessionService struct {
	session.Service
	registry *backendhealth.Registry
	key      backendhealth.Key
}

func observeSessionService(service session.Service, registry *backendhealth.Registry, key backendhealth.Key) session.Service {
	if service == nil || registry == nil || !backendhealth.ShouldProtect(key.Driver) {
		return service
	}
	base := &healthSessionService{Service: service, registry: registry, key: key}
	return wrapHealthSessionCapabilities(base, service)
}

func (s *healthSessionService) CreateSession(ctx context.Context, key session.Key, state session.StateMap, options ...session.Option) (*session.Session, error) {
	return backendhealth.Value(ctx, s.registry, s.key, "create_session", func(ctx context.Context) (*session.Session, error) {
		return s.Service.CreateSession(ctx, key, state, options...)
	})
}

func (s *healthSessionService) GetSession(ctx context.Context, key session.Key, options ...session.Option) (*session.Session, error) {
	return backendhealth.Value(ctx, s.registry, s.key, "get_session", func(ctx context.Context) (*session.Session, error) {
		return s.Service.GetSession(ctx, key, options...)
	})
}

func (s *healthSessionService) ListSessions(ctx context.Context, key session.UserKey, options ...session.Option) ([]*session.Session, error) {
	return backendhealth.Value(ctx, s.registry, s.key, "list_sessions", func(ctx context.Context) ([]*session.Session, error) {
		return s.Service.ListSessions(ctx, key, options...)
	})
}

func (s *healthSessionService) DeleteSession(ctx context.Context, key session.Key, options ...session.Option) error {
	return backendhealth.Do(ctx, s.registry, s.key, "delete_session", func(ctx context.Context) error {
		return s.Service.DeleteSession(ctx, key, options...)
	})
}

func (s *healthSessionService) UpdateAppState(ctx context.Context, appName string, state session.StateMap) error {
	return backendhealth.Do(ctx, s.registry, s.key, "update_app_state", func(ctx context.Context) error {
		return s.Service.UpdateAppState(ctx, appName, state)
	})
}

func (s *healthSessionService) DeleteAppState(ctx context.Context, appName, key string) error {
	return backendhealth.Do(ctx, s.registry, s.key, "delete_app_state", func(ctx context.Context) error {
		return s.Service.DeleteAppState(ctx, appName, key)
	})
}

func (s *healthSessionService) ListAppStates(ctx context.Context, appName string) (session.StateMap, error) {
	return backendhealth.Value(ctx, s.registry, s.key, "list_app_states", func(ctx context.Context) (session.StateMap, error) {
		return s.Service.ListAppStates(ctx, appName)
	})
}

func (s *healthSessionService) UpdateUserState(ctx context.Context, key session.UserKey, state session.StateMap) error {
	return backendhealth.Do(ctx, s.registry, s.key, "update_user_state", func(ctx context.Context) error {
		return s.Service.UpdateUserState(ctx, key, state)
	})
}

func (s *healthSessionService) ListUserStates(ctx context.Context, key session.UserKey) (session.StateMap, error) {
	return backendhealth.Value(ctx, s.registry, s.key, "list_user_states", func(ctx context.Context) (session.StateMap, error) {
		return s.Service.ListUserStates(ctx, key)
	})
}

func (s *healthSessionService) DeleteUserState(ctx context.Context, key session.UserKey, stateKey string) error {
	return backendhealth.Do(ctx, s.registry, s.key, "delete_user_state", func(ctx context.Context) error {
		return s.Service.DeleteUserState(ctx, key, stateKey)
	})
}

func (s *healthSessionService) UpdateSessionState(ctx context.Context, key session.Key, state session.StateMap) error {
	return backendhealth.Do(ctx, s.registry, s.key, "update_session_state", func(ctx context.Context) error {
		return s.Service.UpdateSessionState(ctx, key, state)
	})
}

func (s *healthSessionService) AppendEvent(ctx context.Context, current *session.Session, source *event.Event, options ...session.Option) error {
	return backendhealth.Do(ctx, s.registry, s.key, "append_event", func(ctx context.Context) error {
		return s.Service.AppendEvent(ctx, current, source, options...)
	})
}

func (s *healthSessionService) CreateSessionSummary(ctx context.Context, current *session.Session, filterKey string, force bool) error {
	return backendhealth.Do(ctx, s.registry, s.key, "create_summary", func(ctx context.Context) error {
		return s.Service.CreateSessionSummary(ctx, current, filterKey, force)
	})
}

func (s *healthSessionService) EnqueueSummaryJob(ctx context.Context, current *session.Session, filterKey string, force bool) error {
	return backendhealth.Do(ctx, s.registry, s.key, "enqueue_summary", func(ctx context.Context) error {
		return s.Service.EnqueueSummaryJob(ctx, current, filterKey, force)
	})
}

func (s *healthSessionService) searchEvents(ctx context.Context, searchable session.SearchableService, request session.EventSearchRequest) ([]session.EventSearchResult, error) {
	return backendhealth.Value(ctx, s.registry, s.key, "search_events", func(ctx context.Context) ([]session.EventSearchResult, error) {
		return searchable.SearchEvents(ctx, request)
	})
}

func (s *healthSessionService) getEventWindow(ctx context.Context, window session.WindowService, request session.EventWindowRequest) (*session.EventWindow, error) {
	return backendhealth.Value(ctx, s.registry, s.key, "get_event_window", func(ctx context.Context) (*session.EventWindow, error) {
		return window.GetEventWindow(ctx, request)
	})
}

func (s *healthSessionService) appendTrackEvent(ctx context.Context, track session.TrackService, current *session.Session, trackEvent *session.TrackEvent, options ...session.Option) error {
	return backendhealth.Do(ctx, s.registry, s.key, "append_track_event", func(ctx context.Context) error {
		return track.AppendTrackEvent(ctx, current, trackEvent, options...)
	})
}

type sessionTrackEventReader interface {
	GetTrackEvents(context.Context, session.Key, session.Track, ...session.Option) (*session.TrackEvents, error)
}

func (s *healthSessionService) getTrackEvents(ctx context.Context, reader sessionTrackEventReader, key session.Key, track session.Track, options ...session.Option) (*session.TrackEvents, error) {
	return backendhealth.Value(ctx, s.registry, s.key, "get_track_events", func(ctx context.Context) (*session.TrackEvents, error) {
		return reader.GetTrackEvents(ctx, key, track, options...)
	})
}

type healthSearchSessionService struct {
	*healthSessionService
	searchable session.SearchableService
}

func (s *healthSearchSessionService) SearchEvents(ctx context.Context, request session.EventSearchRequest) ([]session.EventSearchResult, error) {
	return s.searchEvents(ctx, s.searchable, request)
}

type healthWindowSessionService struct {
	*healthSessionService
	window session.WindowService
}

func (s *healthWindowSessionService) GetEventWindow(ctx context.Context, request session.EventWindowRequest) (*session.EventWindow, error) {
	return s.getEventWindow(ctx, s.window, request)
}

type healthTrackSessionService struct {
	*healthSessionService
	track session.TrackService
}

func (s *healthTrackSessionService) AppendTrackEvent(ctx context.Context, current *session.Session, trackEvent *session.TrackEvent, options ...session.Option) error {
	return s.appendTrackEvent(ctx, s.track, current, trackEvent, options...)
}

type healthSearchWindowSessionService struct {
	*healthSessionService
	searchable session.SearchableService
	window     session.WindowService
}

func (s *healthSearchWindowSessionService) SearchEvents(ctx context.Context, request session.EventSearchRequest) ([]session.EventSearchResult, error) {
	return s.searchEvents(ctx, s.searchable, request)
}

func (s *healthSearchWindowSessionService) GetEventWindow(ctx context.Context, request session.EventWindowRequest) (*session.EventWindow, error) {
	return s.getEventWindow(ctx, s.window, request)
}

type healthSearchTrackSessionService struct {
	*healthSessionService
	searchable session.SearchableService
	track      session.TrackService
}

func (s *healthSearchTrackSessionService) SearchEvents(ctx context.Context, request session.EventSearchRequest) ([]session.EventSearchResult, error) {
	return s.searchEvents(ctx, s.searchable, request)
}

func (s *healthSearchTrackSessionService) AppendTrackEvent(ctx context.Context, current *session.Session, trackEvent *session.TrackEvent, options ...session.Option) error {
	return s.appendTrackEvent(ctx, s.track, current, trackEvent, options...)
}

type healthWindowTrackSessionService struct {
	*healthSessionService
	window session.WindowService
	track  session.TrackService
}

func (s *healthWindowTrackSessionService) GetEventWindow(ctx context.Context, request session.EventWindowRequest) (*session.EventWindow, error) {
	return s.getEventWindow(ctx, s.window, request)
}

func (s *healthWindowTrackSessionService) AppendTrackEvent(ctx context.Context, current *session.Session, trackEvent *session.TrackEvent, options ...session.Option) error {
	return s.appendTrackEvent(ctx, s.track, current, trackEvent, options...)
}

type healthSearchWindowTrackSessionService struct {
	*healthSessionService
	searchable session.SearchableService
	window     session.WindowService
	track      session.TrackService
}

func (s *healthSearchWindowTrackSessionService) SearchEvents(ctx context.Context, request session.EventSearchRequest) ([]session.EventSearchResult, error) {
	return s.searchEvents(ctx, s.searchable, request)
}

func (s *healthSearchWindowTrackSessionService) GetEventWindow(ctx context.Context, request session.EventWindowRequest) (*session.EventWindow, error) {
	return s.getEventWindow(ctx, s.window, request)
}

func (s *healthSearchWindowTrackSessionService) AppendTrackEvent(ctx context.Context, current *session.Session, trackEvent *session.TrackEvent, options ...session.Option) error {
	return s.appendTrackEvent(ctx, s.track, current, trackEvent, options...)
}

type healthTrackReaderSessionService struct {
	*healthTrackSessionService
	reader sessionTrackEventReader
}

func (s *healthTrackReaderSessionService) GetTrackEvents(ctx context.Context, key session.Key, track session.Track, options ...session.Option) (*session.TrackEvents, error) {
	return s.getTrackEvents(ctx, s.reader, key, track, options...)
}

type healthSearchTrackReaderSessionService struct {
	*healthSearchTrackSessionService
	reader sessionTrackEventReader
}

func (s *healthSearchTrackReaderSessionService) GetTrackEvents(ctx context.Context, key session.Key, track session.Track, options ...session.Option) (*session.TrackEvents, error) {
	return s.getTrackEvents(ctx, s.reader, key, track, options...)
}

type healthWindowTrackReaderSessionService struct {
	*healthWindowTrackSessionService
	reader sessionTrackEventReader
}

func (s *healthWindowTrackReaderSessionService) GetTrackEvents(ctx context.Context, key session.Key, track session.Track, options ...session.Option) (*session.TrackEvents, error) {
	return s.getTrackEvents(ctx, s.reader, key, track, options...)
}

type healthSearchWindowTrackReaderSessionService struct {
	*healthSearchWindowTrackSessionService
	reader sessionTrackEventReader
}

func (s *healthSearchWindowTrackReaderSessionService) GetTrackEvents(ctx context.Context, key session.Key, track session.Track, options ...session.Option) (*session.TrackEvents, error) {
	return s.getTrackEvents(ctx, s.reader, key, track, options...)
}

func wrapHealthSessionCapabilities(base *healthSessionService, inner session.Service) session.Service {
	searchable, hasSearch := inner.(session.SearchableService)
	window, hasWindow := inner.(session.WindowService)
	track, hasTrack := inner.(session.TrackService)
	reader, hasReader := inner.(sessionTrackEventReader)
	if hasTrack {
		return wrapHealthTrackCapabilities(base, searchable, hasSearch, window, hasWindow, track, reader, hasReader)
	}
	switch {
	case hasSearch && hasWindow:
		return &healthSearchWindowSessionService{healthSessionService: base, searchable: searchable, window: window}
	case hasSearch:
		return &healthSearchSessionService{healthSessionService: base, searchable: searchable}
	case hasWindow:
		return &healthWindowSessionService{healthSessionService: base, window: window}
	default:
		return base
	}
}

func wrapHealthTrackCapabilities(
	base *healthSessionService,
	searchable session.SearchableService,
	hasSearch bool,
	window session.WindowService,
	hasWindow bool,
	track session.TrackService,
	reader sessionTrackEventReader,
	hasReader bool,
) session.Service {
	switch {
	case hasSearch && hasWindow && hasReader:
		return &healthSearchWindowTrackReaderSessionService{
			healthSearchWindowTrackSessionService: &healthSearchWindowTrackSessionService{healthSessionService: base, searchable: searchable, window: window, track: track}, reader: reader,
		}
	case hasSearch && hasWindow:
		return &healthSearchWindowTrackSessionService{healthSessionService: base, searchable: searchable, window: window, track: track}
	case hasSearch && hasReader:
		return &healthSearchTrackReaderSessionService{
			healthSearchTrackSessionService: &healthSearchTrackSessionService{healthSessionService: base, searchable: searchable, track: track}, reader: reader,
		}
	case hasSearch:
		return &healthSearchTrackSessionService{healthSessionService: base, searchable: searchable, track: track}
	case hasWindow && hasReader:
		return &healthWindowTrackReaderSessionService{
			healthWindowTrackSessionService: &healthWindowTrackSessionService{healthSessionService: base, window: window, track: track}, reader: reader,
		}
	case hasWindow:
		return &healthWindowTrackSessionService{healthSessionService: base, window: window, track: track}
	case hasReader:
		return &healthTrackReaderSessionService{
			healthTrackSessionService: &healthTrackSessionService{healthSessionService: base, track: track}, reader: reader,
		}
	default:
		return &healthTrackSessionService{healthSessionService: base, track: track}
	}
}

type healthMemoryReader struct {
	memory.Reader
	registry *backendhealth.Registry
	key      backendhealth.Key
}

func (r *healthMemoryReader) ReadMemories(ctx context.Context, key memory.UserKey, limit int) ([]*memory.Entry, error) {
	return backendhealth.Value(ctx, r.registry, r.key, "read_memories", func(ctx context.Context) ([]*memory.Entry, error) {
		return r.Reader.ReadMemories(ctx, key, limit)
	})
}

func (r *healthMemoryReader) SearchMemories(ctx context.Context, key memory.UserKey, query string, options ...memory.SearchOption) ([]*memory.Entry, error) {
	return backendhealth.Value(ctx, r.registry, r.key, "search_memories", func(ctx context.Context) ([]*memory.Entry, error) {
		return r.Reader.SearchMemories(ctx, key, query, options...)
	})
}

type healthMemoryService struct {
	memory.Service
	registry *backendhealth.Registry
	key      backendhealth.Key
	tools    []agenttool.Tool
}

func observeMemoryService(service memory.Service, registry *backendhealth.Registry, key backendhealth.Key) memory.Service {
	if service == nil || registry == nil || !backendhealth.ShouldProtect(key.Driver) {
		return service
	}
	wrapped := &healthMemoryService{Service: service, registry: registry, key: key}
	wrapped.tools = observeTools(service.Tools(), registry, key)
	return wrapped
}

func (s *healthMemoryService) ReadMemories(ctx context.Context, key memory.UserKey, limit int) ([]*memory.Entry, error) {
	return backendhealth.Value(ctx, s.registry, s.key, "read_memories", func(ctx context.Context) ([]*memory.Entry, error) {
		return s.Service.ReadMemories(ctx, key, limit)
	})
}

func (s *healthMemoryService) SearchMemories(ctx context.Context, key memory.UserKey, query string, options ...memory.SearchOption) ([]*memory.Entry, error) {
	return backendhealth.Value(ctx, s.registry, s.key, "search_memories", func(ctx context.Context) ([]*memory.Entry, error) {
		return s.Service.SearchMemories(ctx, key, query, options...)
	})
}

func (s *healthMemoryService) AddMemory(ctx context.Context, key memory.UserKey, value string, topics []string, options ...memory.AddOption) error {
	return backendhealth.Do(ctx, s.registry, s.key, "add_memory", func(ctx context.Context) error {
		return s.Service.AddMemory(ctx, key, value, topics, options...)
	})
}

func (s *healthMemoryService) UpdateMemory(ctx context.Context, key memory.Key, value string, topics []string, options ...memory.UpdateOption) error {
	return backendhealth.Do(ctx, s.registry, s.key, "update_memory", func(ctx context.Context) error {
		return s.Service.UpdateMemory(ctx, key, value, topics, options...)
	})
}

func (s *healthMemoryService) DeleteMemory(ctx context.Context, key memory.Key) error {
	return backendhealth.Do(ctx, s.registry, s.key, "delete_memory", func(ctx context.Context) error {
		return s.Service.DeleteMemory(ctx, key)
	})
}

func (s *healthMemoryService) ClearMemories(ctx context.Context, key memory.UserKey) error {
	return backendhealth.Do(ctx, s.registry, s.key, "clear_memories", func(ctx context.Context) error {
		return s.Service.ClearMemories(ctx, key)
	})
}

func (s *healthMemoryService) Tools() []agenttool.Tool {
	return append([]agenttool.Tool(nil), s.tools...)
}

func (s *healthMemoryService) EnqueueAutoMemoryJob(ctx context.Context, current *session.Session) error {
	return backendhealth.Do(ctx, s.registry, s.key, "enqueue_auto_memory", func(ctx context.Context) error {
		return s.Service.EnqueueAutoMemoryJob(ctx, current)
	})
}

type healthIngestor struct {
	delegate session.Ingestor
	registry *backendhealth.Registry
	key      backendhealth.Key
}

func (i *healthIngestor) IngestSession(ctx context.Context, current *session.Session, options ...session.IngestOption) error {
	return backendhealth.Do(ctx, i.registry, i.key, "ingest_session", func(ctx context.Context) error {
		return i.delegate.IngestSession(ctx, current, options...)
	})
}

type healthCallableTool struct {
	agenttool.CallableTool
	registry *backendhealth.Registry
	key      backendhealth.Key
}

func (t *healthCallableTool) Call(ctx context.Context, arguments []byte) (any, error) {
	name := "memory_tool"
	if declaration := t.Declaration(); declaration != nil && declaration.Name != "" {
		name = declaration.Name
	}
	return backendhealth.Value(ctx, t.registry, t.key, name, func(ctx context.Context) (any, error) {
		return t.CallableTool.Call(ctx, arguments)
	})
}

func observeTools(tools []agenttool.Tool, registry *backendhealth.Registry, key backendhealth.Key) []agenttool.Tool {
	result := make([]agenttool.Tool, 0, len(tools))
	for _, candidate := range tools {
		if callable, ok := candidate.(agenttool.CallableTool); ok {
			result = append(result, &healthCallableTool{CallableTool: callable, registry: registry, key: key})
			continue
		}
		result = append(result, candidate)
	}
	return result
}

func sessionHealthKey(backend platformstorage.SessionBackendRef) backendhealth.Key {
	return backendhealth.Key{ProfileID: backend.ProfileID, Domain: platformstorage.BackendDomainSession, Driver: backend.Driver}
}

func memoryHealthKey(profileID, driver string) backendhealth.Key {
	return backendhealth.Key{ProfileID: profileID, Domain: platformstorage.BackendDomainMemory, Driver: driver}
}

func observeMemoryBackend(backend MemoryBackend, registry *backendhealth.Registry, key backendhealth.Key) MemoryBackend {
	if registry == nil || !backendhealth.ShouldProtect(key.Driver) {
		return backend
	}
	if backend.Service != nil {
		backend.Service = observeMemoryService(backend.Service, registry, key)
		backend.Tools = backend.Service.Tools()
	} else {
		backend.Tools = observeTools(backend.Tools, registry, key)
	}
	if backend.Ingestor != nil {
		backend.Ingestor = &healthIngestor{delegate: backend.Ingestor, registry: registry, key: key}
	}
	return backend
}

var _ memory.Service = (*healthMemoryService)(nil)
var _ session.Ingestor = (*healthIngestor)(nil)
