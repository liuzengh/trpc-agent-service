package assembly

import (
	"context"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/backendhealth"
	"trpc.group/trpc-go/trpc-agent-go/session"
	sessioninmemory "trpc.group/trpc-go/trpc-agent-go/session/inmemory"
)

type completeOptionalSessionService struct {
	session.Service
	searchCalls int
	windowCalls int
	trackCalls  int
	readCalls   int
}

func (s *completeOptionalSessionService) SearchEvents(context.Context, session.EventSearchRequest) ([]session.EventSearchResult, error) {
	s.searchCalls++
	return nil, nil
}

func (s *completeOptionalSessionService) GetEventWindow(_ context.Context, request session.EventWindowRequest) (*session.EventWindow, error) {
	s.windowCalls++
	return &session.EventWindow{SessionKey: request.Key, AnchorEventID: request.AnchorEventID}, nil
}

func (s *completeOptionalSessionService) AppendTrackEvent(context.Context, *session.Session, *session.TrackEvent, ...session.Option) error {
	s.trackCalls++
	return nil
}

func (s *completeOptionalSessionService) GetTrackEvents(context.Context, session.Key, session.Track, ...session.Option) (*session.TrackEvents, error) {
	s.readCalls++
	return &session.TrackEvents{}, nil
}

func TestBackendHealthSessionWrapperPreservesOnlyUnderlyingOptionalCapabilities(t *testing.T) {
	registry, err := backendhealth.NewRegistry(backendhealth.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	key := backendhealth.Key{ProfileID: "session-postgres", Domain: "session", Driver: "postgres"}
	base := sessioninmemory.NewSessionService()
	t.Cleanup(func() { _ = base.Close() })

	plain := observeSessionService(struct{ session.Service }{Service: base}, registry, key)
	if _, ok := plain.(session.SearchableService); ok {
		t.Fatal("health wrapper invented SearchableService")
	}
	if _, ok := plain.(session.WindowService); ok {
		t.Fatal("health wrapper invented WindowService")
	}
	if _, ok := plain.(session.TrackService); ok {
		t.Fatal("health wrapper invented TrackService")
	}
	if _, ok := plain.(sessionTrackEventReader); ok {
		t.Fatal("health wrapper invented track reader")
	}

	complete := &completeOptionalSessionService{Service: base}
	wrapped := observeSessionService(complete, registry, key)
	searchable, searchOK := wrapped.(session.SearchableService)
	window, windowOK := wrapped.(session.WindowService)
	track, trackOK := wrapped.(session.TrackService)
	reader, readerOK := wrapped.(sessionTrackEventReader)
	if !searchOK || !windowOK || !trackOK || !readerOK {
		t.Fatalf("health wrapper lost optional capabilities: search=%v window=%v track=%v reader=%v", searchOK, windowOK, trackOK, readerOK)
	}
	ctx := context.Background()
	if _, err := searchable.SearchEvents(ctx, session.EventSearchRequest{}); err != nil {
		t.Fatal(err)
	}
	if _, err := window.GetEventWindow(ctx, session.EventWindowRequest{}); err != nil {
		t.Fatal(err)
	}
	if err := track.AppendTrackEvent(ctx, &session.Session{}, &session.TrackEvent{}); err != nil {
		t.Fatal(err)
	}
	if _, err := reader.GetTrackEvents(ctx, session.Key{}, "trace"); err != nil {
		t.Fatal(err)
	}
	if complete.searchCalls != 1 || complete.windowCalls != 1 || complete.trackCalls != 1 || complete.readCalls != 1 {
		t.Fatalf("optional calls = search:%d window:%d track:%d read:%d", complete.searchCalls, complete.windowCalls, complete.trackCalls, complete.readCalls)
	}
}
