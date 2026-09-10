package assembly

import (
	"context"
	"testing"

	"trpc.group/trpc-go/trpc-agent-go/session"
	sessioninmemory "trpc.group/trpc-go/trpc-agent-go/session/inmemory"
)

type windowReaderSessionService struct {
	session.Service
	called bool
}

func (s *windowReaderSessionService) GetEventWindow(_ context.Context, request session.EventWindowRequest) (*session.EventWindow, error) {
	s.called = true
	return &session.EventWindow{SessionKey: request.Key, AnchorEventID: request.AnchorEventID}, nil
}

func TestRoutedSessionServicePreservesReaderWindowCapability(t *testing.T) {
	base := sessioninmemory.NewSessionService()
	t.Cleanup(func() { _ = base.Close() })
	reader := &windowReaderSessionService{Service: base}
	routed := newRoutedSessionService(reader, base)
	windowService, ok := routed.(session.WindowService)
	if !ok {
		t.Fatal("routed Session service hides reader WindowService capability")
	}
	request := session.EventWindowRequest{
		Key:           session.Key{AppName: "tenant/app", UserID: "user", SessionID: "session"},
		AnchorEventID: "event-1",
	}
	window, err := windowService.GetEventWindow(context.Background(), request)
	if err != nil {
		t.Fatalf("GetEventWindow() error = %v", err)
	}
	if !reader.called || window == nil || window.AnchorEventID != "event-1" {
		t.Fatalf("window = %#v, reader called = %v", window, reader.called)
	}
}
