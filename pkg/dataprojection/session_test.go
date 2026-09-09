package dataprojection

import (
	"context"
	"errors"
	"testing"
	"time"

	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/session"
	sessioninmemory "trpc.group/trpc-go/trpc-agent-go/session/inmemory"
)

func TestSessionProjectorCopiesRealServiceTranscriptStateAndTracksIdempotently(t *testing.T) {
	ctx := context.Background()
	source := sessioninmemory.NewSessionService()
	target := sessioninmemory.NewSessionService()
	t.Cleanup(func() { _ = source.Close() })
	t.Cleanup(func() { _ = target.Close() })
	key := session.Key{AppName: "tsa1:8:tenant-a:support", UserID: "owner-1", SessionID: "session-1"}
	value, err := source.CreateSession(ctx, key, session.StateMap{"case_status": []byte("open")})
	if err != nil {
		t.Fatal(err)
	}
	base := time.Date(2026, time.September, 4, 2, 0, 0, 0, time.UTC)
	for index, content := range []string{"hello", "welcome"} {
		role := model.RoleUser
		if index == 1 {
			role = model.RoleAssistant
		}
		if err := source.AppendEvent(ctx, value, &event.Event{
			ID:        "event-" + string(rune('1'+index)),
			Timestamp: base.Add(time.Duration(index) * time.Second),
			Response:  &model.Response{Choices: []model.Choice{{Message: model.Message{Role: role, Content: content}}}},
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := source.AppendTrackEvent(ctx, value, &session.TrackEvent{
		Track: "protocol", Payload: []byte(`{"step":"received"}`), Timestamp: base,
	}); err != nil {
		t.Fatal(err)
	}
	record, err := NewSessionRecord(ctx, source, key, 11, 100)
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	projector, err := NewSessionProjector(func(_ context.Context, tenantID, appName string) (session.Service, error) {
		if tenantID != "tenant-a" || appName != key.AppName {
			t.Fatalf("resolver scope = %q/%q", tenantID, appName)
		}
		return target, nil
	}, 100)
	if err != nil {
		t.Fatal(err)
	}
	for run := 0; run < 2; run++ {
		if err := projector.Apply(ctx, "tenant-a", record); err != nil {
			t.Fatalf("apply run %d: %v", run+1, err)
		}
	}
	got, err := target.GetSession(ctx, key, session.WithEventNum(100))
	if err != nil {
		t.Fatal(err)
	}
	if got == nil || len(got.Events) != 2 || string(got.State["case_status"]) != "open" {
		t.Fatalf("projected session = %#v", got)
	}
	tracked, err := target.GetTrackEvents(ctx, key, "protocol", session.WithEventNum(100))
	if err != nil {
		t.Fatal(err)
	}
	if tracked == nil || len(tracked.Events) != 1 {
		t.Fatalf("projected tracks = %#v", tracked)
	}
}

func TestSessionProjectorAppendsOnlySourceSuffixAndRejectsDivergence(t *testing.T) {
	ctx := context.Background()
	key := session.Key{AppName: "tsa1:8:tenant-a:support", UserID: "owner-1", SessionID: "session-1"}
	newService := func(first string) (*sessioninmemory.SessionService, *session.Session) {
		service := sessioninmemory.NewSessionService()
		value, err := service.CreateSession(ctx, key, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := service.AppendEvent(ctx, value, &event.Event{ID: "event-1", Response: &model.Response{
			Choices: []model.Choice{{Message: model.Message{Role: model.RoleUser, Content: first}}},
		}}); err != nil {
			t.Fatal(err)
		}
		return service, value
	}
	source, sourceValue := newService("same")
	target, _ := newService("same")
	t.Cleanup(func() { _ = source.Close() })
	t.Cleanup(func() { _ = target.Close() })
	if err := source.AppendEvent(ctx, sourceValue, &event.Event{ID: "event-2", Response: &model.Response{
		Choices: []model.Choice{{Message: model.Message{Role: model.RoleAssistant, Content: "suffix"}}},
	}}); err != nil {
		t.Fatal(err)
	}
	record, err := NewSessionRecord(ctx, source, key, 12, 100)
	if err != nil {
		t.Fatal(err)
	}
	projector, _ := NewSessionProjector(func(context.Context, string, string) (session.Service, error) { return target, nil }, 100)
	if err := projector.Apply(ctx, "tenant-a", record); err != nil {
		t.Fatalf("append suffix: %v", err)
	}
	got, _ := target.GetSession(ctx, key, session.WithEventNum(100))
	if len(got.Events) != 2 {
		t.Fatalf("events=%d, want 2", len(got.Events))
	}

	diverged, _ := newService("different")
	t.Cleanup(func() { _ = diverged.Close() })
	projector, _ = NewSessionProjector(func(context.Context, string, string) (session.Service, error) { return diverged, nil }, 100)
	if err := projector.Apply(ctx, "tenant-a", record); !errors.Is(err, ErrInvalidProjection) {
		t.Fatalf("divergence error=%v, want ErrInvalidProjection", err)
	}
}

func TestNewSessionRecordFailsClosedOnPossibleTruncationAndNativeSummary(t *testing.T) {
	ctx := context.Background()
	service := sessioninmemory.NewSessionService()
	t.Cleanup(func() { _ = service.Close() })
	key := session.Key{AppName: "tsa1:8:tenant-a:support", UserID: "owner-1", SessionID: "session-1"}
	value, err := service.CreateSession(ctx, key, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := service.AppendEvent(ctx, value, &event.Event{ID: "event-1", Response: &model.Response{
		Choices: []model.Choice{{Message: model.Message{Role: model.RoleUser, Content: "bounded"}}},
	}}); err != nil {
		t.Fatal(err)
	}
	if _, err := NewSessionRecord(ctx, service, key, 1, 1); !errors.Is(err, ErrInvalidProjection) {
		t.Fatalf("truncation error=%v, want ErrInvalidProjection", err)
	}
	summarySource := summaryInjectingSessionService{Service: service}
	if _, err := NewSessionRecord(ctx, summarySource, key, 1, 100); !errors.Is(err, ErrInvalidProjection) {
		t.Fatalf("native summary error=%v, want ErrInvalidProjection", err)
	}
}

type summaryInjectingSessionService struct{ session.Service }

func (s summaryInjectingSessionService) GetSession(ctx context.Context, key session.Key, options ...session.Option) (*session.Session, error) {
	value, err := s.Service.GetSession(ctx, key, options...)
	if err != nil || value == nil {
		return value, err
	}
	value.SummariesMu.Lock()
	value.Summaries = map[string]*session.Summary{"": {Summary: "native"}}
	value.SummariesMu.Unlock()
	return value, nil
}

func TestSessionTombstoneDeletesProjectedSession(t *testing.T) {
	ctx := context.Background()
	target := sessioninmemory.NewSessionService()
	t.Cleanup(func() { _ = target.Close() })
	key := session.Key{AppName: "tsa1:8:tenant-a:support", UserID: "owner-1", SessionID: "session-1"}
	if _, err := target.CreateSession(ctx, key, nil); err != nil {
		t.Fatal(err)
	}
	record, err := NewSessionTombstone(key, 15)
	if err != nil {
		t.Fatal(err)
	}
	projector, _ := NewSessionProjector(func(context.Context, string, string) (session.Service, error) { return target, nil }, 100)
	if err := projector.Apply(ctx, "tenant-a", record); err != nil {
		t.Fatal(err)
	}
	if got, err := target.GetSession(ctx, key); err != nil || got != nil {
		t.Fatalf("deleted session=%#v err=%v", got, err)
	}
}
