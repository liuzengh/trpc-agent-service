package summary

import (
	"context"
	"errors"
	"testing"
	"time"

	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/storage"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/session"
	sessioninmemory "trpc.group/trpc-go/trpc-agent-go/session/inmemory"
)

type overlayCheckpointReader struct {
	checkpoint Checkpoint
	found      bool
	err        error
	key        Key
}

func (r *overlayCheckpointReader) Get(_ context.Context, key Key) (Checkpoint, bool, error) {
	r.key = key
	return r.checkpoint, r.found, r.err
}

func TestCheckpointSessionServiceHydratesRunnerVisibleSummary(t *testing.T) {
	ctx := context.Background()
	inner := sessioninmemory.NewSessionService()
	appName := "tsa1:8:tenant-a:support"
	key := session.Key{AppName: appName, UserID: "owner-1", SessionID: "session-1"}
	incarnation := "00000000-0000-4000-8000-000000000001"
	value, err := inner.CreateSession(ctx, key, session.StateMap{storage.SessionIncarnationStateKey: []byte(incarnation)})
	if err != nil {
		t.Fatal(err)
	}
	firstAt := time.Date(2026, time.August, 30, 3, 0, 0, 0, time.UTC)
	for i, item := range []event.Event{
		{ID: "event-1", Timestamp: firstAt},
		{ID: "event-2", Timestamp: firstAt.Add(time.Second)},
	} {
		item := item
		if err := inner.AppendEvent(ctx, value, &item); err != nil {
			t.Fatalf("append event %d: %v", i, err)
		}
	}
	reader := &overlayCheckpointReader{found: true, checkpoint: Checkpoint{
		Key:           Key{TenantID: "tenant-a", AgentAppID: "app-1", SessionOwnerID: key.UserID, SessionID: key.SessionID, SessionIncarnationID: incarnation},
		EventSequence: 2, Content: "durable summary", ContentSHA256: HashContent("durable summary"),
		CutoffAt: firstAt.Add(time.Second), LastEventID: "event-2", UpdatedAt: firstAt.Add(2 * time.Second),
	}}
	service, err := NewCheckpointSessionService(inner, reader, "tenant-a", "app-1", appName)
	if err != nil {
		t.Fatal(err)
	}
	got, err := service.GetSession(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	summaryValue := got.Summaries[session.SummaryFilterKeyAllContents]
	if summaryValue == nil || summaryValue.Summary != "durable summary" || summaryValue.Boundary == nil ||
		summaryValue.Boundary.LastEventID != "event-2" || !summaryValue.Boundary.CutoffAt.Equal(firstAt.Add(time.Second)) {
		t.Fatalf("hydrated summary = %#v", summaryValue)
	}
	if reader.key != reader.checkpoint.Key {
		t.Fatalf("checkpoint lookup key = %#v, want %#v", reader.key, reader.checkpoint.Key)
	}
	text, found := service.GetSessionSummaryText(ctx, got)
	if !found || text != "durable summary" {
		t.Fatalf("summary text = %q found=%v", text, found)
	}
}

func TestCheckpointSessionServiceFailsClosedWhenCheckpointReadFails(t *testing.T) {
	ctx := context.Background()
	inner := sessioninmemory.NewSessionService()
	appName := "tsa1:8:tenant-a:support"
	key := session.Key{AppName: appName, UserID: "owner-1", SessionID: "session-1"}
	if _, err := inner.CreateSession(ctx, key, session.StateMap{storage.SessionIncarnationStateKey: []byte("00000000-0000-4000-8000-000000000001")}); err != nil {
		t.Fatal(err)
	}
	reader := &overlayCheckpointReader{err: errors.New("checkpoint unavailable")}
	service, err := NewCheckpointSessionService(inner, reader, "tenant-a", "app-1", appName)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.GetSession(ctx, key); !errors.Is(err, ErrSummaryReadUnavailable) {
		t.Fatalf("GetSession error = %v, want safe summary read failure", err)
	}
}

func TestCheckpointSessionServiceRejectsCrossAppLookupBeforeDelegate(t *testing.T) {
	service, err := NewCheckpointSessionService(
		sessioninmemory.NewSessionService(), &overlayCheckpointReader{},
		"tenant-a", "app-1", "tsa1:8:tenant-a:support",
	)
	if err != nil {
		t.Fatal(err)
	}
	_, err = service.GetSession(context.Background(), session.Key{
		AppName: "tsa1:8:tenant-a:billing", UserID: "owner-1", SessionID: "session-1",
	})
	if !errors.Is(err, ErrSummaryScope) {
		t.Fatalf("cross-app error = %v", err)
	}
}
