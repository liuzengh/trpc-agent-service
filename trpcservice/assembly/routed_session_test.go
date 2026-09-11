package assembly

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	platformstorage "github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/model"
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
	routed := newRoutedSessionService(reader, base, nil, platformstorage.SessionMigrationRoute{}, nil)
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

func TestRoutedSessionServicePreservesSearchAndMirrorsTrackCapability(t *testing.T) {
	base := sessioninmemory.NewSessionService()
	replicaBase := sessioninmemory.NewSessionService()
	t.Cleanup(func() { _ = base.Close(); _ = replicaBase.Close() })
	primary := &completeOptionalSessionService{Service: base}
	replica := &completeOptionalSessionService{Service: replicaBase}
	routed := newRoutedSessionService(
		primary, primary, []session.Service{replica},
		platformstorage.SessionMigrationRoute{MigrationID: "migration-1", TenantID: "tenant", AppCode: "support"},
		&memorySessionRepairStore{},
	)
	if _, ok := routed.(session.SearchableService); !ok {
		t.Fatal("routed service lost SearchableService")
	}
	track, ok := routed.(session.TrackService)
	if !ok {
		t.Fatal("routed service lost TrackService")
	}
	current := &session.Session{ID: "tenant/support/session/session-1", AppName: "tenant/support", UserID: "user-1", State: session.StateMap{}}
	if err := track.AppendTrackEvent(context.Background(), current, &session.TrackEvent{Track: "trace"}); err != nil {
		t.Fatal(err)
	}
	if primary.trackCalls != 1 || replica.trackCalls != 1 {
		t.Fatalf("track mirror calls = primary:%d replica:%d", primary.trackCalls, replica.trackCalls)
	}
}

func TestRoutedSessionServiceMirrorsMutationsToAllWriters(t *testing.T) {
	ctx := context.Background()
	first := sessioninmemory.NewSessionService()
	second := sessioninmemory.NewSessionService()
	third := sessioninmemory.NewSessionService()
	recorder := &recordingSessionWriter{Service: third}
	t.Cleanup(func() { _ = first.Close(); _ = second.Close(); _ = third.Close() })
	repairs := &memorySessionRepairStore{}
	route := platformstorage.SessionMigrationRoute{MigrationID: "migration-1", TenantID: "tenant", AppCode: "support"}
	routed := newRoutedSessionService(first, first, []session.Service{second, recorder}, route, repairs)
	key := session.Key{AppName: "tenant/support", UserID: "user-1", SessionID: "session-1"}
	originalState := session.StateMap{"mode": []byte("brief")}
	created, err := routed.CreateSession(ctx, key, originalState)
	if err != nil || created == nil {
		t.Fatalf("CreateSession() = %#v, %v", created, err)
	}
	// Mutating caller-owned state after the call must not mutate either writer.
	originalState["mode"][0] = 'X'
	for index, writer := range []session.Service{first, second, third} {
		got, err := writer.GetSession(ctx, key)
		if err != nil || got == nil || string(got.SnapshotState()["mode"]) != "brief" {
			t.Fatalf("writer %d session after create = %#v, %v", index, got, err)
		}
	}

	if err := routed.UpdateAppState(ctx, key.AppName, session.StateMap{"app": []byte("ready")}); err != nil {
		t.Fatal(err)
	}
	if err := routed.UpdateUserState(ctx, session.UserKey{AppName: key.AppName, UserID: key.UserID}, session.StateMap{"user": []byte("known")}); err != nil {
		t.Fatal(err)
	}
	if err := routed.UpdateSessionState(ctx, key, session.StateMap{"session": []byte("active")}); err != nil {
		t.Fatal(err)
	}
	current, err := first.GetSession(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	ev := event.NewResponseEvent("invocation-1", "assistant", &model.Response{
		Done: true, Choices: []model.Choice{{Message: model.NewAssistantMessage("ok")}},
	})
	if err := routed.AppendEvent(ctx, current, ev); err != nil {
		t.Fatal(err)
	}
	if recorder.appendCalls != 1 {
		t.Fatalf("AppendEvent() mirror calls = %d, want 1", recorder.appendCalls)
	}
	if err := routed.CreateSessionSummary(ctx, current, "all", true); err != nil {
		t.Fatal(err)
	}
	if err := routed.EnqueueSummaryJob(ctx, current, "all", false); err != nil {
		t.Fatal(err)
	}
	if recorder.summaryCalls != 0 || recorder.enqueueCalls != 0 {
		t.Fatalf("derived summary unexpectedly mirrored = %d/%d", recorder.summaryCalls, recorder.enqueueCalls)
	}
	for index, writer := range []session.Service{first, second, third} {
		got, err := writer.GetSession(ctx, key)
		if err != nil || got == nil {
			t.Fatalf("writer %d session after mirrored mutations = %#v, %v", index, got, err)
		}
		if string(got.SnapshotState()["session"]) != "active" {
			t.Fatalf("writer %d session state = %#v", index, got.SnapshotState())
		}
	}

	if err := routed.DeleteUserState(ctx, session.UserKey{AppName: key.AppName, UserID: key.UserID}, "user"); err != nil {
		t.Fatal(err)
	}
	if err := routed.DeleteAppState(ctx, key.AppName, "app"); err != nil {
		t.Fatal(err)
	}
	if err := routed.DeleteSession(ctx, key); err != nil {
		t.Fatal(err)
	}
	for index, writer := range []session.Service{first, second, third} {
		got, err := writer.GetSession(ctx, key)
		if err != nil {
			t.Fatalf("writer %d GetSession() after delete = %v", index, err)
		}
		if got != nil {
			t.Fatalf("writer %d still has session: %#v", index, got)
		}
	}
	if err := routed.Close(); err != nil {
		t.Fatalf("Close() = %v", err)
	}
}

type recordingSessionWriter struct {
	session.Service
	appendCalls  int
	summaryCalls int
	enqueueCalls int
}

func (w *recordingSessionWriter) AppendEvent(context.Context, *session.Session, *event.Event, ...session.Option) error {
	w.appendCalls++
	return nil
}

func (w *recordingSessionWriter) CreateSessionSummary(context.Context, *session.Session, string, bool) error {
	w.summaryCalls++
	return nil
}

func (w *recordingSessionWriter) EnqueueSummaryJob(context.Context, *session.Session, string, bool) error {
	w.enqueueCalls++
	return nil
}

type failingSessionWriter struct {
	session.Service
	err error
}

func (w failingSessionWriter) CreateSession(context.Context, session.Key, session.StateMap, ...session.Option) (*session.Session, error) {
	return nil, w.err
}

func (w failingSessionWriter) DeleteSession(context.Context, session.Key, ...session.Option) error {
	return w.err
}

func TestRoutedSessionServiceRequiresPrimaryAndRepairJournalBeforeReplicaWrites(t *testing.T) {
	base := sessioninmemory.NewSessionService()
	t.Cleanup(func() { _ = base.Close() })
	key := session.Key{AppName: "tenant/support", UserID: "user-1", SessionID: "session-1"}
	withoutPrimary := newRoutedSessionService(base, nil, nil, platformstorage.SessionMigrationRoute{}, nil)
	if err := withoutPrimary.DeleteSession(context.Background(), key); err == nil {
		t.Fatal("DeleteSession() without primary error = nil")
	}
	withReplicaWithoutJournal := newRoutedSessionService(
		base, base, []session.Service{base},
		platformstorage.SessionMigrationRoute{MigrationID: "migration-1", TenantID: "tenant", AppCode: "support"}, nil,
	)
	if err := withReplicaWithoutJournal.UpdateSessionState(context.Background(), key, session.StateMap{"x": []byte("1")}); err == nil {
		t.Fatal("replica mutation without repair journal error = nil")
	}
}

func TestRoutedSessionServiceKeepsDirtyRepairWhenReplicaFails(t *testing.T) {
	ctx := context.Background()
	primary := sessioninmemory.NewSessionService()
	replicaBase := sessioninmemory.NewSessionService()
	t.Cleanup(func() { _ = primary.Close(); _ = replicaBase.Close() })
	repairs := &memorySessionRepairStore{}
	want := errors.New("replica unavailable")
	replica := failingSessionWriter{Service: replicaBase, err: want}
	route := platformstorage.SessionMigrationRoute{MigrationID: "migration-1", TenantID: "tenant", AppCode: "support"}
	routed := newRoutedSessionService(primary, primary, []session.Service{replica}, route, repairs)
	key := session.Key{AppName: "tenant/support", UserID: "user-1", SessionID: "session-1"}
	created, err := routed.CreateSession(ctx, key, session.StateMap{"mode": []byte("brief")})
	if err != nil || created == nil {
		t.Fatalf("CreateSession() = %#v, %v", created, err)
	}
	if got, _ := primary.GetSession(ctx, key); got == nil {
		t.Fatal("primary mutation was not committed")
	}
	if repairs.pending() != 1 {
		t.Fatalf("pending repair count = %d, want 1", repairs.pending())
	}
}

type memorySessionRepairStore struct {
	mu         sync.Mutex
	revision   uint64
	repairs    map[string]platformstorage.SessionMigrationRepair
	failUpsert error
}

func (s *memorySessionRepairStore) UpsertSessionMigrationRepair(_ context.Context, repair platformstorage.SessionMigrationRepair) (platformstorage.SessionMigrationRepair, error) {
	if s.failUpsert != nil {
		return platformstorage.SessionMigrationRepair{}, s.failUpsert
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.repairs == nil {
		s.repairs = make(map[string]platformstorage.SessionMigrationRepair)
	}
	s.revision++
	repair.Revision = s.revision
	s.repairs[repairKey(repair)] = repair
	return repair, nil
}

func (s *memorySessionRepairStore) CountPendingSessionMigrationRepairs(context.Context, string, string) (int, error) {
	return s.pending(), nil
}

func (s *memorySessionRepairStore) ClaimSessionMigrationRepairs(context.Context, string, string, string, int, time.Duration) ([]platformstorage.SessionMigrationRepair, error) {
	return nil, nil
}

func (s *memorySessionRepairStore) CompleteSessionMigrationRepair(_ context.Context, repair platformstorage.SessionMigrationRepair, _ string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := repairKey(repair)
	current, ok := s.repairs[key]
	if !ok || current.Revision != repair.Revision {
		return false, nil
	}
	delete(s.repairs, key)
	return true, nil
}

func (s *memorySessionRepairStore) FailSessionMigrationRepair(context.Context, platformstorage.SessionMigrationRepair, string, time.Time, string) error {
	return nil
}

func (s *memorySessionRepairStore) pending() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.repairs)
}

func repairKey(repair platformstorage.SessionMigrationRepair) string {
	return repair.MigrationID + "\x00" + repair.Scope + "\x00" + repair.ScopeKey + "\x00" + repair.SubjectID
}
