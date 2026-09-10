package sessionstore_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/XnLemon/trpc-agent-service/trpcservice/agent/sessionstore"
	runtimestorage "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage"
	runtimestorageinmemory "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage/inmemory"
	sessionstorage "github.com/XnLemon/trpc-agent-service/trpcservice/storage/session"
	trpcevent "trpc.group/trpc-go/trpc-agent-go/event"
	trpcmodel "trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/session"
	sessioninmemory "trpc.group/trpc-go/trpc-agent-go/session/inmemory"
)

type failingCreateStore struct {
	sessionstorage.SessionStateStore
	sessionstorage.EventHistoryStore
}

type serviceStore struct {
	*runtimestorageinmemory.Store
	history   []sessionstorage.EventPayload
	listErr   error
	appendErr error
	updateErr error
}

func (s *serviceStore) ListEventPayloads(context.Context, string, string) ([]sessionstorage.EventPayload, error) {
	if s.listErr != nil {
		return nil, s.listErr
	}
	return s.history, nil
}

func (s *serviceStore) AppendEventPayload(ctx context.Context, value sessionstorage.EventPayload) (sessionstorage.EventPayload, error) {
	if s.appendErr != nil {
		return sessionstorage.EventPayload{}, s.appendErr
	}
	return s.Store.AppendEventPayload(ctx, value)
}

func (s *serviceStore) UpdateSessionState(ctx context.Context, tenant, sessionID string, version int64, state map[string]any) (sessionstorage.Session, error) {
	if s.updateErr != nil {
		return sessionstorage.Session{}, s.updateErr
	}
	return s.Store.UpdateSessionState(ctx, tenant, sessionID, version, state)
}

type failingDelegate struct {
	session.Service
	appendErr error
	updateErr error
}

func (d failingDelegate) AppendEvent(context.Context, *session.Session, *trpcevent.Event, ...session.Option) error {
	return d.appendErr
}

func (d failingDelegate) UpdateSessionState(context.Context, session.Key, session.StateMap) error {
	return d.updateErr
}

func (failingCreateStore) CreateSession(context.Context, string, string, map[string]any) (sessionstorage.Session, error) {
	return sessionstorage.Session{}, fmt.Errorf("create unavailable")
}

func (failingCreateStore) DeleteSession(context.Context, string, string) error {
	return fmt.Errorf("delete unavailable")
}

func TestServicePersistsSessionStateAndEventsForFixedTenant(t *testing.T) {
	store := runtimestorageinmemory.New()
	delegate := sessioninmemory.NewSessionService()
	service, err := sessionstore.New("tenant-a", delegate, store)
	if err != nil {
		t.Fatal(err)
	}
	key := session.Key{AppName: "app", UserID: "user", SessionID: "session"}
	created, err := service.CreateSession(context.Background(), key, session.StateMap{"count": []byte("1")})
	if err != nil {
		t.Fatal(err)
	}
	if err := service.UpdateSessionState(context.Background(), key, session.StateMap{"count": []byte("2")}); err != nil {
		t.Fatal(err)
	}
	if err := service.AppendEvent(context.Background(), created, &trpcevent.Event{ID: "event-1"}); err != nil {
		t.Fatal(err)
	}
	persisted, err := store.GetSession(context.Background(), "tenant-a", "session")
	if err != nil || persisted.Version != 2 {
		t.Fatalf("persisted session = %+v, err=%v", persisted, err)
	}
}

func TestServiceRejectsInvalidConstructionAndKeys(t *testing.T) {
	store := runtimestorageinmemory.New()
	delegate := sessioninmemory.NewSessionService()
	if _, err := sessionstore.New("", delegate, store); !errors.Is(err, runtimestorage.ErrInvalid) {
		t.Fatalf("empty tenant error = %v", err)
	}
	service, err := sessionstore.New("tenant-a", delegate, store)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.GetSession(context.Background(), session.Key{AppName: "app", UserID: "user"}); !errors.Is(err, session.ErrSessionIDRequired) {
		t.Fatalf("invalid key error = %v", err)
	}
}

func TestServiceTreatsMissingDurableSessionAsUpstreamMiss(t *testing.T) {
	service, err := sessionstore.New("tenant-a", sessioninmemory.NewSessionService(), runtimestorageinmemory.New())
	if err != nil {
		t.Fatal(err)
	}
	key := session.Key{AppName: "app", UserID: "user", SessionID: "cold-start"}
	value, err := service.GetSession(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	if value != nil {
		t.Fatalf("missing durable session = %+v, want upstream miss", value)
	}
}

func TestServiceRecoversStateWithFreshDelegate(t *testing.T) {
	store := runtimestorageinmemory.New()
	first, err := sessionstore.New("tenant-a", sessioninmemory.NewSessionService(), store)
	if err != nil {
		t.Fatal(err)
	}
	key := session.Key{AppName: "app", UserID: "user", SessionID: "restart"}
	if _, err := first.CreateSession(context.Background(), key, session.StateMap{"answer": []byte("42")}); err != nil {
		t.Fatal(err)
	}
	second, err := sessionstore.New("tenant-a", sessioninmemory.NewSessionService(), store)
	if err != nil {
		t.Fatal(err)
	}
	recovered, err := second.GetSession(context.Background(), key)
	if err != nil || string(recovered.State["answer"]) != "42" {
		t.Fatalf("recovered = %+v, err=%v", recovered, err)
	}
}

func TestServiceRecoversDurableEventHistoryWithFreshDelegate(t *testing.T) {
	store := runtimestorageinmemory.New()
	first, err := sessionstore.New("tenant-a", sessioninmemory.NewSessionService(), store)
	if err != nil {
		t.Fatal(err)
	}
	key := session.Key{AppName: "app", UserID: "user", SessionID: "event-restart"}
	created, err := first.CreateSession(context.Background(), key, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := first.AppendEvent(context.Background(), created, &trpcevent.Event{ID: "event-restart-1", Response: &trpcmodel.Response{Choices: []trpcmodel.Choice{{Message: trpcmodel.NewUserMessage("durable")}}, Done: true}, StateDelta: map[string][]byte{"answer": []byte("replayed-delta")}}); err != nil {
		t.Fatal(err)
	}
	if err := first.UpdateSessionState(context.Background(), key, session.StateMap{"answer": []byte("durable-state")}); err != nil {
		t.Fatal(err)
	}
	second, err := sessionstore.New("tenant-a", sessioninmemory.NewSessionService(), store)
	if err != nil {
		t.Fatal(err)
	}
	recovered, err := second.GetSession(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	if recovered.GetEventCount() != 1 || recovered.GetEvents()[0].ID != "event-restart-1" {
		t.Fatalf("recovered events = %+v", recovered.GetEvents())
	}
	if string(recovered.State["answer"]) != "durable-state" {
		t.Fatalf("replay overwrote durable state = %q", recovered.State["answer"])
	}
}

func TestServiceRefreshesWarmDelegateFromDurableState(t *testing.T) {
	store := runtimestorageinmemory.New()
	first, err := sessionstore.New("tenant-a", sessioninmemory.NewSessionService(), store)
	if err != nil {
		t.Fatal(err)
	}
	second, err := sessionstore.New("tenant-a", sessioninmemory.NewSessionService(), store)
	if err != nil {
		t.Fatal(err)
	}
	key := session.Key{AppName: "app", UserID: "user", SessionID: "warm"}
	if _, err := first.CreateSession(context.Background(), key, session.StateMap{"value": []byte("old"), "removed": []byte("stale")}); err != nil {
		t.Fatal(err)
	}
	if _, err := second.GetSession(context.Background(), key); err != nil {
		t.Fatal(err)
	}
	if err := second.UpdateSessionState(context.Background(), key, session.StateMap{"value": []byte("new")}); err != nil {
		t.Fatal(err)
	}
	refreshed, err := first.GetSession(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	if string(refreshed.State["value"]) != "new" {
		t.Fatalf("warm delegate state = %q, want new", refreshed.State["value"])
	}
	if _, ok := refreshed.State["removed"]; ok {
		t.Fatalf("warm delegate retained removed durable state: %+v", refreshed.State)
	}
}

func TestServiceCompensatesDelegateWhenDurableCreateFails(t *testing.T) {
	delegate := sessioninmemory.NewSessionService()
	service, err := sessionstore.New("tenant-a", delegate, failingCreateStore{})
	if err != nil {
		t.Fatal(err)
	}
	key := session.Key{AppName: "app", UserID: "user", SessionID: "rollback"}
	if _, err := service.CreateSession(context.Background(), key, nil); err == nil {
		t.Fatal("CreateSession succeeded despite durable failure")
	}
	if existing, err := delegate.GetSession(context.Background(), key); err != nil || existing != nil {
		t.Fatal("delegate session remained after durable failure")
	}
}

func TestServiceDeleteSessionRemovesDurableStateBeforeRecreate(t *testing.T) {
	store := runtimestorageinmemory.New()
	service, err := sessionstore.New("tenant-a", sessioninmemory.NewSessionService(), store)
	if err != nil {
		t.Fatal(err)
	}
	key := session.Key{AppName: "app", UserID: "user", SessionID: "recreate"}
	if _, err := service.CreateSession(context.Background(), key, session.StateMap{"value": []byte("old")}); err != nil {
		t.Fatal(err)
	}
	if err := service.DeleteSession(context.Background(), key); err != nil {
		t.Fatal(err)
	}
	created, err := service.CreateSession(context.Background(), key, session.StateMap{"value": []byte("new")})
	if err != nil {
		t.Fatal(err)
	}
	if got := string(created.State["value"]); got != "new" {
		t.Fatalf("recreated state = %q, want new", got)
	}
	restarted, err := sessionstore.New("tenant-a", sessioninmemory.NewSessionService(), store)
	if err != nil {
		t.Fatal(err)
	}
	recovered, err := restarted.GetSession(context.Background(), key)
	if err != nil || string(recovered.State["value"]) != "new" {
		t.Fatalf("recovered recreated session = %+v err=%v", recovered, err)
	}
}

func TestServiceDeleteSessionValidationAndDurableError(t *testing.T) {
	service, err := sessionstore.New("tenant-a", sessioninmemory.NewSessionService(), runtimestorageinmemory.New())
	if err != nil {
		t.Fatal(err)
	}
	if err := service.DeleteSession(context.Background(), session.Key{AppName: "app", UserID: "user"}); !errors.Is(err, session.ErrSessionIDRequired) {
		t.Fatalf("invalid delete = %v", err)
	}
	service, err = sessionstore.New("tenant-a", sessioninmemory.NewSessionService(), failingCreateStore{})
	if err != nil {
		t.Fatal(err)
	}
	key := session.Key{AppName: "app", UserID: "user", SessionID: "delete-error"}
	if err := service.DeleteSession(context.Background(), key); err == nil || err.Error() != "delete unavailable" {
		t.Fatalf("durable delete error = %v", err)
	}
}

func TestServiceForwardsUpstreamCapabilities(t *testing.T) {
	delegate := sessioninmemory.NewSessionService()
	service, err := sessionstore.New("tenant-a", delegate, runtimestorageinmemory.New())
	if err != nil {
		t.Fatal(err)
	}
	key := session.Key{AppName: "app", UserID: "user", SessionID: "forward"}
	value, err := service.CreateSession(context.Background(), key, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = service.ListSessions(context.Background(), session.UserKey{AppName: "app", UserID: "user"})
	_ = service.UpdateAppState(context.Background(), "app", session.StateMap{"key": []byte("value")})
	_ = service.DeleteAppState(context.Background(), "app", "key")
	_, _ = service.ListAppStates(context.Background(), "app")
	_ = service.UpdateUserState(context.Background(), session.UserKey{AppName: "app", UserID: "user"}, session.StateMap{"key": []byte("value")})
	_, _ = service.ListUserStates(context.Background(), session.UserKey{AppName: "app", UserID: "user"})
	_ = service.DeleteUserState(context.Background(), session.UserKey{AppName: "app", UserID: "user"}, "key")
	_ = service.CreateSessionSummary(context.Background(), value, "", false)
	_ = service.EnqueueSummaryJob(context.Background(), value, "", false)
	_, _ = service.GetSessionSummaryText(context.Background(), value)
	_ = service.AppendEvent(context.Background(), value, &trpcevent.Event{ID: "forward-event"})
	if err := service.DeleteSession(context.Background(), key); err != nil {
		t.Fatal(err)
	}
	if err := service.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := delegate.CreateSession(context.Background(), session.Key{AppName: "app", UserID: "user", SessionID: "after-close"}, nil); err != nil {
		t.Fatalf("borrowed delegate was closed by Service.Close: %v", err)
	}
	if err := delegate.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestServiceUpdateAndAppendErrorEdges(t *testing.T) {
	store := runtimestorageinmemory.New()
	service, err := sessionstore.New("tenant-a", sessioninmemory.NewSessionService(), store)
	if err != nil {
		t.Fatal(err)
	}
	if err := service.UpdateSessionState(context.Background(), session.Key{AppName: "app", UserID: "user", SessionID: "missing"}, nil); !errors.Is(err, runtimestorage.ErrNotFound) {
		t.Fatalf("missing update = %v", err)
	}
	if err := service.AppendEvent(context.Background(), nil, nil); !errors.Is(err, session.ErrNilSession) {
		t.Fatalf("nil append = %v", err)
	}
	if _, err := service.GetSession(context.Background(), session.Key{UserID: "user", SessionID: "id"}); !errors.Is(err, session.ErrAppNameRequired) {
		t.Fatalf("invalid app = %v", err)
	}
	if _, err := service.GetSession(context.Background(), session.Key{AppName: "app", SessionID: "id"}); !errors.Is(err, session.ErrUserIDRequired) {
		t.Fatalf("invalid user = %v", err)
	}
	if _, err := store.CreateSession(context.Background(), "tenant-a", "delegate-missing", map[string]any{"number": 1}); err != nil {
		t.Fatal(err)
	}
	if value, err := service.GetSession(context.Background(), session.Key{AppName: "app", UserID: "user", SessionID: "delegate-missing"}); err != nil || string(value.State["number"]) != "1" {
		t.Fatalf("numeric durable state = %+v, err=%v", value, err)
	}
	service2, err := sessionstore.New("tenant-a", sessioninmemory.NewSessionService(), store)
	if err != nil {
		t.Fatal(err)
	}
	if err := service2.UpdateSessionState(context.Background(), session.Key{AppName: "app", UserID: "user", SessionID: "delegate-missing"}, session.StateMap{"value": []byte("new")}); err == nil {
		t.Fatal("delegate update unexpectedly succeeded")
	}
}

func TestServiceHistoryAndDurableAppendErrorBranches(t *testing.T) {
	base := runtimestorageinmemory.New()
	store := &serviceStore{Store: base}
	key := session.Key{AppName: "app", UserID: "user", SessionID: "history-errors"}
	delegate := sessioninmemory.NewSessionService()
	service, err := sessionstore.New("tenant-a", delegate, store)
	if err != nil {
		t.Fatal(err)
	}
	created, err := service.CreateSession(context.Background(), key, nil)
	if err != nil {
		t.Fatal(err)
	}
	store.history = []sessionstorage.EventPayload{{EventID: "bad", Payload: []byte("{")}}
	if _, err := service.GetSession(context.Background(), key); !errors.Is(err, runtimestorage.ErrStorage) {
		t.Fatalf("invalid history = %v", err)
	}
	store.history = nil
	if err := service.AppendEvent(context.Background(), created, &trpcevent.Event{
		ID:         "invalid-json",
		Extensions: map[string]json.RawMessage{"invalid": []byte("{")},
	}); !errors.Is(err, runtimestorage.ErrInvalid) {
		t.Fatalf("invalid event JSON = %v", err)
	}
	if err := service.AppendEvent(context.Background(), created, &trpcevent.Event{}); !errors.Is(err, runtimestorage.ErrInvalid) {
		t.Fatalf("empty event ID = %v", err)
	}
	store.appendErr = errors.New("history append failed")
	if err := service.AppendEvent(context.Background(), created, &trpcevent.Event{ID: "store-error"}); err == nil || err.Error() != "history append failed" {
		t.Fatalf("store append error = %v", err)
	}
	store.appendErr = nil
	delegateError := errors.New("delegate append failed")
	service, err = sessionstore.New("tenant-a", failingDelegate{Service: sessioninmemory.NewSessionService(), appendErr: delegateError}, store)
	if err != nil {
		t.Fatal(err)
	}
	created, err = service.CreateSession(context.Background(), key, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := service.AppendEvent(context.Background(), created, &trpcevent.Event{ID: "delegate-error"}); !errors.Is(err, delegateError) {
		t.Fatalf("delegate append error = %v", err)
	}
	store.updateErr = errors.New("state update failed")
	if err := service.UpdateSessionState(context.Background(), key, session.StateMap{"x": []byte("1")}); err == nil || err.Error() != "state update failed" {
		t.Fatalf("store state error = %v", err)
	}
	store.updateErr = nil
	service, err = sessionstore.New("tenant-a", failingDelegate{Service: sessioninmemory.NewSessionService(), updateErr: errors.New("delegate state failed")}, store)
	if err != nil {
		t.Fatal(err)
	}
	if err := service.UpdateSessionState(context.Background(), key, session.StateMap{"x": []byte("2")}); err == nil || err.Error() != "delegate state failed" {
		t.Fatalf("delegate state error = %v", err)
	}
	historyPayload, _ := json.Marshal(&trpcevent.Event{ID: "history-error"})
	store.history = []sessionstorage.EventPayload{{EventID: "history-error", Payload: historyPayload}}
	service, err = sessionstore.New("tenant-a", failingDelegate{Service: sessioninmemory.NewSessionService(), appendErr: delegateError}, store)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.GetSession(context.Background(), key); !errors.Is(err, delegateError) {
		t.Fatalf("history delegate error = %v", err)
	}
}
