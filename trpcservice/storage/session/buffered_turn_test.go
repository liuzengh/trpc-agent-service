package session_test

import (
	"context"
	"errors"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	sessionstore "github.com/liuzengh/trpc-agent-service/trpcservice/storage/session"
	sessionmemory "github.com/liuzengh/trpc-agent-service/trpcservice/storage/session/inmemory"
	agentevent "trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/model"
	agentsession "trpc.group/trpc-go/trpc-agent-go/session"
	agentmemory "trpc.group/trpc-go/trpc-agent-go/session/inmemory"
)

func TestBufferedTurnPersistsSessionEffectsThroughOfficialService(t *testing.T) {
	ctx := context.Background()
	backing := agentmemory.NewSessionService()
	key := agentsession.Key{AppName: "tenant/app", UserID: "user", SessionID: "session"}
	if _, err := backing.CreateSession(ctx, key, agentsession.StateMap{}); err != nil {
		t.Fatal(err)
	}
	atomic := sessionmemory.New()
	storageKey := sessionstore.SessionKey{TenantID: "tenant", AgentAppID: "app", SessionID: "session"}
	head, err := atomic.OpenForRun(ctx, sessionstore.OpenForRunRequest{SessionKey: storageKey, RequestID: "request", InputSeq: 1, Fence: 1})
	if err != nil {
		t.Fatal(err)
	}
	turn, err := sessionstore.NewBufferedTurn(atomic, backing, storageKey, "user")
	if err != nil {
		t.Fatal(err)
	}
	session, err := turn.SessionService().GetSession(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if err := turn.SessionService().AppendEvent(ctx, session, durableEvent("event-0", model.RoleUser, "input", nil)); err != nil {
		t.Fatal(err)
	}
	event := durableEvent("event-1", model.RoleAssistant, "ready", map[string][]byte{"answer": []byte(`"ready"`)})
	if err := turn.SessionService().AppendEvent(ctx, session, event); err != nil {
		t.Fatal(err)
	}
	base, err := backing.GetSession(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if len(base.Events) != 2 || string(base.State["answer"]) != `"ready"` {
		t.Fatalf("SDK session effects not persisted: %#v", base)
	}
	_, err = turn.Commit(ctx, sessionstore.CommitTurnRequest{RequestID: "request", CommitID: "request:terminal:0", Stage: "terminal", InputSeq: 1, Fence: 1, ExpectedVersion: head.Version, Outcome: runtime.OutcomeSucceeded})
	if err != nil {
		t.Fatal(err)
	}
	events, _, _ := atomic.SnapshotEffects(storageKey)
	if len(events) != 0 {
		t.Fatalf("coordination store duplicated SDK events=%#v", events)
	}
}

func TestBufferedTurnRollbackOnlyDropsCoordinationMetadata(t *testing.T) {
	ctx := context.Background()
	backing := agentmemory.NewSessionService()
	key := agentsession.Key{AppName: "tenant/app", UserID: "user", SessionID: "session"}
	if _, err := backing.CreateSession(ctx, key, agentsession.StateMap{}); err != nil {
		t.Fatal(err)
	}
	atomic := sessionmemory.New()
	storageKey := sessionstore.SessionKey{TenantID: "tenant", AgentAppID: "app", SessionID: "session"}
	turn, err := sessionstore.NewBufferedTurn(atomic, backing, storageKey, "user")
	if err != nil {
		t.Fatal(err)
	}
	session, err := turn.SessionService().GetSession(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if err := turn.SessionService().AppendEvent(ctx, session, durableEvent("event-0", model.RoleUser, "input", nil)); err != nil {
		t.Fatal(err)
	}
	if err := turn.SessionService().AppendEvent(ctx, session, durableEvent("event-1", model.RoleAssistant, "ready", nil)); err != nil {
		t.Fatal(err)
	}
	if err := turn.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	base, err := backing.GetSession(ctx, key)
	if err != nil || len(base.Events) != 2 {
		t.Fatalf("SDK event unexpectedly rolled back: session=%#v err=%v", base, err)
	}
}

func TestDurableBufferedTurnRestoresCommittedHistory(t *testing.T) {
	ctx := context.Background()
	atomic := sessionmemory.New()
	backing := agentmemory.NewSessionService()
	storageKey := sessionstore.SessionKey{TenantID: "tenant", AgentAppID: "app", SessionID: "session"}
	head, err := atomic.OpenForRun(ctx, sessionstore.OpenForRunRequest{SessionKey: storageKey, RequestID: "request-1", InputSeq: 1, Fence: 1})
	if err != nil {
		t.Fatal(err)
	}
	first, err := sessionstore.NewDurableBufferedTurnScoped(atomic, backing, storageKey, "tenant/app", "user")
	if err != nil {
		t.Fatal(err)
	}
	session, err := first.SessionService().GetSession(ctx, agentsession.Key{AppName: "tenant/app", UserID: "user", SessionID: "session"})
	if err != nil {
		t.Fatal(err)
	}
	if err := first.SessionService().AppendEvent(ctx, session, durableEvent("event-0", model.RoleUser, "input", nil)); err != nil {
		t.Fatal(err)
	}
	if err := first.SessionService().AppendEvent(ctx, session, durableEvent("event-1", model.RoleAssistant, "ready", map[string][]byte{"answer": []byte(`"ready"`)})); err != nil {
		t.Fatal(err)
	}
	if _, err := first.Commit(ctx, sessionstore.CommitTurnRequest{RequestID: "request-1", CommitID: "request-1:terminal:0", Stage: "terminal", InputSeq: 1, Fence: 1, ExpectedVersion: head.Version, Outcome: runtime.OutcomeSucceeded}); err != nil {
		t.Fatal(err)
	}
	if _, err := atomic.OpenForRun(ctx, sessionstore.OpenForRunRequest{SessionKey: storageKey, RequestID: "request-2", InputSeq: 2, Fence: 2}); err != nil {
		t.Fatal(err)
	}
	second, err := sessionstore.NewDurableBufferedTurnScoped(atomic, backing, storageKey, "tenant/app", "user")
	if err != nil {
		t.Fatal(err)
	}
	restored, err := second.SessionService().GetSession(ctx, agentsession.Key{AppName: "tenant/app", UserID: "user", SessionID: "session"})
	if err != nil {
		t.Fatal(err)
	}
	if len(restored.Events) != 2 || restored.Events[1].ID != "event-1" || string(restored.State["answer"]) != `"ready"` {
		t.Fatalf("restored=%#v", restored)
	}
}

func TestBufferedTurnRestrictsSDKOperationsToItsScopedSession(t *testing.T) {
	ctx := context.Background()
	atomic := sessionmemory.New()
	backing := agentmemory.NewSessionService()
	storageKey := sessionstore.SessionKey{TenantID: "tenant", AgentAppID: "app", SessionID: "session"}
	turn, err := sessionstore.NewDurableBufferedTurnScoped(atomic, backing, storageKey, "tenant/app", "user")
	if err != nil {
		t.Fatal(err)
	}
	service := turn.SessionService()
	key := agentsession.Key{AppName: "tenant/app", UserID: "user", SessionID: "session"}
	state := agentsession.StateMap{"status": []byte(`"new"`)}
	created, err := service.CreateSession(ctx, key, state)
	if err != nil {
		t.Fatal(err)
	}
	if created.ID != key.SessionID {
		t.Fatalf("created=%#v", created)
	}
	state["status"] = []byte(`"mutated"`)
	if err := service.UpdateSessionState(ctx, key, agentsession.StateMap{"status": []byte(`"active"`)}); err != nil {
		t.Fatal(err)
	}
	fetched, err := service.GetSession(ctx, key)
	if err != nil || string(fetched.State["status"]) != `"active"` {
		t.Fatalf("fetched=%#v err=%v", fetched, err)
	}
	listed, err := service.ListSessions(ctx, agentsession.UserKey{AppName: "tenant/app", UserID: "user"})
	if err != nil || len(listed) != 1 || listed[0].ID != key.SessionID {
		t.Fatalf("sessions=%#v err=%v", listed, err)
	}
	wrongKey := key
	wrongKey.UserID = "other-user"
	if _, err := service.GetSession(ctx, wrongKey); !errors.Is(err, runtime.ErrTenantScope) {
		t.Fatalf("cross-user get err=%v", err)
	}
	if _, err := service.ListSessions(ctx, agentsession.UserKey{AppName: "tenant/app", UserID: "other-user"}); !errors.Is(err, runtime.ErrTenantScope) {
		t.Fatalf("cross-user list err=%v", err)
	}
	if _, err := service.ListAppStates(ctx, "other/app"); !errors.Is(err, runtime.ErrTenantScope) {
		t.Fatalf("cross-app states err=%v", err)
	}
	if _, err := service.ListUserStates(ctx, agentsession.UserKey{AppName: "tenant/app", UserID: "other-user"}); !errors.Is(err, runtime.ErrTenantScope) {
		t.Fatalf("cross-user states err=%v", err)
	}

	unsupported := []struct {
		name string
		call func() error
	}{
		{"delete session", func() error { return service.DeleteSession(ctx, key) }},
		{"update app state", func() error { return service.UpdateAppState(ctx, "tenant/app", agentsession.StateMap{}) }},
		{"delete app state", func() error { return service.DeleteAppState(ctx, "tenant/app", "key") }},
		{"list app states", func() error { _, err := service.ListAppStates(ctx, "tenant/app"); return err }},
		{"update user state", func() error {
			return service.UpdateUserState(ctx, agentsession.UserKey{AppName: "tenant/app", UserID: "user"}, agentsession.StateMap{})
		}},
		{"list user states", func() error {
			_, err := service.ListUserStates(ctx, agentsession.UserKey{AppName: "tenant/app", UserID: "user"})
			return err
		}},
		{"delete user state", func() error {
			return service.DeleteUserState(ctx, agentsession.UserKey{AppName: "tenant/app", UserID: "user"}, "key")
		}},
		{"create summary", func() error { return service.CreateSessionSummary(ctx, fetched, "summary", false) }},
		{"enqueue summary", func() error { return service.EnqueueSummaryJob(ctx, fetched, "summary", false) }},
	}
	for _, test := range unsupported {
		t.Run(test.name, func(t *testing.T) {
			if err := test.call(); !errors.Is(err, runtime.ErrCapabilityUnsupported) {
				t.Fatalf("err=%v", err)
			}
		})
	}
	if summary, ok := service.GetSessionSummaryText(ctx, fetched); ok || summary != "" {
		t.Fatalf("summary=%q ok=%t", summary, ok)
	}
	if err := service.Close(); err != nil {
		t.Fatal(err)
	}
	if err := turn.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if err := service.UpdateSessionState(ctx, key, agentsession.StateMap{}); !errors.Is(err, runtime.ErrCommitConflict) {
		t.Fatalf("closed turn update err=%v", err)
	}
}

// durableEvent mirrors the event shape the official trpc-agent-go Session
// services persist: a completed response plus any StateDelta. Metadata-only
// events intentionally update state without entering the transcript.
func durableEvent(id string, role model.Role, content string, delta map[string][]byte) *agentevent.Event {
	return &agentevent.Event{ID: id, Response: &model.Response{Choices: []model.Choice{{Message: model.Message{
		Role: role, Content: content}}}}, StateDelta: delta}
}
