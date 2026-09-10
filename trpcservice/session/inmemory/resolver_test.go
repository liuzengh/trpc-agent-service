package inmemory

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/worker"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/model"
	frameworksession "trpc.group/trpc-go/trpc-agent-go/session"
	frameworkinmemory "trpc.group/trpc-go/trpc-agent-go/session/inmemory"
)

func TestResolverCacheScopeAndLifecycle(t *testing.T) {
	resolver, err := NewResolver()
	if err != nil {
		t.Fatal(err)
	}

	first, err := resolver.ResolveSession(context.Background(), testExecution("tenant-a", "app-a", "v1", "sessions"))
	if err != nil {
		t.Fatal(err)
	}
	same, err := resolver.ResolveSession(context.Background(), testExecution("tenant-a", "app-a", "v1", "sessions"))
	if err != nil {
		t.Fatal(err)
	}
	if first != same {
		t.Fatal("same pinned backend scope did not reuse the session service")
	}
	for _, execution := range []worker.Execution{
		testExecution("tenant-b", "app-a", "v1", "sessions"),
		testExecution("tenant-a", "app-b", "v1", "sessions"),
		testExecution("tenant-a", "app-a", "v2", "sessions"),
		testExecution("tenant-a", "app-a", "v1", "other-sessions"),
	} {
		other, err := resolver.ResolveSession(context.Background(), execution)
		if err != nil {
			t.Fatal(err)
		}
		if other == first {
			t.Fatal("different backend scope reused the session service")
		}
	}
	if err := resolver.Close(); err != nil {
		t.Fatal(err)
	}
	if err := resolver.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := resolver.ResolveSession(context.Background(), testExecution("tenant-a", "app-a", "v1", "sessions")); err == nil {
		t.Fatal("resolve after close succeeded")
	}
}

func TestSessionContract(t *testing.T) {
	serviceResolver, err := NewResolver(
		frameworkinmemory.WithSessionEventLimit(0),
		frameworkinmemory.WithSummarizer(testSummarizer{}),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = serviceResolver.Close() })
	service, err := serviceResolver.ResolveSession(context.Background(), testExecution("tenant-a", "app-a", "v1", "sessions"))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	key := frameworksession.Key{AppName: "tenant:tenant-a:app:app-a:runner", UserID: "user-a", SessionID: "session-a"}
	sess, err := service.CreateSession(ctx, key, frameworksession.StateMap{"topic": []byte("billing")})
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range []*event.Event{
		newResponseEvent("event-1", "user", "question"),
		newResponseEvent("event-2", "assistant", "answer"),
	} {
		if err := service.AppendEvent(ctx, sess, value); err != nil {
			t.Fatal(err)
		}
	}
	trackService, ok := service.(frameworksession.TrackService)
	if !ok {
		t.Fatal("in-memory session service does not implement TrackService")
	}
	if err := trackService.AppendTrackEvent(ctx, sess, &frameworksession.TrackEvent{
		Track:     frameworksession.Track("turn"),
		Payload:   json.RawMessage(`{"ok":true}`),
		Timestamp: time.Now(),
	}); err != nil {
		t.Fatal(err)
	}
	if err := service.UpdateAppState(ctx, key.AppName, frameworksession.StateMap{"region": []byte("cn")}); err != nil {
		t.Fatal(err)
	}
	userKey := frameworksession.UserKey{AppName: key.AppName, UserID: key.UserID}
	if err := service.UpdateUserState(ctx, userKey, frameworksession.StateMap{"plan": []byte("pro")}); err != nil {
		t.Fatal(err)
	}
	if err := service.UpdateSessionState(ctx, key, frameworksession.StateMap{"phase": []byte("done")}); err != nil {
		t.Fatal(err)
	}
	loaded, err := service.GetSession(ctx, key, frameworksession.WithEventNum(100))
	if err != nil {
		t.Fatal(err)
	}
	if loaded == nil || len(loaded.Events) != 2 || loaded.Events[0].InvocationID != "event-1" || loaded.Events[1].InvocationID != "event-2" {
		t.Fatalf("session events = %#v", loaded)
	}
	if len(loaded.Tracks[frameworksession.Track("turn")].Events) != 1 {
		t.Fatalf("session tracks = %#v", loaded.Tracks)
	}
	if state, err := service.ListAppStates(ctx, key.AppName); err != nil || string(state["region"]) != "cn" {
		t.Fatalf("app state=%q err=%v", state["region"], err)
	}
	if state, err := service.ListUserStates(ctx, userKey); err != nil || string(state["plan"]) != "pro" {
		t.Fatalf("user state=%q err=%v", state["plan"], err)
	}
	if string(loaded.State["phase"]) != "done" {
		t.Fatalf("session state=%q", loaded.State["phase"])
	}
	if err := service.CreateSessionSummary(ctx, loaded, "", true); err != nil {
		t.Fatal(err)
	}
	if summary, ok := service.GetSessionSummaryText(ctx, loaded); !ok || summary != "summary" {
		t.Fatalf("summary=%q ok=%v", summary, ok)
	}

	const concurrentEvents = 16
	var wait sync.WaitGroup
	for index := 0; index < concurrentEvents; index++ {
		index := index
		wait.Add(1)
		go func() {
			defer wait.Done()
			current, err := service.GetSession(ctx, key)
			if err != nil {
				t.Errorf("get concurrent session: %v", err)
				return
			}
			if err := service.AppendEvent(ctx, current, newResponseEvent(fmt.Sprintf("concurrent-%02d", index), "user", "message")); err != nil {
				t.Errorf("append concurrent event: %v", err)
			}
		}()
	}
	wait.Wait()
	loaded, err = service.GetSession(ctx, key, frameworksession.WithEventNum(100))
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded.Events) != 2+concurrentEvents {
		t.Fatalf("concurrent event count=%d, want %d", len(loaded.Events), 2+concurrentEvents)
	}

	isolated, err := serviceResolver.ResolveSession(ctx, testExecution("tenant-a", "app-b", "v1", "sessions"))
	if err != nil {
		t.Fatal(err)
	}
	if got, err := isolated.GetSession(ctx, key); err != nil || got != nil {
		t.Fatalf("cross-app session got=%v err=%v", got, err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := serviceResolver.ResolveSession(canceled, testExecution("tenant-a", "app-a", "v1", "sessions")); !errors.Is(err, context.Canceled) {
		t.Fatalf("resolve canceled error=%v", err)
	}
	if err := service.DeleteSession(ctx, key); err != nil {
		t.Fatal(err)
	}
	if got, err := service.GetSession(ctx, key); err != nil || got != nil {
		t.Fatalf("deleted session got=%v err=%v", got, err)
	}
}

type testSummarizer struct{}

func (testSummarizer) ShouldSummarize(*frameworksession.Session) bool { return true }
func (testSummarizer) Summarize(context.Context, *frameworksession.Session) (string, error) {
	return "summary", nil
}
func (testSummarizer) SetPrompt(string)         {}
func (testSummarizer) SetModel(model.Model)     {}
func (testSummarizer) Metadata() map[string]any { return nil }

func newResponseEvent(id, role, text string) *event.Event {
	message := model.NewUserMessage(text)
	if role == "assistant" {
		message = model.NewAssistantMessage(text)
	}
	return event.NewResponseEvent(id, role, &model.Response{
		Object: model.ObjectTypeChatCompletion,
		Done:   true,
		Choices: []model.Choice{{
			Message: message,
		}},
	})
}

func testExecution(tenantID, appID, version, backendName string) worker.Execution {
	return worker.Execution{
		Tenant: tenant.RuntimeContext{
			TenantID:      tenantID,
			AppID:         appID,
			ConfigVersion: version,
		},
		Config: tenant.AppConfig{
			TenantID: tenantID,
			AppID:    appID,
			Version:  version,
			BackendConfig: tenant.BackendConfig{
				Name: backendName,
				Session: tenant.BackendRef{
					Kind:     tenant.BackendInMemory,
					Provider: providerName,
					Name:     backendName,
				},
			},
		},
	}
}
