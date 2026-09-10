package gateway_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	gatewaymemory "github.com/liuzengh/trpc-agent-service/trpcservice/gateway/inmemory"
	profilememory "github.com/liuzengh/trpc-agent-service/trpcservice/profile/inmemory"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/messaging"
	messagingmemory "github.com/liuzengh/trpc-agent-service/trpcservice/storage/messaging/inmemory"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"trpc.group/trpc-go/trpc-agent-go/agent"
	"trpc.group/trpc-go/trpc-agent-go/model"
)

func TestRunHandlerUsesAuthorizedRouteAndStableIdempotency(t *testing.T) {
	fixture := newRunFixture()
	handler := gateway.RunHandler{Submitter: fixture.submitter, Principals: controlPrincipalResolver{principal: gateway.Principal{
		Authenticated: true, TenantID: "tenant-a", SubjectID: "principal-a", CanRun: true, TraceParent: "first-trace",
	}}, Routes: staticRunRoute{route: fixture.route}}

	first := createRun(t, handler, "same-key", `{"agent_app_id":"app","session_id":"untrusted-alias","text":"hello"}`)
	if first.Code != http.StatusAccepted {
		t.Fatalf("first code=%d body=%q", first.Code, first.Body.String())
	}
	var firstHandle gateway.ExecutionHandle
	if err := json.Unmarshal(first.Body.Bytes(), &firstHandle); err != nil {
		t.Fatal(err)
	}
	second := createRun(t, handler, "same-key", `{"agent_app_id":"app","session_id":"another-alias","text":"hello"}`)
	if second.Code != http.StatusAccepted {
		t.Fatalf("retry code=%d body=%q", second.Code, second.Body.String())
	}
	var secondHandle gateway.ExecutionHandle
	_ = json.Unmarshal(second.Body.Bytes(), &secondHandle)
	if firstHandle.RequestID == "" || secondHandle.RequestID != firstHandle.RequestID {
		t.Fatalf("unstable handles first=%#v second=%#v", firstHandle, secondHandle)
	}
	status, err := fixture.tasks.GetExecution(context.Background(), gateway.ExecutionKey{TenantID: "tenant-a", RequestID: firstHandle.RequestID})
	if err != nil || status.Envelope.UserID != "principal-user" || status.Envelope.SessionID != "canonical-session" || status.Envelope.TraceParent != "first-trace" {
		t.Fatalf("status=%#v err=%v", status, err)
	}
	payload, err := fixture.payloads.GetPayload(context.Background(), "tenant-a", firstHandle.RequestID)
	if err != nil || payload.KeyVersion != 7 {
		t.Fatalf("payload key version=%d err=%v", payload.KeyVersion, err)
	}

	collision := createRun(t, handler, "same-key", `{"agent_app_id":"app","text":"different"}`)
	if collision.Code != http.StatusConflict {
		t.Fatalf("collision code=%d", collision.Code)
	}
}

func TestRunHandlerFailsClosedForAuthAndCrossTenantRoute(t *testing.T) {
	fixture := newRunFixture()
	handler := gateway.RunHandler{Submitter: fixture.submitter, Principals: controlPrincipalResolver{}, Routes: staticRunRoute{route: fixture.route}}
	if response := createRun(t, handler, "key", `{"agent_app_id":"app","text":"hello"}`); response.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated code=%d", response.Code)
	}
	handler.Principals = controlPrincipalResolver{principal: gateway.Principal{Authenticated: true, TenantID: "tenant-a", SubjectID: "p", CanRun: true}}
	bad := fixture.route
	bad.Tenant.TenantID = "tenant-b"
	handler.Routes = staticRunRoute{route: bad}
	if response := createRun(t, handler, "key", `{"agent_app_id":"app","text":"hello"}`); response.Code != http.StatusForbidden {
		t.Fatalf("cross-tenant code=%d", response.Code)
	}
}

func TestGatewayRunnerBridgeRequiresTrustedContextAndReplaysTerminalEvents(t *testing.T) {
	fixture := newRunFixture()
	store := &terminalBridgeStore{}
	bridge := gateway.NewGatewayRunnerBridge(fixture.submitter, store)
	bridge.PollInterval = time.Millisecond
	bridge.ReplayLimit = 1_000
	if _, err := bridge.Run(context.Background(), "principal-user", "canonical-session", model.NewUserMessage("hello")); err == nil {
		t.Fatal("bridge accepted missing trusted context")
	}
	trusted := gateway.ServerInvocationContext{Tenant: fixture.route.Tenant, PrincipalID: "principal-a",
		UserID: fixture.route.UserID, SessionID: fixture.route.SessionID, Protocol: "openai", IdempotencyKey: "openai-key", CanRun: true}
	ctx := gateway.WithServerInvocationContext(context.Background(), trusted)
	if _, err := bridge.Run(ctx, "spoofed", trusted.SessionID, model.NewUserMessage("hello")); err == nil {
		t.Fatal("bridge accepted spoofed user")
	}
	if _, err := bridge.Run(ctx, trusted.UserID, trusted.SessionID, model.NewUserMessage("hello"), agent.WithAppName("other-app")); err == nil {
		t.Fatal("bridge accepted spoofed app")
	}
	events, err := bridge.Run(ctx, trusted.UserID, trusted.SessionID, model.NewUserMessage("hello"), agent.WithAppName("app"))
	if err != nil {
		t.Fatal(err)
	}
	var values int
	for value := range events {
		values++
		if value.RequestID == "" || value.Response == nil {
			t.Fatalf("invalid event %#v", value)
		}
		if len(value.Response.Choices) > 0 && (value.Response.Choices[0].Message.Content != "done" || value.Response.Choices[0].Delta.Content != "done") {
			t.Fatalf("non-stream/stream conversion diverged: %#v", value.Response.Choices[0])
		}
	}
	if values != 2 {
		t.Fatalf("events=%d", values)
	}
	if store.limit != 256 {
		t.Fatalf("replay limit=%d want=256", store.limit)
	}
	if err := bridge.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := bridge.Run(ctx, trusted.UserID, trusted.SessionID, model.NewUserMessage("hello")); err == nil {
		t.Fatal("closed bridge accepted run")
	}
}

func TestGatewayRunnerBridgeDisconnectDoesNotCancelDurableExecution(t *testing.T) {
	fixture := newRunFixture()
	bridge := gateway.NewGatewayRunnerBridge(fixture.submitter, emptyBridgeStore{})
	bridge.PollInterval = time.Millisecond
	ctx, cancel := context.WithCancel(gateway.WithServerInvocationContext(context.Background(), gateway.ServerInvocationContext{
		Tenant: fixture.route.Tenant, PrincipalID: "principal-a", UserID: fixture.route.UserID,
		SessionID: fixture.route.SessionID, Protocol: "a2a", IdempotencyKey: "disconnect-key", CanRun: true,
	}))
	events, err := bridge.Run(ctx, fixture.route.UserID, fixture.route.SessionID, model.NewUserMessage("hello"))
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	select {
	case _, ok := <-events:
		if ok {
			t.Fatal("unexpected event")
		}
	case <-time.After(time.Second):
		t.Fatal("subscription did not close")
	}
	requestID, _ := messaging.StableInboxIdentity(messaging.InboxKey{TenantID: "tenant-a", Channel: "gateway", ExternalAccountID: "app", ExternalMessageID: "disconnect-key"})
	status, err := fixture.tasks.GetExecution(context.Background(), gateway.ExecutionKey{TenantID: "tenant-a", RequestID: requestID})
	if err != nil || status.CancelRequested || status.Outcome != runtime.OutcomeQueued {
		t.Fatalf("durable status=%#v err=%v", status, err)
	}
}

func TestProtocolInvocationMiddlewareFailsClosedAndInjectsTrustedContext(t *testing.T) {
	var observed gateway.ServerInvocationContext
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		observed, _ = gateway.ServerInvocationFromContext(r.Context())
		w.WriteHeader(http.StatusNoContent)
	})
	request := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	response := httptest.NewRecorder()
	gateway.ProtocolInvocationMiddleware{Next: next}.ServeHTTP(response, request)
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("missing resolver code=%d", response.Code)
	}
	fixture := newRunFixture()
	trusted := gateway.ServerInvocationContext{Tenant: fixture.route.Tenant, PrincipalID: "principal-a",
		UserID: fixture.route.UserID, SessionID: fixture.route.SessionID, Protocol: "openai", IdempotencyKey: "key", CanRun: true}
	response = httptest.NewRecorder()
	gateway.ProtocolInvocationMiddleware{Resolver: staticInvocationResolver{trusted: trusted}, Next: next}.ServeHTTP(response, request)
	if response.Code != http.StatusNoContent || observed.IdempotencyKey != "key" || observed.Tenant.TenantID != "tenant-a" {
		t.Fatalf("code=%d observed=%#v", response.Code, observed)
	}
}

type runFixture struct {
	submitter gateway.RunSubmitter
	tasks     *gatewaymemory.TaskStore
	payloads  *messagingmemory.Store
	route     gateway.RunRoute
}

func newRunFixture() runFixture {
	storage := messagingmemory.New()
	tasks := gatewaymemory.NewTaskStore()
	bindings := profilememory.NewBindingResolver()
	bindings.Put("tenant-a", "app", tenant.ExecutionBinding{AgentAppVersion: 1, AgentAppRevision: 1,
		AgentContentDigest: "digest", ConfigVersion: 1, PolicyVersion: 1})
	tc := tenant.Context{TenantID: "tenant-a", TenantVersion: 1, AgentAppID: "app", SubjectID: "principal-a",
		Channel: "gateway", TrustedSource: "test-route"}
	return runFixture{submitter: gateway.RunSubmitter{Inbox: storage, Payloads: storage, PayloadKeyVersion: 7,
		Dispatcher: gateway.BrokerDispatcher{Tasks: tasks, Bindings: bindings}}, tasks: tasks, payloads: storage,
		route: gateway.RunRoute{Tenant: tc, UserID: "principal-user", SessionID: "canonical-session"}}
}

type staticRunRoute struct{ route gateway.RunRoute }

func (r staticRunRoute) ResolveRunRoute(_ *http.Request, _ gateway.Principal, _, _ string) (gateway.RunRoute, error) {
	return r.route, nil
}

type staticInvocationResolver struct {
	trusted gateway.ServerInvocationContext
}

func (r staticInvocationResolver) ResolveProtocolInvocation(*http.Request) (gateway.ServerInvocationContext, error) {
	return r.trusted, nil
}

func createRun(t *testing.T, handler http.Handler, key, body string) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(http.MethodPost, "/v1/agent-runs", bytes.NewBufferString(body))
	request.Header.Set("Idempotency-Key", key)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

type terminalBridgeStore struct{ limit int }

func (s *terminalBridgeStore) Replay(_ context.Context, key gateway.ExecutionKey, after uint64, limit int) (gateway.EventPage, error) {
	s.limit = limit
	now := time.Unix(1_800_000_000, 0).UTC()
	events := []gateway.SharedEvent{{TenantID: key.TenantID, RequestID: key.RequestID, Sequence: 1,
		Type: "message.completed", Data: json.RawMessage(`{"content":"done"}`), CreatedAt: now},
		{TenantID: key.TenantID, RequestID: key.RequestID, Sequence: 2, Type: "run.completed", Data: json.RawMessage(`{}`), Terminal: true, CreatedAt: now}}
	page := gateway.EventPage{LastSequence: 2, Terminal: true}
	for _, value := range events {
		if value.Sequence > after {
			page.Events = append(page.Events, value)
		}
	}
	return page, nil
}

type emptyBridgeStore struct{}

func (emptyBridgeStore) Replay(context.Context, gateway.ExecutionKey, uint64, int) (gateway.EventPage, error) {
	return gateway.EventPage{}, nil
}
