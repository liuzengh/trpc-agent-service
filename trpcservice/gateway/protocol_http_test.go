package gateway_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/redaction"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"trpc.group/trpc-go/trpc-agent-go/agent"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/model"
)

type protocolRunnerStub struct {
	userID, sessionID, appName, requestID string
	emitted                               *event.Event
	ctx                                   context.Context
}

func (r *protocolRunnerStub) Run(ctx context.Context, userID, sessionID string, _ model.Message, options ...agent.RunOption) (<-chan *event.Event, error) {
	r.ctx = ctx
	opts := agent.NewRunOptions(options...)
	r.userID, r.sessionID, r.appName, r.requestID = userID, sessionID, opts.AppName, opts.RequestID
	out := make(chan *event.Event, 1)
	r.emitted = &event.Event{Response: &model.Response{Object: model.ObjectTypeRunnerCompletion, Done: true},
		RequestID: "durable-request", InvocationID: "invocation", Author: "app"}
	out <- r.emitted
	close(out)
	return out, nil
}

func (*protocolRunnerStub) Close() error { return nil }

func TestProtocolHTTPHandlerMountsCanonicalTRPCAgentFacade(t *testing.T) {
	underlying := &protocolRunnerStub{}
	program, err := redaction.Compile(redaction.Config{Level: redaction.LevelStrict})
	if err != nil {
		t.Fatal(err)
	}
	trusted := gateway.ServerInvocationContext{Tenant: newRunFixture().route.Tenant, PrincipalID: "principal-a",
		UserID: "canonical-user", SessionID: "canonical-session", Protocol: "trpc-agent", IdempotencyKey: "stable-key", CanRun: true, RedactionProgram: program}
	handler, err := gateway.NewProtocolHTTPHandler(gateway.ProtocolHTTPOptions{Runner: gateway.CanonicalRunner{Next: underlying},
		Resolver: staticInvocationResolver{trusted: trusted}, Readiness: readinessStub{ready: true}, MaxBody: 1 << 20, RunTimeout: time.Second,
		A2A: newProtocolA2AManager(), PublicURL: "https://gateway.example.test"})
	if err != nil {
		t.Fatal(err)
	}
	body := `{"session":{"userId":"spoofed-user","sessionId":"spoofed-session"},"input":{"role":"user","content":"hello"},"runOptions":{"requestID":"wire-request"}}`
	request := httptest.NewRequest(http.MethodPost, "/trpc-agent/v1/apps/app/runs", strings.NewReader(body))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("code=%d body=%q", response.Code, response.Body.String())
	}
	if underlying.userID != "canonical-user" || underlying.sessionID != "canonical-session" || underlying.appName != "app" || underlying.requestID != "wire-request" {
		t.Fatalf("runner identity user=%q session=%q app=%q request=%q", underlying.userID, underlying.sessionID, underlying.appName, underlying.requestID)
	}
	if scoped, ok := redaction.ProgramFromContext(underlying.ctx); !ok || scoped != program {
		t.Fatalf("tenant redaction program was not bound to runner context")
	}
	var payload struct {
		Events []struct {
			RequestID  string                     `json:"requestID"`
			Extensions map[string]json.RawMessage `json:"extensions"`
		} `json:"events"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &payload); err != nil || len(payload.Events) != 1 {
		t.Fatalf("decode response=%q err=%v", response.Body.String(), err)
	}
	if payload.Events[0].RequestID != "wire-request" {
		t.Fatalf("wire request id=%q", payload.Events[0].RequestID)
	}
	if underlying.emitted.RequestID != "durable-request" || len(underlying.emitted.Extensions) != 0 {
		t.Fatalf("adapter mutated shared event: %#v", underlying.emitted)
	}
	var durable string
	if err := json.Unmarshal(payload.Events[0].Extensions[gateway.DurableRequestIDExtension], &durable); err != nil || durable != "durable-request" {
		t.Fatalf("durable extension=%q err=%v", durable, err)
	}
}

func TestProtocolHTTPHandlerFailsClosedForDrainBodyAndPartialComposition(t *testing.T) {
	if _, err := gateway.NewProtocolHTTPHandler(gateway.ProtocolHTTPOptions{}); err != runtime.ErrInvariantViolation {
		t.Fatalf("partial composition err=%v", err)
	}
	trusted := gateway.ServerInvocationContext{Tenant: newRunFixture().route.Tenant, PrincipalID: "principal-a",
		UserID: "user", SessionID: "session", Protocol: "trpc-agent", IdempotencyKey: "key", CanRun: true}
	for _, test := range []struct {
		name    string
		ready   bool
		maxBody int64
		want    int
	}{
		{name: "draining", ready: false, maxBody: 1024, want: http.StatusServiceUnavailable},
		{name: "oversize", ready: true, maxBody: 8, want: http.StatusRequestEntityTooLarge},
	} {
		t.Run(test.name, func(t *testing.T) {
			handler, err := gateway.NewProtocolHTTPHandler(gateway.ProtocolHTTPOptions{Runner: gateway.CanonicalRunner{Next: &protocolRunnerStub{}},
				Resolver: staticInvocationResolver{trusted: trusted}, Readiness: readinessStub{ready: test.ready}, MaxBody: test.maxBody, RunTimeout: time.Second,
				A2A: newProtocolA2AManager(), PublicURL: "https://gateway.example.test"})
			if err != nil {
				t.Fatal(err)
			}
			request := httptest.NewRequest(http.MethodPost, "/trpc-agent/v1/apps/app/runs", strings.NewReader(`{"oversized":true}`))
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != test.want {
				t.Fatalf("code=%d want=%d body=%q", response.Code, test.want, response.Body.String())
			}
		})
	}
}

func newProtocolA2AManager() *gateway.DurableA2ATaskManager {
	fixture := newRunFixture()
	return &gateway.DurableA2ATaskManager{Submitter: fixture.submitter, Tasks: fixture.tasks, Events: &terminalBridgeStore{}, Readiness: readinessStub{ready: true}, PollInterval: time.Millisecond}
}

func TestProtocolHTTPHandlerA2ATasksSurviveFacadeReconstruction(t *testing.T) {
	fixture := newRunFixture()
	trusted := gateway.ServerInvocationContext{Tenant: fixture.route.Tenant, PrincipalID: "principal-a", UserID: fixture.route.UserID,
		SessionID: fixture.route.SessionID, Protocol: "a2a", IdempotencyKey: "a2a-stable-key", CanRun: true, CanRead: true, CanCancel: true}
	newHandler := func() http.Handler {
		manager := &gateway.DurableA2ATaskManager{Submitter: fixture.submitter, Tasks: fixture.tasks, Events: &terminalBridgeStore{}, Readiness: readinessStub{ready: true}, PollInterval: time.Millisecond}
		handler, err := gateway.NewProtocolHTTPHandler(gateway.ProtocolHTTPOptions{Runner: gateway.CanonicalRunner{Next: &protocolRunnerStub{}},
			Resolver: staticInvocationResolver{trusted: trusted}, Readiness: readinessStub{ready: true}, MaxBody: 1 << 20, RunTimeout: time.Second,
			A2A: manager, PublicURL: "https://gateway.example.test"})
		if err != nil {
			t.Fatal(err)
		}
		return handler
	}
	sendBody := `{"jsonrpc":"2.0","id":"rpc-1","method":"message/send","params":{"message":{"kind":"message","messageId":"untrusted-message","contextId":"untrusted-context","role":"user","parts":[{"kind":"text","text":"hello durable A2A"}]}}}`
	response := serveA2A(t, newHandler(), sendBody)
	if response.Code != http.StatusOK {
		t.Fatalf("send code=%d body=%q", response.Code, response.Body.String())
	}
	var sendResult struct {
		Result struct {
			ID        string `json:"id"`
			ContextID string `json:"contextId"`
			Status    struct {
				State string `json:"state"`
			} `json:"status"`
		} `json:"result"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &sendResult); err != nil || sendResult.Result.ID == "" {
		t.Fatalf("send response=%q err=%v", response.Body.String(), err)
	}
	if sendResult.Result.ContextID != fixture.route.SessionID || sendResult.Result.Status.State != "submitted" {
		t.Fatalf("canonical A2A result=%#v", sendResult.Result)
	}
	status, err := fixture.tasks.GetExecution(context.Background(), gateway.ExecutionKey{TenantID: "tenant-a", RequestID: sendResult.Result.ID})
	if err != nil || status.Envelope.UserID != fixture.route.UserID || status.Envelope.SessionID != fixture.route.SessionID {
		t.Fatalf("durable status=%#v err=%v", status, err)
	}

	// Reconstructing the façade/task manager must not lose query or cancel state.
	getBody := `{"jsonrpc":"2.0","id":"rpc-2","method":"tasks/get","params":{"id":"` + sendResult.Result.ID + `"}}`
	if got := serveA2A(t, newHandler(), getBody); got.Code != http.StatusOK || !strings.Contains(got.Body.String(), sendResult.Result.ID) {
		t.Fatalf("get after reconstruction code=%d body=%q", got.Code, got.Body.String())
	}
	cancelBody := `{"jsonrpc":"2.0","id":"rpc-3","method":"tasks/cancel","params":{"id":"` + sendResult.Result.ID + `"}}`
	if got := serveA2A(t, newHandler(), cancelBody); got.Code != http.StatusOK {
		t.Fatalf("cancel code=%d body=%q", got.Code, got.Body.String())
	}
	status, err = fixture.tasks.GetExecution(context.Background(), gateway.ExecutionKey{TenantID: "tenant-a", RequestID: sendResult.Result.ID})
	if err != nil || !status.CancelRequested {
		t.Fatalf("cancel intent status=%#v err=%v", status, err)
	}

	cardRequest := httptest.NewRequest(http.MethodGet, "/a2a/v1/apps/app/.well-known/agent-card.json", nil)
	cardResponse := httptest.NewRecorder()
	newHandler().ServeHTTP(cardResponse, cardRequest)
	if cardResponse.Code != http.StatusOK || !strings.Contains(cardResponse.Body.String(), `"pushNotifications":false`) ||
		!strings.Contains(cardResponse.Body.String(), `"url":"https://gateway.example.test/a2a/v1/apps/app/"`) {
		t.Fatalf("agent card code=%d body=%q", cardResponse.Code, cardResponse.Body.String())
	}

	draining := readinessStub{ready: false}
	drainingManager := &gateway.DurableA2ATaskManager{Submitter: fixture.submitter, Tasks: fixture.tasks, Events: &terminalBridgeStore{},
		Readiness: draining, PollInterval: time.Millisecond}
	drainingHandler, err := gateway.NewProtocolHTTPHandler(gateway.ProtocolHTTPOptions{Runner: gateway.CanonicalRunner{Next: &protocolRunnerStub{}},
		Resolver: staticInvocationResolver{trusted: trusted}, Readiness: draining, MaxBody: 1 << 20, RunTimeout: time.Second,
		A2A: drainingManager, PublicURL: "https://gateway.example.test"})
	if err != nil {
		t.Fatal(err)
	}
	if got := serveA2A(t, drainingHandler, getBody); got.Code != http.StatusOK {
		t.Fatalf("draining read code=%d body=%q", got.Code, got.Body.String())
	}
	if got := serveA2A(t, drainingHandler, sendBody); got.Code != http.StatusServiceUnavailable {
		t.Fatalf("draining send code=%d body=%q", got.Code, got.Body.String())
	}
}

func serveA2A(t *testing.T, handler http.Handler, body string) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(http.MethodPost, "/a2a/v1/apps/app/", strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}
