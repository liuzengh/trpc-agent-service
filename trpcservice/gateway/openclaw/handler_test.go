package openclaw

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/audit"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/dispatcher"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/idempotency"
	servicelog "github.com/liuzengh/trpc-agent-service/trpcservice/log"
	servicemetrics "github.com/liuzengh/trpc-agent-service/trpcservice/metrics"
	"github.com/liuzengh/trpc-agent-service/trpcservice/policy"
	serviceruntime "github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/sessioncoord"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/worker"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

type captureSubmitter struct {
	request gateway.RunRequest
	calls   atomic.Int32
}

type fakeCanceler struct{ requestID string }
type fakeApprover struct{ tenantID, requestID, toolName string }

func (canceler *fakeCanceler) Cancel(_ string, requestID string) bool {
	canceler.requestID = requestID
	return true
}
func (approver *fakeApprover) Grant(tenantID, requestID, toolName string) bool {
	approver.tenantID, approver.requestID, approver.toolName = tenantID, requestID, toolName
	return true
}

func (submitter *captureSubmitter) Submit(request gateway.RunRequest) error {
	submitter.request = request
	submitter.calls.Add(1)
	return nil
}

func TestHealthAndReadinessHaveSeparateSemantics(t *testing.T) {
	ready := atomic.Bool{}
	handler := (&Handler{Readiness: func(context.Context) error {
		if !ready.Load() {
			return fmt.Errorf("database password must not reach response")
		}
		return nil
	}}).RoutesHandler()

	health := httptest.NewRecorder()
	handler.ServeHTTP(health, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if health.Code != http.StatusOK {
		t.Fatalf("health status=%d body=%s", health.Code, health.Body.String())
	}
	unready := httptest.NewRecorder()
	handler.ServeHTTP(unready, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if unready.Code != http.StatusServiceUnavailable || strings.Contains(unready.Body.String(), "password") {
		t.Fatalf("readiness status=%d body=%s", unready.Code, unready.Body.String())
	}
	ready.Store(true)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("readiness status=%d body=%s", response.Code, response.Body.String())
	}
}

func TestMessageAuthenticatesCanonicalizesClaimsAndAcknowledgesDuplicate(t *testing.T) {
	routes, _ := NewStaticRoutes(Route{TenantID: "tenant-a", AppID: "assistant", BindingID: "binding-a", ChannelType: tenant.ChannelTypeHTTP, ConfigVersion: 1, Credential: "secret-a"})
	inbox := idempotency.NewMemoryStore()
	submitter := &captureSubmitter{}
	handler := (&Handler{Routes: routes, Inbox: inbox, Submitter: submitter, ClaimOwner: "gateway-a"}).RoutesHandler()
	body := `{"channel":"http","from":"external-user","message_id":"message-1","text":"hello"}`
	first := request(t, handler, "/v1/gateway/messages", "binding-a", "secret-a", "trace-a", body)
	if first.Code != http.StatusAccepted {
		t.Fatalf("status=%d body=%s", first.Code, first.Body.String())
	}
	var accepted MessageResponse
	if err := json.Unmarshal(first.Body.Bytes(), &accepted); err != nil {
		t.Fatal(err)
	}
	if accepted.Duplicate || accepted.TraceID != "trace-a" || submitter.calls.Load() != 1 {
		t.Fatalf("response=%+v calls=%d", accepted, submitter.calls.Load())
	}
	if submitter.request.TenantID != "tenant-a" || submitter.request.UserID != "http/binding-a/external-user" || submitter.request.SessionID != "dm/binding-a/external-user" || submitter.request.ConfigVersion != 1 {
		t.Fatalf("request=%+v", submitter.request)
	}
	second := request(t, handler, "/v1/gateway/messages", "binding-a", "secret-a", "trace-b", body)
	var duplicate MessageResponse
	_ = json.Unmarshal(second.Body.Bytes(), &duplicate)
	if !duplicate.Duplicate || duplicate.TraceID != "trace-a" || submitter.calls.Load() != 1 {
		t.Fatalf("response=%+v calls=%d", duplicate, submitter.calls.Load())
	}
	unauthorized := request(t, handler, "/v1/gateway/messages", "binding-a", "wrong", "", body)
	if unauthorized.Code != http.StatusUnauthorized {
		t.Fatalf("unauthorized status=%d", unauthorized.Code)
	}
}

func TestClientCannotChooseSessionOrTenant(t *testing.T) {
	routes, _ := NewStaticRoutes(Route{TenantID: "tenant-a", AppID: "assistant", BindingID: "binding-a", ChannelType: tenant.ChannelTypeHTTP, ConfigVersion: 1, Credential: "secret"})
	handler := (&Handler{Routes: routes, Inbox: idempotency.NewMemoryStore(), Submitter: &captureSubmitter{}, ClaimOwner: "gateway"}).RoutesHandler()
	response := request(t, handler, "/v1/gateway/messages", "binding-a", "secret", "", `{"from":"u","message_id":"m","text":"x","session_id":"tenant-b/session"}`)
	if response.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
}

func TestStatusAndCancelAreAuthenticatedAndTenantScoped(t *testing.T) {
	routes, _ := NewStaticRoutes(Route{TenantID: "tenant-a", AppID: "assistant", BindingID: "binding-a", ChannelType: tenant.ChannelTypeHTTP, ConfigVersion: 1, Credential: "secret"})
	registry := NewRegistry()
	canceler := &fakeCanceler{}
	approver := &fakeApprover{}
	handler := (&Handler{Routes: routes, Inbox: idempotency.NewMemoryStore(), Submitter: &captureSubmitter{}, Status: registry, Canceler: canceler, Approver: approver, ClaimOwner: "gateway"}).RoutesHandler()
	accepted := request(t, handler, "/v1/gateway/messages", "binding-a", "secret", "trace", `{"from":"u","message_id":"m","text":"x"}`)
	var response MessageResponse
	if err := json.Unmarshal(accepted.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	statusRequest := httptest.NewRequest(http.MethodGet, "/v1/gateway/status?request_id="+response.RequestID, nil)
	statusRequest.Header.Set("X-Channel-Binding", "binding-a")
	statusRequest.Header.Set("Authorization", "Bearer secret")
	statusResponse := httptest.NewRecorder()
	handler.ServeHTTP(statusResponse, statusRequest)
	if statusResponse.Code != http.StatusOK || !strings.Contains(statusResponse.Body.String(), "run.accepted") {
		t.Fatalf("status=%d body=%s", statusResponse.Code, statusResponse.Body.String())
	}
	cancelResponse := request(t, handler, "/v1/gateway/cancel", "binding-a", "secret", "", `{"request_id":"`+response.RequestID+`"}`)
	if cancelResponse.Code != http.StatusAccepted || canceler.requestID != response.RequestID {
		t.Fatalf("cancel status=%d request=%q body=%s", cancelResponse.Code, canceler.requestID, cancelResponse.Body.String())
	}
	foreign := request(t, handler, "/v1/gateway/cancel", "binding-a", "secret", "", `{"request_id":"tenant-b/binding-a/m"}`)
	if foreign.Code != http.StatusBadRequest {
		t.Fatalf("foreign cancel status=%d", foreign.Code)
	}
	approved := request(t, handler, "/v1/gateway/approve", "binding-a", "secret", "", `{"request_id":"`+response.RequestID+`","tool_name":"danger"}`)
	if approved.Code != http.StatusAccepted || approver.tenantID != "tenant-a" || approver.requestID != response.RequestID || approver.toolName != "danger" {
		t.Fatalf("approve status=%d approver=%+v body=%s", approved.Code, approver, approved.Body.String())
	}
}

func TestStaticRoutesAllowTenantScopedBindingIDs(t *testing.T) {
	routes, err := NewStaticRoutes(
		Route{TenantID: "tenant-a", AppID: "app", BindingID: "shared", ChannelType: tenant.ChannelTypeHTTP, ConfigVersion: 1, Credential: "secret-a"},
		Route{TenantID: "tenant-b", AppID: "app", BindingID: "shared", ChannelType: tenant.ChannelTypeHTTP, ConfigVersion: 1, Credential: "secret-b"},
	)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct{ credential, tenantID string }{{"secret-a", "tenant-a"}, {"secret-b", "tenant-b"}} {
		route, err := routes.Resolve("shared", test.credential)
		if err != nil || route.TenantID != test.tenantID {
			t.Fatalf("credential=%s route=%+v err=%v", test.credential, route, err)
		}
	}
}

func TestTwoTenantTwoWorkerEndToEndToolMemoryOutboxAndTrace(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	oldProvider := otel.GetTracerProvider()
	otel.SetTracerProvider(provider)
	defer func() {
		_ = provider.Shutdown(context.Background())
		otel.SetTracerProvider(oldProvider)
	}()
	telemetry, err := servicemetrics.New("openclaw-e2e")
	if err != nil {
		t.Fatal(err)
	}
	file := testConfig(t)
	writes := sessioncoord.NewMemoryWriteStore()
	coordinator, _ := sessioncoord.NewCoordinator(writes)
	inbox := idempotency.NewMemoryStore()
	manager, _ := serviceruntime.NewManager(worker.TestRuntimeFactory(writes))
	defer manager.Close(context.Background())
	hub := NewHub()
	processors := []*worker.Processor{
		{WorkerID: "worker-a", Inbox: inbox, Coordinator: coordinator, Writes: writes, Runtimes: manager, Snapshots: gateway.FileSnapshotResolver{File: file}, Publisher: hub, Policy: &policy.Engine{Identity: policy.AuthenticatedIdentityAuthorizer{}}, Telemetry: telemetry},
		{WorkerID: "worker-b", Inbox: inbox, Coordinator: coordinator, Writes: writes, Runtimes: manager, Snapshots: gateway.FileSnapshotResolver{File: file}, Publisher: hub, Policy: &policy.Engine{Identity: policy.AuthenticatedIdentityAuthorizer{}}, Telemetry: telemetry},
	}
	var next atomic.Uint32
	dispatch, _ := dispatcher.New(context.Background(), func(ctx context.Context, request gateway.RunRequest) error {
		return processors[(next.Add(1)-1)%2].Process(ctx, request)
	})
	defer dispatch.Close(context.Background())
	routes, _ := NewStaticRoutes(
		Route{TenantID: "tenant-a", AppID: "assistant", BindingID: "binding-a", ChannelType: tenant.ChannelTypeHTTP, ConfigVersion: 1, Credential: "secret-a"},
		Route{TenantID: "tenant-b", AppID: "assistant", BindingID: "binding-b", ChannelType: tenant.ChannelTypeHTTP, ConfigVersion: 1, Credential: "secret-b"},
	)
	handler := (&Handler{Routes: routes, Inbox: inbox, Submitter: dispatch, Hub: hub, ClaimOwner: "gateway", ClaimTTL: time.Minute, Telemetry: telemetry}).RoutesHandler()
	body := `{"channel":"http","from":"same-user","message_id":"same-message","text":"calculate 6*7"}`
	for _, input := range []struct{ binding, secret, trace string }{{"binding-a", "secret-a", "trace-a"}, {"binding-b", "secret-b", "trace-b"}} {
		response := request(t, handler, "/v1/gateway/messages", input.binding, input.secret, input.trace, body)
		if response.Code != http.StatusAccepted {
			t.Fatalf("%s: %d %s", input.binding, response.Code, response.Body.String())
		}
	}
	for _, expected := range []struct{ tenantID, binding, trace string }{{"tenant-a", "binding-a", "trace-a"}, {"tenant-b", "binding-b", "trace-b"}} {
		inboxID := expected.tenantID + "/" + expected.binding + "/same-message"
		waitFor(t, func() bool {
			out, ok := writes.Outbox(expected.tenantID, "reply:"+inboxID)
			return ok && strings.Contains(out.Text, "42") && out.TraceID == expected.trace && out.ExternalUserID == "same-user" && len(out.TraceContext) == 1 && out.TraceContext["traceparent"] != ""
		})
		key := gateway.SessionKey{TenantID: expected.tenantID, AppID: "assistant", UserID: "http/" + expected.binding + "/same-user", SessionID: "dm/" + expected.binding + "/same-user"}
		head, events, summary, memories := writes.Snapshot(key)
		if head.LastEventSeq != 1 || len(events) != 1 || summary == nil || len(memories) != 1 || events[0].TraceID != expected.trace {
			t.Fatalf("%s snapshot=%+v %v %v %v", expected.tenantID, head, events, summary, memories)
		}
	}
	duplicate := request(t, handler, "/v1/gateway/messages", "binding-a", "secret-a", "trace-new", body)
	var ack MessageResponse
	_ = json.Unmarshal(duplicate.Body.Bytes(), &ack)
	if !ack.Duplicate || next.Load() != 2 {
		t.Fatalf("duplicate=%+v dispatches=%d", ack, next.Load())
	}
	streamBody := `{"channel":"http","from":"same-user","message_id":"stream-message","text":"calculate 6*7"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/gateway/messages:stream", strings.NewReader(streamBody))
	req.Header.Set("X-Channel-Binding", "binding-a")
	req.Header.Set("Authorization", "Bearer secret-a")
	req.Header.Set("X-Trace-ID", "trace-stream")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, req)
	stream := response.Body.String()
	for _, required := range []string{"event: run.started", "event: run.progress", "running_tool", "event: message.completed", "event: run.completed", "trace-stream", "42"} {
		if !strings.Contains(stream, required) {
			t.Fatalf("stream missing %q:\n%s", required, stream)
		}
	}
	key := gateway.SessionKey{TenantID: "tenant-a", AppID: "assistant", UserID: "http/binding-a/same-user", SessionID: "dm/binding-a/same-user"}
	waitFor(t, func() bool { head, _, _, _ := writes.Snapshot(key); return head.LastEventSeq == 2 })
	time.Sleep(50 * time.Millisecond)
	byTrace := make(map[string]map[string]bool)
	for _, span := range exporter.GetSpans() {
		for _, attr := range span.Attributes {
			for _, private := range []string{"same-message", "calculate 6*7", "secret-a", "secret-b", "trace-a", "trace-b"} {
				if strings.Contains(attr.Value.Emit(), private) {
					t.Fatalf("span %q attribute %q leaked caller-controlled data", span.Name, attr.Key)
				}
			}
		}
		if span.SpanContext.TraceState().Len() != 0 {
			t.Fatalf("span %q retained untrusted tracestate", span.Name)
		}
		traceID := span.SpanContext.TraceID().String()
		if byTrace[traceID] == nil {
			byTrace[traceID] = make(map[string]bool)
		}
		byTrace[traceID][span.Name] = true
	}
	for _, names := range byTrace {
		complete := true
		for _, required := range []string{"gateway.callback", "inbox.claim", "worker.run", "session.lease", "runner.execute", "model.stream", "tool.call", "session.write", "memory.summary.write", "outbox.write"} {
			complete = complete && names[required]
		}
		if complete {
			return
		}
	}
	t.Fatalf("no trace covered the full callback-to-outbox chain: %+v", byTrace)
}

func TestLaterSessionTurnDoesNotBuildOrCallRunner(t *testing.T) {
	file := testConfig(t)
	inbox := idempotency.NewMemoryStore()
	writes := sessioncoord.NewMemoryWriteStore()
	coordinator, _ := sessioncoord.NewCoordinator(writes)
	message := func(id string) gateway.InboundMessage {
		return gateway.InboundMessage{TenantID: "tenant-a", AppID: "assistant", BindingID: "binding-a", ExternalMessageID: id, UserID: "http/binding-a/user", SessionID: "dm/binding-a/user", Text: "synthetic", ConfigVersion: 1, ReceivedAt: time.Now()}
	}
	if _, won, err := inbox.Claim(context.Background(), message("first"), "gateway", time.Minute); err != nil || !won {
		t.Fatalf("first claim won=%v err=%v", won, err)
	}
	second, won, err := inbox.Claim(context.Background(), message("second"), "gateway", time.Minute)
	if err != nil || !won || second.InboxSeq != 2 {
		t.Fatalf("second claim=%+v won=%v err=%v", second, won, err)
	}
	var builds atomic.Int32
	factory := worker.TestRuntimeFactory(writes)
	manager, _ := serviceruntime.NewManager(func(snapshot config.RuntimeSnapshot) (serviceruntime.Runtime, error) {
		builds.Add(1)
		return factory(snapshot)
	})
	defer manager.Close(context.Background())
	telemetry, _ := servicemetrics.New("session-order-test")
	processor := &worker.Processor{
		WorkerID: "worker-b", Inbox: inbox, Coordinator: coordinator, Writes: writes,
		Runtimes: manager, Snapshots: gateway.FileSnapshotResolver{File: file},
		Policy: &policy.Engine{Identity: policy.AuthenticatedIdentityAuthorizer{}},
		Audit:  audit.NewMemoryStore(servicelog.NewRedactor(nil, nil)), Telemetry: telemetry,
	}
	if err := processor.Process(context.Background(), second.RunRequest()); err != nil {
		t.Fatal(err)
	}
	if builds.Load() != 0 {
		t.Fatalf("out-of-order request built Runner bundle %d times", builds.Load())
	}
}

func TestDeniedIMIdentityNeverBuildsOrCallsRunner(t *testing.T) {
	file := testConfig(t)
	file.Tenants[0].Apps[0].Channels[0].AllowedUsers = []string{"alice"}
	inbox := idempotency.NewMemoryStore()
	writes := sessioncoord.NewMemoryWriteStore()
	coordinator, _ := sessioncoord.NewCoordinator(writes)
	message := gateway.InboundMessage{
		TenantID: "tenant-a", AppID: "assistant", BindingID: "binding-a",
		ExternalMessageID: "denied-message", ExternalUserID: "mallory",
		UserID: "http/binding-a/mallory", SessionID: "dm/binding-a/mallory",
		Text: "must not reach runner", ConfigVersion: 1, ReceivedAt: time.Now(),
	}
	claim, won, err := inbox.Claim(context.Background(), message, "gateway", time.Minute)
	if err != nil || !won {
		t.Fatalf("claim=%+v won=%v err=%v", claim, won, err)
	}
	var builds atomic.Int32
	factory := worker.TestRuntimeFactory(writes)
	manager, _ := serviceruntime.NewManager(func(snapshot config.RuntimeSnapshot) (serviceruntime.Runtime, error) {
		builds.Add(1)
		return factory(snapshot)
	})
	defer manager.Close(context.Background())
	telemetry, _ := servicemetrics.New("identity-denial-test")
	processor := &worker.Processor{
		WorkerID: "worker-a", Inbox: inbox, Coordinator: coordinator, Writes: writes,
		Runtimes: manager, Snapshots: gateway.FileSnapshotResolver{File: file},
		Policy: &policy.Engine{Identity: policy.AuthenticatedIdentityAuthorizer{}},
		Audit:  audit.NewMemoryStore(servicelog.NewRedactor(nil, nil)), Telemetry: telemetry,
	}
	if err := processor.Process(context.Background(), claim.RunRequest()); !errors.Is(err, policy.ErrIdentityDenied) {
		t.Fatalf("Process() error=%v", err)
	}
	if builds.Load() != 0 {
		t.Fatalf("denied identity built Runner bundle %d times", builds.Load())
	}
}

func testConfig(t *testing.T) *config.File {
	t.Helper()
	var tenants strings.Builder
	for _, id := range []string{"tenant-a", "tenant-b"} {
		tenants.WriteString(fmt.Sprintf(`
- tenant_id: %s
  name: Tenant
  enabled: true
  config_version: 1
  audit: {enabled: true, retention_days: 30, store_content: false}
  apps:
  - app_id: assistant
    name: Assistant
    enabled: true
    config: {instruction: Answer the user.}
    model: {provider: mock, name: offline-mock}
    tools: {allow: [calculator], deny: []}
    channels: [{binding_id: binding-%s, type: http, provider_account_id: local, enabled: true}]
    storage:
      session: {type: inmemory}
      memory: {type: inmemory}
      summary: {type: inmemory}
      artifact: {type: inmemory}
      knowledge: {type: inmemory}
      audit: {type: inmemory}
`, id, strings.TrimPrefix(id, "tenant-")))
	}
	file, err := config.Load(strings.NewReader("schema_version: 1\ntenants:" + tenants.String()))
	if err != nil {
		t.Fatal(err)
	}
	return file
}

func request(t *testing.T, handler http.Handler, path, binding, secret, traceID, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, path, bytes.NewBufferString(body))
	req.Header.Set("X-Channel-Binding", binding)
	req.Header.Set("Authorization", "Bearer "+secret)
	if traceID != "" {
		req.Header.Set("X-Trace-ID", traceID)
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, req)
	return response
}
func waitFor(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition not met")
}
