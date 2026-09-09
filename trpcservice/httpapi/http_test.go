package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/executor"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/governance"
	"github.com/liuzengh/trpc-agent-service/trpcservice/message"
	"github.com/liuzengh/trpc-agent-service/trpcservice/messaging"
)

type fakeBackend struct {
	readyErr error
	handle   func(context.Context, executor.Request) (executor.Reply, error)
}

type traceBackend struct {
	fakeBackend
	digestV2 bool
}

func (f traceBackend) TraceDigestV2Enabled() bool { return f.digestV2 }

type fakeAsyncBackend struct {
	accepted message.InboundMessage
	accept   func(message.InboundMessage) (channels.AcceptResult, error)
	snapshot messaging.Snapshot
	snapErr  error
	readyErr error
}

func (f *fakeAsyncBackend) Ready(context.Context) error { return f.readyErr }
func (f *fakeAsyncBackend) Handle(context.Context, message.InboundMessage) (message.OutboundMessage, error) {
	return message.OutboundMessage{}, nil
}
func (f *fakeAsyncBackend) Accept(_ context.Context, inbound message.InboundMessage) (channels.AcceptResult, error) {
	f.accepted = inbound
	if f.accept != nil {
		return f.accept(inbound)
	}
	return channels.AcceptResult{TaskID: "task", RequestID: inbound.RequestID, TraceID: inbound.TraceID}, nil
}
func (f *fakeAsyncBackend) Snapshot(context.Context, string, string, string) (messaging.Snapshot, error) {
	return f.snapshot, f.snapErr
}

func (f fakeBackend) Ready(context.Context) error { return f.readyErr }
func (f fakeBackend) Handle(ctx context.Context, req executor.Request) (executor.Reply, error) {
	return f.handle(ctx, req)
}

func TestMessageHandlerSuccessAndIDs(t *testing.T) {
	handler := NewHandler(fakeBackend{handle: func(_ context.Context, req executor.Request) (executor.Reply, error) {
		if req.Channel != "demo" || req.ReceivedAt.IsZero() {
			t.Fatalf("request was not converted to the internal message contract: %#v", req)
		}
		return executor.Reply{Channel: req.Channel, BindingID: req.BindingID, RequestID: req.RequestID, TraceID: req.TraceID, SessionID: "s_test", Text: "ok"}, nil
	}})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/demo/messages", strings.NewReader(`{"binding_id":"demo-binding","message_id":"m1","external_user_id":"u1","conversation_id":"c1","text":"hello"}`))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	var body map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if len(body["request_id"]) != 16 || len(body["trace_id"]) != 32 {
		t.Fatalf("unexpected IDs: %#v", body)
	}
	if _, exists := body["channel"]; exists {
		t.Fatalf("internal channel leaked into HTTP response: %#v", body)
	}
	if _, exists := body["binding_id"]; exists {
		t.Fatalf("internal binding leaked into HTTP response: %#v", body)
	}
}

func TestMessageHandlerGatesTraceParentByDigestV2(t *testing.T) {
	const traceParent = "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01"
	for _, test := range []struct {
		name    string
		enabled bool
		want    string
	}{
		{name: "disabled", enabled: false},
		{name: "enabled", enabled: true, want: traceParent},
	} {
		t.Run(test.name, func(t *testing.T) {
			var received message.InboundMessage
			backend := traceBackend{digestV2: test.enabled, fakeBackend: fakeBackend{handle: func(_ context.Context, req executor.Request) (executor.Reply, error) {
				received = req
				return executor.Reply{RequestID: req.RequestID, TraceID: req.TraceID, SessionID: "session", Text: "ok"}, nil
			}}}
			req := httptest.NewRequest(http.MethodPost, "/api/v1/demo/messages", strings.NewReader(`{"binding_id":"demo-binding","message_id":"m1","external_user_id":"u1","conversation_id":"c1","text":"hello"}`))
			req.Header.Set("traceparent", traceParent)
			rec := httptest.NewRecorder()
			NewHandler(backend).ServeHTTP(rec, req)
			if rec.Code != http.StatusOK {
				t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
			}
			if received.TraceParent != test.want {
				t.Fatalf("trace parent = %q, want %q", received.TraceParent, test.want)
			}
		})
	}
}

func TestMessageHandlerValidationAndErrorMapping(t *testing.T) {
	tests := []struct {
		name   string
		input  string
		err    error
		status int
	}{
		{name: "invalid json", input: "{", status: http.StatusBadRequest},
		{name: "unknown field", input: `{"binding_id":"demo-binding","message_id":"m1","external_user_id":"u1","conversation_id":"c1","text":"hello","tenant_id":"bad"}`, status: http.StatusBadRequest},
		{name: "missing field", input: `{"binding_id":"demo-binding","message_id":"m1","external_user_id":"u1","text":"hello"}`, status: http.StatusBadRequest},
		{name: "empty text", input: `{"binding_id":"demo-binding","message_id":"m1","external_user_id":"u1","conversation_id":"c1","text":"  "}`, status: http.StatusBadRequest},
		{name: "long id", input: `{"binding_id":"demo-binding","message_id":"` + strings.Repeat("m", maxIDBytes+1) + `","external_user_id":"u1","conversation_id":"c1","text":"hello"}`, status: http.StatusBadRequest},
		{name: "long text", input: `{"binding_id":"demo-binding","message_id":"m1","external_user_id":"u1","conversation_id":"c1","text":"` + strings.Repeat("x", maxTextBytes+1) + `"}`, status: http.StatusBadRequest},
		{name: "multiple objects", input: `{"binding_id":"demo-binding","message_id":"m1","external_user_id":"u1","conversation_id":"c1","text":"hello"} {}`, status: http.StatusBadRequest},
		{name: "body too large", input: `{"binding_id":"demo-binding","message_id":"m1","external_user_id":"u1","conversation_id":"c1","text":"` + strings.Repeat("x", maxBodyBytes) + `"}`, status: http.StatusBadRequest},
		{name: "unknown binding", input: `{"binding_id":"other","message_id":"m1","external_user_id":"u1","conversation_id":"c1","text":"hello"}`, err: executor.ErrUnknownBinding, status: http.StatusNotFound},
		{name: "actor forbidden", input: `{"binding_id":"demo-binding","message_id":"m1","external_user_id":"u1","conversation_id":"c1","text":"hello"}`, err: governance.ErrActorForbidden, status: http.StatusForbidden},
		{name: "policy missing", input: `{"binding_id":"demo-binding","message_id":"m1","external_user_id":"u1","conversation_id":"c1","text":"hello"}`, err: governance.ErrPolicyMissing, status: http.StatusServiceUnavailable},
		{name: "dependency unavailable", input: `{"binding_id":"demo-binding","message_id":"m1","external_user_id":"u1","conversation_id":"c1","text":"hello"}`, err: executor.ErrDependencyUnavailable, status: http.StatusServiceUnavailable},
		{name: "configuration unavailable", input: `{"binding_id":"demo-binding","message_id":"m1","external_user_id":"u1","conversation_id":"c1","text":"hello"}`, err: executor.ErrConfigurationUnavailable, status: http.StatusServiceUnavailable},
		{name: "runner draining", input: `{"binding_id":"demo-binding","message_id":"m1","external_user_id":"u1","conversation_id":"c1","text":"hello"}`, err: executor.ErrRunnerDraining, status: http.StatusServiceUnavailable},
		{name: "timeout", input: `{"binding_id":"demo-binding","message_id":"m1","external_user_id":"u1","conversation_id":"c1","text":"hello"}`, err: executor.ErrAgentTimeout, status: http.StatusGatewayTimeout},
		{name: "worker lost", input: `{"binding_id":"demo-binding","message_id":"m1","external_user_id":"u1","conversation_id":"c1","text":"hello"}`, err: executor.ErrWorkerLost, status: http.StatusBadGateway},
		{name: "failure", input: `{"binding_id":"demo-binding","message_id":"m1","external_user_id":"u1","conversation_id":"c1","text":"hello"}`, err: executor.ErrEmptyAgentResponse, status: http.StatusBadGateway},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler := NewHandler(fakeBackend{handle: func(_ context.Context, _ executor.Request) (executor.Reply, error) {
				return executor.Reply{}, tt.err
			}})
			req := httptest.NewRequest(http.MethodPost, "/api/v1/demo/messages", strings.NewReader(tt.input))
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)
			if rec.Code != tt.status {
				t.Fatalf("status = %d, want %d, body = %s", rec.Code, tt.status, rec.Body.String())
			}
		})
	}
}

func TestWorkerLostErrorMapping(t *testing.T) {
	handler := NewHandler(fakeBackend{handle: func(context.Context, executor.Request) (executor.Reply, error) {
		return executor.Reply{TraceID: "task-trace"}, executor.ErrWorkerLost
	}})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/demo/messages", strings.NewReader(`{"binding_id":"demo-binding","message_id":"m-worker-lost","external_user_id":"u1","conversation_id":"c1","text":"hello"}`))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502", rec.Code)
	}
	var body map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body["code"] != "worker_lost" || body["trace_id"] != "task-trace" {
		t.Fatalf("unexpected worker_lost response: %#v", body)
	}
}

func TestMessageHandlerDoesNotExposeBackendErrors(t *testing.T) {
	const secret = "api-key-and-upstream-response-must-not-leak"
	handler := NewHandler(fakeBackend{handle: func(context.Context, executor.Request) (executor.Reply, error) {
		return executor.Reply{}, errors.New(secret)
	}})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/demo/messages", strings.NewReader(`{"binding_id":"demo-binding","message_id":"m1","external_user_id":"u1","conversation_id":"c1","text":"hello"}`))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502", rec.Code)
	}
	if strings.Contains(rec.Body.String(), secret) {
		t.Fatalf("response leaked backend error: %s", rec.Body.String())
	}
	var body map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"code", "message", "request_id", "trace_id"} {
		if body[field] == "" {
			t.Fatalf("error response field %s is empty: %#v", field, body)
		}
	}
}

func TestPhase3ErrorMappingAndOriginalTaskTrace(t *testing.T) {
	tests := []struct {
		name   string
		err    error
		status int
		code   string
	}{
		{name: "conflict", err: gateway.ErrMessageConflict, status: http.StatusConflict, code: "message_conflict"},
		{name: "pending", err: gateway.ErrTaskPending, status: http.StatusGatewayTimeout, code: "task_pending"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler := NewHandler(fakeBackend{handle: func(context.Context, executor.Request) (executor.Reply, error) {
				return executor.Reply{TraceID: "original-task-trace"}, tt.err
			}})
			req := httptest.NewRequest(http.MethodPost, "/api/v1/demo/messages", strings.NewReader(`{"binding_id":"demo-binding","message_id":"m1","external_user_id":"u1","conversation_id":"c1","text":"hello"}`))
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)
			if rec.Code != tt.status {
				t.Fatalf("status = %d, want %d, body = %s", rec.Code, tt.status, rec.Body.String())
			}
			var body map[string]string
			if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
				t.Fatal(err)
			}
			if body["code"] != tt.code || body["trace_id"] != "original-task-trace" || body["request_id"] == "" {
				t.Fatalf("unexpected response body: %#v", body)
			}
		})
	}
}

func TestReadyEndpoint(t *testing.T) {
	handler := NewHandler(fakeBackend{readyErr: errors.New("redis unavailable"), handle: func(context.Context, executor.Request) (executor.Reply, error) { return executor.Reply{}, nil }})
	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rec.Code)
	}
}

func TestWebMessageSubmitUsesTrustedDemoChannel(t *testing.T) {
	backend := &fakeAsyncBackend{}
	handler := NewHandler(backend)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/web/messages", strings.NewReader(`{"binding_id":"custom-demo","message_id":"m1","external_user_id":"u1","conversation_id":"c1","text":"hello"}`))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if backend.accepted.Channel != "demo" || backend.accepted.BindingID != "custom-demo" || backend.accepted.ConversationType != message.ConversationDirect || backend.accepted.RequestID == "" || backend.accepted.TraceID == "" {
		t.Fatalf("accepted web message = %#v", backend.accepted)
	}
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil || body["submitted"] != true || body["message_id"] != "m1" {
		t.Fatalf("response = %#v, error=%v", body, err)
	}

	backend.accept = func(inbound message.InboundMessage) (channels.AcceptResult, error) {
		return channels.AcceptResult{RequestID: inbound.RequestID, TraceID: inbound.TraceID, Terminal: true}, nil
	}
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/api/v1/web/messages", strings.NewReader(`{"binding_id":"custom-demo","message_id":"m1","external_user_id":"u1","conversation_id":"c1","text":"hello"}`)))
	if rec.Code != http.StatusOK {
		t.Fatalf("terminal duplicate status = %d", rec.Code)
	}
}

func TestWebMessageSubmitRejectsWhenBackendIsNotReady(t *testing.T) {
	backend := &fakeAsyncBackend{readyErr: errors.New("control plane unavailable")}
	rec := httptest.NewRecorder()
	NewHandler(backend).ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/api/v1/web/messages", strings.NewReader(`{"binding_id":"demo","message_id":"m1","external_user_id":"u1","conversation_id":"c1","text":"hello"}`)))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil || body["code"] != "not_ready" {
		t.Fatalf("response = %#v, error=%v", body, err)
	}
	if backend.accepted.MessageID != "" {
		t.Fatalf("not-ready backend accepted message: %#v", backend.accepted)
	}
}

func TestWebMessageSubmitRejectsUntrustedAndMultipleFields(t *testing.T) {
	handler := NewHandler(&fakeAsyncBackend{})
	for _, payload := range []string{
		`{"binding_id":"demo","message_id":"m1","external_user_id":"u1","conversation_id":"c1","text":"hello","tenant_id":"forged"}`,
		`{"binding_id":"demo","message_id":"m1","external_user_id":"u1","conversation_id":"c1","text":"hello"} {}`,
	} {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/api/v1/web/messages", strings.NewReader(payload)))
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("payload %q status = %d", payload, rec.Code)
		}
	}

	backend := &fakeAsyncBackend{accept: func(message.InboundMessage) (channels.AcceptResult, error) {
		return channels.AcceptResult{}, executor.ErrUnknownBinding
	}}
	rec := httptest.NewRecorder()
	NewHandler(backend).ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/api/v1/web/messages", strings.NewReader(`{"binding_id":"missing","message_id":"m1","external_user_id":"u1","conversation_id":"c1","text":"hello"}`)))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("unknown binding status = %d", rec.Code)
	}
}

func TestWebSnapshotStatusAndErrorProjection(t *testing.T) {
	tests := []struct {
		name       string
		snapshot   messaging.Snapshot
		wantStatus string
		wantText   string
		wantError  string
	}{
		{name: "submitted", snapshot: messaging.Snapshot{State: messaging.StateQueued, RequestID: "request", TraceID: "trace"}, wantStatus: "submitted"},
		{name: "processing", snapshot: messaging.Snapshot{State: messaging.StateRetryWait, RequestID: "request", TraceID: "trace"}, wantStatus: "processing"},
		{name: "succeeded", snapshot: messaging.Snapshot{State: messaging.StateSucceeded, RequestID: "request", TraceID: "trace", Result: &message.TaskResult{Succeeded: true, Reply: message.OutboundMessage{Text: "answer"}}}, wantStatus: "succeeded", wantText: "answer"},
		{name: "failed", snapshot: messaging.Snapshot{State: messaging.StateFailedTerminal, RequestID: "request", TraceID: "trace", Result: &message.TaskResult{ErrorCode: "model_timeout"}}, wantStatus: "failed", wantError: "model_timeout"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			handler := NewHandler(&fakeAsyncBackend{snapshot: test.snapshot})
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/web/messages/m1?binding_id=custom-demo", nil))
			if rec.Code != http.StatusOK {
				t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
			}
			var body map[string]any
			if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil || body["status"] != test.wantStatus || body["request_id"] != "request" || body["trace_id"] != "trace" {
				t.Fatalf("snapshot response = %#v, error=%v", body, err)
			}
			if test.wantText != "" && body["text"] != test.wantText {
				t.Fatalf("text = %#v", body["text"])
			}
			if test.wantError != "" && body["error_code"] != test.wantError {
				t.Fatalf("error_code = %#v", body["error_code"])
			}
		})
	}

	backend := &fakeAsyncBackend{snapErr: executor.ErrUnknownBinding}
	rec := httptest.NewRecorder()
	NewHandler(backend).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/web/messages/m1?binding_id=missing", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("unknown binding GET status = %d", rec.Code)
	}
}

func TestEmbeddedWebUIUsesConfiguredBindingAndPolling(t *testing.T) {
	rec := httptest.NewRecorder()
	NewHandler(&fakeAsyncBackend{}).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "binding_id:fields.binding.value") || !strings.Contains(rec.Body.String(), "},1500)") {
		t.Fatalf("embedded UI response status=%d body=%s", rec.Code, rec.Body.String())
	}
}

func TestHTTPRuntimeEndToEndWithMockModelAndRedis(t *testing.T) {
	redisServer := miniredis.RunT(t)
	modelServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"e2e","object":"chat.completion","created":1,"model":"mock","choices":[{"index":0,"message":{"role":"assistant","content":"end to end ok"},"finish_reason":"stop"}]}`))
	}))
	defer modelServer.Close()
	cfg := config.Config{
		ModelName: "mock-model", ModelBaseURL: modelServer.URL, ModelAPIKeyEnv: "MOCK_KEY", ModelAPIKey: "test-key",
		ModelRequestTimeout: time.Second, ModelMaxOutput: 128,
		IdentitySecret: []byte("01234567890123456789012345678901"), RedisURL: "redis://" + redisServer.Addr() + "/0", RedisKeyPrefix: "http-e2e",
		BindingID: "demo-binding", TenantID: "tenant-demo", AgentAppID: "assistant", ConfigVersion: "v1",
		AppName: "tenant/tenant-demo/app/assistant",
	}
	runtime, err := executor.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	handler := NewHandler(runtime)

	request := httptest.NewRequest(http.MethodPost, "/api/v1/demo/messages", strings.NewReader(`{"binding_id":"demo-binding","message_id":"m1","external_user_id":"u1","conversation_id":"c1","text":"hello"}`))
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
	var reply executor.Reply
	if err := json.Unmarshal(recorder.Body.Bytes(), &reply); err != nil {
		t.Fatal(err)
	}
	if reply.Text != "end to end ok" || reply.SessionID == "" {
		t.Fatalf("unexpected reply: %#v", reply)
	}
}
