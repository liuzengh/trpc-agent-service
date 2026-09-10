package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/XnLemon/trpc-agent-service/trpcservice/channels"
	"github.com/XnLemon/trpc-agent-service/trpcservice/runtime/budget"
	runtimerunner "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/runner"
	trpcagent "trpc.group/trpc-go/trpc-agent-go/agent"
	trpcevent "trpc.group/trpc-go/trpc-agent-go/event"
	trpcmodel "trpc.group/trpc-go/trpc-agent-go/model"
)

type httpDispatchStub struct {
	mu        sync.Mutex
	events    []DispatchEvent
	err       error
	calls     int
	last      DispatchRequest
	blockCall bool
}

func TestHTTPBudgetRejectionsAreRedacted(t *testing.T) {
	for _, test := range []struct {
		err     error
		status  int
		message string
	}{
		{budget.ErrExceeded, http.StatusTooManyRequests, "budget exceeded"},
		{budget.ErrCostUnavailable, http.StatusServiceUnavailable, "cost configuration unavailable"},
		{budget.ErrUnavailable, http.StatusServiceUnavailable, "budget unavailable"},
	} {
		t.Run(test.message, func(t *testing.T) {
			stub := &httpDispatchStub{err: errors.Join(test.err, errors.New("private ledger detail"))}
			handler := newHTTPTestHandler(t, stub, func() bool { return true })
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, newHTTPChatRequest(http.MethodPost, "/v1/chat", validHTTPChatBody("budget-rejection")))
			body := decodeHTTPBody(t, recorder)
			if recorder.Code != test.status || body["error"] != test.message {
				t.Fatalf("HTTP status=%d body=%v", recorder.Code, body)
			}
		})
	}
}

func TestHTTPHandlerAdminRouteBoundary(t *testing.T) {
	admin := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusNoContent) })
	handler, err := NewHTTPHandler(HTTPConfig{Admin: admin, Ready: func() bool { return true }})
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"/admin/v1", "/admin/v1/tenants"} {
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, path, nil))
		if recorder.Code != http.StatusNoContent {
			t.Fatalf("admin path %s status = %d", path, recorder.Code)
		}
	}
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/admin/v12", nil))
	if recorder.Code != http.StatusNotFound {
		t.Fatalf("near-miss admin path status = %d", recorder.Code)
	}
}

func TestHTTPHandlerDelegatesBrowserRoutesToWeb(t *testing.T) {
	admin := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusNoContent) })
	web := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, "web:"+r.URL.Path) })
	handler, err := NewHTTPHandler(HTTPConfig{Admin: admin, Web: web, Ready: func() bool { return true }})
	if err != nil {
		t.Fatal(err)
	}

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/", nil))
	if recorder.Code != http.StatusOK || recorder.Body.String() != "web:/" {
		t.Fatalf("browser route status=%d body=%q", recorder.Code, recorder.Body.String())
	}

	recorder = httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/admin/v1/tenants", nil))
	if recorder.Code != http.StatusNoContent {
		t.Fatalf("admin route status=%d", recorder.Code)
	}
}

func TestHTTPHandlerAdminAuthRouteBoundary(t *testing.T) {
	auth := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusNoContent) })
	handler, err := NewHTTPHandler(HTTPConfig{AdminAuth: auth, Ready: func() bool { return true }})
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"/admin/auth/login", "/admin/auth/session", "/admin/auth/logout"} {
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, path, nil))
		if recorder.Code != http.StatusNoContent {
			t.Fatalf("admin auth path %s status = %d", path, recorder.Code)
		}
	}
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/admin/authentic", nil))
	if recorder.Code != http.StatusNotFound {
		t.Fatalf("near-miss admin auth path status = %d", recorder.Code)
	}
}

type nilDispatchService struct{}

func (nilDispatchService) Dispatch(context.Context, DispatchRequest) (<-chan DispatchEvent, error) {
	return nil, nil
}

func (stub *httpDispatchStub) Dispatch(ctx context.Context, request DispatchRequest) (<-chan DispatchEvent, error) {
	stub.mu.Lock()
	stub.calls++
	stub.last = request
	err := stub.err
	events := append([]DispatchEvent(nil), stub.events...)
	blockCall := stub.blockCall
	stub.mu.Unlock()
	if err != nil {
		return nil, err
	}
	if blockCall {
		output := make(chan DispatchEvent)
		go func() {
			<-ctx.Done()
			close(output)
		}()
		return output, nil
	}
	output := make(chan DispatchEvent, len(events))
	for _, event := range events {
		output <- event
	}
	close(output)
	return output, nil
}

func (stub *httpDispatchStub) snapshot() (int, DispatchRequest) {
	stub.mu.Lock()
	defer stub.mu.Unlock()
	return stub.calls, stub.last
}

func newHTTPTestHandler(t *testing.T, stub *httpDispatchStub, ready func() bool) *HTTPHandler {
	t.Helper()
	identity := APIIdentity{TenantID: "t_01J1K9ZQTVE4PAWF1TSB2WMHNP", AppID: "app_01J1K9ZQTVE4PAWF1TSB2WMHNP", SubjectID: "api-subject"}
	authenticator, err := NewStaticAPIAuthenticator(map[string]APIIdentity{"credential": identity})
	if err != nil {
		t.Fatal(err)
	}
	limiter, err := NewTenantLimiter(TenantLimiterConfig{MaxConcurrent: 8, MaxRequests: 100, Window: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	idempotency, err := NewIdempotencyStore(IdempotencyConfig{TTL: time.Minute, MaxEntries: 100})
	if err != nil {
		t.Fatal(err)
	}
	handler, err := NewHTTPHandler(HTTPConfig{Dispatcher: stub, Authenticator: authenticator, Ready: ready, Limiter: limiter, Idempotency: idempotency, RequestTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(handler.BeginShutdown)
	return handler
}

func newHTTPChatRequest(method, path, body string) *http.Request {
	request := httptest.NewRequest(method, path, strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer credential")
	return request
}

func validHTTPChatBody(externalMessageID string) string {
	return `{"content":"  hello  ","external_message_id":"` + externalMessageID + `","external_user_id":"user","conversation_kind":"direct","external_peer_id":"peer"}`
}

func decodeHTTPBody(t *testing.T, recorder *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var body map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatalf("response JSON = %q: %v", recorder.Body.String(), err)
	}
	return body
}

func TestHTTPHandlerHealthReadinessAndConfigurationEdges(t *testing.T) {
	if _, err := NewHTTPHandler(HTTPConfig{MaxBodyBytes: -1}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("negative body limit error = %v", err)
	}
	if _, err := NewHTTPHandler(HTTPConfig{RequestTimeout: -time.Second}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("negative request timeout error = %v", err)
	}
	bare, err := NewHTTPHandler(HTTPConfig{})
	if err != nil {
		t.Fatal(err)
	}
	health := httptest.NewRecorder()
	bare.ServeHTTP(health, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if health.Code != http.StatusOK || health.Body.String() != "ok\n" {
		t.Fatalf("health response = %d %q", health.Code, health.Body.String())
	}
	ready := httptest.NewRecorder()
	bare.ServeHTTP(ready, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if ready.Code != http.StatusServiceUnavailable || decodeHTTPBody(t, ready)["error"] != "not ready" {
		t.Fatalf("bare readiness response = %d %q", ready.Code, ready.Body.String())
	}
	method := httptest.NewRecorder()
	bare.ServeHTTP(method, httptest.NewRequest(http.MethodPost, "/healthz", nil))
	if method.Code != http.StatusMethodNotAllowed || method.Header().Get("Allow") != http.MethodGet {
		t.Fatalf("health method response = %d allow=%q", method.Code, method.Header().Get("Allow"))
	}
	missing := httptest.NewRecorder()
	bare.ServeHTTP(missing, httptest.NewRequest(http.MethodGet, "/missing", nil))
	if missing.Code != http.StatusNotFound {
		t.Fatalf("missing route status = %d", missing.Code)
	}
	bare.BeginShutdown()
	if bare.Ready() {
		t.Fatal("bare handler became ready after shutdown")
	}

	stub := &httpDispatchStub{}
	handler := newHTTPTestHandler(t, stub, func() bool { return true })
	ready = httptest.NewRecorder()
	handler.ServeHTTP(ready, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if ready.Code != http.StatusOK || ready.Body.String() != "ready\n" {
		t.Fatalf("configured readiness response = %d %q", ready.Code, ready.Body.String())
	}
	handler.BeginShutdown()
	ready = httptest.NewRecorder()
	handler.ServeHTTP(ready, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if ready.Code != http.StatusServiceUnavailable {
		t.Fatalf("shutdown readiness response = %d", ready.Code)
	}
}

func TestHTTPHandlerShutdownDefersOwnedStateClose(t *testing.T) {
	handler, err := NewHTTPHandler(HTTPConfig{})
	if err != nil {
		t.Fatal(err)
	}
	fixture := newGatewayFixture(t)
	principal := mustAPIPrincipal(t, fixture.tenant.TenantID, fixture.app.AppID)
	message := InboundMessage{
		Content: "in-flight", ExternalUserID: "user", ConversationKind: channels.ConversationDirect,
		ExternalPeerID: "peer", ExternalMessageID: "message",
	}
	claim, _, err := handler.idempotency.Begin(context.Background(), principal, message)
	if err != nil {
		t.Fatal(err)
	}
	handler.BeginShutdown()
	if _, _, err := handler.idempotency.Begin(context.Background(), principal, message); !errors.Is(err, ErrDuplicateMessage) {
		t.Fatalf("BeginShutdown closed in-flight idempotency state: %v", err)
	}
	if err := claim.Complete([]DispatchEvent{{Type: DispatchEventDone, Done: true}}); err != nil {
		t.Fatal(err)
	}
	if err := handler.Close(); err != nil {
		t.Fatal(err)
	}
	if err := handler.Close(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := handler.idempotency.Begin(context.Background(), principal, message); !errors.Is(err, ErrClosed) {
		t.Fatalf("closed idempotency state accepted a new claim: %v", err)
	}
}

func TestHTTPJSONChatAuthenticatesNormalizesAndRedacts(t *testing.T) {
	stub := &httpDispatchStub{events: []DispatchEvent{
		{Type: DispatchEventMessage, Text: "hello"},
		{Type: DispatchEventDone, Status: "complete", Done: true},
	}}
	handler := newHTTPTestHandler(t, stub, func() bool { return true })
	request := newHTTPChatRequest(http.MethodPost, "/v1/chat", validHTTPChatBody(""))
	request.Header.Set(traceIDHeader, "trace-123")
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK || !strings.HasPrefix(recorder.Header().Get("Content-Type"), "application/json") {
		t.Fatalf("JSON response = %d headers=%v body=%q", recorder.Code, recorder.Header(), recorder.Body.String())
	}
	body := decodeHTTPBody(t, recorder)
	requestID, ok := body["request_id"].(string)
	if !ok || requestID == "" || body["trace_id"] != "trace-123" || body["text"] != "hello" || body["status"] != "complete" || body["done"] != true {
		t.Fatalf("JSON response body = %+v", body)
	}
	calls, dispatched := stub.snapshot()
	if calls != 1 || dispatched.Principal.Kind() != PrincipalAPI || dispatched.RequestID != requestID || dispatched.TraceID != "trace-123" || dispatched.Message.Content != "hello" || dispatched.Message.ExternalMessageID != requestID {
		t.Fatalf("dispatch request = calls=%d request=%+v", calls, dispatched)
	}

	stub.err = errors.New("provider secret endpoint")
	errorRequest := newHTTPChatRequest(http.MethodPost, "/v1/chat", validHTTPChatBody("error-message"))
	errorRecorder := httptest.NewRecorder()
	handler.ServeHTTP(errorRecorder, errorRequest)
	if errorRecorder.Code != http.StatusInternalServerError || strings.Contains(errorRecorder.Body.String(), "provider secret") || !strings.Contains(errorRecorder.Body.String(), "gateway error") {
		t.Fatalf("redacted dispatch error = %d %q", errorRecorder.Code, errorRecorder.Body.String())
	}
}

func TestHTTPChatStrictInputAuthenticationAndCorrelation(t *testing.T) {
	stub := &httpDispatchStub{events: []DispatchEvent{{Type: DispatchEventDone, Status: "complete", Done: true}}}
	handler := newHTTPTestHandler(t, stub, func() bool { return true })
	cases := []struct {
		name       string
		request    func() *http.Request
		statusCode int
	}{
		{name: "unknown field", request: func() *http.Request {
			request := newHTTPChatRequest(http.MethodPost, "/v1/chat", `{"content":"hello","external_user_id":"user","conversation_kind":"direct","external_peer_id":"peer","tenant_id":"forged"}`)
			return request
		}, statusCode: http.StatusBadRequest},
		{name: "missing content type", request: func() *http.Request {
			request := newHTTPChatRequest(http.MethodPost, "/v1/chat", validHTTPChatBody("missing-content-type"))
			request.Header.Del("Content-Type")
			return request
		}, statusCode: http.StatusBadRequest},
		{name: "invalid trace", request: func() *http.Request {
			request := newHTTPChatRequest(http.MethodPost, "/v1/chat", validHTTPChatBody("invalid-trace"))
			request.Header.Set(traceIDHeader, "bad\ntrace")
			return request
		}, statusCode: http.StatusBadRequest},
		{name: "unknown credential", request: func() *http.Request {
			request := newHTTPChatRequest(http.MethodPost, "/v1/chat", validHTTPChatBody("unknown-credential"))
			request.Header.Set("Authorization", "Bearer unknown")
			return request
		}, statusCode: http.StatusUnauthorized},
		{name: "invalid message", request: func() *http.Request {
			return newHTTPChatRequest(http.MethodPost, "/v1/chat", `{"content":"hello","external_user_id":"user","conversation_kind":"direct"}`)
		}, statusCode: http.StatusBadRequest},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, test.request())
			if recorder.Code != test.statusCode {
				t.Fatalf("status = %d body=%q", recorder.Code, recorder.Body.String())
			}
			body := decodeHTTPBody(t, recorder)
			if body["error"] == "" {
				t.Fatalf("missing stable error body: %+v", body)
			}
			if test.name == "invalid trace" {
				if requestID, ok := body["request_id"].(string); !ok || requestID == "" {
					t.Fatalf("invalid trace response lost request ID: %+v", body)
				}
			}
		})
	}

	large := newHTTPChatRequest(http.MethodPost, "/v1/chat", strings.Repeat("x", 2<<20))
	largeRecorder := httptest.NewRecorder()
	handler.ServeHTTP(largeRecorder, large)
	if largeRecorder.Code != http.StatusBadRequest {
		t.Fatalf("large body status = %d", largeRecorder.Code)
	}
	noAuth := newHTTPChatRequest(http.MethodPost, "/v1/chat", validHTTPChatBody("no-auth"))
	noAuth.Header.Del("Authorization")
	noAuthRecorder := httptest.NewRecorder()
	handler.ServeHTTP(noAuthRecorder, noAuth)
	if noAuthRecorder.Code != http.StatusUnauthorized {
		t.Fatalf("missing auth status = %d", noAuthRecorder.Code)
	}
}

func TestHTTPSSEReplayAndDuplicateProtection(t *testing.T) {
	stub := &httpDispatchStub{events: []DispatchEvent{
		{Type: DispatchEventMessage, Text: "streamed"},
		{Type: DispatchEventDone, Status: "complete", Done: true},
	}}
	handler := newHTTPTestHandler(t, stub, func() bool { return true })
	request := newHTTPChatRequest(http.MethodPost, "/v1/chat/stream", validHTTPChatBody("stream-message"))
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK || !strings.HasPrefix(recorder.Header().Get("Content-Type"), "text/event-stream") || !strings.Contains(recorder.Body.String(), "event: message") || !strings.Contains(recorder.Body.String(), "event: done") {
		t.Fatalf("SSE response = %d headers=%v body=%q", recorder.Code, recorder.Header(), recorder.Body.String())
	}
	calls, _ := stub.snapshot()
	if calls != 1 {
		t.Fatalf("first SSE dispatch calls = %d", calls)
	}
	duplicate := newHTTPChatRequest(http.MethodPost, "/v1/chat/stream", validHTTPChatBody("stream-message"))
	duplicateRecorder := httptest.NewRecorder()
	handler.ServeHTTP(duplicateRecorder, duplicate)
	if duplicateRecorder.Code != http.StatusOK || !strings.Contains(duplicateRecorder.Header().Get("Content-Type"), "text/event-stream") || !strings.Contains(duplicateRecorder.Body.String(), "streamed") {
		t.Fatalf("completed duplicate response = %d %q", duplicateRecorder.Code, duplicateRecorder.Body.String())
	}
	calls, _ = stub.snapshot()
	if calls != 1 {
		t.Fatalf("duplicate started another dispatch: %d", calls)
	}
	pendingStub := &httpDispatchStub{blockCall: true}
	pendingHandler := newHTTPTestHandler(t, pendingStub, func() bool { return true })
	first := newHTTPChatRequest(http.MethodPost, "/v1/chat", validHTTPChatBody("pending-message"))
	firstCtx, cancel := context.WithCancel(first.Context())
	first = first.WithContext(firstCtx)
	finished := make(chan struct{})
	go func() {
		recorder := httptest.NewRecorder()
		pendingHandler.ServeHTTP(recorder, first)
		close(finished)
	}()
	deadline := time.After(time.Second)
	for {
		calls, _ := pendingStub.snapshot()
		if calls == 1 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("pending dispatch did not start")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	second := newHTTPChatRequest(http.MethodPost, "/v1/chat", validHTTPChatBody("pending-message"))
	secondRecorder := httptest.NewRecorder()
	pendingHandler.ServeHTTP(secondRecorder, second)
	if secondRecorder.Code != http.StatusConflict {
		t.Fatalf("pending duplicate status = %d body=%q", secondRecorder.Code, secondRecorder.Body.String())
	}
	cancel()
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("canceled pending request did not finish")
	}
}

func TestHTTPJSONUsesRealDispatcherExecutionPath(t *testing.T) {
	runner := &testRunner{runFn: func(context.Context, string, string, trpcmodel.Message, ...trpcagent.RunOption) (<-chan *trpcevent.Event, error) {
		events := make(chan *trpcevent.Event, 2)
		events <- &trpcevent.Event{Response: &trpcmodel.Response{Choices: []trpcmodel.Choice{{Delta: trpcmodel.Message{Content: "real"}}}}}
		events <- &trpcevent.Event{Response: &trpcmodel.Response{Done: true}}
		close(events)
		return events, nil
	}}
	dispatcher, principal := newTestDispatcher(t, runner)
	authenticated, err := newAuthenticatedAPI(APIIdentity{TenantID: principal.TenantID(), AppID: principal.AppID(), SubjectID: principal.SubjectID()})
	if err != nil {
		t.Fatal(err)
	}
	authenticator := APIAuthenticatorFunc(func(context.Context, *http.Request) (AuthenticatedAPI, error) { return authenticated, nil })
	limiter, err := NewTenantLimiter(TenantLimiterConfig{MaxConcurrent: 2, MaxRequests: 10, Window: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	idempotency, err := NewIdempotencyStore(IdempotencyConfig{TTL: time.Minute, MaxEntries: 10})
	if err != nil {
		t.Fatal(err)
	}
	handler, err := NewHTTPHandler(HTTPConfig{Dispatcher: dispatcher, Authenticator: authenticator, Ready: dispatcher.Ready, Limiter: limiter, Idempotency: idempotency})
	if err != nil {
		t.Fatal(err)
	}
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, newHTTPChatRequest(http.MethodPost, "/v1/chat", validHTTPChatBody("real-dispatch")))
	if recorder.Code != http.StatusOK || decodeHTTPBody(t, recorder)["text"] != "real" {
		t.Fatalf("real dispatcher response = %d %q", recorder.Code, recorder.Body.String())
	}
	if handler.Handler() == nil {
		t.Fatal("HTTP handler returned a nil net/http handler")
	}
}

func TestHTTPStreamWriterAndErrorMappingEdges(t *testing.T) {
	stub := &httpDispatchStub{events: []DispatchEvent{{Type: DispatchEventDone, Status: "complete", Done: true}}}
	handler := newHTTPTestHandler(t, stub, func() bool { return true })
	noFlush := &noFlushResponseWriter{header: make(http.Header)}
	handler.ServeHTTP(noFlush, newHTTPChatRequest(http.MethodPost, "/v1/chat/stream", validHTTPChatBody("no-flush")))
	if noFlush.status != http.StatusInternalServerError || !strings.Contains(noFlush.body, "streaming unavailable") {
		t.Fatalf("non-flusher response = %d %q", noFlush.status, noFlush.body)
	}
	for _, test := range []struct {
		err    error
		status int
	}{
		{err: ErrUnauthenticated, status: http.StatusUnauthorized},
		{err: ErrInvalid, status: http.StatusBadRequest},
		{err: ErrRateLimited, status: http.StatusTooManyRequests},
		{err: ErrDuplicateMessage, status: http.StatusConflict},
		{err: ErrNotReady, status: http.StatusServiceUnavailable},
		{err: ErrExecution, status: http.StatusBadGateway},
		{err: errors.New("secret endpoint"), status: http.StatusInternalServerError},
	} {
		status, message := mapHTTPError(test.err)
		if status != test.status || strings.Contains(message, "secret") {
			t.Fatalf("mapped %v = %d %q", test.err, status, message)
		}
	}
	if status, message := mapHTTPError(auditWriteFailure()); status != http.StatusBadGateway || message != ErrAuditWriteFailed.Error() {
		t.Fatalf("mapped audit write failure = %d %q", status, message)
	}
	if !isJSONContentType("application/json; charset=utf-8") || isJSONContentType("text/plain") || isJSONContentType("") {
		t.Fatal("JSON content type validation is incorrect")
	}
	if _, _, err := requestCorrelation(httptest.NewRequest(http.MethodPost, "/", nil)); err != nil {
		t.Fatal(err)
	}
	if _, _, err := requestCorrelation(func() *http.Request {
		request := httptest.NewRequest(http.MethodPost, "/", nil)
		request.Header.Set(requestIDHeader, "bad\nrequest")
		return request
	}()); err == nil {
		t.Fatal("invalid request correlation was accepted")
	}
	if writer, ok := io.Writer(httptest.NewRecorder()).(http.Flusher); !ok || writer == nil {
		t.Fatal("httptest recorder is not a flushing writer")
	}
}

func TestHTTPAdditionalBoundaryBranches(t *testing.T) {
	stub := &httpDispatchStub{events: []DispatchEvent{{Type: DispatchEventDone, Status: "complete", Done: true}}}
	notReady := newHTTPTestHandler(t, stub, func() bool { return false })
	nilRequestRecorder := httptest.NewRecorder()
	notReady.ServeHTTP(nilRequestRecorder, nil)
	notReadyRecorder := httptest.NewRecorder()
	notReady.ServeHTTP(notReadyRecorder, newHTTPChatRequest(http.MethodPost, "/v1/chat", validHTTPChatBody("not-ready")))
	if notReadyRecorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("not-ready chat status = %d", notReadyRecorder.Code)
	}
	badAuth := APIAuthenticatorFunc(func(context.Context, *http.Request) (AuthenticatedAPI, error) { return AuthenticatedAPI{}, nil })
	badAuthHandler, err := NewHTTPHandler(HTTPConfig{Dispatcher: stub, Authenticator: badAuth, Ready: func() bool { return true }})
	if err != nil {
		t.Fatal(err)
	}
	badAuthRecorder := httptest.NewRecorder()
	badAuthHandler.ServeHTTP(badAuthRecorder, newHTTPChatRequest(http.MethodPost, "/v1/chat", validHTTPChatBody("bad-proof")))
	if badAuthRecorder.Code != http.StatusUnauthorized {
		t.Fatalf("bad authenticator status = %d", badAuthRecorder.Code)
	}

	nilStreamHandler, err := NewHTTPHandler(HTTPConfig{Dispatcher: nilDispatchService{}, Authenticator: notReady.authenticator, Ready: func() bool { return true }})
	if err != nil {
		t.Fatal(err)
	}
	nilStreamRecorder := httptest.NewRecorder()
	nilStreamHandler.ServeHTTP(nilStreamRecorder, newHTTPChatRequest(http.MethodPost, "/v1/chat", validHTTPChatBody("nil-stream")))
	if nilStreamRecorder.Code != http.StatusBadGateway {
		t.Fatalf("nil dispatch stream status = %d", nilStreamRecorder.Code)
	}

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := collectHTTPEvents(canceled, make(chan DispatchEvent)); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled event collection error = %v", err)
	}
	for _, test := range []struct {
		err    error
		status int
	}{
		{err: context.Canceled, status: http.StatusRequestTimeout},
		{err: context.DeadlineExceeded, status: http.StatusGatewayTimeout},
		{err: ErrIdempotencyCapacity, status: http.StatusServiceUnavailable},
		{err: ErrPlanUnavailable, status: http.StatusBadGateway},
		{err: runtimerunner.ErrRunnerUnavailable, status: http.StatusBadGateway},
		{err: ErrClosed, status: http.StatusServiceUnavailable},
		{err: nil, status: http.StatusInternalServerError},
	} {
		status, _ := mapHTTPError(test.err)
		if status != test.status {
			t.Fatalf("mapped status for %v = %d", test.err, status)
		}
	}
	response := finalChatResponse("request", "trace", []DispatchEvent{
		{Type: DispatchEventStatus, Status: "partial"},
		{Type: DispatchEventError, Error: "execution failed"},
		{Type: DispatchEventDone, Status: "error", Done: true},
	})
	if response.Status != "error" || response.Error == "" || !response.Done {
		t.Fatalf("final error response = %+v", response)
	}
	failingWriter := &errorWriter{}
	if err := writeSSEEvent(failingWriter, DispatchEvent{Type: DispatchEventMessage, Text: "x"}); err == nil {
		t.Fatal("SSE write failure was not returned")
	}
	noFlush := &noFlushResponseWriter{header: make(http.Header)}
	stubHandler := newHTTPTestHandler(t, stub, func() bool { return true })
	stubHandler.writeReplayStream(noFlush, nil)
	if noFlush.status != http.StatusInternalServerError {
		t.Fatalf("non-flusher replay status = %d", noFlush.status)
	}
}

type noFlushResponseWriter struct {
	header http.Header
	body   string
	status int
}

type errorWriter struct{}

func (*errorWriter) Write([]byte) (int, error) { return 0, errors.New("write failed") }

func (writer *noFlushResponseWriter) Header() http.Header { return writer.header }

func (writer *noFlushResponseWriter) WriteHeader(status int) { writer.status = status }

func (writer *noFlushResponseWriter) Write(value []byte) (int, error) {
	if writer.status == 0 {
		writer.status = http.StatusOK
	}
	writer.body += string(value)
	return len(value), nil
}
