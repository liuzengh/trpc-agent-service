package ingress

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/auth"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/model"
)

func TestOpenAIHandlerDoesNotProjectTerminalExecutionError(t *testing.T) {
	handler, _, source, _ := newTestOpenAIHandler(t)
	secret := "provider-secret-must-not-cross-http"
	source.events = []gateway.ExecutionEvent{{
		Sequence: 1,
		Event: event.NewResponseEvent("invocation-1", "assistant", &model.Response{
			Object: model.ObjectTypeError,
			Done:   true,
			Error:  &model.ResponseError{Type: model.ErrorTypeAPIError, Message: secret},
		}),
	}}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, validOpenAIRequest())

	if response.Code != http.StatusInternalServerError {
		t.Fatalf("response status = %d, want %d", response.Code, http.StatusInternalServerError)
	}
	if strings.Contains(response.Body.String(), secret) || strings.Contains(response.Body.String(), "data: [DONE]") {
		t.Fatalf("terminal execution error leaked or became successful response: %q", response.Body.String())
	}
}

func TestOpenAIHandlerStreamsCompletionChunksAndSafeLateError(t *testing.T) {
	handler, _, source, _ := newTestOpenAIHandler(t)
	secret := "late-provider-secret"
	request := validOpenAIRequest()
	request.Body = io.NopCloser(bytes.NewBufferString(`{"stream":true,"messages":[{"role":"user","content":"hello"}]}`))
	source.events = []gateway.ExecutionEvent{
		{Sequence: 1, Event: event.NewResponseEvent("invocation-1", "assistant", &model.Response{
			Object:  model.ObjectTypeChatCompletionChunk,
			Choices: []model.Choice{{Delta: model.Message{Role: model.RoleAssistant, Content: "part"}}},
		})},
		{Sequence: 2, Event: event.NewResponseEvent("invocation-1", "assistant", &model.Response{
			Object: model.ObjectTypeError,
			Done:   true,
			Error:  &model.ResponseError{Type: model.ErrorTypeAPIError, Message: secret},
		})},
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)

	body := response.Body.String()
	if response.Code != http.StatusOK || !strings.Contains(body, `"content":"part"`) {
		t.Fatalf("stream response = status %d body %q", response.Code, body)
	}
	if !strings.Contains(body, `"type":"internal_error"`) || strings.Contains(body, secret) || strings.Contains(body, "data: [DONE]") {
		t.Fatalf("stream terminal error was not projected safely: %q", body)
	}
}

func TestOpenAIHandlerRejectsCallerIdentityHeaders(t *testing.T) {
	tests := []struct {
		name   string
		header string
	}{
		{name: "user", header: headerUserID},
		{name: "session principal", header: headerSessionPrincipal},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler, admitter, _, credentials := newTestOpenAIHandler(t)
			request := validOpenAIRequest()
			request.Header.Set(tt.header, "caller-selected")
			response := httptest.NewRecorder()

			handler.ServeHTTP(response, request)

			if response.Code != http.StatusBadRequest {
				t.Fatalf("response status = %d, want %d", response.Code, http.StatusBadRequest)
			}
			if admitter.called || credentials.called {
				t.Fatal("caller identity header reached authentication or admission")
			}
		})
	}
}

func TestOpenAIHandlerRejectsMissingRequestIdentity(t *testing.T) {
	handler, admitter, _, credentials := newTestOpenAIHandler(t)
	request := validOpenAIRequest()
	request.Header.Del(headerSessionID)
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	if response.Code != http.StatusBadRequest {
		t.Fatalf("response status = %d, want %d", response.Code, http.StatusBadRequest)
	}
	if admitter.called || credentials.called {
		t.Fatal("invalid request reached authentication or admission")
	}
}

func TestOpenAIHandlerRejectsOversizedRequestIdentity(t *testing.T) {
	handler, admitter, _, credentials := newTestOpenAIHandler(t)
	request := validOpenAIRequest()
	request.Header.Set(headerSessionID, strings.Repeat("x", maxOpenAIIdentityBytes+1))
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	if response.Code != http.StatusBadRequest {
		t.Fatalf("response status = %d, want %d", response.Code, http.StatusBadRequest)
	}
	if admitter.called || credentials.called {
		t.Fatal("oversized request identity reached authentication or admission")
	}
}

func TestOpenAIHandlerMapsAuthenticationFailure(t *testing.T) {
	handler, admitter, _, credentials := newTestOpenAIHandler(t)
	credentials.err = errors.New("credential store unavailable")
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, validOpenAIRequest())

	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("response status = %d, want %d", response.Code, http.StatusServiceUnavailable)
	}
	if !credentials.called || admitter.called {
		t.Fatal("authentication failure did not stop before admission")
	}
}

func TestOpenAIHandlerMapsDrainingAdmission(t *testing.T) {
	handler, admitter, source, _ := newTestOpenAIHandler(t)
	admitter.err = gateway.ErrAdmissionDraining
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, validOpenAIRequest())

	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("response status = %d, want %d", response.Code, http.StatusServiceUnavailable)
	}
	if response.Header().Get("Retry-After") != retryAfterSeconds {
		t.Fatalf("retry after = %q", response.Header().Get("Retry-After"))
	}
	if !admitter.called || source.requestID != "" {
		t.Fatal("draining admission reached event subscription")
	}
}

func TestOpenAIHandlerMapsBackendAdmissionFailure(t *testing.T) {
	handler, admitter, _, _ := newTestOpenAIHandler(t)
	admitter.err = errors.New("database unavailable")
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, validOpenAIRequest())

	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("response status = %d, want %d", response.Code, http.StatusServiceUnavailable)
	}
}

func TestOpenAIHandlerMapsInvalidArtifactReference(t *testing.T) {
	response := httptest.NewRecorder()
	writeAdmissionError(response, fmt.Errorf("message: %w", gateway.ErrInvalidArtifactRef))

	if response.Code != http.StatusBadRequest {
		t.Fatalf("response status = %d, want %d", response.Code, http.StatusBadRequest)
	}
}

func TestOpenAIHandlerDoesNotAdmitOtherRoutes(t *testing.T) {
	handler, admitter, _, _ := newTestOpenAIHandler(t)
	request := validOpenAIRequest()
	request.URL.Path = "/v1/not-found"
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	if response.Code != http.StatusNotFound {
		t.Fatalf("response status = %d, want %d", response.Code, http.StatusNotFound)
	}
	if admitter.called {
		t.Fatal("unknown route reached admission")
	}
}

func TestOpenAIHandlerRejectsUnsupportedToolCallsBeforeAdmission(t *testing.T) {
	handler, admitter, _, _ := newTestOpenAIHandler(t)
	request := validOpenAIRequest()
	request.Body = io.NopCloser(bytes.NewBufferString(`{
"messages":[{"role":"user","content":"hello","tool_calls":[{"id":"call-1"}]}]
}`))
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	if response.Code != http.StatusBadRequest {
		t.Fatalf("response status = %d, want %d", response.Code, http.StatusBadRequest)
	}
	if admitter.called {
		t.Fatal("unsupported tool call reached admission")
	}
}

func TestOpenAIHandlerRejectsUnsupportedGenerationParameters(t *testing.T) {
	tests := []struct {
		name  string
		field string
		body  string
	}{
		{name: "temperature", field: "temperature", body: `{"temperature":0.2,"messages":[{"role":"user","content":"hello"}]}`},
		{name: "temperature case variant", field: "temperature", body: `{"Temperature":0.2,"messages":[{"role":"user","content":"hello"}]}`},
		{name: "max_tokens", field: "max_tokens", body: `{"max_tokens":10,"messages":[{"role":"user","content":"hello"}]}`},
		{name: "max_tokens case variant", field: "max_tokens", body: `{"MAX_TOKENS":10,"messages":[{"role":"user","content":"hello"}]}`},
		{name: "top_p", field: "top_p", body: `{"top_p":0.5,"messages":[{"role":"user","content":"hello"}]}`},
		{name: "top_p case variant", field: "top_p", body: `{"Top_P":0.5,"messages":[{"role":"user","content":"hello"}]}`},
		{name: "stop", field: "stop", body: `{"stop":["END"],"messages":[{"role":"user","content":"hello"}]}`},
		{name: "stop case variant", field: "stop", body: `{"Stop":["END"],"messages":[{"role":"user","content":"hello"}]}`},
		{name: "presence_penalty", field: "presence_penalty", body: `{"presence_penalty":0.1,"messages":[{"role":"user","content":"hello"}]}`},
		{name: "frequency_penalty", field: "frequency_penalty", body: `{"frequency_penalty":0.1,"messages":[{"role":"user","content":"hello"}]}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler, admitter, _, _ := newTestOpenAIHandler(t)
			request := validOpenAIRequest()
			request.Body = io.NopCloser(bytes.NewBufferString(tt.body))
			response := httptest.NewRecorder()

			handler.ServeHTTP(response, request)

			if response.Code != http.StatusBadRequest {
				t.Fatalf("response status = %d, want %d", response.Code, http.StatusBadRequest)
			}
			if !strings.Contains(response.Body.String(), tt.field) {
				t.Fatalf("response body = %q, want unsupported field %q", response.Body.String(), tt.field)
			}
			if admitter.called {
				t.Fatal("unsupported generation parameter reached admission")
			}
		})
	}
}

func TestOpenAIHandlerMapsAdmissionErrors(t *testing.T) {
	tests := []struct {
		name string
		err  error
		code int
	}{
		{name: "revoked credential", err: auth.ErrCredentialInactive, code: http.StatusForbidden},
		{name: "idempotency conflict", err: gateway.ErrIdempotencyConflict, code: http.StatusConflict},
		{name: "shared rate limit", err: &gateway.AdmissionRateLimitError{RetryAfter: 2 * time.Second}, code: http.StatusTooManyRequests},
		{name: "local admission concurrency", err: gateway.ErrAdmissionConcurrencyLimit, code: http.StatusTooManyRequests},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler, admitter, _, _ := newTestOpenAIHandler(t)
			admitter.err = tt.err
			response := httptest.NewRecorder()

			handler.ServeHTTP(response, validOpenAIRequest())

			if response.Code != tt.code {
				t.Fatalf("response status = %d, want %d", response.Code, tt.code)
			}
		})
	}
}

func TestOpenAIHandlerTimeoutCancelsOnlyDurableEventWait(t *testing.T) {
	credentials := &recordingCredentialStore{credential: auth.Credential{
		ID: "credential-1", TenantID: "tenant-a", AppID: "support", Status: auth.CredentialActive,
	}}
	directory := testDirectory{
		tenant: tenant.Tenant{ID: "tenant-a", Name: "Tenant A", Status: tenant.StatusActive},
		app:    tenant.AgentApp{TenantID: "tenant-a", AppID: "support", Name: "Support", ActiveConfigVersion: "v1", Status: tenant.StatusActive},
	}
	admitter := &recordingAdmitter{}
	source := &recordingEventSource{block: true}
	queued, err := gateway.NewQueuedRunner(gateway.New(admitter), source)
	if err != nil {
		t.Fatalf("new queued runner: %v", err)
	}
	handler, err := NewOpenAIHandlerWithOptions(
		auth.HTTPAPIKeyResolver{Credentials: credentials, Directory: directory},
		queued,
		WithDurableEventWaitTimeout(10*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("new OpenAI handler: %v", err)
	}
	parent, cancelParent := context.WithCancel(context.Background())
	defer cancelParent()
	request := validOpenAIRequest().WithContext(parent)
	response := httptest.NewRecorder()
	started := time.Now()
	handler.ServeHTTP(response, request)
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("HTTP durable wait exceeded timeout bound: %s", elapsed)
	}
	if !admitter.called {
		t.Fatalf("request was not durably admitted before event wait: status=%d body=%q credentials=%t", response.Code, response.Body.String(), credentials.called)
	}
	if admitter.ctx == nil {
		t.Fatal("admission context was not captured")
	}
	if admitter.ctx.Err() != nil {
		t.Fatalf("admission context was canceled by HTTP wait timeout: %v", admitter.ctx.Err())
	}
}

func TestOpenAIHandlerBoundsActiveStreamingAndReleasesOnDisconnect(t *testing.T) {
	handler, admitter, source, _ := newTestOpenAIHandlerWithOptions(t,
		WithActiveStreamingLimit(1),
		WithDurableEventWaitTimeout(5*time.Second),
	)
	source.block = true
	source.started = make(chan struct{})
	parent, cancelParent := context.WithCancel(context.Background())
	defer cancelParent()
	first := validOpenAIStreamRequest().WithContext(parent)
	firstDone := make(chan struct{})
	go func() {
		handler.ServeHTTP(httptest.NewRecorder(), first)
		close(firstDone)
	}()
	select {
	case <-source.started:
	case <-time.After(time.Second):
		t.Fatal("first stream did not enter durable event wait")
	}

	second := validOpenAIStreamRequest()
	second.Header.Set(headerRequestID, "request-2")
	secondResponse := httptest.NewRecorder()
	handler.ServeHTTP(secondResponse, second)
	if secondResponse.Code != http.StatusTooManyRequests {
		t.Fatalf("second stream status = %d, want %d", secondResponse.Code, http.StatusTooManyRequests)
	}
	if admitter.request.RequestID != "request-1" {
		t.Fatalf("stream limit request reached admission: %#v", admitter.request)
	}

	cancelParent()
	select {
	case <-firstDone:
	case <-time.After(2 * time.Second):
		t.Fatal("first stream did not exit after client disconnect")
	}
	source.block = false
	third := validOpenAIStreamRequest()
	third.Header.Set(headerRequestID, "request-3")
	thirdResponse := httptest.NewRecorder()
	handler.ServeHTTP(thirdResponse, third)
	if thirdResponse.Code == http.StatusTooManyRequests {
		t.Fatal("stream slot was not released after disconnect")
	}
}

func TestOpenAIHandlerRejectsUnpersistablePayload(t *testing.T) {
	handler, admitter, _, credentials := newTestOpenAIHandler(t)
	request := validOpenAIRequest()
	request.Body = io.NopCloser(bytes.NewBufferString(
		`{"messages":[{"role":"system","content":"ignored"},{"role":"user","content":"hello"}]}`,
	))
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	if response.Code != http.StatusBadRequest {
		t.Fatalf("response status = %d, want %d", response.Code, http.StatusBadRequest)
	}
	if !credentials.called || admitter.called {
		t.Fatal("unsupported payload did not stop before admission")
	}
}

func validOpenAIRequest() *http.Request {
	request := httptest.NewRequest(
		http.MethodPost,
		"/v1/chat/completions",
		bytes.NewBufferString(`{"model":"ignored","user":"payload-user","messages":[{"role":"user","content":"hello"}]}`),
	)
	request.Header.Set("Authorization", "Bearer test-key")
	request.Header.Set(headerRequestID, "request-1")
	request.Header.Set(headerIdempotencyKey, "idempotency-1")
	request.Header.Set(headerSessionID, "session-1")
	return request
}

func validOpenAIStreamRequest() *http.Request {
	request := validOpenAIRequest()
	request.Body = io.NopCloser(strings.NewReader(`{"model":"ignored","stream":true,"messages":[{"role":"user","content":"hello"}]}`))
	return request
}

func newTestOpenAIHandler(t *testing.T) (
	http.Handler,
	*recordingAdmitter,
	*recordingEventSource,
	*recordingCredentialStore,
) {
	return newTestOpenAIHandlerWithOptions(t)
}

func newTestOpenAIHandlerWithOptions(t *testing.T, opts ...OpenAIHandlerOption) (
	http.Handler,
	*recordingAdmitter,
	*recordingEventSource,
	*recordingCredentialStore,
) {
	t.Helper()
	credentials := &recordingCredentialStore{credential: auth.Credential{
		ID:       "credential-1",
		TenantID: "tenant-a",
		AppID:    "support",
		Status:   auth.CredentialActive,
	}}
	directory := testDirectory{
		tenant: tenant.Tenant{ID: "tenant-a", Name: "Tenant A", Status: tenant.StatusActive},
		app: tenant.AgentApp{
			TenantID:            "tenant-a",
			AppID:               "support",
			Name:                "Support",
			ActiveConfigVersion: "v1",
			Status:              tenant.StatusActive,
		},
	}
	admitter := &recordingAdmitter{}
	source := &recordingEventSource{}
	queued, err := gateway.NewQueuedRunner(gateway.New(admitter), source)
	if err != nil {
		t.Fatalf("new queued runner: %v", err)
	}
	handler, err := NewOpenAIHandlerWithOptions(auth.HTTPAPIKeyResolver{
		Credentials: credentials,
		Directory:   directory,
	}, queued, opts...)
	if err != nil {
		t.Fatalf("new OpenAI handler: %v", err)
	}
	return handler, admitter, source, credentials
}

type recordingCredentialStore struct {
	credential auth.Credential
	err        error
	called     bool
}

func (s *recordingCredentialStore) ResolveAPIKey(context.Context, auth.APIKeyDigest) (auth.Credential, error) {
	s.called = true
	if s.err != nil {
		return auth.Credential{}, s.err
	}
	return s.credential, nil
}

type testDirectory struct {
	tenant tenant.Tenant
	app    tenant.AgentApp
}

func (d testDirectory) ResolveTenant(context.Context, string) (tenant.Tenant, error) {
	return d.tenant, nil
}

func (d testDirectory) ResolveAgentApp(context.Context, string, string) (tenant.AgentApp, error) {
	return d.app, nil
}

type recordingAdmitter struct {
	called  bool
	ctx     context.Context
	request gateway.AdmissionRequest
	err     error
}

func (a *recordingAdmitter) Admit(
	ctx context.Context,
	request gateway.AdmissionRequest,
) (gateway.AdmissionResult, error) {
	a.called = true
	a.ctx = ctx
	a.request = request
	if a.err != nil {
		return gateway.AdmissionResult{}, a.err
	}
	return gateway.AdmissionResult{
		RequestID:     request.RequestID,
		ConfigVersion: "v1",
		TurnSeq:       1,
	}, nil
}

type recordingEventSource struct {
	scope     tenant.Scope
	requestID string
	events    []gateway.ExecutionEvent
	block     bool
	started   chan struct{}
	startOnce sync.Once
}

func (s *recordingEventSource) SubscribeExecutionEvents(
	ctx context.Context,
	scope tenant.Scope,
	requestID string,
	_ int64,
) (<-chan gateway.ExecutionEvent, error) {
	if s.started != nil {
		s.startOnce.Do(func() { close(s.started) })
	}
	if s.block {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	s.scope = scope
	s.requestID = requestID
	items := s.events
	if len(items) == 0 {
		items = []gateway.ExecutionEvent{{
			Sequence: 1,
			Event: &event.Event{Response: &model.Response{
				Object:  model.ObjectTypeChatCompletion,
				Choices: []model.Choice{{Message: model.Message{Role: model.RoleAssistant, Content: "accepted"}}},
				Done:    true,
			}},
		}}
	}
	stream := make(chan gateway.ExecutionEvent, len(items))
	for _, item := range items {
		stream <- item
	}
	close(stream)
	return stream, nil
}
