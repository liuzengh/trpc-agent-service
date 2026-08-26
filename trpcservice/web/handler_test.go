package web

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	agentservice "github.com/liuzengh/trpc-agent-service/trpcservice/agent"
)

type fakeChatService struct {
	result agentservice.ChatResult
	err    error
	ready  error

	userID    string
	sessionID string
	message   string
}

func (f *fakeChatService) Ready(context.Context) error {
	return f.ready
}

func (f *fakeChatService) Chat(
	_ context.Context,
	userID string,
	sessionID string,
	message string,
) (agentservice.ChatResult, error) {
	f.userID = userID
	f.sessionID = sessionID
	f.message = message
	return f.result, f.err
}

func TestHandlerChat(t *testing.T) {
	service := &fakeChatService{result: agentservice.ChatResult{
		Reply:      "hello",
		RequestID:  "request-1",
		EventCount: 3,
	}}
	handler := NewHandler(service)
	body := bytes.NewBufferString(`{
        "user_id": " alice ",
        "session_id": " demo ",
        "message": " hello "
    }`)
	request := httptest.NewRequest(http.MethodPost, "/chat", body)
	recorder := httptest.NewRecorder()

	handler.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
	if service.userID != "alice" || service.sessionID != "demo" || service.message != "hello" {
		t.Fatalf(
			"chat input = (%q, %q, %q)",
			service.userID,
			service.sessionID,
			service.message,
		)
	}

	var response chatResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if response.Reply != "hello" || response.RequestID != "request-1" || response.EventCount != 3 {
		t.Fatalf("unexpected response: %+v", response)
	}
}

func TestHandlerRejectsInvalidRequests(t *testing.T) {
	handler := NewHandler(&fakeChatService{})
	tests := []struct {
		name   string
		method string
		body   string
		status int
	}{
		{name: "wrong method", method: http.MethodGet, status: http.StatusMethodNotAllowed},
		{name: "invalid JSON", method: http.MethodPost, body: `{`, status: http.StatusBadRequest},
		{name: "unknown field", method: http.MethodPost, body: `{"user_id":"u","session_id":"s","message":"m","extra":true}`, status: http.StatusBadRequest},
		{name: "missing user", method: http.MethodPost, body: `{"session_id":"s","message":"m"}`, status: http.StatusBadRequest},
		{name: "missing session", method: http.MethodPost, body: `{"user_id":"u","message":"m"}`, status: http.StatusBadRequest},
		{name: "missing message", method: http.MethodPost, body: `{"user_id":"u","session_id":"s"}`, status: http.StatusBadRequest},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest(test.method, "/chat", bytes.NewBufferString(test.body))
			recorder := httptest.NewRecorder()

			handler.ServeHTTP(recorder, request)

			if recorder.Code != test.status {
				t.Fatalf("status = %d, want %d, body = %s", recorder.Code, test.status, recorder.Body.String())
			}
		})
	}
}

func TestHandlerHealth(t *testing.T) {
	handler := NewHandler(&fakeChatService{})
	request := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	recorder := httptest.NewRecorder()

	handler.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
}

func TestHandlerReadiness(t *testing.T) {
	handler := NewHandler(&fakeChatService{})
	request := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	recorder := httptest.NewRecorder()

	handler.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
}

func TestHandlerReadinessFailure(t *testing.T) {
	handler := NewHandler(&fakeChatService{ready: errors.New("Redis is unavailable")})
	request := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	recorder := httptest.NewRecorder()

	handler.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
}

func TestHandlerMaintainsSessionAcrossRequests(t *testing.T) {
	runtime := agentservice.NewDemoRuntime()
	t.Cleanup(func() {
		if err := runtime.Close(); err != nil {
			t.Fatalf("close runtime: %v", err)
		}
	})
	handler := NewHandler(runtime)

	first := postChat(t, handler, `{
        "user_id": "alice",
        "session_id": "http-session",
        "message": "我叫小明。"
    }`)
	if first.Reply == "" {
		t.Fatal("first reply is empty")
	}

	second := postChat(t, handler, `{
        "user_id": "alice",
        "session_id": "http-session",
        "message": "我叫什么？"
    }`)
	if second.Reply != "你叫小明。这个名字来自当前 Session 的历史消息。" {
		t.Fatalf("second reply = %q", second.Reply)
	}
}

func postChat(t *testing.T, handler http.Handler, body string) chatResponse {
	t.Helper()
	request := httptest.NewRequest(http.MethodPost, "/chat", bytes.NewBufferString(body))
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
	var response chatResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	return response
}
