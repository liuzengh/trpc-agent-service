package platform

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"
)

type sseEnvelope struct {
	EventID   string          `json:"event_id"`
	RequestID string          `json:"request_id"`
	SessionID string          `json:"session_id"`
	Sequence  uint64          `json:"sequence"`
	Type      string          `json:"type"`
	Data      json.RawMessage `json:"data"`
}

func readSSE(t *testing.T, client *channelTestClient, path string, headers map[string]string) []sseEnvelope {
	t.Helper()
	request, err := http.NewRequest(http.MethodGet, client.server.URL+path, nil)
	if err != nil {
		t.Fatal(err)
	}
	for name, value := range headers {
		request.Header.Set(name, value)
	}
	response, err := client.client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK || response.Header.Get("Content-Type") != "text/event-stream; charset=utf-8" {
		t.Fatalf("SSE response = %d/%s", response.StatusCode, response.Header.Get("Content-Type"))
	}
	scanner := bufio.NewScanner(response.Body)
	var envelopes []sseEnvelope
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		var envelope sseEnvelope
		if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &envelope); err != nil {
			t.Fatal(err)
		}
		envelopes = append(envelopes, envelope)
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	return envelopes
}

func TestChatSSEStreamsOrderedEnvelopeAndResumes(t *testing.T) {
	runs := make(chan RunnerRequest, 1)
	client := newChannelTestClient(t, requestCapturingRunner{requests: runs})
	client.activateApp("app-one", "deploy-one")
	client.post("/api/v1/chat/sessions", `{"app_id":"app-one","session_id":"session-one"}`, nil, http.StatusCreated, nil)
	client.post("/api/v1/chat/sessions/session-one/messages", `{"input":"hello"}`, map[string]string{"X-Request-ID": "request-one"}, http.StatusAccepted, nil)
	<-runs
	if err := waitForChatEvent(client, "session-one", "run.completed"); err != nil {
		t.Fatal(err)
	}

	envelopes := readSSE(t, client, "/api/v1/chat/sessions/session-one/stream?after=0&request_id=request-one", nil)
	if len(envelopes) < 5 {
		t.Fatalf("envelopes = %#v", envelopes)
	}
	seen := map[string]bool{}
	var sequence uint64
	for _, envelope := range envelopes {
		if envelope.EventID == "" || envelope.RequestID == "" || envelope.SessionID != "session-one" || envelope.Sequence <= sequence {
			t.Fatalf("invalid envelope = %#v", envelope)
		}
		if seen[envelope.EventID] {
			t.Fatalf("duplicate event id = %q", envelope.EventID)
		}
		seen[envelope.EventID] = true
		sequence = envelope.Sequence
	}
	required := map[string]bool{"run.started": false, "message.delta": false, "message.completed": false, "run.completed": false}
	for _, envelope := range envelopes {
		if _, ok := required[envelope.Type]; ok {
			required[envelope.Type] = true
		}
	}
	for eventType, found := range required {
		if !found {
			t.Fatalf("missing SSE event %q", eventType)
		}
	}
	if envelopes[len(envelopes)-1].Type != "run.completed" {
		t.Fatalf("terminal event = %#v", envelopes[len(envelopes)-1])
	}

	lastID := envelopes[0].EventID
	resumed := readSSE(t, client, "/api/v1/chat/sessions/session-one/stream?request_id=request-one", map[string]string{"Last-Event-ID": lastID})
	if len(resumed) == 0 || resumed[0].Sequence != 3 {
		t.Fatalf("resumed envelopes = %#v", resumed)
	}
}

func TestChatSSERejectsInvalidRequestID(t *testing.T) {
	client := newChannelTestClient(t, EchoRunner{})
	client.activateApp("app-one", "deploy-one")
	client.post("/api/v1/chat/sessions", `{"app_id":"app-one","session_id":"session-one"}`, nil, http.StatusCreated, nil)

	response := client.do(http.MethodGet, "/api/v1/chat/sessions/session-one/stream?request_id=bad%7Frequest", "", nil)
	assertChannelAPIError(t, response, http.StatusBadRequest, "invalid_request_id")
}

type chatBlockingRunner struct {
	started chan struct{}
	once    chan struct{}
}

func (r *chatBlockingRunner) Run(ctx context.Context, _ RunnerRequest) (RunnerResponse, error) {
	select {
	case <-r.once:
	default:
		close(r.once)
	}
	select {
	case r.started <- struct{}{}:
	default:
	}
	<-ctx.Done()
	return RunnerResponse{}, ctx.Err()
}

func TestChatSSEEmitsCancellationTerminalEvent(t *testing.T) {
	runner := &chatBlockingRunner{started: make(chan struct{}, 1), once: make(chan struct{})}
	client := newChannelTestClient(t, runner)
	client.activateApp("app-one", "deploy-one")
	client.post("/api/v1/chat/sessions", `{"app_id":"app-one","session_id":"session-one"}`, nil, http.StatusCreated, nil)
	client.post("/api/v1/chat/sessions/session-one/messages", `{"input":"hello"}`, map[string]string{"X-Request-ID": "request-one"}, http.StatusAccepted, nil)
	select {
	case <-runner.started:
	case <-time.After(time.Second):
		t.Fatal("runner did not start")
	}

	result := make(chan []sseEnvelope, 1)
	go func() {
		result <- readSSE(t, client, "/api/v1/chat/sessions/session-one/stream?after=0&request_id=request-one", nil)
	}()
	client.post("/api/v1/chat/sessions/session-one/cancel", `{"request_id":"request-one"}`, nil, http.StatusOK, nil)
	select {
	case envelopes := <-result:
		found := false
		for _, envelope := range envelopes {
			if envelope.Type == "run.cancelled" {
				found = true
			}
		}
		if !found {
			t.Fatalf("cancelled envelopes = %#v", envelopes)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("SSE stream did not terminate")
	}
}

func TestChatSSEEmitsFailureTerminalEvent(t *testing.T) {
	client := newChannelTestClient(t, failingRunner{})
	client.activateApp("app-one", "deploy-one")
	client.post("/api/v1/chat/sessions", `{"app_id":"app-one","session_id":"session-one"}`, nil, http.StatusCreated, nil)
	client.post("/api/v1/chat/sessions/session-one/messages", `{"input":"hello"}`, map[string]string{"X-Request-ID": "request-one"}, http.StatusAccepted, nil)
	if err := waitForChatEvent(client, "session-one", "run.failed"); err != nil {
		t.Fatal(err)
	}
	envelopes := readSSE(t, client, "/api/v1/chat/sessions/session-one/stream?after=0&request_id=request-one", nil)
	terminal := envelopes[len(envelopes)-1]
	if terminal.Type != "run.failed" || terminal.RequestID != "request-one" {
		t.Fatalf("terminal envelope = %#v", terminal)
	}
}

func TestChatServiceShutdownCancelsActiveRun(t *testing.T) {
	runner := &chatBlockingRunner{started: make(chan struct{}, 1), once: make(chan struct{})}
	client := newChannelTestClient(t, runner)
	client.activateApp("app-one", "deploy-one")
	client.post("/api/v1/chat/sessions", `{"app_id":"app-one","session_id":"session-one"}`, nil, http.StatusCreated, nil)
	client.post("/api/v1/chat/sessions/session-one/messages", `{"input":"hello"}`, map[string]string{"X-Request-ID": "request-one"}, http.StatusAccepted, nil)
	select {
	case <-runner.started:
	case <-time.After(time.Second):
		t.Fatal("runner did not start")
	}

	closed := make(chan error, 1)
	go func() { closed <- client.handler.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("service close waited for an uncancellable run")
	}
}

type failingRunner struct{}

func (failingRunner) Run(_ context.Context, _ RunnerRequest) (RunnerResponse, error) {
	return RunnerResponse{}, fmt.Errorf("deterministic failure")
}
