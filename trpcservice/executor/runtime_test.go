package executor

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/liuzengh/trpc-agent-service/trpcservice/agent"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/identity"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/memory"
	memoryinmemory "trpc.group/trpc-go/trpc-agent-go/memory/inmemory"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/session"
	sessioninmemory "trpc.group/trpc-go/trpc-agent-go/session/inmemory"
)

func TestRuntimeUsesRedisBackedSessionAcrossRuntimeInstances(t *testing.T) {
	redisServer := miniredis.RunT(t)
	var requestsMu sync.Mutex
	var messageCounts []int
	modelServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/chat/completions") {
			t.Fatalf("unexpected model path %s", r.URL.Path)
		}
		var request map[string]any
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Fatalf("decode model request: %v", err)
		}
		messages, _ := request["messages"].([]any)
		requestsMu.Lock()
		messageCounts = append(messageCounts, len(messages))
		requestsMu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"mock","object":"chat.completion","created":1,"model":"mock","choices":[{"index":0,"message":{"role":"assistant","content":"hello from mock"},"finish_reason":"stop"}]}`))
	}))
	defer modelServer.Close()

	cfg := config.Config{
		ModelName:           "mock-model",
		ModelBaseURL:        modelServer.URL,
		ModelAPIKeyEnv:      "MOCK_KEY",
		ModelAPIKey:         "test-key",
		ModelRequestTimeout: 5 * time.Second,
		ModelMaxOutput:      128,
		IdentitySecret:      []byte("01234567890123456789012345678901"),
		RedisURL:            "redis://" + redisServer.Addr(),
		RedisKeyPrefix:      "phase1-test",
		BindingID:           "demo-binding",
		TenantID:            "tenant-demo",
		AgentAppID:          "assistant",
		ConfigVersion:       "v1",
		AppName:             "tenant/tenant-demo/app/assistant",
	}

	first, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	reply, err := first.Handle(context.Background(), Request{
		BindingID:      cfg.BindingID,
		MessageID:      "m1",
		ExternalUserID: "u1",
		ConversationID: "c1",
		Text:           "hello",
		RequestID:      "req1",
		TraceID:        "trace1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if reply.Text != "hello from mock" {
		t.Fatalf("reply text = %q", reply.Text)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}

	second, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	if _, err := second.Handle(context.Background(), Request{
		BindingID: cfg.BindingID, MessageID: "m2", ExternalUserID: "u1", ConversationID: "c1",
		Text: "hello again", RequestID: "req2", TraceID: "trace2",
	}); err != nil {
		t.Fatal(err)
	}
	key := sessionKeyForTest(cfg)
	backend, err := second.backendForBinding(context.Background(), "demo", cfg.BindingID)
	if err != nil {
		t.Fatal(err)
	}
	sess, err := backend.Session().GetSession(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	if sess == nil || len(sess.Events) == 0 {
		t.Fatalf("expected persisted session events, got %#v", sess)
	}
	if err := second.Ready(context.Background()); err != nil {
		t.Fatal(err)
	}
	requestsMu.Lock()
	defer requestsMu.Unlock()
	if len(messageCounts) != 2 || messageCounts[1] <= messageCounts[0] {
		t.Fatalf("expected second request to include Redis history, message counts = %v", messageCounts)
	}
}

func TestCollectTextStreamingEmptyAndErrorEvents(t *testing.T) {
	events := make(chan *event.Event, 2)
	events <- &event.Event{Response: &model.Response{
		Object: model.ObjectTypeChatCompletionChunk, IsPartial: true,
		Choices: []model.Choice{{Delta: model.NewAssistantMessage("hello ")}},
	}}
	events <- &event.Event{Response: &model.Response{
		Object: model.ObjectTypeChatCompletionChunk, IsPartial: true,
		Choices: []model.Choice{{Delta: model.NewAssistantMessage("world")}},
	}}
	close(events)
	text, err := collectText(context.Background(), events)
	if err != nil || text != "hello world" {
		t.Fatalf("collectText() = (%q, %v), want streaming text", text, err)
	}

	empty := make(chan *event.Event)
	close(empty)
	if _, err := collectText(context.Background(), empty); !errors.Is(err, ErrEmptyAgentResponse) {
		t.Fatalf("collectText(empty) error = %v, want ErrEmptyAgentResponse", err)
	}

	failed := make(chan *event.Event, 1)
	failed <- &event.Event{Response: &model.Response{Error: &model.ResponseError{Message: "upstream-secret-detail"}}}
	close(failed)
	if _, err := collectText(context.Background(), failed); !errors.Is(err, ErrAgentFailed) {
		t.Fatalf("collectText(error) error = %v, want ErrAgentFailed", err)
	} else if strings.Contains(err.Error(), "upstream-secret-detail") {
		t.Fatalf("collectText leaked upstream error detail: %v", err)
	}
}

func TestContextDeadlineExceededUsesElapsedDeadline(t *testing.T) {
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Millisecond))
	defer cancel()
	if !contextDeadlineExceeded(ctx) {
		t.Fatal("elapsed deadline was not detected")
	}
	if contextDeadlineExceeded(context.Background()) {
		t.Fatal("background context was classified as timed out")
	}
}

func TestRuntimeClassifiesModelTimeoutAndEmptyResponse(t *testing.T) {
	tests := []struct {
		name    string
		handler http.HandlerFunc
		timeout time.Duration
		wantErr error
	}{
		{
			name: "empty response",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{"id":"empty","object":"chat.completion","created":1,"model":"mock","choices":[{"index":0,"message":{"role":"assistant","content":""},"finish_reason":"stop"}]}`))
			},
			timeout: time.Second,
			wantErr: ErrEmptyAgentResponse,
		},
		{
			name: "upstream 400",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusBadRequest)
				_, _ = w.Write([]byte(`{"error":{"message":"bad request detail","type":"invalid_request_error"}}`))
			},
			timeout: time.Second,
			wantErr: ErrAgentFailed,
		},
		{
			name: "upstream 500",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusInternalServerError)
				_, _ = w.Write([]byte(`{"error":{"message":"upstream-secret-detail","type":"server_error"}}`))
			},
			timeout: time.Second,
			wantErr: ErrAgentFailed,
		},
		{
			name: "timeout",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				time.Sleep(100 * time.Millisecond)
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{"id":"late","object":"chat.completion","created":1,"model":"mock","choices":[{"index":0,"message":{"role":"assistant","content":"late"},"finish_reason":"stop"}]}`))
			},
			timeout: 20 * time.Millisecond,
			wantErr: ErrAgentTimeout,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			redisServer := miniredis.RunT(t)
			modelServer := httptest.NewServer(tt.handler)
			defer modelServer.Close()
			cfg := testRuntimeConfig("redis://"+redisServer.Addr()+"/0", modelServer.URL, "model-error-test")
			cfg.ModelRequestTimeout = tt.timeout
			runtime, err := New(cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer runtime.Close()
			_, err = runtime.Handle(context.Background(), Request{
				BindingID: cfg.BindingID, MessageID: "m", ExternalUserID: "u", ConversationID: "c",
				Text: "hello", RequestID: "request", TraceID: "trace",
			})
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("Handle() error = %v, want %v", err, tt.wantErr)
			}
		})
	}
}

func TestRuntimeRedisFailureDoesNotFallback(t *testing.T) {
	redisServer := miniredis.RunT(t)
	modelServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"mock","object":"chat.completion","created":1,"model":"mock","choices":[{"index":0,"message":{"role":"assistant","content":"must not run"},"finish_reason":"stop"}]}`))
	}))
	defer modelServer.Close()
	cfg := testRuntimeConfig("redis://"+redisServer.Addr()+"/0", modelServer.URL, "redis-failure-test")
	runtime, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	redisServer.Close()

	_, err = runtime.Handle(context.Background(), Request{
		BindingID: cfg.BindingID, MessageID: "m", ExternalUserID: "u", ConversationID: "c",
		Text: "hello", RequestID: "request", TraceID: "trace",
	})
	if !errors.Is(err, ErrDependencyUnavailable) {
		t.Fatalf("Handle() error = %v, want ErrDependencyUnavailable", err)
	}
}

func TestRuntimeCloseDrainsActiveHandle(t *testing.T) {
	redisServer := miniredis.RunT(t)
	started := make(chan struct{})
	release := make(chan struct{})
	modelServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		close(started)
		<-release
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"mock","object":"chat.completion","created":1,"model":"mock","choices":[{"index":0,"message":{"role":"assistant","content":"drained"},"finish_reason":"stop"}]}`))
	}))
	defer modelServer.Close()
	cfg := testRuntimeConfig("redis://"+redisServer.Addr()+"/0", modelServer.URL, "close-drain")
	runtime, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	requestDone := make(chan error, 1)
	go func() {
		_, requestErr := runtime.Handle(context.Background(), Request{
			BindingID: cfg.BindingID, MessageID: "m", ExternalUserID: "u", ConversationID: "c",
			Text: "hello", RequestID: "request", TraceID: "trace",
		})
		requestDone <- requestErr
	}()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("model request did not start")
	}
	closeDone := make(chan error, 1)
	go func() { closeDone <- runtime.Close() }()
	select {
	case err := <-closeDone:
		t.Fatalf("Close returned while Handle was active: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	if err := <-requestDone; err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	if err := <-closeDone; err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

type closeCountingSession struct {
	session.Service
	closes atomic.Int32
}

func (s *closeCountingSession) Close() error {
	s.closes.Add(1)
	return nil
}

type closeCountingMemory struct {
	memory.Service
	closes atomic.Int32
}

func (m *closeCountingMemory) Close() error {
	m.closes.Add(1)
	return nil
}

func TestRunnerDoesNotCloseSharedServices(t *testing.T) {
	sessions := &closeCountingSession{Service: sessioninmemory.NewSessionService()}
	memories := &closeCountingMemory{Service: memoryinmemory.NewMemoryService()}
	cfg := testRuntimeConfig("redis://127.0.0.1:6379/0", "https://example.test", "ownership-test")
	catalog, credentials, err := cfg.RuntimeCatalog()
	if err != nil {
		t.Fatal(err)
	}
	configVersion := catalog.ConfigVersions[0]
	apiKey, err := credentials.Resolve(configVersion.Model.CredentialRef)
	if err != nil {
		t.Fatal(err)
	}
	runner, err := newRunner(context.Background(), agentCacheKey(cfg), configVersion, apiKey, sessions, memories)
	if err != nil {
		t.Fatal(err)
	}
	if err := runner.Close(); err != nil {
		t.Fatal(err)
	}
	if sessions.closes.Load() != 0 || memories.closes.Load() != 0 {
		t.Fatalf("runner closed borrowed services: sessions=%d memories=%d", sessions.closes.Load(), memories.closes.Load())
	}
}

func agentCacheKey(cfg config.Config) agent.CacheKey {
	return agent.CacheKey{TenantID: cfg.TenantID, AgentAppID: cfg.AgentAppID, ConfigVersion: cfg.ConfigVersion}
}

func sessionKeyForTest(cfg config.Config) session.Key {
	return session.Key{
		AppName:   cfg.AppName,
		UserID:    identity.RunnerUserID(cfg.IdentitySecret, cfg.BindingID, "u1"),
		SessionID: identity.SessionID(cfg.IdentitySecret, cfg.BindingID, "c1"),
	}
}
