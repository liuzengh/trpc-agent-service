package agent

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/coordination"
	platformstorage "github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"trpc.group/trpc-go/trpc-agent-go/model"
)

func TestRuntimeSerializesSameSession(t *testing.T) {
	controlled := newControlledModel()
	runtime, err := NewRuntime(controlled, false)
	if err != nil {
		t.Fatalf("create runtime: %v", err)
	}
	t.Cleanup(func() { _ = runtime.Close() })

	results := make(chan error, 2)
	go runControlledChat(runtime, "same-session", "first", results)
	controlled.waitForStart(t, "first")
	go runControlledChat(runtime, "same-session", "second", results)
	controlled.expectNoStart(t, 75*time.Millisecond)

	controlled.allowOne()
	waitForChatResult(t, results)
	controlled.waitForStart(t, "second")
	controlled.allowOne()
	waitForChatResult(t, results)

	if maximum := controlled.maxActive.Load(); maximum != 1 {
		t.Fatalf("maximum concurrent model calls = %d, want 1", maximum)
	}
}

func TestRuntimeAllowsDifferentSessionsConcurrently(t *testing.T) {
	controlled := newControlledModel()
	runtime, err := NewRuntime(controlled, false)
	if err != nil {
		t.Fatalf("create runtime: %v", err)
	}
	t.Cleanup(func() { _ = runtime.Close() })

	results := make(chan error, 2)
	go runControlledChat(runtime, "session-a", "first", results)
	controlled.waitForStart(t, "first")
	go runControlledChat(runtime, "session-b", "second", results)
	controlled.waitForStart(t, "second")

	controlled.allowOne()
	controlled.allowOne()
	waitForChatResult(t, results)
	waitForChatResult(t, results)

	if maximum := controlled.maxActive.Load(); maximum < 2 {
		t.Fatalf("maximum concurrent model calls = %d, want at least 2", maximum)
	}
}

func TestRuntimeCancellationReleasesSessionLease(t *testing.T) {
	controlled := newControlledModel()
	runtime, err := NewRuntime(controlled, false)
	if err != nil {
		t.Fatalf("create runtime: %v", err)
	}
	t.Cleanup(func() { _ = runtime.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	firstResult := make(chan error, 1)
	go func() {
		_, chatErr := runtime.Chat(ctx, "alice", "cancelled-session", "cancelled")
		firstResult <- chatErr
	}()
	controlled.waitForStart(t, "cancelled")
	cancel()
	select {
	case err := <-firstResult:
		if err == nil {
			t.Fatal("cancelled chat unexpectedly succeeded")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("cancelled chat did not return")
	}

	secondResult := make(chan error, 1)
	go runControlledChat(runtime, "cancelled-session", "after-cancel", secondResult)
	controlled.waitForStart(t, "after-cancel")
	controlled.allowOne()
	waitForChatResult(t, secondResult)
}

func TestRedisCoordinatorSerializesAcrossRuntimes(t *testing.T) {
	server := miniredis.RunT(t)
	controlled := newControlledModel()
	runtimeA := newCoordinatedRedisRuntime(t, server, "node-a", controlled)
	runtimeB := newCoordinatedRedisRuntime(t, server, "node-b", controlled)
	results := make(chan error, 2)

	go runControlledChat(runtimeA, "distributed-session", "node-a", results)
	controlled.waitForStart(t, "node-a")
	go runControlledChat(runtimeB, "distributed-session", "node-b", results)
	controlled.expectNoStart(t, 75*time.Millisecond)

	controlled.allowOne()
	waitForChatResult(t, results)
	controlled.waitForStart(t, "node-b")
	controlled.allowOne()
	waitForChatResult(t, results)

	if maximum := controlled.maxActive.Load(); maximum != 1 {
		t.Fatalf("maximum distributed model calls = %d, want 1", maximum)
	}
}

func TestNewRuntimeWithServicesRequiresCoordinator(t *testing.T) {
	service, err := platformstorage.NewSessionService(context.Background(), config.SessionConfig{
		Backend: config.SessionBackendInMemory,
	})
	if err != nil {
		t.Fatalf("create session service: %v", err)
	}
	defer func() { _ = service.Close() }()
	if _, err := NewRuntimeWithServices(NewTutorialModel(), service, nil, false); err == nil {
		t.Fatal("expected nil coordinator error")
	}
}

func newCoordinatedRedisRuntime(
	t *testing.T,
	server *miniredis.Miniredis,
	node string,
	selectedModel model.Model,
) *Runtime {
	t.Helper()
	sessionService, err := platformstorage.NewSessionService(
		context.Background(),
		config.SessionConfig{
			Backend:        config.SessionBackendRedis,
			RedisURL:       "redis://" + server.Addr() + "/0",
			RedisKeyPrefix: "runtime-coordinator-session",
		},
	)
	if err != nil {
		t.Fatalf("create Redis session service for %s: %v", node, err)
	}
	coordinator, err := coordination.New(
		context.Background(),
		config.CoordinatorConfig{
			Backend:       config.CoordinatorBackendRedis,
			RedisURL:      "redis://" + server.Addr() + "/0",
			RedisPrefix:   "runtime-coordinator-lock",
			LeaseTTL:      2 * time.Second,
			RenewInterval: 500 * time.Millisecond,
			RetryInterval: 5 * time.Millisecond,
		},
	)
	if err != nil {
		_ = sessionService.Close()
		t.Fatalf("create Redis coordinator for %s: %v", node, err)
	}
	runtime, err := NewRuntimeWithServices(selectedModel, sessionService, coordinator, false)
	if err != nil {
		_ = coordinator.Close()
		_ = sessionService.Close()
		t.Fatalf("create runtime for %s: %v", node, err)
	}
	t.Cleanup(func() {
		if err := runtime.Close(); err != nil {
			t.Fatalf("close runtime %s: %v", node, err)
		}
	})
	return runtime
}

func runControlledChat(runtime *Runtime, sessionID, message string, result chan<- error) {
	_, err := runtime.Chat(context.Background(), "alice", sessionID, message)
	result <- err
}

func waitForChatResult(t *testing.T, results <-chan error) {
	t.Helper()
	select {
	case err := <-results:
		if err != nil {
			t.Fatalf("chat failed: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("chat did not complete")
	}
}

type controlledModel struct {
	started   chan string
	permits   chan struct{}
	active    atomic.Int32
	maxActive atomic.Int32
	sequence  atomic.Uint64
}

func newControlledModel() *controlledModel {
	return &controlledModel{
		started: make(chan string, 4),
		permits: make(chan struct{}, 4),
	}
}

func (m *controlledModel) Info() model.Info {
	return model.Info{Name: "controlled-model", ContextWindow: 4096}
}

func (m *controlledModel) GenerateContent(
	ctx context.Context,
	request *model.Request,
) (<-chan *model.Response, error) {
	active := m.active.Add(1)
	defer m.active.Add(-1)
	for {
		maximum := m.maxActive.Load()
		if active <= maximum || m.maxActive.CompareAndSwap(maximum, active) {
			break
		}
	}
	message := latestRequestUserMessage(request)
	select {
	case m.started <- message:
	case <-ctx.Done():
		return nil, context.Cause(ctx)
	}
	select {
	case <-m.permits:
	case <-ctx.Done():
		return nil, context.Cause(ctx)
	}

	stop := "stop"
	responses := make(chan *model.Response, 1)
	responses <- &model.Response{
		ID:        fmt.Sprintf("controlled-%d", m.sequence.Add(1)),
		Object:    model.ObjectTypeChatCompletion,
		Created:   time.Now().Unix(),
		Model:     "controlled-model",
		Done:      true,
		IsPartial: false,
		Choices: []model.Choice{{
			Index:        0,
			Message:      model.NewAssistantMessage("completed " + message),
			FinishReason: &stop,
		}},
	}
	close(responses)
	return responses, nil
}

func (m *controlledModel) waitForStart(t *testing.T, want string) {
	t.Helper()
	select {
	case got := <-m.started:
		if got != want {
			t.Fatalf("started message = %q, want %q", got, want)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("model did not start for %q", want)
	}
}

func (m *controlledModel) expectNoStart(t *testing.T, wait time.Duration) {
	t.Helper()
	select {
	case got := <-m.started:
		t.Fatalf("model unexpectedly started for %q", got)
	case <-time.After(wait):
	}
}

func (m *controlledModel) allowOne() {
	m.permits <- struct{}{}
}

func latestRequestUserMessage(request *model.Request) string {
	if request == nil {
		return ""
	}
	for i := len(request.Messages) - 1; i >= 0; i-- {
		if request.Messages[i].Role == model.RoleUser {
			return strings.TrimSpace(request.Messages[i].Content)
		}
	}
	return ""
}
