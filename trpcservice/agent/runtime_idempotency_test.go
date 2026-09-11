package agent

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/coordination"
	"github.com/liuzengh/trpc-agent-service/trpcservice/idempotency"
	platformstorage "github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"trpc.group/trpc-go/trpc-agent-go/model"
)

func TestRuntimeDeduplicatesConcurrentMessage(t *testing.T) {
	controlled := newControlledModel()
	runtime, err := NewRuntime(controlled, false)
	if err != nil {
		t.Fatalf("create runtime: %v", err)
	}
	t.Cleanup(func() { _ = runtime.Close() })

	results := make(chan idempotentChatOutcome, 2)
	for i := 0; i < 2; i++ {
		go func() {
			result, chatErr := runtime.ChatWithMessageID(
				context.Background(),
				"same-message",
				"alice",
				"same-session",
				"hello",
			)
			results <- idempotentChatOutcome{result: result, err: chatErr}
		}()
	}
	controlled.waitForStart(t, "hello")
	controlled.expectNoStart(t, 75*time.Millisecond)
	controlled.allowOne()

	first := waitForIdempotentOutcome(t, results)
	second := waitForIdempotentOutcome(t, results)
	if first.result.RequestID != second.result.RequestID {
		t.Fatalf("request IDs differ: %q and %q", first.result.RequestID, second.result.RequestID)
	}
	if first.result.MessageID != "same-message" || second.result.MessageID != "same-message" {
		t.Fatalf("message IDs = %q and %q", first.result.MessageID, second.result.MessageID)
	}
	if first.result.Replayed == second.result.Replayed {
		t.Fatalf("replayed flags = %t and %t, want one replay", first.result.Replayed, second.result.Replayed)
	}
	if calls := controlled.sequence.Load(); calls != 1 {
		t.Fatalf("model calls = %d, want 1", calls)
	}
}

func TestRuntimeReplaysCompletedMessageAndRejectsConflict(t *testing.T) {
	runtime := NewDemoRuntime()
	t.Cleanup(func() { _ = runtime.Close() })

	first, err := runtime.ChatWithMessageID(
		context.Background(),
		"completed-message",
		"alice",
		"replay-session",
		"hello",
	)
	if err != nil {
		t.Fatalf("first chat: %v", err)
	}
	replay, err := runtime.ChatWithMessageID(
		context.Background(),
		"completed-message",
		"alice",
		"replay-session",
		"hello",
	)
	if err != nil {
		t.Fatalf("replay chat: %v", err)
	}
	if !replay.Replayed || replay.RequestID != first.RequestID || replay.Reply != first.Reply {
		t.Fatalf("replay = %+v, first = %+v", replay, first)
	}
	if _, err := runtime.ChatWithMessageID(
		context.Background(),
		"completed-message",
		"alice",
		"replay-session",
		"different text",
	); !errors.Is(err, idempotency.ErrKeyConflict) {
		t.Fatalf("conflict error = %v", err)
	}
}

func TestRuntimeRetriesMessageAfterCancelledAttempt(t *testing.T) {
	controlled := newControlledModel()
	runtime, err := NewRuntime(controlled, false)
	if err != nil {
		t.Fatalf("create runtime: %v", err)
	}
	t.Cleanup(func() { _ = runtime.Close() })

	firstCtx, cancelFirst := context.WithCancel(context.Background())
	firstErr := make(chan error, 1)
	go func() {
		_, chatErr := runtime.ChatWithMessageID(
			firstCtx,
			"retry-message",
			"alice",
			"retry-session",
			"retry me",
		)
		firstErr <- chatErr
	}()
	controlled.waitForStart(t, "retry me")
	cancelFirst()
	select {
	case err := <-firstErr:
		if err == nil {
			t.Fatal("cancelled attempt unexpectedly succeeded")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("cancelled attempt did not return")
	}

	secondResult := make(chan idempotentChatOutcome, 1)
	go func() {
		result, chatErr := runtime.ChatWithMessageID(
			context.Background(),
			"retry-message",
			"alice",
			"retry-session",
			"retry me",
		)
		secondResult <- idempotentChatOutcome{result: result, err: chatErr}
	}()
	controlled.waitForStart(t, "retry me")
	controlled.allowOne()
	outcome := waitForIdempotentOutcome(t, secondResult)
	if outcome.result.Replayed {
		t.Fatal("retried execution was incorrectly marked replayed")
	}
}

func TestRuntimeDeduplicatesAcrossRedisBackedRuntimes(t *testing.T) {
	server := miniredis.RunT(t)
	controlled := newControlledModel()
	runtimeA := newFullyRedisRuntime(t, server, "a", controlled)
	runtimeB := newFullyRedisRuntime(t, server, "b", controlled)
	results := make(chan idempotentChatOutcome, 2)

	go runIdempotentChat(runtimeA, results)
	controlled.waitForStart(t, "distributed duplicate")
	go runIdempotentChat(runtimeB, results)
	controlled.expectNoStart(t, 75*time.Millisecond)
	controlled.allowOne()

	first := waitForIdempotentOutcome(t, results)
	second := waitForIdempotentOutcome(t, results)
	if first.result.RequestID != second.result.RequestID ||
		first.result.Replayed == second.result.Replayed {
		t.Fatalf("distributed outcomes = %+v and %+v", first.result, second.result)
	}
	if calls := controlled.sequence.Load(); calls != 1 {
		t.Fatalf("distributed model calls = %d, want 1", calls)
	}
}

func TestMessageIDScopeIncludesSession(t *testing.T) {
	controlled := newControlledModel()
	runtime, err := NewRuntime(controlled, false)
	if err != nil {
		t.Fatalf("create runtime: %v", err)
	}
	t.Cleanup(func() { _ = runtime.Close() })
	results := make(chan idempotentChatOutcome, 2)
	for _, sessionID := range []string{"scope-a", "scope-b"} {
		go func(session string) {
			result, chatErr := runtime.ChatWithMessageID(
				context.Background(),
				"same-external-id",
				"alice",
				session,
				"scoped",
			)
			results <- idempotentChatOutcome{result: result, err: chatErr}
		}(sessionID)
	}
	controlled.waitForStart(t, "scoped")
	controlled.waitForStart(t, "scoped")
	controlled.allowOne()
	controlled.allowOne()
	waitForIdempotentOutcome(t, results)
	waitForIdempotentOutcome(t, results)
	if calls := controlled.sequence.Load(); calls != 2 {
		t.Fatalf("model calls = %d, want 2", calls)
	}
}

func newFullyRedisRuntime(
	t *testing.T,
	server *miniredis.Miniredis,
	node string,
	selectedModel model.Model,
) *Runtime {
	t.Helper()
	redisURL := "redis://" + server.Addr() + "/0"
	sessionService, err := platformstorage.NewSessionService(
		context.Background(),
		config.SessionConfig{
			Backend:        config.SessionBackendRedis,
			RedisURL:       redisURL,
			RedisKeyPrefix: "idempotency-runtime-session",
		},
	)
	if err != nil {
		t.Fatalf("create Session service for %s: %v", node, err)
	}
	coordinator, err := coordination.New(context.Background(), config.CoordinatorConfig{
		Backend:       config.CoordinatorBackendRedis,
		RedisURL:      redisURL,
		RedisPrefix:   "idempotency-runtime-coordinator",
		LeaseTTL:      2 * time.Second,
		RenewInterval: 500 * time.Millisecond,
		RetryInterval: 5 * time.Millisecond,
	})
	if err != nil {
		_ = sessionService.Close()
		t.Fatalf("create coordinator for %s: %v", node, err)
	}
	idempotencyStore, err := idempotency.New(context.Background(), config.IdempotencyConfig{
		Backend:       config.IdempotencyBackendRedis,
		RedisURL:      redisURL,
		RedisPrefix:   "idempotency-runtime-store",
		ProcessingTTL: 2 * time.Second,
		CompletedTTL:  time.Hour,
		RenewInterval: 500 * time.Millisecond,
		PollInterval:  5 * time.Millisecond,
	})
	if err != nil {
		_ = coordinator.Close()
		_ = sessionService.Close()
		t.Fatalf("create idempotency store for %s: %v", node, err)
	}
	runtime, err := NewRuntimeWithServices(
		selectedModel,
		sessionService,
		coordinator,
		idempotencyStore,
		false,
	)
	if err != nil {
		_ = idempotencyStore.Close()
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

func runIdempotentChat(runtime *Runtime, outcomes chan<- idempotentChatOutcome) {
	result, err := runtime.ChatWithMessageID(
		context.Background(),
		"distributed-message",
		"alice",
		"distributed-session",
		"distributed duplicate",
	)
	outcomes <- idempotentChatOutcome{result: result, err: err}
}

type idempotentChatOutcome struct {
	result ChatResult
	err    error
}

func waitForIdempotentOutcome(
	t *testing.T,
	outcomes <-chan idempotentChatOutcome,
) idempotentChatOutcome {
	t.Helper()
	select {
	case outcome := <-outcomes:
		if outcome.err != nil {
			t.Fatalf("idempotent chat failed: %v", outcome.err)
		}
		return outcome
	case <-time.After(2 * time.Second):
		t.Fatal("idempotent chat did not complete")
		return idempotentChatOutcome{}
	}
}
