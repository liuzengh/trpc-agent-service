package worker

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	miniserver "github.com/alicebob/miniredis/v2/server"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/executor"
	"github.com/liuzengh/trpc-agent-service/trpcservice/keyspace"
	"github.com/liuzengh/trpc-agent-service/trpcservice/message"
	"github.com/liuzengh/trpc-agent-service/trpcservice/messaging"
	"github.com/liuzengh/trpc-agent-service/trpcservice/sessionfence"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/redis/go-redis/v9"
	"trpc.group/trpc-go/trpc-agent-go/session"
)

type cancelAwareFencedExecutor struct {
	started chan struct{}
	once    sync.Once
}

func (e *cancelAwareFencedExecutor) Ready(context.Context) error { return nil }
func (e *cancelAwareFencedExecutor) Execute(context.Context, message.ExecutionTask) (message.OutboundMessage, error) {
	return message.OutboundMessage{}, errors.New("legacy execution is not expected")
}
func (e *cancelAwareFencedExecutor) ExecuteFenced(ctx context.Context, _ message.ExecutionTask, _ sessionfence.Fence) (message.OutboundMessage, sessionfence.TurnCommit, error) {
	e.once.Do(func() { close(e.started) })
	<-ctx.Done()
	return message.OutboundMessage{}, sessionfence.TurnCommit{}, ctx.Err()
}

type stubbornFencedExecutor struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

type overLimitFencedExecutor struct {
	mu    sync.Mutex
	calls int
}

type emptyCommitFencedExecutor struct{}

func (*emptyCommitFencedExecutor) Ready(context.Context) error { return nil }
func (*emptyCommitFencedExecutor) Execute(context.Context, message.ExecutionTask) (message.OutboundMessage, error) {
	return message.OutboundMessage{}, errors.New("legacy execution is not expected")
}
func (*emptyCommitFencedExecutor) ExecuteFenced(_ context.Context, task message.ExecutionTask, _ sessionfence.Fence) (message.OutboundMessage, sessionfence.TurnCommit, error) {
	return message.OutboundMessage{Channel: task.Channel, BindingID: task.ChannelBindingID, RequestID: task.RequestID, TraceID: task.TraceID, SessionID: task.SessionID, Text: "invalid"}, sessionfence.TurnCommit{}, nil
}

func (e *overLimitFencedExecutor) Ready(context.Context) error { return nil }
func (e *overLimitFencedExecutor) Execute(context.Context, message.ExecutionTask) (message.OutboundMessage, error) {
	return message.OutboundMessage{}, errors.New("legacy execution is not expected")
}
func (e *overLimitFencedExecutor) ExecuteFenced(context.Context, message.ExecutionTask, sessionfence.Fence) (message.OutboundMessage, sessionfence.TurnCommit, error) {
	e.mu.Lock()
	e.calls++
	e.mu.Unlock()
	return message.OutboundMessage{}, sessionfence.TurnCommit{}, executor.ErrTurnTooLarge
}

func (e *overLimitFencedExecutor) count() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.calls
}

func (e *stubbornFencedExecutor) Ready(context.Context) error { return nil }
func (e *stubbornFencedExecutor) Execute(context.Context, message.ExecutionTask) (message.OutboundMessage, error) {
	return message.OutboundMessage{}, errors.New("legacy execution is not expected")
}
func (e *stubbornFencedExecutor) ExecuteFenced(_ context.Context, task message.ExecutionTask, fence sessionfence.Fence) (message.OutboundMessage, sessionfence.TurnCommit, error) {
	e.once.Do(func() { close(e.started) })
	<-e.release
	return message.OutboundMessage{Channel: task.Channel, BindingID: task.ChannelBindingID, RequestID: task.RequestID, TraceID: task.TraceID, SessionID: task.SessionID, Text: "late"}, sessionfence.TurnCommit{
		SessionCoord: fence.SessionCoord, SessionSeq: fence.SessionSeq,
		AppName: tenant.AppName(task.TenantID, task.AgentAppID), UserID: task.RunnerUserID, SessionID: task.SessionID,
		FinalState: session.StateMap{},
	}, nil
}

func TestAgentCancellationReleasesLockAfterTransitionFailure(t *testing.T) {
	store, server, cfg := newStrongWorkerStore(t, time.Second)
	executor := &cancelAwareFencedExecutor{started: make(chan struct{})}
	service, err := New(store, executor, "cancel-cleanup-worker")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- service.Run(ctx) }()
	task := workerTask("strong-cancel-cleanup")
	if _, _, err := store.Submit(context.Background(), task); err != nil {
		t.Fatal(err)
	}
	select {
	case <-executor.started:
	case <-time.After(time.Second):
		t.Fatal("fenced executor did not start")
	}
	if err := server.Set(keyspace.CoordinationPrefix(cfg.KeyPrefix)+":agent.retry", "wrong-type"); err != nil {
		t.Fatal(err)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	assertStrongLockMissing(t, server, cfg.KeyPrefix, task)
}

func TestShutdownTimeoutActivelyReleasesCurrentSessionLock(t *testing.T) {
	store, server, cfg := newStrongWorkerStore(t, 30*time.Millisecond)
	executor := &stubbornFencedExecutor{started: make(chan struct{}), release: make(chan struct{})}
	service, err := New(store, executor, "shutdown-timeout-worker")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- service.Run(context.Background()) }()
	task := workerTask("strong-shutdown-timeout")
	if _, _, err := store.Submit(context.Background(), task); err != nil {
		t.Fatal(err)
	}
	select {
	case <-executor.started:
	case <-time.After(time.Second):
		t.Fatal("stubborn fenced executor did not start")
	}
	if err := service.Close(); err != nil {
		t.Fatal(err)
	}
	assertStrongLockMissing(t, server, cfg.KeyPrefix, task)
	close(executor.release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("worker did not stop after executor release")
	}
}

func TestTurnOverLimitProducesOneTerminalReply(t *testing.T) {
	store, _, _ := newStrongWorkerStore(t, time.Second)
	exec := &overLimitFencedExecutor{}
	service, err := New(store, exec, "over-limit-worker")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- service.Run(ctx) }()
	task := workerTask("strong-over-limit")
	if _, _, err := store.Submit(context.Background(), task); err != nil {
		t.Fatal(err)
	}
	snapshot := waitTerminal(t, store, task.InboxID(), 2*time.Second)
	if snapshot.ErrorCode != "session_turn_too_large" || exec.count() != 1 {
		t.Fatalf("over-limit snapshot=%#v calls=%d", snapshot, exec.count())
	}
	reply, err := store.ReadReply(context.Background(), "over-limit-gateway", time.Millisecond)
	if err != nil || reply.Result.ErrorCode != "session_turn_too_large" {
		t.Fatalf("over-limit reply=(%#v,%v)", reply, err)
	}
	if _, err := store.ReadReply(context.Background(), "over-limit-gateway", time.Millisecond); !errors.Is(err, redis.Nil) {
		t.Fatalf("second over-limit reply error=%v, want redis.Nil", err)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestStrongWorkerRejectsEmptyTurnCommit(t *testing.T) {
	store, _, _ := newStrongWorkerStore(t, time.Second)
	service, err := New(store, &emptyCommitFencedExecutor{}, "empty-commit-worker")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- service.Run(ctx) }()
	task := workerTask("strong-empty-commit")
	if _, _, err := store.Submit(context.Background(), task); err != nil {
		t.Fatal(err)
	}
	snapshot := waitTerminal(t, store, task.InboxID(), 2*time.Second)
	if snapshot.ErrorCode != "session_commit_invalid" {
		t.Fatalf("empty commit snapshot=%#v", snapshot)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func newStrongWorkerStore(t *testing.T, shutdownTimeout time.Duration) (*messaging.Store, *miniredis.Miniredis, config.MessagingConfig) {
	t.Helper()
	server := miniredis.RunT(t)
	server.Server().SetPreHook(func(peer *miniserver.Peer, command string, _ ...string) bool {
		switch command {
		case "ROLE":
			peer.WriteLen(3)
			peer.WriteBulk("master")
			peer.WriteInt(0)
			peer.WriteLen(0)
			return true
		case "INFO":
			peer.WriteBulk("# Server\r\nrun_id:worker-test\r\n")
			return true
		case "CLUSTER":
			peer.WriteError("ERR This instance has cluster support disabled")
			return true
		default:
			return false
		}
	})
	cfg := config.MessagingConfig{
		RedisURL: "redis://" + server.Addr() + "/0", KeyPrefix: "phase4-worker-shutdown",
		LeaseDuration: 300 * time.Millisecond, HeartbeatInterval: 50 * time.Millisecond,
		InitialBackoff: 10 * time.Millisecond, MaxBackoff: 50 * time.Millisecond, MaxAttempts: 3,
		InboxRetention: time.Hour, ReplyWaitTimeout: time.Second,
		SessionFencing: "strong", SessionLockDuration: 300 * time.Millisecond,
		SessionWaitBackoff: 10 * time.Millisecond, SessionWaitMaxBackoff: 50 * time.Millisecond,
		MaxTurnEvents: 32, MaxTurnBytes: 32 << 10, ShutdownTimeout: shutdownTimeout,
	}
	store, err := messaging.NewStore(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store, server, cfg
}

func assertStrongLockMissing(t *testing.T, server *miniredis.Miniredis, prefix string, task message.ExecutionTask) {
	t.Helper()
	coord := keyspace.SessionCoord(task.TenantID, task.ChannelBindingID, task.RunnerUserID, task.SessionID)
	if server.Exists(keyspace.SessionLock(prefix, coord)) {
		t.Fatal("Session lock remained after standalone cleanup")
	}
}
