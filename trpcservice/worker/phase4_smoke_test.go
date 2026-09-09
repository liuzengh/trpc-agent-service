package worker

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/message"
	"github.com/liuzengh/trpc-agent-service/trpcservice/messaging"
	"github.com/liuzengh/trpc-agent-service/trpcservice/sessionfence"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/redis/go-redis/v9"
	"trpc.group/trpc-go/trpc-agent-go/session"
)

type phase4SmokeExecutor struct {
	mu              sync.Mutex
	activeBySession map[string]int
	activeTotal     int
	maxActiveTotal  int
	overlapped      bool
}

func (e *phase4SmokeExecutor) Ready(context.Context) error { return nil }
func (e *phase4SmokeExecutor) Execute(context.Context, message.ExecutionTask) (message.OutboundMessage, error) {
	return message.OutboundMessage{}, errors.New("legacy execution is not expected")
}
func (e *phase4SmokeExecutor) ExecuteFenced(ctx context.Context, task message.ExecutionTask, fence sessionfence.Fence) (message.OutboundMessage, sessionfence.TurnCommit, error) {
	e.mu.Lock()
	if e.activeBySession[task.SessionID] != 0 {
		e.overlapped = true
	}
	e.activeBySession[task.SessionID]++
	e.activeTotal++
	if e.activeTotal > e.maxActiveTotal {
		e.maxActiveTotal = e.activeTotal
	}
	e.mu.Unlock()
	defer func() {
		e.mu.Lock()
		e.activeBySession[task.SessionID]--
		e.activeTotal--
		e.mu.Unlock()
	}()
	select {
	case <-ctx.Done():
		return message.OutboundMessage{}, sessionfence.TurnCommit{}, ctx.Err()
	case <-time.After(150 * time.Millisecond):
	}
	return message.OutboundMessage{Channel: task.Channel, BindingID: task.ChannelBindingID, RequestID: task.RequestID, TraceID: task.TraceID, SessionID: task.SessionID, Text: "ok"}, sessionfence.TurnCommit{
		SessionCoord: fence.SessionCoord, SessionSeq: fence.SessionSeq,
		AppName: tenant.AppName(task.TenantID, task.AgentAppID), UserID: task.RunnerUserID, SessionID: task.SessionID,
		FinalState: session.StateMap{},
	}, nil
}

func TestRedis7Phase4TwoWorkersSmoke(t *testing.T) {
	rawURL := os.Getenv("PHASE4_REDIS_SMOKE_URL")
	if rawURL == "" {
		t.Skip("PHASE4_REDIS_SMOKE_URL is not set")
	}
	redisURL, err := config.NormalizeRedisURL(rawURL)
	if err != nil {
		t.Fatal(err)
	}
	prefix := fmt.Sprintf("phase4-workers:%d", time.Now().UnixNano())
	cfg := config.MessagingConfig{
		RedisURL: redisURL, KeyPrefix: prefix,
		LeaseDuration: 500 * time.Millisecond, HeartbeatInterval: 100 * time.Millisecond,
		InitialBackoff: 20 * time.Millisecond, MaxBackoff: 100 * time.Millisecond, MaxAttempts: 3,
		InboxRetention: time.Minute, ReplyWaitTimeout: time.Second,
		SessionFencing: "strong", SessionLockDuration: 800 * time.Millisecond,
		SessionWaitBackoff: 10 * time.Millisecond, SessionWaitMaxBackoff: 50 * time.Millisecond,
		MaxTurnEvents: 32, MaxTurnBytes: 32 << 10, ShutdownTimeout: time.Second,
	}
	storeA, err := messaging.NewStore(cfg)
	if err != nil {
		t.Fatal(err)
	}
	storeB, err := messaging.NewStore(cfg)
	if err != nil {
		storeA.Close()
		t.Fatal(err)
	}
	clientOptions, _ := redis.ParseURL(redisURL)
	cleanupClient := redis.NewClient(clientOptions)
	t.Cleanup(func() {
		var cursor uint64
		for {
			keys, next, scanErr := cleanupClient.Scan(context.Background(), cursor, prefix+":reliable-v1:*", 100).Result()
			if scanErr == nil && len(keys) > 0 {
				_ = cleanupClient.Del(context.Background(), keys...).Err()
			}
			if scanErr != nil || next == 0 {
				break
			}
			cursor = next
		}
		_ = cleanupClient.Close()
		_ = storeB.Close()
		_ = storeA.Close()
	})
	executor := &phase4SmokeExecutor{activeBySession: make(map[string]int)}
	workerA, err := New(storeA, executor, "phase4-worker-a")
	if err != nil {
		t.Fatal(err)
	}
	workerB, err := New(storeB, executor, "phase4-worker-b")
	if err != nil {
		t.Fatal(err)
	}
	runCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 2)
	go func() { errCh <- workerA.Run(runCtx) }()
	go func() { errCh <- workerB.Run(runCtx) }()

	tasks := []message.ExecutionTask{
		phase4SmokeTask("worker-task-1", "worker-message-1", "shared-session"),
		phase4SmokeTask("worker-task-2", "worker-message-2", "shared-session"),
		phase4SmokeTask("worker-task-3", "worker-message-3", "parallel-session"),
	}
	for _, task := range tasks {
		if _, _, err := storeA.Submit(context.Background(), task); err != nil {
			t.Fatal(err)
		}
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		completed := 0
		for _, task := range tasks {
			snapshot, snapshotErr := storeA.Snapshot(context.Background(), task.InboxID())
			if snapshotErr == nil && snapshot.State == messaging.StateSucceeded {
				completed++
			}
		}
		if completed == len(tasks) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("only %d/%d tasks completed", completed, len(tasks))
		}
		time.Sleep(25 * time.Millisecond)
	}
	executor.mu.Lock()
	overlapped, maxActive := executor.overlapped, executor.maxActiveTotal
	executor.mu.Unlock()
	if overlapped {
		t.Fatal("same Session executed concurrently")
	}
	if maxActive < 2 {
		t.Fatalf("different Sessions never ran in parallel; max active=%d", maxActive)
	}
	cancel()
	for i := 0; i < 2; i++ {
		if err := <-errCh; err != nil {
			t.Fatal(err)
		}
	}
}

func phase4SmokeTask(taskID, messageID, sessionID string) message.ExecutionTask {
	task := message.ExecutionTask{
		SchemaVersion: message.TaskSchemaVersion, TaskID: taskID, Channel: "demo", ChannelBindingID: "binding-a", ExternalAccountID: "demo-account",
		TenantID: "tenant-a", AgentAppID: "assistant", ConfigVersion: "v1", RunnerUserID: "user-a",
		SessionID: sessionID, PlatformMessageID: messageID, ActorUserID: "actor-a", ConversationID: sessionID,
		ConversationType: message.ConversationDirect, Text: "hello", RequestID: taskID + "-request",
		TraceID: taskID + "-trace", ReceivedAt: time.Now().UTC(), Attempt: 1,
	}
	task.PayloadDigest = task.CanonicalDigest()
	return task
}
