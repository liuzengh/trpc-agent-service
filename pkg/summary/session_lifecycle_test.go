package summary

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	redisv8 "github.com/go-redis/redis/v8"
	"trpc.group/trpc-go/trpc-agent-go/agent"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/fence"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/storage"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/runner"
	"trpc.group/trpc-go/trpc-agent-go/session"
	sessionredis "trpc.group/trpc-go/trpc-agent-go/session/redis"
	"trpc.group/trpc-go/trpc-agent-go/tool"
)

type lifecycleFence struct{}

func (lifecycleFence) Acquire(context.Context, fence.Token) (func() error, error) {
	return func() error { return nil }, nil
}

type lifecycleAgent struct{ seen []string }

func (a *lifecycleAgent) Run(_ context.Context, inv *agent.Invocation) (<-chan *event.Event, error) {
	text := ""
	if value := inv.Session.Summaries[session.SummaryFilterKeyAllContents]; value != nil {
		text = value.Summary
	}
	a.seen = append(a.seen, text)
	result := make(chan *event.Event)
	close(result)
	return result, nil
}

func (*lifecycleAgent) Tools() []tool.Tool              { return nil }
func (*lifecycleAgent) Info() agent.Info                { return agent.Info{Name: "lifecycle-agent"} }
func (*lifecycleAgent) SubAgents() []agent.Agent        { return nil }
func (*lifecycleAgent) FindSubAgent(string) agent.Agent { return nil }

func TestRunnerExpiredSessionUsesNewIncarnationAndRejectsOldSummary(t *testing.T) {
	ctx := context.Background()
	server := miniredis.RunT(t)
	inner, err := sessionredis.NewService(sessionredis.WithRedisClientURL("redis://"+server.Addr()), sessionredis.WithSessionTTL(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = inner.Close() })
	coordRedis := miniredis.RunT(t)
	redisClient := redisv8.NewClient(&redisv8.Options{Addr: coordRedis.Addr()})
	t.Cleanup(func() { _ = redisClient.Close() })
	manager := storage.NewSessionLockManager(redisClient)
	key := Key{TenantID: "tenant-a", AgentAppID: "support", SessionOwnerID: "owner-1", SessionID: "stable-channel-session"}
	sessionKey := session.Key{AppName: "tsa1:8:tenant-a:support", UserID: key.SessionOwnerID, SessionID: key.SessionID}
	fenced, err := storage.NewStrictFencedSessionService(inner, lifecycleFence{}, key.TenantID)
	if err != nil {
		t.Fatal(err)
	}
	sink := NewMemorySink(nil)
	service, err := NewCheckpointSessionService(fenced, sink, key.TenantID, key.AgentAppID, sessionKey.AppName)
	if err != nil {
		t.Fatal(err)
	}
	probe := &lifecycleAgent{}
	run := runner.NewRunner(sessionKey.AppName, probe, runner.WithSessionService(service))
	t.Cleanup(func() { _ = run.Close() })
	invoke := func(message string) string {
		t.Helper()
		lease, err := manager.AcquireLease(ctx, storage.SessionInvocationLeaseKey(key.TenantID, sessionKey), time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := lease.Release(ctx); err != nil {
				t.Error(err)
			}
		}()
		runCtx := fence.WithToken(ctx, fence.Token{TenantID: key.TenantID, AgentAppID: key.AgentAppID, AgentAppName: "support",
			ScopedAppName: sessionKey.AppName, UserID: key.SessionOwnerID, SessionID: key.SessionID, ExecutionID: 1, Generation: 1, Value: "test-fence"})
		runCtx = storage.ContextWithSessionLease(runCtx, sessionKey, lease)
		events, err := run.Run(runCtx, key.SessionOwnerID, key.SessionID, model.NewUserMessage(message))
		if err != nil {
			t.Fatal(err)
		}
		for item := range events {
			if item.Error != nil {
				t.Fatalf("runner error: %#v", item.Error)
			}
		}
		id := storage.SessionIncarnationFromContext(runCtx)
		if id == "" {
			t.Fatal("Runner did not persist and capture the Session incarnation")
		}
		return id
	}
	oldID := invoke("facts from the first conversation")
	oldKey := key
	oldKey.SessionIncarnationID = oldID
	if _, err := sink.Publish(ctx, candidateFor(oldKey, 1, "old private facts")); err != nil {
		t.Fatal(err)
	}
	if current := invoke("second message before expiry"); current != oldID || probe.seen[1] != "old private facts" {
		t.Fatalf("normal current summary did not hydrate: incarnation=%s seen=%v", current, probe.seen)
	}
	server.FastForward(2 * time.Hour)
	newID := invoke("first message after expiry")
	if newID == oldID {
		t.Fatal("expired Session reused its old incarnation")
	}
	if current := invoke("second message after expiry"); current != newID || probe.seen[3] != "" {
		t.Fatalf("recreated Session received old summary: incarnation=%s seen=%v", current, probe.seen)
	}
	// A candidate generated before expiry can finish after the new Session exists.
	if _, err := sink.Publish(ctx, candidateFor(oldKey, 99, "late old summary")); err != nil {
		t.Fatal(err)
	}
	if current := invoke("third message after expiry"); current != newID || probe.seen[4] != "" {
		t.Fatalf("late old worker affected new Session: incarnation=%s seen=%v", current, probe.seen)
	}
	newKey := key
	newKey.SessionIncarnationID = newID
	store := NewMemoryStore(nil)
	if _, err := store.Enqueue(ctx, summaryRequest(oldKey, 99)); err != nil {
		t.Fatal(err)
	}
	oldJob, err := store.Claim(ctx, "worker", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Complete(ctx, oldJob, 99); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Enqueue(ctx, summaryRequest(newKey, 2)); err != nil {
		t.Fatal(err)
	}
	newJob, err := store.Claim(ctx, "worker", time.Minute)
	if err != nil || newJob.Key != newKey || newJob.TargetEventSequence != 2 {
		t.Fatalf("old monotonic checkpoint blocked new incarnation: %#v err=%v", newJob, err)
	}
	reader, err := NewTRPCSessionTranscriptReader(inner, func(Key) (string, error) { return sessionKey.AppName, nil })
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reader.ReadTranscript(ctx, oldKey, 1); !errors.Is(err, ErrTranscriptIncomplete) {
		t.Fatalf("old job read recreated Session history: %v", err)
	}
}
