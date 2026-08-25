package platform

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/DocJlm/trpc-agent-service/trpcservice/agent"
	"github.com/DocJlm/trpc-agent-service/trpcservice/channels"
	"github.com/DocJlm/trpc-agent-service/trpcservice/config"
	"github.com/DocJlm/trpc-agent-service/trpcservice/queue"
	"github.com/DocJlm/trpc-agent-service/trpcservice/store"
	"github.com/DocJlm/trpc-agent-service/trpcservice/tenant"
	"github.com/google/uuid"
)

type countingEngine struct {
	mu   sync.Mutex
	runs int
}

func (e *countingEngine) Run(_ context.Context, req agent.Request) (agent.Result, error) {
	e.mu.Lock()
	e.runs++
	e.mu.Unlock()
	return agent.Result{Content: "answer"}, nil
}

func (*countingEngine) Close() error { return nil }

type flakyAdapter struct {
	mu       sync.Mutex
	failures int
	calls    int
}

type transientReceiveQueue struct {
	calls      int
	retryCalls int
	deadCalls  int
	mu         sync.Mutex
}

func (*transientReceiveQueue) Publish(context.Context, store.DispatchTask) error { return nil }
func (q *transientReceiveQueue) Receive(ctx context.Context, _ string, _ time.Duration) (queue.Delivery, error) {
	q.mu.Lock()
	q.calls++
	calls := q.calls
	q.mu.Unlock()
	if calls == 1 {
		return queue.Delivery{}, errors.New("temporary redis read timeout")
	}
	<-ctx.Done()
	return queue.Delivery{}, ctx.Err()
}

func (*transientReceiveQueue) Ack(context.Context, queue.Delivery) error { return nil }
func (q *transientReceiveQueue) Retry(context.Context, queue.Delivery) error {
	q.mu.Lock()
	q.retryCalls++
	q.mu.Unlock()
	return nil
}
func (q *transientReceiveQueue) Dead(context.Context, queue.Delivery, error) error {
	q.mu.Lock()
	q.deadCalls++
	q.mu.Unlock()
	return nil
}
func (*transientReceiveQueue) Close() error { return nil }

type eventualLocker struct {
	mu        sync.Mutex
	busyCount int
	calls     int
}

func (l *eventualLocker) Acquire(context.Context, string, time.Duration) (queue.Lease, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.calls++
	if l.calls <= l.busyCount {
		return nil, queue.ErrLeaseBusy
	}
	return testLease{}, nil
}

type testLease struct{}

func (testLease) Fence() int64                               { return 1 }
func (testLease) Renew(context.Context, time.Duration) error { return nil }
func (testLease) Release(context.Context) error              { return nil }

func (*flakyAdapter) ID() string                    { return "binding" }
func (*flakyAdapter) Run(ctx context.Context) error { <-ctx.Done(); return nil }
func (a *flakyAdapter) Send(context.Context, channels.OutboundEnvelope) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.calls++
	if a.calls <= a.failures {
		return errors.New("temporary IM failure")
	}
	return nil
}
func (*flakyAdapter) Health() channels.ChannelHealth {
	return channels.ChannelHealth{Ready: true, State: "ready"}
}

func TestOfflineMessagePipeline(t *testing.T) {
	cfg := config.Config{
		HTTPAddr: "127.0.0.1:0",
		Database: config.Database{QueueName: "test", ConsumerGroup: "test"},
		Tenants: []tenant.Tenant{{
			ID: "tenant", Name: "Tenant", Enabled: true,
			Agent:   tenant.AgentProfile{ID: "assistant", Version: "1"},
			Backend: tenant.BackendProfile{Session: "memory"},
		}},
	}
	repository := store.NewMemoryRepository()
	memoryQueue := queue.NewMemoryQueue(16)
	ctx, cancel := context.WithCancel(context.Background())
	app, err := New(ctx, cfg, Options{
		Repository: repository, Queue: memoryQueue, Locker: queue.NewMemoryLocker(), Engine: agent.EchoEngine{},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer app.Close()
	done := make(chan error, 2)
	go func() { done <- app.dispatchRelay(ctx) }()
	go func() { done <- app.worker(ctx) }()

	message := channels.InboundEnvelope{
		TenantID: "tenant", BindingID: "binding", Channel: "feishu",
		ExternalMessageID: "message-1", ExternalUserID: "user", ExternalConversationID: "chat",
		ConversationType: channels.ConversationP2P, Content: "hello", ReceivedAt: time.Now(),
		TraceID: "trace-1", ReplyToken: "message-1",
	}
	if err := app.acceptInbound(ctx, message); err != nil {
		t.Fatal(err)
	}
	if err := app.acceptInbound(ctx, message); err != nil {
		t.Fatalf("duplicate input should be a no-op: %v", err)
	}
	replyID := uuid.NewSHA1(uuid.NameSpaceOID, []byte(message.IdempotencyKey()+"|reply")).String()
	deadline := time.Now().Add(3 * time.Second)
	for {
		exists, existsErr := repository.ReplyExists(ctx, replyID)
		if existsErr != nil {
			t.Fatal(existsErr)
		}
		if exists {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("reply was not committed")
		}
		time.Sleep(10 * time.Millisecond)
	}
	stats, err := repository.Stats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if stats.InboundTotal != 1 || stats.AuditTotal != 1 {
		t.Fatalf("unexpected stats: %+v", stats)
	}
	cancel()
	for range 2 {
		if err := <-done; err != nil {
			t.Fatalf("pipeline shutdown: %v", err)
		}
	}
}

func TestReplyRetryDoesNotRerunAgent(t *testing.T) {
	cfg := config.Config{
		HTTPAddr: "127.0.0.1:0", Database: config.Database{QueueName: "test", ConsumerGroup: "test"},
		Tenants: []tenant.Tenant{{ID: "tenant", Enabled: true, Agent: tenant.AgentProfile{ID: "assistant", Version: "1"}}},
	}
	repository := store.NewMemoryRepository()
	memoryQueue := queue.NewMemoryQueue(16)
	engine := &countingEngine{}
	app, err := New(context.Background(), cfg, Options{Repository: repository, Queue: memoryQueue, Locker: queue.NewMemoryLocker(), Engine: engine})
	if err != nil {
		t.Fatal(err)
	}
	defer app.Close()
	adapter := &flakyAdapter{failures: 2}
	app.adapters[adapterKey("tenant", "binding")] = adapter
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go app.dispatchRelay(ctx)
	go app.worker(ctx)
	go app.replyRelay(ctx)
	message := channels.InboundEnvelope{
		TenantID: "tenant", BindingID: "binding", Channel: "feishu", ExternalMessageID: "message-retry",
		ExternalUserID: "user", ExternalConversationID: "chat", ConversationType: channels.ConversationP2P,
		Content: "hello", ReceivedAt: time.Now(), TraceID: uuid.NewString(), ReplyToken: "message-retry",
	}
	if err := app.acceptInbound(ctx, message); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		adapter.mu.Lock()
		calls := adapter.calls
		adapter.mu.Unlock()
		if calls >= 3 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("reply calls=%d", calls)
		}
		time.Sleep(10 * time.Millisecond)
	}
	engine.mu.Lock()
	runs := engine.runs
	engine.mu.Unlock()
	if runs != 1 {
		t.Fatalf("agent runs=%d want=1", runs)
	}
}

func TestWorkerRetriesTransientQueueReceiveFailure(t *testing.T) {
	cfg := config.Config{
		HTTPAddr: "127.0.0.1:0", Database: config.Database{QueueName: "test", ConsumerGroup: "test"},
		Tenants: []tenant.Tenant{{ID: "tenant", Enabled: true, Agent: tenant.AgentProfile{ID: "assistant", Version: "1"}}},
	}
	transientQueue := &transientReceiveQueue{}
	app, err := New(context.Background(), cfg, Options{
		Repository: store.NewMemoryRepository(), Queue: transientQueue,
		Locker: queue.NewMemoryLocker(), Engine: agent.EchoEngine{},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer app.Close()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- app.worker(ctx) }()
	deadline := time.Now().Add(2 * time.Second)
	for {
		transientQueue.mu.Lock()
		calls := transientQueue.calls
		transientQueue.mu.Unlock()
		if calls >= 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("worker did not retry a transient queue error")
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestAcquireSessionLeaseWaitsThroughContention(t *testing.T) {
	locker := &eventualLocker{busyCount: 3}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	lease, contended, err := acquireLeaseWithWait(ctx, locker, "session", time.Minute, time.Millisecond)
	if err != nil || lease == nil || !contended {
		t.Fatalf("lease=%v contended=%v err=%v", lease, contended, err)
	}
	locker.mu.Lock()
	calls := locker.calls
	locker.mu.Unlock()
	if calls != 4 {
		t.Fatalf("acquire calls=%d want=4", calls)
	}
}

func TestRetryOrDeadCapsNonModelFailures(t *testing.T) {
	trackedQueue := &transientReceiveQueue{}
	app := &Platform{queue: trackedQueue}
	app.retryOrDead(context.Background(), queue.Delivery{Task: store.DispatchTask{Attempts: 6}}, errors.New("temporary"))
	app.retryOrDead(context.Background(), queue.Delivery{Task: store.DispatchTask{Attempts: 7}}, errors.New("persistent"))
	trackedQueue.mu.Lock()
	retries, dead := trackedQueue.retryCalls, trackedQueue.deadCalls
	trackedQueue.mu.Unlock()
	if retries != 1 || dead != 1 {
		t.Fatalf("retries=%d dead=%d", retries, dead)
	}
}
