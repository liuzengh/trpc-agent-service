package worker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/control"
	"github.com/liuzengh/trpc-agent-service/trpcservice/executor"
	"github.com/liuzengh/trpc-agent-service/trpcservice/governance"
	"github.com/liuzengh/trpc-agent-service/trpcservice/message"
	"github.com/liuzengh/trpc-agent-service/trpcservice/messaging"
	"github.com/liuzengh/trpc-agent-service/trpcservice/scheduler"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

type fakeExecutor struct {
	mu       sync.Mutex
	attempts int
	failFor  int
	block    time.Duration
}

func TestClassifyToolGovernanceErrors(t *testing.T) {
	if code, retry := classify(governance.ErrToolForbidden); code != "tool_rejected" || retry {
		t.Fatalf("forbidden=(%q,%t)", code, retry)
	}
	if code, retry := classify(governance.ErrDangerousConfirmation); code != "confirmation_required" || retry {
		t.Fatalf("confirmation=(%q,%t)", code, retry)
	}
}

type denyingExecutor struct {
	fakeExecutor
	err error
}

func (e *denyingExecutor) AuthorizeTask(context.Context, message.ExecutionTask) error { return e.err }

type trackingAuthorizerExecutor struct {
	fakeExecutor
	authMu      sync.Mutex
	authCalls   int
	authErr     error
	onAuthorize func()
}

func (e *trackingAuthorizerExecutor) AuthorizeTask(context.Context, message.ExecutionTask) error {
	e.authMu.Lock()
	e.authCalls++
	hook := e.onAuthorize
	err := e.authErr
	e.authMu.Unlock()
	if hook != nil {
		hook()
	}
	return err
}

func (e *trackingAuthorizerExecutor) authorizationCount() int {
	e.authMu.Lock()
	defer e.authMu.Unlock()
	return e.authCalls
}

func (f *fakeExecutor) Ready(context.Context) error { return nil }

func (f *fakeExecutor) Execute(ctx context.Context, task message.ExecutionTask) (message.OutboundMessage, error) {
	f.mu.Lock()
	f.attempts++
	attempt := f.attempts
	f.mu.Unlock()
	if f.block > 0 {
		timer := time.NewTimer(f.block)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return message.OutboundMessage{}, ctx.Err()
		case <-timer.C:
		}
	}
	if attempt <= f.failFor {
		return message.OutboundMessage{}, executor.ErrAgentFailed
	}
	return message.OutboundMessage{
		Channel: task.Channel, BindingID: task.ChannelBindingID, RequestID: task.RequestID,
		TraceID: task.TraceID, SessionID: task.SessionID, Text: "ok",
	}, nil
}

func (f *fakeExecutor) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.attempts
}

func TestWorkerRetriesThenCompletes(t *testing.T) {
	store := newWorkerStore(t, 20*time.Millisecond, 200*time.Millisecond)
	exec := &fakeExecutor{failFor: 1}
	service, err := New(store, exec, "worker-test")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- service.Run(ctx) }()
	waitReady(t, service)
	task := workerTask("retry")
	if _, _, err := store.Submit(context.Background(), task); err != nil {
		t.Fatal(err)
	}
	snapshot := waitTerminal(t, store, task.InboxID(), 4*time.Second)
	if snapshot.State != messaging.StateSucceeded || exec.count() != 2 {
		t.Fatalf("snapshot=%#v attempts=%d", snapshot, exec.count())
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestWorkerPolicyDenialDoesNotExecuteOrAcquireSession(t *testing.T) {
	store := newWorkerStore(t, 20*time.Millisecond, 200*time.Millisecond)
	exec := &denyingExecutor{err: governance.ErrActorForbidden}
	service, err := New(store, exec, "worker-governance")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- service.Run(ctx) }()
	waitReady(t, service)
	task := workerTask("actor-forbidden")
	if _, _, err := store.Submit(context.Background(), task); err != nil {
		t.Fatal(err)
	}
	snapshot := waitTerminal(t, store, task.InboxID(), 3*time.Second)
	if snapshot.ErrorCode != "actor_forbidden" || exec.count() != 0 {
		t.Fatalf("snapshot=%#v attempts=%d", snapshot, exec.count())
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestWorkerAuthorizesOnlyAfterTargetNodeAdmission(t *testing.T) {
	t.Run("non-target worker defers without authorization", func(t *testing.T) {
		store := newWorkerStore(t, 20*time.Millisecond, 200*time.Millisecond)
		controller, repository := newWorkerController(t, store, "worker-other")
		task := workerTask("node-mismatch")
		submitAssignedTask(t, repository, store, task, "worker-target")
		delivery, err := store.ReadTask(context.Background(), "consumer-other", time.Second)
		if err != nil {
			t.Fatal(err)
		}
		exec := &trackingAuthorizerExecutor{}
		service, err := NewWithController(store, exec, "consumer-other", controller)
		if err != nil {
			t.Fatal(err)
		}
		service.process(context.Background(), delivery)
		snapshot, err := store.Snapshot(context.Background(), task.InboxID())
		if err != nil || snapshot.State != "node_wait" || exec.authorizationCount() != 0 || exec.count() != 0 {
			t.Fatalf("non-target result = (%#v, %v), authorizations=%d executions=%d", snapshot, err, exec.authorizationCount(), exec.count())
		}
	})

	t.Run("target worker authorizes after admission", func(t *testing.T) {
		store := newWorkerStore(t, 20*time.Millisecond, 200*time.Millisecond)
		controller, repository := newWorkerController(t, store, "worker-target")
		task := workerTask("target-denied")
		submitAssignedTask(t, repository, store, task, "worker-target")
		delivery, err := store.ReadTask(context.Background(), "consumer-target", time.Second)
		if err != nil {
			t.Fatal(err)
		}
		admittedBeforeAuthorization := false
		exec := &trackingAuthorizerExecutor{authErr: governance.ErrActorForbidden}
		exec.onAuthorize = func() {
			snapshot, snapshotErr := store.Snapshot(context.Background(), task.InboxID())
			admittedBeforeAuthorization = snapshotErr == nil && snapshot.AssignmentState == string(control.AssignmentAdmitted)
		}
		service, err := NewWithController(store, exec, "consumer-target", controller)
		if err != nil {
			t.Fatal(err)
		}
		service.process(context.Background(), delivery)
		snapshot, err := store.Snapshot(context.Background(), task.InboxID())
		if err != nil || snapshot.State != messaging.StateFailedTerminal || snapshot.ErrorCode != "actor_forbidden" || !admittedBeforeAuthorization || exec.authorizationCount() != 1 || exec.count() != 0 {
			t.Fatalf("target result = (%#v, %v), admitted=%t authorizations=%d executions=%d", snapshot, err, admittedBeforeAuthorization, exec.authorizationCount(), exec.count())
		}
	})
}

func TestWorkerHeartbeatPreventsStaleClaimDuringExecution(t *testing.T) {
	store := newWorkerStore(t, 10*time.Millisecond, 90*time.Millisecond)
	exec := &fakeExecutor{block: 300 * time.Millisecond}
	service, _ := New(store, exec, "worker-heartbeat")
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- service.Run(ctx) }()
	waitReady(t, service)
	task := workerTask("heartbeat")
	_, _, _ = store.Submit(context.Background(), task)
	deadline := time.Now().Add(time.Second)
	for exec.count() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	time.Sleep(180 * time.Millisecond)
	claimed, err := store.ClaimStale(context.Background(), "other-worker", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(claimed) != 0 {
		t.Fatalf("active task was claimed: %#v", claimed)
	}
	waitTerminal(t, store, task.InboxID(), 2*time.Second)
	cancel()
	_ = <-done
}

func TestWorkerRejectsTamperedTaskWithoutExecutingAgent(t *testing.T) {
	store := newWorkerStore(t, 10*time.Millisecond, 90*time.Millisecond)
	if err := store.Ready(context.Background()); err != nil {
		t.Fatal(err)
	}
	exec := &fakeExecutor{}
	service, _ := New(store, exec, "worker-tamper")
	task := workerTask("tamper")
	_, _, _ = store.Submit(context.Background(), task)
	delivery, err := store.ReadTask(context.Background(), "worker-tamper", time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	delivery.Task.TenantID = "tenant-other"
	delivery.Task.PayloadDigest = delivery.Task.CanonicalDigest()
	service.process(context.Background(), delivery)
	snapshot := waitTerminal(t, store, task.InboxID(), time.Second)
	if snapshot.ErrorCode != "invalid_task" || exec.count() != 0 {
		t.Fatalf("snapshot=%#v attempts=%d", snapshot, exec.count())
	}
}

func TestRepeatedGracefulShutdownRespectsMaxAttempts(t *testing.T) {
	store := newWorkerStore(t, 10*time.Millisecond, 90*time.Millisecond)
	exec := &fakeExecutor{block: time.Minute}
	task := workerTask("shutdown-limit")
	if _, _, err := store.Submit(context.Background(), task); err != nil {
		t.Fatal(err)
	}
	for attempt := 1; attempt <= store.Config().MaxAttempts; attempt++ {
		service, err := New(store, exec, fmt.Sprintf("worker-shutdown-%d", attempt))
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- service.Run(ctx) }()
		waitReady(t, service)
		deadline := time.Now().Add(2 * time.Second)
		for exec.count() < attempt && time.Now().Before(deadline) {
			time.Sleep(5 * time.Millisecond)
		}
		if exec.count() != attempt {
			cancel()
			t.Fatalf("attempt %d did not start", attempt)
		}
		cancel()
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	snapshot := waitTerminal(t, store, task.InboxID(), time.Second)
	if snapshot.ErrorCode != "worker_shutdown" || exec.count() != store.Config().MaxAttempts {
		t.Fatalf("snapshot=%#v attempts=%d", snapshot, exec.count())
	}
}

func newWorkerStore(t *testing.T, backoff, lease time.Duration) *messaging.Store {
	t.Helper()
	server := miniredis.RunT(t)
	cfg := config.MessagingConfig{
		RedisURL: "redis://" + server.Addr() + "/0", KeyPrefix: "worker-" + fmt.Sprint(time.Now().UnixNano()),
		LeaseDuration: lease, HeartbeatInterval: lease / 4,
		InitialBackoff: backoff, MaxBackoff: backoff * 4, MaxAttempts: 3,
		InboxRetention: time.Hour, ReplyWaitTimeout: time.Second,
	}
	store, err := messaging.NewStore(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func newWorkerController(t *testing.T, store *messaging.Store, nodeID string) (*scheduler.Controller, control.Repository) {
	t.Helper()
	if err := store.Ready(context.Background()); err != nil {
		t.Fatal(err)
	}
	server := miniredis.RunT(t)
	cfg := config.ControlPlaneConfig{
		RedisEndpoint: "redis://" + server.Addr(), RedisURL: "redis://" + server.Addr() + "/4", LogicalDB: 4,
		KeyPrefix: "worker-control-" + fmt.Sprint(time.Now().UnixNano()), PolicyCacheTTL: time.Minute,
		AuditRetention: time.Hour, MetricRetention: time.Hour, NodeHeartbeatInterval: 10 * time.Millisecond,
		NodeOfflineAfter: 300 * time.Millisecond, NodeAssignmentWaitBackoff: 10 * time.Millisecond,
		TraceDigestV2Enabled: true, NodeAssignmentEnabled: true, DevelopmentAllowSharedRedis: true,
	}
	raw, err := control.NewRedisRepository(cfg)
	if err != nil {
		t.Fatal(err)
	}
	repository := control.NewInitializingRepository(raw, []tenant.Tenant{{ID: "tenant", Enabled: true}}, nil)
	t.Cleanup(func() { _ = repository.Close() })
	controller, err := scheduler.NewController(repository, store, &cfg, nodeID, "test")
	if err != nil {
		t.Fatal(err)
	}
	if err := controller.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = controller.Close() })
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if controller.Ready(context.Background()) == nil {
			return controller, repository
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("controller did not become ready")
	return nil, nil
}

func submitAssignedTask(t *testing.T, repository control.Repository, store *messaging.Store, task message.ExecutionTask, nodeID string) {
	t.Helper()
	now := time.Now().UTC()
	assignment := control.NodeAssignment{
		InboxID: task.InboxID(), TenantID: task.TenantID, AgentAppID: task.AgentAppID,
		PayloadDigest: task.PayloadDigest, NodeID: nodeID, Mode: control.PlacementShared,
		State: control.AssignmentPlanned, Revision: 1, CreatedAt: now, UpdatedAt: now,
	}
	if _, _, err := repository.CreateAssignment(context.Background(), assignment); err != nil {
		t.Fatal(err)
	}
	if _, created, err := store.SubmitAssigned(context.Background(), task, assignment); err != nil || !created {
		t.Fatalf("SubmitAssigned = (%t, %v)", created, err)
	}
}

func workerTask(id string) message.ExecutionTask {
	task := message.ExecutionTask{
		SchemaVersion: message.TaskSchemaVersion, TaskID: "task-" + id, Channel: "demo", ChannelBindingID: "binding", ExternalAccountID: "demo-account",
		TenantID: "tenant", AgentAppID: "app", ConfigVersion: "v1", RunnerUserID: "u", SessionID: "s",
		PlatformMessageID: "message-" + id, ActorUserID: "actor", ConversationID: "conversation", ConversationType: message.ConversationDirect,
		Text: "hello", RequestID: "request", TraceID: "trace",
		ReceivedAt: time.Now().UTC(), Attempt: 1,
	}
	task.PayloadDigest = task.CanonicalDigest()
	return task
}

func waitReady(t *testing.T, service *Worker) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if err := service.Ready(context.Background()); err == nil {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("worker did not become ready")
}

func waitTerminal(t *testing.T, store *messaging.Store, inboxID string, timeout time.Duration) messaging.Snapshot {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		snapshot, err := store.Snapshot(context.Background(), inboxID)
		if err == nil && snapshot.Terminal() {
			return snapshot
		}
		if err != nil && !errors.Is(err, messaging.ErrInboxMissing) {
			t.Fatal(err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("task did not reach a terminal state")
	return messaging.Snapshot{}
}
