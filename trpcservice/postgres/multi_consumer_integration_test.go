//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/liuzengh/trpc-agent-service/internal/execution"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	platformpostgres "github.com/liuzengh/trpc-agent-service/trpcservice/postgres"
	platformredis "github.com/liuzengh/trpc-agent-service/trpcservice/redis"
	"github.com/liuzengh/trpc-agent-service/trpcservice/relay"
	"github.com/liuzengh/trpc-agent-service/trpcservice/worker"
)

// laneTrackingExecutor records global completion order and concurrent session
// execution across independent consumers.
type laneTrackingExecutor struct {
	tracker *laneTracker
	owner   string
}

type laneCompletion struct {
	Order        int
	Owner        string
	PartitionKey string
	RequestID    string
}

type laneTracker struct {
	mu          sync.Mutex
	inFlight    map[string]int
	started     map[string]struct{}
	release     chan struct{}
	released    bool
	violations  []string
	completions []laneCompletion
	overlapped  bool
}

func newLaneTracker() *laneTracker {
	return &laneTracker{
		inFlight: make(map[string]int),
		started:  make(map[string]struct{}),
		release:  make(chan struct{}),
	}
}

func newLaneTrackingExecutor(tracker *laneTracker, owner string) *laneTrackingExecutor {
	return &laneTrackingExecutor{tracker: tracker, owner: owner}
}

func (e *laneTrackingExecutor) Run(ctx context.Context, job execution.Job) (worker.RunResult, error) {
	partitionKey, err := job.PartitionKey()
	if err != nil {
		return worker.RunResult{}, err
	}
	e.tracker.start(partitionKey, job.RequestID())
	defer e.tracker.complete(e.owner, partitionKey, job.RequestID())
	select {
	case <-e.tracker.release:
	case <-ctx.Done():
		return worker.RunResult{}, ctx.Err()
	}
	select {
	case <-time.After(40 * time.Millisecond):
	case <-ctx.Done():
		return worker.RunResult{}, ctx.Err()
	}
	return worker.RunResult{RunnerCompleted: true}, nil
}

func (t *laneTracker) start(partitionKey, requestID string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for activePartition, count := range t.inFlight {
		if activePartition != partitionKey && count > 0 {
			t.overlapped = true
		}
	}
	t.inFlight[partitionKey]++
	if t.inFlight[partitionKey] > 1 {
		t.violations = append(t.violations, partitionKey+"/"+requestID)
	}
	t.started[partitionKey] = struct{}{}
	if len(t.started) > 1 && !t.released {
		t.released = true
		close(t.release)
	}
}

func (t *laneTracker) complete(owner, partitionKey, requestID string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.inFlight[partitionKey]--
	t.completions = append(t.completions, laneCompletion{
		Order:        len(t.completions) + 1,
		Owner:        owner,
		PartitionKey: partitionKey,
		RequestID:    requestID,
	})
}

func (t *laneTracker) snapshot() (completions []laneCompletion, violations []string, overlapped bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]laneCompletion(nil), t.completions...), append([]string(nil), t.violations...), t.overlapped
}

// TestTwoConsumersPreserveSessionLanes verifies the multi-node acceptance
// claims: two consumers with independent owners drain one stream, turns of
// the same session complete strictly in turn_seq order without overlapping,
// and different sessions interleave freely.
func TestTwoConsumersPreserveSessionLanes(t *testing.T) {
	if *relayRedisTestURL == "" {
		t.Fatal("TRPC_AGENT_SERVICE_REDIS_TEST_URL is required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool := openIntegrationPool(t)
	if _, err := pool.Exec(ctx, `DROP SCHEMA IF EXISTS platform CASCADE`); err != nil {
		t.Fatalf("reset platform schema: %v", err)
	}
	store, err := platformpostgres.New(pool)
	if err != nil {
		t.Fatalf("new postgres store: %v", err)
	}
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("migrate postgres store: %v", err)
	}
	identity := seedRelayIntegrationScope(t, ctx, store)

	gatewayService := gateway.New(store)
	admit := func(requestID, sessionID string, wantTurnSeq int64) {
		t.Helper()
		admission := identity
		admission.Tenant.SessionID = sessionID
		result, err := gatewayService.Handle(ctx, gateway.Request{
			RequestID:      requestID,
			IdempotencyKey: "key-" + requestID,
			Tenant:         relayIdentityResolver{identity: admission},
			Message:        gateway.Message{Text: "turn " + requestID},
		})
		if err != nil {
			t.Fatalf("admit %s: %v", requestID, err)
		}
		if result.TurnSeq != wantTurnSeq {
			t.Fatalf("admission %s turn_seq = %d, want %d", requestID, result.TurnSeq, wantTurnSeq)
		}
	}
	const totalTurns = 5
	for i := 1; i <= 3; i++ {
		admit(fmt.Sprintf("request-lane-a-%d", i), "session-lane-a", int64(i))
	}
	for i := 1; i <= 2; i++ {
		admit(fmt.Sprintf("request-lane-b-%d", i), "session-lane-b", int64(i))
	}

	client, err := platformredis.NewClient(ctx, *relayRedisTestURL)
	if err != nil {
		t.Fatalf("new redis client: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	stream, err := platformredis.NewStream(client, "trpc-agent-service:relay-test:"+uuid.NewString(), "workers", 10*time.Millisecond)
	if err != nil {
		t.Fatalf("new redis stream: %v", err)
	}
	if err := stream.Init(ctx); err != nil {
		t.Fatalf("initialize redis stream: %v", err)
	}

	tracker := newLaneTracker()
	executorA := newLaneTrackingExecutor(tracker, "worker-lane-a")
	executorB := newLaneTrackingExecutor(tracker, "worker-lane-b")
	consumerA, err := worker.NewConsumer(executorA, stream, store, "worker-lane-a")
	if err != nil {
		t.Fatalf("new consumer a: %v", err)
	}
	consumerB, err := worker.NewConsumer(executorB, stream, store, "worker-lane-b")
	if err != nil {
		t.Fatalf("new consumer b: %v", err)
	}
	dispatcher, err := relay.New(store, stream, "relay-lane-test")
	if err != nil {
		t.Fatalf("new relay: %v", err)
	}

	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	consumerErrors := make(chan error, 2)
	go func() { consumerErrors <- consumerA.Run(runCtx) }()
	go func() { consumerErrors <- consumerB.Run(runCtx) }()
	relayResult := make(chan error, 1)
	go func() { relayResult <- dispatcher.Run(runCtx) }()

	deadline := time.Now().Add(20 * time.Second)
	for {
		var succeeded int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM platform.execution WHERE status='SUCCEEDED'`).Scan(&succeeded); err != nil {
			t.Fatalf("count succeeded executions: %v", err)
		}
		if succeeded == totalTurns {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("succeeded executions = %d, want %d", succeeded, totalTurns)
		}
		select {
		case <-ctx.Done():
			t.Fatalf("wait for succeeded executions: %v", ctx.Err())
		case <-time.After(20 * time.Millisecond):
		}
	}

	consumerA.StopClaiming()
	consumerB.StopClaiming()
	stop()
	for i := 0; i < 2; i++ {
		if err := <-consumerErrors; err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("consumer run: %v", err)
		}
	}
	if err := <-relayResult; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("relay run: %v", err)
	}

	completions, violations, overlapped := tracker.snapshot()
	if len(violations) != 0 {
		t.Fatalf("same-session concurrent executions = %v", violations)
	}
	if !overlapped {
		t.Fatal("different session executions did not overlap")
	}
	if len(completions) != totalTurns {
		t.Fatalf("completion count = %d, want %d", len(completions), totalTurns)
	}
	partitionKeyA := sessionPartitionKey(t, identity, "session-lane-a")
	partitionKeyB := sessionPartitionKey(t, identity, "session-lane-b")
	assertTurnOrder(t, completions, partitionKeyA, []string{"request-lane-a-1", "request-lane-a-2", "request-lane-a-3"})
	assertTurnOrder(t, completions, partitionKeyB, []string{"request-lane-b-1", "request-lane-b-2"})
}

func sessionPartitionKey(t *testing.T, identity gateway.AdmissionIdentity, sessionID string) string {
	t.Helper()
	tenantContext := identity.Tenant
	tenantContext.SessionID = sessionID
	partitionKey, err := tenantContext.Scope().Key("session", tenantContext.SessionPrincipalID, tenantContext.SessionID)
	if err != nil {
		t.Fatalf("session partition key: %v", err)
	}
	return partitionKey
}

func assertTurnOrder(t *testing.T, completions []laneCompletion, partitionKey string, wantRequests []string) {
	t.Helper()
	got := make([]string, 0, len(wantRequests))
	for _, completion := range completions {
		if completion.PartitionKey == partitionKey {
			got = append(got, completion.RequestID)
		}
	}
	if len(got) != len(wantRequests) {
		t.Fatalf("partition %q completions = %v, want %v", partitionKey, got, wantRequests)
	}
	for i := range wantRequests {
		if got[i] != wantRequests[i] {
			t.Fatalf("partition %q completion order = %v, want %v", partitionKey, got, wantRequests)
		}
	}
}
