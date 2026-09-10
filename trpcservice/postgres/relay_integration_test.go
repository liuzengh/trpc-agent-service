//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"flag"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/liuzengh/trpc-agent-service/internal/execution"
	"github.com/liuzengh/trpc-agent-service/trpcservice/auth"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	platformpostgres "github.com/liuzengh/trpc-agent-service/trpcservice/postgres"
	"github.com/liuzengh/trpc-agent-service/trpcservice/queue"
	platformredis "github.com/liuzengh/trpc-agent-service/trpcservice/redis"
	"github.com/liuzengh/trpc-agent-service/trpcservice/relay"
	platformtelemetry "github.com/liuzengh/trpc-agent-service/trpcservice/telemetry"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/worker"
)

var relayRedisTestURL = flag.String(
	"redis-test-url",
	os.Getenv("TRPC_AGENT_SERVICE_REDIS_TEST_URL"),
	"Redis integration URL",
)

func TestRelayDeliversAdmittedExecutionToWorker(t *testing.T) {
	if *relayRedisTestURL == "" {
		t.Fatal("TRPC_AGENT_SERVICE_REDIS_TEST_URL is required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	telemetryRuntime := platformtelemetry.NewNoop(ctx, "relay-integration-test")
	t.Cleanup(func() { _ = telemetryRuntime.Close(context.Background()) })
	callbackCtx, callbackSpan := platformtelemetry.StartSpan(ctx, "channel.callback")
	defer callbackSpan.End()
	wantTraceID := platformtelemetry.TraceID(callbackCtx)

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

	gatewayService := gateway.New(store)
	result, err := gatewayService.Handle(callbackCtx, gateway.Request{
		RequestID:      "request-relay-1",
		IdempotencyKey: "client-relay-1",
		Tenant:         relayIdentityResolver{identity: identity},
		Message:        gateway.Message{Text: "run through the durable queue"},
	})
	if err != nil {
		t.Fatalf("admit request: %v", err)
	}
	if result.ConfigVersion != "v1" || result.TurnSeq != 1 {
		t.Fatalf("admission result = %#v", result)
	}
	assertExecutionAndOutboxStatus(t, ctx, pool, result.RequestID, "PENDING", "PENDING")

	executor := &relayExecutor{executed: make(chan execution.Job, 1), traceIDs: make(chan string, 1)}
	trackedStream := &ackTrackingStream{Stream: stream}
	consumer, err := worker.NewConsumer(executor, trackedStream, store, "worker-relay-test")
	if err != nil {
		t.Fatalf("new worker consumer: %v", err)
	}
	dispatcher, err := relay.New(store, stream, "relay-test")
	if err != nil {
		t.Fatalf("new relay: %v", err)
	}

	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	consumerResult := make(chan error, 1)
	go func() { consumerResult <- consumer.Run(runCtx) }()
	relayResult := make(chan error, 1)
	go func() { relayResult <- dispatcher.Run(runCtx) }()

	select {
	case job := <-executor.executed:
		if job.RequestID() != result.RequestID || job.Tenant().ConfigVersion != "v1" {
			t.Fatalf("executed job = request=%q config=%q", job.RequestID(), job.Tenant().ConfigVersion)
		}
	case <-ctx.Done():
		t.Fatalf("worker did not execute admitted request: %v", ctx.Err())
	}
	select {
	case gotTraceID := <-executor.traceIDs:
		if gotTraceID != wantTraceID {
			t.Fatalf("worker trace id = %q, want %q", gotTraceID, wantTraceID)
		}
	case <-ctx.Done():
		t.Fatalf("worker did not propagate trace context: %v", ctx.Err())
	}
	assertExecutionAndOutboxStatus(t, ctx, pool, result.RequestID, "SUCCEEDED", "CONSUMED")
	assertRedisAcknowledgements(t, ctx, &trackedStream.acks, 1)
	if got := executor.runs.Load(); got != 1 {
		t.Fatalf("worker executions = %d, want 1", got)
	}

	consumer.StopClaiming()
	stop()
	if err := <-consumerResult; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("consumer run: %v", err)
	}
	if err := <-relayResult; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("relay run: %v", err)
	}
}

func TestExpiredExecutionLeaseIsRecoveredWithoutConcurrentOwner(t *testing.T) {
	p := newIM05Fixture(t, newIntegrationTargetProtector(t, "v1"))
	admitted, err := p.store.Admit(p.ctx, newIM05Request(t, p.route, p.binding, "message-lease-recovery", "request-lease-recovery", channels.MessageTypeText, "recover"))
	if err != nil {
		t.Fatalf("admit execution: %v", err)
	}
	dispatches, err := p.store.ClaimDispatches(p.ctx, "relay-first", time.Second, 1)
	if err != nil || len(dispatches) != 1 {
		t.Fatalf("claim initial dispatches = %#v, %v", dispatches, err)
	}
	first, found, err := p.store.Claim(p.ctx, dispatches[0], queue.ClaimRequest{Owner: "worker-a", LeaseDuration: time.Second})
	if err != nil || !found {
		t.Fatalf("first claim = %#v, found=%t, err=%v", first, found, err)
	}
	if _, found, err := p.store.Claim(p.ctx, dispatches[0], queue.ClaimRequest{Owner: "worker-b", LeaseDuration: time.Second}); err != nil || found {
		t.Fatalf("concurrent claim found=%t err=%v, want no second owner", found, err)
	}

	wait := time.Until(first.Lease.Until)
	if wait < 0 {
		wait = 0
	}
	time.Sleep(wait + 50*time.Millisecond)
	if err := p.store.RecoverDispatches(p.ctx, time.Second); err != nil {
		t.Fatalf("recover expired execution: %v", err)
	}
	recovered, err := p.store.ClaimDispatches(p.ctx, "relay-recovery", time.Second, 1)
	if err != nil || len(recovered) != 1 {
		t.Fatalf("claim recovered dispatches = %#v, %v", recovered, err)
	}
	second, found, err := p.store.Claim(p.ctx, recovered[0], queue.ClaimRequest{Owner: "worker-b", LeaseDuration: time.Second})
	if err != nil || !found {
		t.Fatalf("recovery claim = %#v, found=%t, err=%v", second, found, err)
	}
	if first.Lease.Token == second.Lease.Token || second.Lease.Owner != "worker-b" || second.Job.RequestID() != admitted.RequestID {
		t.Fatalf("recovered claim = %#v", second)
	}
	if err := p.store.Complete(p.ctx, second, queue.CompletionSucceeded); err != nil {
		t.Fatalf("complete recovered execution: %v", err)
	}
	if _, found, err := p.store.Claim(p.ctx, recovered[0], queue.ClaimRequest{Owner: "worker-c", LeaseDuration: time.Second}); err != nil || found {
		t.Fatalf("completed execution claim found=%t err=%v, want no claim", found, err)
	}
}

func seedRelayIntegrationScope(t *testing.T, ctx context.Context, store *platformpostgres.Store) gateway.AdmissionIdentity {
	t.Helper()
	tenantValue := tenant.Tenant{ID: "tenant-relay", Name: "Relay Tenant", Status: tenant.StatusActive}
	if err := store.CreateTenant(ctx, tenantValue); err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	config := integrationAppConfig("v1", "relay-model")
	config.TenantID = tenantValue.ID
	config.AppID = "relay-app"
	app := tenant.AgentApp{
		TenantID:            tenantValue.ID,
		AppID:               config.AppID,
		Name:                "Relay App",
		ActiveConfigVersion: config.Version,
		Status:              tenant.StatusActive,
	}
	if err := store.CreateAgentApp(ctx, app, config); err != nil {
		t.Fatalf("create agent app: %v", err)
	}
	digest, err := auth.DigestAPIKey("tas_relay_integration_key_with_at_least_32_random_bytes")
	if err != nil {
		t.Fatalf("digest api key: %v", err)
	}
	credential := auth.Credential{
		ID:        "credential-relay",
		TenantID:  tenantValue.ID,
		AppID:     config.AppID,
		KeyPrefix: "tas_rela",
		Status:    auth.CredentialActive,
	}
	if err := store.CreateCredential(ctx, digest, credential); err != nil {
		t.Fatalf("create credential: %v", err)
	}
	return gateway.AdmissionIdentity{
		Tenant: tenant.RuntimeContext{
			TenantID:           tenantValue.ID,
			AppID:              config.AppID,
			ConfigVersion:      "stale-version",
			SessionPrincipalID: "principal-relay",
			SessionID:          "session-relay",
			UserID:             "user-relay",
			TraceID:            "trace-relay-1",
		},
		Source:           gateway.TenantSourceAuthenticatedClaims,
		SourceID:         credential.ID,
		CredentialDigest: gateway.CredentialDigest(digest),
	}
}

func assertExecutionAndOutboxStatus(t *testing.T, ctx context.Context, pool *pgxpool.Pool, requestID, wantExecution, wantOutbox string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		var executionStatus, outboxStatus string
		err := pool.QueryRow(ctx, `SELECT e.status, o.status
FROM platform.execution e
JOIN platform.dispatch_outbox o USING (tenant_id, app_id, request_id)
WHERE e.request_id = $1`, requestID).Scan(&executionStatus, &outboxStatus)
		if err == nil && executionStatus == wantExecution && outboxStatus == wantOutbox {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("execution and outbox status = %q, %q (err=%v), want %q, %q", executionStatus, outboxStatus, err, wantExecution, wantOutbox)
		}
		select {
		case <-ctx.Done():
			t.Fatalf("wait for execution and outbox status: %v", ctx.Err())
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func assertRedisAcknowledgements(t *testing.T, ctx context.Context, acknowledgements *atomic.Int32, want int32) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		if got := acknowledgements.Load(); got == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("redis acknowledgements = %d, want %d", acknowledgements.Load(), want)
		}
		select {
		case <-ctx.Done():
			t.Fatalf("wait for redis acknowledgements: %v", ctx.Err())
		case <-time.After(10 * time.Millisecond):
		}
	}
}

type relayIdentityResolver struct {
	identity gateway.AdmissionIdentity
}

func (r relayIdentityResolver) ResolveTenant(context.Context) (tenant.RuntimeContext, gateway.TenantSource, error) {
	return r.identity.Tenant, r.identity.Source, nil
}

func (r relayIdentityResolver) ResolveAdmissionIdentity(context.Context) (gateway.AdmissionIdentity, error) {
	return r.identity, nil
}

type relayExecutor struct {
	executed chan execution.Job
	traceIDs chan string
	runs     atomic.Int32
}

func (e *relayExecutor) Run(ctx context.Context, job execution.Job) (worker.RunResult, error) {
	e.runs.Add(1)
	e.executed <- job
	e.traceIDs <- platformtelemetry.TraceID(ctx)
	return worker.RunResult{RunnerCompleted: true}, nil
}

type ackTrackingStream struct {
	queue.Stream
	acks atomic.Int32
}

func (s *ackTrackingStream) Ack(ctx context.Context, delivery queue.Delivery) error {
	if err := s.Stream.Ack(ctx, delivery); err != nil {
		return err
	}
	s.acks.Add(1)
	return nil
}
