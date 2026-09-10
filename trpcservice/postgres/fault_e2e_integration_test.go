//go:build integration && e2e

package postgres_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/liuzengh/trpc-agent-service/trpcservice/auth"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/ingress"
	platformpostgres "github.com/liuzengh/trpc-agent-service/trpcservice/postgres"
	platformredis "github.com/liuzengh/trpc-agent-service/trpcservice/redis"
	"github.com/liuzengh/trpc-agent-service/trpcservice/relay"
	"github.com/redis/go-redis/v9"
)

func TestFaultWorkerRecoveryAcrossRealProcesses(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	pool, postgresDSN := openRuntimePool(t)
	store, err := platformpostgres.New(pool)
	if err != nil {
		t.Fatalf("new postgres store: %v", err)
	}
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("migrate postgres: %v", err)
	}
	runID := strings.ReplaceAll(uuid.NewString(), "-", "")
	scope := seedRuntimeScope(t, ctx, store, "fault-tenant-"+runID, "support")
	redisContainer := requiredFaultContainer(t, "TRPC_FAULT_E2E_REDIS_CONTAINER")
	postgresContainer := requiredFaultContainer(t, "TRPC_FAULT_E2E_POSTGRES_CONTAINER")
	redisURL := runtimeRedisURL()
	redisClient, err := platformredis.NewClient(ctx, redisURL)
	if err != nil {
		t.Fatalf("new redis client: %v", err)
	}
	defer redisClient.Close()
	streamName := "trpc-agent-service:fault-e2e:" + uuid.NewString()
	group := "fault-e2e-workers"
	stream, err := platformredis.NewStream(redisClient, streamName, group, 6*time.Second)
	if err != nil {
		t.Fatalf("new fault stream: %v", err)
	}
	if err := stream.Init(ctx); err != nil {
		t.Fatalf("initialize fault stream: %v", err)
	}
	evidence := faultEvidence{Stream: streamName}
	defer func() { writeFaultEvidence(t, evidence) }()
	dispatcher, err := relay.New(store, stream, "fault-e2e-relay")
	if err != nil {
		t.Fatalf("new fault relay: %v", err)
	}
	relayCtx, stopRelay := context.WithCancel(ctx)
	relayDone := make(chan error, 1)
	go func() { relayDone <- dispatcher.Run(relayCtx) }()
	t.Cleanup(func() {
		stopRelay()
		if err := <-relayDone; err != nil && !errors.Is(err, context.Canceled) {
			t.Logf("fault relay stopped with error: %v", err)
		}
	})

	events, err := platformpostgres.NewExecutionEventJournal(store)
	if err != nil {
		t.Fatalf("new execution event journal: %v", err)
	}
	queued, err := gateway.NewQueuedRunner(gateway.New(store), events)
	if err != nil {
		t.Fatalf("new queued runner: %v", err)
	}
	dataPlane, err := ingress.NewOpenAIHandler(auth.HTTPAPIKeyResolver{
		Credentials: store,
		Directory:   store,
	}, queued)
	if err != nil {
		t.Fatalf("new OpenAI handler: %v", err)
	}
	dataPlaneServer := httptest.NewServer(dataPlane)
	defer dataPlaneServer.Close()
	workerBinary := buildRuntimeWorker(t)
	workerEnv := runtimeWorkerEnv(postgresDSN, redisURL, streamName, group)

	crashed := startRuntimeWorker(t, ctx, workerBinary, workerEnv, "fault-worker-a", "block")
	crashRequest := launchRuntimeRequest(ctx, dataPlaneServer.URL, scope.APIKey, "fault-crash", "fault-idem-crash", "fault-crash-session", "crash")
	if _, err := waitRuntimeExecution(ctx, pool, "fault-crash", func(value runtimeExecution) bool {
		return value.Status == "RUNNING" && value.Owner == crashed.owner
	}); err != nil {
		t.Fatalf("wait crashed worker ownership: %v", err)
	}
	evidence.Crash.Claimed = mustReadFaultExecution(t, ctx, pool, "fault-crash")
	stopRuntimeWorker(crashed, true)

	reclaimer := startRuntimeWorker(t, ctx, workerBinary, workerEnv, "fault-worker-b", "slow")
	claimed, err := waitRuntimeExecution(ctx, pool, "fault-crash", func(value runtimeExecution) bool {
		return value.Status == "RUNNING" && value.Owner == reclaimer.owner && value.Attempt >= 2
	})
	if err != nil {
		t.Fatalf("wait reclaimed execution: %v", err)
	}
	evidence.Crash.Reclaimed = faultExecutionEvidence{
		RequestID: claimed.RequestID, Worker: claimed.Owner, Status: claimed.Status, Attempt: claimed.Attempt,
	}
	if err := requireRuntimeHTTPSuccess(<-crashRequest); err != nil {
		t.Fatalf("recovered request: %v", err)
	}
	terminal, err := waitRuntimeExecution(ctx, pool, "fault-crash", func(value runtimeExecution) bool {
		return value.Status == "SUCCEEDED"
	})
	if err != nil {
		t.Fatalf("wait recovered terminal state: %v", err)
	}
	evidence.Crash.Terminal = faultExecutionEvidence{
		RequestID: terminal.RequestID, Worker: terminal.Owner, Status: terminal.Status, Attempt: terminal.Attempt,
	}
	if terminal.Attempt < claimed.Attempt || terminal.Owner != "" {
		t.Fatalf("recovered terminal execution = %#v", terminal)
	}
	assertFaultDispatchConsumed(t, ctx, pool, "fault-crash")
	if err := waitFaultRedisPendingZero(ctx, t, redisURL, streamName, group); err != nil {
		t.Fatalf("redis pending after crash recovery: %v", err)
	}
	evidence.Crash.DispatchStatus = faultLatestDispatchStatus(t, ctx, pool, "fault-crash")
	evidence.Crash.RedisPending = faultRedisPending(t, ctx, redisURL, streamName, group)
	stopRuntimeWorker(reclaimer, true)

	timeoutEnv := cloneRuntimeWorkerEnv(workerEnv)
	timeoutEnv["TRPC_AGENT_SERVICE_MODEL_TIMEOUT"] = "200ms"
	timeoutWorker := startRuntimeWorker(t, ctx, workerBinary, timeoutEnv, "fault-worker-timeout", "block")
	timeoutRequest := launchRuntimeRequest(ctx, dataPlaneServer.URL, scope.APIKey, "fault-timeout", "fault-idem-timeout", "fault-timeout-session", "timeout")
	timeoutTerminal, err := waitRuntimeTerminal(ctx, pool, "fault-timeout")
	if err != nil {
		t.Fatalf("wait model timeout terminal state: %v", err)
	}
	if timeoutTerminal.Status != "UNCERTAIN" && timeoutTerminal.Status != "FAILED" {
		t.Fatalf("model timeout status = %q", timeoutTerminal.Status)
	}
	evidence.Timeout = faultExecutionEvidence{
		RequestID: timeoutTerminal.RequestID, Worker: timeoutTerminal.Owner, Status: timeoutTerminal.Status, Attempt: timeoutTerminal.Attempt,
	}
	if result := <-timeoutRequest; result.Err != nil {
		t.Logf("model timeout HTTP result: %v", result.Err)
	}
	stopRuntimeWorker(timeoutWorker, true)

	redisWorker := startRuntimeWorker(t, ctx, workerBinary, workerEnv, "fault-worker-redis-outage", "fault-slow")
	redisRequest := launchRuntimeRequest(ctx, dataPlaneServer.URL, scope.APIKey, "fault-redis-outage", "fault-idem-redis-outage", "fault-redis-session", "redis outage")
	if _, err := waitRuntimeExecution(ctx, pool, "fault-redis-outage", func(value runtimeExecution) bool {
		return value.Status == "RUNNING" && value.Owner == redisWorker.owner
	}); err != nil {
		t.Fatalf("wait Redis outage worker ownership: %v", err)
	}
	if err := faultDockerAction(ctx, "stop", redisContainer); err != nil {
		t.Fatalf("stop Redis for outage: %v", err)
	}
	if err := waitFaultDuration(ctx, 2*time.Second); err != nil {
		t.Fatalf("hold Redis outage: %v", err)
	}
	assertRuntimeWorkerAlive(t, ctx, redisWorker)
	if err := faultDockerAction(ctx, "start", redisContainer); err != nil {
		t.Fatalf("restart Redis after outage: %v", err)
	}
	redisTerminal, err := waitRuntimeTerminal(ctx, pool, "fault-redis-outage")
	if err != nil {
		t.Fatalf("wait Redis outage terminal state: %v", err)
	}
	if redisTerminal.Status != "SUCCEEDED" {
		t.Fatalf("Redis outage status = %q, want SUCCEEDED", redisTerminal.Status)
	}
	if err := requireRuntimeHTTPSuccess(<-redisRequest); err != nil {
		t.Fatalf("Redis outage request: %v", err)
	}
	assertFaultDispatchConsumed(t, ctx, pool, "fault-redis-outage")
	if err := waitFaultRedisPendingZero(ctx, t, redisURL, streamName, group); err != nil {
		t.Fatalf("Redis pending after outage recovery: %v", err)
	}
	evidence.RedisOutage = faultExecutionEvidence{
		RequestID: redisTerminal.RequestID, Worker: redisTerminal.Owner, Status: redisTerminal.Status,
		Attempt: redisTerminal.Attempt, DispatchStatus: faultLatestDispatchStatus(t, ctx, pool, "fault-redis-outage"),
		RedisPending: faultRedisPending(t, ctx, redisURL, streamName, group),
	}
	stopRuntimeWorker(redisWorker, true)

	postgresWorker := startRuntimeWorker(t, ctx, workerBinary, workerEnv, "fault-worker-postgres-outage", "fault-slow")
	postgresRequest := launchRuntimeRequest(ctx, dataPlaneServer.URL, scope.APIKey, "fault-postgres-outage", "fault-idem-postgres-outage", "fault-postgres-session", "postgres outage")
	if _, err := waitRuntimeExecution(ctx, pool, "fault-postgres-outage", func(value runtimeExecution) bool {
		return value.Status == "RUNNING" && value.Owner == postgresWorker.owner
	}); err != nil {
		t.Fatalf("wait PostgreSQL outage worker ownership: %v", err)
	}
	if err := faultDockerAction(ctx, "stop", postgresContainer); err != nil {
		t.Fatalf("stop PostgreSQL for outage: %v", err)
	}
	if err := waitFaultDuration(ctx, 2*time.Second); err != nil {
		t.Fatalf("hold PostgreSQL outage: %v", err)
	}
	assertRuntimeWorkerAlive(t, ctx, postgresWorker)
	if err := faultDockerAction(ctx, "start", postgresContainer); err != nil {
		t.Fatalf("restart PostgreSQL after outage: %v", err)
	}
	if err := waitFaultPostgresReady(ctx, pool); err != nil {
		t.Fatalf("wait PostgreSQL after outage: %v", err)
	}
	postgresTerminal, err := waitRuntimeTerminal(ctx, pool, "fault-postgres-outage")
	if err != nil {
		t.Fatalf("wait PostgreSQL outage terminal state: %v", err)
	}
	if postgresTerminal.Status == "CANCELED" {
		t.Fatal("PostgreSQL outage produced an unexpected canceled terminal state")
	}
	if err := requireRuntimeHTTPSuccess(<-postgresRequest); err != nil {
		t.Fatalf("PostgreSQL outage request: %v", err)
	}
	assertFaultDispatchConsumed(t, ctx, pool, "fault-postgres-outage")
	if err := waitFaultRedisPendingZero(ctx, t, redisURL, streamName, group); err != nil {
		t.Fatalf("Redis pending after PostgreSQL outage recovery: %v", err)
	}
	evidence.PostgresOutage = faultExecutionEvidence{
		RequestID: postgresTerminal.RequestID, Worker: postgresTerminal.Owner, Status: postgresTerminal.Status,
		Attempt: postgresTerminal.Attempt, DispatchStatus: faultLatestDispatchStatus(t, ctx, pool, "fault-postgres-outage"),
		RedisPending: faultRedisPending(t, ctx, redisURL, streamName, group),
	}
	stopRuntimeWorker(postgresWorker, true)

	termWorker := startRuntimeWorker(t, ctx, workerBinary, workerEnv, "fault-worker-term", "block")
	termCtx, cancelTerm := context.WithTimeout(ctx, 10*time.Second)
	defer cancelTerm()
	termRequest := launchRuntimeRequest(termCtx, dataPlaneServer.URL, scope.APIKey, "fault-term", "fault-idem-term", "fault-term-session", "sigterm")
	if _, err := waitRuntimeExecution(ctx, pool, "fault-term", func(value runtimeExecution) bool {
		return value.Status == "RUNNING" && value.Owner == termWorker.owner
	}); err != nil {
		t.Fatalf("wait SIGTERM worker ownership: %v", err)
	}
	terminateRuntimeWorker(termWorker)
	termState, err := readRuntimeExecution(ctx, pool, "fault-term")
	if err != nil {
		t.Fatalf("read SIGTERM execution: %v", err)
	}
	if termState.Status == "SUCCEEDED" {
		t.Fatal("SIGTERM canceled execution was incorrectly marked succeeded")
	}
	evidence.SIGTERM = faultExecutionEvidence{
		RequestID: termState.RequestID, Worker: termState.Owner, Status: termState.Status, Attempt: termState.Attempt,
	}
	if result := <-termRequest; result.Err == nil && result.Status == 200 {
		t.Fatalf("SIGTERM request unexpectedly succeeded: %s", result.Body)
	}
	if status := faultLatestDispatchStatus(t, ctx, pool, "fault-term"); status == "CONSUMED" {
		t.Fatal("SIGTERM left no unacknowledged retry dispatch")
	} else {
		evidence.SIGTERM.DispatchStatus = status
	}
}

type faultEvidence struct {
	Stream         string                 `json:"stream"`
	Crash          faultCrashEvidence     `json:"worker_crash"`
	Timeout        faultExecutionEvidence `json:"model_timeout"`
	RedisOutage    faultExecutionEvidence `json:"redis_outage"`
	PostgresOutage faultExecutionEvidence `json:"postgres_outage"`
	SIGTERM        faultExecutionEvidence `json:"sigterm"`
}

type faultCrashEvidence struct {
	Claimed        faultExecutionEvidence `json:"claimed"`
	Reclaimed      faultExecutionEvidence `json:"reclaimed"`
	Terminal       faultExecutionEvidence `json:"terminal"`
	DispatchStatus string                 `json:"dispatch_status"`
	RedisPending   int64                  `json:"redis_pending"`
}

type faultExecutionEvidence struct {
	RequestID      string `json:"request_id"`
	Worker         string `json:"worker"`
	Status         string `json:"status"`
	Attempt        int    `json:"attempt"`
	DispatchStatus string `json:"dispatch_status,omitempty"`
	RedisPending   int64  `json:"redis_pending,omitempty"`
}

func mustReadFaultExecution(t *testing.T, ctx context.Context, pool *pgxpool.Pool, requestID string) faultExecutionEvidence {
	t.Helper()
	value, err := readRuntimeExecution(ctx, pool, requestID)
	if err != nil {
		t.Fatalf("read fault execution %s: %v", requestID, err)
	}
	return faultExecutionEvidence{
		RequestID: value.RequestID, Worker: value.Owner, Status: value.Status, Attempt: value.Attempt,
	}
}

func writeFaultEvidence(t *testing.T, evidence faultEvidence) {
	t.Helper()
	path := strings.TrimSpace(os.Getenv("TRPC_FAULT_E2E_REPORT"))
	if path == "" {
		t.Logf("fault e2e evidence: %+v", evidence)
		return
	}
	encoded, err := json.MarshalIndent(evidence, "", "  ")
	if err != nil {
		t.Logf("encode fault evidence: %v", err)
		return
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Logf("create fault evidence directory: %v", err)
		return
	}
	if err := os.WriteFile(path, encoded, 0o600); err != nil {
		t.Logf("write fault evidence: %v", err)
	}
}

func cloneRuntimeWorkerEnv(source map[string]string) map[string]string {
	clone := make(map[string]string, len(source))
	for key, value := range source {
		clone[key] = value
	}
	return clone
}

func requiredFaultContainer(t *testing.T, name string) string {
	t.Helper()
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		t.Fatalf("%s is required for dependency outage E2E", name)
	}
	return value
}

func faultDockerAction(ctx context.Context, action, container string) error {
	dockerCommand := "docker"
	if runtime.GOOS == "windows" {
		dockerCommand = "docker.exe"
	}
	output, err := exec.CommandContext(ctx, dockerCommand, action, container).CombinedOutput()
	if err != nil {
		return fmt.Errorf("docker %s %s: %w (%s)", action, container, err, strings.TrimSpace(string(output)))
	}
	return nil
}

func waitFaultDuration(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func assertRuntimeWorkerAlive(t *testing.T, ctx context.Context, worker *runtimeWorker) {
	t.Helper()
	select {
	case <-worker.done:
		t.Fatalf("worker %s exited during dependency outage: %v\n%s", worker.owner, worker.error(), worker.logs.String())
	default:
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+worker.addr+"/readyz", nil)
	if err != nil {
		t.Fatalf("build worker readiness request: %v", err)
	}
	response, err := (&http.Client{Timeout: time.Second}).Do(request)
	if err != nil {
		t.Fatalf("worker %s readiness during outage: %v", worker.owner, err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("worker %s readiness during outage = %d, want %d", worker.owner, response.StatusCode, http.StatusOK)
	}
}

func waitFaultPostgresReady(ctx context.Context, pool *pgxpool.Pool) error {
	pool.Reset()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		if err := pool.Ping(ctx); err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func waitRuntimeTerminal(ctx context.Context, pool *pgxpool.Pool, requestID string) (runtimeExecution, error) {
	return waitRuntimeExecution(ctx, pool, requestID, func(value runtimeExecution) bool {
		switch value.Status {
		case "SUCCEEDED", "FAILED", "UNCERTAIN", "CANCELED":
			return true
		default:
			return false
		}
	})
}

func assertFaultDispatchConsumed(t *testing.T, ctx context.Context, pool *pgxpool.Pool, requestID string) {
	t.Helper()
	var status string
	if err := pool.QueryRow(ctx, `
SELECT o.status
FROM platform.dispatch_outbox o
WHERE o.request_id = $1
ORDER BY o.outbox_id DESC LIMIT 1`, requestID).Scan(&status); err != nil {
		t.Fatalf("read dispatch status %s: %v", requestID, err)
	}
	if status != "CONSUMED" {
		t.Fatalf("dispatch status %s = %q, want CONSUMED", requestID, status)
	}
}

func faultLatestDispatchStatus(t *testing.T, ctx context.Context, pool *pgxpool.Pool, requestID string) string {
	t.Helper()
	var status string
	if err := pool.QueryRow(ctx, `
SELECT status
FROM platform.dispatch_outbox
WHERE request_id = $1
ORDER BY outbox_id DESC LIMIT 1`, requestID).Scan(&status); err != nil {
		t.Fatalf("read latest dispatch status %s: %v", requestID, err)
	}
	return status
}

func faultRedisPending(t *testing.T, ctx context.Context, rawURL, stream, group string) int64 {
	t.Helper()
	options, err := redis.ParseURL(rawURL)
	if err != nil {
		t.Fatalf("parse fault redis url: %v", err)
	}
	client := redis.NewClient(options)
	defer client.Close()
	pending, err := client.XPending(ctx, stream, group).Result()
	if err != nil {
		t.Fatalf("read fault redis pending: %v", err)
	}
	return pending.Count
}

func waitFaultRedisPendingZero(ctx context.Context, t *testing.T, rawURL, stream, group string) error {
	t.Helper()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		if faultRedisPending(t, ctx, rawURL, stream, group) == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func terminateRuntimeWorker(worker *runtimeWorker) {
	if worker == nil || worker.cmd == nil || worker.cmd.Process == nil {
		return
	}
	if err := worker.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		_ = worker.cmd.Process.Kill()
	}
	select {
	case <-worker.done:
	case <-time.After(15 * time.Second):
		_ = worker.cmd.Process.Kill()
		<-worker.done
	}
}
