//go:build integration && e2e

package postgres_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/liuzengh/trpc-agent-service/trpcservice/admin"
	"github.com/liuzengh/trpc-agent-service/trpcservice/auth"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/ingress"
	platformpostgres "github.com/liuzengh/trpc-agent-service/trpcservice/postgres"
	platformredis "github.com/liuzengh/trpc-agent-service/trpcservice/redis"
	"github.com/liuzengh/trpc-agent-service/trpcservice/relay"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

const (
	runtimePostgresDefaultDSN = "postgres://trpc:development-only-change-me@127.0.0.1:55432/trpc_agent_service_test?sslmode=disable"
	runtimeRedisDefaultURL    = "redis://127.0.0.1:56379/0"
	runtimeSessionSchema      = "runtime_e2e"
	runtimeWorkerBinaryEnv    = "TRPC_AGENT_SERVICE_E2E_WORKER_BINARY"
	runtimeReportPathEnv      = "TRPC_RUNTIME_E2E_REPORT"
)

func TestMultiTenantRuntimeAcrossRealWorkers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
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
	tenantA := seedRuntimeScope(t, ctx, store, "runtime-tenant-a-"+runID, "support")
	tenantB := seedRuntimeScope(t, ctx, store, "runtime-tenant-b-"+runID, "support")
	api := admin.API{Repository: store}
	v2 := runtimeConfig(tenantA.TenantID, tenantA.AppID, "v2", "fake-v2")
	if err := api.PublishAppConfig(ctx, v2); err != nil {
		t.Fatalf("publish v2: %v", err)
	}

	redisURL := runtimeRedisURL()
	redisClient, err := platformredis.NewClient(ctx, redisURL)
	if err != nil {
		t.Fatalf("new redis client: %v", err)
	}
	defer redisClient.Close()
	streamName := "trpc-agent-service:runtime-e2e:" + uuid.NewString()
	group := "runtime-e2e-workers"
	stream, err := platformredis.NewStream(redisClient, streamName, group, time.Second)
	if err != nil {
		t.Fatalf("new runtime stream: %v", err)
	}
	if err := stream.Init(ctx); err != nil {
		t.Fatalf("initialize runtime stream: %v", err)
	}
	dispatcher, err := relay.New(store, stream, "runtime-e2e-relay")
	if err != nil {
		t.Fatalf("new runtime relay: %v", err)
	}
	relayCtx, stopRelay := context.WithCancel(ctx)
	relayDone := make(chan error, 1)
	go func() { relayDone <- dispatcher.Run(relayCtx) }()
	t.Cleanup(func() {
		stopRelay()
		if err := <-relayDone; err != nil && !errors.Is(err, context.Canceled) {
			t.Logf("runtime relay stopped with error: %v", err)
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
	workerA := startRuntimeWorker(t, ctx, workerBinary, workerEnv, "runtime-worker-a", "slow")

	evidence := runtimeEvidence{
		Stream: streamName,
		Tenants: []runtimeTenantEvidence{
			{TenantID: tenantA.TenantID, AppID: tenantA.AppID},
			{TenantID: tenantB.TenantID, AppID: tenantB.AppID},
		},
	}

	first := launchRuntimeRequest(ctx, dataPlaneServer.URL, tenantA.APIKey, "runtime-a-1", "idem-a-1", "same-session", "first")
	firstRunning, err := waitRuntimeExecution(ctx, pool, "runtime-a-1", func(value runtimeExecution) bool {
		return value.Status == "RUNNING" && value.Owner == "runtime-worker-a"
	})
	if err != nil {
		t.Fatalf("wait first execution on worker A: %v", err)
	}
	if err := requireRuntimeHTTPSuccess(<-first); err != nil {
		t.Fatalf("first runtime request: %v", err)
	}
	evidence.CrossWorker = append(evidence.CrossWorker, runtimeExecutionEvidence{
		RequestID: firstRunning.RequestID, Worker: firstRunning.Owner, ConfigVersion: firstRunning.ConfigVersion,
		TurnSeq: firstRunning.TurnSeq,
	})
	stopRuntimeWorker(workerA, true)

	workerB := startRuntimeWorker(t, ctx, workerBinary, workerEnv, "runtime-worker-b", "slow")
	second := launchRuntimeRequest(ctx, dataPlaneServer.URL, tenantA.APIKey, "runtime-a-2", "idem-a-2", "same-session", "second")
	secondRunning, err := waitRuntimeExecution(ctx, pool, "runtime-a-2", func(value runtimeExecution) bool {
		return value.Status == "RUNNING" && value.Owner == "runtime-worker-b"
	})
	if err != nil {
		t.Fatalf("wait second execution on worker B: %v", err)
	}
	if secondRunning.TurnSeq != firstRunning.TurnSeq+1 {
		t.Fatalf("cross-worker turn sequence = %d, want %d", secondRunning.TurnSeq, firstRunning.TurnSeq+1)
	}
	if err := requireRuntimeHTTPSuccess(<-second); err != nil {
		t.Fatalf("second runtime request: %v", err)
	}
	evidence.CrossWorker = append(evidence.CrossWorker, runtimeExecutionEvidence{
		RequestID: secondRunning.RequestID, Worker: secondRunning.Owner, ConfigVersion: secondRunning.ConfigVersion,
		TurnSeq: secondRunning.TurnSeq,
	})

	otherTenant := launchRuntimeRequest(ctx, dataPlaneServer.URL, tenantB.APIKey, "runtime-b-1", "idem-b-1", "same-session", "tenant b")
	otherExecution, err := waitRuntimeExecution(ctx, pool, "runtime-b-1", func(value runtimeExecution) bool {
		return value.Status == "RUNNING" && value.TenantID == tenantB.TenantID
	})
	if err != nil {
		t.Fatalf("wait tenant B execution: %v", err)
	}
	if err := requireRuntimeHTTPSuccess(<-otherTenant); err != nil {
		t.Fatalf("tenant B runtime request: %v", err)
	}
	if otherExecution.AppID != tenantB.AppID || otherExecution.ConfigVersion != "v1" {
		t.Fatalf("tenant B execution = %#v", otherExecution)
	}
	evidence.Isolation = append(evidence.Isolation, runtimeExecutionEvidence{
		RequestID: otherExecution.RequestID, Worker: otherExecution.Owner, ConfigVersion: otherExecution.ConfigVersion,
		TurnSeq: otherExecution.TurnSeq,
	})
	stopRuntimeWorker(workerB, true)

	oldRequest := launchRuntimeRequest(ctx, dataPlaneServer.URL, tenantA.APIKey, "runtime-pin-old", "idem-pin-old", "pin-session", "admitted on v1")
	oldPending, err := waitRuntimeExecution(ctx, pool, "runtime-pin-old", func(value runtimeExecution) bool {
		return value.Status == "PENDING" && value.ConfigVersion == "v1"
	})
	if err != nil {
		t.Fatalf("wait pinned v1 admission: %v", err)
	}
	if err := api.ActivateAppConfig(ctx, tenant.Scope{TenantID: tenantA.TenantID, AppID: tenantA.AppID}, "v2"); err != nil {
		t.Fatalf("activate v2: %v", err)
	}
	workerC := startRuntimeWorker(t, ctx, workerBinary, workerEnv, "runtime-worker-c", "slow")
	if err := requireRuntimeHTTPSuccess(<-oldRequest); err != nil {
		t.Fatalf("pinned v1 runtime request: %v", err)
	}
	oldTerminal, err := waitRuntimeExecution(ctx, pool, oldPending.RequestID, func(value runtimeExecution) bool {
		return value.Status == "SUCCEEDED"
	})
	if err != nil {
		t.Fatalf("wait pinned v1 terminal state: %v", err)
	}
	if oldTerminal.ConfigVersion != "v1" {
		t.Fatalf("pinned execution config version = %q, want v1", oldTerminal.ConfigVersion)
	}

	newRequest := launchRuntimeRequest(ctx, dataPlaneServer.URL, tenantA.APIKey, "runtime-pin-new", "idem-pin-new", "pin-session", "admitted on v2")
	newExecution, err := waitRuntimeExecution(ctx, pool, "runtime-pin-new", func(value runtimeExecution) bool {
		return value.ConfigVersion == "v2" && value.Status == "RUNNING" && value.Owner == workerC.owner
	})
	if err != nil {
		t.Fatalf("wait v2 admission: %v", err)
	}
	if err := requireRuntimeHTTPSuccess(<-newRequest); err != nil {
		t.Fatalf("v2 runtime request: %v", err)
	}
	evidence.ConfigPinning = append(evidence.ConfigPinning, runtimeExecutionEvidence{
		RequestID: oldTerminal.RequestID, Worker: oldTerminal.Owner, ConfigVersion: oldTerminal.ConfigVersion, TurnSeq: oldTerminal.TurnSeq,
	}, runtimeExecutionEvidence{
		RequestID: newExecution.RequestID, Worker: newExecution.Owner, ConfigVersion: newExecution.ConfigVersion, TurnSeq: newExecution.TurnSeq,
	})

	if err := api.RollbackAppConfig(ctx, tenant.Scope{TenantID: tenantA.TenantID, AppID: tenantA.AppID}, "v1"); err != nil {
		t.Fatalf("rollback v1: %v", err)
	}
	rollbackRequest := launchRuntimeRequest(ctx, dataPlaneServer.URL, tenantA.APIKey, "runtime-rollback", "idem-rollback", "rollback-session", "after rollback")
	rollbackExecution, err := waitRuntimeExecution(ctx, pool, "runtime-rollback", func(value runtimeExecution) bool {
		return value.ConfigVersion == "v1" && value.Status == "RUNNING"
	})
	if err != nil {
		t.Fatalf("wait rollback admission: %v", err)
	}
	if err := requireRuntimeHTTPSuccess(<-rollbackRequest); err != nil {
		t.Fatalf("rollback runtime request: %v", err)
	}
	evidence.Rollback = runtimeExecutionEvidence{
		RequestID: rollbackExecution.RequestID, Worker: rollbackExecution.Owner,
		ConfigVersion: rollbackExecution.ConfigVersion, TurnSeq: rollbackExecution.TurnSeq,
	}

	canaryVersion := "v3"
	if err := api.PublishAppConfig(ctx, runtimeConfig(tenantA.TenantID, tenantA.AppID, canaryVersion, "fake-v3")); err != nil {
		t.Fatalf("publish canary config: %v", err)
	}
	if _, err := api.EnableAppCanary(ctx, tenant.Scope{TenantID: tenantA.TenantID, AppID: tenantA.AppID}, canaryVersion, 100); err != nil {
		t.Fatalf("enable canary: %v", err)
	}
	canaryRequest := launchRuntimeRequest(ctx, dataPlaneServer.URL, tenantA.APIKey, "runtime-canary-enable", "idem-canary-enable", "canary-session", "canary enabled")
	canaryExecution, err := waitRuntimeExecution(ctx, pool, "runtime-canary-enable", func(value runtimeExecution) bool {
		return value.ConfigVersion == canaryVersion && value.Status == "RUNNING"
	})
	if err != nil {
		t.Fatalf("wait canary admission: %v", err)
	}
	if err := requireRuntimeHTTPSuccess(<-canaryRequest); err != nil {
		t.Fatalf("canary request: %v", err)
	}

	if _, err := api.PauseAppCanary(ctx, tenant.Scope{TenantID: tenantA.TenantID, AppID: tenantA.AppID}); err != nil {
		t.Fatalf("pause canary: %v", err)
	}
	pausedRequest := launchRuntimeRequest(ctx, dataPlaneServer.URL, tenantA.APIKey, "runtime-canary-paused", "idem-canary-paused", "canary-session", "canary paused")
	pausedExecution, err := waitRuntimeExecution(ctx, pool, "runtime-canary-paused", func(value runtimeExecution) bool {
		return value.Status == "RUNNING"
	})
	if err != nil {
		t.Fatalf("wait paused canary admission: %v", err)
	}
	if pausedExecution.ConfigVersion != "v1" {
		t.Fatalf("paused canary config version = %q, want v1", pausedExecution.ConfigVersion)
	}
	if err := requireRuntimeHTTPSuccess(<-pausedRequest); err != nil {
		t.Fatalf("paused canary request: %v", err)
	}

	if _, err := api.EnableAppCanary(ctx, tenant.Scope{TenantID: tenantA.TenantID, AppID: tenantA.AppID}, canaryVersion, 100); err != nil {
		t.Fatalf("re-enable canary: %v", err)
	}
	if _, err := api.PromoteAppCanary(ctx, tenant.Scope{TenantID: tenantA.TenantID, AppID: tenantA.AppID}); err != nil {
		t.Fatalf("promote canary: %v", err)
	}
	promotedRequest := launchRuntimeRequest(ctx, dataPlaneServer.URL, tenantA.APIKey, "runtime-canary-promoted", "idem-canary-promoted", "canary-promoted-session", "canary promoted")
	promotedExecution, err := waitRuntimeExecution(ctx, pool, "runtime-canary-promoted", func(value runtimeExecution) bool {
		return value.Status == "RUNNING"
	})
	if err != nil {
		t.Fatalf("wait promoted canary admission: %v", err)
	}
	if promotedExecution.ConfigVersion != canaryVersion {
		t.Fatalf("promoted canary config version = %q, want %s", promotedExecution.ConfigVersion, canaryVersion)
	}
	if err := requireRuntimeHTTPSuccess(<-promotedRequest); err != nil {
		t.Fatalf("promoted canary request: %v", err)
	}

	rollbackCanaryVersion := "v4"
	if err := api.PublishAppConfig(ctx, runtimeConfig(tenantA.TenantID, tenantA.AppID, rollbackCanaryVersion, "fake-v4")); err != nil {
		t.Fatalf("publish rollback canary config: %v", err)
	}
	if _, err := api.EnableAppCanary(ctx, tenant.Scope{TenantID: tenantA.TenantID, AppID: tenantA.AppID}, rollbackCanaryVersion, 100); err != nil {
		t.Fatalf("enable rollback canary: %v", err)
	}
	if _, err := api.RollbackAppCanary(ctx, tenant.Scope{TenantID: tenantA.TenantID, AppID: tenantA.AppID}); err != nil {
		t.Fatalf("rollback canary: %v", err)
	}
	rolledBackCanaryRequest := launchRuntimeRequest(ctx, dataPlaneServer.URL, tenantA.APIKey, "runtime-canary-rollback", "idem-canary-rollback", "canary-rollback-session", "canary rollback")
	rolledBackCanaryExecution, err := waitRuntimeExecution(ctx, pool, "runtime-canary-rollback", func(value runtimeExecution) bool {
		return value.Status == "RUNNING"
	})
	if err != nil {
		t.Fatalf("wait rolled back canary admission: %v", err)
	}
	if rolledBackCanaryExecution.ConfigVersion != canaryVersion {
		t.Fatalf("rolled back canary config version = %q, want promoted stable %s", rolledBackCanaryExecution.ConfigVersion, canaryVersion)
	}
	if err := requireRuntimeHTTPSuccess(<-rolledBackCanaryRequest); err != nil {
		t.Fatalf("rolled back canary request: %v", err)
	}
	evidence.Canary = []runtimeExecutionEvidence{
		{RequestID: canaryExecution.RequestID, Worker: canaryExecution.Owner, ConfigVersion: canaryExecution.ConfigVersion, TurnSeq: canaryExecution.TurnSeq},
		{RequestID: pausedExecution.RequestID, Worker: pausedExecution.Owner, ConfigVersion: pausedExecution.ConfigVersion, TurnSeq: pausedExecution.TurnSeq},
		{RequestID: promotedExecution.RequestID, Worker: promotedExecution.Owner, ConfigVersion: promotedExecution.ConfigVersion, TurnSeq: promotedExecution.TurnSeq},
		{RequestID: rolledBackCanaryExecution.RequestID, Worker: rolledBackCanaryExecution.Owner, ConfigVersion: rolledBackCanaryExecution.ConfigVersion, TurnSeq: rolledBackCanaryExecution.TurnSeq},
	}

	concurrentOne := launchRuntimeRequest(ctx, dataPlaneServer.URL, tenantA.APIKey, "runtime-concurrent-1", "idem-concurrent-1", "same-session", "parallel one")
	concurrentTwo := launchRuntimeRequest(ctx, dataPlaneServer.URL, tenantA.APIKey, "runtime-concurrent-2", "idem-concurrent-2", "same-session", "parallel two")
	parallelOne, err := waitRuntimeExecution(ctx, pool, "runtime-concurrent-1", func(value runtimeExecution) bool {
		return value.Status == "SUCCEEDED"
	})
	if err != nil {
		t.Fatalf("wait first concurrent session execution: %v", err)
	}
	parallelTwo, err := waitRuntimeExecution(ctx, pool, "runtime-concurrent-2", func(value runtimeExecution) bool {
		return value.Status == "SUCCEEDED"
	})
	if err != nil {
		t.Fatalf("wait second concurrent session execution: %v", err)
	}
	if err := requireRuntimeHTTPSuccess(<-concurrentOne); err != nil {
		t.Fatalf("first concurrent runtime request: %v", err)
	}
	if err := requireRuntimeHTTPSuccess(<-concurrentTwo); err != nil {
		t.Fatalf("second concurrent runtime request: %v", err)
	}
	if parallelOne.TurnSeq == parallelTwo.TurnSeq || parallelOne.TurnSeq > parallelTwo.TurnSeq {
		parallelOne, parallelTwo = parallelTwo, parallelOne
	}
	if parallelTwo.TurnSeq != parallelOne.TurnSeq+1 {
		t.Fatalf("concurrent session turn sequence = %d, %d", parallelOne.TurnSeq, parallelTwo.TurnSeq)
	}
	evidence.ConcurrentSession = []runtimeExecutionEvidence{
		{RequestID: parallelOne.RequestID, Worker: parallelOne.Owner, ConfigVersion: parallelOne.ConfigVersion, TurnSeq: parallelOne.TurnSeq},
		{RequestID: parallelTwo.RequestID, Worker: parallelTwo.Owner, ConfigVersion: parallelTwo.ConfigVersion, TurnSeq: parallelTwo.TurnSeq},
	}

	evidence.SessionEventCounts = map[string]int64{
		tenantA.TenantID: sessionEventCount(t, ctx, pool, tenantA, "same-session"),
		tenantB.TenantID: sessionEventCount(t, ctx, pool, tenantB, "same-session"),
	}
	if evidence.SessionEventCounts[tenantA.TenantID] < 4 || evidence.SessionEventCounts[tenantB.TenantID] < 2 {
		t.Fatalf("session event counts = %#v", evidence.SessionEventCounts)
	}
	writeRuntimeEvidence(t, evidence)
}

type runtimeScope struct {
	TenantID     string
	AppID        string
	APIKey       string
	CredentialID string
}

func seedRuntimeScope(t *testing.T, ctx context.Context, store *platformpostgres.Store, tenantID, appID string) runtimeScope {
	t.Helper()
	api := admin.API{Repository: store}
	if err := api.CreateTenant(ctx, tenant.Tenant{
		ID: tenantID, Name: "Runtime Tenant", Status: tenant.StatusActive,
	}); err != nil {
		t.Fatalf("create tenant %s: %v", tenantID, err)
	}
	cfg := runtimeConfig(tenantID, appID, "v1", "fake-v1")
	if err := api.CreateAgentApp(ctx, tenant.AgentApp{
		TenantID: tenantID, AppID: appID, Name: "Support", ActiveConfigVersion: cfg.Version, Status: tenant.StatusActive,
	}, cfg); err != nil {
		t.Fatalf("create app %s/%s: %v", tenantID, appID, err)
	}
	issued, err := api.IssueCredential(ctx, tenant.Scope{TenantID: tenantID, AppID: appID}, time.Time{})
	if err != nil {
		t.Fatalf("issue credential %s/%s: %v", tenantID, appID, err)
	}
	return runtimeScope{TenantID: tenantID, AppID: appID, APIKey: issued.APIKey, CredentialID: issued.Credential.ID}
}

func runtimeConfig(tenantID, appID, version, modelName string) tenant.AppConfig {
	return tenant.AppConfig{
		TenantID: tenantID,
		AppID:    appID,
		Version:  version,
		Model: tenant.ModelConfig{
			Provider:  tenant.ModelProviderOpenAI,
			Model:     modelName,
			APIKeyRef: tenant.SecretRef{Name: "model-key"},
		},
		BackendConfig: tenant.BackendConfig{
			Name: "runtime-session",
			Session: tenant.BackendRef{
				Kind:     tenant.BackendSQL,
				Provider: "postgres",
				Name:     "runtime-postgres",
				Options:  map[string]string{"schema": runtimeSessionSchema},
			},
		},
	}
}

type runtimeExecution struct {
	RequestID     string
	TenantID      string
	AppID         string
	ConfigVersion string
	Status        string
	Owner         string
	SessionID     string
	TurnSeq       int64
	Attempt       int
}

func waitRuntimeExecution(ctx context.Context, pool *pgxpool.Pool, requestID string, predicate func(runtimeExecution) bool) (runtimeExecution, error) {
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		value, err := readRuntimeExecution(ctx, pool, requestID)
		if err == nil && predicate(value) {
			return value, nil
		}
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return runtimeExecution{}, err
		}
		select {
		case <-ctx.Done():
			return runtimeExecution{}, fmt.Errorf("wait execution %s: %w", requestID, ctx.Err())
		case <-ticker.C:
		}
	}
}

func readRuntimeExecution(ctx context.Context, pool *pgxpool.Pool, requestID string) (runtimeExecution, error) {
	var value runtimeExecution
	err := pool.QueryRow(ctx, `
SELECT request_id, tenant_id, app_id, config_version, status,
       COALESCE(lease_owner, ''), session_id, turn_seq, attempt
FROM platform.execution
WHERE request_id = $1
ORDER BY created_at DESC
LIMIT 1`, requestID).Scan(
		&value.RequestID, &value.TenantID, &value.AppID, &value.ConfigVersion,
		&value.Status, &value.Owner, &value.SessionID, &value.TurnSeq, &value.Attempt,
	)
	return value, err
}

func sessionEventCount(t *testing.T, ctx context.Context, pool *pgxpool.Pool, scope runtimeScope, sessionID string) int64 {
	t.Helper()
	appName, err := tenant.Scope{TenantID: scope.TenantID, AppID: scope.AppID}.Key("runner")
	if err != nil {
		t.Fatalf("build session app name: %v", err)
	}
	var count int64
	query := fmt.Sprintf(`SELECT count(*) FROM %s.session_events WHERE app_name = $1 AND user_id = $2 AND session_id = $3`,
		`"`+runtimeSessionSchema+`"`)
	if err := pool.QueryRow(ctx, query, appName, "service:"+scope.CredentialID, sessionID).Scan(&count); err != nil {
		t.Fatalf("count session events for %s: %v", scope.TenantID, err)
	}
	return count
}

type runtimeHTTPResult struct {
	Status int
	Body   []byte
	Err    error
}

func launchRuntimeRequest(ctx context.Context, endpoint, apiKey, requestID, idempotencyKey, sessionID, message string) <-chan runtimeHTTPResult {
	result := make(chan runtimeHTTPResult, 1)
	go func() {
		body := bytes.NewBufferString(fmt.Sprintf(`{"model":"ignored","messages":[{"role":"user","content":%q}]}`, message))
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint+"/v1/chat/completions", body)
		if err != nil {
			result <- runtimeHTTPResult{Err: err}
			return
		}
		req.Header.Set("Authorization", "Bearer "+apiKey)
		req.Header.Set("X-Request-ID", requestID)
		req.Header.Set("Idempotency-Key", idempotencyKey)
		req.Header.Set("X-Session-ID", sessionID)
		response, err := http.DefaultClient.Do(req)
		if err != nil {
			result <- runtimeHTTPResult{Err: err}
			return
		}
		defer response.Body.Close()
		encoded, readErr := io.ReadAll(response.Body)
		result <- runtimeHTTPResult{Status: response.StatusCode, Body: encoded, Err: readErr}
	}()
	return result
}

func requireRuntimeHTTPSuccess(result runtimeHTTPResult) error {
	if result.Err != nil {
		return result.Err
	}
	if result.Status != http.StatusOK {
		return fmt.Errorf("status=%d body=%q", result.Status, result.Body)
	}
	if !bytes.Contains(result.Body, []byte("deterministic response")) {
		return fmt.Errorf("response did not contain deterministic result: %q", result.Body)
	}
	return nil
}

func openRuntimePool(t *testing.T) (*pgxpool.Pool, string) {
	t.Helper()
	dsn := strings.TrimSpace(*postgresTestDSN)
	if dsn == "" {
		dsn = runtimePostgresDefaultDSN
	}
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse runtime postgres dsn: %v", err)
	}
	if !strings.HasSuffix(config.ConnConfig.Database, "_test") {
		t.Fatalf("runtime postgres database %q must end in _test", config.ConnConfig.Database)
	}
	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		t.Fatalf("open runtime postgres pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool, dsn
}

func runtimeRedisURL() string {
	if value := strings.TrimSpace(*relayRedisTestURL); value != "" {
		return value
	}
	return runtimeRedisDefaultURL
}

type runtimeWorker struct {
	owner   string
	addr    string
	cmd     *exec.Cmd
	logs    *lockedBuffer
	done    chan struct{}
	waitMu  sync.Mutex
	waitErr error
}

type lockedBuffer struct {
	mu   sync.Mutex
	data bytes.Buffer
}

func (b *lockedBuffer) Write(value []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.data.Write(value)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.data.String()
}

func buildRuntimeWorker(t *testing.T) string {
	t.Helper()
	if value := strings.TrimSpace(os.Getenv(runtimeWorkerBinaryEnv)); value != "" {
		return value
	}
	root := repositoryRoot(t)
	name := "e2e-worker"
	if runtime.GOOS == "windows" {
		name += ".exe"
	}
	path := filepath.Join(t.TempDir(), name)
	command := exec.Command("go", "build", "-o", path, "./cmd/e2e-worker")
	command.Dir = root
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("build e2e-worker: %v\n%s", err, output)
	}
	return path
}

func repositoryRoot(t *testing.T) string {
	t.Helper()
	directory, err := os.Getwd()
	if err != nil {
		t.Fatalf("get test directory: %v", err)
	}
	for {
		if _, err := os.Stat(filepath.Join(directory, "go.mod")); err == nil {
			return directory
		}
		parent := filepath.Dir(directory)
		if parent == directory {
			t.Fatal("go.mod was not found from test directory")
		}
		directory = parent
	}
}

func runtimeWorkerEnv(postgresDSN, redisURL, stream, group string) map[string]string {
	return map[string]string{
		"TRPC_AGENT_SERVICE_POSTGRES_DSN":         postgresDSN,
		"TRPC_AGENT_SERVICE_REDIS_URL":            redisURL,
		"TRPC_AGENT_SERVICE_REDIS_STREAM":         stream,
		"TRPC_AGENT_SERVICE_REDIS_GROUP":          group,
		"TRPC_AGENT_SERVICE_REDIS_CLAIM_MIN_IDLE": "6s",
		"TRPC_AGENT_SERVICE_MODEL_TIMEOUT":        "15s",
		"TRPC_AGENT_SERVICE_WORKER_CONCURRENCY":   "1",
	}
}

func startRuntimeWorker(t *testing.T, ctx context.Context, binary string, baseEnv map[string]string, owner, mode string) *runtimeWorker {
	t.Helper()
	address := freeTCPAddress(t)
	env := append([]string(nil), os.Environ()...)
	for key, value := range baseEnv {
		env = append(env, key+"="+value)
	}
	env = append(env,
		"TRPC_AGENT_SERVICE_WORKER_ID="+owner,
		"TRPC_AGENT_SERVICE_HTTP_ADDR="+address,
		"TRPC_E2E_MODEL_MODE="+mode,
	)
	logs := &lockedBuffer{}
	command := exec.CommandContext(ctx, binary)
	command.Dir = repositoryRoot(t)
	command.Env = env
	command.Stdout = logs
	command.Stderr = logs
	if err := command.Start(); err != nil {
		t.Fatalf("start %s: %v", owner, err)
	}
	worker := &runtimeWorker{owner: owner, addr: address, cmd: command, logs: logs, done: make(chan struct{})}
	go func() {
		err := command.Wait()
		worker.waitMu.Lock()
		worker.waitErr = err
		worker.waitMu.Unlock()
		close(worker.done)
	}()
	if err := waitRuntimeWorkerReady(ctx, worker); err != nil {
		stopRuntimeWorker(worker, true)
		t.Fatalf("wait %s ready: %v\n%s", owner, err, worker.logs.String())
	}
	t.Cleanup(func() { stopRuntimeWorker(worker, true) })
	return worker
}

func freeTCPAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("allocate worker port: %v", err)
	}
	address := listener.Addr().String()
	_ = listener.Close()
	return address
}

func waitRuntimeWorkerReady(ctx context.Context, worker *runtimeWorker) error {
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	client := &http.Client{Timeout: 500 * time.Millisecond}
	for {
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+worker.addr+"/readyz", nil)
		if err == nil {
			response, requestErr := client.Do(request)
			if requestErr == nil {
				_ = response.Body.Close()
				if response.StatusCode == http.StatusOK {
					return nil
				}
			}
		}
		select {
		case <-worker.done:
			return fmt.Errorf("process exited: %v", worker.error())
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (w *runtimeWorker) error() error {
	w.waitMu.Lock()
	defer w.waitMu.Unlock()
	return w.waitErr
}

func stopRuntimeWorker(worker *runtimeWorker, kill bool) {
	if worker == nil || worker.cmd == nil || worker.cmd.Process == nil {
		return
	}
	if kill {
		_ = worker.cmd.Process.Kill()
	}
	select {
	case <-worker.done:
	case <-time.After(5 * time.Second):
		_ = worker.cmd.Process.Kill()
		<-worker.done
	}
}

type runtimeEvidence struct {
	Stream             string                     `json:"stream"`
	Tenants            []runtimeTenantEvidence    `json:"tenants"`
	CrossWorker        []runtimeExecutionEvidence `json:"cross_worker"`
	Isolation          []runtimeExecutionEvidence `json:"isolation"`
	ConfigPinning      []runtimeExecutionEvidence `json:"config_pinning"`
	Rollback           runtimeExecutionEvidence   `json:"rollback"`
	Canary             []runtimeExecutionEvidence `json:"canary"`
	ConcurrentSession  []runtimeExecutionEvidence `json:"concurrent_session"`
	SessionEventCounts map[string]int64           `json:"session_event_counts"`
}

type runtimeTenantEvidence struct {
	TenantID string `json:"tenant_id"`
	AppID    string `json:"app_id"`
}

type runtimeExecutionEvidence struct {
	RequestID     string `json:"request_id"`
	Worker        string `json:"worker"`
	ConfigVersion string `json:"config_version"`
	TurnSeq       int64  `json:"turn_seq"`
}

func writeRuntimeEvidence(t *testing.T, evidence runtimeEvidence) {
	t.Helper()
	path := strings.TrimSpace(os.Getenv(runtimeReportPathEnv))
	if path == "" {
		t.Logf("runtime e2e evidence: %+v", evidence)
		return
	}
	encoded, err := json.MarshalIndent(evidence, "", "  ")
	if err != nil {
		t.Fatalf("encode runtime evidence: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("create runtime evidence directory: %v", err)
	}
	if err := os.WriteFile(path, encoded, 0o600); err != nil {
		t.Fatalf("write runtime evidence: %v", err)
	}
}
