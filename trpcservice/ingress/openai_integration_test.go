//go:build integration

package ingress_test

import (
	"bytes"
	"context"
	"flag"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/liuzengh/trpc-agent-service/trpcservice/admin"
	"github.com/liuzengh/trpc-agent-service/trpcservice/auth"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/ingress"
	platformpostgres "github.com/liuzengh/trpc-agent-service/trpcservice/postgres"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

const ingressPostgresTestDSNEnv = "TRPC_AGENT_SERVICE_POSTGRES_TEST_DSN"

var ingressPostgresTestDSN = flag.String(
	"ingress-postgres-test-dsn",
	os.Getenv(ingressPostgresTestDSNEnv),
	"dedicated PostgreSQL integration database DSN",
)

func TestOpenAIHandlerPersistsAuthenticatedAdmission(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	pool := openIngressIntegrationPool(t)
	if _, err := pool.Exec(ctx, `DROP SCHEMA IF EXISTS platform CASCADE`); err != nil {
		t.Fatalf("reset platform schema: %v", err)
	}
	store, err := platformpostgres.New(pool)
	if err != nil {
		t.Fatalf("new postgres store: %v", err)
	}
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("migrate postgres: %v", err)
	}
	issued := seedIngressControlPlane(t, ctx, store)
	events, err := platformpostgres.NewExecutionEventJournal(store)
	if err != nil {
		t.Fatalf("new event journal: %v", err)
	}
	queued, err := gateway.NewQueuedRunner(gateway.New(store), events)
	if err != nil {
		t.Fatalf("new queued runner: %v", err)
	}
	handler, err := ingress.NewOpenAIHandler(auth.HTTPAPIKeyResolver{
		Credentials: store,
		Directory:   store,
	}, queued)
	if err != nil {
		t.Fatalf("new openai handler: %v", err)
	}

	requestCtx, cancelRequest := context.WithCancel(ctx)
	defer cancelRequest()
	request := httptest.NewRequest(
		http.MethodPost,
		"/v1/chat/completions",
		bytes.NewBufferString(`{"model":"ignored","messages":[{"role":"user","content":"hello"}]}`),
	).WithContext(requestCtx)
	request.Header.Set("Authorization", "Bearer "+issued.APIKey)
	request.Header.Set("X-Request-ID", "request-http-admission")
	request.Header.Set("Idempotency-Key", "idempotency-http-admission")
	request.Header.Set("X-Session-ID", "session-http-admission")
	request.Header.Set("X-Trace-ID", "trace-http-admission")
	response := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		defer close(done)
		handler.ServeHTTP(response, request)
	}()

	job := waitForIngressJob(t, ctx, pool, "request-http-admission")
	if job.tenantID != "tenant-ingress" || job.appID != "support" ||
		job.configVersion != "v1" || job.status != "PENDING" ||
		job.sessionPrincipalID != "service:"+issued.Credential.ID ||
		job.sessionID != "session-http-admission" || job.userID != "service:"+issued.Credential.ID {
		t.Fatalf("persisted job = %#v", job)
	}
	cancelRequest()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("OpenAI request did not stop after client cancellation")
	}
}

type ingressJob struct {
	tenantID           string
	appID              string
	configVersion      string
	status             string
	sessionPrincipalID string
	sessionID          string
	userID             string
}

func waitForIngressJob(t *testing.T, ctx context.Context, pool *pgxpool.Pool, requestID string) ingressJob {
	t.Helper()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		var job ingressJob
		err := pool.QueryRow(
			ctx,
			`SELECT tenant_id, app_id, config_version, status,
    session_principal_id, session_id, user_id
FROM platform.execution
WHERE request_id = $1`,
			requestID,
		).Scan(
			&job.tenantID,
			&job.appID,
			&job.configVersion,
			&job.status,
			&job.sessionPrincipalID,
			&job.sessionID,
			&job.userID,
		)
		if err == nil {
			return job
		}
		select {
		case <-ctx.Done():
			t.Fatalf("wait for admitted job: %v", ctx.Err())
		case <-ticker.C:
		}
	}
}

func seedIngressControlPlane(t *testing.T, ctx context.Context, store *platformpostgres.Store) admin.IssuedCredential {
	t.Helper()
	api := admin.API{Repository: store}
	tenantValue := tenant.Tenant{ID: "tenant-ingress", Name: "Ingress Tenant", Status: tenant.StatusActive}
	if err := api.CreateTenant(ctx, tenantValue); err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	config := tenant.AppConfig{
		TenantID: "tenant-ingress",
		AppID:    "support",
		Version:  "v1",
		Model: tenant.ModelConfig{
			Provider:  "openai",
			APIKeyRef: tenant.SecretRef{Name: "model-key"},
			Model:     "gpt-4.1-mini",
		},
		BackendConfig: tenant.BackendConfig{
			Name: "shared",
			Session: tenant.BackendRef{
				Kind:     tenant.BackendSQL,
				Provider: "postgres",
				Name:     "session-postgres",
			},
		},
	}
	app := tenant.AgentApp{
		TenantID:            config.TenantID,
		AppID:               config.AppID,
		Name:                "Support",
		ActiveConfigVersion: config.Version,
		Status:              tenant.StatusActive,
	}
	if err := api.CreateAgentApp(ctx, app, config); err != nil {
		t.Fatalf("create app: %v", err)
	}
	issued, err := api.IssueCredential(ctx, tenant.Scope{TenantID: config.TenantID, AppID: config.AppID}, time.Time{})
	if err != nil {
		t.Fatalf("issue credential: %v", err)
	}
	return issued
}

func openIngressIntegrationPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := *ingressPostgresTestDSN
	if dsn == "" {
		t.Skipf("%s is not set", ingressPostgresTestDSNEnv)
	}
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse postgres test DSN: %v", err)
	}
	if !strings.HasSuffix(config.ConnConfig.Database, "_test") {
		t.Fatalf("postgres integration database %q must end in _test", config.ConnConfig.Database)
	}
	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		t.Fatalf("open postgres test pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}
