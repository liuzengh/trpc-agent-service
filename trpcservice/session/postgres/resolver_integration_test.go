//go:build integration

package postgres_test

import (
	"context"
	"flag"
	"os"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	sessionpostgres "github.com/liuzengh/trpc-agent-service/trpcservice/session/postgres"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/worker"
	"trpc.group/trpc-go/trpc-agent-go/session"
)

const sessionPostgresTestDSNEnv = "TRPC_AGENT_SERVICE_POSTGRES_TEST_DSN"

var sessionPostgresTestDSN = flag.String(
	"session-postgres-test-dsn",
	os.Getenv(sessionPostgresTestDSNEnv),
	"dedicated PostgreSQL integration database DSN",
)

func TestSessionResolverPersistsDistinctCredentialSessions(t *testing.T) {
	pool := openSessionIntegrationPool(t)
	ctx := context.Background()
	const (
		firstSchema  = "agent_session_integration_a"
		secondSchema = "agent_session_integration_b"
	)
	for _, schema := range []string{firstSchema, secondSchema} {
		if _, err := pool.Exec(ctx, "DROP SCHEMA IF EXISTS "+schema+" CASCADE"); err != nil {
			t.Fatalf("reset session schema %q: %v", schema, err)
		}
		if sessionSchemaExists(t, ctx, pool, schema) {
			t.Fatalf("session schema %q exists before resolver use", schema)
		}
	}
	t.Cleanup(func() {
		for _, schema := range []string{firstSchema, secondSchema} {
			if _, err := pool.Exec(context.Background(), "DROP SCHEMA IF EXISTS "+schema+" CASCADE"); err != nil {
				t.Errorf("drop session schema %q: %v", schema, err)
			}
		}
	})

	resolver, err := sessionpostgres.NewSessionResolver(staticSessionSecret(*sessionPostgresTestDSN))
	if err != nil {
		t.Fatalf("new session resolver: %v", err)
	}
	firstService, err := resolver.ResolveSession(ctx, sessionTestExecution(t, "service:credential-a", firstSchema))
	if err != nil {
		t.Fatalf("resolve first session service: %v", err)
	}
	if !sessionSchemaExists(t, ctx, pool, firstSchema) {
		t.Fatalf("session schema %q was not created", firstSchema)
	}
	if _, err := firstService.CreateSession(ctx, session.Key{
		AppName:   "tenant:tenant-session:app:support:runner",
		UserID:    "service:credential-a",
		SessionID: "shared-session-id",
	}, session.StateMap{"marker": []byte("credential-a")}); err != nil {
		t.Fatalf("create first session: %v", err)
	}
	if _, err := firstService.CreateSession(ctx, session.Key{
		AppName:   "tenant:tenant-session:app:support:runner",
		UserID:    "service:credential-b",
		SessionID: "shared-session-id",
	}, session.StateMap{"marker": []byte("credential-b")}); err != nil {
		t.Fatalf("create second session: %v", err)
	}
	if err := resolver.Close(); err != nil {
		t.Fatalf("close first session resolver: %v", err)
	}

	resolver, err = sessionpostgres.NewSessionResolver(staticSessionSecret(*sessionPostgresTestDSN))
	if err != nil {
		t.Fatalf("new second session resolver: %v", err)
	}
	t.Cleanup(func() {
		if err := resolver.Close(); err != nil {
			t.Errorf("close second session resolver: %v", err)
		}
	})
	secondService, err := resolver.ResolveSession(ctx, sessionTestExecution(t, "service:credential-b", firstSchema))
	if err != nil {
		t.Fatalf("resolve second session service: %v", err)
	}
	assertSessionMarker(t, ctx, secondService, "service:credential-a", "credential-a")
	assertSessionMarker(t, ctx, secondService, "service:credential-b", "credential-b")

	isolatedService, err := resolver.ResolveSession(ctx, sessionTestExecution(t, "service:credential-a", secondSchema))
	if err != nil {
		t.Fatalf("resolve isolated session service: %v", err)
	}
	if !sessionSchemaExists(t, ctx, pool, secondSchema) {
		t.Fatalf("session schema %q was not created", secondSchema)
	}
	if _, err := isolatedService.CreateSession(ctx, session.Key{
		AppName:   "tenant:tenant-session:app:support:runner",
		UserID:    "service:credential-a",
		SessionID: "shared-session-id",
	}, session.StateMap{"marker": []byte("isolated")}); err != nil {
		t.Fatalf("create isolated session: %v", err)
	}
	assertSessionMarker(t, ctx, secondService, "service:credential-a", "credential-a")
	assertSessionMarker(t, ctx, isolatedService, "service:credential-a", "isolated")
}

func sessionSchemaExists(t *testing.T, ctx context.Context, pool *pgxpool.Pool, schema string) bool {
	t.Helper()
	var exists bool
	if err := pool.QueryRow(ctx, "SELECT to_regnamespace($1) IS NOT NULL", schema).Scan(&exists); err != nil {
		t.Fatalf("check session schema %q: %v", schema, err)
	}
	return exists
}

func assertSessionMarker(t *testing.T, ctx context.Context, service session.Service, userID, want string) {
	t.Helper()
	stored, err := service.GetSession(ctx, session.Key{
		AppName:   "tenant:tenant-session:app:support:runner",
		UserID:    userID,
		SessionID: "shared-session-id",
	})
	if err != nil {
		t.Fatalf("get session for %q: %v", userID, err)
	}
	if stored == nil {
		t.Fatalf("session for %q was not persisted", userID)
	}
	marker, ok := stored.GetState("marker")
	if !ok || string(marker) != want {
		t.Fatalf("session marker for %q = %q, %t, want %q", userID, marker, ok, want)
	}
}

func sessionTestExecution(t *testing.T, principal, schema string) worker.Execution {
	t.Helper()
	backend := tenant.BackendConfig{
		Name: "session-postgres",
		Session: tenant.BackendRef{
			Kind:      tenant.BackendSQL,
			Provider:  "postgres",
			Name:      "session-postgres",
			SecretRef: tenant.SecretRef{Name: "session-postgres-dsn", Version: "1"},
			Options: map[string]string{
				"schema": schema,
			},
		},
	}
	runtimeContext := tenant.RuntimeContext{
		TenantID:           "tenant-session",
		AppID:              "support",
		ConfigVersion:      "v1",
		SessionPrincipalID: principal,
		SessionID:          "shared-session-id",
		UserID:             principal,
		TraceID:            "trace-session",
	}
	return worker.Execution{
		TenantSource: gateway.TenantSourceAuthenticatedClaims,
		Tenant:       runtimeContext,
		Config: tenant.AppConfig{
			TenantID:      runtimeContext.TenantID,
			AppID:         runtimeContext.AppID,
			Version:       runtimeContext.ConfigVersion,
			BackendConfig: backend,
		},
	}
}

type staticSessionSecret string

func (r staticSessionSecret) ResolveSecret(context.Context, tenant.Scope, tenant.SecretRef) (string, error) {
	return string(r), nil
}

func openSessionIntegrationPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	if *sessionPostgresTestDSN == "" {
		t.Skip(sessionPostgresTestDSNEnv + " is not set")
	}
	config, err := pgxpool.ParseConfig(*sessionPostgresTestDSN)
	if err != nil {
		t.Fatalf("parse PostgreSQL integration DSN: %v", err)
	}
	if !strings.HasSuffix(config.ConnConfig.Database, "_test") {
		t.Fatalf("postgres integration database %q must end in _test", config.ConnConfig.Database)
	}
	pool, err := pgxpool.New(context.Background(), *sessionPostgresTestDSN)
	if err != nil {
		t.Fatalf("open PostgreSQL integration pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}
