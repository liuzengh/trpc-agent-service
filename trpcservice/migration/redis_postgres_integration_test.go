//go:build integration

package migration_test

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/liuzengh/trpc-agent-service/trpcservice/migration"
	sessionpostgres "github.com/liuzengh/trpc-agent-service/trpcservice/session/postgres"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/worker"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/session"
	redisprovider "trpc.group/trpc-go/trpc-agent-go/session/redis"
)

var (
	redisPostgresTestDSN = flag.String(
		"redis-postgres-test-dsn",
		os.Getenv("TRPC_AGENT_SERVICE_POSTGRES_TEST_DSN"),
		"dedicated PostgreSQL integration database DSN",
	)
	redisPostgresTestURL = flag.String(
		"redis-postgres-test-url",
		os.Getenv("TRPC_AGENT_SERVICE_REDIS_TEST_URL"),
		"Redis integration URL",
	)
)

func TestRedisPostgresCopierPreservesSessionSummary(t *testing.T) {
	if *redisPostgresTestDSN == "" || *redisPostgresTestURL == "" {
		t.Skip("TRPC_AGENT_SERVICE_POSTGRES_TEST_DSN and TRPC_AGENT_SERVICE_REDIS_TEST_URL are required")
	}
	pool := openRedisPostgresIntegrationPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	schema := fmt.Sprintf("mig%x", time.Now().UnixNano()&0xffffff)
	if _, err := pool.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatalf("create target schema: %v", err)
	}
	t.Cleanup(func() {
		if _, err := pool.Exec(context.Background(), "DROP SCHEMA IF EXISTS "+schema+" CASCADE"); err != nil {
			t.Errorf("drop target schema: %v", err)
		}
	})

	key := session.Key{
		AppName:   "tenant:integration:app:migration:runner",
		UserID:    fmt.Sprintf("user-%d", time.Now().UnixNano()),
		SessionID: "session-1",
	}
	sourceBase, err := redisprovider.NewService(redisprovider.WithRedisClientURL(*redisPostgresTestURL))
	if err != nil {
		t.Fatalf("create redis source service: %v", err)
	}
	t.Cleanup(func() { _ = sourceBase.Close() })
	sourceSession, err := sourceBase.CreateSession(ctx, key, session.StateMap{"topic": []byte("billing")})
	if err != nil {
		t.Fatalf("create redis source session: %v", err)
	}
	t.Cleanup(func() { _ = sourceBase.DeleteSession(context.Background(), key) })
	if err := sourceBase.AppendEvent(ctx, sourceSession, event.NewResponseEvent("event-1", "user", &model.Response{
		Object: model.ObjectTypeChatCompletion,
		Done:   true,
		Choices: []model.Choice{{
			Message: model.NewUserMessage("summarize billing"),
		}},
	})); err != nil {
		t.Fatalf("append redis source user event: %v", err)
	}
	if err := sourceBase.AppendEvent(ctx, sourceSession, event.NewResponseEvent("event-2", "assistant", &model.Response{
		Object: model.ObjectTypeChatCompletion,
		Done:   true,
		Choices: []model.Choice{{
			Message: model.NewAssistantMessage("billing reply"),
		}},
	})); err != nil {
		t.Fatalf("append redis source event: %v", err)
	}

	targetExec := integrationTargetExecution(t, key, schema)
	resolver, err := sessionpostgres.NewSessionResolver(integrationSecret(*redisPostgresTestDSN))
	if err != nil {
		t.Fatalf("create postgres resolver: %v", err)
	}
	t.Cleanup(func() {
		if err := resolver.Close(); err != nil {
			t.Errorf("close postgres resolver: %v", err)
		}
	})
	target, err := resolver.ResolveSession(ctx, targetExec)
	if err != nil {
		t.Fatalf("resolve postgres target: %v", err)
	}
	importer, err := resolver.NewSummaryImporter(ctx, targetExec)
	if err != nil {
		t.Fatalf("create summary importer: %v", err)
	}
	t.Cleanup(importer.Close)

	source := summaryRedisSource{
		Service: sourceBase,
		summary: &session.Summary{Summary: "billing summary", UpdatedAt: time.Now().UTC()},
	}
	copier := migration.RedisPostgresCopier{Source: source, Target: target, Summaries: importer}
	if err := copier.CopySession(ctx, key); err != nil {
		t.Fatalf("copy redis session: %v", err)
	}
	if err := copier.VerifySession(ctx, key); err != nil {
		t.Fatalf("verify postgres session: %v", err)
	}
}

type summaryRedisSource struct {
	session.Service
	summary *session.Summary
}

func (s summaryRedisSource) GetSession(
	ctx context.Context,
	key session.Key,
	options ...session.Option,
) (*session.Session, error) {
	value, err := s.Service.GetSession(ctx, key, options...)
	if err != nil || value == nil {
		return value, err
	}
	value.Summaries[""] = s.summary.Clone()
	return value, nil
}

type integrationSecret string

func (r integrationSecret) ResolveSecret(context.Context, tenant.Scope, tenant.SecretRef) (string, error) {
	return string(r), nil
}

func integrationTargetExecution(t *testing.T, key session.Key, schema string) worker.Execution {
	t.Helper()
	runtime := tenant.RuntimeContext{
		TenantID:           "integration",
		AppID:              "migration",
		ConfigVersion:      "v2",
		SessionPrincipalID: key.UserID,
		SessionID:          key.SessionID,
		UserID:             key.UserID,
		TraceID:            "migration-integration",
	}
	backend := tenant.BackendConfig{
		Name: "postgres-target",
		Session: tenant.BackendRef{
			Kind:      tenant.BackendSQL,
			Provider:  "postgres",
			Name:      "postgres-target",
			SecretRef: tenant.SecretRef{Name: "migration-postgres-dsn", Version: "1"},
			Options:   map[string]string{"schema": schema},
		},
	}
	return worker.Execution{
		Tenant: runtime,
		Config: tenant.AppConfig{
			TenantID:      runtime.TenantID,
			AppID:         runtime.AppID,
			Version:       runtime.ConfigVersion,
			BackendConfig: backend,
		},
	}
}

func openRedisPostgresIntegrationPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	config, err := pgxpool.ParseConfig(*redisPostgresTestDSN)
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
