package feishu

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	platformagent "github.com/liuzengh/trpc-agent-service/trpcservice/agent"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	channelspostgres "github.com/liuzengh/trpc-agent-service/trpcservice/channels/postgres"
	channeltext "github.com/liuzengh/trpc-agent-service/trpcservice/channels/text"
	platformconfig "github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/security"
	"github.com/liuzengh/trpc-agent-service/trpcservice/sessionbackend"
	"github.com/liuzengh/trpc-agent-service/trpcservice/sessiondir"
	sessiondirpostgres "github.com/liuzengh/trpc-agent-service/trpcservice/sessiondir/postgres"
	"github.com/liuzengh/trpc-agent-service/trpcservice/sessionlease"
	"github.com/liuzengh/trpc-agent-service/trpcservice/sessionrun"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	tenantpostgres "github.com/liuzengh/trpc-agent-service/trpcservice/tenant/postgres"
)

const (
	envIntegration     = "TRPC_SERVICE_SESSION_INTEGRATION"
	envPostgresDSN     = "TRPC_SERVICE_POSTGRES_DSN"
	integrationTimeout = 30 * time.Second
	settleFor          = 400 * time.Millisecond
)

// TestIntegrationOneFeishuEventBecomesOneRunAndOneReply joins this adapter to
// the pipeline it was written for: the real SDK on a local socket at one end,
// the real Store, Runner and session on PostgreSQL at the other, and nothing
// stubbed in between. The duplicate is the point — the adapter has no state
// that could recognise it, so recognising it is the boundary's work.
func TestIntegrationOneFeishuEventBecomesOneRunAndOneReply(t *testing.T) {
	if os.Getenv(envIntegration) != "1" {
		t.Skipf("set %s=1 to run the feishu end to end test", envIntegration)
	}
	dsn := os.Getenv(envPostgresDSN)
	if dsn == "" {
		t.Skipf("set %s to run the feishu end to end test", envPostgresDSN)
	}
	ctx, cancel := context.WithTimeout(context.Background(), integrationTimeout)
	defer cancel()
	pool, schema := integrationSchema(t, ctx, dsn)
	require.NoError(t, channelspostgres.Migrate(ctx, pool))
	store, err := channelspostgres.New(pool)
	require.NoError(t, err)
	runs := integrationRuns(t, ctx, pool, dsn, schema)

	// The bot is bound to the seeded demo app, which is the app the Runner can
	// resolve a revision for.
	p := newPlatform(t)
	socket := serveConnection(t, p)
	binding := testBinding()
	binding.TenantID = platformconfig.DemoTenantID
	binding.AgentAppID = platformconfig.DemoAgentAppID
	config := testConfig(p, &grantingAuthorizer{tenantID: binding.TenantID, ref: testSecretRef})
	config.Binding = binding
	client, err := New(ctx, config)
	require.NoError(t, err)
	adapter, err := NewAdapter(client, nil)
	require.NoError(t, err)
	consumer, err := channeltext.New(channeltext.Config{
		Identity:  adapter.Identity(),
		Adapter:   adapter,
		Store:     store,
		Runs:      runs,
		Revisions: func(context.Context, string, string, string) error { return nil },
	})
	require.NoError(t, err)
	runCtx, stopConsumer := context.WithCancel(context.Background())
	var running sync.WaitGroup
	running.Add(1)
	go func() { defer running.Done(); _ = consumer.Run(runCtx) }()
	t.Cleanup(func() { stopConsumer(); running.Wait() })

	// The same message twice, as two websocket deliveries. Both are
	// acknowledged, because both are durably settled.
	payload := event(nil)
	socket.deliver(t, "ws-1", payload)
	require.Equal(t, http.StatusOK, socket.waitAck(t).code)
	socket.deliver(t, "ws-2", payload)
	require.Equal(t, http.StatusOK, socket.waitAck(t).code)

	scope := tenant.TenantContext{TenantID: binding.TenantID}
	principal := principalID(binding, testTenantKey, testOpenID)
	key := sessiondir.Key{
		TenantID:    binding.TenantID,
		AppID:       binding.AgentAppID,
		PrincipalID: principal,
		SessionID:   directSessionID(binding, testTenantKey, principal, testChatID),
	}
	var recorded []channels.Run
	var answer channels.OutboxPart
	require.Eventually(t, func() bool {
		recorded, err = store.ListSessionRuns(ctx, scope, key, channels.ListRequest{Limit: 10})
		if err != nil || len(recorded) != 1 {
			return false
		}
		parts, err := store.ListRunOutbox(ctx, scope, recorded[0].RunID, channels.ListRequest{Limit: 10})
		if err != nil || len(parts) != 1 || !parts[0].Status.Terminal() {
			return false
		}
		answer = parts[0]
		return true
	}, integrationTimeout, 20*time.Millisecond, "the answer never settled")
	// Nothing follows the settled answer: no second execution, no second send.
	require.Never(t, func() bool { return len(p.seenPath(replyPathPrefix)) > 1 },
		settleFor, 20*time.Millisecond)

	stopConsumer()
	running.Wait()
	require.Len(t, recorded, 1, "a redelivery is not a second Run")
	require.Equal(t, channels.RunSucceeded, recorded[0].Status)
	require.Equal(t, int32(1), recorded[0].Attempt, "no Run was executed twice")
	require.Equal(t, channels.OutboxSent, answer.Status)
	require.False(t, answer.DuplicateRisk)

	// The one reply the platform saw is the one this Run recorded.
	replies := p.seenPath(replyPathPrefix)
	require.Len(t, replies, 1)
	require.Equal(t, replyPathPrefix+testMessageID+"/reply", replies[0].path)
	var sent replyRequest
	require.NoError(t, json.Unmarshal(replies[0].body, &sent))
	require.Equal(t, replyUUID(binding, testTenantKey, testMessageID), sent.UUID)
	require.Equal(t, int32(0), answer.PartNo, "one answer, one part")
	require.Contains(t, sent.Content, "echo: how do I reset my password?")
}

// integrationSchema gives the test a schema of its own, dropped afterwards.
func integrationSchema(t *testing.T, ctx context.Context, dsn string) (*pgxpool.Pool, string) {
	t.Helper()
	schema := "feishu_e2e_" + strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
	admin, err := pgxpool.New(ctx, dsn)
	require.NoError(t, err)
	t.Cleanup(admin.Close)
	_, err = admin.Exec(ctx, `CREATE SCHEMA "`+schema+`"`)
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), integrationTimeout)
		defer cancel()
		_, err := admin.Exec(cleanupCtx, `DROP SCHEMA "`+schema+`" CASCADE`)
		require.NoError(t, err)
	})
	config, err := pgxpool.ParseConfig(dsn)
	require.NoError(t, err)
	config.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, config)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	return pool, schema
}

// integrationRuns is the real Session Run service over the same database.
func integrationRuns(
	t *testing.T, ctx context.Context, pool *pgxpool.Pool, dsn, schema string,
) *sessionrun.Service {
	t.Helper()
	require.NoError(t, tenantpostgres.Migrate(ctx, pool))
	require.NoError(t, sessiondirpostgres.Migrate(ctx, pool))
	repository, err := tenantpostgres.New(pool)
	require.NoError(t, err)
	require.NoError(t, platformconfig.SeedDemo(context.Background(), repository))
	sessions, err := sessionbackend.New(sessionbackend.Config{
		Backend:  sessionbackend.BackendPostgres,
		Postgres: sessionbackend.PostgresConfig{DSN: dsn, Schema: schema},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, sessions.Close()) })
	resolver, err := platformagent.NewRuntimeResolver(repository,
		func(_ context.Context, revision tenant.AgentRevision) (*platformagent.Runtime, error) {
			return platformagent.NewRuntimeFromRevision(
				revision, sessions, security.DenyCapabilities())
		})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, resolver.Close()) })
	coordinator, err := sessionlease.NewMemoryCoordinator(
		sessionlease.NewMemoryStore(), sessionlease.Config{})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, coordinator.Close()) })
	directory, err := sessiondirpostgres.New(pool)
	require.NoError(t, err)
	runs, err := sessionrun.NewService(resolver, directory, coordinator)
	require.NoError(t, err)
	return runs
}
