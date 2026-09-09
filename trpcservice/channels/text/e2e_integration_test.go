package text

import (
	"context"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/session"

	platformagent "github.com/liuzengh/trpc-agent-service/trpcservice/agent"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	channelspostgres "github.com/liuzengh/trpc-agent-service/trpcservice/channels/postgres"
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
	envIntegration = "TRPC_SERVICE_SESSION_INTEGRATION"
	envPostgresDSN = "TRPC_SERVICE_POSTGRES_DSN"
	// e2eTimeout bounds a wait that involves the database and the Runner.
	e2eTimeout = 30 * time.Second
	// settleFor is how long a test watches for work that must not happen.
	settleFor = 400 * time.Millisecond
)

// The tests below run the consumer over a channel this repository does not
// implement. The durable half is entirely real — the PostgreSQL Store, the real
// Runner, the real session — and the platform half is the fake adapter of
// consumer_test.go. What passes here is what an adapter outside this repository
// gets by implementing four methods.

func requireDSN(t *testing.T) string {
	t.Helper()
	if os.Getenv(envIntegration) != "1" {
		t.Skipf("set %s=1 to run the consumer end to end tests", envIntegration)
	}
	dsn := os.Getenv(envPostgresDSN)
	if dsn == "" {
		t.Skipf("set %s to run the consumer end to end tests", envPostgresDSN)
	}
	return dsn
}

func e2eContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), e2eTimeout)
	t.Cleanup(cancel)
	return ctx
}

// fakeE2E is one binding of the fake channel, on the real pipeline.
type fakeE2E struct {
	identity channels.BindingIdentity
	adapter  *fakeAdapter
	store    channels.Store
	runs     *sessionrun.Service
	sessions session.Service
	scope    tenant.TenantContext
	// durable carries the number of Runs the Store held at the moment accept
	// returned, which is the promise an adapter acknowledges its platform on.
	durable chan int

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func newFakeE2E(t *testing.T) *fakeE2E {
	t.Helper()
	dsn := requireDSN(t)
	ctx := e2eContext(t)
	schema := "text_e2e_" + strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
	admin, err := pgxpool.New(ctx, dsn)
	require.NoError(t, err)
	t.Cleanup(admin.Close)
	_, err = admin.Exec(ctx, `CREATE SCHEMA "`+schema+`"`)
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), e2eTimeout)
		defer cancel()
		if _, err := admin.Exec(cleanupCtx, `DROP SCHEMA "`+schema+`" CASCADE`); err != nil {
			t.Errorf("drop schema: %v", err)
		}
	})
	config, err := pgxpool.ParseConfig(dsn)
	require.NoError(t, err)
	config.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, config)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	require.NoError(t, channelspostgres.Migrate(ctx, pool))
	store, err := channelspostgres.New(pool)
	require.NoError(t, err)

	// The seeded demo app, which is the app the Runner can resolve a revision
	// for, reached over a channel it was never told about.
	adapter := newFakeAdapter()
	adapter.identity = channels.BindingIdentity{
		TenantID:   platformconfig.DemoTenantID,
		AgentAppID: platformconfig.DemoAgentAppID,
		BindingID:  "binding-a",
		Channel:    fakeChannel,
	}
	fixture := &fakeE2E{
		identity: adapter.identity,
		adapter:  adapter,
		store:    store,
		scope:    tenant.TenantContext{TenantID: adapter.identity.TenantID},
		durable:  make(chan int, 4),
	}
	fixture.runs = newRunService(t, ctx, pool, dsn, schema, &fixture.sessions)
	adapter.afterAccept = func(channels.InboundEnvelope) {
		fixture.durable <- fixture.runCount()
	}
	return fixture
}

// newRunService builds the real Session Run service over the same database.
func newRunService(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	dsn, schema string,
	out *session.Service,
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
	*out = sessions
	return runs
}

// start runs the consumer, which runs the adapter.
func (e *fakeE2E) start(t *testing.T) {
	t.Helper()
	consumer, err := New(Config{
		Identity:  e.identity,
		Adapter:   e.adapter,
		Store:     e.store,
		Runs:      e.runs,
		Revisions: func(context.Context, string, string, string) error { return nil },
	})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	e.cancel = cancel
	e.wg.Add(1)
	go func() { defer e.wg.Done(); _ = consumer.Run(ctx) }()
	t.Cleanup(e.stop)
}

// stop cancels the consumer and waits for it, so a test that goes on to read
// the database is not racing an execution still writing to it.
func (e *fakeE2E) stop() {
	if e.cancel == nil {
		return
	}
	e.cancel()
	e.wg.Wait()
	e.cancel = nil
}

// deliver hands the adapter one inbound message, as its platform would.
func (e *fakeE2E) deliver(externalID, text string) {
	e.adapter.inbound <- e.adapter.envelope(externalID, fakeSessionID, text)
}

const fakeSessionID = "d-fake-session"

func (e *fakeE2E) sessionKey() sessiondir.Key {
	return sessiondir.Key{
		TenantID:    e.identity.TenantID,
		AppID:       e.identity.AgentAppID,
		PrincipalID: fakePrincipal,
		SessionID:   fakeSessionID,
	}
}

// runCount is what the Store holds right now, read without a testing.T because
// the adapter calls it from the serve loop.
func (e *fakeE2E) runCount() int {
	ctx, cancel := context.WithTimeout(context.Background(), e2eTimeout)
	defer cancel()
	runs, err := e.store.ListSessionRuns(
		ctx, e.scope, e.sessionKey(), channels.ListRequest{Limit: 10})
	if err != nil {
		return -1
	}
	return len(runs)
}

func (e *fakeE2E) recordedRuns(t *testing.T) []channels.Run {
	t.Helper()
	runs, err := e.store.ListSessionRuns(
		e2eContext(t), e.scope, e.sessionKey(), channels.ListRequest{Limit: 10})
	require.NoError(t, err)
	return runs
}

// answerOf waits until a Run's answer reaches a terminal status. Sending is
// recorded after the attempt returns, so a test that read the row the instant
// it saw the message would be racing that write.
func (e *fakeE2E) answerOf(t *testing.T, run channels.Run) channels.OutboxPart {
	t.Helper()
	ctx := e2eContext(t)
	for {
		parts, err := e.store.ListRunOutbox(ctx, e.scope, run.RunID, channels.ListRequest{Limit: 10})
		require.NoError(t, err)
		require.Len(t, parts, 1, "one answer per Run")
		if parts[0].Status.Terminal() {
			return parts[0]
		}
		select {
		case <-ctx.Done():
			t.Fatal("the answer did not settle")
		case <-time.After(20 * time.Millisecond):
		}
	}
}

// awaitSends waits for the adapter to have been asked to send count messages.
func (e *fakeE2E) awaitSends(t *testing.T, count int) []channels.OutboundMessage {
	t.Helper()
	var sent []channels.OutboundMessage
	require.Eventually(t, func() bool {
		sent = e.adapter.delivered()
		return len(sent) >= count
	}, e2eTimeout, 20*time.Millisecond)
	return sent
}

// userTurns is how many user messages the conversation actually holds.
func (e *fakeE2E) userTurns(t *testing.T) int {
	t.Helper()
	key := e.sessionKey()
	sess, err := e.sessions.GetSession(e2eContext(t), session.Key{
		AppName: platformagent.DemoAppName, UserID: "u/" + key.PrincipalID, SessionID: key.SessionID,
	})
	require.NoError(t, err)
	count := 0
	for _, recorded := range sess.Events {
		if recorded.Response != nil && len(recorded.Choices) > 0 &&
			recorded.Choices[0].Message.Role == model.RoleUser {
			count++
		}
	}
	return count
}

// TestIntegrationAnyTextAdapterGetsTheSamePipeline is the extraction's own
// acceptance: a channel with no code in this repository is recorded, executed
// once and answered once, and a redelivery of the same event changes nothing.
func TestIntegrationAnyTextAdapterGetsTheSamePipeline(t *testing.T) {
	fixture := newFakeE2E(t)
	fixture.start(t)

	fixture.deliver("event-1", "fake question")
	require.Equal(t, 1, <-fixture.durable,
		"accept returns only once the event is durable, which is what an adapter acknowledges on")
	sent := fixture.awaitSends(t, 1)
	require.Equal(t, "echo: fake question", sent[0].Text)

	// The same external event id again: the Store recognises it, so nothing is
	// executed and nothing is answered a second time.
	fixture.deliver("event-1", "fake question")
	require.Equal(t, 1, <-fixture.durable, "a redelivery is not a second Run")
	require.Never(t, func() bool { return len(fixture.adapter.delivered()) > 1 },
		settleFor, 20*time.Millisecond)

	fixture.stop()
	recorded := fixture.recordedRuns(t)
	require.Len(t, recorded, 1)
	require.Equal(t, channels.RunSucceeded, recorded[0].Status)
	require.Equal(t, int32(1), recorded[0].Attempt, "no Run was executed twice")
	require.Equal(t, channels.OutboxSent, fixture.answerOf(t, recorded[0]).Status)
	require.Equal(t, 1, fixture.userTurns(t), "duplicate input must not enter the Runner")
}

// TestIntegrationARefusedDeliveryDoesNotReplayTheAgent is the other half of the
// contract an adapter inherits: reporting a refusal ends the answer, and never
// costs the user a second execution of the agent that already answered.
func TestIntegrationARefusedDeliveryDoesNotReplayTheAgent(t *testing.T) {
	fixture := newFakeE2E(t)
	fixture.adapter.report = channels.DeliveryReport{Outcome: channels.DeliveryRejected}
	fixture.start(t)

	fixture.deliver("event-1", "refused question")
	require.Equal(t, 1, <-fixture.durable)
	require.Equal(t, "echo: refused question", fixture.awaitSends(t, 1)[0].Text)
	// A refusal is settled, so nothing is sent again and nothing is executed
	// again for as long as the loops keep polling.
	require.Never(t, func() bool { return len(fixture.adapter.delivered()) > 1 },
		settleFor, 20*time.Millisecond)

	fixture.stop()
	recorded := fixture.recordedRuns(t)
	require.Len(t, recorded, 1)
	require.Equal(t, channels.RunSucceeded, recorded[0].Status)
	require.Equal(t, int32(1), recorded[0].Attempt)
	answer := fixture.answerOf(t, recorded[0])
	require.Equal(t, channels.OutboxFailed, answer.Status)
	require.False(t, answer.DuplicateRisk, "a refusal is a receipt, not an unknown")
	require.Equal(t, 1, fixture.userTurns(t))
}
