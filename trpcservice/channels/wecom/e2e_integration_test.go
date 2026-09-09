package wecom

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
	channeltext "github.com/liuzengh/trpc-agent-service/trpcservice/channels/text"
	platformconfig "github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/security"
	"github.com/liuzengh/trpc-agent-service/trpcservice/sessionbackend"
	"github.com/liuzengh/trpc-agent-service/trpcservice/sessiondir"
	sessiondirpostgres "github.com/liuzengh/trpc-agent-service/trpcservice/sessiondir/postgres"
	"github.com/liuzengh/trpc-agent-service/trpcservice/sessionlease"
	"github.com/liuzengh/trpc-agent-service/trpcservice/sessionrun"
	"github.com/liuzengh/trpc-agent-service/trpcservice/telemetry"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	tenantpostgres "github.com/liuzengh/trpc-agent-service/trpcservice/tenant/postgres"
)

const (
	envIntegration = "TRPC_SERVICE_SESSION_INTEGRATION"
	envPostgresDSN = "TRPC_SERVICE_POSTGRES_DSN"
	// e2eTimeout bounds a wait that involves the database and the Runner, so it
	// is longer than testTimeout, which bounds one frame on a local socket.
	e2eTimeout = 30 * time.Second
)

func requireDSN(t *testing.T) string {
	t.Helper()
	if os.Getenv(envIntegration) != "1" {
		t.Skipf("set %s=1 to run the WeCom end to end tests", envIntegration)
	}
	dsn := os.Getenv(envPostgresDSN)
	if dsn == "" {
		t.Skipf("set %s to run the WeCom end to end tests", envPostgresDSN)
	}
	return dsn
}

func e2eContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), e2eTimeout)
	t.Cleanup(cancel)
	return ctx
}

// newStore opens an isolated schema of the local database and migrates it.
func newStore(t *testing.T, dsn string) (channels.Store, *pgxpool.Pool, string) {
	t.Helper()
	schema := "wecom_e2e_" + strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
	ctx := e2eContext(t)
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
	return store, pool, schema
}

// Reconstruct all Runtime and Session objects against the same persistent data.
func (e *e2e) rebuildRuns(t *testing.T) {
	t.Helper()
	if e.closeRuns != nil {
		e.closeRuns()
	}
	ctx := e2eContext(t)
	require.NoError(t, tenantpostgres.Migrate(ctx, e.pool))
	require.NoError(t, sessiondirpostgres.Migrate(ctx, e.pool))
	repository, err := tenantpostgres.New(e.pool)
	require.NoError(t, err)
	require.NoError(t, platformconfig.SeedDemo(context.Background(), repository))
	sessions, err := sessionbackend.New(sessionbackend.Config{
		Backend:  sessionbackend.BackendPostgres,
		Postgres: sessionbackend.PostgresConfig{DSN: e.dsn, Schema: e.schema},
	})
	require.NoError(t, err)
	closeSessions := sync.OnceFunc(func() { require.NoError(t, sessions.Close()) })
	t.Cleanup(closeSessions)
	resolver, err := platformagent.NewRuntimeResolver(repository,
		func(_ context.Context, revision tenant.AgentRevision) (*platformagent.Runtime, error) {
			return platformagent.NewRuntimeFromRevision(
				revision, sessions, security.DenyCapabilities())
		})
	require.NoError(t, err)
	closeResolver := sync.OnceFunc(func() { require.NoError(t, resolver.Close()) })
	t.Cleanup(closeResolver)
	coordinator, err := sessionlease.NewMemoryCoordinator(
		sessionlease.NewMemoryStore(), sessionlease.Config{})
	require.NoError(t, err)
	closeCoordinator := sync.OnceFunc(func() { require.NoError(t, coordinator.Close()) })
	t.Cleanup(closeCoordinator)
	directory, err := sessiondirpostgres.New(e.pool)
	require.NoError(t, err)
	runs, err := sessionrun.NewService(resolver, directory, coordinator)
	require.NoError(t, err)
	e.runs, e.leases, e.sessions, e.directory = runs, coordinator, sessions, directory
	e.closeRuns = func() { closeResolver(); closeCoordinator(); closeSessions() }
}

// e2e is one binding running against the mock platform and the real database.
type e2e struct {
	binding     Binding
	server      *mockServer
	store       channels.Store
	runs        *sessionrun.Service
	leases      sessionlease.Coordinator
	scope       tenant.TenantContext
	pool        *pgxpool.Pool
	dsn, schema string
	sessions    session.Service
	directory   sessiondir.Directory
	closeRuns   func()
	// observer is nil unless a test turns telemetry on for the consumer below.
	observer *telemetry.Telemetry

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func newE2E(t *testing.T) *e2e {
	t.Helper()
	// testBinding on the seeded demo tenant, which is the app the Runner can
	// resolve a revision for.
	binding := testBinding()
	binding.TenantID = platformconfig.DemoTenantID
	binding.AgentAppID = platformconfig.DemoAgentAppID
	dsn := requireDSN(t)
	store, pool, schema := newStore(t, dsn)
	fixture := &e2e{
		binding: binding,
		server:  newMockServer(t),
		store:   store,
		pool:    pool, dsn: dsn, schema: schema,
		scope: tenant.TenantContext{TenantID: binding.TenantID},
	}
	fixture.rebuildRuns(t)
	return fixture
}

func (e *e2e) userTurns(t *testing.T) int {
	t.Helper()
	sess, err := e.sessions.GetSession(e2eContext(t), session.Key{
		AppName: platformagent.DemoAppName, UserID: "u/" + e.sessionKey().PrincipalID,
		SessionID: e.sessionKey().SessionID,
	})
	require.NoError(t, err)
	count := 0
	for _, event := range sess.Events {
		if event.Response != nil && len(event.Choices) > 0 && event.Choices[0].Message.Role == model.RoleUser {
			count++
		}
	}
	return count
}

// identity is what the service configures this binding as, spelled out the way
// cmd spells it rather than read back from the adapter, so a consumer built
// here is held to the same identity check as one built by the process.
func (e *e2e) identity() channels.BindingIdentity {
	return channels.BindingIdentity{
		TenantID:   e.binding.TenantID,
		AgentAppID: e.binding.AgentAppID,
		BindingID:  e.binding.BindingID,
		Channel:    channels.ChannelWeCom,
	}
}

// consumerFor is the wiring under test: a WeCom adapter over one Client, driven
// by the common text consumer.
func (e *e2e) consumerFor(t *testing.T, client *Client, now func() time.Time) *channeltext.Consumer {
	t.Helper()
	adapter, err := NewAdapter(client)
	require.NoError(t, err)
	consumer, err := channeltext.New(channeltext.Config{
		Identity:  e.identity(),
		Adapter:   adapter,
		Store:     e.store,
		Runs:      e.runs,
		Revisions: func(context.Context, string, string, string) error { return nil },
		Telemetry: e.observer,
		Now:       now,
	})
	require.NoError(t, err)
	return consumer
}

// start runs one Client and one Consumer against a Store that may already hold
// this binding's rows, and returns the authenticated connection.
func (e *e2e) start(t *testing.T) *mockConn {
	t.Helper()
	// A previous Client may have reconnected once before it was stopped, and
	// that connection is accepted but dead. Dropping it here is what makes the
	// connection returned below the new Client's.
	for stale := true; stale; {
		select {
		case <-e.server.conns:
		default:
			stale = false
		}
	}
	client, err := New(testConfig(t, e.server, e.binding))
	require.NoError(t, err)
	consumer := e.consumerFor(t, client, nil)
	ctx, cancel := context.WithCancel(context.Background())
	e.cancel = cancel
	e.wg.Add(2)
	go func() { defer e.wg.Done(); _ = client.Run(ctx) }()
	go func() { defer e.wg.Done(); _ = consumer.Run(ctx) }()
	t.Cleanup(e.stop)
	return e.server.authenticate()
}

// stop cancels both loops and waits for them, so a test that goes on to read
// the database is not racing an execution still writing to it.
func (e *e2e) stop() {
	if e.cancel == nil {
		return
	}
	e.cancel()
	e.wg.Wait()
	e.cancel = nil
}

// sessionKey is the conversation the mock user's messages all belong to.
func (e *e2e) sessionKey() sessiondir.Key {
	principal := principalID(e.binding.TenantID, e.binding.BindingID, userIDMarker)
	return sessiondir.Key{
		TenantID:    e.binding.TenantID,
		AppID:       e.binding.AgentAppID,
		PrincipalID: principal,
		SessionID: directSessionID(
			e.binding.TenantID, e.binding.AgentAppID, e.binding.BindingID, principal),
	}
}

// recordedRuns returns every Run of that conversation, in accept order.
func (e *e2e) recordedRuns(t *testing.T) []channels.Run {
	t.Helper()
	runs, err := e.store.ListSessionRuns(
		e2eContext(t), e.scope, e.sessionKey(), channels.ListRequest{Limit: 10})
	require.NoError(t, err)
	return runs
}

func (e *e2e) answerOf(t *testing.T, run channels.Run) channels.OutboxPart {
	t.Helper()
	parts, err := e.store.ListRunOutbox(
		e2eContext(t), e.scope, run.RunID, channels.ListRequest{Limit: 10})
	require.NoError(t, err)
	require.Len(t, parts, 1, "one answer per Run")
	return parts[0]
}

// awaitAnswer waits until a Run's answer reaches a terminal status. Sending is
// recorded after the wire attempt returns, so a test that stopped the consumer
// the instant it saw a receipt would be cancelling the write it wants to read.
func (e *e2e) awaitAnswer(t *testing.T, run channels.Run) channels.OutboxPart {
	t.Helper()
	ctx := e2eContext(t)
	for {
		parts, err := e.store.ListRunOutbox(
			ctx, e.scope, run.RunID, channels.ListRequest{Limit: 10})
		require.NoError(t, err)
		if len(parts) == 1 && parts[0].Status.Terminal() {
			return parts[0]
		}
		select {
		case <-ctx.Done():
			t.Fatal("the answer did not settle")
		case <-time.After(20 * time.Millisecond):
		}
	}
}

// message is one inbound single-chat text with the ids a redelivery repeats.
func message(msgID, text string) map[string]any {
	body := textCallback()
	body["msgid"] = msgID
	body["text"] = map[string]any{"content": text}
	return body
}

// waitReply reads the next final stream frame and returns it with its text.
func waitReply(t *testing.T, conn *mockConn) (frame, string) {
	t.Helper()
	in := conn.next()
	require.Equal(t, cmdRespond, in.Cmd)
	var body respondBody
	decodeBody(t, in, &body)
	require.Equal(t, msgTypeStream, body.MsgType)
	require.True(t, body.Stream.Finish)
	return in, body.Stream.Content
}

// TestIntegrationDeliversOneAnswerPerAcceptedMessage is the acceptance path:
// real protocol frames, the real Runner and the real Store, with a redelivery
// of the first message and a second message in the same conversation.
func TestIntegrationDeliversOneAnswerPerAcceptedMessage(t *testing.T) {
	fixture := newE2E(t)
	conn := fixture.start(t)

	conn.callback("req-1", message("msg-1", "first question"))
	in, answer := waitReply(t, conn)
	require.Equal(t, "echo: first question", answer)
	conn.ack(in.Headers.ReqID, 0)

	// The same platform message id again, on a new req_id: the Store recognises
	// the external event, so nothing is executed and nothing is answered twice.
	conn.callback("req-2", message("msg-1", "first question"))
	conn.silent(500 * time.Millisecond)

	conn.callback("req-3", message("msg-2", "second question"))
	in, answer = waitReply(t, conn)
	require.Equal(t, "echo: second question", answer,
		"the conversation is answered in the order it was accepted")
	conn.ack(in.Headers.ReqID, 0)

	recorded := fixture.recordedRuns(t)
	require.Len(t, recorded, 2, "the redelivery created no third Run")
	for _, run := range recorded {
		require.Equal(t, channels.OutboxSent, fixture.awaitAnswer(t, run).Status)
	}
	fixture.stop()
	require.Equal(t, 2, fixture.userTurns(t), "duplicate input must not enter the Runner")
	for _, run := range fixture.recordedRuns(t) {
		require.Equal(t, channels.RunSucceeded, run.Status)
		require.Equal(t, int32(1), run.Attempt, "no Run was executed twice")
	}
}

// TestIntegrationRefusedReplyFailsWithoutRerunningTheAgent covers a send the
// platform refuses. It must not put the Run back on the queue: the model has
// already answered, and the answer is already recorded.
func TestIntegrationRefusedReplyFailsWithoutRerunningTheAgent(t *testing.T) {
	fixture := newE2E(t)
	conn := fixture.start(t)

	conn.callback("req-1", message("msg-1", "refused question"))
	in, _ := waitReply(t, conn)
	conn.ack(in.Headers.ReqID, 40001)

	// A refusal is permanent, so nothing is sent again and nothing is executed
	// again for as long as the loop keeps polling.
	conn.silent(800 * time.Millisecond)

	fixture.stop()
	recorded := fixture.recordedRuns(t)
	require.Len(t, recorded, 1)
	require.Equal(t, channels.RunSucceeded, recorded[0].Status)
	require.Equal(t, int32(1), recorded[0].Attempt)
	answer := fixture.answerOf(t, recorded[0])
	require.Equal(t, channels.OutboxFailed, answer.Status)
	require.False(t, answer.DuplicateRisk, "a refusal is a receipt, not an unknown")
}

// TestIntegrationRestartKeepsItsRecordAndItsBoundaries restarts the consumer
// over the same database: the accepted message is still deduplicated, and the
// answer of a connection that is gone fails rather than being replayed on the
// new one.
func TestIntegrationRestartKeepsItsRecordAndItsBoundaries(t *testing.T) {
	fixture := newE2E(t)
	conn := fixture.start(t)
	conn.callback("req-1", message("msg-1", "first question"))
	waitReply(t, conn)
	// The connection dies before the receipt, so the answer is recorded with an
	// unknown outcome and its target belongs to a generation that is over.
	conn.close()
	first := fixture.recordedRuns(t)
	require.Len(t, first, 1)
	stale := fixture.awaitAnswer(t, first[0])
	require.Equal(t, channels.OutboxFailed, stale.Status,
		"an answer addressed to a closed connection cannot be delivered later")
	require.True(t, stale.DuplicateRisk, "the first send ended without a receipt")
	fixture.stop()
	fixture.rebuildRuns(t)
	require.Equal(t, 1, fixture.userTurns(t), "history survives Runtime reconstruction")
	pin, exists, err := fixture.directory.GetPin(e2eContext(t), fixture.sessionKey())
	require.NoError(t, err)
	require.True(t, exists)
	require.Equal(t, first[0].RevisionID, pin)

	next := fixture.start(t)
	next.callback("req-2", message("msg-1", "first question"))
	next.callback("req-3", message("msg-2", "second question"))
	final, answer := waitReply(t, next)
	require.Equal(t, "echo: second question", answer,
		"the stale answer is not replayed on the new connection")
	next.ack(final.Headers.ReqID, 0)
	for _, run := range fixture.recordedRuns(t) {
		fixture.awaitAnswer(t, run)
	}

	fixture.stop()
	require.Equal(t, 2, fixture.userTurns(t), "replayed msgid adds no user turn after restart")
	recorded := fixture.recordedRuns(t)
	require.Len(t, recorded, 2, "the redelivery created no third Run")
	for _, run := range recorded {
		require.Equal(t, int32(1), run.Attempt, "no Run was executed twice")
	}
}

// TestIntegrationWaitsOutAWebLeaseWithoutSpendingAttempts is the shared-session
// case: the web entry is holding the run lease of the same conversation. The IM
// side must wait inside its execution budget and answer once the lease comes
// back, rather than yielding for each refusal and exhausting the Run.
func TestIntegrationWaitsOutAWebLeaseWithoutSpendingAttempts(t *testing.T) {
	fixture := newE2E(t)
	conn := fixture.start(t)

	lease, err := fixture.leases.Acquire(e2eContext(t), fixture.sessionKey())
	require.NoError(t, err)
	conn.callback("req-1", message("msg-1", "held question"))
	conn.silent(400 * time.Millisecond)
	require.NoError(t, lease.Release(e2eContext(t)))

	in, answer := waitReply(t, conn)
	require.Equal(t, "echo: held question", answer)
	conn.ack(in.Headers.ReqID, 0)

	recorded := fixture.recordedRuns(t)
	require.Len(t, recorded, 1)
	require.Equal(t, channels.OutboxSent, fixture.awaitAnswer(t, recorded[0]).Status)
	fixture.stop()
	recorded = fixture.recordedRuns(t)
	require.Equal(t, channels.RunSucceeded, recorded[0].Status)
	require.Equal(t, int32(1), recorded[0].Attempt,
		"waiting for the lease is not a failed attempt")
}

func TestIntegrationRecoveryExecutesOnlyNeverStartedWork(t *testing.T) {
	for _, state := range []string{"pending", "started"} {
		t.Run(state, func(t *testing.T) {
			fixture := newE2E(t)
			ctx := e2eContext(t)
			past := time.Now().Add(-10 * time.Minute)
			client, err := New(testConfig(t, fixture.server, fixture.binding))
			require.NoError(t, err)
			consumer := fixture.consumerFor(t, client, func() time.Time { return past })
			adapter, err := NewAdapter(client)
			require.NoError(t, err)
			key := fixture.sessionKey()
			// The work a previous process accepted, addressed to the connection
			// that process held.
			envelope, err := adapter.envelope(DirectText{
				PrincipalID: key.PrincipalID, SessionID: key.SessionID,
				ExternalMessageID: "recover-msg", Text: "recover question", ReceivedAt: past,
				Reply: ReplyTarget{generation: "previous-process", reqID: "old-req", streamID: "old-stream"},
			})
			require.NoError(t, err)
			require.NoError(t, consumer.Accept(ctx, envelope))
			recorded := fixture.recordedRuns(t)
			require.Len(t, recorded, 1)
			if state == "started" {
				claim, ok, err := fixture.store.ClaimNextRun(ctx, fixture.scope, key,
					channels.ClaimRunRequest{ClaimToken: "old-claim", ClaimedBy: "old-process", Now: past})
				require.NoError(t, err)
				require.True(t, ok)
				token := channels.RunToken{RunID: claim.Run.RunID, ClaimToken: claim.Run.ClaimToken}
				require.NoError(t, fixture.store.RecordRunRevision(ctx, fixture.scope, token, platformconfig.DemoRevisionID, past))
				require.NoError(t, fixture.store.MarkRunStarted(ctx, fixture.scope, token, past))
			}
			fixture.rebuildRuns(t)
			conn := fixture.start(t)
			var settled channels.Run
			require.Eventually(t, func() bool {
				settled, err = fixture.store.GetRun(ctx, fixture.scope, recorded[0].RunID)
				return err == nil && settled.Status.Terminal()
			}, 5*time.Second, 20*time.Millisecond)
			if state == "started" {
				require.Equal(t, channels.RunFailed, settled.Status)
				_, pinned, err := fixture.directory.GetPin(ctx, key)
				require.NoError(t, err)
				require.False(t, pinned, "an ambiguous started Run must not enter Session Run again")
			} else {
				require.Equal(t, channels.RunSucceeded, settled.Status)
				part := fixture.awaitAnswer(t, settled)
				require.Equal(t, channels.OutboxFailed, part.Status, "old connection target expires")
				require.False(t, part.DuplicateRisk, "no frame was sent on the new connection")
				require.Equal(t, 1, fixture.userTurns(t))
			}
			conn.silent(100 * time.Millisecond)
			fixture.stop()
		})
	}
}
