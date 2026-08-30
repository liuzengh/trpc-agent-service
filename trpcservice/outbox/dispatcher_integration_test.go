package outbox

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/liuzengh/trpc-agent-service/internal/testinfra"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	larkchannel "github.com/liuzengh/trpc-agent-service/trpcservice/channels/lark"
	telegramchannel "github.com/liuzengh/trpc-agent-service/trpcservice/channels/telegram"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	postgresstore "github.com/liuzengh/trpc-agent-service/trpcservice/storage/postgres"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

type postgresDispatcherFixture struct {
	base   *pgxpool.Pool
	pool   *pgxpool.Pool
	repo   *postgresstore.OutboxRepository
	ctx    context.Context
	cancel context.CancelFunc
	tc     tenant.TenantContext
	schema string
}

func newPostgresDispatcherFixture(t *testing.T, lockDuration time.Duration) *postgresDispatcherFixture {
	t.Helper()
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL is not set; PostgreSQL Dispatcher not verified")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	cfg := postgresstore.PostgresConfig{URL: url, MaxConns: 8, MinConns: 1, AllowDestructiveDown: true}
	base, err := postgresstore.NewPool(ctx, cfg)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	schema := fmt.Sprintf("p009e_outbox_%d", time.Now().UnixNano())
	if _, err := base.Exec(ctx, "CREATE SCHEMA "+quoteSchemaForTest(schema)); err != nil {
		base.Close()
		cancel()
		t.Fatal(err)
	}
	cfg.SearchPath = schema
	pool, err := postgresstore.NewPool(ctx, cfg)
	if err != nil {
		_, _ = base.Exec(context.Background(), "DROP SCHEMA "+quoteSchemaForTest(schema)+" CASCADE")
		base.Close()
		cancel()
		t.Fatal(err)
	}
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		pool.Close()
		_, _ = base.Exec(context.Background(), "DROP SCHEMA "+quoteSchemaForTest(schema)+" CASCADE")
		base.Close()
		cancel()
		t.Fatal("cannot locate repository root")
	}
	migrator, err := postgresstore.NewMigratorWithPool(pool, cfg, os.DirFS(filepath.Join(filepath.Dir(file), "../../migrations")))
	if err != nil {
		pool.Close()
		_, _ = base.Exec(context.Background(), "DROP SCHEMA "+quoteSchemaForTest(schema)+" CASCADE")
		base.Close()
		cancel()
		t.Fatal(err)
	}
	if err := migrator.Up(ctx); err != nil {
		pool.Close()
		_, _ = base.Exec(context.Background(), "DROP SCHEMA "+quoteSchemaForTest(schema)+" CASCADE")
		base.Close()
		cancel()
		t.Fatal(err)
	}
	repo, err := postgresstore.NewOutboxRepository(pool, postgresstore.OutboxRepositoryConfig{LockDuration: lockDuration, MaxBatchSize: 100})
	if err != nil {
		pool.Close()
		_, _ = base.Exec(context.Background(), "DROP SCHEMA "+quoteSchemaForTest(schema)+" CASCADE")
		base.Close()
		cancel()
		t.Fatal(err)
	}
	fixture := &postgresDispatcherFixture{base: base, pool: pool, repo: repo, ctx: ctx, cancel: cancel, schema: schema, tc: testTenant("p009e-tenant")}
	t.Cleanup(func() {
		pool.Close()
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		_, _ = base.Exec(cleanupCtx, "DROP SCHEMA "+quoteSchemaForTest(schema)+" CASCADE")
		base.Close()
		cancel()
	})
	return fixture
}

func quoteSchemaForTest(schema string) string {
	return `"` + schema + `"`
}

func (f *postgresDispatcherFixture) enqueue(t *testing.T, id string) {
	t.Helper()
	if err := f.repo.Enqueue(f.ctx, f.tc, storage.OutboxMessage{
		TenantID: f.tc.TenantID, ID: id, Kind: "reply", AggregateID: "aggregate-" + id,
		DedupKey: "dedup-" + id, Payload: []byte(`{"message":"hello"}`),
	}); err != nil {
		t.Fatal(err)
	}
}

func (f *postgresDispatcherFixture) repoForAnotherPool(t *testing.T) *postgresstore.OutboxRepository {
	t.Helper()
	pool, err := postgresstore.NewPool(f.ctx, postgresstore.PostgresConfig{URL: os.Getenv("TEST_DATABASE_URL"), SearchPath: f.schema, MaxConns: 4, MinConns: 1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	repo, err := postgresstore.NewOutboxRepository(pool, postgresstore.OutboxRepositoryConfig{LockDuration: 2 * time.Second, MaxBatchSize: 100})
	if err != nil {
		t.Fatal(err)
	}
	return repo
}

func waitForPostgresStatus(t *testing.T, fixture *postgresDispatcherFixture, id string, status storage.OutboxStatus) storage.OutboxMessage {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		value, err := fixture.repo.Get(fixture.ctx, fixture.tc, id)
		if err == nil && value.Status == status {
			return value
		}
		if time.Now().After(deadline) {
			t.Fatalf("outbox %s did not reach %s: value=%+v err=%v", id, status, value, err)
		}
		runtime.Gosched()
	}
}

func runPostgresDispatcher(t *testing.T, fixture *postgresDispatcherFixture, owner string, sender Sender) (*Dispatcher, <-chan error) {
	t.Helper()
	config := testConfig(fixture.tc, owner)
	config.ClaimInterval = 5 * time.Millisecond
	config.ShutdownTimeout = 3 * time.Second
	config.LockGuard = 50 * time.Millisecond
	dispatcher, err := NewDispatcher(fixture.repo, sender, config)
	if err != nil {
		t.Fatal(err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- dispatcher.Run(fixture.ctx) }()
	return dispatcher, runDone
}

func stopPostgresDispatcher(t *testing.T, dispatcher *Dispatcher, runDone <-chan error) {
	t.Helper()
	if err := dispatcher.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := <-runDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run error=%v", err)
	}
}

func TestPostgresDispatcherClaimsCommittedChannelReply(t *testing.T) {
	fixture := newPostgresDispatcherFixture(t, 2*time.Second)
	message := channelOutboxMessage(t, fixture.tc.TenantID)
	if err := fixture.repo.Enqueue(fixture.ctx, fixture.tc, message); err != nil {
		t.Fatal(err)
	}
	doer := &sequenceHTTPDoer{responses: []sequenceHTTPResponse{{status: http.StatusOK, body: `{"accepted":true}`}}}
	httpSender, err := channels.NewHTTPSender(channels.HTTPSenderConfig{
		Registry: channels.DefaultRegistry(), Client: doer,
		Endpoints: map[string]string{"web": "https://web.invalid/base"},
	})
	if err != nil {
		t.Fatal(err)
	}
	sender, err := NewChannelSender(httpSender)
	if err != nil {
		t.Fatal(err)
	}
	dispatcher, runDone := runPostgresDispatcher(t, fixture, "dispatcher-channel-pg", sender)
	waitForPostgresStatus(t, fixture, message.ID, storage.OutboxCompleted)
	stopPostgresDispatcher(t, dispatcher, runDone)
	doer.mu.Lock()
	keys := append([]string(nil), doer.keys...)
	doer.mu.Unlock()
	if len(keys) != 1 {
		t.Fatalf("channel-aware PostgreSQL claim sent %d times", len(keys))
	}
}

func TestPostgresDispatcherClaimsCommittedTelegramReply(t *testing.T) {
	fixture := newPostgresDispatcherFixture(t, 2*time.Second)
	payload := channels.ReplyOutboxPayload{
		SchemaVersion: channels.ReplyOutboxSchemaVersion, Kind: channels.ReplyOutboxKind,
		TenantID: fixture.tc.TenantID, SessionID: "session-telegram-pg", JobID: "job-telegram-pg", ExecutionID: "execution-telegram-pg",
		RequestID: "request-telegram-pg", MessageID: "message-telegram-pg", TraceID: "trace-telegram-pg", BindingID: fixture.tc.BindingID,
		Channel: telegramchannel.Channel, DestinationType: channels.DestinationTypeChat, DestinationID: "-100123456789",
		ReplyText: "reply text", SenderRoutingVersion: channels.SenderRoutingVersion,
	}
	encoded, err := channels.EncodeReplyOutboxPayload(payload)
	if err != nil {
		t.Fatal(err)
	}
	message := storage.OutboxMessage{
		TenantID: fixture.tc.TenantID, ID: "reply-" + payload.ExecutionID, Kind: channels.ReplyOutboxKind,
		AggregateID: payload.ExecutionID, DedupKey: fixture.tc.TenantID + "|" + payload.ExecutionID + "|" + channels.ReplyOutboxKind, Payload: encoded,
	}
	if err := fixture.repo.Enqueue(fixture.ctx, fixture.tc, message); err != nil {
		t.Fatal(err)
	}
	doer := &sequenceHTTPDoer{responses: []sequenceHTTPResponse{{status: http.StatusOK, body: `{"ok":true,"result":{"message_id":9}}`}}}
	telegramSenderValue := telegramSender(t, doer, fixture.tc.TenantID, fixture.tc.BindingID)
	sender, err := NewChannelSender(telegramSenderValue)
	if err != nil {
		t.Fatal(err)
	}
	dispatcher, runDone := runPostgresDispatcher(t, fixture, "dispatcher-telegram-pg", sender)
	waitForPostgresStatus(t, fixture, message.ID, storage.OutboxCompleted)
	stopPostgresDispatcher(t, dispatcher, runDone)
	doer.mu.Lock()
	calls := len(doer.keys)
	doer.mu.Unlock()
	if calls != 1 {
		t.Fatalf("Telegram PostgreSQL claim sent %d times", calls)
	}
}

func TestPostgresDispatcherClaimsCommittedLarkReply(t *testing.T) {
	fixture := newPostgresDispatcherFixture(t, 2*time.Second)
	payload := channels.ReplyOutboxPayload{
		SchemaVersion: channels.ReplyOutboxSchemaVersion, Kind: channels.ReplyOutboxKind,
		TenantID: fixture.tc.TenantID, SessionID: "session-lark-pg", JobID: "job-lark-pg", ExecutionID: "execution-lark-pg",
		RequestID: "request-lark-pg", MessageID: "message-lark-pg", TraceID: "trace-lark-pg", BindingID: fixture.tc.BindingID,
		Channel: larkchannel.Channel, DestinationType: channels.DestinationTypeUser, DestinationID: "ou_lark_pg",
		ReplyText: "reply text", SenderRoutingVersion: channels.SenderRoutingVersion,
	}
	encoded, err := channels.EncodeReplyOutboxPayload(payload)
	if err != nil {
		t.Fatal(err)
	}
	message := storage.OutboxMessage{
		TenantID: fixture.tc.TenantID, ID: "reply-" + payload.ExecutionID, Kind: channels.ReplyOutboxKind,
		AggregateID: payload.ExecutionID, DedupKey: fixture.tc.TenantID + "|" + payload.ExecutionID + "|" + channels.ReplyOutboxKind, Payload: encoded,
	}
	if err := fixture.repo.Enqueue(fixture.ctx, fixture.tc, message); err != nil {
		t.Fatal(err)
	}
	doer := &sequenceHTTPDoer{responses: []sequenceHTTPResponse{{status: http.StatusOK, body: `{"code":0,"data":{"message_id":"om_lark_pg"}}`}}}
	larkSender, err := larkchannel.NewSender(larkchannel.SenderConfig{
		Bindings: []larkchannel.Binding{{TenantID: fixture.tc.TenantID, BindingID: fixture.tc.BindingID, Channel: larkchannel.Channel, AppID: "cli_test", SecretRef: "secret-ref", ReceiverIDType: larkchannel.ReceiverIDTypeOpenID, Enabled: true}},
		Tokens:   larkchannel.TokenResolverFunc(func(context.Context, larkchannel.Binding) (string, error) { return "tenant-token", nil }),
		Client:   doer,
	})
	if err != nil {
		t.Fatal(err)
	}
	sender, err := NewChannelSender(larkSender)
	if err != nil {
		t.Fatal(err)
	}
	dispatcher, runDone := runPostgresDispatcher(t, fixture, "dispatcher-lark-pg", sender)
	waitForPostgresStatus(t, fixture, message.ID, storage.OutboxCompleted)
	stopPostgresDispatcher(t, dispatcher, runDone)
	doer.mu.Lock()
	calls := len(doer.keys)
	doer.mu.Unlock()
	if calls != 1 {
		t.Fatalf("Lark PostgreSQL claim sent %d times", calls)
	}
}

func TestPostgresDispatcherSuccessCompletesDurableMessage(t *testing.T) {
	fixture := newPostgresDispatcherFixture(t, 2*time.Second)
	fixture.enqueue(t, "outbox-dispatch-success")
	called := make(chan struct{}, 1)
	sender := SenderFunc(func(context.Context, storage.OutboxMessage) SenderOutcome {
		called <- struct{}{}
		return SenderOutcome{Class: Delivered}
	})
	dispatcher, runDone := runPostgresDispatcher(t, fixture, "dispatcher-success", sender)
	select {
	case <-called:
	case <-time.After(10 * time.Second):
		t.Fatal("PostgreSQL Dispatcher did not call Sender")
	}
	stored := waitForPostgresStatus(t, fixture, "outbox-dispatch-success", storage.OutboxCompleted)
	if stored.LockedBy != "" || stored.LastError != "" {
		t.Fatalf("completed message retained processing fields: %+v", stored)
	}
	if dispatcher.Stats().Completed != 1 {
		t.Fatalf("dispatcher stats=%+v", dispatcher.Stats())
	}
	stopPostgresDispatcher(t, dispatcher, runDone)
}

func TestPostgresDispatcherRetryScheduleAndDueRetry(t *testing.T) {
	fixture := newPostgresDispatcherFixture(t, 2*time.Second)
	fixture.enqueue(t, "outbox-dispatch-retry")
	called := make(chan struct{}, 2)
	firstSender := SenderFunc(func(context.Context, storage.OutboxMessage) SenderOutcome {
		called <- struct{}{}
		return SenderOutcome{Class: RetryableFailure, Code: SenderRateLimitedCode}
	})
	config := testConfig(fixture.tc, "dispatcher-retry-first")
	config.ClaimInterval = 5 * time.Millisecond
	config.ShutdownTimeout = 3 * time.Second
	config.LockGuard = 50 * time.Millisecond
	dispatcher, err := NewDispatcher(fixture.repo, firstSender, config)
	if err != nil {
		t.Fatal(err)
	}
	firstRun := make(chan error, 1)
	go func() { firstRun <- dispatcher.Run(fixture.ctx) }()
	select {
	case <-called:
	case <-time.After(10 * time.Second):
		t.Fatal("retry Sender was not called")
	}
	stored := waitForPostgresStatus(t, fixture, "outbox-dispatch-retry", storage.OutboxRetry)
	if stored.LastError != SenderRateLimitedCode || !stored.NextAttempt.After(time.Now().UTC()) || stored.Attempt != 2 {
		t.Fatalf("retry state=%+v", stored)
	}
	stopPostgresDispatcher(t, dispatcher, firstRun)

	if _, err := fixture.pool.Exec(fixture.ctx, `UPDATE outbox_message SET next_attempt_at=clock_timestamp()-interval '1 second' WHERE tenant_id=$1 AND outbox_id=$2`, fixture.tc.TenantID, "outbox-dispatch-retry"); err != nil {
		t.Fatal(err)
	}
	secondCalled := make(chan struct{}, 1)
	secondSender := SenderFunc(func(context.Context, storage.OutboxMessage) SenderOutcome {
		secondCalled <- struct{}{}
		return SenderOutcome{Class: Delivered}
	})
	second, secondRun := runPostgresDispatcher(t, fixture, "dispatcher-retry-second", secondSender)
	select {
	case <-secondCalled:
	case <-time.After(10 * time.Second):
		t.Fatal("due retry was not reclaimed by a new Dispatcher")
	}
	waitForPostgresStatus(t, fixture, "outbox-dispatch-retry", storage.OutboxCompleted)
	if second.Stats().Completed != 1 {
		t.Fatalf("second dispatcher stats=%+v", second.Stats())
	}
	stopPostgresDispatcher(t, second, secondRun)
}

func TestPostgresDispatcherMaximumAttemptMovesToDLQ(t *testing.T) {
	fixture := newPostgresDispatcherFixture(t, 2*time.Second)
	fixture.enqueue(t, "outbox-dispatch-max")
	claimed, err := fixture.repo.ClaimBatch(fixture.ctx, fixture.tc, "pre-owner", 1)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("preclaim err=%v items=%+v", err, claimed)
	}
	if err := fixture.repo.MarkRetry(fixture.ctx, fixture.tc, "pre-owner", "outbox-dispatch-max", time.Now().UTC().Add(-time.Second), SenderUnavailableCode); err != nil {
		t.Fatal(err)
	}
	called := make(chan struct{}, 1)
	dispatcherConfig := testConfig(fixture.tc, "dispatcher-max")
	dispatcherConfig.RetryPolicy = RetryPolicy{MaxAttempts: 2, BaseDelay: time.Second, MaxDelay: time.Minute}
	dispatcherConfig.ClaimInterval = 5 * time.Millisecond
	dispatcherConfig.LockGuard = 50 * time.Millisecond
	dispatcher, err := NewDispatcher(fixture.repo, SenderFunc(func(context.Context, storage.OutboxMessage) SenderOutcome {
		called <- struct{}{}
		return SenderOutcome{Class: RetryableFailure, Code: SenderTimeoutCode}
	}), dispatcherConfig)
	if err != nil {
		t.Fatal(err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- dispatcher.Run(fixture.ctx) }()
	select {
	case <-called:
	case <-time.After(10 * time.Second):
		t.Fatal("maximum-attempt Sender was not called")
	}
	dead := waitForPostgresStatus(t, fixture, "outbox-dispatch-max", storage.OutboxDead)
	if dead.LastError != SenderTimeoutCode || dispatcher.Stats().DeadLettered != 1 {
		t.Fatalf("maximum-attempt state=%+v stats=%+v", dead, dispatcher.Stats())
	}
	stopPostgresDispatcher(t, dispatcher, runDone)
	var count int
	if err := fixture.pool.QueryRow(fixture.ctx, `SELECT count(*) FROM dead_letter WHERE tenant_id=$1 AND outbox_id=$2`, fixture.tc.TenantID, "outbox-dispatch-max").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("dead-letter row count=%d", count)
	}
}

func TestPostgresDispatcherUnknownOutcomeUsesSafeRetryPolicy(t *testing.T) {
	fixture := newPostgresDispatcherFixture(t, 2*time.Second)
	fixture.enqueue(t, "outbox-dispatch-unknown")
	called := make(chan struct{}, 1)
	config := testConfig(fixture.tc, "dispatcher-unknown")
	config.RetryPolicy = RetryPolicy{MaxAttempts: 2, BaseDelay: time.Hour, MaxDelay: time.Hour}
	dispatcher, err := NewDispatcher(fixture.repo, SenderFunc(func(context.Context, storage.OutboxMessage) SenderOutcome {
		called <- struct{}{}
		return SenderOutcome{Class: Unknown, Code: "provider body Authorization secret"}
	}), config)
	if err != nil {
		t.Fatal(err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- dispatcher.Run(fixture.ctx) }()
	select {
	case <-called:
	case <-time.After(10 * time.Second):
		t.Fatal("unknown Sender was not called")
	}
	stored := waitForPostgresStatus(t, fixture, "outbox-dispatch-unknown", storage.OutboxRetry)
	if stored.LastError != DeliveryOutcomeUnknownCode || stored.Status == storage.OutboxCompleted || stored.Status == storage.OutboxDead {
		t.Fatalf("unknown durable policy state=%+v", stored)
	}
	if stored.LastError == "provider body Authorization secret" {
		t.Fatal("raw sender detail reached durable last_error")
	}
	stopPostgresDispatcher(t, dispatcher, runDone)
}

func TestPostgresTwoDispatchersHaveOneActiveSender(t *testing.T) {
	fixture := newPostgresDispatcherFixture(t, 2*time.Second)
	fixture.enqueue(t, "outbox-dispatch-competing")
	secondRepo := fixture.repoForAnotherPool(t)
	calls := make(chan string, 2)
	var senderMu sync.Mutex
	senderCalls := 0
	sender := SenderFunc(func(context.Context, storage.OutboxMessage) SenderOutcome {
		senderMu.Lock()
		senderCalls++
		senderMu.Unlock()
		calls <- "called"
		return SenderOutcome{Class: Delivered}
	})
	firstConfig := testConfig(fixture.tc, "dispatcher-pg-a")
	firstConfig.ClaimInterval = 5 * time.Millisecond
	firstConfig.LockGuard = 50 * time.Millisecond
	secondConfig := firstConfig
	secondConfig.OwnerID = "dispatcher-pg-b"
	first, err := NewDispatcher(fixture.repo, sender, firstConfig)
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewDispatcher(secondRepo, sender, secondConfig)
	if err != nil {
		t.Fatal(err)
	}
	firstRun, secondRun := make(chan error, 1), make(chan error, 1)
	go func() { firstRun <- first.Run(fixture.ctx) }()
	go func() { secondRun <- second.Run(fixture.ctx) }()
	select {
	case <-calls:
	case <-time.After(10 * time.Second):
		t.Fatal("neither PostgreSQL Dispatcher called Sender")
	}
	waitForPostgresStatus(t, fixture, "outbox-dispatch-competing", storage.OutboxCompleted)
	stopPostgresDispatcher(t, first, firstRun)
	stopPostgresDispatcher(t, second, secondRun)
	senderMu.Lock()
	count := senderCalls
	senderMu.Unlock()
	if count != 1 || first.Stats().SendCalls+second.Stats().SendCalls != 1 {
		t.Fatalf("competing sender count=%d first=%+v second=%+v", count, first.Stats(), second.Stats())
	}
}

func TestPostgresDispatcherExpiredLockFencesOldOwnerAfterTakeover(t *testing.T) {
	fixture := newPostgresDispatcherFixture(t, 2*time.Second)
	fixture.enqueue(t, "outbox-dispatch-fence-overlap")
	secondRepo := fixture.repoForAnotherPool(t)
	oldStarted := make(chan struct{}, 1)
	newCalled := make(chan struct{}, 1)
	releaseOld := make(chan struct{})
	releaseNew := make(chan struct{})
	var senderMu sync.Mutex
	senderCalls := 0
	sender := SenderFunc(func(_ context.Context, _ storage.OutboxMessage) SenderOutcome {
		senderMu.Lock()
		senderCalls++
		call := senderCalls
		senderMu.Unlock()
		if call == 1 {
			oldStarted <- struct{}{}
			<-releaseOld
			return SenderOutcome{Class: Delivered}
		}
		newCalled <- struct{}{}
		<-releaseNew
		return SenderOutcome{Class: Delivered}
	})
	old, oldRun := runPostgresDispatcher(t, fixture, "dispatcher-fence-old", sender)
	select {
	case <-oldStarted:
	case <-time.After(10 * time.Second):
		t.Fatal("old owner did not start Sender")
	}
	if _, err := fixture.pool.Exec(fixture.ctx, `UPDATE outbox_message SET locked_until=clock_timestamp()-interval '1 second' WHERE tenant_id=$1 AND outbox_id=$2`, fixture.tc.TenantID, "outbox-dispatch-fence-overlap"); err != nil {
		t.Fatal(err)
	}
	newConfig := testConfig(fixture.tc, "dispatcher-fence-new")
	newConfig.ClaimInterval = 5 * time.Millisecond
	newConfig.LockGuard = 50 * time.Millisecond
	new, err := NewDispatcher(secondRepo, sender, newConfig)
	if err != nil {
		t.Fatal(err)
	}
	newRun := make(chan error, 1)
	go func() { newRun <- new.Run(fixture.ctx) }()
	select {
	case <-newCalled:
	case <-time.After(10 * time.Second):
		t.Fatal("new owner did not reclaim and call Sender")
	}
	waitForPostgresStatus(t, fixture, "outbox-dispatch-fence-overlap", storage.OutboxProcessing)
	close(releaseOld)
	if err := old.Stop(context.Background()); !errors.Is(err, ErrMutationFailed) || !errors.Is(err, storage.ErrOutboxLockLost) {
		t.Fatalf("old owner stop error=%v", err)
	}
	if err := <-oldRun; !errors.Is(err, context.Canceled) {
		t.Fatalf("old owner Run error=%v", err)
	}
	close(releaseNew)
	waitForPostgresStatus(t, fixture, "outbox-dispatch-fence-overlap", storage.OutboxCompleted)
	if old.Stats().Completed != 0 || old.Stats().MutationErrors != 1 || old.Stats().LockLost != 1 {
		t.Fatalf("old owner was not fenced: %+v", old.Stats())
	}
	if new.Stats().Completed != 1 || new.Stats().SendCalls != 1 {
		t.Fatalf("new owner stats=%+v", new.Stats())
	}
	stopPostgresDispatcher(t, new, newRun)
}

func TestPostgresDispatcherExpiredLockFencesOldOwner(t *testing.T) {
	fixture := newPostgresDispatcherFixture(t, 2*time.Second)
	fixture.enqueue(t, "outbox-dispatch-expired")
	claimed, err := fixture.repo.ClaimBatch(fixture.ctx, fixture.tc, "dispatcher-old", 1)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("old claim err=%v items=%+v", err, claimed)
	}
	if err := fixture.repo.MarkCompleted(fixture.ctx, fixture.tc, "dispatcher-old", "outbox-dispatch-expired"); err != nil {
		t.Fatal(err)
	}
	fixture.enqueue(t, "outbox-dispatch-expired-reclaim")
	claimed, err = fixture.repo.ClaimBatch(fixture.ctx, fixture.tc, "dispatcher-old", 1)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("reclaim setup err=%v items=%+v", err, claimed)
	}
	if _, err := fixture.pool.Exec(fixture.ctx, `UPDATE outbox_message SET locked_until=clock_timestamp()-interval '1 second' WHERE tenant_id=$1 AND outbox_id=$2`, fixture.tc.TenantID, "outbox-dispatch-expired-reclaim"); err != nil {
		t.Fatal(err)
	}
	if err := fixture.repo.MarkCompleted(fixture.ctx, fixture.tc, "dispatcher-old", "outbox-dispatch-expired-reclaim"); !errors.Is(err, storage.ErrOutboxLockExpired) {
		t.Fatalf("expired old owner error=%v", err)
	}
	called := make(chan struct{}, 1)
	dispatcher, runDone := runPostgresDispatcher(t, fixture, "dispatcher-new", SenderFunc(func(context.Context, storage.OutboxMessage) SenderOutcome {
		called <- struct{}{}
		return SenderOutcome{Class: Delivered}
	}))
	select {
	case <-called:
	case <-time.After(10 * time.Second):
		t.Fatal("new Dispatcher did not reclaim expired lock")
	}
	waitForPostgresStatus(t, fixture, "outbox-dispatch-expired-reclaim", storage.OutboxCompleted)
	stopPostgresDispatcher(t, dispatcher, runDone)
}

func TestPostgresDispatcherRecoversAfterDockerRestart(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	lab := testinfra.NewDockerLab(t)
	lab.Start(ctx)
	lab.WaitHealthy(ctx)
	cfg := postgresstore.PostgresConfig{URL: lab.PostgresURL(ctx), MaxConns: 8, MinConns: 1, AllowDestructiveDown: true}
	base, err := postgresstore.NewPool(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	schema := fmt.Sprintf("p009e_docker_%d", time.Now().UnixNano())
	if _, err := base.Exec(ctx, "CREATE SCHEMA "+quoteSchemaForTest(schema)); err != nil {
		base.Close()
		t.Fatal(err)
	}
	cfg.SearchPath = schema
	pool, err := postgresstore.NewPool(ctx, cfg)
	if err != nil {
		base.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		pool.Close()
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		_, _ = base.Exec(cleanupCtx, "DROP SCHEMA "+quoteSchemaForTest(schema)+" CASCADE")
		base.Close()
	})
	_, file, _, _ := runtime.Caller(0)
	migrator, err := postgresstore.NewMigratorWithPool(pool, cfg, os.DirFS(filepath.Join(filepath.Dir(file), "../../migrations")))
	if err != nil {
		t.Fatal(err)
	}
	if err := migrator.Up(ctx); err != nil {
		t.Fatal(err)
	}
	tc := testTenant("p009e-docker")
	repo, err := postgresstore.NewOutboxRepository(pool, postgresstore.OutboxRepositoryConfig{LockDuration: 2 * time.Second, MaxBatchSize: 100})
	if err != nil {
		t.Fatal(err)
	}
	message := storage.OutboxMessage{TenantID: tc.TenantID, ID: "outbox-dispatch-restart", Kind: "reply", AggregateID: "aggregate-restart", Payload: []byte(`{"restart":true}`)}
	if err := repo.Enqueue(ctx, tc, message); err != nil {
		t.Fatal(err)
	}
	claimed, err := repo.ClaimBatch(ctx, tc, "dispatcher-before-restart", 1)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("pre-restart claim err=%v items=%+v", err, claimed)
	}
	lab.StopPostgres(ctx)
	lab.RestartPostgres(ctx)
	lab.WaitHealthy(ctx)
	recoveredCfg := cfg
	recoveredCfg.URL = lab.PostgresURL(ctx)
	recoveredPool, err := postgresstore.NewPool(ctx, recoveredCfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(recoveredPool.Close)
	recoveredRepo, err := postgresstore.NewOutboxRepository(recoveredPool, postgresstore.OutboxRepositoryConfig{LockDuration: 2 * time.Second, MaxBatchSize: 100})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := recoveredPool.Exec(ctx, `UPDATE outbox_message SET locked_until=clock_timestamp()-interval '1 second' WHERE tenant_id=$1 AND outbox_id=$2`, tc.TenantID, message.ID); err != nil {
		t.Fatal(err)
	}
	called := make(chan struct{}, 1)
	config := testConfig(tc, "dispatcher-after-restart")
	config.ClaimInterval = 5 * time.Millisecond
	config.LockGuard = 50 * time.Millisecond
	dispatcher, err := NewDispatcher(recoveredRepo, SenderFunc(func(context.Context, storage.OutboxMessage) SenderOutcome {
		called <- struct{}{}
		return SenderOutcome{Class: Delivered}
	}), config)
	if err != nil {
		t.Fatal(err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- dispatcher.Run(ctx) }()
	select {
	case <-called:
	case <-time.After(20 * time.Second):
		t.Fatal("Dispatcher did not recover after PostgreSQL restart")
	}
	deadline := time.Now().Add(20 * time.Second)
	for {
		stored, getErr := recoveredRepo.Get(ctx, tc, message.ID)
		if getErr == nil && stored.Status == storage.OutboxCompleted {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("recovered message not completed: value=%+v err=%v", stored, getErr)
		}
		runtime.Gosched()
	}
	if err := dispatcher.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := <-runDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run error=%v", err)
	}
}
