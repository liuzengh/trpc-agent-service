package postgresadapter_test

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	wire "github.com/liuzengh/trpc-agent-service/api/events/execution/v1"
	governancev1 "github.com/liuzengh/trpc-agent-service/api/runtime/governance/v1"
	admissionpg "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/admission/adapter/outbound/postgres"
	"github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/admission/application"
	"github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/admission/domain"
	routepg "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/routing/adapter/outbound/postgres"
	routedomain "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/routing/domain"
	"github.com/liuzengh/trpc-agent-service/services/channel-gateway/migrations"
)

func setup(t *testing.T) (*admissionpg.Store, *pgxpool.Pool, *routepg.Store) {
	t.Helper()
	dsn := os.Getenv("GATEWAY_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set GATEWAY_TEST_DATABASE_URL to run real PostgreSQL acceptance tests")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)
	var random [8]byte
	if _, err = rand.Read(random[:]); err != nil {
		t.Fatal(err)
	}
	schema := pgx.Identifier{"gateway_admission_" + hex.EncodeToString(random[:])}.Sanitize()
	if _, err = admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if _, err := admin.Exec(ctx, "DROP SCHEMA "+schema+" CASCADE"); err != nil {
			t.Errorf("drop test schema: %v", err)
		}
	})
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	config.ConnConfig.RuntimeParams["search_path"] = schema
	config.MaxConns = 32
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if err = migrations.Apply(ctx, pool); err != nil {
		t.Fatal(err)
	}
	routes := routepg.NewStore(pool)
	if err = routes.BeginReplay(ctx, routedomain.ReplaySource{StreamName: "ROUTE_TEST", StreamID: "2026-09-05T00:00:00Z", FirstSequence: 1, LastSequence: 1, MessageCount: 1}); err != nil {
		t.Fatal(err)
	}
	if err = applyRoute(ctx, routes, routeEvent(1, true)); err != nil {
		t.Fatal(err)
	}
	return admissionpg.NewStore(pool, routes), pool, routes
}
func applyRoute(ctx context.Context, routes *routepg.Store, event routedomain.RouteEvent) error {
	return routes.ApplyFromStream(ctx, routedomain.StreamPosition{StreamName: "ROUTE_TEST", StreamID: "2026-09-05T00:00:00Z", Sequence: uint64(event.Route.Generation)}, event)
}
func routeEvent(generation int64, enabled bool) routedomain.RouteEvent {
	r := routedomain.RouteSnapshot{Provider: "telegram", AccountID: "account", Generation: generation}
	if enabled {
		r.TenantID = "tenant"
		r.BindingID = "binding"
		r.DeploymentRevisionID = "revision"
		r.ManifestRef = "manifest/revision"
		r.ManifestDigest = "sha256:" + strings.Repeat("b", 64)
	}
	return routedomain.RouteEvent{EventID: fmt.Sprintf("route-%d", generation), SchemaVersion: 1, Enabled: enabled, Route: r}
}
func telegramEventID(id string) string {
	if n, err := strconv.ParseInt(id, 10, 64); err == nil && n >= 0 {
		return id
	}
	sum := sha256.Sum256([]byte(id))
	return strconv.FormatUint(binary.BigEndian.Uint64(sum[:8])&0x7fffffffffffffff, 10)
}
func acceptance(id string) domain.Acceptance {
	return domain.Acceptance{Input: domain.Inbound{Key: domain.EventKey{Provider: "telegram", AccountID: "account", EventID: telegramEventID(id)}, Kind: "text", ConversationID: "100", SenderID: "100", Text: "hello", ReplyContext: json.RawMessage(`{"chat_id":"100","source_message_id":"42"}`), SourceDigest: strings.Repeat("a", 64), ReceivedAt: time.Now().UTC()}, Receipt: domain.Receipt{Decision: "admit-run", AdmissionID: "admission-" + id, RunID: "run-" + id}, Route: &domain.RouteSnapshot{Provider: "telegram", AccountID: "account", TenantID: "tenant", BindingID: "binding", Generation: 1, DeploymentRevisionID: "revision", ManifestRef: "manifest/revision", ManifestDigest: "sha256:" + strings.Repeat("b", 64)}}
}
func ignore(id, kind string) domain.Acceptance {
	c := acceptance(id)
	c.Input.Kind = kind
	c.Input.Text = ""
	c.Route = nil
	c.Receipt = domain.Receipt{Decision: kind, Reason: "audited"}
	return c
}
func counts(t *testing.T, pool *pgxpool.Pool, wantInbox, wantAdmission, wantOutbox, wantBudget int) {
	t.Helper()
	var inbox, admission, outbox, budget int
	err := pool.QueryRow(context.Background(), `SELECT (SELECT count(*) FROM gateway_inbox),(SELECT count(*) FROM gateway_admissions),(SELECT count(*) FROM gateway_outbox),(SELECT new_events FROM gateway_admission_budget)`).Scan(&inbox, &admission, &outbox, &budget)
	if err != nil {
		t.Fatal(err)
	}
	if inbox != wantInbox || admission != wantAdmission || outbox != wantOutbox || budget != wantBudget {
		t.Fatalf("counts inbox/admission/outbox/budget=%d/%d/%d/%d want=%d/%d/%d/%d", inbox, admission, outbox, budget, wantInbox, wantAdmission, wantOutbox, wantBudget)
	}
}
func TestIntegrationSingleWinnerAndSourceConflict(t *testing.T) {
	store, pool, routes := setup(t)
	ctx := context.Background()
	const workers = 24
	results := make(chan domain.Receipt, workers)
	errs := make(chan error, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			c := acceptance("same")
			c.Receipt.AdmissionID = fmt.Sprintf("candidate-%d", i)
			c.Receipt.RunID = fmt.Sprintf("candidate-run-%d", i)
			r, err := store.Commit(ctx, c)
			results <- r
			errs <- err
		}(i)
	}
	wg.Wait()
	close(results)
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	var winner domain.Receipt
	for r := range results {
		if winner.RunID == "" {
			winner = r
		} else if r != winner {
			t.Fatalf("multiple winners %+v vs %+v", winner, r)
		}
	}
	counts(t, pool, 1, 1, 1, 1)
	if err := applyRoute(ctx, routes, routeEvent(2, false)); err != nil {
		t.Fatal(err)
	}
	replay := acceptance("same")
	r, err := store.Commit(ctx, replay)
	if err != nil || r != winner {
		t.Fatalf("disabled route changed replay: %+v %v", r, err)
	}
	replay.Input.SourceDigest = strings.Repeat("c", 64)
	if _, err = store.Commit(ctx, replay); !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("source digest conflict: %v", err)
	}
	counts(t, pool, 1, 1, 1, 1)
	var payload []byte
	var subject string
	if err = pool.QueryRow(ctx, `SELECT subject,payload FROM gateway_outbox`).Scan(&subject, &payload); err != nil {
		t.Fatal(err)
	}
	event, err := wire.DecodeRunRequested(payload)
	if err != nil {
		t.Fatal(err)
	}
	if subject != wire.RunRequestedSubject || event.EventID != winner.AdmissionID || event.RunID != winner.RunID || event.Route.Generation != 1 || event.Input.Key.EventID != telegramEventID("same") {
		t.Fatalf("incorrect immutable wire event: %+v", event)
	}
}

func TestIntegrationUsageRateIsSharedAndAtomic(t *testing.T) {
	store, pool, _ := setup(t)
	first := acceptance("rate-1")
	policy := governancev1.Policy{
		SchemaVersion: 1, TenantID: "tenant", Revision: 1, Enabled: true,
		IM:        governancev1.IMPolicy{AllowAll: true, Rules: []governancev1.IMRule{}},
		Requests:  governancev1.RequestPolicy{TenantPerMinute: 2, UserPerMinute: 1},
		Execution: governancev1.ExecutionPolicy{MaxConcurrentRuns: 2},
		Tokens:    governancev1.TokenPolicy{PeriodSeconds: 3600, Limit: 10000, ReservationPerRun: 100},
	}
	first.Policy = &policy
	if _, err := store.Commit(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	second := acceptance("rate-2")
	second.Policy = &policy
	if _, err := store.Commit(context.Background(), second); !errors.Is(err, domain.ErrRateLimited) {
		t.Fatalf("second user request was not limited: %v", err)
	}
	var tenantCount, userCount int
	if err := pool.QueryRow(context.Background(), `SELECT
COALESCE(max(request_count) FILTER(WHERE subject_kind='tenant'),0),
COALESCE(max(request_count) FILTER(WHERE subject_kind='user'),0)
FROM gateway_usage_rate_windows WHERE tenant_id='tenant'`).Scan(&tenantCount, &userCount); err != nil {
		t.Fatal(err)
	}
	if tenantCount != 1 || userCount != 1 {
		t.Fatalf("rejected request leaked a partial charge: tenant=%d user=%d", tenantCount, userCount)
	}
}
func TestIntegrationIgnoreAndInteractionDoNotUseRouteGuard(t *testing.T) {
	_, pool, routes := setup(t)
	ctx := context.Background()
	if err := applyRoute(ctx, routes, routeEvent(2, false)); err != nil {
		t.Fatal(err)
	}
	store := admissionpg.NewStore(pool, nil)
	for _, kind := range []string{"ignore", "interaction"} {
		c := ignore(kind, kind)
		if _, err := store.Commit(ctx, c); err != nil {
			t.Fatalf("%s requires route guard: %v", kind, err)
		}
	}
	counts(t, pool, 2, 0, 0, 2)
	if _, err := store.Commit(ctx, acceptance("text")); !errors.Is(err, domain.ErrUnavailable) {
		t.Fatalf("run without guard accepted: %v", err)
	}
	counts(t, pool, 2, 0, 0, 2)
}
func TestIntegrationRollbackOfWritesAndCommitFailure(t *testing.T) {
	for _, deferred := range []bool{false, true} {
		t.Run(fmt.Sprintf("deferred-%t", deferred), func(t *testing.T) {
			store, pool, _ := setup(t)
			ctx := context.Background()
			_, err := pool.Exec(ctx, `CREATE FUNCTION reject_outbox() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected outbox failure'; END $$`)
			if err != nil {
				t.Fatal(err)
			}
			statement := `CREATE TRIGGER fail_outbox BEFORE INSERT ON gateway_outbox FOR EACH ROW EXECUTE FUNCTION reject_outbox()`
			if deferred {
				statement = `CREATE CONSTRAINT TRIGGER fail_outbox AFTER INSERT ON gateway_outbox DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION reject_outbox()`
			}
			if _, err = pool.Exec(ctx, statement); err != nil {
				t.Fatal(err)
			}
			c := acceptance("rollback")
			if _, err = store.Commit(ctx, c); err == nil {
				t.Fatal("injected transaction failure acknowledged")
			}
			counts(t, pool, 0, 0, 0, 0)
			if _, _, found, err := store.Find(ctx, c.Input.Key); err != nil || found {
				t.Fatalf("rollback left receipt found=%t err=%v", found, err)
			}
			if _, err = pool.Exec(ctx, `DROP TRIGGER fail_outbox ON gateway_outbox`); err != nil {
				t.Fatal(err)
			}
			if _, err = store.Commit(ctx, c); err != nil {
				t.Fatal(err)
			}
			counts(t, pool, 1, 1, 1, 1)
		})
	}
}
func TestIntegrationRouteUpdateRacesAndSharedGuard(t *testing.T) {
	store, pool, routes := setup(t)
	ctx := context.Background()
	if err := applyRoute(ctx, routes, routeEvent(2, true)); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Commit(ctx, acceptance("stale")); !errors.Is(err, domain.ErrRouteChanged) {
		t.Fatalf("stale route committed: %v", err)
	}
	counts(t, pool, 0, 0, 0, 0)
	c := acceptance("fresh")
	c.Route.Generation = 2
	if _, err := store.Commit(ctx, c); err != nil {
		t.Fatal(err)
	}
	// Real routing Guard holds an account shared lock until the acceptance tx ends.
	entered := make(chan struct{})
	release := make(chan struct{})
	done := make(chan error, 1)
	guarded := admissionpg.NewStore(pool, guardFunc(func(ctx context.Context, tx pgx.Tx, p, a string, g int64) error {
		if err := routes.VerifyGeneration(ctx, tx, p, a, g); err != nil {
			return err
		}
		close(entered)
		<-release
		return nil
	}))
	c = acceptance("guarded")
	c.Route.Generation = 2
	go func() { _, err := guarded.Commit(ctx, c); done <- err }()
	<-entered
	updateCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	err := applyRoute(updateCtx, routes, routeEvent(3, false))
	cancel()
	if err == nil {
		t.Fatal("route mutation crossed active admission guard")
	}
	close(release)
	if err = <-done; err != nil {
		t.Fatal(err)
	}
	if err = applyRoute(ctx, routes, routeEvent(3, false)); err != nil {
		t.Fatal(err)
	}
	counts(t, pool, 2, 2, 2, 2)
}

type guardFunc func(context.Context, pgx.Tx, string, string, int64) error

func (f guardFunc) VerifyGeneration(ctx context.Context, tx pgx.Tx, p, a string, g int64) error {
	return f(ctx, tx, p, a, g)
}
func TestIntegrationPendingBudgetHasAtomicGlobalWinner(t *testing.T) {
	_, pool, routes := setup(t)
	budget := admissionpg.DefaultBudget()
	budget.MaxPendingOutbox = 3
	store, err := admissionpg.NewStoreWithBudget(pool, routes, budget)
	if err != nil {
		t.Fatal(err)
	}
	const workers = 20
	var winners atomic.Int32
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	ctx := context.Background()
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := store.Commit(ctx, acceptance(fmt.Sprintf("budget-%d", i)))
			if err == nil {
				winners.Add(1)
			} else if !errors.Is(err, domain.ErrUnavailable) {
				errs <- err
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	if winners.Load() != 3 {
		t.Fatalf("budget overshoot/wrong winners %d", winners.Load())
	}
	counts(t, pool, 3, 3, 3, 3)
	health, err := store.Health(ctx)
	if err != nil || !health.Saturated || health.Pending != 3 {
		t.Fatalf("health: %+v %v", health, err)
	}
	// Event replay remains available even after the budget is saturated.
	var id string
	if err = pool.QueryRow(ctx, `SELECT event_id FROM gateway_inbox LIMIT 1`).Scan(&id); err != nil {
		t.Fatal(err)
	}
	if _, err = store.Commit(ctx, acceptance(id)); err != nil {
		t.Fatalf("budget blocked receipt replay: %v", err)
	}
	// Non-run receipts consume the Inbox quota, but not the Outbox allowance.
	if _, err = store.Commit(ctx, ignore("ignored", "ignore")); err != nil {
		t.Fatal(err)
	}
	counts(t, pool, 4, 3, 3, 4)
	msg, found, err := store.Claim(ctx)
	if err != nil || !found {
		t.Fatalf("claim: %v %v", found, err)
	}
	if err = store.Published(ctx, msg); err != nil {
		t.Fatal(err)
	}
	if _, err = store.Commit(ctx, acceptance("after-publish")); err != nil {
		t.Fatalf("publish did not free pending capacity: %v", err)
	}
	counts(t, pool, 5, 4, 4, 5)
}
func TestIntegrationOldestBudgetAndInboxFixedWindow(t *testing.T) {
	store, pool, routes := setup(t)
	ctx := context.Background()
	if _, err := store.Commit(ctx, acceptance("old")); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE gateway_outbox SET created_at=clock_timestamp()-interval '11 minutes'`); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Commit(ctx, acceptance("denied")); !errors.Is(err, domain.ErrUnavailable) {
		t.Fatalf("stale backlog ignored: %v", err)
	}
	health, err := store.Health(ctx)
	if err != nil || !health.Saturated || health.OldestAge < 10*time.Minute {
		t.Fatalf("stale health: %+v %v", health, err)
	}
	budget := admissionpg.DefaultBudget()
	budget.MaxNewInboxPerMinute = 3
	store, err = admissionpg.NewStoreWithBudget(pool, routes, budget)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.Commit(ctx, ignore("one", "ignore")); err != nil {
		t.Fatal(err)
	}
	if _, err = store.Commit(ctx, ignore("two", "interaction")); err != nil {
		t.Fatal(err)
	}
	if _, err = store.Commit(ctx, ignore("three", "ignore")); !errors.Is(err, domain.ErrUnavailable) {
		t.Fatalf("new inbox budget bypass: %v", err)
	}
	counts(t, pool, 3, 1, 1, 3)
	if _, err = store.Commit(ctx, ignore("two", "interaction")); err != nil {
		t.Fatalf("full inbox quota blocked replay: %v", err)
	}
	if _, err = pool.Exec(ctx, `UPDATE gateway_admission_budget SET window_started_at=clock_timestamp()-interval '61 seconds'`); err != nil {
		t.Fatal(err)
	}
	if _, err = store.Commit(ctx, ignore("three", "ignore")); err != nil {
		t.Fatalf("fixed window did not reset: %v", err)
	}
	counts(t, pool, 4, 1, 1, 1)
}
func TestIntegrationOutboxClaimsRetryExpiryAndCrashRecovery(t *testing.T) {
	store, pool, _ := setup(t)
	ctx := context.Background()
	if _, err := store.Commit(ctx, acceptance("claim")); err != nil {
		t.Fatal(err)
	}
	const workers = 12
	claims := make(chan application.OutboxMessage, workers)
	errs := make(chan error, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			m, found, err := store.Claim(ctx)
			if err != nil {
				errs <- err
			}
			if found {
				claims <- m
			}
		}()
	}
	wg.Wait()
	close(claims)
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	var first application.OutboxMessage
	count := 0
	for msg := range claims {
		first = msg
		count++
	}
	if count != 1 {
		t.Fatalf("concurrent active claims=%d", count)
	}
	wrong := first
	wrong.ClaimToken = "wrong"
	if err := store.Published(ctx, wrong); !errors.Is(err, domain.ErrClaimLost) {
		t.Fatalf("wrong publish claim: %v", err)
	}
	if err := store.Retry(ctx, wrong); !errors.Is(err, domain.ErrClaimLost) {
		t.Fatalf("wrong retry claim: %v", err)
	}
	if err := store.Retry(ctx, first); err != nil {
		t.Fatal(err)
	}
	if _, found, err := store.Claim(ctx); err != nil || found {
		t.Fatalf("retry backoff skipped: %v %v", found, err)
	}
	if _, err := pool.Exec(ctx, `UPDATE gateway_outbox SET next_attempt_at=clock_timestamp()-interval '1 second'`); err != nil {
		t.Fatal(err)
	}
	second, found, err := store.Claim(ctx)
	if err != nil || !found || second.ClaimToken == first.ClaimToken || second.EventID != first.EventID || string(second.Payload) != string(first.Payload) {
		t.Fatalf("retry changed event or reused claim: %+v %v", second, err)
	}
	if err = store.Published(ctx, first); !errors.Is(err, domain.ErrClaimLost) {
		t.Fatalf("stale worker publish accepted: %v", err)
	}
	// Simulate process death after broker acceptance but before marking Published.
	if _, err = pool.Exec(ctx, `UPDATE gateway_outbox SET claimed_until=clock_timestamp()-interval '1 second'`); err != nil {
		t.Fatal(err)
	}
	if err = store.Retry(ctx, second); !errors.Is(err, domain.ErrClaimLost) {
		t.Fatalf("expired retry claim accepted: %v", err)
	}
	third, found, err := store.Claim(ctx)
	if err != nil || !found || third.ClaimToken == second.ClaimToken || third.EventID != first.EventID {
		t.Fatalf("crash recovery: %+v %v", third, err)
	}
	if err = store.Published(ctx, second); !errors.Is(err, domain.ErrClaimLost) {
		t.Fatalf("stale ack accepted: %v", err)
	}
	if err = store.Published(ctx, third); err != nil {
		t.Fatal(err)
	}
	if _, found, err = store.Claim(ctx); err != nil || found {
		t.Fatalf("published event claimed again: %v %v", found, err)
	}
	var attempts int
	if err = pool.QueryRow(ctx, `SELECT attempts FROM gateway_outbox`).Scan(&attempts); err != nil || attempts != 3 {
		t.Fatalf("attempts=%d err=%v", attempts, err)
	}
}
func TestIntegrationCancellationRollsBackBudgetWait(t *testing.T) {
	store, pool, _ := setup(t)
	ctx := context.Background()
	lock, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Rollback(ctx)
	if _, err = lock.Exec(ctx, `SELECT singleton FROM gateway_admission_budget FOR UPDATE`); err != nil {
		t.Fatal(err)
	}
	deadline, cancel := context.WithTimeout(ctx, 25*time.Millisecond)
	defer cancel()
	if _, err = store.Commit(deadline, acceptance("timeout")); err == nil {
		t.Fatal("lock wait ignored cancellation")
	}
	if err = lock.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	counts(t, pool, 0, 0, 0, 0)
	if _, err = store.Commit(ctx, acceptance("timeout")); err != nil {
		t.Fatalf("cancellation stranded locks: %v", err)
	}
	counts(t, pool, 1, 1, 1, 1)
}

func TestIntegrationCorruptReceiptFailsClosed(t *testing.T) {
	store, pool, _ := setup(t)
	ctx := context.Background()
	c := ignore("receipt", "ignore")
	if _, err := store.Commit(ctx, c); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE gateway_inbox SET receipt='{"decision":"admit-run"}'::jsonb`); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := store.Find(ctx, c.Input.Key); !errors.Is(err, domain.ErrUnavailable) {
		t.Fatalf("corrupt receipt replayed: %v", err)
	}
	if _, err := store.Commit(ctx, c); !errors.Is(err, domain.ErrUnavailable) {
		t.Fatalf("corrupt receipt overwritten: %v", err)
	}
	counts(t, pool, 1, 0, 0, 1)
}

func TestIntegrationAllFreshInboxKindsShareAtomicWindow(t *testing.T) {
	_, pool, routes := setup(t)
	budget := admissionpg.DefaultBudget()
	budget.MaxNewInboxPerMinute = 3
	store, err := admissionpg.NewStoreWithBudget(pool, routes, budget)
	if err != nil {
		t.Fatal(err)
	}
	var winners atomic.Int32
	var wg sync.WaitGroup
	errs := make(chan error, 20)
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			kind := "ignore"
			if i%2 == 0 {
				kind = "interaction"
			}
			_, err := store.Commit(context.Background(), ignore(fmt.Sprintf("fresh-%d", i), kind))
			if err == nil {
				winners.Add(1)
			} else if !errors.Is(err, domain.ErrUnavailable) {
				errs <- err
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	if winners.Load() != 3 {
		t.Fatalf("fresh inbox quota overshot: %d", winners.Load())
	}
	counts(t, pool, 3, 0, 0, 3)
	health, err := store.Health(context.Background())
	if err != nil || !health.Saturated || health.Pending != 0 || health.NewInbox != 3 {
		t.Fatalf("inbox saturation health: %+v %v", health, err)
	}
}
