package postgresadapter_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	wire "github.com/liuzengh/trpc-agent-service/api/events/execution/v1"
	"github.com/liuzengh/trpc-agent-service/platform/im/wecom"
	wecomadapter "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/admission/adapter/inbound/wecomadapter"
	admissionpg "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/admission/adapter/outbound/postgres"
	"github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/admission/application"
	"github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/admission/domain"
	connectionpg "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/adapter/outbound/postgres"
	connectiondomain "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/domain"
	routepg "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/routing/adapter/outbound/postgres"
	routedomain "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/routing/domain"
)

type ownerGuardFunc func(context.Context, pgx.Tx, string, string, int64, int64) error

func (f ownerGuardFunc) VerifyOwner(ctx context.Context, tx pgx.Tx, account, instance string, epoch, revision int64) error {
	return f(ctx, tx, account, instance, epoch, revision)
}
func setupWeCom(t *testing.T) (*admissionpg.Store, *pgxpool.Pool, *routepg.Store) {
	t.Helper()
	store, pool, routes := setup(t)
	event := routeEvent(1, true)
	event.EventID = "route-wecom"
	event.Route.Provider = "wecom"
	if err := routes.ApplyFromStream(context.Background(), routedomain.StreamPosition{StreamName: "ROUTE_TEST", StreamID: "2026-09-05T00:00:00Z", Sequence: 2}, event); err != nil {
		t.Fatal(err)
	}
	return store, pool, routes
}
func wecomAcceptance(id string) domain.Acceptance {
	c := acceptance(id)
	c.Input.Key.Provider = "wecom"
	c.Input.Key.EventID = id
	c.Route.Provider = "wecom"
	c.Input.ConnectionFence = &domain.ConnectionFence{InstanceID: "owner-a", Epoch: 7, Revision: 3}
	c.Input.ReplyContext = json.RawMessage(`{"chat_type":"single","chatid_or_userid":"100","callback_req_id":"request-original","received_at":"` + c.Input.ReceivedAt.Format(time.RFC3339Nano) + `"}`)
	return c
}

func TestOwnerGuardRejectsNewWeComAndRollsBackBudget(t *testing.T) {
	store, pool, _ := setupWeCom(t)
	for _, decision := range []string{"admit-run", "ignore", "interaction"} {
		t.Run(decision, func(t *testing.T) {
			c := wecomAcceptance(decision)
			if decision != "admit-run" {
				c.Input.Kind = decision
				c.Input.Text = ""
				c.Route = nil
				c.Receipt = domain.Receipt{Decision: decision}
			}
			if _, err := store.Commit(context.Background(), c); !errors.Is(err, domain.ErrUnavailable) {
				t.Fatalf("missing owner guard accepted: %v", err)
			}
			counts(t, pool, 0, 0, 0, 0)
			var called atomic.Int32
			denied := store.WithConnectionGuard(ownerGuardFunc(func(ctx context.Context, tx pgx.Tx, account, instance string, epoch, revision int64) error {
				called.Add(1)
				if account != "account" || instance != "owner-a" || epoch != 7 || revision != 3 {
					t.Errorf("incorrect owner fence arguments")
				}
				return errors.New("synthetic private owner failure")
			}))
			_, err := denied.Commit(context.Background(), c)
			if !errors.Is(err, domain.ErrUnavailable) || strings.Contains(err.Error(), "private") {
				t.Fatalf("owner failure result=%v", err)
			}
			if called.Load() != 1 {
				t.Fatal("owner guard did not run exactly once")
			}
			counts(t, pool, 0, 0, 0, 0)
		})
	}
}

func TestOwnerGuardReplayKeepsFirstReplyAndFenceNeverLeavesProcess(t *testing.T) {
	store, pool, _ := setupWeCom(t)
	var calls atomic.Int32
	guard := ownerGuardFunc(func(context.Context, pgx.Tx, string, string, int64, int64) error { calls.Add(1); return nil })
	guarded := store.WithConnectionGuard(guard)
	first := wecomAcceptance("same")
	receipt, err := guarded.Commit(context.Background(), first)
	if err != nil {
		t.Fatal(err)
	}
	replay := wecomAcceptance("same")
	replay.Input.ConnectionFence = &domain.ConnectionFence{InstanceID: "owner-old", Epoch: 1, Revision: 1}
	replay.Input.ReplyContext = json.RawMessage(`{"chat_type":"single","chatid_or_userid":"100","callback_req_id":"request-new","received_at":"` + replay.Input.ReceivedAt.Format(time.RFC3339Nano) + `"}`)
	replay.Receipt.AdmissionID = "different-admission"
	replay.Receipt.RunID = "different-run"
	denied := store.WithConnectionGuard(ownerGuardFunc(func(context.Context, pgx.Tx, string, string, int64, int64) error {
		t.Error("old receipt rechecked owner")
		return errors.New("expired")
	}))
	if got, err := denied.Commit(context.Background(), replay); err != nil || got != receipt {
		t.Fatalf("receipt replay changed: %+v %v", got, err)
	}
	if got, err := store.Commit(context.Background(), replay); err != nil || got != receipt {
		t.Fatalf("missing guard blocked old receipt: %+v %v", got, err)
	}
	if calls.Load() != 1 {
		t.Fatal("initial owner check missing")
	}
	var input, payload []byte
	if err := pool.QueryRow(context.Background(), `SELECT input FROM gateway_admissions`).Scan(&input); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(context.Background(), `SELECT payload FROM gateway_outbox`).Scan(&payload); err != nil {
		t.Fatal(err)
	}
	for _, raw := range [][]byte{input, payload} {
		if !strings.Contains(string(raw), "request-original") || strings.Contains(string(raw), "request-new") {
			t.Fatalf("first ReplyContext changed: %s", raw)
		}
		for _, forbidden := range []string{"connection_fence", "ConnectionFence", "owner-a", "owner-old", "instance_id", "Epoch", "Revision"} {
			if strings.Contains(string(raw), forbidden) {
				t.Fatalf("local fence leaked: %s", raw)
			}
		}
	}
	if _, err := wire.DecodeRunRequested(payload); err != nil {
		t.Fatal(err)
	}
	replay.Input.SourceDigest = strings.Repeat("c", 64)
	if _, err := denied.Commit(context.Background(), replay); !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("digest conflict hidden by replay: %v", err)
	}
	counts(t, pool, 1, 1, 1, 1)
}

func TestTelegramBypassesConnectionGuard(t *testing.T) {
	store, pool, _ := setup(t)
	store = store.WithConnectionGuard(ownerGuardFunc(func(context.Context, pgx.Tx, string, string, int64, int64) error {
		t.Error("Telegram entered Connection")
		return errors.New("denied")
	}))
	if _, err := store.Commit(context.Background(), acceptance("telegram")); err != nil {
		t.Fatal(err)
	}
	counts(t, pool, 1, 1, 1, 1)
}

func acquireOwner(t *testing.T, pool *pgxpool.Pool, ttl time.Duration) (*connectionpg.Store, connectiondomain.OwnerGrant) {
	t.Helper()
	owners := connectionpg.NewStore(pool)
	grant, err := owners.ApplyAndAcquire(context.Background(), connectiondomain.Account{ID: "account", BotID: "bot-1", CredentialRef: "env:WECOM_TEST_SECRET", Revision: 3, Enabled: true}, "owner-a", ttl)
	if err != nil {
		t.Fatal(err)
	}
	return owners, grant
}
func useGrant(c *domain.Acceptance, g connectiondomain.OwnerGrant) {
	c.Input.ConnectionFence = &domain.ConnectionFence{InstanceID: g.InstanceID, Epoch: g.Epoch, Revision: g.Revision}
}

func waitForDatabaseBlocker(t *testing.T, ctx context.Context, pool *pgxpool.Pool, pid int32) {
	t.Helper()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		var blocked bool
		if err := pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE $1=ANY(pg_blocking_pids(pid)))`, pid).Scan(&blocked); err != nil {
			t.Fatal(err)
		}
		if blocked {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal("second transaction did not actually wait on the owner row lock")
		case <-ticker.C:
		}
	}
}

func TestOwnerGuardHoldsLeaseLockThroughAdmissionCommit(t *testing.T) {
	store, pool, _ := setupWeCom(t)
	owners, grant := acquireOwner(t, pool, 10*time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	verified := make(chan int32, 1)
	proceed := make(chan struct{})
	store = store.WithConnectionGuard(ownerGuardFunc(func(ctx context.Context, tx pgx.Tx, account, instance string, epoch, revision int64) error {
		if err := owners.VerifyOwner(ctx, tx, account, instance, epoch, revision); err != nil {
			return err
		}
		var pid int32
		if err := tx.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&pid); err != nil {
			return err
		}
		verified <- pid
		select {
		case <-proceed:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}))
	c := wecomAcceptance("held-lock")
	useGrant(&c, grant)
	commitResult := make(chan error, 1)
	go func() { _, err := store.Commit(ctx, c); commitResult <- err }()
	var ownerPID int32
	select {
	case ownerPID = <-verified:
	case err := <-commitResult:
		t.Fatalf("guard failed before holding lock: %v", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	released := make(chan error, 1)
	go func() { released <- owners.Release(ctx, grant) }()
	waitForDatabaseBlocker(t, ctx, pool, ownerPID)
	select {
	case err := <-released:
		t.Fatalf("release crossed an open Admission transaction: %v", err)
	default:
	}
	close(proceed)
	if err := <-commitResult; err != nil {
		t.Fatal(err)
	}
	if err := <-released; err != nil {
		t.Fatal(err)
	}
	c = wecomAcceptance("after-release")
	useGrant(&c, grant)
	if _, err := store.WithConnectionGuard(owners).Commit(ctx, c); !errors.Is(err, domain.ErrUnavailable) {
		t.Fatalf("released owner still admitted: %v", err)
	}
	counts(t, pool, 1, 1, 1, 1)
}

func TestOwnerGuardRechecksDatabaseClockAfterWaiting(t *testing.T) {
	store, pool, _ := setupWeCom(t)
	owners, grant := acquireOwner(t, pool, 500*time.Millisecond)
	store = store.WithConnectionGuard(owners)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	blocker, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = blocker.Rollback(cleanup)
	}()
	var pid int32
	if err = blocker.QueryRow(ctx, `SELECT pg_backend_pid() FROM gateway_connection_accounts WHERE account_id='account' FOR UPDATE`).Scan(&pid); err != nil {
		t.Fatal(err)
	}
	c := wecomAcceptance("waited-past-expiry")
	useGrant(&c, grant)
	result := make(chan error, 1)
	go func() { _, err := store.Commit(ctx, c); result <- err }()
	waitForDatabaseBlocker(t, ctx, pool, pid)
	// The database, not a forged input timestamp or test-only Go clock, determines expiry.
	for {
		var expired bool
		if err = pool.QueryRow(ctx, `SELECT clock_timestamp()>$1::timestamptz`, grant.LeaseUntil).Scan(&expired); err != nil {
			t.Fatal(err)
		}
		if expired {
			break
		}
		select {
		case <-time.After(5 * time.Millisecond):
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	if err = blocker.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err = <-result; !errors.Is(err, domain.ErrUnavailable) {
		t.Fatalf("lease expired during lock wait was accepted: %v", err)
	}
	counts(t, pool, 0, 0, 0, 0)
}

type wecomRouteResolver struct{ routes *routepg.Store }

func (r wecomRouteResolver) Resolve(ctx context.Context, provider, account string) (domain.RouteSnapshot, error) {
	route, err := r.routes.Resolve(ctx, provider, account)
	return domain.RouteSnapshot{Provider: route.Provider, AccountID: route.AccountID, TenantID: route.TenantID, BindingID: route.BindingID, Generation: route.Generation, DeploymentRevisionID: route.DeploymentRevisionID, ManifestRef: route.ManifestRef, ManifestDigest: route.ManifestDigest}, err
}

func TestWeComAdapterSemanticReplayThroughRealAdmission(t *testing.T) {
	store, pool, routes := setupWeCom(t)
	owners, grant := acquireOwner(t, pool, 10*time.Second)
	service := application.New(store.WithConnectionGuard(owners), wecomRouteResolver{routes})
	handler, err := wecomadapter.NewHandler("account", "bot-1", domain.ConnectionFence{InstanceID: grant.InstanceID, Epoch: grant.Epoch, Revision: grant.Revision}, service)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	event := wecom.Event{Kind: wecom.EventText, BodyDigest: strings.Repeat("a", 64), MessageID: "stable-message", BotID: "bot-1", RequestID: "request-original", Generation: 1, SenderID: "100", ChatType: "single", Text: "hello"}
	if err = handler.Handle(ctx, event); err != nil {
		t.Fatal(err)
	}
	if err = owners.Release(ctx, grant); err != nil {
		t.Fatal(err)
	}
	event.Generation = 99
	event.RequestID = "changed-request"
	if err = handler.Handle(ctx, event); err != nil {
		t.Fatalf("equivalent redelivery failed after lease release: %v", err)
	}
	var payload []byte
	if err = pool.QueryRow(ctx, `SELECT payload FROM gateway_outbox`).Scan(&payload); err != nil {
		t.Fatal(err)
	}
	parsed, err := wire.DecodeRunRequested(payload)
	if err != nil {
		t.Fatal(err)
	}
	if parsed.Input.ReplyContext.CallbackReqID != "request-original" || parsed.Input.Key.EventID != "stable-message" {
		t.Fatalf("redelivery replaced immutable original context: %+v", parsed.Input)
	}
	event.BodyDigest = strings.Repeat("b", 64)
	if err = handler.Handle(ctx, event); !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("unknown provider-body mutation silently deduplicated: %v", err)
	}
	counts(t, pool, 1, 1, 1, 1)
}

func TestWeComChatlessNoticeCommitsOnlyGuardedReceipt(t *testing.T) {
	store, pool, routes := setupWeCom(t)
	owners, grant := acquireOwner(t, pool, 10*time.Second)
	service := application.New(store.WithConnectionGuard(owners), wecomRouteResolver{routes})
	handler, err := wecomadapter.NewHandler("account", "bot-1", domain.ConnectionFence{InstanceID: grant.InstanceID, Epoch: grant.Epoch, Revision: grant.Revision}, service)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		kind            wecom.EventKind
		eventType, want string
	}{{wecom.EventNotice, "enter_chat", "interaction"}, {wecom.EventUnsupported, "future_event", "ignore"}} {
		event := wecom.Event{Kind: tc.kind, EventType: tc.eventType, BodyDigest: strings.Repeat("a", 64), MessageID: tc.eventType, BotID: "bot-1", RequestID: "request-1", Generation: 1, SenderID: "100"}
		if err := handler.Handle(context.Background(), event); err != nil {
			t.Fatal(err)
		}
		receipt, _, found, err := store.Find(context.Background(), domain.EventKey{Provider: "wecom", AccountID: "account", EventID: event.MessageID})
		if err != nil || !found || receipt.Decision != tc.want {
			t.Fatalf("chatless receipt=%+v found=%v err=%v", receipt, found, err)
		}
	}
	counts(t, pool, 2, 0, 0, 2)
}
