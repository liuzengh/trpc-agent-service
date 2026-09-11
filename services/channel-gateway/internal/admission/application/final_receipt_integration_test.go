package application_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	wire "github.com/liuzengh/trpc-agent-service/api/events/execution/v1"
	admissionpg "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/admission/adapter/outbound/postgres"
	app "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/admission/application"
	"github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/admission/domain"
	routepg "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/routing/adapter/outbound/postgres"
	routedomain "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/routing/domain"
	"github.com/liuzengh/trpc-agent-service/services/channel-gateway/migrations"
)

type finalReceiptResolver func(context.Context, string, string) (domain.RouteSnapshot, error)

func (f finalReceiptResolver) Resolve(ctx context.Context, p, a string) (domain.RouteSnapshot, error) {
	return f(ctx, p, a)
}

type observedLedger struct {
	store          *admissionpg.Store
	finds, commits atomic.Int32
}

func (l *observedLedger) Find(ctx context.Context, k domain.EventKey) (domain.Receipt, string, bool, error) {
	l.finds.Add(1)
	return l.store.Find(ctx, k)
}
func (l *observedLedger) Commit(ctx context.Context, c domain.Acceptance) (domain.Receipt, error) {
	l.commits.Add(1)
	return l.store.Commit(ctx, c)
}

func setupFinalReceiptPG(t *testing.T) (*pgxpool.Pool, *admissionpg.Store, *routepg.Store) {
	t.Helper()
	dsn := os.Getenv("GATEWAY_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set GATEWAY_TEST_DATABASE_URL to run real PostgreSQL final receipt tests")
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
	schema := pgx.Identifier{"gateway_final_receipt_" + hex.EncodeToString(random[:])}.Sanitize()
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
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	cfg.MaxConns = 8
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if err = migrations.Apply(ctx, pool); err != nil {
		t.Fatal(err)
	}
	routes := routepg.NewStore(pool)
	if err = routes.BeginReplay(ctx, routedomain.ReplaySource{StreamName: "CGR33_ROUTE", StreamID: "2026-09-05T00:00:00Z", FirstSequence: 1, LastSequence: 1, MessageCount: 1}); err != nil {
		t.Fatal(err)
	}
	if err = applyFinalReceiptRoute(ctx, routes, 1, true); err != nil {
		t.Fatal(err)
	}
	return pool, admissionpg.NewStore(pool, routes), routes
}
func applyFinalReceiptRoute(ctx context.Context, store *routepg.Store, generation int64, enabled bool) error {
	route := routedomain.RouteSnapshot{Provider: "telegram", AccountID: "account", Generation: generation}
	id := "disable"
	if enabled {
		id = "enable"
		route.TenantID = "tenant"
		route.BindingID = "binding"
		route.DeploymentRevisionID = "revision"
		route.ManifestRef = "manifest/revision"
		route.ManifestDigest = "sha256:" + strings.Repeat("b", 64)
	}
	return store.ApplyFromStream(ctx, routedomain.StreamPosition{StreamName: "CGR33_ROUTE", StreamID: "2026-09-05T00:00:00Z", Sequence: uint64(generation)}, routedomain.RouteEvent{SchemaVersion: 1, EventID: id, Enabled: enabled, Route: route})
}
func finalReceiptRouteBridge(routes *routepg.Store) finalReceiptResolver {
	return func(ctx context.Context, p, a string) (domain.RouteSnapshot, error) {
		route, err := routes.Resolve(ctx, p, a)
		return domain.RouteSnapshot{Provider: route.Provider, AccountID: route.AccountID, TenantID: route.TenantID, BindingID: route.BindingID, Generation: route.Generation, DeploymentRevisionID: route.DeploymentRevisionID, ManifestRef: route.ManifestRef, ManifestDigest: route.ManifestDigest}, err
	}
}
func finalReceiptInput() domain.Inbound {
	return domain.Inbound{Key: domain.EventKey{Provider: "telegram", AccountID: "account", EventID: "1001"}, Kind: "text", ConversationID: "100", SenderID: "100", Text: "original text", ReplyContext: json.RawMessage(`{"chat_id":"100","source_message_id":"42"}`), SourceDigest: strings.Repeat("a", 64), ReceivedAt: time.Now().UTC()}
}

func TestIntegrationFinalReceiptAfterWinnerAndRouteDisable(t *testing.T) {
	for _, conflict := range []bool{false, true} {
		name := "equivalent"
		if conflict {
			name = "different-digest"
		}
		t.Run(name, func(t *testing.T) {
			pool, store, routes := setupFinalReceiptPG(t)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			entered, resume := make(chan struct{}), make(chan struct{})
			bridge := finalReceiptRouteBridge(routes)
			loserLedger := &observedLedger{store: store}
			var routeFailed atomic.Bool
			loser := app.New(loserLedger, finalReceiptResolver(func(ctx context.Context, p, a string) (domain.RouteSnapshot, error) {
				close(entered)
				select {
				case <-resume:
				case <-ctx.Done():
					return domain.RouteSnapshot{}, ctx.Err()
				}
				route, err := bridge.Resolve(ctx, p, a)
				routeFailed.Store(err != nil)
				return route, err
			}))
			type outcome struct {
				receipt domain.Receipt
				err     error
			}
			done := make(chan outcome, 1)
			original := finalReceiptInput()
			go func() { r, err := loser.AcceptInbound(ctx, original); done <- outcome{r, err} }()
			select {
			case <-entered:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			winnerInput := original
			if conflict {
				winnerInput.Text = "different text"
				winnerInput.SourceDigest = strings.Repeat("c", 64)
			}
			winner, err := app.New(store, bridge).AcceptInbound(ctx, winnerInput)
			if err != nil {
				t.Fatal(err)
			}
			if err = applyFinalReceiptRoute(ctx, routes, 2, false); err != nil {
				t.Fatal(err)
			}
			close(resume)
			select {
			case got := <-done:
				if conflict {
					if !errors.Is(got.err, domain.ErrConflict) || got.receipt != (domain.Receipt{}) {
						t.Fatalf("conflicting replay: %+v %v", got.receipt, got.err)
					}
				} else if got.err != nil || got.receipt != winner {
					t.Fatalf("winner lost after route disabled: %+v %v; expected %+v", got.receipt, got.err, winner)
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			if !routeFailed.Load() {
				t.Fatal("fixture never reached the real disabled-route failure")
			}
			if loserLedger.finds.Load() != 2 || loserLedger.commits.Load() != 0 {
				t.Fatalf("A finds=%d commits=%d; must do one final read without second commit", loserLedger.finds.Load(), loserLedger.commits.Load())
			}
			var inbox, admissions, outbox, budget int
			if err = pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM gateway_inbox),(SELECT count(*) FROM gateway_admissions),(SELECT count(*) FROM gateway_outbox),(SELECT new_events FROM gateway_admission_budget)`).Scan(&inbox, &admissions, &outbox, &budget); err != nil {
				t.Fatal(err)
			}
			if inbox != 1 || admissions != 1 || outbox != 1 || budget != 1 {
				t.Fatalf("duplicate facts/budget: %d/%d/%d/%d", inbox, admissions, outbox, budget)
			}
			var payload []byte
			if err = pool.QueryRow(ctx, `SELECT payload FROM gateway_outbox`).Scan(&payload); err != nil {
				t.Fatal(err)
			}
			event, err := wire.DecodeRunRequested(payload)
			if err != nil {
				t.Fatal(err)
			}
			if event.AdmissionID != winner.AdmissionID || event.RunID != winner.RunID || event.Route.Generation != 1 || event.Input.Text != winnerInput.Text {
				t.Fatal("final lookup rewrote winner facts or fixed route")
			}
			t.Log("CGR33_VERIFIED: A first PG read misses; B commits; route generation 2 disables; A real Resolve fails; one new PG snapshot replays or conflicts; one Inbox/Admission/Outbox/budget")
		})
	}
}
