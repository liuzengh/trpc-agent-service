package bootstrap

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-telegram/bot"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	wire "github.com/liuzengh/trpc-agent-service/api/events/execution/v1"
	dto "github.com/liuzengh/trpc-agent-service/gen/events/execution/v1"
	admissionpg "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/admission/adapter/outbound/postgres"
	ad "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/admission/domain"
	eventadapter "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/delivery/adapter/inbound/eventadapter"
	deliverypg "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/delivery/adapter/outbound/postgres"
	telegramsender "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/delivery/adapter/outbound/telegram"
	app "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/delivery/application"
	d "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/delivery/domain"
	routepg "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/routing/adapter/outbound/postgres"
	rd "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/routing/domain"
	"github.com/liuzengh/trpc-agent-service/services/channel-gateway/migrations"
)

func deliveryDB(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("GATEWAY_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("dedicated real PostgreSQL required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)
	name := fmt.Sprintf("delivery_bridge_%d", time.Now().UnixNano())
	quoted := pgx.Identifier{name}.Sanitize()
	if _, err = admin.Exec(ctx, "CREATE SCHEMA "+quoted); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		c, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		if _, err := admin.Exec(c, "DROP SCHEMA "+quoted+" CASCADE"); err != nil {
			t.Error(err)
		}
	})
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	config.ConnConfig.RuntimeParams["search_path"] = name
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if err = migrations.Apply(ctx, pool); err != nil {
		t.Fatal(err)
	}
	return pool
}
func deliveryRoute(t *testing.T, pool *pgxpool.Pool, provider string) *routepg.Store {
	t.Helper()
	ctx := context.Background()
	store := routepg.NewStore(pool)
	if err := store.BeginReplay(ctx, rd.ReplaySource{StreamName: "DELIVERY_TEST", StreamID: "2026-09-05T00:00:00Z", FirstSequence: 1, LastSequence: 1, MessageCount: 1}); err != nil {
		t.Fatal(err)
	}
	route := rd.RouteSnapshot{Provider: provider, AccountID: "account", TenantID: "tenant", BindingID: "binding", Generation: 1, DeploymentRevisionID: "revision", ManifestRef: "manifest/revision", ManifestDigest: "sha256:" + strings.Repeat("a", 64)}
	if err := store.ApplyFromStream(ctx, rd.StreamPosition{StreamName: "DELIVERY_TEST", StreamID: "2026-09-05T00:00:00Z", Sequence: 1}, rd.RouteEvent{EventID: "route-1", SchemaVersion: 1, Enabled: true, Route: route}); err != nil {
		t.Fatal(err)
	}
	return store
}

// This fixture stands for an immutable Execution-owned committed Final record.
// It is deliberately not registered in the production bootstrap or called a Worker.
type committedFinalFixture struct {
	proof       app.FinalAuthorization
	unavailable bool
}

func (f *committedFinalFixture) VerifyCommittedFinal(context.Context, d.Intent, string) (app.FinalAuthorization, error) {
	if f.unavailable {
		return app.FinalAuthorization{}, d.ErrUnavailable
	}
	return f.proof, nil
}
func finalWire(t *testing.T, i d.Intent) []byte {
	t.Helper()
	raw, err := wire.EncodeReplyIntent(dto.ReplyIntent{SchemaVersion: 1, IntentID: i.ID, AdmissionID: i.AdmissionID, RunID: i.RunID, Execution: dto.ReplyExecution{AttemptID: i.AttemptID, Generation: i.ExecutionGeneration, CompletionID: i.CompletionID}, Sequence: i.Sequence, Kind: "final", Content: dto.FinalTextContent{Type: "text", Text: i.Text}, Deadline: i.Deadline.UTC().Format(time.RFC3339Nano)})
	if err != nil {
		t.Fatal(err)
	}
	return raw
}
func fixtureProof(t *testing.T, i d.Intent, target d.Target) *committedFinalFixture {
	t.Helper()
	digest, err := d.IntentDigest(i)
	if err != nil {
		t.Fatal(err)
	}
	return &committedFinalFixture{proof: app.FinalAuthorization{IntentID: i.ID, Digest: digest, AdmissionID: i.AdmissionID, RunID: i.RunID, AttemptID: i.AttemptID, CompletionID: i.CompletionID, ExecutionGeneration: i.ExecutionGeneration, Sequence: i.Sequence, TenantID: target.TenantID, ManifestDigest: target.ManifestDigest}}
}
func TestTelegramDeliveryRealPGHTTPOrderedFinal(t *testing.T) { testTelegramDelivery(t, false) }
func TestTelegramRunnerRealPGHTTPOrderedFinal(t *testing.T)   { testTelegramDelivery(t, true) }
func testTelegramDelivery(t *testing.T, automatic bool) {
	for _, unknownSecond := range []bool{false, true} {
		t.Run(fmt.Sprint("unknown-second=", unknownSecond), func(t *testing.T) {
			pool := deliveryDB(t)
			routes := deliveryRoute(t, pool, "telegram")
			admissions := admissionpg.NewStore(pool, routes)
			ctx := context.Background()
			received := time.Now().UTC()
			c := ad.Acceptance{Input: ad.Inbound{Key: ad.EventKey{Provider: "telegram", AccountID: "account", EventID: "1"}, Kind: "text", ConversationID: "123", ThreadID: "9", SenderID: "100", Text: "input", ReplyContext: json.RawMessage(`{"chat_id":"123","message_thread_id":"9","source_message_id":"7"}`), SourceDigest: strings.Repeat("b", 64), ReceivedAt: received}, Receipt: ad.Receipt{Decision: "admit-run", AdmissionID: "admission", RunID: "run"}, Route: &ad.RouteSnapshot{Provider: "telegram", AccountID: "account", TenantID: "tenant", BindingID: "binding", Generation: 1, DeploymentRevisionID: "revision", ManifestRef: "manifest/revision", ManifestDigest: "sha256:" + strings.Repeat("a", 64)}}
			if _, err := admissions.Commit(ctx, c); err != nil {
				t.Fatal(err)
			}
			reader := admissionDeliveryReader{admissions}
			target, err := reader.ReadReplyTarget(ctx, "admission", "run")
			if err != nil {
				t.Fatal(err)
			}
			i := d.Intent{ID: "final", AdmissionID: "admission", RunID: "run", AttemptID: "execution-1", CompletionID: "completion-1", ExecutionGeneration: 1, Sequence: 1, Text: strings.Repeat("x", 8192) + "last", Deadline: time.Now().UTC().Add(time.Minute)}
			ledger, err := deliverypg.NewStore(pool, nil, deliverypg.Options{})
			if err != nil {
				t.Fatal(err)
			}
			proof := fixtureProof(t, i, target)
			acceptor, err := app.NewAcceptor(ledger, reader, proof, app.AcceptOptions{})
			if err != nil {
				t.Fatal(err)
			}
			handler, _ := eventadapter.NewHandler(acceptor)
			raw := finalWire(t, i)
			receipt, err := handler.Handle(ctx, raw)
			if err != nil || receipt.PartCount != 3 {
				t.Fatalf("accept: %+v %v", receipt, err)
			}
			// A duplicate remains replayable after the dynamic Execution owner disappears.
			proof.unavailable = true
			if _, err = handler.Handle(ctx, raw); err != nil {
				t.Fatal(err)
			}
			var calls atomic.Int32
			bad := make(chan string, 8)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				n := calls.Add(1)
				if err := r.ParseMultipartForm(1 << 20); err != nil {
					bad <- "form"
				}
				if r.FormValue("chat_id") != "123" || r.FormValue("message_thread_id") != "9" {
					bad <- "recipient"
				}
				var reply map[string]any
				if json.Unmarshal([]byte(r.FormValue("reply_parameters")), &reply) != nil || reply["message_id"] != float64(7) {
					bad <- "reply context"
				}
				expected := strings.Repeat("x", 4096)
				if n == 3 {
					expected = "last"
				}
				if r.FormValue("text") != expected {
					bad <- "part"
				}
				if unknownSecond && n == 2 {
					w.WriteHeader(200)
					_, _ = w.Write([]byte("broken-json"))
					return
				}
				w.Header().Set("Content-Type", "application/json")
				fmt.Fprintf(w, `{"ok":true,"result":{"message_id":%d,"date":1,"message_thread_id":9,"chat":{"id":123,"type":"private"},"text":"accepted"}}`, n)
			}))
			defer server.Close()
			b, err := bot.New("123:synthetic-fixture", bot.WithSkipGetMe(), bot.WithServerURL(server.URL), bot.WithHTTPClient(time.Second, server.Client()))
			if err != nil {
				t.Fatal(err)
			}
			senders, err := telegramsender.NewProvider(map[string]*bot.Bot{"account": b})
			if err != nil {
				t.Fatal(err)
			}
			dispatcher, err := app.NewDispatcher(ledger, senders, app.DispatchOptions{})
			if err != nil {
				t.Fatal(err)
			}
			request := d.ClaimRequest{Provider: "telegram", AccountID: "account", InstanceID: "gateway", Limit: 8, Lease: 5 * time.Second}
			if automatic {
				stopRunner := startDeliveryRunnerFixture(t, ctx, ledger, dispatcher, telegramRunnerFixtureEligibility{}, "gateway", "telegram")
				eventually(t, func() bool {
					state, e := ledger.Get(ctx, i.ID)
					if e != nil || len(state.Parts) != 3 {
						return false
					}
					if unknownSecond {
						return state.Parts[1].State == d.Unknown
					}
					return state.Parts[2].State == d.Accepted
				}, "Runner completed ordered HTTP Final or retained UNKNOWN barrier")
				stopRunner()
			} else {
				for step := 0; step < 5; step++ {
					if _, err = dispatcher.DispatchAccount(ctx, request); err != nil {
						t.Fatal(err)
					}
				}
			}
			state, err := ledger.Get(ctx, i.ID)
			if err != nil {
				t.Fatal(err)
			}
			if state.Parts[0].State != d.Accepted {
				t.Fatal(state)
			}
			if unknownSecond {
				if calls.Load() != 2 || state.Parts[1].State != d.Unknown || state.Parts[2].State != d.Pending {
					t.Fatalf("UNKNOWN retried or unblocked later part: %d %+v", calls.Load(), state)
				}
			} else {
				if calls.Load() != 3 || state.Parts[1].State != d.Accepted || state.Parts[2].State != d.Accepted {
					t.Fatal(state)
				}
			}
			select {
			case failure := <-bad:
				t.Fatal(failure)
			default:
			}
			// An accepted Run cannot acquire a second Final barrier with a new intent ID.
			second := i
			second.ID = "other-final"
			proof.unavailable = false
			proof.proof = fixtureProof(t, second, target).proof
			if _, err = handler.Handle(ctx, finalWire(t, second)); !errors.Is(err, d.ErrConflict) {
				t.Fatalf("second Final accepted: %v", err)
			}
			t.Logf("AUTOMATIC_RUNNER=%t", automatic)
			t.Log("TELEGRAM_DELIVERY_VERIFIED: strict ReplyIntent; original Admission target; committed-proof fixture; real PG A1/A2/observations; direct SDK HTTP; ordered parts; UNKNOWN blocks later parts; no resend of ACCEPTED; one Final per Run")
		})
	}
}
