package bootstrap

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/jackc/pgx/v5/pgxpool"
	adpg "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/admission/adapter/outbound/postgres"
	adapp "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/admission/application"
	ad "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/admission/domain"
	catpg "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/adapter/outbound/catalogpostgres"
	regpg "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/adapter/outbound/registrationpostgres"
	c "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/domain/accountcatalog"
	dp "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/delivery/adapter/outbound/postgres"
	d "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/delivery/domain"
	"strings"
	"testing"
	"time"
)

const controlEpoch = "00000000-0000-4000-8000-000000000001"
const controlBoot = "00000000-0000-4000-8000-000000000002"

func controlSnapshot() c.Snapshot {
	s := c.Snapshot{SchemaVersion: 1, ScopeID: "pool", SourceEpoch: controlEpoch, Revision: 1, Complete: true, Accounts: []c.Account{{ID: "account", TenantID: "tenant", Provider: "telegram", ProviderAccountID: "123", Revision: 1, ConnectionRevision: 1, Enabled: true, MinRouteGeneration: 1, Config: c.Config{WebhookPath: "/v1/telegram/account"}, Credentials: []c.Credential{{Purpose: "telegram.bot_token", ID: "token", Version: 1, Configured: true}, {Purpose: "telegram.webhook_secret", ID: "webhook", Version: 1, Configured: true}}}}}
	s.Digest, _ = s.ComputedDigest()
	return s
}
func catalogFixture(t *testing.T, pool *pgxpool.Pool) (*catpg.Store, c.Snapshot, c.Poll, c.Qualification) {
	t.Helper()
	store, e := catpg.New(pool, "pool", controlEpoch, "gw", controlBoot)
	if e != nil {
		t.Fatal(e)
	}
	s := controlSnapshot()
	p, q := applyControl(t, store, s)
	return store, s, p, q
}
func applyControl(t *testing.T, store *catpg.Store, s c.Snapshot) (c.Poll, c.Qualification) {
	t.Helper()
	s.Digest, _ = s.ComputedDigest()
	p, e := store.BeginPoll(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	_, q, e := store.Apply(context.Background(), p, s)
	if e != nil {
		t.Fatal(e)
	}
	return p, q
}
func controlPermit(t *testing.T, s *catpg.Store, p c.Poll, q c.Qualification, a c.Account, kind string, generation int64) *c.Permit {
	t.Helper()
	permit, e := s.Issue(context.Background(), p, q, a, kind, generation)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(permit.Revoke)
	return permit
}
func admissionFixture(id string) ad.Acceptance {
	return ad.Acceptance{Input: ad.Inbound{Key: ad.EventKey{Provider: "telegram", AccountID: "account", EventID: id}, Kind: "text", ConversationID: "123", SenderID: "100", Text: "input", ReplyContext: json.RawMessage(`{"chat_id":"123","source_message_id":"7"}`), SourceDigest: strings.Repeat("b", 64), ReceivedAt: time.Now().UTC()}, Receipt: ad.Receipt{Decision: "admit-run", AdmissionID: "admission-" + id, RunID: "run-" + id}, Route: &ad.RouteSnapshot{Provider: "telegram", AccountID: "account", TenantID: "tenant", BindingID: "binding", Generation: 1, DeploymentRevisionID: "revision", ManifestRef: "manifest/revision", ManifestDigest: "sha256:" + strings.Repeat("a", 64)}}
}

type fixedControlRoute struct{}

func (fixedControlRoute) Resolve(context.Context, string, string) (ad.RouteSnapshot, error) {
	return *admissionFixture("1").Route, nil
}
func TestControlAdmissionActualGuardReceiptFloorAndTenant(t *testing.T) {
	pool := deliveryDB(t)
	routes := deliveryRoute(t, pool, "telegram")
	catalog, s, p, q := catalogFixture(t, pool)
	ledger := adpg.NewStore(pool, routes).WithAccountUseGuard(accountGuardBridge{catalog, true})
	permit := controlPermit(t, catalog, p, q, s.Accounts[0], "telegram_webhook", 1)
	ctx := context.WithValue(context.Background(), permitKey{}, permit)
	first := admissionFixture("1")
	if _, e := ledger.Commit(ctx, first); e != nil {
		t.Fatal(e)
	}
	// Cross-tenant route cannot use this account even if generation is current.
	wrong := admissionFixture("2")
	wrong.Route.TenantID = "another"
	if _, e := ledger.Commit(ctx, wrong); !errors.Is(e, ad.ErrAccountUnavailable) {
		t.Fatalf("tenant gate: %v", e)
	}
	s.Revision++
	s.Accounts[0].MinRouteGeneration = 2
	p, q = applyControl(t, catalog, s)
	permit2 := controlPermit(t, catalog, p, q, s.Accounts[0], "telegram_webhook", 1)
	ctx2 := context.WithValue(context.Background(), permitKey{}, permit2)
	if _, e := ledger.Commit(ctx2, admissionFixture("3")); !errors.Is(e, ad.ErrAccountUnavailable) {
		t.Fatalf("floor gate: %v", e)
	}
	s.Revision++
	s.Accounts[0].Revision++
	s.Accounts[0].ConnectionRevision++
	s.Accounts[0].Enabled = false
	applyControl(t, catalog, s)
	if _, e := ledger.Commit(ctx, admissionFixture("4")); !errors.Is(e, ad.ErrAccountUnavailable) {
		t.Fatalf("disable gate: %v", e)
	}
	// Receipt replay is intentionally independent of the expired local grant.
	permit.Revoke()
	service := adapp.New(ledger, fixedControlRoute{})
	got, e := service.AcceptInbound(ctx, first.Input)
	if e != nil || got != first.Receipt {
		t.Fatalf("receipt replay: %+v %v", got, e)
	}
	var runs int
	if e = pool.QueryRow(context.Background(), `SELECT count(*) FROM gateway_outbox`).Scan(&runs); e != nil || runs != 1 {
		t.Fatalf("runs=%d err=%v", runs, e)
	}
}
func TestControlDeliveryActualA1A2AndEvidenceAfterDisable(t *testing.T) {
	pool := deliveryDB(t)
	catalog, s, p, q := catalogFixture(t, pool)
	ledger, e := dp.NewStore(pool, nil, dp.Options{})
	if e != nil {
		t.Fatal(e)
	}
	ledger = ledger.WithAccountUseGuard(accountGuardBridge{store: catalog})
	for _, id := range []string{"first", "second"} {
		i := d.Intent{ID: id, AdmissionID: "admission", RunID: "run-" + id, AttemptID: "exec", CompletionID: "complete", ExecutionGeneration: 1, Sequence: 1, Text: "reply", Deadline: time.Now().Add(time.Minute)}
		digest, _ := d.IntentDigest(i)
		target := d.Target{Provider: "telegram", AccountID: "account", TenantID: "tenant", ManifestDigest: "sha256:" + strings.Repeat("a", 64), ConversationID: "123", SourceMessageID: "7", SourceEventID: "1", ReceivedAt: time.Now()}
		_, e = ledger.Accept(context.Background(), d.Prepared{Intent: i, Digest: digest, Target: target, Parts: []string{"reply"}})
		if e != nil {
			t.Fatal(e)
		}
	}
	permit := controlPermit(t, catalog, p, q, s.Accounts[0], "telegram_delivery", 11)
	ctx := context.WithValue(context.Background(), permitKey{}, permit)
	claims, e := ledger.ClaimDue(ctx, d.ClaimRequest{Provider: "telegram", AccountID: "account", InstanceID: "gw", Limit: 2, Lease: 15 * time.Second})
	if e != nil || len(claims) != 2 {
		t.Fatalf("claim=%d %v", len(claims), e)
	}
	if claims[0].UseBinding != bindingDigest(permit) {
		t.Fatal("A1 missing binding")
	}
	call := func(ctx context.Context, claim d.Claim) (d.Attempt, error) {
		digest, _ := d.RequestDigest(claim)
		return ledger.MarkCalling(ctx, d.CallingRequest{Claim: claim, RequestID: "request-" + claim.Intent.ID, RequestDigest: digest, Timeout: 10 * time.Second})
	}
	other := controlPermit(t, catalog, p, q, s.Accounts[0], "telegram_delivery", 12)
	if _, e = call(context.WithValue(context.Background(), permitKey{}, other), claims[0]); !errors.Is(e, d.ErrUnauthorized) {
		t.Fatalf("client generation swap: %v", e)
	}
	attempt, e := call(ctx, claims[0])
	if e != nil || attempt.UseBinding != claims[0].UseBinding {
		t.Fatalf("A2: %v", e)
	}
	s.Revision++
	s.Accounts[0].Revision++
	s.Accounts[0].ConnectionRevision++
	s.Accounts[0].Enabled = false
	applyControl(t, catalog, s)
	permit.Revoke()
	if _, e = call(ctx, claims[1]); !errors.Is(e, d.ErrUnauthorized) {
		t.Fatalf("A2 after disable: %v", e)
	}
	result := d.Result{Certainty: d.CertaintyAccepted, ProviderMessageID: "42"}
	obs := d.Observation{ID: "observation", AttemptID: attempt.ID, EvidenceToken: attempt.EvidenceToken, ProviderRequestID: attempt.RequestID, RequestDigest: attempt.RequestDigest, Result: result}
	if e = ledger.Observe(context.Background(), obs); e != nil {
		t.Fatal(e)
	}
	if e = ledger.Finish(context.Background(), attempt, result); e != nil {
		t.Fatal(e)
	}
	got, e := ledger.Get(context.Background(), claims[0].Intent.ID)
	if e != nil || got.Parts[0].State != d.Accepted {
		t.Fatalf("settlement: %+v %v", got, e)
	}
}
func TestControlRegistrationFenceBeginAndLateOriginalFact(t *testing.T) {
	pool := deliveryDB(t)
	catalog, s, p, q := catalogFixture(t, pool)
	reg := regpg.New(pool, catalog)
	permit := controlPermit(t, catalog, p, q, s.Accounts[0], "telegram_registration", 1)
	ctx := context.Background()
	op, ok, e := reg.Acquire(ctx, permit)
	if e != nil || !ok {
		t.Fatalf("acquire %v", e)
	}
	if _, ok, e = reg.Acquire(ctx, permit); e != nil || ok {
		t.Fatalf("parallel acquire %v %v", ok, e)
	}
	if e = reg.BeginCall(ctx, permit, op); e != nil {
		t.Fatal(e)
	}
	if e = reg.BeginCall(ctx, permit, op); e == nil {
		t.Fatal("BEGIN_CALL reused")
	}
	s.Revision++
	s.Accounts[0].Revision++
	s.Accounts[0].ConnectionRevision++
	s.Accounts[0].Enabled = false
	applyControl(t, catalog, s)
	if e = reg.Finish(ctx, permit, op, "READY"); e == nil {
		t.Fatal("old ACK promoted revoked account")
	}
	var result, state string
	if e = pool.QueryRow(ctx, `SELECT result FROM gateway_telegram_registration_attempts WHERE operation_id=$1`, op.ID).Scan(&result); e != nil || result != "READY" {
		t.Fatalf("late original fact=%q %v", result, e)
	}
	if e = pool.QueryRow(ctx, `SELECT state FROM gateway_telegram_registrations WHERE account_id='account'`).Scan(&state); e != nil || state == "READY" {
		t.Fatalf("current state=%q %v", state, e)
	}
}
