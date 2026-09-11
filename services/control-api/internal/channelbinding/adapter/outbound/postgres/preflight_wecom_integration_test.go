package postgresadapter

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5/pgconn"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	channelv1 "github.com/liuzengh/trpc-agent-service/api/schemas/channel/v1"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/channelbinding/adapter/outbound/credentialcrypto"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/channelbinding/application"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/channelbinding/domain"
	"github.com/liuzengh/trpc-agent-service/services/control-api/migrations"
)

func TestWeComPreflightConsentClaimsAndPersistenceAgainstPostgreSQL(t *testing.T) {
	store, accounts, pool := preflightPG(t)
	ctx := context.Background()
	upgrade, err := migrations.Files.ReadFile("0004_wecom_preflights.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, string(upgrade)); err != nil {
		t.Fatal("WeCom upgrade", err)
	}
	secret := "WECOM_PG_PRIVATE_CANARY"
	a, err := accounts.CreateAccount(ctx, testActor, "wecom-pg-account", application.CreateAccountInput{Provider: domain.WeCom, ProviderAccountID: "wecom-pg-bot", Name: "WeCom", Credentials: map[string]domain.CredentialEdit{domain.WeComBotSecret: {Action: "replace", Value: &secret}}})
	if err != nil {
		t.Fatal(err)
	}
	cipher, err := credentialcrypto.New("k1", map[string]credentialcrypto.Key{"k1": {Encryption: bytes.Repeat([]byte{1}, 32), MAC: bytes.Repeat([]byte{2}, 32)}})
	if err != nil {
		t.Fatal(err)
	}
	var seq atomic.Int64
	s, err := application.NewPreflightService(application.PreflightDependencies{Store: store, Access: allowOwner{}, Accounts: store.base, Cipher: cipher, ScopeID: testScope, SourceEpoch: testEpoch, NewID: func(prefix string) (string, error) { return fmt.Sprintf("%s_wecom_%d", prefix, seq.Add(1)), nil }})
	if err != nil {
		t.Fatal(err)
	}
	operational := func() string {
		t.Helper()
		var state string
		if err := pool.QueryRow(ctx, `SELECT json_build_object('account',(SELECT row_to_json(a) FROM channel_accounts a WHERE id=$1),'catalog',(SELECT row_to_json(c) FROM channel_account_catalog c WHERE scope_id=$2),'outbox',(SELECT count(*) FROM control_outbox))::text`, a.Account.ID, testScope).Scan(&state); err != nil {
			t.Fatal(err)
		}
		return state
	}
	before := operational()
	in := channelv1.PreflightCreateRequest{ExpectedAccountRevision: 1, ExpectedConnectionRevision: 1, ExpectedBotSecretVersion: 1}
	if _, err := s.Create(ctx, testActor, a.Account.ID, "wecom-probe", in); err == nil || err.Error() != "CHANNEL_PREFLIGHT_CONNECTION_PROBE_CONFIRMATION_REQUIRED" {
		t.Fatal("consent", err)
	}
	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM channel_preflights`).Scan(&count); err != nil || count != 0 {
		t.Fatal("unconfirmed write", count, err)
	}
	in.AllowConnectionProbe = true
	created, err := s.Create(ctx, testActor, a.Account.ID, "wecom-probe", in)
	if err != nil {
		t.Fatal(err)
	}
	replay, err := s.Create(ctx, testActor, a.Account.ID, "wecom-probe", in)
	if err != nil || replay.PreflightID != created.PreflightID {
		t.Fatal("create replay", err)
	}
	for _, policy := range []string{"", channelv1.PreflightReceiveModesPolicy} {
		if _, found, err := store.Candidate(ctx, testScope, policy); err != nil || found {
			t.Fatal("Telegram selected WeCom", err)
		}
	}
	if _, found, err := store.Candidate(ctx, testScope, channelv1.PreflightWeComPolicy); err != nil || !found {
		t.Fatal("WeCom candidate", err)
	}
	p := application.WorkloadPrincipal{PrincipalID: "spiffe://test/gateway", ScopeID: testScope, InstanceID: "wecom-gateway", Audience: application.WorkloadAudience, Consumers: []string{"wecom_preflight"}}
	digest, _ := channelv1.PreflightConfigDigestForPolicy(channelv1.PreflightWeComPolicy, testScope, testEpoch, nil, "PUBLIC_ORIGIN_NOT_APPLICABLE")
	claims := make([]channelv1.PreflightClaimRequest, 2)
	grants := make([]*channelv1.PreflightGrant, 2)
	errs := make([]error, 2)
	var wg sync.WaitGroup
	for i := range claims {
		claims[i] = channelv1.PreflightClaimRequest{DiagnosticPolicy: channelv1.PreflightWeComPolicy, SchemaVersion: 1, ScopeID: testScope, SourceEpoch: testEpoch, InstanceEpoch: "22222222-2222-4222-8222-222222222222", ClaimRequestID: fmt.Sprintf("33333333-3333-4333-8333-%012d", i+1), ClaimToken: strings.Repeat("A", 43), GatewayConfigDigest: digest, OriginStatus: "PUBLIC_ORIGIN_NOT_APPLICABLE", Limit: 1}
		wg.Go(func() { grants[i], errs[i] = s.Claim(ctx, p, claims[i]) })
	}
	wg.Wait()
	winner := -1
	for i, grant := range grants {
		if errs[i] != nil {
			t.Fatal(errs[i])
		}
		if grant != nil {
			if winner != -1 {
				t.Fatal("two live claims")
			}
			winner = i
		}
	}
	if winner < 0 {
		t.Fatal("no live claim")
	}
	c, g := claims[winner], grants[winner]
	r := channelv1.PreflightResolveRequest{SchemaVersion: 1, ScopeID: testScope, SourceEpoch: testEpoch, InstanceEpoch: c.InstanceEpoch, LeaseEpoch: g.LeaseEpoch, ClaimToken: c.ClaimToken}
	telegram := p
	telegram.Consumers = []string{"telegram_preflight"}
	if _, err := s.Resolve(ctx, telegram, created.PreflightID, r); !errors.Is(err, application.ErrWorkloadDenied) {
		t.Fatal("Telegram read WeCom secret", err)
	}
	value, err := s.Resolve(ctx, p, created.PreflightID, r)
	if err != nil || value.Purpose != domain.WeComBotSecret || value.Value != secret {
		t.Fatal("exact resolve", err)
	}
	checks := []channelv1.PreflightCheck{
		{ID: "credential_configuration", Status: "PASS", Code: "CREDENTIALS_CONFIGURED", Details: json.RawMessage(`{"bot_secret_configured":true}`)},
		{ID: "connection_authentication", Status: "PASS", Code: "WECOM_AUTHENTICATED", Details: json.RawMessage(`{"authenticated":true}`)},
		{ID: "delivery_verification", Status: "UNKNOWN", Code: "DELIVERY_NOT_TESTED", Details: json.RawMessage(`{"verification":"NOT_TESTED"}`)},
	}
	complete := channelv1.PreflightCompleteRequest{DiagnosticPolicy: channelv1.PreflightWeComPolicy, ReceiveMode: channelv1.PreflightWeComMode, EffectiveConfigDigest: g.EffectiveConfigDigest, ConnectionRevision: g.ConnectionRevision, OriginStatus: c.OriginStatus, SchemaVersion: 1, ScopeID: testScope, SourceEpoch: testEpoch, InstanceEpoch: c.InstanceEpoch, LeaseEpoch: g.LeaseEpoch, ClaimToken: c.ClaimToken, GatewayConfigDigest: digest, ObservedAt: time.Now().UTC(), Checks: checks}
	if err := s.Complete(ctx, p, created.PreflightID, complete); err != nil {
		t.Fatal(err)
	}
	if err := s.Complete(ctx, p, created.PreflightID, complete); err != nil {
		t.Fatal("complete replay", err)
	}
	view, err := s.Get(ctx, testActor, a.Account.ID, created.PreflightID)
	if err != nil || view.State != "COMPLETED" || view.Outcome != "PASS" || view.BotSecretVersion != 1 {
		t.Fatal("durable read", err)
	}
	var raw string
	if err := pool.QueryRow(ctx, `SELECT record_jsonb::text FROM channel_preflights WHERE id=$1`, created.PreflightID).Scan(&raw); err != nil || strings.Contains(raw, secret) || strings.Contains(raw, c.ClaimToken) {
		t.Fatal("secret/claim token persisted", err)
	}
	for _, mutation := range []string{
		"record_jsonb #- '{view,provider}'",
		"record_jsonb #- '{view,allow_connection_probe}'",
		"jsonb_set(record_jsonb,'{view,allow_connection_probe}','false'::jsonb)",
		"jsonb_set(record_jsonb,'{view,provider}','\"telegram\"'::jsonb)",
		"jsonb_set(record_jsonb,'{view,diagnostic_policy}','\"telegram-receive-modes-v1\"'::jsonb)",
		"record_jsonb #- '{view,receive_mode}'",
	} {
		_, err := pool.Exec(ctx, "UPDATE channel_preflights SET record_jsonb="+mutation+" WHERE id=$1", created.PreflightID)
		var pgerr *pgconn.PgError
		if !errors.As(err, &pgerr) || pgerr.Code != "23514" {
			t.Fatal("completed result SQL guard accepted missing/cross-provider metadata", err)
		}
	}
	if operational() != before {
		t.Fatal("probe mutated operational state")
	}
	// The persisted owner confirmation and exact secret version cannot be edited
	// through an active or historical Save transition.
	if err := store.WithTransaction(ctx, "task:"+created.PreflightID, func(tx application.PreflightTransaction) error {
		if _, err := tx.LockAccount(ctx, testActor.TenantID, a.Account.ID, testActor.UserID, false); err != nil {
			return err
		}
		stored, _, err := tx.Load(ctx, created.PreflightID)
		if err != nil {
			return err
		}
		stored.View.AllowConnectionProbe = false
		return tx.Save(ctx, stored)
	}); err == nil {
		t.Fatal("consent rewritten")
	}
	t.Log("WECOM_CONTROL_PG=consent+CAS+provider-policy+single-live-claim+exact-purpose-resolve+immutable-completion+no-operational-writes")
}

func TestWeComMigrationPreservesHistoricalTelegramResult(t *testing.T) {
	store, accounts, pool := preflightPG(t)
	account := createAccount(t, accounts)
	r := insertPreflight(t, store, account.Account.ID, "cpf_old_telegram", 0)
	modifyStoredPreflight(t, store, r, markPreflightRunning)
	raw, err := os.ReadFile("../../../../../../../api/schemas/channel/v1/fixtures/preflight-complete-valid.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixture struct {
		Document channelv1.PreflightCompleteRequest `json:"document"`
	}
	if err := json.Unmarshal(raw, &fixture); err != nil {
		t.Fatal(err)
	}
	modifyStoredPreflight(t, store, r, func(r *application.PreflightRecord) {
		checked := r.View.StartedAt.Add(time.Second)
		expires := checked.Add(5 * time.Minute)
		r.View.State = "COMPLETED"
		r.View.ReasonCode = "CHANNEL_PREFLIGHT_COMPLETED"
		r.View.Outcome = "WARN"
		r.View.Freshness = "CURRENT"
		r.View.CheckedAt = &checked
		r.View.ExpiresAt = &expires
		r.View.Checks = fixture.Document.Checks
		r.CompleteDigest = "sha256:" + strings.Repeat("e", 64)
		r.LastCheckedAt = checked
	})
	ctx := context.Background()
	var before, after string
	if err := pool.QueryRow(ctx, "SELECT record_jsonb::text FROM channel_preflights WHERE id=$1", r.View.PreflightID).Scan(&before); err != nil {
		t.Fatal(err)
	}
	upgrade, err := migrations.Files.ReadFile("0004_wecom_preflights.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, string(upgrade)); err != nil {
		t.Fatal("upgrade with historical eight-check row", err)
	}
	if err := pool.QueryRow(ctx, "SELECT record_jsonb::text FROM channel_preflights WHERE id=$1", r.View.PreflightID).Scan(&after); err != nil || before != after {
		t.Fatal("historical result changed", err)
	}
	err = store.WithTransaction(ctx, "task:"+r.View.PreflightID, func(tx application.PreflightTransaction) error {
		if _, err := tx.LockAccount(ctx, r.View.TenantID, r.View.AccountID, r.View.RequestedBy, false); err != nil {
			return err
		}
		got, found, err := tx.Load(ctx, r.View.PreflightID)
		if err != nil {
			return err
		}
		if !found || len(got.View.Checks) != 8 || got.View.DiagnosticPolicy != "" {
			return errors.New("legacy result no longer readable")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
