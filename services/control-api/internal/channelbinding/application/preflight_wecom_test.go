package application

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	channelv1 "github.com/liuzengh/trpc-agent-service/api/schemas/channel/v1"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/channelbinding/domain"
)

func wecomPreflightSetup(t *testing.T) (*PreflightService, *preflightMemory, string, channelv1.PreflightCreateRequest) {
	t.Helper()
	base, accounts, access, _ := setup(t)
	secret := "WECOM_TEST_PRIVATE_SECRET"
	created, err := base.CreateAccount(context.Background(), owner, "wecom-create", CreateAccountInput{
		Provider: domain.WeCom, ProviderAccountID: "wecom-test-bot", Name: "WeCom",
		Credentials: map[string]domain.CredentialEdit{domain.WeComBotSecret: {Action: "replace", Value: &secret}},
	})
	if err != nil {
		t.Fatal(err)
	}
	m := &preflightMemory{now: base.deps.Now(), epoch: "11111111-1111-4111-8111-111111111111", accounts: accounts, records: map[string]PreflightRecord{}, receipts: map[string]PreflightRequest{}, requester: true, session: true, tenant: true}
	s, err := NewPreflightService(PreflightDependencies{Store: m, Access: access, Accounts: accounts, Cipher: base.deps.Cipher, ScopeID: base.deps.ScopeID, SourceEpoch: m.epoch, NewID: base.deps.NewID})
	if err != nil {
		t.Fatal(err)
	}
	return s, m, created.Account.ID, channelv1.PreflightCreateRequest{ExpectedAccountRevision: 1, ExpectedConnectionRevision: 1, ExpectedBotSecretVersion: 1, AllowConnectionProbe: true}
}

func wecomPrincipal() WorkloadPrincipal {
	p := preflightPrincipal()
	p.Consumers = []string{"wecom_preflight"}
	return p
}

func wecomClaim(t *testing.T, seq int) channelv1.PreflightClaimRequest {
	c := preflightInput(t, seq)
	c.DiagnosticPolicy, c.ExpectedPublicOrigin, c.OriginStatus = channelv1.PreflightWeComPolicy, nil, "PUBLIC_ORIGIN_NOT_APPLICABLE"
	c.GatewayConfigDigest, _ = channelv1.PreflightConfigDigestForPolicy(c.DiagnosticPolicy, c.ScopeID, c.SourceEpoch, nil, c.OriginStatus)
	return c
}

func wecomComplete(t *testing.T, c channelv1.PreflightClaimRequest, g *channelv1.PreflightGrant) channelv1.PreflightCompleteRequest {
	t.Helper()
	raw, err := os.ReadFile("../../../../../api/schemas/channel/v1/fixtures/preflight-wecom-complete-valid.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixture struct {
		Document channelv1.PreflightCompleteRequest `json:"document"`
	}
	if err := json.Unmarshal(raw, &fixture); err != nil {
		t.Fatal(err)
	}
	r := fixture.Document
	r.ScopeID, r.SourceEpoch, r.InstanceEpoch, r.ClaimToken = c.ScopeID, c.SourceEpoch, c.InstanceEpoch, c.ClaimToken
	r.GatewayConfigDigest, r.EffectiveConfigDigest, r.ConnectionRevision, r.LeaseEpoch = g.GatewayConfigDigest, g.EffectiveConfigDigest, g.ConnectionRevision, g.LeaseEpoch
	return r
}

func TestWeComPreflightRequiresConsentAndExactCredentialCAS(t *testing.T) {
	for _, tc := range []struct {
		name, code string
		change     func(*preflightMemory, string, *channelv1.PreflightCreateRequest)
	}{
		{"consent", "CHANNEL_PREFLIGHT_CONNECTION_PROBE_CONFIRMATION_REQUIRED", func(_ *preflightMemory, _ string, in *channelv1.PreflightCreateRequest) {
			in.AllowConnectionProbe = false
		}},
		{"credential version", domain.CredentialVersionConflict, func(_ *preflightMemory, _ string, in *channelv1.PreflightCreateRequest) {
			in.ExpectedBotSecretVersion++
		}},
		{"account version", domain.RevisionConflict, func(_ *preflightMemory, _ string, in *channelv1.PreflightCreateRequest) { in.ExpectedAccountRevision++ }},
		{"connection version", domain.RevisionConflict, func(_ *preflightMemory, _ string, in *channelv1.PreflightCreateRequest) {
			in.ExpectedConnectionRevision++
		}},
		{"enabled", domain.AccountMustBeDisabled, func(m *preflightMemory, id string, _ *channelv1.PreflightCreateRequest) {
			a := m.accounts.data.accounts[id]
			a.Account.Enabled = true
			m.accounts.data.accounts[id] = a
		}},
		{"wrong credential field", domain.InputInvalid, func(_ *preflightMemory, _ string, in *channelv1.PreflightCreateRequest) {
			in.ExpectedBotTokenVersion, in.ExpectedBotSecretVersion = 1, 0
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, m, id, in := wecomPreflightSetup(t)
			tc.change(m, id, &in)
			_, err := s.Create(context.Background(), owner, id, "probe", in)
			requirePFError(t, err, tc.code)
			if len(m.records) != 0 || len(m.receipts) != 0 {
				t.Fatal("rejected request created a probe")
			}
		})
	}
	s, m, id, in := wecomPreflightSetup(t)
	created := createPF(t, s, id, in)
	in.AllowConnectionProbe = false
	_, err := s.Create(context.Background(), owner, id, "create-preflight", in)
	if !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatal("consent absent from idempotency digest", err)
	}
	_, err = s.Create(context.Background(), owner, id, "fresh-without-consent", in)
	requirePFError(t, err, "CHANNEL_PREFLIGHT_CONNECTION_PROBE_CONFIRMATION_REQUIRED")
	if !m.records[created.PreflightID].View.AllowConnectionProbe {
		t.Fatal("consent not fixed in record")
	}
}

func TestWeComPreflightOwnConsumerResolveAndCompletion(t *testing.T) {
	s, m, id, in := wecomPreflightSetup(t)
	before := cloneState(m.accounts.data)
	created := createPF(t, s, id, in)
	c := wecomClaim(t, 1)
	if _, err := s.Claim(context.Background(), preflightPrincipal(), c); !errors.Is(err, ErrWorkloadDenied) {
		t.Fatal("Telegram consumer claimed WeCom", err)
	}
	if g, err := s.Claim(context.Background(), preflightPrincipal(), preflightInput(t, 2)); err != nil || g != nil {
		t.Fatal("Telegram policy selected WeCom", err)
	}
	g, err := s.Claim(context.Background(), wecomPrincipal(), c)
	if err != nil || g == nil {
		t.Fatal(err)
	}
	if !g.AllowConnectionProbe || g.Provider != "wecom" || g.ReceiveMode != channelv1.PreflightWeComMode || g.Credentials.Purpose != domain.WeComBotSecret || g.WebhookPath != "" || g.WebhookSecretConfigured {
		t.Fatal("grant isolation")
	}
	if err = preflightWire("preflight-grant.schema.json", g); err != nil {
		t.Fatal(err)
	}
	resolve := preflightResolveInput(c, g.LeaseEpoch)
	if _, err = s.Resolve(context.Background(), preflightPrincipal(), created.PreflightID, resolve); !errors.Is(err, ErrWorkloadDenied) {
		t.Fatal("cross-provider resolve", err)
	}
	value, err := s.Resolve(context.Background(), wecomPrincipal(), created.PreflightID, resolve)
	if err != nil || value.Purpose != domain.WeComBotSecret || value.Value != "WECOM_TEST_PRIVATE_SECRET" {
		t.Fatal("exact purpose resolve", err)
	}
	result := wecomComplete(t, c, g)
	if err := s.Complete(context.Background(), preflightPrincipal(), created.PreflightID, result); !errors.Is(err, ErrWorkloadDenied) {
		t.Fatal("cross-provider complete", err)
	}
	if err := s.Complete(context.Background(), wecomPrincipal(), created.PreflightID, result); err != nil {
		t.Fatal(err)
	}
	if err := s.Complete(context.Background(), wecomPrincipal(), created.PreflightID, result); err != nil {
		t.Fatal("completion replay", err)
	}
	view, err := s.Get(context.Background(), owner, id, created.PreflightID)
	if err != nil || view.State != "COMPLETED" || view.Outcome != "PASS" || view.BotSecretVersion != 1 || view.BotTokenVersion != 0 || view.ExpectedPublicOrigin != nil {
		t.Fatal("result", err)
	}
	raw, _ := json.Marshal(view)
	if strings.Contains(string(raw), "bot_token_version") || strings.Contains(string(raw), value.Value) {
		t.Fatal("incorrect public fields")
	}
	if !reflect.DeepEqual(before, m.accounts.data) {
		t.Fatal("probe changed Account, Binding, Route or Outbox")
	}
}

func TestWeComPreflightRevocationFencesCredentialsAndRetry(t *testing.T) {
	for _, action := range []string{"owner", "enabled", "rotation", "tenant", "expired"} {
		t.Run(action, func(t *testing.T) {
			s, m, id, in := wecomPreflightSetup(t)
			created := createPF(t, s, id, in)
			c := wecomClaim(t, 1)
			g, err := s.Claim(context.Background(), wecomPrincipal(), c)
			if err != nil || g == nil {
				t.Fatal(err)
			}
			a := m.accounts.data.accounts[id]
			switch action {
			case "owner":
				m.requester = false
			case "tenant":
				m.tenant = false
			case "enabled":
				a.Account.Enabled = true
			case "rotation":
				a.Credentials[0].Meta.Version++
				a.Account.ConnectionRevision++
				a.Account.Revision++
			case "expired":
				m.now = m.now.Add(31 * time.Second)
			}
			m.accounts.data.accounts[id] = a
			if value, err := s.Resolve(context.Background(), wecomPrincipal(), created.PreflightID, preflightResolveInput(c, g.LeaseEpoch)); err == nil || value.Value != "" {
				t.Fatal("stale grant resolved secret")
			}
			if action != "expired" {
				return
			}
			retry := wecomClaim(t, 2)
			next, err := s.Claim(context.Background(), wecomPrincipal(), retry)
			if err != nil || next == nil || next.LeaseEpoch != 2 {
				t.Fatal("bounded recovery claim", err)
			}
			if _, err := s.Resolve(context.Background(), wecomPrincipal(), created.PreflightID, preflightResolveInput(c, g.LeaseEpoch)); err == nil {
				t.Fatal("old lease accepted after reassign")
			}
			m.now = m.now.Add(31 * time.Second)
			if third, err := s.Claim(context.Background(), wecomPrincipal(), wecomClaim(t, 3)); err != nil || third != nil {
				t.Fatal("third subscription attempt authorized", err)
			}
		})
	}
}

func TestWeComMissingSecretAndRevokedCreateDoNotGainAuthority(t *testing.T) {
	for _, cause := range []string{"requester", "session", "tenant"} {
		t.Run(cause, func(t *testing.T) {
			s, m, id, in := wecomPreflightSetup(t)
			switch cause {
			case "requester":
				m.requester = false
			case "session":
				m.session = false
			case "tenant":
				m.tenant = false
			}
			if _, err := s.Create(context.Background(), owner, id, "revoked", in); !errors.Is(err, ErrPermissionDenied) {
				t.Fatal(err)
			}
			if len(m.records) != 0 || len(m.receipts) != 0 {
				t.Fatal("revoked creation wrote task")
			}
		})
	}
	s, m, id, in := wecomPreflightSetup(t)
	a := m.accounts.data.accounts[id]
	a.Credentials[0].Meta.Configured = false
	a.Credentials[0].Ciphertext = nil
	a.Credentials[0].KeyID = ""
	m.accounts.data.accounts[id] = a
	created := createPF(t, s, id, in)
	c := wecomClaim(t, 1)
	g, err := s.Claim(context.Background(), wecomPrincipal(), c)
	if err != nil || g == nil || g.Credentials.Configured {
		t.Fatal(err)
	}
	if value, err := s.Resolve(context.Background(), wecomPrincipal(), created.PreflightID, preflightResolveInput(c, g.LeaseEpoch)); err == nil || value.Value != "" {
		t.Fatal("missing secret resolved")
	}
	result := wecomComplete(t, c, g)
	result.Checks[0] = channelv1.PreflightCheck{ID: "credential_configuration", Status: "FAIL", Code: "BOT_SECRET_MISSING", Details: json.RawMessage(`{"bot_secret_configured":false}`)}
	result.Checks[1] = channelv1.PreflightCheck{ID: "connection_authentication", Status: "SKIPPED", Code: "NOT_EXECUTED", Details: json.RawMessage(`{"authenticated":null}`)}
	if err := s.Complete(context.Background(), wecomPrincipal(), created.PreflightID, result); err != nil {
		t.Fatal(err)
	}
	if m.records[created.PreflightID].View.Outcome != "FAIL" {
		t.Fatal("missing secret diagnostic outcome")
	}
}
