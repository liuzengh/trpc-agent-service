package application

import (
	"context"
	"encoding/json"
	"os"
	"reflect"
	"testing"
	"time"

	channelv1 "github.com/liuzengh/trpc-agent-service/api/schemas/channel/v1"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/channelbinding/domain"
)

func pollingPreflightSetup(t *testing.T) (*PreflightService, *preflightMemory, string, channelv1.PreflightCreateRequest) {
	t.Helper()
	s, m, id, in := preflightSetup(t)
	a := m.accounts.data.accounts[id]
	a.Account.Config.ReceiveMode = domain.LongPolling
	for i := range a.Credentials {
		if a.Credentials[i].Meta.Purpose == domain.TelegramWebhookSecret {
			a.Credentials[i].Meta.Configured = false
			a.Credentials[i].Ciphertext = nil
			a.Credentials[i].KeyID = ""
		}
	}
	m.accounts.data.accounts[id] = a
	return s, m, id, in
}
func pollingComplete(t *testing.T, c channelv1.PreflightClaimRequest, g *channelv1.PreflightGrant) channelv1.PreflightCompleteRequest {
	t.Helper()
	raw, err := os.ReadFile("../../../../../api/schemas/channel/v1/fixtures/preflight-polling-complete-valid.json")
	if err != nil {
		t.Fatal(err)
	}
	var f struct {
		Document channelv1.PreflightCompleteRequest `json:"document"`
	}
	if json.Unmarshal(raw, &f) != nil {
		t.Fatal("fixture")
	}
	r := f.Document
	r.ScopeID, r.SourceEpoch, r.InstanceEpoch, r.ClaimToken = c.ScopeID, c.SourceEpoch, c.InstanceEpoch, c.ClaimToken
	r.LeaseEpoch, r.ConnectionRevision = g.LeaseEpoch, g.ConnectionRevision
	r.GatewayConfigDigest, r.EffectiveConfigDigest = c.GatewayConfigDigest, g.EffectiveConfigDigest
	r.ExpectedPublicOrigin, r.OriginStatus = c.ExpectedPublicOrigin, c.OriginStatus
	return r
}
func TestPollingPreflightPolicyAndNoOperationalWrites(t *testing.T) {
	ctx := context.Background()
	s, m, id, in := pollingPreflightSetup(t)
	before := cloneState(m.accounts.data)
	created := createPF(t, s, id, in)
	view, err := s.Get(ctx, owner, id, created.PreflightID)
	if err != nil || view.ReceiveMode != domain.LongPolling || view.DiagnosticPolicy != channelv1.PreflightReceiveModesPolicy || view.EffectiveConfigDigest != "" {
		t.Fatal("queued contract", err)
	}
	legacy := preflightInput(t, 1)
	legacy.DiagnosticPolicy = ""
	g, err := s.Claim(ctx, preflightPrincipal(), legacy)
	if err != nil || g != nil {
		t.Fatal("legacy worker claimed new task", err)
	}
	c := preflightInput(t, 2)
	g = claimPF(t, s, c)
	if g.ReceiveMode != domain.LongPolling || g.WebhookSecretConfigured || g.EffectiveConfigDigest == "" {
		t.Fatal("grant mode/config")
	}
	result := pollingComplete(t, c, g)
	if err = s.Complete(ctx, preflightPrincipal(), created.PreflightID, result); err != nil {
		t.Fatal(err)
	}
	if err = s.Complete(ctx, preflightPrincipal(), created.PreflightID, result); err != nil {
		t.Fatal("idempotent completion", err)
	}
	view, err = s.Get(ctx, owner, id, created.PreflightID)
	if err != nil || view.State != "COMPLETED" || view.Outcome != "PASS" || view.ExpectedPublicOrigin != nil || view.Checks[2].Status != "NOT_APPLICABLE" || view.Checks[7].Code != "DELIVERY_NOT_TESTED" {
		t.Fatal("polling result", err)
	}
	if !reflect.DeepEqual(before, m.accounts.data) {
		t.Fatal("diagnostic wrote operational state")
	}
}
func TestPollingPreflightOriginChangeIsLeaseScoped(t *testing.T) {
	for _, retry := range []bool{false, true} {
		t.Run(map[bool]string{false: "same-lease-conflict", true: "expired-lease-refresh"}[retry], func(t *testing.T) {
			ctx := context.Background()
			s, m, id, in := pollingPreflightSetup(t)
			created := createPF(t, s, id, in)
			c := preflightInput(t, 1)
			g := claimPF(t, s, c)
			next := preflightInput(t, 2)
			next.ExpectedPublicOrigin = nil
			next.OriginStatus = "PUBLIC_ORIGIN_INVALID"
			next.GatewayConfigDigest, _ = channelv1.PreflightConfigDigest(next.ScopeID, next.SourceEpoch, nil, next.OriginStatus)
			if retry {
				oldDigest := g.GatewayConfigDigest
				m.now = m.now.Add(31 * time.Second)
				g = claimPF(t, s, next)
				if g.LeaseEpoch != 2 || g.GatewayConfigDigest == oldDigest {
					t.Fatal("re-lease did not refresh global evidence")
				}
				old := pollingComplete(t, c, g)
				old.LeaseEpoch = 1
				requirePFError(t, s.Complete(ctx, preflightPrincipal(), created.PreflightID, old), "CHANNEL_PREFLIGHT_CLAIM_CONFLICT")
				if err := s.Complete(ctx, preflightPrincipal(), created.PreflightID, pollingComplete(t, next, g)); err != nil {
					t.Fatal(err)
				}
			} else {
				requirePFError(t, s.Complete(ctx, preflightPrincipal(), created.PreflightID, pollingComplete(t, next, g)), "CHANNEL_PREFLIGHT_RESULT_CONFLICT")
				r := m.records[created.PreflightID]
				if r.View.State != "RUNNING" || *r.View.GatewayConfigDigest != c.GatewayConfigDigest {
					t.Fatal("same lease silently replaced evidence")
				}
				if err := s.Complete(ctx, preflightPrincipal(), created.PreflightID, pollingComplete(t, c, g)); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}
func TestPreflightLegacyTasksAndModeChangeRemainDistinct(t *testing.T) {
	t.Run("historical-task", func(t *testing.T) {
		s, m, id, in := preflightSetup(t)
		created := createPF(t, s, id, in)
		r := m.records[created.PreflightID]
		r.View.ReceiveMode = ""
		r.View.DiagnosticPolicy = ""
		m.records[created.PreflightID] = r
		modern := preflightInput(t, 1)
		g, err := s.Claim(context.Background(), preflightPrincipal(), modern)
		if err != nil || g != nil {
			t.Fatal("new policy consumed old task", err)
		}
		legacy := preflightInput(t, 2)
		legacy.DiagnosticPolicy = ""
		g = claimPF(t, s, legacy)
		if g.ReceiveMode != "" || g.DiagnosticPolicy != "" || g.EffectiveConfigDigest != "" {
			t.Fatal("legacy grant normalized")
		}
		if err := s.Complete(context.Background(), preflightPrincipal(), created.PreflightID, preflightCompleteInput(t, legacy, 1)); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("mode-mismatch-stales-before-resolve", func(t *testing.T) {
		s, m, id, in := pollingPreflightSetup(t)
		created := createPF(t, s, id, in)
		c := preflightInput(t, 1)
		claimPF(t, s, c)
		a := m.accounts.data.accounts[id]
		a.Account.Config.ReceiveMode = domain.Webhook
		m.accounts.data.accounts[id] = a
		_, err := s.Resolve(context.Background(), preflightPrincipal(), created.PreflightID, preflightResolveInput(c, 1))
		requirePFError(t, err, "CHANNEL_PREFLIGHT_STALE")
	})
}
