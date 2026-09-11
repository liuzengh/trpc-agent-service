package preflight

import (
	"context"
	"encoding/json"
	"testing"

	wire "github.com/liuzengh/trpc-agent-service/api/schemas/channel/v1"
)

func pollingGrant(t *testing.T, cfg ConfigSnapshot) Grant {
	t.Helper()
	g := testGrant(cfg)
	g.ReceiveMode = "long_polling"
	g.DiagnosticPolicy = wire.PreflightReceiveModesPolicy
	g.Request.DiagnosticPolicy = g.DiagnosticPolicy
	g.WebhookSecretConfigured = false
	var e error
	g.EffectiveConfigDigest, e = wire.PreflightEffectiveConfigDigest(g.ScopeID, g.SourceEpoch, g.ReceiveMode, g.ConnectionRevision, cfg.PublicOrigin, cfg.OriginStatus)
	if e != nil {
		t.Fatal(e)
	}
	return g
}
func TestPollingChecksShareContractAndKeepDiagnosticsReadOnly(t *testing.T) {
	for _, tc := range []struct {
		name, origin   string
		present, token bool
		want           string
	}{{"no_origin", "", false, true, "PASS"}, {"private_origin", "https://127.0.0.1", false, true, "PASS"}, {"existing_webhook", "", true, true, "FAIL"}, {"missing_token", "", false, false, "FAIL"}} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := testConfig(t, tc.origin)
			g := pollingGrant(t, cfg)
			g.Credential.Configured = tc.token
			control := &serviceControl{}
			p := ProbeResult{IdentityCode: "BOT_IDENTITY_MATCH", IdentityMatch: ptr(true), WebhookCode: "WEBHOOK_NONE", Presence: ptr(false), Relation: "NONE", PendingUpdates: ptr(int64(0)), HasLastError: ptr(false)}
			if tc.present {
				p.WebhookCode = "WEBHOOK_COMPARISON_UNAVAILABLE"
				p.Presence = ptr(true)
				p.Relation = "UNKNOWN"
			}
			probe := &serviceProbe{value: p}
			result, e := (Service{Control: control, Probe: probe}).Execute(context.Background(), g, cfg)
			if e != nil {
				t.Fatal(e)
			}
			var checks []wire.PreflightCheck
			raw, _ := json.Marshal(result.Checks)
			if e = json.Unmarshal(raw, &checks); e != nil {
				t.Fatal(e)
			}
			outcome, e := wire.ValidatePreflightChecksForMode(g.DiagnosticPolicy, g.ReceiveMode, checks)
			if e != nil || outcome != tc.want {
				t.Fatal(outcome, e, string(raw))
			}
			for _, i := range []int{2, 5, 6} {
				if checks[i].Status != "NOT_APPLICABLE" {
					t.Fatal(i, checks[i])
				}
			}
			if checks[7].Code != "DELIVERY_NOT_TESTED" || probe.calls > 1 || control.calls > 1 {
				t.Fatal("diagnostic side effect or false delivery evidence")
			}
		})
	}
}
func TestModeGrantRejectsChangedPolicyAndEffectiveConfig(t *testing.T) {
	cfg := testConfig(t, "")
	g := pollingGrant(t, cfg)
	if e := ValidateGrant(g, cfg); e != nil {
		t.Fatal(e)
	}
	original := g
	g.ReceiveMode = "webhook"
	if ValidateGrant(g, cfg) == nil {
		t.Fatal("mode swap authorized")
	}
	g = original
	g.Request.DiagnosticPolicy = ""
	if ValidateGrant(g, cfg) == nil {
		t.Fatal("unclaimed policy authorized")
	}
	g = original
	g.ConnectionRevision++
	if ValidateGrant(g, cfg) == nil {
		t.Fatal("old effective digest authorized new revision")
	}
}
func TestLPOriginChangeDoesNotChangeEffectiveButCannotRewriteSameClaim(t *testing.T) {
	first := testConfig(t, "")
	second := testConfig(t, "https://gateway.example.com")
	a, b := pollingGrant(t, first), pollingGrant(t, second)
	if a.ConfigDigest == b.ConfigDigest || a.EffectiveConfigDigest != b.EffectiveConfigDigest {
		t.Fatal("global and effective configuration conflated")
	}
	if ValidateGrant(a, second) == nil {
		t.Fatal("same lease configuration silently replaced")
	}
}
