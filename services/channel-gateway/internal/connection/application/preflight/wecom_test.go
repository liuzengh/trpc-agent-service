package preflight

import (
	"context"
	"encoding/base64"
	"encoding/json"
	wire "github.com/liuzengh/trpc-agent-service/api/schemas/channel/v1"
	"testing"
	"time"
)

type wecomControl struct {
	secretCalls int
	fail        error
}

func (c *wecomControl) Claim(context.Context, ClaimRequest) (*Grant, error) { return nil, nil }
func (c *wecomControl) ResolveCredential(context.Context, Grant) (Secret, error) {
	c.secretCalls++
	return NewSecret("private-bot-secret"), c.fail
}
func (c *wecomControl) Complete(context.Context, Grant, Result) error { return nil }

type wecomProbe struct {
	calls  int
	result ConnectionProbeResult
}

func (p *wecomProbe) InspectConnection(_ context.Context, _ Secret, id string) (ConnectionProbeResult, error) {
	p.calls++
	return p.result, nil
}
func wecomGrant(t *testing.T) (Grant, ConfigSnapshot) {
	t.Helper()
	epoch := "11111111-1111-4111-8111-111111111111"
	cfg, err := NewWeComConfig("pool", epoch)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	effective, err := wire.PreflightEffectiveConfigDigest("pool", epoch, "long_connection", 3, nil, "PUBLIC_ORIGIN_NOT_APPLICABLE")
	if err != nil {
		t.Fatal(err)
	}
	return Grant{AllowConnectionProbe: true, Provider: "wecom", ProviderAccountID: "synthetic-bot", PreflightID: "cpf_test", ScopeID: "pool", SourceEpoch: epoch, TenantID: "tnt_test", AccountID: "cha_test", AccountRevision: 5, ConnectionRevision: 3, LeaseEpoch: 1, Credential: Credential{Purpose: "wecom.bot_secret", ID: "ccr_test", Version: 2, Configured: true}, ServerTime: now, LeaseExpiresAt: now.Add(30 * time.Second), JobDeadlineAt: now.Add(120 * time.Second), ConfigDigest: cfg.Digest, ReceiveMode: "long_connection", DiagnosticPolicy: cfg.Policy, EffectiveConfigDigest: effective, Request: ClaimRequest{Config: cfg, DiagnosticPolicy: cfg.Policy, InstanceEpoch: epoch, RequestID: epoch, Token: NewSecret(base64.RawURLEncoding.EncodeToString(make([]byte, 32)))}}, cfg
}
func TestWeComPreflightAuthenticationIsNotDelivery(t *testing.T) {
	for _, tc := range []struct {
		name, code, status string
		value              *bool
	}{
		{"accepted", "WECOM_AUTHENTICATED", "PASS", boolAddress(true)},
		{"rejected", "WECOM_AUTH_REJECTED", "FAIL", boolAddress(false)},
		{"timeout", "PROVIDER_TIMEOUT", "UNKNOWN", nil},
		{"network", "PROVIDER_NETWORK", "UNKNOWN", nil},
		{"replaced", "WECOM_CONNECTION_REPLACED", "UNKNOWN", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g, cfg := wecomGrant(t)
			control := &wecomControl{}
			probe := &wecomProbe{result: ConnectionProbeResult{Code: tc.code, Authenticated: tc.value}}
			result, err := (WeComService{Control: control, Probe: probe}).Execute(context.Background(), g, cfg)
			if err != nil {
				t.Fatal(err)
			}
			if len(result.Checks) != 3 || result.Checks[1].Status != tc.status || result.Checks[2].Code != "DELIVERY_NOT_TESTED" || result.Checks[2].Status != "UNKNOWN" {
				t.Fatalf("%+v", result)
			}
			if control.secretCalls != 1 || probe.calls != 1 {
				t.Fatal("expected one resolve/probe")
			}
		})
	}
}
func boolAddress(b bool) *bool { return &b }
func TestWeComPreflightFences(t *testing.T) {
	cases := map[string]func(*Grant){
		"no explicit optin": func(g *Grant) { g.AllowConnectionProbe = false },
		"wrong credential":  func(g *Grant) { g.Credential.Purpose = "telegram.bot_token" },
		"wrong runner":      func(g *Grant) { g.Request.DiagnosticPolicy = wire.PreflightReceiveModesPolicy },
		"wrong mode":        func(g *Grant) { g.ReceiveMode = "long_polling" },
		"webhook smuggling": func(g *Grant) { g.WebhookPath = "/v1/telegram/cha_test" },
		"revision change":   func(g *Grant) { g.ConnectionRevision++ },
		"bot ID whitespace": func(g *Grant) { g.ProviderAccountID = "bot\n" },
	}
	for name, change := range cases {
		t.Run(name, func(t *testing.T) {
			g, cfg := wecomGrant(t)
			change(&g)
			control := &wecomControl{}
			probe := &wecomProbe{}
			if _, err := (WeComService{Control: control, Probe: probe}).Execute(context.Background(), g, cfg); err == nil {
				t.Fatal("invalid grant accepted")
			}
			if control.secretCalls != 0 || probe.calls != 0 {
				t.Fatal("invalid grant reached credentials/network")
			}
		})
	}
}
func TestWeComPreflightMissingSecretAndClosedResult(t *testing.T) {
	g, cfg := wecomGrant(t)
	g.Credential.Configured = false
	result, err := (WeComService{}).Execute(context.Background(), g, cfg)
	if err != nil || result.Checks[0].Code != "BOT_SECRET_MISSING" || result.Checks[1].Code != "NOT_EXECUTED" {
		t.Fatalf("%+v %v", result, err)
	}
	g.Credential.Configured = true
	probe := &wecomProbe{result: ConnectionProbeResult{Code: "WECOM_AUTHENTICATED"}}
	if _, err = (WeComService{Control: &wecomControl{}, Probe: probe}).Execute(context.Background(), g, cfg); err == nil {
		t.Fatal("missing positive ACK assertion accepted")
	}
}
func TestWeComRunnerClaimsOnlyItsPolicy(t *testing.T) {
	g, cfg := wecomGrant(t)
	r, err := NewWeComRunner(&wecomControl{}, &wecomProbe{}, cfg, g.Request.InstanceEpoch)
	if err != nil {
		t.Fatal(err)
	}
	request, err := r.newClaim()
	if err != nil || request.DiagnosticPolicy != "wecom_long_connection_v1" || request.Config.PublicOrigin != nil {
		t.Fatalf("%+v %v", request, err)
	}
	if _, err := NewRunner(&wecomControl{}, nil, cfg, g.Request.InstanceEpoch); err == nil {
		t.Fatal("cross-provider runner accepted")
	}
	b, _ := json.Marshal(g.Request.Token)
	if string(b) != `"[REDACTED]"` {
		t.Fatal("claim token exposed")
	}
}
