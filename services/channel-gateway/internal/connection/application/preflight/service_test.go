package preflight

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

func ptr[T any](v T) *T { return &v }
func testConfig(t *testing.T, origin string) ConfigSnapshot {
	t.Helper()
	cfg, err := NewConfig("pool", configEpoch, origin)
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}
func testGrant(cfg ConfigSnapshot) Grant {
	now := time.Date(2026, 9, 6, 4, 0, 2, 0, time.UTC)
	return Grant{PreflightID: "cpf_example", ScopeID: cfg.ScopeID, SourceEpoch: cfg.SourceEpoch, TenantID: "tnt_example", AccountID: "cha_example", Provider: "telegram", ProviderAccountID: "123456789", WebhookPath: "/v1/telegram/cha_example", AccountRevision: 7, ConnectionRevision: 4, LeaseEpoch: 1, Credential: Credential{Purpose: "telegram.bot_token", ID: "ccr_example", Version: 2, Configured: true}, WebhookSecretConfigured: true, ServerTime: now, LeaseExpiresAt: now.Add(30 * time.Second), JobDeadlineAt: now.Add(118 * time.Second), ConfigDigest: cfg.Digest, Request: ClaimRequest{Config: cfg, InstanceEpoch: "22222222-2222-4222-8222-222222222222", RequestID: "33333333-3333-4333-8333-333333333333", Token: NewSecret(strings.Repeat("A", 43))}}
}
func matchingProbe() ProbeResult {
	return ProbeResult{IdentityCode: "BOT_IDENTITY_MATCH", IdentityMatch: ptr(true), WebhookCode: "WEBHOOK_MATCH", Presence: ptr(true), Relation: "MATCH", PendingUpdates: ptr(int64(0)), HasLastError: ptr(false)}
}

type serviceControl struct {
	calls   int
	err     error
	resolve func(context.Context, Grant) (Secret, error)
}

func (*serviceControl) Claim(context.Context, ClaimRequest) (*Grant, error) {
	panic("service must not claim")
}
func (*serviceControl) Complete(context.Context, Grant, Result) error {
	panic("service must not complete")
}
func (c *serviceControl) ResolveCredential(ctx context.Context, g Grant) (Secret, error) {
	c.calls++
	if c.resolve != nil {
		return c.resolve(ctx, g)
	}
	return NewSecret("service-token-CANARY"), c.err
}

type serviceProbe struct {
	calls int
	got   ProbeRequest
	value ProbeResult
	err   error
}

func (p *serviceProbe) Inspect(_ context.Context, r ProbeRequest) (ProbeResult, error) {
	p.calls++
	p.got = r
	return p.value, p.err
}

func TestServiceFixedChecksAndDeclassification(t *testing.T) {
	cfg := testConfig(t, "https://gateway.example.com")
	g := testGrant(cfg)
	control, probe := &serviceControl{}, &serviceProbe{value: matchingProbe()}
	before := time.Now().UTC()
	result, err := (Service{Control: control, Probe: probe}).Execute(context.Background(), g, cfg)
	if err != nil || control.calls != 1 || probe.calls != 1 {
		t.Fatalf("err=%v calls=%d/%d", err, control.calls, probe.calls)
	}
	if probe.got.ExpectedIdentity != g.ProviderAccountID || probe.got.ExpectedWebhook == nil || *probe.got.ExpectedWebhook != "https://gateway.example.com/v1/telegram/cha_example" || probe.got.Token.Reveal() != "service-token-CANARY" {
		t.Fatal("incorrect probe request")
	}
	if result.ObservedAt.Before(before) || result.ObservedAt.After(time.Now().UTC()) || result.ObservedAt.Location() != time.UTC {
		t.Fatal("invalid observation time")
	}
	ids := []string{"credential_configuration", "bot_identity", "public_origin", "webhook_registration", "pending_updates", "delivery_errors", "recovery_materials", "delivery_verification"}
	keys := [][]string{{"bot_token_configured", "webhook_secret_configured"}, {"identity_match"}, {"validation"}, {"presence", "relation"}, {"pending_update_count"}, {"has_last_error", "last_error_at"}, {"secret_token_readable", "restore_available"}, {"verification"}}
	if len(result.Checks) != 8 {
		t.Fatal("not eight checks")
	}
	for i, check := range result.Checks {
		if check.ID != ids[i] {
			t.Fatalf("check[%d]=%+v", i, check)
		}
		var details map[string]any
		if json.Unmarshal(check.Details, &details) != nil || len(details) != len(keys[i]) {
			t.Fatal("details not closed")
		}
		for _, key := range keys[i] {
			if _, ok := details[key]; !ok {
				t.Fatalf("missing %s", key)
			}
		}
		want := "PASS"
		if i >= 6 {
			want = "UNKNOWN"
		}
		if check.Status != want {
			t.Fatalf("check %d status=%s", i, check.Status)
		}
	}
	for _, v := range []any{result, probe.got.Token, g.Request.Token, probe.got} {
		b, _ := json.Marshal(v)
		formatted := fmt.Sprintf("%v %+v %#v", v, v, v)
		if strings.Contains(string(b)+formatted, "service-token-CANARY") || strings.Contains(formatted, strings.Repeat("A", 43)) {
			t.Fatal("secret formatting leaked")
		}
	}
	if string(result.Checks[6].Details) != `{"secret_token_readable":false,"restore_available":false}` || string(result.Checks[7].Details) != `{"verification":"NOT_TESTED"}` {
		t.Fatal("capability checks changed")
	}
	// Result owns a snapshot, not the caller's mutable origin pointer.
	*cfg.PublicOrigin = "https://changed.example.com"
	if *result.Config.PublicOrigin != "https://gateway.example.com" {
		t.Fatal("result configuration aliased")
	}
}

func TestServiceMissingCredentials(t *testing.T) {
	for _, token := range []bool{false, true} {
		for _, secret := range []bool{false, true} {
			t.Run(fmt.Sprintf("token=%t_secret=%t", token, secret), func(t *testing.T) {
				cfg := testConfig(t, "https://gateway.example.com")
				g := testGrant(cfg)
				g.Credential.Configured = token
				g.WebhookSecretConfigured = secret
				control, probe := &serviceControl{}, &serviceProbe{value: matchingProbe()}
				r, err := (Service{Control: control, Probe: probe}).Execute(context.Background(), g, cfg)
				if err != nil {
					t.Fatal(err)
				}
				code := "CREDENTIALS_CONFIGURED"
				if !token {
					code = "BOT_TOKEN_MISSING"
				} else if !secret {
					code = "WEBHOOK_SECRET_MISSING"
				}
				if r.Checks[0].Code != code {
					t.Fatal(r.Checks[0])
				}
				calls := 0
				if token {
					calls = 1
				}
				if control.calls != calls || probe.calls != calls {
					t.Fatal("incorrect calls")
				}
				if !token {
					for _, i := range []int{1, 3, 4, 5} {
						if r.Checks[i].Code != "NOT_EXECUTED" || r.Checks[i].Status != "SKIPPED" {
							t.Fatal(r.Checks[i])
						}
					}
				}
			})
		}
	}
}

func TestServiceProviderBranches(t *testing.T) {
	for _, identity := range []string{"BOT_IDENTITY_MISMATCH", "TOKEN_REJECTED", "PROVIDER_NETWORK", "PROVIDER_TIMEOUT", "PROVIDER_RATE_LIMITED", "PROVIDER_UNAVAILABLE", "PROVIDER_RESPONSE_INVALID"} {
		t.Run(identity, func(t *testing.T) {
			cfg := testConfig(t, "https://gateway.example.com")
			p := ProbeResult{IdentityCode: identity}
			if identity == "BOT_IDENTITY_MISMATCH" {
				p.IdentityMatch = ptr(false)
			}
			r, err := (Service{Control: &serviceControl{}, Probe: &serviceProbe{value: p}}).Execute(context.Background(), testGrant(cfg), cfg)
			if err != nil || r.Checks[1].Code != identity {
				t.Fatalf("err=%v result=%+v", err, r)
			}
			for _, i := range []int{3, 4, 5} {
				if r.Checks[i].Status != "SKIPPED" {
					t.Fatal(r.Checks[i])
				}
			}
		})
	}
	for _, webhook := range []string{"TOKEN_REJECTED", "PROVIDER_NETWORK", "PROVIDER_TIMEOUT", "PROVIDER_RATE_LIMITED", "PROVIDER_UNAVAILABLE", "PROVIDER_RESPONSE_INVALID"} {
		t.Run("webhook_"+webhook, func(t *testing.T) {
			cfg := testConfig(t, "https://gateway.example.com")
			p := ProbeResult{IdentityCode: "BOT_IDENTITY_MATCH", IdentityMatch: ptr(true), WebhookCode: webhook, Relation: "UNKNOWN"}
			r, err := (Service{Control: &serviceControl{}, Probe: &serviceProbe{value: p}}).Execute(context.Background(), testGrant(cfg), cfg)
			if err != nil || r.Checks[3].Code != webhook || r.Checks[4].Status != "SKIPPED" || r.Checks[5].Status != "SKIPPED" {
				t.Fatalf("err=%v result=%+v", err, r)
			}
			want := "UNKNOWN"
			if webhook == "TOKEN_REJECTED" {
				want = "FAIL"
			}
			if r.Checks[3].Status != want {
				t.Fatal(r.Checks[3])
			}
		})
	}
}

func TestServiceOriginDoesNotGateRead(t *testing.T) {
	for _, origin := range []string{"https://gateway.example.com", "http://CANARY.invalid/path", "https://127.0.0.1"} {
		for _, present := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s_present=%t", origin, present), func(t *testing.T) {
				cfg := testConfig(t, origin)
				p := matchingProbe()
				p.PendingUpdates = ptr(int64(5))
				p.HasLastError = ptr(true)
				p.LastErrorAt = ptr(time.Date(2026, 9, 6, 4, 0, 0, 0, time.UTC))
				if !present {
					p.WebhookCode = "WEBHOOK_NONE"
					p.Presence = ptr(false)
					p.Relation = "NONE"
				} else if cfg.PublicOrigin == nil {
					p.WebhookCode = "WEBHOOK_COMPARISON_UNAVAILABLE"
					p.Relation = "UNKNOWN"
				} else {
					p.WebhookCode = "WEBHOOK_DIFFERENT"
					p.Relation = "DIFFERENT"
				}
				probe := &serviceProbe{value: p}
				r, err := (Service{Control: &serviceControl{}, Probe: probe}).Execute(context.Background(), testGrant(cfg), cfg)
				if err != nil || probe.calls != 1 || r.Checks[3].Code != p.WebhookCode || r.Checks[4].Status != "WARN" || r.Checks[5].Status != "WARN" {
					t.Fatalf("err=%v result=%+v", err, r)
				}
				if cfg.PublicOrigin == nil && probe.got.ExpectedWebhook != nil {
					t.Fatal("invalid origin passed to probe")
				}
			})
		}
	}
}

func TestServiceStopsOnResolveAndContextErrors(t *testing.T) {
	for _, err := range []error{ErrDenied, ErrConflict, ErrUnavailable, ErrExpired, fmt.Errorf("CANARY: %w", ErrConflict), errors.New("raw-secret-CANARY"), context.Canceled} {
		t.Run(fmt.Sprint(err), func(t *testing.T) {
			cfg := testConfig(t, "https://gateway.example.com")
			control, probe := &serviceControl{err: err}, &serviceProbe{value: matchingProbe()}
			r, got := (Service{Control: control, Probe: probe}).Execute(context.Background(), testGrant(cfg), cfg)
			if got == nil || strings.Contains(got.Error(), "CANARY") || probe.calls != 0 || len(r.Checks) != 0 {
				t.Fatalf("error=%v calls=%d result=%+v", got, probe.calls, r)
			}
		})
	}
	cfg := testConfig(t, "https://gateway.example.com")
	for _, cancelOnResolve := range []bool{false, true} {
		ctx, cancel := context.WithCancel(context.Background())
		control := &serviceControl{}
		probe := &serviceProbe{value: matchingProbe()}
		if cancelOnResolve {
			control.resolve = func(context.Context, Grant) (Secret, error) { cancel(); return NewSecret("CANARY"), nil }
		} else {
			cancel()
		}
		_, err := (Service{Control: control, Probe: probe}).Execute(ctx, testGrant(cfg), cfg)
		if err != ErrExpired || probe.calls != 0 {
			t.Fatalf("err=%v", err)
		}
		cancel()
	}
}

func TestServiceRejectsMalformedProbe(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*ProbeResult)
	}{
		{"identity_text", func(p *ProbeResult) { p.IdentityCode = "raw-CANARY" }},
		{"identity_nil", func(p *ProbeResult) { p.IdentityMatch = nil }},
		{"identity_contradiction", func(p *ProbeResult) { p.IdentityMatch = ptr(false) }},
		{"mismatch_read", func(p *ProbeResult) { p.IdentityCode = "BOT_IDENTITY_MISMATCH"; p.IdentityMatch = ptr(false) }},
		{"webhook_text", func(p *ProbeResult) { p.WebhookCode = "raw-CANARY" }},
		{"relation", func(p *ProbeResult) { p.Relation = "raw-CANARY" }},
		{"presence", func(p *ProbeResult) { p.Presence = ptr(false) }},
		{"count_missing", func(p *ProbeResult) { p.PendingUpdates = nil }},
		{"count_negative", func(p *ProbeResult) { p.PendingUpdates = ptr(int64(-1)) }},
		{"count_unsafe", func(p *ProbeResult) { p.PendingUpdates = ptr(int64(9007199254740992)) }},
		{"error_missing", func(p *ProbeResult) { p.HasLastError = nil }},
		{"error_false_with_time", func(p *ProbeResult) { p.LastErrorAt = ptr(time.Now().UTC()) }},
		{"non_utc", func(p *ProbeResult) {
			p.HasLastError = ptr(true)
			p.LastErrorAt = ptr(time.Now().In(time.FixedZone("offset", 3600)))
		}},
		{"unrepresentable_time", func(p *ProbeResult) {
			p.HasLastError = ptr(true)
			p.LastErrorAt = ptr(time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC))
		}},
		{"unavailable_with_valid_origin", func(p *ProbeResult) { p.WebhookCode = "WEBHOOK_COMPARISON_UNAVAILABLE"; p.Relation = "UNKNOWN" }},
		{"token_rejected_with_data", func(p *ProbeResult) { p.WebhookCode = "TOKEN_REJECTED"; p.Relation = "UNKNOWN" }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := testConfig(t, "https://gateway.example.com")
			p := matchingProbe()
			tc.mutate(&p)
			r, err := (Service{Control: &serviceControl{}, Probe: &serviceProbe{value: p}}).Execute(context.Background(), testGrant(cfg), cfg)
			if err != ErrInvalid || len(r.Checks) != 0 {
				t.Fatalf("err=%v result=%+v", err, r)
			}
		})
	}
}

func TestValidateGrantFencing(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*Grant)
	}{
		{"scope", func(g *Grant) { g.ScopeID = "different" }}, {"epoch", func(g *Grant) { g.SourceEpoch = "22222222-2222-4222-8222-222222222222" }},
		{"digest", func(g *Grant) { g.ConfigDigest = "sha256:" + strings.Repeat("a", 64) }}, {"request_config", func(g *Grant) { g.Request.Config.Digest = "bad" }},
		{"task", func(g *Grant) { g.PreflightID = "../bad" }}, {"tenant", func(g *Grant) { g.TenantID = "" }}, {"account", func(g *Grant) { g.AccountID = "" }},
		{"provider", func(g *Grant) { g.Provider = "wecom" }}, {"identity", func(g *Grant) { g.ProviderAccountID = "0123" }}, {"path", func(g *Grant) { g.WebhookPath = "https://localhost/CANARY" }},
		{"revision", func(g *Grant) { g.ConnectionRevision = g.AccountRevision + 1 }}, {"purpose", func(g *Grant) { g.Credential.Purpose = "telegram.webhook_secret" }},
		{"credential_id", func(g *Grant) { g.Credential.ID = "" }}, {"credential_version", func(g *Grant) { g.Credential.Version = 0 }},
		{"instance", func(g *Grant) { g.Request.InstanceEpoch = "" }}, {"request_id", func(g *Grant) { g.Request.RequestID = "" }}, {"token", func(g *Grant) { g.Request.Token = NewSecret("CANARY") }},
		{"token_noncanonical", func(g *Grant) { g.Request.Token = NewSecret(strings.Repeat("A", 42) + "B") }},
		{"lease_epoch", func(g *Grant) { g.LeaseEpoch = 3 }}, {"zero_time", func(g *Grant) { g.ServerTime = time.Time{} }},
		{"expired", func(g *Grant) { g.LeaseExpiresAt = g.ServerTime }}, {"deadline", func(g *Grant) { g.JobDeadlineAt = g.ServerTime }},
		{"lease_extended", func(g *Grant) { g.LeaseExpiresAt = g.ServerTime.Add(31 * time.Second) }}, {"job_extended", func(g *Grant) { g.JobDeadlineAt = g.ServerTime.Add(121 * time.Second) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := testConfig(t, "https://gateway.example.com")
			g := testGrant(cfg)
			tc.mutate(&g)
			control, probe := &serviceControl{}, &serviceProbe{}
			_, err := (Service{Control: control, Probe: probe}).Execute(context.Background(), g, cfg)
			if err == nil || control.calls != 0 || probe.calls != 0 || strings.Contains(err.Error(), "CANARY") {
				t.Fatalf("err=%v", err)
			}
		})
	}
	// A later metadata-only revision and clock skew do not invalidate a pinned connection.
	cfg := testConfig(t, "https://gateway.example.com")
	g := testGrant(cfg)
	g.AccountRevision++
	g.ServerTime = g.ServerTime.Add(-24 * time.Hour)
	g.LeaseExpiresAt = g.LeaseExpiresAt.Add(-24 * time.Hour)
	g.JobDeadlineAt = g.JobDeadlineAt.Add(-24 * time.Hour)
	if err := ValidateGrant(g, cfg); err != nil {
		t.Fatal(err)
	}
}
