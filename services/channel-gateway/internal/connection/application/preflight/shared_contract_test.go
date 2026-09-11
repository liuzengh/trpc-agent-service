package preflight

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	wire "github.com/liuzengh/trpc-agent-service/api/schemas/channel/v1"
)

// This matrix crosses the real shared contract, not a copied digest function.
// Raw invalid origins never leave NewConfig; Control accepts their null+code
// snapshots and independently rejects attempts to present the raw URL as valid.
func TestSharedContractConfigOriginMatrix(t *testing.T) {
	groups := []struct {
		status string
		values []string
	}{
		{OriginValid, []string{
			"https://gateway.example.com:80", "https://gateway.example.com:88/", "https://gateway.example.com:8443", "HTTPS://GATEWAY.EXAMPLE.COM", "https://8.8.8.8", "https://[2606:4700:4700::1111]:8443", "https://xn--bcher-kva.example",
			"https://gateway.example.com", "https://GATEWAY.EXAMPLE.COM:443/", "https://[2606:4700:4700::1111]", "https://GATEWAY.EXAMPLE.COM:8443/",
		}},
		{OriginInvalid, []string{
			"", "http://gateway.example.com", "https://u:p@gateway.example.com", "https://gateway.example.com?", "https://gateway.example.com#", "https://gateway.example.com/%2f", "https://gateway.example.com:444", "https://gateway.example.com:0443", "https://gateway.example.com:", "https://gateway.example.com.", "https://中文.example", "https://gateway_example.com", "https://-gateway.example.com", "https://gateway..example.com", "https://gateway.example.com/path", "https://gateway.example.com\\evil", " https://gateway.example.com", "https://[::1%25eth0]", "https://[gateway.example.com]",
			"https://localhost:18443",
		}},
		{OriginNotPublic, []string{
			"https://localhost", "https://host", "https://api.localhost", "https://api.local", "https://api.internal", "https://host.home.arpa", "https://127.1", "https://0177.0.0.1", "https://0x7f.0.0.1", "https://0.0.0.0", "https://10.0.0.1", "https://100.64.0.1", "https://127.0.0.1", "https://169.254.169.254", "https://172.16.0.1", "https://192.0.0.8", "https://192.0.2.1", "https://192.88.99.1", "https://192.168.1.1", "https://198.18.0.1", "https://198.51.100.1", "https://203.0.113.1", "https://224.0.0.1", "https://240.0.0.1", "https://255.255.255.255", "https://[::]", "https://[::1]", "https://[fc00::1]", "https://[fe80::1]", "https://[ff02::1]", "https://[::ffff:127.0.0.1]", "https://[2001:db8::1]", "https://[2002:0808:0808::1]", "https://[3fff::1]",
			"https://[64:ff9b::a00:1]", "https://[::ffff:8.8.8.8]",
		}},
	}
	count := 0
	for _, group := range groups {
		for _, raw := range group.values {
			count++
			t.Run(fmt.Sprintf("%02d_%s", count, raw), func(t *testing.T) {
				cfg, err := NewConfig("pool", configEpoch, raw)
				if err != nil || cfg.OriginStatus != group.status || ValidateConfig(cfg) != nil {
					t.Fatalf("application classification=%s want=%s err=%v", cfg.OriginStatus, group.status, err)
				}
				digest, err := wire.PreflightConfigDigest(cfg.ScopeID, cfg.SourceEpoch, cfg.PublicOrigin, cfg.OriginStatus)
				if err != nil || digest != cfg.Digest {
					t.Fatalf("shared digest=%s application=%s err=%v", digest, cfg.Digest, err)
				}
				claim := wire.PreflightClaimRequest{SchemaVersion: 1, ScopeID: cfg.ScopeID, SourceEpoch: cfg.SourceEpoch, InstanceEpoch: "22222222-2222-4222-8222-222222222222", ClaimRequestID: "33333333-3333-4333-8333-333333333333", ClaimToken: strings.Repeat("A", 43), GatewayConfigDigest: cfg.Digest, ExpectedPublicOrigin: cfg.PublicOrigin, OriginStatus: cfg.OriginStatus, Limit: 1}
				if err := wire.Validate("preflight-claim.schema.json", sharedJSON(t, claim)); err != nil {
					t.Fatalf("shared claim rejects application config: %v", err)
				}
				if cfg.PublicOrigin == nil || raw != *cfg.PublicOrigin {
					if _, err := wire.PreflightConfigDigest(cfg.ScopeID, cfg.SourceEpoch, &raw, OriginValid); err == nil {
						t.Fatal("shared contract accepted noncanonical or invalid raw origin as valid")
					}
				}
			})
		}
	}
	if count != 67 {
		t.Fatalf("origin matrix changed: got %d, want 67", count)
	}
}

func TestSharedContractConfigFrozenVectors(t *testing.T) {
	for _, tc := range []struct{ raw, digest string }{
		{"https://gateway.example.com", "sha256:699f4ff6f77d9069bb632ac9e1f29b7a0f996780217aa18d127dfe135e7f314d"},
		{"https://127.0.0.1", "sha256:63c09ea92b63a02e1750e76d92f3d6ea76be142d7b6d3dd1b6c590936b9ff58a"},
		{"http://gateway.example.com", "sha256:03f2b058b89dcf96f3b74459cf4851902e480cb112593a2e253c2d7527b03ed4"},
	} {
		t.Run(tc.raw, func(t *testing.T) {
			cfg := testConfig(t, tc.raw)
			digest, err := wire.PreflightConfigDigest(cfg.ScopeID, cfg.SourceEpoch, cfg.PublicOrigin, cfg.OriginStatus)
			if err != nil || digest != tc.digest || cfg.Digest != tc.digest {
				t.Fatalf("shared=%s application=%s want=%s err=%v", digest, cfg.Digest, tc.digest, err)
			}
		})
	}
}

func TestSharedContractServiceResults(t *testing.T) {
	type scenario struct {
		name, origin, outcome string
		grant                 func(*Grant)
		probe                 func(*ProbeResult)
	}
	scenarios := []scenario{
		{name: "match", outcome: "PASS"},
		{name: "token_missing", outcome: "FAIL", grant: func(g *Grant) { g.Credential.Configured = false }},
		{name: "both_credentials_missing", outcome: "FAIL", grant: func(g *Grant) { g.Credential.Configured = false; g.WebhookSecretConfigured = false }},
		{name: "webhook_secret_missing", outcome: "FAIL", grant: func(g *Grant) { g.WebhookSecretConfigured = false }},
		{name: "identity_mismatch", outcome: "FAIL", probe: func(p *ProbeResult) {
			*p = ProbeResult{IdentityCode: "BOT_IDENTITY_MISMATCH", IdentityMatch: ptr(false)}
		}},
		{name: "identity_401_or_local_token_syntax", outcome: "FAIL", probe: func(p *ProbeResult) {
			*p = ProbeResult{IdentityCode: "TOKEN_REJECTED", WebhookCode: "NOT_EXECUTED", Relation: "UNKNOWN"}
		}},
		{name: "origin_invalid_existing_webhook", origin: "http://bad.example/path", outcome: "FAIL", probe: func(p *ProbeResult) { p.WebhookCode = "WEBHOOK_COMPARISON_UNAVAILABLE"; p.Relation = "UNKNOWN" }},
		{name: "origin_nonpublic_existing_webhook", origin: "https://127.0.0.1", outcome: "FAIL", probe: func(p *ProbeResult) { p.WebhookCode = "WEBHOOK_COMPARISON_UNAVAILABLE"; p.Relation = "UNKNOWN" }},
		{name: "origin_invalid_no_webhook", origin: "http://bad.example/path", outcome: "FAIL", probe: func(p *ProbeResult) { p.WebhookCode = "WEBHOOK_NONE"; p.Relation = "NONE"; p.Presence = ptr(false) }},
		{name: "origin_nonpublic_no_webhook", origin: "https://127.0.0.1", outcome: "FAIL", probe: func(p *ProbeResult) { p.WebhookCode = "WEBHOOK_NONE"; p.Relation = "NONE"; p.Presence = ptr(false) }},
		{name: "webhook_none", outcome: "WARN", probe: func(p *ProbeResult) { p.WebhookCode = "WEBHOOK_NONE"; p.Relation = "NONE"; p.Presence = ptr(false) }},
		{name: "webhook_different", outcome: "WARN", probe: func(p *ProbeResult) { p.WebhookCode = "WEBHOOK_DIFFERENT"; p.Relation = "DIFFERENT" }},
		{name: "webhook_401", outcome: "FAIL", probe: func(p *ProbeResult) {
			*p = ProbeResult{IdentityCode: "BOT_IDENTITY_MATCH", IdentityMatch: ptr(true), WebhookCode: "TOKEN_REJECTED", Relation: "UNKNOWN"}
		}},
		{name: "pending_updates", outcome: "WARN", probe: func(p *ProbeResult) { p.PendingUpdates = ptr(int64(5)) }},
		{name: "pending_safe_integer_max", outcome: "WARN", probe: func(p *ProbeResult) { p.PendingUpdates = ptr(int64(9007199254740991)) }},
		{name: "last_error_utc", outcome: "WARN", probe: func(p *ProbeResult) {
			p.HasLastError = ptr(true)
			p.LastErrorAt = ptr(time.Date(2026, 9, 6, 4, 0, 0, 0, time.UTC))
		}},
		{name: "last_error_without_date", outcome: "WARN", probe: func(p *ProbeResult) { p.HasLastError = ptr(true) }},
		{name: "last_error_max_date", outcome: "WARN", probe: func(p *ProbeResult) {
			p.HasLastError = ptr(true)
			p.LastErrorAt = ptr(time.Unix(253402300799, 0).UTC())
		}},
		{name: "metadata_only_revision", outcome: "PASS", grant: func(g *Grant) { g.AccountRevision++ }},
	}
	for _, code := range []string{"PROVIDER_NETWORK", "PROVIDER_TIMEOUT", "PROVIDER_RATE_LIMITED", "PROVIDER_UNAVAILABLE", "PROVIDER_RESPONSE_INVALID"} {
		scenarios = append(scenarios,
			scenario{name: "identity_" + code, outcome: "UNKNOWN", probe: func(p *ProbeResult) { *p = ProbeResult{IdentityCode: code} }},
			scenario{name: "webhook_" + code, outcome: "UNKNOWN", probe: func(p *ProbeResult) {
				*p = ProbeResult{IdentityCode: "BOT_IDENTITY_MATCH", IdentityMatch: ptr(true), WebhookCode: code, Relation: "UNKNOWN"}
			}},
		)
	}
	for _, tc := range scenarios {
		t.Run(tc.name, func(t *testing.T) {
			origin := tc.origin
			if origin == "" {
				origin = "https://gateway.example.com"
			}
			cfg := testConfig(t, origin)
			g := testGrant(cfg)
			if tc.grant != nil {
				tc.grant(&g)
			}
			p := matchingProbe()
			if tc.probe != nil {
				tc.probe(&p)
			}
			control, probe := &serviceControl{}, &serviceProbe{value: p}
			r, err := (Service{Control: control, Probe: probe}).Execute(context.Background(), g, cfg)
			if err != nil {
				t.Fatal(err)
			}
			wantCalls := 1
			if !g.Credential.Configured {
				wantCalls = 0
			}
			if control.calls != wantCalls || probe.calls != wantCalls {
				t.Fatalf("resolve/probe calls=%d/%d want=%d", control.calls, probe.calls, wantCalls)
			}
			if cfg.PublicOrigin == nil && probe.got.ExpectedWebhook != nil {
				t.Fatal("invalid origin became a probe target")
			}
			complete := sharedComplete(g, r)
			outcome, err := wire.ValidatePreflightChecks(complete.Checks)
			if err != nil || outcome != tc.outcome {
				t.Fatalf("shared outcome=%s want=%s err=%v", outcome, tc.outcome, err)
			}
			if err := wire.Validate("preflight-complete.schema.json", sharedJSON(t, complete)); err != nil {
				t.Fatalf("real complete schema rejected service result: %v", err)
			}
			if len(complete.Checks) != 8 || complete.Checks[6].Status != "UNKNOWN" || complete.Checks[7].Status != "UNKNOWN" {
				t.Fatal("capability limits missing")
			}
			if strings.Contains(string(sharedJSON(t, complete.Checks)), "CANARY") {
				t.Fatal("secret retained in shared checks")
			}
		})
	}
}

func TestSharedContractRejectsResultTampering(t *testing.T) {
	cfg := testConfig(t, "https://gateway.example.com")
	g := testGrant(cfg)
	for _, tc := range []struct {
		name   string
		mutate func(*wire.PreflightCompleteRequest)
	}{
		{"wrong_digest", func(r *wire.PreflightCompleteRequest) { r.GatewayConfigDigest = "sha256:" + strings.Repeat("0", 64) }},
		{"wrong_scope", func(r *wire.PreflightCompleteRequest) { r.ScopeID = "another-scope" }},
		{"origin_disagrees", func(r *wire.PreflightCompleteRequest) { r.ExpectedPublicOrigin = nil }},
		{"non_utc", func(r *wire.PreflightCompleteRequest) { r.ObservedAt = r.ObservedAt.In(time.FixedZone("other", 3600)) }},
		{"missing_check", func(r *wire.PreflightCompleteRequest) { r.Checks = r.Checks[:7] }},
		{"identity_not_executed", func(r *wire.PreflightCompleteRequest) {
			r.Checks[1] = wire.PreflightCheck{ID: "bot_identity", Status: "SKIPPED", Code: "NOT_EXECUTED", Details: json.RawMessage(`{"identity_match":null}`)}
		}},
		{"webhook_not_executed", func(r *wire.PreflightCompleteRequest) {
			r.Checks[3] = wire.PreflightCheck{ID: "webhook_registration", Status: "SKIPPED", Code: "NOT_EXECUTED", Details: json.RawMessage(`{"presence":null,"relation":"UNKNOWN"}`)}
		}},
		{"check_extra_secret_field", func(r *wire.PreflightCompleteRequest) {
			r.Checks[3].Details = json.RawMessage(`{"presence":true,"relation":"MATCH","raw_url":"secret-CANARY"}`)
		}},
		{"check_duplicate_field", func(r *wire.PreflightCompleteRequest) {
			r.Checks[3].Details = json.RawMessage(`{"presence":true,"presence":true,"relation":"MATCH"}`)
		}},
		{"check_false_presence", func(r *wire.PreflightCompleteRequest) {
			r.Checks[3].Details = json.RawMessage(`{"presence":false,"relation":"MATCH"}`)
		}},
		{"unsafe_pending", func(r *wire.PreflightCompleteRequest) {
			r.Checks[4].Details = json.RawMessage(`{"pending_update_count":9007199254740992}`)
		}},
		{"invented_recovery", func(r *wire.PreflightCompleteRequest) {
			r.Checks[6].Details = json.RawMessage(`{"secret_token_readable":true,"restore_available":true}`)
		}},
		{"invented_delivery_pass", func(r *wire.PreflightCompleteRequest) { r.Checks[7].Status = "PASS" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, err := (Service{Control: &serviceControl{}, Probe: &serviceProbe{value: matchingProbe()}}).Execute(context.Background(), g, cfg)
			if err != nil {
				t.Fatal(err)
			}
			complete := sharedComplete(g, r)
			tc.mutate(&complete)
			if err := wire.Validate("preflight-complete.schema.json", sharedJSON(t, complete)); err == nil {
				t.Fatal("real complete contract accepted contradictory or leaking result")
			}
		})
	}
}

func sharedComplete(g Grant, r Result) wire.PreflightCompleteRequest {
	checks := make([]wire.PreflightCheck, len(r.Checks))
	for i, c := range r.Checks {
		checks[i] = wire.PreflightCheck{ID: c.ID, Status: c.Status, Code: c.Code, Details: append(json.RawMessage(nil), c.Details...)}
	}
	return wire.PreflightCompleteRequest{SchemaVersion: 1, ScopeID: g.ScopeID, SourceEpoch: g.SourceEpoch, InstanceEpoch: g.Request.InstanceEpoch, LeaseEpoch: g.LeaseEpoch, ClaimToken: g.Request.Token.Reveal(), GatewayConfigDigest: r.Config.Digest, ExpectedPublicOrigin: r.Config.PublicOrigin, ObservedAt: r.ObservedAt, Checks: checks}
}

func sharedJSON(t *testing.T, value any) []byte {
	t.Helper()
	body, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return body
}
