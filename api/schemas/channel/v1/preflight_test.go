package channelv1

import (
	"bytes"
	"encoding/json"
	"os"
	"strings"
	"testing"
)

func TestPreflightFrozenConfigDigest(t *testing.T) {
	origin := "https://gateway.example.com"
	got, err := PreflightConfigDigest("pool", "11111111-1111-4111-8111-111111111111", &origin, "PUBLIC_ORIGIN_STATIC_VALID")
	if err != nil || got != "sha256:699f4ff6f77d9069bb632ac9e1f29b7a0f996780217aa18d127dfe135e7f314d" {
		t.Fatalf("digest=%s error=%v", got, err)
	}
}

func TestPreflightChecksAggregateOnlyDiagnosticItems(t *testing.T) {
	checks := validPreflightChecks(t)
	outcome, err := ValidatePreflightChecks(checks)
	if err != nil || outcome != "WARN" {
		t.Fatalf("outcome=%q error=%v", outcome, err)
	}
}

func validPreflightChecks(t *testing.T) []PreflightCheck {
	t.Helper()
	var checks []PreflightCheck
	if err := json.Unmarshal([]byte(`[
 {"id":"credential_configuration","status":"PASS","code":"CREDENTIALS_CONFIGURED","details":{"bot_token_configured":true,"webhook_secret_configured":true}},
 {"id":"bot_identity","status":"PASS","code":"BOT_IDENTITY_MATCH","details":{"identity_match":true}},
 {"id":"public_origin","status":"PASS","code":"PUBLIC_ORIGIN_STATIC_VALID","details":{"validation":"STATIC_ONLY"}},
 {"id":"webhook_registration","status":"WARN","code":"WEBHOOK_NONE","details":{"presence":false,"relation":"NONE"}},
 {"id":"pending_updates","status":"PASS","code":"PENDING_UPDATES_ZERO","details":{"pending_update_count":0}},
 {"id":"delivery_errors","status":"PASS","code":"DELIVERY_ERROR_NOT_REPORTED","details":{"has_last_error":false,"last_error_at":null}},
 {"id":"recovery_materials","status":"UNKNOWN","code":"RECOVERY_MATERIALS_UNAVAILABLE","details":{"secret_token_readable":false,"restore_available":false}},
 {"id":"delivery_verification","status":"UNKNOWN","code":"DELIVERY_NOT_TESTED","details":{"verification":"NOT_TESTED"}}
 ]`), &checks); err != nil {
		t.Fatal(err)
	}
	return checks
}

func TestPreflightConfigRejectsNoncanonicalAndNonpublicOrigins(t *testing.T) {
	for _, raw := range []string{"https://GATEWAY.EXAMPLE.COM:443/", "https://127.0.0.1", "https://gateway.example.com?CANARY", "https://u:CANARY@gateway.example.com"} {
		if _, err := PreflightConfigDigest("pool", "11111111-1111-4111-8111-111111111111", &raw, "PUBLIC_ORIGIN_STATIC_VALID"); err != ErrInvalidDocument {
			t.Fatalf("accepted raw origin: %v", err)
		}
	}
}

func TestPreflightClaimRequiresMatchingStaticDigest(t *testing.T) {
	raw := preflightFixture(t, "preflight-claim")
	raw = bytes.ReplaceAll(raw, []byte("699f4ff6f77d9069bb632ac9e1f29b7a0f996780217aa18d127dfe135e7f314d"), []byte(strings.Repeat("0", 64)))
	if err := Validate("preflight-claim.schema.json", raw); err != ErrInvalidDocument {
		t.Fatalf("digest mismatch accepted: %v", err)
	}
}
func preflightFixture(t *testing.T, name string) []byte {
	t.Helper()
	raw, err := os.ReadFile("fixtures/" + name + "-valid.json")
	if err != nil {
		t.Fatal(err)
	}
	var f struct {
		Document json.RawMessage `json:"document"`
	}
	if err := json.Unmarshal(raw, &f); err != nil {
		t.Fatal(err)
	}
	return f.Document
}

// Vectors agreed with the Gateway owner; inputs are never fetched or resolved.
func TestPreflightStaticOriginFrozenMatrix(t *testing.T) {
	cases := []struct {
		status string
		values []string
	}{
		{"PUBLIC_ORIGIN_STATIC_VALID", []string{"https://gateway.example.com:80", "https://gateway.example.com:88/", "https://gateway.example.com:8443", "HTTPS://GATEWAY.EXAMPLE.COM", "https://8.8.8.8", "https://[2606:4700:4700::1111]:8443", "https://xn--bcher-kva.example"}},
		{"PUBLIC_ORIGIN_INVALID", []string{"", "http://gateway.example.com", "https://u:p@gateway.example.com", "https://gateway.example.com?", "https://gateway.example.com#", "https://gateway.example.com/%2f", "https://gateway.example.com:444", "https://gateway.example.com:0443", "https://gateway.example.com:", "https://gateway.example.com.", "https://中文.example", "https://gateway_example.com", "https://-gateway.example.com", "https://gateway..example.com", "https://gateway.example.com/path", "https://gateway.example.com\\evil", " https://gateway.example.com", "https://[::1%25eth0]", "https://[gateway.example.com]"}},
		{"PUBLIC_ORIGIN_NOT_PUBLIC", []string{"https://localhost", "https://host", "https://api.localhost", "https://api.local", "https://api.internal", "https://host.home.arpa", "https://127.1", "https://0177.0.0.1", "https://0x7f.0.0.1", "https://0.0.0.0", "https://10.0.0.1", "https://100.64.0.1", "https://127.0.0.1", "https://169.254.169.254", "https://172.16.0.1", "https://192.0.0.8", "https://192.0.2.1", "https://192.88.99.1", "https://192.168.1.1", "https://198.18.0.1", "https://198.51.100.1", "https://203.0.113.1", "https://224.0.0.1", "https://240.0.0.1", "https://255.255.255.255", "https://[::]", "https://[::1]", "https://[fc00::1]", "https://[fe80::1]", "https://[ff02::1]", "https://[::ffff:127.0.0.1]", "https://[2001:db8::1]", "https://[2002:0808:0808::1]", "https://[3fff::1]"}},
	}
	for _, group := range cases {
		for _, input := range group.values {
			t.Run(input, func(t *testing.T) {
				origin, status := normalizePreflightOrigin(input)
				if status != group.status {
					t.Fatalf("status=%q expected=%q", status, group.status)
				}
				if group.status != "PUBLIC_ORIGIN_STATIC_VALID" && origin != nil {
					t.Fatal("retained invalid origin")
				}
				if _, err := PreflightConfigDigest("pool", "11111111-1111-4111-8111-111111111111", origin, status); err != nil {
					t.Fatal(err)
				}

			})
		}
	}
}

func TestPreflightUnavailableConfigDigestVectors(t *testing.T) {
	for status, want := range map[string]string{
		"PUBLIC_ORIGIN_NOT_PUBLIC": "sha256:63c09ea92b63a02e1750e76d92f3d6ea76be142d7b6d3dd1b6c590936b9ff58a",
		"PUBLIC_ORIGIN_INVALID":    "sha256:03f2b058b89dcf96f3b74459cf4851902e480cb112593a2e253c2d7527b03ed4",
	} {
		got, err := PreflightConfigDigest("pool", "11111111-1111-4111-8111-111111111111", nil, status)
		if err != nil || got != want {
			t.Fatalf("digest=%s err=%v", got, err)
		}
	}
	for _, tc := range []struct {
		scope, epoch, status string
		origin               *string
	}{
		{"../bad", "11111111-1111-4111-8111-111111111111", "PUBLIC_ORIGIN_INVALID", nil},
		{"pool", "bad", "PUBLIC_ORIGIN_INVALID", nil},
		{"pool", "11111111-1111-4111-8111-111111111111", "UNKNOWN", nil},
		{"pool", "11111111-1111-4111-8111-111111111111", "PUBLIC_ORIGIN_STATIC_VALID", nil},
	} {
		if _, err := PreflightConfigDigest(tc.scope, tc.epoch, tc.origin, tc.status); err != ErrInvalidDocument {
			t.Fatal("accepted invalid config", err)
		}
	}
}

func TestPreflightChecksRejectContradictionsAndLeaks(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func([]PreflightCheck) []PreflightCheck
	}{
		{"missing_item", func(c []PreflightCheck) []PreflightCheck { return c[:7] }},
		{"extra_item", func(c []PreflightCheck) []PreflightCheck { return append(c, c[7]) }},
		{"wrong_order", func(c []PreflightCheck) []PreflightCheck { c[0], c[1] = c[1], c[0]; return c }},
		{"wrong_status", func(c []PreflightCheck) []PreflightCheck { c[3].Status = "PASS"; return c }},
		{"unknown_code", func(c []PreflightCheck) []PreflightCheck { c[1].Code = "CANARY"; return c }},
		{"raw_webhook_url", func(c []PreflightCheck) []PreflightCheck {
			c[3].Details = json.RawMessage(`{"presence":false,"relation":"NONE","url":"https://other.invalid/CANARY"}`)
			return c
		}},
		{"duplicate_detail", func(c []PreflightCheck) []PreflightCheck {
			c[1].Details = json.RawMessage(`{"identity_match":true,"identity_match":false}`)
			return c
		}},
		{"missing_detail", func(c []PreflightCheck) []PreflightCheck { c[1].Details = json.RawMessage(`{}`); return c }},
		{"identity_fact_contradiction", func(c []PreflightCheck) []PreflightCheck {
			c[1].Details = json.RawMessage(`{"identity_match":false}`)
			return c
		}},
		{"unverified_bot_webhook_fact", func(c []PreflightCheck) []PreflightCheck {
			c[1] = PreflightCheck{"bot_identity", "FAIL", "TOKEN_REJECTED", json.RawMessage(`{"identity_match":null}`)}
			return c
		}},
		{"invalid_origin_false_match", func(c []PreflightCheck) []PreflightCheck {
			c[2].Code = "PUBLIC_ORIGIN_INVALID"
			c[2].Status = "FAIL"
			c[3] = PreflightCheck{"webhook_registration", "PASS", "WEBHOOK_MATCH", json.RawMessage(`{"presence":true,"relation":"MATCH"}`)}
			return c
		}},
		{"false_comparison_unknown", func(c []PreflightCheck) []PreflightCheck {
			c[3] = PreflightCheck{"webhook_registration", "UNKNOWN", "WEBHOOK_COMPARISON_UNAVAILABLE", json.RawMessage(`{"presence":true,"relation":"UNKNOWN"}`)}
			return c
		}},
		{"negative_pending", func(c []PreflightCheck) []PreflightCheck {
			c[4].Details = json.RawMessage(`{"pending_update_count":-1}`)
			return c
		}},
		{"unsafe_integer", func(c []PreflightCheck) []PreflightCheck {
			c[4] = PreflightCheck{"pending_updates", "WARN", "PENDING_UPDATES_PRESENT", json.RawMessage(`{"pending_update_count":9007199254740992}`)}
			return c
		}},
		{"fraction", func(c []PreflightCheck) []PreflightCheck {
			c[4] = PreflightCheck{"pending_updates", "WARN", "PENDING_UPDATES_PRESENT", json.RawMessage(`{"pending_update_count":1.5}`)}
			return c
		}},
		{"zero_marked_present", func(c []PreflightCheck) []PreflightCheck {
			c[4] = PreflightCheck{"pending_updates", "WARN", "PENDING_UPDATES_PRESENT", json.RawMessage(`{"pending_update_count":0}`)}
			return c
		}},
		{"raw_error", func(c []PreflightCheck) []PreflightCheck {
			c[5].Details = json.RawMessage(`{"has_last_error":true,"last_error_at":null,"last_error_message":"CANARY"}`)
			return c
		}},
		{"restore_claim", func(c []PreflightCheck) []PreflightCheck {
			c[6].Details = json.RawMessage(`{"secret_token_readable":true,"restore_available":true}`)
			return c
		}},
		{"delivery_claim", func(c []PreflightCheck) []PreflightCheck { c[7].Status = "PASS"; return c }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ValidatePreflightChecks(tc.edit(validPreflightChecks(t)))
			if err != ErrInvalidDocument {
				t.Fatalf("invalid result accepted: %v", err)
			}
		})
	}
}

func TestPreflightCheckOutcomeAndReadOnlyFailures(t *testing.T) {
	for _, tc := range []struct {
		name, want string
		edit       func([]PreflightCheck)
	}{
		{"pass_except_fixed_limits", "PASS", func(c []PreflightCheck) {
			c[3] = PreflightCheck{"webhook_registration", "PASS", "WEBHOOK_MATCH", json.RawMessage(`{"presence":true,"relation":"MATCH"}`)}
		}},
		{"token_missing_priority", "FAIL", func(c []PreflightCheck) {
			c[0] = PreflightCheck{"credential_configuration", "FAIL", "BOT_TOKEN_MISSING", json.RawMessage(`{"bot_token_configured":false,"webhook_secret_configured":false}`)}
			c[1] = PreflightCheck{"bot_identity", "SKIPPED", "NOT_EXECUTED", json.RawMessage(`{"identity_match":null}`)}
			skipPreflightWebhook(c)
		}},
		{"webhook_secret_missing", "FAIL", func(c []PreflightCheck) {
			c[0] = PreflightCheck{"credential_configuration", "FAIL", "WEBHOOK_SECRET_MISSING", json.RawMessage(`{"bot_token_configured":true,"webhook_secret_configured":false}`)}
		}},
		{"identity_network", "UNKNOWN", func(c []PreflightCheck) {
			c[1] = PreflightCheck{"bot_identity", "UNKNOWN", "PROVIDER_NETWORK", json.RawMessage(`{"identity_match":null}`)}
			skipPreflightWebhook(c)
		}},
		{"webhook_token_rejected", "FAIL", func(c []PreflightCheck) { skipPreflightWebhook(c); c[3].Status = "FAIL"; c[3].Code = "TOKEN_REJECTED" }},
		{"bad_origin_still_read_webhook", "FAIL", func(c []PreflightCheck) {
			c[2].Status = "FAIL"
			c[2].Code = "PUBLIC_ORIGIN_INVALID"
			c[3] = PreflightCheck{"webhook_registration", "UNKNOWN", "WEBHOOK_COMPARISON_UNAVAILABLE", json.RawMessage(`{"presence":true,"relation":"UNKNOWN"}`)}
		}},
		{"historical_error_without_timestamp", "WARN", func(c []PreflightCheck) {
			c[5] = PreflightCheck{"delivery_errors", "WARN", "DELIVERY_ERROR_REPORTED", json.RawMessage(`{"has_last_error":true,"last_error_at":null}`)}
		}},
		{"historical_error_timestamp", "WARN", func(c []PreflightCheck) {
			c[5] = PreflightCheck{"delivery_errors", "WARN", "DELIVERY_ERROR_REPORTED", json.RawMessage(`{"has_last_error":true,"last_error_at":"2026-09-06T04:00:00Z"}`)}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := validPreflightChecks(t)
			tc.edit(c)
			got, err := ValidatePreflightChecks(c)
			if err != nil || got != tc.want {
				t.Fatalf("outcome=%q err=%v", got, err)
			}
		})
	}
}
func skipPreflightWebhook(c []PreflightCheck) {
	c[3] = PreflightCheck{"webhook_registration", "SKIPPED", "NOT_EXECUTED", json.RawMessage(`{"presence":null,"relation":"UNKNOWN"}`)}
	c[4] = PreflightCheck{"pending_updates", "SKIPPED", "NOT_EXECUTED", json.RawMessage(`{"pending_update_count":null}`)}
	c[5] = PreflightCheck{"delivery_errors", "SKIPPED", "NOT_EXECUTED", json.RawMessage(`{"has_last_error":null,"last_error_at":null}`)}
}

func TestPreflightWiresRejectAmbiguousJSONAndForbiddenFields(t *testing.T) {
	for _, name := range []string{"preflight-create", "preflight-created", "preflight-view", "preflight-claim", "preflight-grant", "preflight-resolve", "preflight-resolved", "preflight-complete"} {
		raw := preflightFixture(t, name)
		var doc map[string]any
		if err := json.Unmarshal(raw, &doc); err != nil {
			t.Fatal(err)
		}
		var key string
		for k := range doc {
			key = k
			break
		}
		field, _ := json.Marshal(key)
		duplicate := bytes.Replace(raw, field, append(append([]byte{}, field...), append([]byte(`:null,`), field...)...), 1)
		for variant, rejected := range map[string][]byte{
			"duplicate":    duplicate,
			"trailing":     append(append([]byte{}, raw...), []byte(` {}`)...),
			"invalid_utf8": append(append([]byte{}, raw...), 0xff),
			"case_varied":  bytes.Replace(raw, field, []byte(`"`+strings.ToUpper(key)+`"`), 1),
		} {
			t.Run(name+"/"+variant, func(t *testing.T) {
				err := Validate(name+".schema.json", rejected)
				if err != ErrInvalidDocument {
					t.Fatalf("ambiguous input accepted: %v", err)
				}
			})
		}
	}
}

func TestPreflightCannotBroadenRuntimeConsumers(t *testing.T) {
	raw := []byte(`{"schema_version":1,"scope_id":"pool","source_epoch":"11111111-1111-4111-8111-111111111111","connection_revision":1,"consumer":{"kind":"telegram_preflight","instance_id":"gateway"},"uses":[{"purpose":"telegram.bot_token","credential_id":"ccr_a","credential_version":1}]}`)
	if err := Validate("credentials-resolve-request.schema.json", raw); err != ErrInvalidDocument {
		t.Fatal("normal runtime consumer broadened", err)
	}
	for _, name := range []string{"preflight-resolve", "preflight-claim"} {
		raw = preflightFixture(t, name)
		var doc map[string]any
		_ = json.Unmarshal(raw, &doc)
		doc["purpose"] = "telegram.webhook_secret"
		raw, _ = json.Marshal(doc)
		if err := Validate(name+".schema.json", raw); err != ErrInvalidDocument {
			t.Fatal("diagnostic purpose selector accepted", err)
		}
	}
}

func TestPreflightVersionsAndClaimTokensAreBounded(t *testing.T) {
	for _, v := range []any{0, -1, 1.5, 9007199254740992.0, nil, "1"} {
		raw := preflightFixture(t, "preflight-create")
		var d map[string]any
		_ = json.Unmarshal(raw, &d)
		d["expected_bot_token_version"] = v
		raw, _ = json.Marshal(d)
		if err := Validate("preflight-create.schema.json", raw); err != ErrInvalidDocument {
			t.Fatal("invalid version accepted", err)
		}
	}
	for _, v := range []string{strings.Repeat("A", 42), strings.Repeat("A", 44), strings.Repeat("A", 42) + "B", strings.Repeat("A", 42) + "=", strings.Repeat("A", 42) + "\n"} {
		raw := preflightFixture(t, "preflight-claim")
		var d map[string]any
		_ = json.Unmarshal(raw, &d)
		d["claim_token"] = v
		raw, _ = json.Marshal(d)
		if err := Validate("preflight-claim.schema.json", raw); err != ErrInvalidDocument {
			t.Fatal("noncanonical token accepted", err)
		}
	}
}
