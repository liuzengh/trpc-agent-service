package channelv1

import (
	"encoding/json"
	"strings"
	"testing"
)

func wecomChecks() []PreflightCheck {
	return []PreflightCheck{
		{ID: "credential_configuration", Status: "PASS", Code: "CREDENTIALS_CONFIGURED", Details: json.RawMessage(`{"bot_secret_configured":true}`)},
		{ID: "connection_authentication", Status: "PASS", Code: "WECOM_AUTHENTICATED", Details: json.RawMessage(`{"authenticated":true}`)},
		{ID: "delivery_verification", Status: "UNKNOWN", Code: "DELIVERY_NOT_TESTED", Details: json.RawMessage(`{"verification":"NOT_TESTED"}`)},
	}
}

func TestWeComPreflightChecksAndProviderIsolation(t *testing.T) {
	checks := wecomChecks()
	if outcome, err := ValidatePreflightChecksForMode(PreflightWeComPolicy, PreflightWeComMode, checks); err != nil || outcome != "PASS" {
		t.Fatal(outcome, err)
	}
	for _, policy := range []string{"", PreflightReceiveModesPolicy} {
		if _, err := ValidatePreflightChecksForMode(policy, "webhook", checks); err == nil {
			t.Fatal("Telegram policy accepted connection probe checks")
		}
	}
	for _, code := range []string{"PROVIDER_NETWORK", "PROVIDER_TIMEOUT", "PROVIDER_RESPONSE_INVALID", "PROVIDER_UNAVAILABLE", "WECOM_CONNECTION_REPLACED"} {
		checks[1] = PreflightCheck{ID: "connection_authentication", Status: "UNKNOWN", Code: code, Details: json.RawMessage(`{"authenticated":null}`)}
		if outcome, err := ValidatePreflightChecksForMode(PreflightWeComPolicy, PreflightWeComMode, checks); err != nil || outcome != "UNKNOWN" {
			t.Fatal(code, outcome, err)
		}
	}
	checks[1] = PreflightCheck{ID: "connection_authentication", Status: "FAIL", Code: "WECOM_AUTH_REJECTED", Details: json.RawMessage(`{"authenticated":false}`)}
	if outcome, err := ValidatePreflightChecksForMode(PreflightWeComPolicy, PreflightWeComMode, checks); err != nil || outcome != "FAIL" {
		t.Fatal(outcome, err)
	}
	checks[1] = PreflightCheck{ID: "connection_authentication", Status: "SKIPPED", Code: "NOT_EXECUTED", Details: json.RawMessage(`{"authenticated":null}`)}
	if _, err := ValidatePreflightChecksForMode(PreflightWeComPolicy, PreflightWeComMode, checks); err == nil {
		t.Fatal("configured secret skipped authentication")
	}
	checks[0] = PreflightCheck{ID: "credential_configuration", Status: "FAIL", Code: "BOT_SECRET_MISSING", Details: json.RawMessage(`{"bot_secret_configured":false}`)}
	if outcome, err := ValidatePreflightChecksForMode(PreflightWeComPolicy, PreflightWeComMode, checks); err != nil || outcome != "FAIL" {
		t.Fatal(outcome, err)
	}
	checks = wecomChecks()
	checks[1].Details = json.RawMessage(`{"authenticated":true,"secret":"CANARY"}`)
	if _, err := ValidatePreflightChecksForMode(PreflightWeComPolicy, PreflightWeComMode, checks); err == nil {
		t.Fatal("provider/credential detail escaped closed schema")
	}
}

func TestWeComDigestAndClaimPolicyAreIndependent(t *testing.T) {
	epoch := "11111111-1111-4111-8111-111111111111"
	digest, err := PreflightConfigDigestForPolicy(PreflightWeComPolicy, "pool", epoch, nil, "PUBLIC_ORIGIN_NOT_APPLICABLE")
	if err != nil || !strings.HasPrefix(digest, "sha256:") {
		t.Fatal(digest, err)
	}
	origin := "https://gateway.example.com"
	if _, err := PreflightConfigDigestForPolicy(PreflightWeComPolicy, "pool", epoch, &origin, "PUBLIC_ORIGIN_STATIC_VALID"); err == nil {
		t.Fatal("WeCom accepted Telegram inbound origin")
	}
	a, err := PreflightEffectiveConfigDigest("pool", epoch, PreflightWeComMode, 1, nil, "PUBLIC_ORIGIN_NOT_APPLICABLE")
	if err != nil {
		t.Fatal(err)
	}
	b, _ := PreflightEffectiveConfigDigest("pool", epoch, PreflightWeComMode, 2, nil, "PUBLIC_ORIGIN_NOT_APPLICABLE")
	if a == b || a == digest {
		t.Fatal("connection revision omitted from effective digest")
	}
	var claim PreflightClaimRequest
	if err := json.Unmarshal(preflightFixture(t, "preflight-claim"), &claim); err != nil {
		t.Fatal(err)
	}
	claim.DiagnosticPolicy, claim.GatewayConfigDigest, claim.ExpectedPublicOrigin, claim.OriginStatus = PreflightWeComPolicy, digest, nil, "PUBLIC_ORIGIN_NOT_APPLICABLE"
	raw, _ := json.Marshal(claim)
	if err := Validate("preflight-claim.schema.json", raw); err != nil {
		t.Fatal(err)
	}
	claim.DiagnosticPolicy = ""
	raw, _ = json.Marshal(claim)
	if Validate("preflight-claim.schema.json", raw) == nil {
		t.Fatal("old runner negotiated WeCom implicitly")
	}
}

func TestWeComCreateVersionFieldsAreExclusive(t *testing.T) {
	for _, raw := range []string{
		`{"expected_account_revision":1,"expected_connection_revision":1,"expected_bot_secret_version":1,"allow_connection_probe":true}`,
		`{"expected_account_revision":1,"expected_connection_revision":1,"expected_bot_secret_version":1}`,
	} {
		if Validate("preflight-create.schema.json", []byte(raw)) != nil {
			t.Fatal("application must classify missing explicit consent")
		}
	}
	for _, raw := range []string{
		`{"expected_account_revision":1,"expected_connection_revision":1,"expected_bot_secret_version":1,"expected_bot_token_version":1}`,
		`{"expected_account_revision":1,"expected_connection_revision":1,"expected_bot_secret_version":0,"allow_connection_probe":true}`,
		`{"expected_account_revision":1,"expected_connection_revision":1,"expected_bot_secret_version":1,"allow_connection_probe":"true"}`,
	} {
		if Validate("preflight-create.schema.json", []byte(raw)) == nil {
			t.Fatal("invalid version/confirmation accepted")
		}
	}
}
