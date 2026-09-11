package controlhttp

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	wire "github.com/liuzengh/trpc-agent-service/api/schemas/channel/v1"
	app "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/application/preflight"
)

func TestWeComPreflightMutualTLSClosedContract(t *testing.T) {
	request, grant, _ := preflightFixture(t)
	cfg, err := app.NewWeComConfig(request.Config.ScopeID, request.Config.SourceEpoch)
	if err != nil {
		t.Fatal(err)
	}
	request.Config = cfg
	request.DiagnosticPolicy = wire.PreflightWeComPolicy
	grant.Provider = "wecom"
	grant.ProviderAccountID = "test-bot"
	grant.AllowConnectionProbe = true
	grant.WebhookPath = ""
	grant.WebhookSecretConfigured = false
	grant.DiagnosticPolicy = wire.PreflightWeComPolicy
	grant.ReceiveMode = wire.PreflightWeComMode
	grant.GatewayConfigDigest = cfg.Digest
	grant.EffectiveConfigDigest, err = wire.PreflightEffectiveConfigDigest(grant.ScopeID, grant.SourceEpoch, grant.ReceiveMode, grant.ConnectionRevision, nil, cfg.OriginStatus)
	if err != nil {
		t.Fatal(err)
	}
	grant.Credentials.Purpose = "wecom.bot_secret"
	var claims, resolves, completes int
	cl := preflightTLSFixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		preflightHeaders(w)
		if r.TLS == nil || len(r.TLS.VerifiedChains) == 0 {
			t.Error("missing mTLS")
		}
		raw, _ := io.ReadAll(r.Body)
		switch r.URL.Path {
		case "/internal/v1/channel-preflights:claim":
			claims++
			var body wire.PreflightClaimRequest
			if wire.Decode("preflight-claim.schema.json", raw, &body) != nil || body.DiagnosticPolicy != wire.PreflightWeComPolicy || body.ExpectedPublicOrigin != nil || body.OriginStatus != "PUBLIC_ORIGIN_NOT_APPLICABLE" {
				t.Error("wrong policy/config")
			}
			_ = json.NewEncoder(w).Encode(grant)
		case "/internal/v1/channel-preflights/cpf_test/credentials:resolve":
			resolves++
			_ = json.NewEncoder(w).Encode(wire.PreflightResolveResponse{SchemaVersion: 1, PreflightID: grant.PreflightID, ConnectionRevision: grant.ConnectionRevision, Purpose: "wecom.bot_secret", CredentialID: grant.Credentials.CredentialID, CredentialVersion: grant.Credentials.CredentialVersion, Value: "synthetic-private-secret", LeaseExpiresAt: grant.LeaseExpiresAt})
		case "/internal/v1/channel-preflights/cpf_test:complete":
			completes++
			var body wire.PreflightCompleteRequest
			if wire.Decode("preflight-complete.schema.json", raw, &body) != nil || body.DiagnosticPolicy != wire.PreflightWeComPolicy || len(body.Checks) != 3 {
				t.Error("wrong result contract")
			}
			if strings.Contains(string(raw), "synthetic-private-secret") {
				t.Error("secret leaked")
			}
			w.WriteHeader(204)
		default:
			t.Error("unexpected endpoint")
			w.WriteHeader(404)
		}
	}))
	g, err := cl.Claim(context.Background(), request)
	if err != nil || g == nil {
		t.Fatal(err)
	}
	secret, err := cl.ResolveCredential(context.Background(), *g)
	if err != nil || secret.Reveal() != "synthetic-private-secret" {
		t.Fatal("wrong scoped secret", err)
	}
	result := app.Result{Config: cfg, ObservedAt: grant.ServerTime, Checks: []app.Check{
		{ID: "credential_configuration", Status: "PASS", Code: "CREDENTIALS_CONFIGURED", Details: json.RawMessage(`{"bot_secret_configured":true}`)},
		{ID: "connection_authentication", Status: "PASS", Code: "WECOM_AUTHENTICATED", Details: json.RawMessage(`{"authenticated":true}`)},
		{ID: "delivery_verification", Status: "UNKNOWN", Code: "DELIVERY_NOT_TESTED", Details: json.RawMessage(`{"verification":"NOT_TESTED"}`)},
	}}
	if err = cl.Complete(context.Background(), *g, result); err != nil {
		t.Fatal(err)
	}
	result.Checks[0].Details = json.RawMessage(`{"bot_secret_configured":false}`)
	result.Checks[0].Code = "BOT_SECRET_MISSING"
	result.Checks[0].Status = "FAIL"
	result.Checks[1].Code = "NOT_EXECUTED"
	result.Checks[1].Status = "SKIPPED"
	result.Checks[1].Details = json.RawMessage(`{"authenticated":null}`)
	if err = cl.Complete(context.Background(), *g, result); err == nil {
		t.Fatal("metadata falsehood accepted")
	}
	g.AllowConnectionProbe = false
	if _, err = cl.ResolveCredential(context.Background(), *g); err == nil {
		t.Fatal("unconfirmed grant resolved")
	}
	if claims != 1 || resolves != 1 || completes != 1 {
		t.Fatalf("unexpected network %d %d %d", claims, resolves, completes)
	}
}
