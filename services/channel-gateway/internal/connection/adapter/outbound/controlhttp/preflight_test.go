package controlhttp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	wire "github.com/liuzengh/trpc-agent-service/api/schemas/channel/v1"
	p "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/application/preflight"
)

func preflightTLSFixture(t *testing.T, h http.Handler) *PreflightClient {
	t.Helper()
	normal, server := tlsFixture(t, h)
	tlsConfig := normal.transport.TLSClientConfig
	cl, err := NewPreflight(Options{BaseURL: server.URL, ScopeID: normal.scope, SourceEpoch: normal.epoch, InstanceID: normal.instance, RootCAs: tlsConfig.RootCAs, Certificate: tlsConfig.Certificates[0]})
	if err != nil {
		t.Fatal(err)
	}
	if cl.client == normal || cl.client.transport == normal.transport || cl.client.http == normal.http {
		t.Fatal("diagnostics reused normal connection pool")
	}
	if cl.client.http.Timeout != 5*time.Second || !cl.client.transport.DisableCompression || cl.client.transport.Proxy != nil || cl.client.transport.MaxResponseHeaderBytes != 16<<10 {
		t.Fatal("bounded transport policy changed")
	}
	t.Cleanup(cl.Close)
	return cl
}
func preflightFixture(t *testing.T) (p.ClaimRequest, wire.PreflightGrant, p.Grant) {
	t.Helper()
	config, err := p.NewConfig("pool", fixture().SourceEpoch, "https://gateway.example.com")
	if err != nil {
		t.Fatal(err)
	}
	req := p.ClaimRequest{Config: config, InstanceEpoch: "22222222-2222-4222-8222-222222222222", RequestID: "33333333-3333-4333-8333-333333333333", Token: p.NewSecret(strings.Repeat("A", 43))}
	// Deliberately years away from the test machine's clock: lease comparison
	// must be anchored to Control server_time, never time.Now().
	now := time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC)
	w := wire.PreflightGrant{SchemaVersion: 1, ServerTime: now, PreflightID: "cpf_test", ScopeID: "pool", SourceEpoch: config.SourceEpoch, TenantID: "tnt_test", AccountID: "cha_test", Provider: "telegram", ProviderAccountID: "123456789", AccountRevision: 7, ConnectionRevision: 4, WebhookPath: "/v1/telegram/cha_test", Credentials: wire.PreflightCredential{Purpose: "telegram.bot_token", CredentialID: "ccr_test", CredentialVersion: 2, Configured: true}, WebhookSecretConfigured: true, LeaseEpoch: 1, LeaseExpiresAt: now.Add(30 * time.Second), JobDeadlineAt: now.Add(120 * time.Second), GatewayConfigDigest: config.Digest}
	g := p.Grant{PreflightID: w.PreflightID, ScopeID: w.ScopeID, SourceEpoch: w.SourceEpoch, TenantID: w.TenantID, AccountID: w.AccountID, Provider: w.Provider, ProviderAccountID: w.ProviderAccountID, WebhookPath: w.WebhookPath, AccountRevision: w.AccountRevision, ConnectionRevision: w.ConnectionRevision, LeaseEpoch: w.LeaseEpoch, Credential: p.Credential{Purpose: w.Credentials.Purpose, ID: w.Credentials.CredentialID, Version: w.Credentials.CredentialVersion, Configured: true}, WebhookSecretConfigured: true, ServerTime: w.ServerTime, LeaseExpiresAt: w.LeaseExpiresAt, JobDeadlineAt: w.JobDeadlineAt, ConfigDigest: w.GatewayConfigDigest, Request: req}
	return req, w, g
}
func preflightResult(g p.Grant) p.Result {
	return p.Result{Config: g.Request.Config, ObservedAt: g.ServerTime.Add(3 * time.Second), Checks: []p.Check{
		{ID: "credential_configuration", Status: "PASS", Code: "CREDENTIALS_CONFIGURED", Details: json.RawMessage(`{"bot_token_configured":true,"webhook_secret_configured":true}`)},
		{ID: "bot_identity", Status: "PASS", Code: "BOT_IDENTITY_MATCH", Details: json.RawMessage(`{"identity_match":true}`)},
		{ID: "public_origin", Status: "PASS", Code: "PUBLIC_ORIGIN_STATIC_VALID", Details: json.RawMessage(`{"validation":"STATIC_ONLY"}`)},
		{ID: "webhook_registration", Status: "WARN", Code: "WEBHOOK_NONE", Details: json.RawMessage(`{"presence":false,"relation":"NONE"}`)},
		{ID: "pending_updates", Status: "PASS", Code: "PENDING_UPDATES_ZERO", Details: json.RawMessage(`{"pending_update_count":0}`)},
		{ID: "delivery_errors", Status: "PASS", Code: "DELIVERY_ERROR_NOT_REPORTED", Details: json.RawMessage(`{"has_last_error":false,"last_error_at":null}`)},
		{ID: "recovery_materials", Status: "UNKNOWN", Code: "RECOVERY_MATERIALS_UNAVAILABLE", Details: json.RawMessage(`{"secret_token_readable":false,"restore_available":false}`)},
		{ID: "delivery_verification", Status: "UNKNOWN", Code: "DELIVERY_NOT_TESTED", Details: json.RawMessage(`{"verification":"NOT_TESTED"}`)},
	}}
}
func preflightHeaders(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
}
func TestPreflightMutualTLSClaimResolveCompleteExactWire(t *testing.T) {
	req, grantWire, _ := preflightFixture(t)
	var calls atomic.Int32
	var completions [][]byte
	cl := preflightTLSFixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.TLS == nil || len(r.TLS.VerifiedChains) == 0 {
			t.Error("missing verified client certificate")
		}
		if r.Method != "POST" || r.URL.RawQuery != "" || r.Header.Get("Cache-Control") != "no-store" || r.Header.Get("Accept-Encoding") != "identity" {
			t.Error("request transport contract")
		}
		calls.Add(1)
		preflightHeaders(w)
		raw, _ := io.ReadAll(r.Body)
		var body map[string]json.RawMessage
		if json.Unmarshal(raw, &body) != nil {
			t.Error("invalid request")
		}
		switch r.URL.Path {
		case "/internal/v1/channel-preflights:claim":
			if len(body) != 10 || string(body["limit"]) != "1" || string(body["expected_public_origin"]) != `"https://gateway.example.com"` || string(body["origin_status"]) != `"PUBLIC_ORIGIN_STATIC_VALID"` || string(body["claim_token"]) != `"`+req.Token.Reveal()+`"` {
				t.Error("claim wire mismatch")
			}
			_ = json.NewEncoder(w).Encode(grantWire)
		case "/internal/v1/channel-preflights/cpf_test/credentials:resolve":
			if len(body) != 6 || body["uses"] != nil || body["purpose"] != nil {
				t.Error("resolve exposes runtime credential selectors")
			}
			_ = json.NewEncoder(w).Encode(wire.PreflightResolveResponse{SchemaVersion: 1, PreflightID: grantWire.PreflightID, ConnectionRevision: grantWire.ConnectionRevision, Purpose: "telegram.bot_token", CredentialID: grantWire.Credentials.CredentialID, CredentialVersion: grantWire.Credentials.CredentialVersion, Value: "synthetic-token-do-not-log", LeaseExpiresAt: grantWire.LeaseExpiresAt})
		case "/internal/v1/channel-preflights/cpf_test:complete":
			if len(body) != 10 || body["result"] != nil || body["outcome"] != nil || body["origin_status"] != nil {
				t.Error("complete envelope mismatch")
			}
			completions = append(completions, raw)
			w.WriteHeader(http.StatusNoContent)
		default:
			t.Error("unexpected runtime path")
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	g, err := cl.Claim(context.Background(), req)
	if err != nil || g == nil || g.Request.Token.Reveal() != req.Token.Reveal() || g.Credential.Version != 2 {
		t.Fatal("claim", err)
	}
	token, err := cl.ResolveCredential(context.Background(), *g)
	if err != nil || token.Reveal() != "synthetic-token-do-not-log" || strings.Contains(fmt.Sprintf("%#v", token), "synthetic") {
		t.Fatal("resolve", err)
	}
	result := preflightResult(*g)
	for i := 0; i < 2; i++ {
		if err := cl.Complete(context.Background(), *g, result); err != nil {
			t.Fatal("complete", err)
		}
	}
	if calls.Load() != 4 || len(completions) != 2 || string(completions[0]) != string(completions[1]) {
		t.Fatal("completed replay payload changed")
	}
}
func TestPreflightEmptyClaimAndInvalidLocalRequest(t *testing.T) {
	req, _, _ := preflightFixture(t)
	var calls atomic.Int32
	cl := preflightTLSFixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		preflightHeaders(w)
		w.WriteHeader(http.StatusNoContent)
	}))
	if got, err := cl.Claim(context.Background(), req); got != nil || err != nil {
		t.Fatal("empty claim", err)
	}
	for _, mutate := range []func(*p.ClaimRequest){
		func(r *p.ClaimRequest) { r.Config.ScopeID = "other" }, func(r *p.ClaimRequest) { r.Config.SourceEpoch = "00000000-0000-4000-8000-000000000002" }, func(r *p.ClaimRequest) { r.InstanceEpoch = "bad" }, func(r *p.ClaimRequest) { r.RequestID = "bad" }, func(r *p.ClaimRequest) { r.Token = p.NewSecret("bad") }, func(r *p.ClaimRequest) { r.Token = p.NewSecret(strings.Repeat("A", 42) + "B") }, func(r *p.ClaimRequest) { r.Config.Digest = "sha256:" + strings.Repeat("0", 64) },
	} {
		bad := req
		mutate(&bad)
		if _, err := cl.Claim(context.Background(), bad); !errors.Is(err, p.ErrInvalid) {
			t.Fatal("invalid local claim", err)
		}
	}
	if _, err := cl.Claim(nil, req); !errors.Is(err, p.ErrInvalid) {
		t.Fatal("nil context", err)
	}
	if calls.Load() != 1 {
		t.Fatal("invalid local request reached transport")
	}
}
func TestPreflightClaimRejectsMalformedOrMismatchedGrant(t *testing.T) {
	req, w, _ := preflightFixture(t)
	raw, _ := json.Marshal(w)
	cases := map[string]string{
		"duplicate":       strings.Replace(string(raw), `"schema_version":1`, `"schema_version":1,"schema_version":1`, 1),
		"case":            strings.Replace(string(raw), `"schema_version"`, `"Schema_Version"`, 1),
		"missing_false":   strings.Replace(string(raw), `"webhook_secret_configured":true,`, ``, 1),
		"null_scalar":     strings.Replace(string(raw), `"webhook_secret_configured":true`, `"webhook_secret_configured":null`, 1),
		"null_credential": strings.Replace(string(raw), `"configured":true`, `"configured":null`, 1),
		"unsafe":          strings.Replace(string(raw), `"account_revision":7`, `"account_revision":9007199254740992`, 1),
		"wrong_scope":     strings.Replace(string(raw), `"scope_id":"pool"`, `"scope_id":"other"`, 1),
		"wrong_epoch":     strings.Replace(string(raw), w.SourceEpoch, "00000000-0000-4000-8000-000000000099", 1),
		"wrong_provider":  strings.Replace(string(raw), `"provider":"telegram"`, `"provider":"wecom"`, 1),
		"wrong_purpose":   strings.Replace(string(raw), `"telegram.bot_token"`, `"telegram.webhook_secret"`, 1),
		"wrong_digest":    strings.Replace(string(raw), w.GatewayConfigDigest, "sha256:"+strings.Repeat("0", 64), 1),
		"wrong_path":      strings.Replace(string(raw), `/v1/telegram/cha_test`, `/v1/telegram/cha_other`, 1),
		"trailing":        string(raw) + ` {}`,
		"unknown":         strings.Replace(string(raw), `"configured":true`, `"configured":true,"value":"secret"`, 1),
		"zero_revision":   strings.Replace(string(raw), `"connection_revision":4`, `"connection_revision":0`, 1),
		"non_utc":         strings.Replace(string(raw), `2020-01-02T03:04:05Z`, `2020-01-02T03:04:05+00:00`, 1),
		"envelope":        `{"grant":` + string(raw) + `}`,
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			cl := preflightTLSFixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { preflightHeaders(w); _, _ = io.WriteString(w, body) }))
			if got, err := cl.Claim(context.Background(), req); got != nil || err == nil {
				t.Fatal("accepted invalid grant")
			}
		})
	}
	w.LeaseExpiresAt = w.ServerTime
	body, _ := json.Marshal(w)
	cl := preflightTLSFixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { preflightHeaders(w); _, _ = w.Write(body) }))
	if got, err := cl.Claim(context.Background(), req); got != nil || !errors.Is(err, p.ErrExpired) {
		t.Fatal("expired grant", err)
	}
}
func TestPreflightStrictDecoder(t *testing.T) {
	_, grant, _ := preflightFixture(t)
	raw, _ := json.Marshal(grant)
	original := string(raw)
	var out wire.PreflightGrant
	for _, value := range []string{`7,"account_revision":8`, `null`, `"7"`, `9007199254740992`, `1e400`, strings.Repeat("[", 18) + `0` + strings.Repeat("]", 18)} {
		bad := strings.Replace(original, `"account_revision":7`, `"account_revision":`+value, 1)
		if err := preflightWireError(wire.Decode("preflight-grant.schema.json", []byte(bad), &out)); !errors.Is(err, p.ErrInvalid) {
			t.Fatal("malformed JSON accepted", err)
		}
	}
	for _, bad := range []string{strings.Replace(original, `"account_revision"`, `"ACCOUNT_REVISION"`, 1), original + `true`, strings.Replace(original, `"credentials":{`, `"credentials":{"bad":"\xff",`, 1)} {
		if err := preflightWireError(wire.Decode("preflight-grant.schema.json", []byte(bad), &out)); !errors.Is(err, p.ErrInvalid) {
			t.Fatal("malformed JSON accepted", err)
		}
	}
	safe := strings.Replace(original, `"account_revision":7`, `"account_revision":9007199254740991`, 1)
	if err := wire.Decode("preflight-grant.schema.json", []byte(safe), &out); err != nil {
		t.Fatal(err)
	}
}

func TestPreflightResolveExactVersionLeaseAndRequiredFields(t *testing.T) {
	_, _, g := preflightFixture(t)
	w := wire.PreflightResolveResponse{SchemaVersion: 1, PreflightID: g.PreflightID, ConnectionRevision: g.ConnectionRevision, Purpose: "telegram.bot_token", CredentialID: g.Credential.ID, CredentialVersion: g.Credential.Version, Value: "synthetic-do-not-echo", LeaseExpiresAt: g.LeaseExpiresAt}
	raw, _ := json.Marshal(w)
	cases := map[string]string{
		"task":            strings.Replace(string(raw), g.PreflightID, "cpf_other", 1),
		"connection":      strings.Replace(string(raw), `"connection_revision":4`, `"connection_revision":5`, 1),
		"version":         strings.Replace(string(raw), `"credential_version":2`, `"credential_version":3`, 1),
		"id":              strings.Replace(string(raw), g.Credential.ID, "ccr_other", 1),
		"purpose":         strings.Replace(string(raw), `telegram.bot_token`, `telegram.webhook_secret`, 1),
		"lease":           strings.Replace(string(raw), `2020-01-02T03:04:35Z`, `2020-01-02T03:04:36Z`, 1),
		"missing":         strings.Replace(string(raw), `"credential_version":2,`, ``, 1),
		"empty":           strings.Replace(string(raw), `synthetic-do-not-echo`, ``, 1),
		"too_large_value": strings.Replace(string(raw), `synthetic-do-not-echo`, strings.Repeat("s", (16<<10)+1), 1),
		"unsafe":          strings.Replace(string(raw), `"credential_version":2`, `"credential_version":9007199254740992`, 1),
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			cl := preflightTLSFixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { preflightHeaders(w); _, _ = io.WriteString(w, body) }))
			token, err := cl.ResolveCredential(context.Background(), g)
			if token.Reveal() != "" || !errors.Is(err, p.ErrInvalid) || strings.Contains(err.Error(), "synthetic") {
				t.Fatal("accepted mismatched or unsafe resolve", err)
			}
		})
	}
	var calls atomic.Int32
	cl := preflightTLSFixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1) }))
	g.Credential.Configured = false
	if _, err := cl.ResolveCredential(context.Background(), g); !errors.Is(err, p.ErrInvalid) || calls.Load() != 0 {
		t.Fatal("missing credential reached transport")
	}
}
func TestPreflightHTTPFailuresBoundedSanitizedAndNoRetry(t *testing.T) {
	req, _, _ := preflightFixture(t)
	for _, tc := range []struct {
		status int
		code   string
		want   error
	}{
		{400, "", p.ErrInvalid}, {401, "", p.ErrDenied}, {403, "", p.ErrDenied}, {404, "", p.ErrDenied}, {409, "CHANNEL_PREFLIGHT_LEASE_EXPIRED", p.ErrExpired}, {409, "CHANNEL_PREFLIGHT_REQUESTER_REVOKED", p.ErrConflict}, {409, "CHANNEL_PREFLIGHT_RESULT_CONFLICT", p.ErrConflict}, {429, "", p.ErrUnavailable}, {503, "", p.ErrUnavailable}, {307, "", p.ErrUnavailable},
	} {
		t.Run(fmt.Sprint(tc.status, tc.code), func(t *testing.T) {
			var calls atomic.Int32
			cl := preflightTLSFixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				w.Header().Set("Location", "https://must-not-visit.example/canary-secret")
				w.WriteHeader(tc.status)
				_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]string{"code": tc.code, "message": "canary-secret"}})
			}))
			_, err := cl.Claim(context.Background(), req)
			if !errors.Is(err, tc.want) || calls.Load() != 1 || strings.Contains(err.Error(), "canary") || strings.Contains(err.Error(), "http") {
				t.Fatal("failure escaped or retried", err)
			}
		})
	}
	for _, kind := range []string{"compressed", "oversized", "chunked_oversized", "no_store_missing", "wrong_content_type", "truncated"} {
		t.Run(kind, func(t *testing.T) {
			cl := preflightTLSFixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				preflightHeaders(w)
				switch kind {
				case "compressed":
					w.Header().Set("Content-Encoding", "gzip")
				case "no_store_missing":
					w.Header().Del("Cache-Control")
				case "wrong_content_type":
					w.Header().Set("Content-Type", "text/plain")
				case "oversized":
					w.Header().Set("Content-Length", fmt.Sprint(preflightResponseLimit+1))
				case "chunked_oversized":
					w.(http.Flusher).Flush()
				case "truncated":
					w.Header().Set("Content-Length", "200")
				}
				if strings.Contains(kind, "oversized") {
					_, _ = io.WriteString(w, strings.Repeat("x", preflightResponseLimit+1))
				} else {
					_, _ = io.WriteString(w, `{}`)
				}
			}))
			if _, err := cl.Claim(context.Background(), req); err == nil {
				t.Fatal("accepted unsafe response")
			}
		})
	}
}
func TestPreflightContextCancellation(t *testing.T) {
	req, _, _ := preflightFixture(t)
	cl := preflightTLSFixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		select {
		case <-r.Context().Done():
		case <-time.After(time.Second):
		}
	}))
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	started := time.Now()
	if _, err := cl.Claim(ctx, req); !errors.Is(err, p.ErrExpired) || time.Since(started) > time.Second {
		t.Fatal("parent deadline ignored", err)
	}
}
func TestPreflightCompleteRejectsUnsafeResultsBeforeNetwork(t *testing.T) {
	_, _, g := preflightFixture(t)
	var calls atomic.Int32
	cl := preflightTLSFixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1); preflightHeaders(w); w.WriteHeader(204) }))
	for name, mutate := range map[string]func(*p.Result){
		"missing_check": func(r *p.Result) { r.Checks = r.Checks[:7] },
		"reordered":     func(r *p.Result) { r.Checks[0], r.Checks[1] = r.Checks[1], r.Checks[0] },
		"metadata": func(r *p.Result) {
			r.Checks[0].Details = json.RawMessage(`{"bot_token_configured":false,"webhook_secret_configured":true}`)
		},
		"secret_field": func(r *p.Result) {
			r.Checks[3].Details = json.RawMessage(`{"presence":false,"relation":"NONE","url":"canary-secret"}`)
		},
		"case_field": func(r *p.Result) { r.Checks[1].Details = json.RawMessage(`{"Identity_match":true}`) },
		"duplicate": func(r *p.Result) {
			r.Checks[1].Details = json.RawMessage(`{"identity_match":true,"identity_match":true}`)
		},
		"unsafe_count": func(r *p.Result) { r.Checks[4].Details = json.RawMessage(`{"pending_update_count":9007199254740992}`) },
		"wrong_status": func(r *p.Result) { r.Checks[7].Status = "PASS" },
		"false_recovery": func(r *p.Result) {
			r.Checks[6].Details = json.RawMessage(`{"secret_token_readable":false,"restore_available":true}`)
		},
		"wrong_identity": func(r *p.Result) { r.Checks[1].Details = json.RawMessage(`{"identity_match":false}`) },
		"wrong_relation": func(r *p.Result) { r.Checks[3].Details = json.RawMessage(`{"presence":true,"relation":"MATCH"}`) },
		"wrong_digest":   func(r *p.Result) { r.Config.Digest = "sha256:" + strings.Repeat("0", 64) },
		"no_time":        func(r *p.Result) { r.ObservedAt = time.Time{} },
		"origin_code":    func(r *p.Result) { r.Checks[2].Code = "PUBLIC_ORIGIN_INVALID" },
	} {
		t.Run(name, func(t *testing.T) {
			r := preflightResult(g)
			mutate(&r)
			if err := cl.Complete(context.Background(), g, r); !errors.Is(err, p.ErrInvalid) {
				t.Fatal("unsafe result reached transport", err)
			}
		})
	}
	if calls.Load() != 0 {
		t.Fatal("invalid complete performed network I/O")
	}
	if err := cl.Complete(context.Background(), g, preflightResult(g)); err != nil || calls.Load() != 1 {
		t.Fatal("valid result", err)
	}
}

func TestPreflightRejectsMissingClientCertificate(t *testing.T) {
	req, _, _ := preflightFixture(t)
	var calls atomic.Int32
	cl := preflightTLSFixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1); preflightHeaders(w); w.WriteHeader(204) }))
	// The connection is fresh and no transport goroutine is using this config.
	cl.client.transport.TLSClientConfig.Certificates = nil
	if _, err := cl.Claim(context.Background(), req); !errors.Is(err, p.ErrUnavailable) || calls.Load() != 0 {
		t.Fatal("unauthenticated request reached handler", err)
	}
}
func TestPreflightClaimReplayConsumesFreshServerTimeWithoutNewLease(t *testing.T) {
	req, w, _ := preflightFixture(t)
	var calls atomic.Int32
	cl := preflightTLSFixture(t, http.HandlerFunc(func(out http.ResponseWriter, r *http.Request) {
		preflightHeaders(out)
		reply := w
		reply.ServerTime = reply.ServerTime.Add(time.Duration(calls.Add(1)-1) * 10 * time.Second)
		_ = json.NewEncoder(out).Encode(reply)
	}))
	first, err := cl.Claim(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	second, err := cl.Claim(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if second.ServerTime.Sub(first.ServerTime) != 10*time.Second || !second.LeaseExpiresAt.Equal(first.LeaseExpiresAt) || second.Request.RequestID != first.Request.RequestID || second.Request.Token.Reveal() != first.Request.Token.Reveal() {
		t.Fatal("claim replay extended or froze server-time grant")
	}
}
func TestPreflightCompletedReceiptAllowsClockDriftAndConflictStops(t *testing.T) {
	_, _, g := preflightFixture(t)
	var calls atomic.Int32
	cl := preflightTLSFixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		preflightHeaders(w)
		if calls.Add(1) == 1 {
			w.WriteHeader(204)
		} else {
			w.WriteHeader(409)
			_, _ = io.WriteString(w, `{"error":{"code":"CHANNEL_PREFLIGHT_RESULT_CONFLICT","message":"canary-secret"}}`)
		}
	}))
	// The adapter does not use local wall time to reject confirmation of an
	// already committed historical receipt; Control arbitrates first vs replay.
	if err := cl.Complete(context.Background(), g, preflightResult(g)); err != nil {
		t.Fatal(err)
	}
	result := preflightResult(g)
	result.ObservedAt = result.ObservedAt.Add(time.Second)
	if err := cl.Complete(context.Background(), g, result); !errors.Is(err, p.ErrConflict) || calls.Load() != 2 {
		t.Fatal("conflict retried or misclassified", err)
	}
}
func TestPreflightCompleteAcceptsClosedDiagnosticBranches(t *testing.T) {
	_, _, g := preflightFixture(t)
	for _, branch := range []string{"identity_rejected", "identity_network", "identity_mismatch", "webhook_token_revoked", "invalid_origin_existing_webhook", "missing_token", "missing_secret", "webhook_different", "pending_error"} {
		t.Run(branch, func(t *testing.T) {
			grant := g
			r := preflightResult(g)
			skipDownstream := func() {
				r.Checks[3] = p.Check{ID: "webhook_registration", Code: "NOT_EXECUTED", Status: "SKIPPED", Details: json.RawMessage(`{"presence":null,"relation":"UNKNOWN"}`)}
				r.Checks[4] = p.Check{ID: "pending_updates", Code: "NOT_EXECUTED", Status: "SKIPPED", Details: json.RawMessage(`{"pending_update_count":null}`)}
				r.Checks[5] = p.Check{ID: "delivery_errors", Code: "NOT_EXECUTED", Status: "SKIPPED", Details: json.RawMessage(`{"has_last_error":null,"last_error_at":null}`)}
			}
			switch branch {
			case "identity_rejected":
				skipDownstream()
				r.Checks[1] = p.Check{ID: "bot_identity", Code: "TOKEN_REJECTED", Status: "FAIL", Details: json.RawMessage(`{"identity_match":null}`)}
			case "identity_network":
				skipDownstream()
				r.Checks[1] = p.Check{ID: "bot_identity", Code: "PROVIDER_NETWORK", Status: "UNKNOWN", Details: json.RawMessage(`{"identity_match":null}`)}
			case "identity_mismatch":
				skipDownstream()
				r.Checks[1] = p.Check{ID: "bot_identity", Code: "BOT_IDENTITY_MISMATCH", Status: "FAIL", Details: json.RawMessage(`{"identity_match":false}`)}
			case "webhook_token_revoked":
				skipDownstream()
				r.Checks[3].Code = "TOKEN_REJECTED"
				r.Checks[3].Status = "FAIL"
			case "invalid_origin_existing_webhook":
				config, err := p.NewConfig(g.ScopeID, g.SourceEpoch, "https://localhost")
				if err != nil {
					t.Fatal(err)
				}
				grant.Request.Config = config
				grant.ConfigDigest = config.Digest
				r.Config = config
				r.Checks[2].Code = config.OriginStatus
				r.Checks[2].Status = "FAIL"
				r.Checks[3] = p.Check{ID: "webhook_registration", Code: "WEBHOOK_COMPARISON_UNAVAILABLE", Status: "UNKNOWN", Details: json.RawMessage(`{"presence":true,"relation":"UNKNOWN"}`)}
			case "missing_token":
				grant.Credential.Configured = false
				r.Checks[0] = p.Check{ID: "credential_configuration", Code: "BOT_TOKEN_MISSING", Status: "FAIL", Details: json.RawMessage(`{"bot_token_configured":false,"webhook_secret_configured":true}`)}
				skipDownstream()
				r.Checks[1] = p.Check{ID: "bot_identity", Code: "NOT_EXECUTED", Status: "SKIPPED", Details: json.RawMessage(`{"identity_match":null}`)}
			case "missing_secret":
				grant.WebhookSecretConfigured = false
				r.Checks[0] = p.Check{ID: "credential_configuration", Code: "WEBHOOK_SECRET_MISSING", Status: "FAIL", Details: json.RawMessage(`{"bot_token_configured":true,"webhook_secret_configured":false}`)}
			case "webhook_different":
				r.Checks[3] = p.Check{ID: "webhook_registration", Code: "WEBHOOK_DIFFERENT", Status: "WARN", Details: json.RawMessage(`{"presence":true,"relation":"DIFFERENT"}`)}
			case "pending_error":
				r.Checks[4] = p.Check{ID: "pending_updates", Code: "PENDING_UPDATES_PRESENT", Status: "WARN", Details: json.RawMessage(`{"pending_update_count":42}`)}
				r.Checks[5] = p.Check{ID: "delivery_errors", Code: "DELIVERY_ERROR_REPORTED", Status: "WARN", Details: json.RawMessage(`{"has_last_error":true,"last_error_at":"2020-01-02T03:04:00Z"}`)}
			}
			if err := validatePreflightResult(grant, r); err != nil {
				t.Fatal("closed diagnostic branch rejected", err)
			}
		})
	}
}

func TestPreflightCompleteAcceptsUTCOffsetWithoutLocationIdentity(t *testing.T) {
	_, _, g := preflightFixture(t)
	r := preflightResult(g)
	r.ObservedAt = r.ObservedAt.In(time.FixedZone("zero-offset", 0))
	if err := validatePreflightResult(g, r); err != nil {
		t.Fatal("UTC value rejected due to Location identity", err)
	}
	r.ObservedAt = r.ObservedAt.In(time.FixedZone("non-UTC", 3600))
	if err := validatePreflightResult(g, r); !errors.Is(err, p.ErrInvalid) {
		t.Fatal("non-UTC value accepted", err)
	}
}

// A timed-out HTTP attempt is uncertain, not proof that the enclosing diagnostic
// lease expired. The runner must still be able to retransmit the same request.
func TestPreflightAttemptTimeoutRemainsRetryable(t *testing.T) {
	for _, operation := range []string{"claim", "complete"} {
		for _, parentExpired := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/parent_expired=%v", operation, parentExpired), func(t *testing.T) {
				req, _, g := preflightFixture(t)
				var calls atomic.Int32
				cl := preflightTLSFixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					calls.Add(1)
					_, _ = io.Copy(io.Discard, r.Body)
					select {
					case <-r.Context().Done():
					case <-time.After(time.Second):
					}
				}))
				parent, cancel := context.WithCancel(context.Background())
				defer cancel()
				cl.client.http.Timeout = 40 * time.Millisecond
				if parentExpired {
					parent, cancel = context.WithTimeout(parent, 20*time.Millisecond)
					defer cancel()
					cl.client.http.Timeout = time.Second
				}
				var err error
				if operation == "claim" {
					_, err = cl.Claim(parent, req)
				} else {
					err = cl.Complete(parent, g, preflightResult(g))
				}
				want := p.ErrUnavailable
				if parentExpired {
					want = p.ErrExpired
				}
				if !errors.Is(err, want) || calls.Load() != 1 {
					t.Fatal("wrong attempt/parent timeout classification", err)
				}
				if !parentExpired && parent.Err() != nil {
					t.Fatal("HTTP timeout expired the caller context")
				}
			})
		}
	}
}
func TestPreflightResponseBodyTimeoutUsesParentDeadline(t *testing.T) {
	req, w, _ := preflightFixture(t)
	raw, _ := json.Marshal(w)
	for _, parentExpired := range []bool{false, true} {
		t.Run(fmt.Sprint(parentExpired), func(t *testing.T) {
			cl := preflightTLSFixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				preflightHeaders(w)
				w.Header().Set("Content-Length", fmt.Sprint(len(raw)))
				_, _ = w.Write(raw[:1])
				w.(http.Flusher).Flush()
				select {
				case <-r.Context().Done():
				case <-time.After(time.Second):
				}
			}))
			parent, cancel := context.WithCancel(context.Background())
			defer cancel()
			cl.client.http.Timeout = 40 * time.Millisecond
			if parentExpired {
				parent, cancel = context.WithTimeout(parent, 20*time.Millisecond)
				defer cancel()
				cl.client.http.Timeout = time.Second
			}
			_, err := cl.Claim(parent, req)
			want := p.ErrUnavailable
			if parentExpired {
				want = p.ErrExpired
			}
			if !errors.Is(err, want) {
				t.Fatal("body timeout misclassified", err)
			}
		})
	}
}
func TestPreflightFiveSecondChildDeadlineDoesNotExpireCaller(t *testing.T) {
	for _, operation := range []string{"claim", "complete"} {
		t.Run(operation, func(t *testing.T) {
			t.Parallel()
			req, _, g := preflightFixture(t)
			cl := preflightTLSFixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				select {
				case <-r.Context().Done():
				case <-time.After(6 * time.Second):
				}
			}))
			// Leave Client/transport timeouts later than the unchanged production 5s
			// exchange deadline, so this specifically regresses the shadowed context bug.
			cl.client.http.Timeout = 10 * time.Second
			cl.client.transport.ResponseHeaderTimeout = 10 * time.Second
			parent, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			var err error
			if operation == "claim" {
				_, err = cl.Claim(parent, req)
			} else {
				err = cl.Complete(parent, g, preflightResult(g))
			}
			if parent.Err() != nil || !errors.Is(err, p.ErrUnavailable) {
				t.Fatal("child HTTP deadline became task expiry", err)
			}
		})
	}
}
