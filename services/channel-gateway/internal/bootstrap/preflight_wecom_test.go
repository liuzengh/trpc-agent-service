package bootstrap

import (
	"context"
	"encoding/json"
	wire "github.com/liuzengh/trpc-agent-service/api/schemas/channel/v1"
	app "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/application/preflight"
	"net/http"
	"sync/atomic"
	"testing"
	"time"
)

func TestWeComOnlyRunnerWithoutTelegramOrigin(t *testing.T) {
	completed := make(chan struct{}, 1)
	var claims atomic.Int32
	cfg := controlTLSFiles(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/internal/v1/channel-preflights:claim" {
			claims.Add(1)
			var claim wire.PreflightClaimRequest
			if json.NewDecoder(r.Body).Decode(&claim) != nil {
				t.Error("invalid claim")
				return
			}
			if claim.DiagnosticPolicy != wire.PreflightWeComPolicy || claim.ExpectedPublicOrigin != nil || claim.OriginStatus != "PUBLIC_ORIGIN_NOT_APPLICABLE" {
				t.Error("incorrect provider policy")
			}
			now := time.Now().UTC()
			effective, err := wire.PreflightEffectiveConfigDigest(claim.ScopeID, claim.SourceEpoch, wire.PreflightWeComMode, 4, nil, claim.OriginStatus)
			if err != nil {
				t.Error(err)
			}
			grant := wire.PreflightGrant{SchemaVersion: 1, ServerTime: now, PreflightID: "cpf_wecom", ScopeID: claim.ScopeID, SourceEpoch: claim.SourceEpoch, TenantID: "tnt_test", AccountID: "cha_test", Provider: "wecom", ProviderAccountID: "test-bot", AccountRevision: 7, ConnectionRevision: 4, Credentials: wire.PreflightCredential{Purpose: "wecom.bot_secret", CredentialID: "ccr_test", CredentialVersion: 2, Configured: false}, LeaseEpoch: 1, LeaseExpiresAt: now.Add(30 * time.Second), JobDeadlineAt: now.Add(120 * time.Second), GatewayConfigDigest: claim.GatewayConfigDigest, DiagnosticPolicy: wire.PreflightWeComPolicy, ReceiveMode: wire.PreflightWeComMode, EffectiveConfigDigest: effective, AllowConnectionProbe: true}
			_ = json.NewEncoder(w).Encode(grant)
			return
		}
		if r.URL.Path == "/internal/v1/channel-preflights/cpf_wecom:complete" {
			var result wire.PreflightCompleteRequest
			_ = json.NewDecoder(r.Body).Decode(&result)
			if len(result.Checks) != 3 || result.Checks[0].Code != "BOT_SECRET_MISSING" || result.Checks[1].Code != "NOT_EXECUTED" || result.Checks[2].Status != "UNKNOWN" {
				t.Error("wrong result")
			}
			w.WriteHeader(204)
			completed <- struct{}{}
			return
		}
		t.Error("unexpected runtime or credential call")
		w.WriteHeader(404)
	}))
	cfg.PublicOrigin = ""
	runtime, err := newPreflight(Config{AccountSource: "control", WeComPreflightEnabled: true, Control: cfg, InstanceID: "gw"}, controlBoot)
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- runtime.Run(ctx) }()
	select {
	case <-completed:
	case <-time.After(4 * time.Second):
		t.Fatal("no result")
	}
	cancel()
	select {
	case err = <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("runner did not drain")
	}
	if claims.Load() != 1 {
		t.Fatal("unexpected duplicate claim")
	}
}

type timedClaimControl struct{ times chan time.Time }

func (c timedClaimControl) Claim(context.Context, app.ClaimRequest) (*app.Grant, error) {
	c.times <- time.Now()
	return nil, nil
}
func (c timedClaimControl) ResolveCredential(context.Context, app.Grant) (app.Secret, error) {
	return app.NewSecret("test"), nil
}
func (c timedClaimControl) Complete(context.Context, app.Grant, app.Result) error { return nil }
func TestProviderRunnersShareClaimRateGate(t *testing.T) {
	gate := &preflightClaimGate{slot: make(chan struct{}, 1)}
	calls := make(chan time.Time, 2)
	first, second := gate.wrap(timedClaimControl{calls}), gate.wrap(timedClaimControl{calls})
	if _, err := first.Claim(context.Background(), app.ClaimRequest{}); err != nil {
		t.Fatal(err)
	}
	if _, err := second.Claim(context.Background(), app.ClaimRequest{}); err != nil {
		t.Fatal(err)
	}
	a, b := <-calls, <-calls
	if b.Sub(a) < 500*time.Millisecond {
		t.Fatal("separate provider rate budgets")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := second.Claim(ctx, app.ClaimRequest{}); err != app.ErrExpired {
		t.Fatal("canceled request escaped gate")
	}
	if len(calls) != 0 {
		t.Fatal("canceled request reached Control")
	}
}
