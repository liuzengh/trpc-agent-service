package bootstrap

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	wire "github.com/liuzengh/trpc-agent-service/api/schemas/channel/v1"
)

func TestPreflightBootstrapDisabledAccountWithoutRuntime(t *testing.T) {
	var claims, completes, unexpected atomic.Int32
	submitted := make(chan struct{}, 1)
	cfg := controlTLSFiles(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("Content-Type", "application/json")
		if r.TLS == nil || len(r.TLS.VerifiedChains) == 0 {
			t.Error("missing mTLS identity")
		}
		raw, _ := io.ReadAll(io.LimitReader(r.Body, 16385))
		var body map[string]json.RawMessage
		if json.Unmarshal(raw, &body) != nil {
			t.Error("invalid request")
		}
		switch r.URL.Path {
		case "/internal/v1/channel-preflights:claim":
			claims.Add(1)
			var scope, epoch, digest string
			_ = json.Unmarshal(body["scope_id"], &scope)
			_ = json.Unmarshal(body["source_epoch"], &epoch)
			_ = json.Unmarshal(body["gateway_config_digest"], &digest)
			if wire.Validate("preflight-claim.schema.json", raw) != nil || len(body) != 11 || body["origin_status"] == nil || string(body["limit"]) != "1" {
				t.Error("claim wire drift")
			}
			now := time.Now().UTC()
			grant := map[string]any{"schema_version": 1, "server_time": now, "preflight_id": "cpf_disabled", "scope_id": scope, "source_epoch": epoch, "tenant_id": "tnt_test", "account_id": "cha_disabled", "provider": "telegram", "provider_account_id": "123", "account_revision": 7, "connection_revision": 4, "webhook_path": "/v1/telegram/cha_disabled", "credentials": map[string]any{"purpose": "telegram.bot_token", "credential_id": "ccr_token", "credential_version": 2, "configured": false}, "webhook_secret_configured": false, "lease_epoch": 1, "lease_expires_at": now.Add(30 * time.Second), "job_deadline_at": now.Add(120 * time.Second), "gateway_config_digest": digest}
			var origin *string
			var originStatus string
			_ = json.Unmarshal(body["expected_public_origin"], &origin)
			_ = json.Unmarshal(body["origin_status"], &originStatus)
			effective, e := wire.PreflightEffectiveConfigDigest(scope, epoch, "webhook", 4, origin, originStatus)
			if e != nil {
				t.Error(e)
			}
			grant["receive_mode"] = "webhook"
			grant["diagnostic_policy"] = wire.PreflightReceiveModesPolicy
			grant["effective_config_digest"] = effective
			if e := json.NewEncoder(w).Encode(grant); e != nil {
				t.Error(e)
			}
		case "/internal/v1/channel-preflights/cpf_disabled:complete":
			completes.Add(1)
			var checks []struct{ ID, Status, Code string }
			if json.Unmarshal(body["checks"], &checks) != nil || len(checks) != 8 {
				t.Error("missing eight checks")
			} else {
				if checks[0].Code != "BOT_TOKEN_MISSING" || checks[1].Status != "SKIPPED" || checks[6].Status != "UNKNOWN" || checks[7].Code != "DELIVERY_NOT_TESTED" {
					t.Error("incorrect disabled diagnostic")
				}
			}
			if body["result"] != nil || body["outcome"] != nil {
				t.Error("legacy result envelope")
			}
			w.WriteHeader(http.StatusNoContent)
			select {
			case submitted <- struct{}{}:
			default:
			}
		default:
			unexpected.Add(1)
			t.Error("runtime endpoint or credential resolve called for missing Token")
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	runtime, e := newPreflight(Config{AccountSource: "control", TelegramPreflightEnabled: true, Control: cfg, InstanceID: "gw"}, controlBoot)
	if e != nil {
		t.Fatal(e)
	}
	defer runtime.Close()
	if claims.Load() != 0 {
		t.Fatal("bootstrap performed network call")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- runtime.Run(ctx) }()
	select {
	case <-submitted:
	case <-time.After(4 * time.Second):
		t.Fatal("diagnostic did not complete")
	}
	cancel()
	select {
	case e := <-done:
		if e != nil {
			t.Fatal(e)
		}
	case <-time.After(time.Second):
		t.Fatal("diagnostic did not stop")
	}
	if claims.Load() != 1 || completes.Load() != 1 || unexpected.Load() != 0 {
		t.Fatal("unexpected request counts")
	}
}
func TestPreflightBootstrapSwitchDoesNotAffectFixtures(t *testing.T) {
	for _, cfg := range []Config{{AccountSource: "fixture", TelegramPreflightEnabled: true}, {AccountSource: "control", TelegramPreflightEnabled: false}, {}} {
		r, e := newPreflight(cfg, "")
		if e != nil || r != nil {
			t.Fatal("unconfigured diagnostic was started")
		}
	}
}
func TestPreflightLoadConfigExplicitSwitch(t *testing.T) {
	// Preflight is independent at runtime, but LoadConfig still validates the
	// complete V1 workload, including its migration identity and Final proof peer.
	for key, value := range map[string]string{
		"GATEWAY_MIGRATION_DATABASE_URL": "postgres://migrator/db",
		"GATEWAY_WORKER_URL":             "https://worker.example",
		"GATEWAY_WORKER_CA_FILE":         "ca.pem",
		"GATEWAY_WORKER_CERT_FILE":       "client.pem",
		"GATEWAY_WORKER_KEY_FILE":        "key.pem",
	} {
		t.Setenv(key, value)
	}
	for k, v := range map[string]string{"GATEWAY_DATABASE_URL": "postgres://unused/db", "GATEWAY_NATS_URL": "nats://unused:4222", "GATEWAY_NATS_TOPOLOGY_FILE": "../../../../deploy/nats/streams.yaml", "GATEWAY_ACCOUNT_SOURCE": "control", "GATEWAY_WECOM_ACCOUNTS_FILE": "", "GATEWAY_TELEGRAM_ACCOUNTS_FILE": "", "GATEWAY_CONTROL_URL": "https://control.example", "GATEWAY_CONTROL_SCOPE_ID": "pool", "GATEWAY_CONTROL_SOURCE_EPOCH": controlEpoch, "GATEWAY_CONTROL_CA_FILE": "ca.pem", "GATEWAY_CONTROL_CERT_FILE": "client.pem", "GATEWAY_CONTROL_KEY_FILE": "key.pem", "GATEWAY_PUBLIC_ORIGIN": "https://gateway.example", "GATEWAY_INSTANCE_ID": "gw"} {
		t.Setenv(k, v)
	}
	for _, tc := range []struct {
		value          string
		enabled, valid bool
	}{{"", true, true}, {"true", true, true}, {"false", false, true}, {"enabled", false, false}} {
		t.Run(tc.value, func(t *testing.T) {
			t.Setenv("GATEWAY_TELEGRAM_PREFLIGHT_ENABLED", tc.value)
			cfg, e := LoadConfig()
			if (e == nil) != tc.valid || e == nil && cfg.TelegramPreflightEnabled != tc.enabled {
				t.Fatal("preflight switch incorrect")
			}
		})
	}
	t.Setenv("GATEWAY_TELEGRAM_PREFLIGHT_ENABLED", "false")
	for _, tc := range []struct {
		value          string
		enabled, valid bool
	}{{"", true, true}, {"true", true, true}, {"false", false, true}, {"enabled", false, false}} {
		t.Run("wecom_"+tc.value, func(t *testing.T) {
			t.Setenv("GATEWAY_WECOM_PREFLIGHT_ENABLED", tc.value)
			cfg, err := LoadConfig()
			if (err == nil) != tc.valid || err == nil && cfg.WeComPreflightEnabled != tc.enabled {
				t.Fatal("WeCom preflight switch incorrect")
			}
		})
	}

}
