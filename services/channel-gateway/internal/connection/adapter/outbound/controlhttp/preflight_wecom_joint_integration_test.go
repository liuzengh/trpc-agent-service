package controlhttp

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/jackc/pgx/v5/pgxpool"
	wire "github.com/liuzengh/trpc-agent-service/api/schemas/channel/v1"
	"github.com/liuzengh/trpc-agent-service/platform/im/wecom"
	provider "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/adapter/outbound/wecompreflight"
	p "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/application/preflight"
)

// Only the provider WebSocket peer is synthetic. Session, public HTTP, Control
// process, PostgreSQL migrations, mTLS, credential decryption, Gateway Service,
// public connector and completion/readback all execute production code.
func jointWeComPreflight(t *testing.T, ctx context.Context, public *http.Client, base, tenant, scope, epoch string, gateway *PreflightClient, pool *pgxpool.Pool, logs *jointPreflightLogs) {
	cfg, err := p.NewWeComConfig(scope, epoch)
	if err != nil {
		t.Fatal(err)
	}
	for i, tc := range []struct {
		name, code, outcome string
		providerCode        int
	}{
		{"wecom_accepted", "WECOM_AUTHENTICATED", "PASS", 0},
		{"wecom_rejected", "WECOM_AUTH_REJECTED", "FAIL", 40013},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const secret = "JOINT_WECOM_PRIVATE_CANARY"
			raw := jointPreflightPublic(t, ctx, public, base, "POST", "/v1/tenants/"+tenant+"/channel-accounts", "joint-account-"+tc.name, map[string]any{"provider": "wecom", "provider_account_id": tc.name, "name": tc.name, "credentials": map[string]any{"wecom.bot_secret": map[string]string{"action": "replace", "value": secret}}}, 201)
			var account struct {
				Account struct {
					ID string `json:"account_id"`
				} `json:"account"`
			}
			if json.Unmarshal(raw, &account) != nil || account.Account.ID == "" {
				t.Fatal("no saved account")
			}
			path := "/v1/tenants/" + tenant + "/channel-accounts/" + account.Account.ID + "/preflights"
			input := wire.PreflightCreateRequest{ExpectedAccountRevision: 1, ExpectedConnectionRevision: 1, ExpectedBotSecretVersion: 1}
			before := jointPreflightRuntimeHashes(t, ctx, pool)
			jointPreflightPublic(t, ctx, public, base, "POST", path, "joint-consent-"+tc.name, input, 422)
			var count int
			if err := pool.QueryRow(ctx, "SELECT count(*) FROM channel_preflights WHERE account_id=$1", account.Account.ID).Scan(&count); err != nil || count != 0 {
				t.Fatal("missing consent created a job")
			}
			input.AllowConnectionProbe = true
			stale := input
			stale.ExpectedAccountRevision = 2
			jointPreflightPublic(t, ctx, public, base, "POST", path, "joint-stale-"+tc.name, stale, 409)
			raw = jointPreflightPublic(t, ctx, public, base, "POST", path, "joint-create-"+tc.name, input, 202)
			var created wire.PreflightCreated
			if wire.Decode("preflight-created.schema.json", raw, &created) != nil {
				t.Fatal("invalid created receipt")
			}
			time.Sleep(600 * time.Millisecond)
			claim := p.ClaimRequest{DiagnosticPolicy: wire.PreflightWeComPolicy, Config: cfg, InstanceEpoch: "55555555-5555-4555-8555-555555555555", RequestID: fmt.Sprintf("77777777-7777-4777-8777-%012d", i+1), Token: p.NewSecret(base64.RawURLEncoding.EncodeToString(jointPreflightRandomBytes(t, 32)))}
			grant, err := gateway.Claim(ctx, claim)
			if err != nil || grant == nil {
				t.Fatal("WeCom claim", err)
			}
			if grant.PreflightID != created.PreflightID || grant.Provider != "wecom" || grant.Credential.Purpose != "wecom.bot_secret" || !grant.AllowConnectionProbe || grant.ReceiveMode != wire.PreflightWeComMode {
				t.Fatal("wrong provider grant")
			}
			commands := make(chan string, 4)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				conn, err := websocket.Accept(w, r, nil)
				if err != nil {
					return
				}
				defer conn.CloseNow()
				callctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
				defer cancel()
				_, raw, err := conn.Read(callctx)
				if err != nil {
					return
				}
				var f struct {
					Cmd     string            `json:"cmd"`
					Headers map[string]string `json:"headers"`
					Body    map[string]string `json:"body"`
				}
				if json.Unmarshal(raw, &f) != nil || f.Body["bot_id"] != tc.name || f.Body["secret"] != secret {
					t.Error("incorrect exact credential")
					return
				}
				commands <- f.Cmd
				reply, _ := json.Marshal(map[string]any{"headers": f.Headers, "errcode": tc.providerCode, "errmsg": "PRIVATE_PROVIDER_CANARY"})
				_ = conn.Write(callctx, websocket.MessageText, reply)
				for {
					_, _, err := conn.Read(callctx)
					if err != nil {
						return
					}
					commands <- "unexpected"
				}
			}))
			defer server.Close()
			service := p.WeComService{Control: gateway, Probe: jointWeComSocketProbe{url: "ws" + strings.TrimPrefix(server.URL, "http")}}
			result, err := service.Execute(ctx, *grant, cfg)
			if err != nil {
				t.Fatal("WeCom production service", err)
			}
			if len(result.Checks) != 3 || result.Checks[1].Code != tc.code || result.Checks[2].Code != "DELIVERY_NOT_TESTED" {
				t.Fatal("WeCom result mismatch")
			}
			if err := gateway.Complete(ctx, *grant, result); err != nil {
				t.Fatal("WeCom completion", err)
			}
			raw = jointPreflightPublic(t, ctx, public, base, "GET", created.StatusURL, "", nil, 200)
			var view wire.PreflightView
			if wire.Decode("preflight-view.schema.json", raw, &view) != nil || view.State != "COMPLETED" || view.Outcome != tc.outcome || view.CredentialVersion() != 1 || view.Checks[2].Status != "UNKNOWN" {
				t.Fatal("wrong persisted WeCom view")
			}
			if err := gateway.Complete(ctx, *grant, result); err != nil {
				t.Fatal("WeCom replay", err)
			}
			replay := jointPreflightPublic(t, ctx, public, base, "GET", created.StatusURL, "", nil, 200)
			if !bytes.Equal(raw, replay) || !reflect.DeepEqual(before, jointPreflightRuntimeHashes(t, ctx, pool)) {
				t.Fatal("replay or runtime state changed")
			}
			for _, canary := range []string{secret, claim.Token.Reveal(), "PRIVATE_PROVIDER_CANARY"} {
				if bytes.Contains(raw, []byte(canary)) || logs.contains(canary) {
					t.Fatal("private material in public result/log")
				}
			}
			if len(commands) != 1 || <-commands != "aibot_subscribe" {
				t.Fatal("unexpected business command")
			}
			t.Log("WECOM_JOINT=PASS real HTTP+Session+Control+PG+mTLS+Service+Go WebSocket; provider peer=fixture; consent+CAS+3 checks+replay+runtime hashes verified")
		})
	}
	if handoff := os.Getenv("GATEWAY_PREFLIGHT_BROWSER_HANDOFF"); handoff != "" {
		probe := provider.New()
		defer probe.Close()
		runner, err := p.NewWeComRunner(gateway, probe, cfg, "88888888-8888-4888-8888-888888888888")
		if err != nil {
			t.Fatal(err)
		}
		runctx, stop := context.WithCancel(ctx)
		done := make(chan error, 1)
		go func() { done <- runner.Run(runctx) }()
		defer func() {
			stop()
			if err := <-done; err != nil {
				t.Error(err)
			}
		}()
		metadata, _ := json.Marshal(map[string]string{"control_origin": base, "tenant_id": tenant, "login_username": "joint-admin", "login_password": "Joint-final-Password-5678", "provider": "official WeCom endpoint; no fixture peer in browser phase"})
		if err := os.WriteFile(handoff, metadata, 0600); err != nil {
			t.Fatal(err)
		}
		t.Log("BROWSER_HANDOFF_READY: isolated real Control + Gateway preflight runner")
		ticker := time.NewTicker(250 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				t.Fatal("browser handoff timed out")
			case <-ticker.C:
				if _, err := os.Stat(handoff + ".done"); err == nil {
					var completed, enabled, bindings int
					if err := pool.QueryRow(ctx, "SELECT count(*) FROM channel_preflights WHERE state='COMPLETED' AND record_jsonb->'view'->>'provider'='wecom'").Scan(&completed); err != nil {
						t.Fatal(err)
					}
					if err := pool.QueryRow(ctx, "SELECT count(*) FROM channel_accounts WHERE enabled").Scan(&enabled); err != nil {
						t.Fatal(err)
					}
					if err := pool.QueryRow(ctx, "SELECT count(*) FROM channel_bindings").Scan(&bindings); err != nil {
						t.Fatal(err)
					}
					if completed < 3 || enabled != 0 || bindings != 0 {
						t.Fatalf("browser closure completed=%d enabled=%d bindings=%d", completed, enabled, bindings)
					}
					t.Logf("BROWSER_JOINT_DATABASE=PASS completed_wecom=%d enabled_accounts=%d bindings=%d", completed, enabled, bindings)
					return
				}
			}
		}
	}
}

type jointWeComSocketProbe struct{ url string }

func (s jointWeComSocketProbe) InspectConnection(ctx context.Context, secret p.Secret, id string) (p.ConnectionProbeResult, error) {
	result, err := wecom.ProbeAuthentication(ctx, wecom.Config{BotID: id, Secret: secret.Reveal(), URL: s.url})
	return p.ConnectionProbeResult{Code: result.Code, Authenticated: result.Authenticated}, err
}
