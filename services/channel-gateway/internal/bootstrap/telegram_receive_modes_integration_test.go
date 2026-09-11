package bootstrap

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	wire "github.com/liuzengh/trpc-agent-service/api/schemas/channel/v1"
	protocol "github.com/liuzengh/trpc-agent-service/platform/im/telegram"
	control "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/adapter/outbound/controlhttp"
	runtime "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/application/telegramruntime"
	c "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/domain/accountcatalog"
	transport "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/infra/nats"
	"github.com/nats-io/nats.go/jetstream"
)

type httpReceiverFactory struct{ base string }
type httpReceiver struct{ api *protocol.Client }

func (f httpReceiverFactory) New(token string) (runtime.Remote, error) {
	api, e := protocol.New(protocol.Options{Token: token, BaseURL: f.base})
	return &httpReceiver{api}, e
}
func (r *httpReceiver) Identity(ctx context.Context) (string, error) { return r.api.Identity(ctx) }
func (r *httpReceiver) Webhook(ctx context.Context) (string, error) {
	v, e := r.api.WebhookInfo(ctx)
	return v.URL, e
}
func (r *httpReceiver) Register(ctx context.Context, address, secret string) (bool, error) {
	e := r.api.SetWebhook(ctx, address, secret, []string{"message", "callback_query"})
	return e == nil, e
}
func (r *httpReceiver) DeleteWebhook(ctx context.Context) error { return r.api.DeleteWebhook(ctx) }
func (r *httpReceiver) Poll(ctx context.Context, offset int64, timeout int) ([]json.RawMessage, error) {
	return r.api.PollOnce(ctx, protocol.PollRequest{Offset: offset, Limit: 100, TimeoutSeconds: timeout, AllowedUpdates: []string{"message", "callback_query"}})
}
func (r *httpReceiver) Close() { r.api.Close() }

func TestTelegramReceiveModesRealHTTPPGNATS(t *testing.T) {
	natsURL := os.Getenv("GATEWAY_TEST_NATS_URL")
	if natsURL == "" {
		t.Skip("dedicated NATS required")
	}
	if os.Getenv("GATEWAY_TEST_ALLOW_NATS_RESET") != "1" {
		t.Fatal("dedicated reset flag required")
	}
	for _, origin := range []string{"", "https://gateway.example"} {
		t.Run(fmt.Sprintf("origin=%s", origin), func(t *testing.T) {
			pool := deliveryDB(t)
			ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
			defer cancel()
			topology, e := transport.LoadTopology("../../../../deploy/nats/streams.yaml")
			if e != nil {
				t.Fatal(e)
			}
			broker, e := transport.Connect(natsURL, topology, transport.Auth{})
			if e != nil {
				t.Fatal(e)
			}
			defer broker.Close()
			for _, name := range []string{transport.RouteStream, transport.RunStream, transport.ManifestStream, transport.ReplyStream} {
				if e = broker.JS.DeleteStream(ctx, name); e != nil && e != jetstream.ErrStreamNotFound {
					t.Fatal(e)
				}
			}
			if e = broker.Reconcile(ctx); e != nil {
				t.Fatal(e)
			}
			defer func() {
				for _, name := range []string{transport.RouteStream, transport.RunStream, transport.ManifestStream, transport.ReplyStream} {
					_ = broker.JS.DeleteStream(context.Background(), name)
				}
			}()
			rawUpdate := json.RawMessage(`{"update_id":101,"future":{"preserve":true},"message":{"message_id":7,"date":1700000000,"chat":{"id":123,"type":"private"},"from":{"id":100,"is_bot":false},"text":"dual receive durable marker"}}`)
			var remoteMu sync.Mutex
			remoteURL := ""
			var polls, registers, deletes atomic.Int32
			api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				remoteMu.Lock()
				defer remoteMu.Unlock()
				w.Header().Set("Content-Type", "application/json")
				var request map[string]json.RawMessage
				if e := json.NewDecoder(r.Body).Decode(&request); e != nil {
					t.Error(e)
				}
				result := any(true)
				switch r.URL.Path {
				case "/bot123:synthetic_test_token/getMe":
					result = map[string]any{"id": 123, "is_bot": true}
				case "/bot123:synthetic_test_token/getWebhookInfo":
					result = map[string]any{"url": remoteURL, "pending_update_count": 0}
				case "/bot123:synthetic_test_token/getUpdates":
					polls.Add(1)
					var offset int64
					_ = json.Unmarshal(request["offset"], &offset)
					if remoteURL != "" {
						t.Error("polled while webhook registered")
					}
					updates := []json.RawMessage{}
					if offset <= 101 {
						updates = append(updates, rawUpdate)
					}
					result = updates
				case "/bot123:synthetic_test_token/setWebhook":
					registers.Add(1)
					_ = json.Unmarshal(request["url"], &remoteURL)
					if string(request["drop_pending_updates"]) != "false" {
						t.Error("discarded backlog")
					}
				case "/bot123:synthetic_test_token/deleteWebhook":
					deletes.Add(1)
					remoteURL = ""
					if string(request["drop_pending_updates"]) != "false" {
						t.Error("discarded backlog")
					}
				default:
					t.Error("unexpected Telegram method")
					w.WriteHeader(404)
					return
				}
				_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "result": result})
			}))
			defer api.Close()
			var mu sync.Mutex
			snapshot := controlSnapshot()
			snapshot.Accounts[0].Config.ReceiveMode = "long_polling"
			snapshot.Accounts[0].Credentials[1].Configured = false
			snapshot.Digest, _ = snapshot.ComputedDigest()
			cfg := controlTLSFiles(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				defer mu.Unlock()
				w.Header().Set("Content-Type", "application/json")
				switch r.URL.Path {
				case "/internal/v1/channel-accounts/snapshot":
					_ = json.NewEncoder(w).Encode(snapshot)
				case "/internal/v1/tenants/tenant/channel-accounts/account/credentials:resolve":
					var req c.ResolveRequest
					if json.NewDecoder(r.Body).Decode(&req) != nil || req.Validate(snapshot.Accounts[0]) != nil {
						w.WriteHeader(409)
						return
					}
					values := []control.Value{}
					for _, u := range req.Uses {
						value := "123:synthetic_test_token"
						if u.Purpose == "telegram.webhook_secret" {
							if snapshot.Accounts[0].ReceiveMode() != "webhook" {
								t.Error("LP required webhook secret")
							}
							value = "synthetic_secret"
						}
						values = append(values, control.Value{Purpose: u.Purpose, ID: u.ID, Version: u.Version, Value: value})
					}
					_ = json.NewEncoder(w).Encode(map[string]any{"scope_id": snapshot.ScopeID, "source_epoch": snapshot.SourceEpoch, "tenant_id": "tenant", "account_id": "account", "connection_revision": snapshot.Accounts[0].ConnectionRevision, "values": values})
				case "/internal/v1/channel-account-observations":
					body, _ := io.ReadAll(r.Body)
					if wire.Validate("observations.schema.json", body) != nil {
						t.Error("observation drift")
					}
					w.WriteHeader(204)
				default:
					t.Error("unexpected Control endpoint")
					w.WriteHeader(404)
				}
			}))
			cfg.PublicOrigin = origin
			dsn, _ := url.Parse(os.Getenv("GATEWAY_TEST_DATABASE_URL"))
			query := dsn.Query()
			query.Set("search_path", pool.Config().ConnConfig.RuntimeParams["search_path"])
			dsn.RawQuery = query.Encode()
			database := fixtureDatabaseConfig(t, dsn.String())
			app, e := newWithDatabaseTarget(ctx, Config{AccountSource: "control", Control: cfg,
				Worker:     WorkerConfig{URL: cfg.URL, CAFile: cfg.CAFile, CertificateFile: cfg.CertificateFile, KeyFile: cfg.KeyFile},
				InstanceID: "gw", HTTPAddress: "127.0.0.1:0", AdminAddress: "localhost:0", DatabaseURL: database.runtimeURL,
				MigrationDatabaseURL: database.migrationURL, NATSURL: natsURL, Topology: topology, telegramFactory: httpReceiverFactory{api.URL}}, database.target)
			if e != nil {
				t.Fatal(e)
			}
			defer app.Close()
			public := httptest.NewServer(app.Handler())
			defer public.Close()
			runCtx, stop := context.WithCancel(ctx)
			done := make(chan error, 1)
			go func() { done <- app.Run(runCtx) }()
			defer func() {
				stop()
				select {
				case e := <-done:
					if e != nil {
						t.Error(e)
					}
				case <-time.After(16 * time.Second):
					t.Error("receiver did not stop")
				}
			}()
			eventually(t, func() bool { return polls.Load() > 0 }, "initial polling without public origin or webhook secret")
			var cursor int64
			if e = pool.QueryRow(ctx, `SELECT next_offset FROM gateway_telegram_receivers`).Scan(&cursor); e != nil || cursor != 0 {
				t.Fatal("route-unavailable update acknowledged", cursor, e)
			}
			route := map[string]any{"event_id": "dual-mode-route", "schema_version": 1, "enabled": true, "route": map[string]any{"provider": "telegram", "account_id": "account", "generation": 1, "tenant_id": "tenant", "binding_id": "binding", "deployment_revision_id": "revision", "manifest_ref": "manifest", "manifest_digest": "sha256:" + strings.Repeat("a", 64)}}
			body, _ := json.Marshal(route)
			if _, e = broker.JS.Publish(ctx, transport.RouteSubject, body); e != nil {
				t.Fatal(e)
			}
			eventually(t, func() bool {
				e := pool.QueryRow(ctx, `SELECT next_offset FROM gateway_telegram_receivers`).Scan(&cursor)
				return e == nil && cursor == 102
			}, "durable cursor after route and admission")
			eventually(t, func() bool {
				stream, e := broker.JS.Stream(ctx, transport.RunStream)
				if e != nil {
					return false
				}
				info, e := stream.Info(ctx)
				return e == nil && info.State.Msgs == 1
			}, "polling transactional RunRequested")
			if registers.Load() != 0 || deletes.Load() != 0 {
				t.Fatal("pure polling changed webhook")
			}
			if origin == "" {
				t.Log("PURE_POLLING_VERIFIED: no public origin, token only, raw HTTP -> Receipt/Admission/Outbox -> durable NATS, cursor 102")
				return
			}
			// Intermediate disabled/config snapshots may be coalesced by the catalog.
			// The newer connection revision must still fence and drain the old receiver.
			change := func(mode string) {
				mu.Lock()
				snapshot.Revision++
				snapshot.Accounts[0].Revision++
				snapshot.Accounts[0].ConnectionRevision++
				snapshot.Accounts[0].Config.ReceiveMode = mode
				if !snapshot.Accounts[0].Credentials[1].Configured {
					snapshot.Accounts[0].Credentials[1].Configured = true
					snapshot.Accounts[0].Credentials[1].Version++
				}
				snapshot.Digest, _ = snapshot.ComputedDigest()
				mu.Unlock()
				if _, e := app.catalog.Refresh(ctx); e != nil {
					t.Fatal(e)
				}
			}
			change("webhook")
			eventually(t, func() bool { return registers.Load() == 1 && app.telegram.Ready() }, "polling to webhook")
			request, _ := http.NewRequest(http.MethodPost, public.URL+"/v1/telegram/account", strings.NewReader(string(rawUpdate)))
			request.Header.Set("X-Telegram-Bot-Api-Secret-Token", "synthetic_secret")
			response, e := http.DefaultClient.Do(request)
			if e != nil {
				t.Fatal(e)
			}
			response.Body.Close()
			if response.StatusCode != 200 {
				t.Fatal("cross-mode replay", response.StatusCode)
			}
			var receipts, admissions int
			if e = pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM gateway_inbox),(SELECT count(*) FROM gateway_admissions)`).Scan(&receipts, &admissions); e != nil || receipts != 1 || admissions != 1 {
				t.Fatal("duplicate admission across modes", receipts, admissions, e)
			}
			change("long_polling")
			eventually(t, func() bool { return deletes.Load() == 1 && app.telegram.Ready() }, "managed webhook to polling")
			if e = pool.QueryRow(ctx, `SELECT next_offset FROM gateway_telegram_receivers`).Scan(&cursor); e != nil || cursor != 102 {
				t.Fatal("mode switch reset cursor", cursor, e)
			}
			t.Log("DUAL_MODE_VERIFIED: HTTP polling -> durable Receipt/Admission/Outbox/NATS, cursor after commit, two explicit mode changes, preserved pending/cursor, webhook replay deduplicated")
		})
	}
}
