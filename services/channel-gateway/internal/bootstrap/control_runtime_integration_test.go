package bootstrap

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	wire "github.com/liuzengh/trpc-agent-service/api/schemas/channel/v1"
	control "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/adapter/outbound/controlhttp"
	runtime "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/application/telegramruntime"
	c "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/domain/accountcatalog"
	transport "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/infra/nats"
	"github.com/nats-io/nats.go/jetstream"
)

func controlTLSFiles(t *testing.T, h http.Handler) ControlConfig {
	t.Helper()
	pub, key, e := ed25519.GenerateKey(rand.Reader)
	if e != nil {
		t.Fatal(e)
	}
	root := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "control-test"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	der, e := x509.CreateCertificate(rand.Reader, root, root, pub, key)
	if e != nil {
		t.Fatal(e)
	}
	ca, e := x509.ParseCertificate(der)
	if e != nil {
		t.Fatal(e)
	}
	roots := x509.NewCertPool()
	roots.AddCert(ca)
	issue := func(n int64, usage x509.ExtKeyUsage) (tls.Certificate, []byte, []byte) {
		pub, k, e := ed25519.GenerateKey(rand.Reader)
		if e != nil {
			t.Fatal(e)
		}
		uri, _ := url.Parse("spiffe://trpc-agent-service/gateway/gw")
		leaf := &x509.Certificate{SerialNumber: big.NewInt(n), DNSNames: []string{"localhost"}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, URIs: []*url.URL{uri}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), ExtKeyUsage: []x509.ExtKeyUsage{usage}, KeyUsage: x509.KeyUsageDigitalSignature}
		der, e := x509.CreateCertificate(rand.Reader, leaf, ca, pub, key)
		if e != nil {
			t.Fatal(e)
		}
		kb, e := x509.MarshalPKCS8PrivateKey(k)
		if e != nil {
			t.Fatal(e)
		}
		certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
		keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: kb})
		cert, e := tls.X509KeyPair(certPEM, keyPEM)
		if e != nil {
			t.Fatal(e)
		}
		return cert, certPEM, keyPEM
	}
	serverCert, _, _ := issue(2, x509.ExtKeyUsageServerAuth)
	server := httptest.NewUnstartedServer(h)
	server.TLS = &tls.Config{Certificates: []tls.Certificate{serverCert}, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: roots, MinVersion: tls.VersionTLS12}
	server.StartTLS()
	t.Cleanup(server.Close)
	_, cert, keyBytes := issue(3, x509.ExtKeyUsageClientAuth)
	dir := t.TempDir()
	write := func(name string, b []byte) string {
		p := filepath.Join(dir, name)
		if e := os.WriteFile(p, b, 0600); e != nil {
			t.Fatal(e)
		}
		return p
	}
	return ControlConfig{URL: server.URL, CAFile: write("ca.pem", pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})), CertificateFile: write("client.pem", cert), KeyFile: write("client-key.pem", keyBytes), ScopeID: "pool", SourceEpoch: controlEpoch, PublicOrigin: "https://gateway.example"}
}

type remoteRuntimeFixture struct {
	calls     atomic.Int32
	t         *testing.T
	remoteURL string
}

func (f *remoteRuntimeFixture) New(token string) (runtime.Remote, error) {
	if token != "synthetic_token" {
		return nil, c.ErrInvalid
	}
	return f, nil
}
func (f *remoteRuntimeFixture) Identity(context.Context) (string, error) { return "123", nil }
func (f *remoteRuntimeFixture) Register(ctx context.Context, url, secret string) (bool, error) {
	if url != "https://gateway.example/v1/telegram/account" || !strings.HasPrefix(secret, "synthetic_secret_") {
		f.t.Error("registration config mismatch")
	}
	f.calls.Add(1)
	f.remoteURL = url
	return true, nil
}
func (*remoteRuntimeFixture) Close()                                    {}
func (f *remoteRuntimeFixture) Webhook(context.Context) (string, error) { return f.remoteURL, nil }
func (f *remoteRuntimeFixture) DeleteWebhook(context.Context) error     { f.remoteURL = ""; return nil }
func (f *remoteRuntimeFixture) Poll(context.Context, int64, int) ([]json.RawMessage, error) {
	return []json.RawMessage{}, nil
}
func TestControlRuntimeMTLSRealPGNATSRotationAndInbound(t *testing.T) {
	natsURL := os.Getenv("GATEWAY_TEST_NATS_URL")
	if natsURL == "" {
		t.Skip("dedicated NATS required")
	}
	if os.Getenv("GATEWAY_TEST_ALLOW_NATS_RESET") != "1" {
		t.Fatal("dedicated reset flag required")
	}
	pool := deliveryDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
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
	var mu sync.Mutex
	snapshot := controlSnapshot()
	var unavailable bool
	var reports atomic.Int32
	var resolves atomic.Int32
	controlConfig := controlTLSFiles(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.TLS == nil || len(r.TLS.VerifiedChains) == 0 {
			t.Error("missing verified mTLS")
			w.WriteHeader(401)
			return
		}
		mu.Lock()
		defer mu.Unlock()
		if unavailable {
			w.WriteHeader(503)
			return
		}
		switch r.URL.Path {
		case "/internal/v1/channel-accounts/snapshot":
			_ = json.NewEncoder(w).Encode(snapshot)
		case "/internal/v1/tenants/tenant/channel-accounts/account/credentials:resolve":
			var request c.ResolveRequest
			if json.NewDecoder(r.Body).Decode(&request) != nil || request.Validate(snapshot.Accounts[0]) != nil {
				w.WriteHeader(409)
				return
			}
			resolves.Add(1)
			values := []control.Value{}
			for _, u := range request.Uses {
				value := "synthetic_token"
				if u.Purpose == "telegram.webhook_secret" {
					value = "synthetic_secret_1"
					if u.Version == 2 {
						value = "synthetic_secret_2"
					}
				}
				values = append(values, control.Value{Purpose: u.Purpose, ID: u.ID, Version: u.Version, Value: value})
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"scope_id": snapshot.ScopeID, "source_epoch": snapshot.SourceEpoch, "tenant_id": "tenant", "account_id": "account", "connection_revision": snapshot.Accounts[0].ConnectionRevision, "values": values})
		case "/internal/v1/channel-account-observations":
			body, _ := io.ReadAll(r.Body)
			if wire.Validate("observations.schema.json", body) != nil {
				t.Error("invalid observations envelope")
				w.WriteHeader(400)
				return
			}
			reports.Add(1)
			w.WriteHeader(204)
		default:
			t.Error("unexpected internal path")
			w.WriteHeader(404)
		}
	}))
	remote := &remoteRuntimeFixture{t: t}
	dsn, _ := url.Parse(os.Getenv("GATEWAY_TEST_DATABASE_URL"))
	query := dsn.Query()
	query.Set("search_path", pool.Config().ConnConfig.RuntimeParams["search_path"])
	dsn.RawQuery = query.Encode()
	database := fixtureDatabaseConfig(t, dsn.String())
	config := Config{AccountSource: "control", Worker: WorkerConfig{URL: controlConfig.URL, CAFile: controlConfig.CAFile, CertificateFile: controlConfig.CertificateFile, KeyFile: controlConfig.KeyFile}, Control: controlConfig, InstanceID: "gw", HTTPAddress: "127.0.0.1:0", AdminAddress: "localhost:0", DatabaseURL: database.runtimeURL, MigrationDatabaseURL: database.migrationURL, NATSURL: natsURL, Topology: topology, telegramFactory: remote}
	app, e := newWithDatabaseTarget(ctx, config, database.target)
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
			t.Error("runtime shutdown timeout")
		}
	}()
	eventually(t, func() bool { return app.catalog.Ready() && app.telegram.Ready() }, "Control snapshot and Telegram registration")
	if app.deliveryRunner == nil {
		t.Fatal("production delivery runner missing")
	}
	// Registration precedes any route, Binding event or inbound message.
	if remote.calls.Load() != 1 {
		t.Fatalf("remote calls=%d", remote.calls.Load())
	}
	event := map[string]any{"event_id": "control-route-1", "schema_version": 1, "enabled": true, "route": map[string]any{"provider": "telegram", "account_id": "account", "generation": 1, "tenant_id": "tenant", "binding_id": "binding", "deployment_revision_id": "revision", "manifest_ref": "manifest", "manifest_digest": "sha256:" + strings.Repeat("a", 64)}}
	raw, _ := json.Marshal(event)
	if _, e = broker.JS.Publish(ctx, transport.RouteSubject, raw); e != nil {
		t.Fatal(e)
	}
	eventually(t, func() bool { _, e := app.routes.Resolve(ctx, "telegram", "account"); return e == nil }, "route consumer applied actual NATS event")
	send := func(id, secret string) int {
		r, _ := http.NewRequest(http.MethodPost, public.URL+"/v1/telegram/account", strings.NewReader(`{"update_id":`+id+`,"message":{"message_id":7,"date":1700000000,"chat":{"id":123,"type":"private"},"from":{"id":100,"is_bot":false},"text":"control integration"}}`))
		r.Header.Set("X-Telegram-Bot-Api-Secret-Token", secret)
		res, e := http.DefaultClient.Do(r)
		if e != nil {
			t.Fatal(e)
		}
		defer res.Body.Close()
		return res.StatusCode
	}
	if code := send("1", "synthetic_secret_1"); code != 200 {
		t.Fatalf("first inbound=%d", code)
	}
	eventually(t, func() bool {
		info, e := broker.JS.Stream(ctx, transport.RunStream)
		if e != nil {
			return false
		}
		state, e := info.Info(ctx)
		return e == nil && state.State.Msgs == 1
	}, "transactional outbox published RunRequested")
	mu.Lock()
	snapshot.Revision++
	snapshot.Accounts[0].Revision++
	snapshot.Accounts[0].ConnectionRevision++
	snapshot.Accounts[0].Credentials[1].Version++
	snapshot.Digest, _ = snapshot.ComputedDigest()
	mu.Unlock()
	if _, e = app.catalog.Refresh(ctx); e != nil {
		t.Fatal(e)
	}
	eventually(t, func() bool { return remote.calls.Load() == 2 && app.telegram.Ready() }, "immutable handler and registration version rotation")
	if code := send("2", "synthetic_secret_1"); code != 401 {
		t.Fatalf("old secret=%d", code)
	}
	if code := send("1", "synthetic_secret_2"); code != 200 {
		t.Fatalf("receipt under new secret=%d", code)
	}
	if code := send("2", "synthetic_secret_2"); code != 200 {
		t.Fatalf("new secret=%d", code)
	}
	mu.Lock()
	unavailable = true
	mu.Unlock()
	if _, e = app.catalog.Refresh(ctx); e == nil {
		t.Fatal("source failure accepted")
	}
	if code := send("3", "synthetic_secret_2"); code != 503 {
		t.Fatalf("source unavailable inbound=%d", code)
	}
	if app.catalog.Ready() {
		t.Fatal("freshness still ready")
	}
	var count int
	if e = pool.QueryRow(ctx, `SELECT count(*) FROM gateway_inbox`).Scan(&count); e != nil || count != 2 {
		t.Fatalf("receipts=%d %v", count, e)
	}
	if reports.Load() == 0 || resolves.Load() < 4 {
		t.Fatalf("reports=%d resolves=%d", reports.Load(), resolves.Load())
	}
	t.Log("CONTROL_RUNTIME_VERIFIED: real mTLS, PG and NATS; versioned resolve, registration before route, A1/A2 runner installed, webhook admission+outbox, rotation, source failure, observations")
}
