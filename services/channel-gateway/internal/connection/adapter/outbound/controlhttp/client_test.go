package controlhttp

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"errors"
	c "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/domain/accountcatalog"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func fixture() c.Snapshot {
	s := c.Snapshot{SchemaVersion: 1, ScopeID: "pool", SourceEpoch: "00000000-0000-4000-8000-000000000001", Revision: 1, Complete: true, Accounts: []c.Account{{TenantID: "tnt_test", ID: "cha_test", Provider: "telegram", ProviderAccountID: "123", Revision: 1, ConnectionRevision: 1, Enabled: true, Config: c.Config{WebhookPath: "/v1/telegram/cha_test"}, Credentials: []c.Credential{{Purpose: "telegram.bot_token", ID: "ccr_token", Version: 1, Configured: true}, {Purpose: "telegram.webhook_secret", ID: "ccr_webhook", Version: 1, Configured: true}}}}}
	s.Digest, _ = s.ComputedDigest()
	return s
}
func tlsFixture(t *testing.T, h http.Handler) (*Client, *httptest.Server) {
	t.Helper()
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	root := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test-root"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	der, err := x509.CreateCertificate(rand.Reader, root, root, pub, key)
	if err != nil {
		t.Fatal(err)
	}
	ca, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(ca)
	issue := func(n int64, usage x509.ExtKeyUsage) tls.Certificate {
		pub, k, e := ed25519.GenerateKey(rand.Reader)
		if e != nil {
			t.Fatal(e)
		}
		x := &x509.Certificate{SerialNumber: big.NewInt(n), Subject: pkix.Name{CommonName: "test-leaf"}, DNSNames: []string{"localhost"}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), ExtKeyUsage: []x509.ExtKeyUsage{usage}, KeyUsage: x509.KeyUsageDigitalSignature}
		b, e := x509.CreateCertificate(rand.Reader, x, ca, pub, key)
		if e != nil {
			t.Fatal(e)
		}
		kb, e := x509.MarshalPKCS8PrivateKey(k)
		if e != nil {
			t.Fatal(e)
		}
		cert, e := tls.X509KeyPair(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: b}), pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: kb}))
		if e != nil {
			t.Fatal(e)
		}
		return cert
	}
	server := httptest.NewUnstartedServer(h)
	server.TLS = &tls.Config{Certificates: []tls.Certificate{issue(2, x509.ExtKeyUsageServerAuth)}, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: roots, MinVersion: tls.VersionTLS12}
	server.StartTLS()
	t.Cleanup(server.Close)
	client, e := New(Options{BaseURL: server.URL, ScopeID: "pool", SourceEpoch: fixture().SourceEpoch, InstanceID: "gw", RootCAs: roots, Certificate: issue(3, x509.ExtKeyUsageClientAuth)})
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(client.Close)
	return client, server
}
func TestMutualTLSExactSnapshotAndCredentials(t *testing.T) {
	s := fixture()
	var calls atomic.Int32
	cl, _ := tlsFixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.TLS == nil || len(r.TLS.VerifiedChains) == 0 {
			t.Error("missing authenticated client")
		}
		calls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		if r.Method == "GET" {
			if r.URL.Path != "/internal/v1/channel-accounts/snapshot" || r.URL.RawQuery != "" {
				t.Error("wrong snapshot path")
			}
			_ = json.NewEncoder(w).Encode(s)
			return
		}
		var request c.ResolveRequest
		if json.NewDecoder(r.Body).Decode(&request) != nil {
			t.Error("bad request")
		}
		if r.URL.Path != "/internal/v1/tenants/tnt_test/channel-accounts/cha_test/credentials:resolve" {
			t.Error("wrong resolve path")
		}
		_ = json.NewEncoder(w).Encode(resolveResponse{ScopeID: s.ScopeID, SourceEpoch: s.SourceEpoch, TenantID: "tnt_test", AccountID: "cha_test", ConnectionRevision: 1, Values: []Value{{Purpose: "telegram.bot_token", ID: "ccr_token", Version: 1, Value: "synthetic-not-a-real-token"}, {Purpose: "telegram.webhook_secret", ID: "ccr_webhook", Version: 1, Value: "synthetic_secret"}}})
	}))
	got, e := cl.Fetch(context.Background())
	if e != nil || got.Digest != s.Digest {
		t.Fatal(e)
	}
	epoch := int64(1)
	r := c.ResolveRequest{SchemaVersion: 1, ScopeID: s.ScopeID, SourceEpoch: s.SourceEpoch, ConnectionRevision: 1, Consumer: c.Consumer{Kind: "telegram_registration", InstanceID: "gw", RegistrationEpoch: &epoch}, Uses: []c.Use{{Purpose: "telegram.bot_token", ID: "ccr_token", Version: 1}, {Purpose: "telegram.webhook_secret", ID: "ccr_webhook", Version: 1}}}
	v, e := cl.Resolve(context.Background(), s.Accounts[0], r)
	if e != nil || len(v) != 2 {
		t.Fatal(e)
	}
	r.Consumer.InstanceID = "other"
	if _, e = cl.Resolve(context.Background(), s.Accounts[0], r); !errors.Is(e, c.ErrUnauthorized) || calls.Load() != 2 {
		t.Fatal("untrusted identity reached network")
	}
}
func TestStrictSnapshot(t *testing.T) {
	raw, _ := json.Marshal(fixture())
	for _, x := range []string{strings.Replace(string(raw), `"complete":true`, `"complete":true,"complete":true`, 1), strings.Replace(string(raw), `"complete":true,`, ``, 1), strings.Replace(string(raw), `"complete"`, `"Complete"`, 1), strings.Replace(string(raw), `"config":{`, `"config":{"unknown":1,`, 1), string(raw) + ` {}`, strings.Replace(string(raw), `"accounts":[`, `"accounts":null,"unused":[`, 1)} {
		var got c.Snapshot
		if decode("account-snapshot.schema.json", []byte(x), &got) == nil {
			t.Fatal("malformed response accepted")
		}
	}
}
func TestHTTPFailuresAreBoundedAndSanitized(t *testing.T) {
	for _, status := range []int{http.StatusForbidden, http.StatusConflict, http.StatusInternalServerError, http.StatusTemporaryRedirect} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			cl, _ := tlsFixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Location", "https://invalid.example/secret")
				w.WriteHeader(status)
				_, _ = w.Write([]byte("provider-token-must-not-escape"))
			}))
			_, e := cl.Fetch(context.Background())
			if e == nil || strings.Contains(e.Error(), "token") || strings.Contains(e.Error(), "https") {
				t.Fatal("leaked or accepted failure")
			}
		})
	}
	cl, _ := tlsFixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Encoding", "gzip")
		_, _ = w.Write([]byte("not-gzip"))
	}))
	if _, e := cl.Fetch(context.Background()); !errors.Is(e, c.ErrInvalid) {
		t.Fatal(e)
	}
}
func TestResponseOversizeAndWrongCredentialVersion(t *testing.T) {
	cl, _ := tlsFixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(strings.Repeat("x", c.MaxSnapshotBytes+1)))
	}))
	if _, e := cl.Fetch(context.Background()); !errors.Is(e, c.ErrInvalid) {
		t.Fatal("oversized accepted")
	}
	s := fixture()
	cl, _ = tlsFixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(resolveResponse{ScopeID: s.ScopeID, SourceEpoch: s.SourceEpoch, TenantID: "tnt_test", AccountID: "cha_test", ConnectionRevision: 2, Values: []Value{{Purpose: "telegram.webhook_secret", ID: "ccr_webhook", Version: 1, Value: "synthetic"}}})
	}))
	r := c.ResolveRequest{SchemaVersion: 1, ScopeID: s.ScopeID, SourceEpoch: s.SourceEpoch, ConnectionRevision: 1, Consumer: c.Consumer{Kind: "telegram_webhook", InstanceID: "gw"}, Uses: []c.Use{{Purpose: "telegram.webhook_secret", ID: "ccr_webhook", Version: 1}}}
	if _, e := cl.Resolve(context.Background(), s.Accounts[0], r); !errors.Is(e, c.ErrIntegrity) {
		t.Fatal("wrong version accepted")
	}
}
func TestParentCancellation(t *testing.T) {
	cl, _ := tlsFixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { <-r.Context().Done() }))
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	start := time.Now()
	if _, e := cl.Fetch(ctx); e == nil {
		t.Fatal("cancelled request succeeded")
	}
	if time.Since(start) > time.Second {
		t.Fatal("parent cancellation ignored")
	}
}
