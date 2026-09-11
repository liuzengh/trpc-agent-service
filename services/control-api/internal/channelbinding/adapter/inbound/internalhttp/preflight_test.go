package internalhttp

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"io"
	"log"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	channelv1 "github.com/liuzengh/trpc-agent-service/api/schemas/channel/v1"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/channelbinding/application"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/channelbinding/domain"
)

type runtimeFake struct{}

func (runtimeFake) ReadSnapshot(context.Context, application.WorkloadPrincipal) (domain.Snapshot, error) {
	return domain.Snapshot{}, nil
}
func (runtimeFake) ResolveCredentials(context.Context, application.WorkloadPrincipal, string, string, application.ResolveRequest) (application.ResolveResponse, error) {
	return application.ResolveResponse{}, nil
}
func (runtimeFake) ReportObservations(context.Context, application.WorkloadPrincipal, application.ObservationsRequest) error {
	return nil
}

type preflightRuntimeFake struct {
	claims, resolves, completes int
	principal                   application.WorkloadPrincipal
	id                          string
	claim                       channelv1.PreflightClaimRequest
	resolve                     channelv1.PreflightResolveRequest
	complete                    channelv1.PreflightCompleteRequest
	grant                       *channelv1.PreflightGrant
	resolved                    channelv1.PreflightResolveResponse
	err                         error
}

func (s *preflightRuntimeFake) Claim(_ context.Context, p application.WorkloadPrincipal, in channelv1.PreflightClaimRequest) (*channelv1.PreflightGrant, error) {
	s.claims++
	s.principal = p
	s.claim = in
	return s.grant, s.err
}
func (s *preflightRuntimeFake) Resolve(_ context.Context, p application.WorkloadPrincipal, id string, in channelv1.PreflightResolveRequest) (channelv1.PreflightResolveResponse, error) {
	s.resolves++
	s.principal = p
	s.id = id
	s.resolve = in
	return s.resolved, s.err
}
func (s *preflightRuntimeFake) Complete(_ context.Context, p application.WorkloadPrincipal, id string, in channelv1.PreflightCompleteRequest) error {
	s.completes++
	s.principal = p
	s.id = id
	s.complete = in
	return s.err
}

const preflightClaimBody = `{"schema_version":1,"scope_id":"pool","source_epoch":"11111111-1111-4111-8111-111111111111","instance_epoch":"22222222-2222-4222-8222-222222222222","claim_request_id":"33333333-3333-4333-8333-333333333333","claim_token":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA","gateway_config_digest":"sha256:699f4ff6f77d9069bb632ac9e1f29b7a0f996780217aa18d127dfe135e7f314d","expected_public_origin":"https://gateway.example.com","origin_status":"PUBLIC_ORIGIN_STATIC_VALID","limit":1}`

func preflightPrincipal() application.WorkloadPrincipal {
	return application.WorkloadPrincipal{PrincipalID: "spiffe://test/gateway", InstanceID: "gateway_one", ScopeID: "pool", Audience: application.WorkloadAudience, Consumers: []string{"telegram_preflight"}}
}
func preflightInternalRequest(h http.Handler, path, body string, authenticated bool) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	if authenticated {
		u, _ := url.Parse(preflightPrincipal().PrincipalID)
		cert := &x509.Certificate{URIs: []*url.URL{u}, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}}
		req.TLS = &tls.ConnectionState{PeerCertificates: []*x509.Certificate{cert}, VerifiedChains: [][]*x509.Certificate{{cert}}}
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	return w
}
func TestPreflightClaimEmptyQueueIsAuthenticatedNoStore204(t *testing.T) {
	s := &preflightRuntimeFake{}
	h, err := NewHandler(runtimeFake{}, []application.WorkloadPrincipal{preflightPrincipal()}, s)
	if err != nil {
		t.Fatal(err)
	}
	w := preflightInternalRequest(h, "/internal/v1/channel-preflights:claim", preflightClaimBody, true)
	if w.Code != 204 || w.Body.Len() != 0 || w.Header().Get("Cache-Control") != "no-store" || w.Header().Get("Retry-After") != "2" {
		t.Fatalf("status=%d headers=%v body=%s", w.Code, w.Header(), w.Body)
	}
	if s.claims != 1 || s.principal.PrincipalID != preflightPrincipal().PrincipalID || s.claim.Limit != 1 {
		t.Fatalf("runtime=%+v", s)
	}
}

func TestPreflightPrivateRequiresTLSAndDiagnosticConsumerBeforeDecode(t *testing.T) {
	for _, tc := range []struct {
		name          string
		authenticated bool
		consumers     []string
		want          int
	}{
		{"no TLS", false, []string{"telegram_preflight"}, 401},
		{"registration is not diagnostics", true, []string{"telegram_registration"}, 403},
		{"webhook is not diagnostics", true, []string{"telegram_webhook"}, 403},
		{"delivery is not diagnostics", true, []string{"telegram_delivery"}, 403},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := &preflightRuntimeFake{}
			p := preflightPrincipal()
			p.Consumers = tc.consumers
			h, err := NewHandler(runtimeFake{}, []application.WorkloadPrincipal{p}, s)
			if err != nil {
				t.Fatal(err)
			}
			for _, path := range []string{"/internal/v1/channel-preflights:claim", "/internal/v1/channel-preflights/cpf_test/credentials:resolve", "/internal/v1/channel-preflights/cpf_test:complete"} {
				w := preflightInternalRequest(h, path, "not-json", tc.authenticated)
				if w.Code != tc.want || w.Header().Get("Cache-Control") != "no-store" {
					t.Fatalf("path=%s status=%d headers=%v", path, w.Code, w.Header())
				}
			}
			if s.claims+s.resolves+s.completes != 0 {
				t.Fatal("untrusted call reached application")
			}
		})
	}
}
func preflightWireFixture(t *testing.T, name string) []byte {
	t.Helper()
	raw, err := os.ReadFile("../../../../../../../api/schemas/channel/v1/fixtures/preflight-" + name + "-valid.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixture struct {
		Document json.RawMessage `json:"document"`
	}
	if err := json.Unmarshal(raw, &fixture); err != nil {
		t.Fatal(err)
	}
	if len(fixture.Document) == 0 {
		t.Fatal("fixture has no document")
	}
	return fixture.Document
}
func TestPreflightPrivateSuccessfulGrantResolveAndComplete(t *testing.T) {
	s := &preflightRuntimeFake{grant: &channelv1.PreflightGrant{}}
	if err := json.Unmarshal(preflightWireFixture(t, "grant"), s.grant); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(preflightWireFixture(t, "resolved"), &s.resolved); err != nil {
		t.Fatal(err)
	}
	h, err := NewHandler(runtimeFake{}, []application.WorkloadPrincipal{preflightPrincipal()}, s)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, path, fixture string
		want                int
	}{
		{"claim", "/internal/v1/channel-preflights:claim", "claim", 200},
		{"resolve", "/internal/v1/channel-preflights/cpf_example/credentials:resolve", "resolve", 200},
		{"complete", "/internal/v1/channel-preflights/cpf_example:complete", "complete", 204},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := preflightInternalRequest(h, tc.path, string(preflightWireFixture(t, tc.fixture)), true)
			if w.Code != tc.want || w.Header().Get("Cache-Control") != "no-store" {
				t.Fatalf("status=%d headers=%v body=%s", w.Code, w.Header(), w.Body)
			}
			if tc.want == 200 {
				if w.Header().Get("Content-Type") != "application/json" {
					t.Fatal("missing JSON content type")
				}
				if strings.Contains(w.Body.String(), "claim_token") {
					t.Fatal("claim token echoed")
				}
			} else if w.Body.Len() != 0 {
				t.Fatal("204 contained body")
			}
		})
	}
	if s.claims != 1 || s.resolves != 1 || s.completes != 1 || s.id != "cpf_example" || s.principal.InstanceID != "gateway_one" {
		t.Fatalf("runtime=%+v", s)
	}
}
func TestPreflightPrivateInvalidRequestsNeverReachApplication(t *testing.T) {
	resolve := string(preflightWireFixture(t, "resolve"))
	complete := string(preflightWireFixture(t, "complete"))
	for _, tc := range []struct {
		name, path, body string
		want             int
	}{
		{"unknown field", "/internal/v1/channel-preflights:claim", strings.Replace(preflightClaimBody, `"limit":1`, `"limit":1,"probe_url":"https://evil.test"`, 1), 400},
		{"duplicate field", "/internal/v1/channel-preflights:claim", strings.Replace(preflightClaimBody, `"limit":1`, `"limit":1,"limit":1`, 1), 400},
		{"unsafe limit", "/internal/v1/channel-preflights:claim", strings.Replace(preflightClaimBody, `"limit":1`, `"limit":9007199254740992`, 1), 400},
		{"scope query", "/internal/v1/channel-preflights:claim?scope=evil", preflightClaimBody, 400},
		{"credential uses injection", "/internal/v1/channel-preflights/cpf_example/credentials:resolve", strings.TrimSuffix(strings.TrimSpace(resolve), "}") + `,"purpose":"telegram.webhook_secret"}`, 400},
		{"tail json", "/internal/v1/channel-preflights/cpf_example/credentials:resolve", resolve + `{}`, 400},
		{"invalid task", "/internal/v1/channel-preflights/_invalid/credentials:resolve", resolve, 400},
		{"claim body bound", "/internal/v1/channel-preflights:claim", strings.Repeat(" ", 4097), 413},
		{"resolve body bound", "/internal/v1/channel-preflights/cpf_example/credentials:resolve", strings.Repeat(" ", 4097), 413},
		{"complete body bound", "/internal/v1/channel-preflights/cpf_example:complete", strings.Repeat(" ", 16385), 413},
		{"unknown action", "/internal/v1/channel-preflights/cpf_example:cancel", complete, 404},
		{"invalid complete task", "/internal/v1/channel-preflights/_invalid:complete", complete, 400},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := &preflightRuntimeFake{}
			h, err := NewHandler(runtimeFake{}, []application.WorkloadPrincipal{preflightPrincipal()}, s)
			if err != nil {
				t.Fatal(err)
			}
			w := preflightInternalRequest(h, tc.path, tc.body, true)
			if w.Code != tc.want || s.claims+s.resolves+s.completes != 0 {
				t.Fatalf("status=%d body=%s", w.Code, w.Body)
			}
		})
	}
}
func TestPreflightPrivateErrorsAreTypedAndRedacted(t *testing.T) {
	for _, tc := range []struct {
		err  error
		want int
		code string
	}{
		{application.ErrWorkloadDenied, 403, "CHANNEL_WORKLOAD_DENIED"},
		{application.ErrEpochMismatch, 409, "CHANNEL_SOURCE_EPOCH_MISMATCH"},
		{&domain.Error{Code: "CHANNEL_PREFLIGHT_NOT_FOUND"}, 404, "CHANNEL_PREFLIGHT_NOT_FOUND"},
		{&domain.Error{Code: "CHANNEL_PREFLIGHT_RATE_LIMITED"}, 429, "CHANNEL_PREFLIGHT_RATE_LIMITED"},
		{&domain.Error{Code: "CHANNEL_PREFLIGHT_LEASE_EXPIRED"}, 409, "CHANNEL_PREFLIGHT_LEASE_EXPIRED"},
		{&domain.Error{Code: "CHANNEL_PREFLIGHT_RESULT_CONFLICT"}, 409, "CHANNEL_PREFLIGHT_RESULT_CONFLICT"},
		{errors.New("RAW_TOKEN_PROVIDER_ERROR"), 503, "CHANNEL_DEPENDENCY_UNAVAILABLE"},
	} {
		t.Run(tc.code, func(t *testing.T) {
			s := &preflightRuntimeFake{err: tc.err}
			h, err := NewHandler(runtimeFake{}, []application.WorkloadPrincipal{preflightPrincipal()}, s)
			if err != nil {
				t.Fatal(err)
			}
			w := preflightInternalRequest(h, "/internal/v1/channel-preflights:claim", preflightClaimBody, true)
			if w.Code != tc.want || !strings.Contains(w.Body.String(), tc.code) || strings.Contains(w.Body.String(), "RAW_TOKEN_PROVIDER_ERROR") {
				t.Fatalf("status=%d body=%s", w.Code, w.Body)
			}
			if tc.want == 429 && w.Header().Get("Retry-After") == "" {
				t.Fatal("no retry header")
			}
		})
	}
}

func preflightTLSCertificate(t *testing.T, ca *x509.Certificate, key ed25519.PrivateKey, client bool, uri string) (tls.Certificate, *x509.Certificate) {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 120))
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: serial, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature}
	if ca == nil {
		template.IsCA = true
		template.BasicConstraintsValid = true
		template.KeyUsage |= x509.KeyUsageCertSign
		ca = template
		key = priv
	} else if client {
		u, err := url.Parse(uri)
		if err != nil {
			t.Fatal(err)
		}
		template.URIs = []*url.URL{u}
		template.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
	} else {
		template.IPAddresses = []net.IP{net.ParseIP("127.0.0.1")}
		template.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
	}
	der, err := x509.CreateCertificate(rand.Reader, template, ca, pub, key)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: priv, Leaf: parsed}, parsed
}
func TestPreflightPrivateRealMTLSHandshakeAndSANMapping(t *testing.T) {
	caCert, ca := preflightTLSCertificate(t, nil, nil, false, "")
	caKey := caCert.PrivateKey.(ed25519.PrivateKey)
	serverCert, _ := preflightTLSCertificate(t, ca, caKey, false, "")
	clientCert, _ := preflightTLSCertificate(t, ca, caKey, true, preflightPrincipal().PrincipalID)
	otherCert, _ := preflightTLSCertificate(t, ca, caKey, true, "spiffe://test/not-mapped")
	pool := x509.NewCertPool()
	pool.AddCert(ca)
	s := &preflightRuntimeFake{}
	h, err := NewHandler(runtimeFake{}, []application.WorkloadPrincipal{preflightPrincipal()}, s)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewUnstartedServer(h)
	server.Config.ErrorLog = log.New(io.Discard, "", 0)
	server.TLS = &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{serverCert}, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: pool}
	server.StartTLS()
	defer server.Close()
	for _, tc := range []struct {
		name string
		cert *tls.Certificate
		want int
	}{{"trusted diagnostic", &clientCert, 204}, {"unknown SAN", &otherCert, 403}, {"missing client", nil, 0}} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &tls.Config{MinVersion: tls.VersionTLS13, RootCAs: pool}
			if tc.cert != nil {
				cfg.Certificates = []tls.Certificate{*tc.cert}
			}
			transport := &http.Transport{TLSClientConfig: cfg}
			defer transport.CloseIdleConnections()
			client := &http.Client{Transport: transport, Timeout: 3 * time.Second}
			req, err := http.NewRequest(http.MethodPost, server.URL+"/internal/v1/channel-preflights:claim", strings.NewReader(preflightClaimBody))
			if err != nil {
				t.Fatal(err)
			}
			req.Header.Set("Content-Type", "application/json")
			response, err := client.Do(req)
			if tc.want == 0 {
				if err == nil {
					response.Body.Close()
					t.Fatal("mTLS accepted absent certificate")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			defer response.Body.Close()
			if response.StatusCode != tc.want || response.Header.Get("Cache-Control") != "no-store" {
				t.Fatalf("status=%d headers=%v", response.StatusCode, response.Header)
			}
		})
	}
	if s.claims != 1 {
		t.Fatalf("authenticated calls=%d", s.claims)
	}
}
func TestPreflightPrivateOptionalSurfaceDoesNotChangeLegacyConstructor(t *testing.T) {
	h, err := NewHandler(runtimeFake{}, []application.WorkloadPrincipal{preflightPrincipal()})
	if err != nil {
		t.Fatal(err)
	}
	w := preflightInternalRequest(h, "/internal/v1/channel-preflights:claim", preflightClaimBody, true)
	if w.Code != 404 {
		t.Fatalf("unwired surface status=%d", w.Code)
	}
	for _, consumers := range [][]string{{"telegram_preflight", "telegram_preflight"}, {"unknown_diagnostic"}} {
		p := preflightPrincipal()
		p.Consumers = consumers
		if _, err := NewHandler(runtimeFake{}, []application.WorkloadPrincipal{p}, &preflightRuntimeFake{}); !errors.Is(err, application.ErrWorkloadDenied) {
			t.Fatal("invalid consumers accepted")
		}
	}
}
