package httpadapter

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	proof "github.com/liuzengh/trpc-agent-service/api/runtime/execution/v1"
)

const controlID = "spiffe://agent-platform/control-api"
const gatewayID = "spiffe://agent-platform/channel-gateway"
const workerID = "spiffe://agent-platform/agent-worker"

type attemptStub struct {
	calls            atomic.Int64
	err              error
	response         *proof.AttemptResponse
	entered, release chan struct{}
}

func (a *attemptStub) VerifyAttempt(ctx context.Context, r proof.AttemptRequest) (proof.AttemptResponse, error) {
	a.calls.Add(1)
	if a.entered != nil {
		close(a.entered)
		select {
		case <-a.release:
		case <-ctx.Done():
			return proof.AttemptResponse{}, ctx.Err()
		}
	}
	if a.response != nil {
		return *a.response, a.err
	}
	return proof.AttemptResponse{TenantID: "tenant", ProfileID: "profile", ProfileRevisionNumber: 1, RunID: "run", AttemptID: "attempt", WorkerID: r.WorkloadIdentity, LeaseEpoch: 1, ExpiresAt: time.Now().UTC().Add(time.Minute), ManifestID: r.ManifestID, ManifestDigest: r.ManifestDigest, AllowedUses: []proof.CredentialUse{}}, a.err
}

type finalStub struct {
	calls    int
	err      error
	response *proof.FinalResponse
}

func (f *finalStub) VerifyFinal(_ context.Context, r proof.FinalRequest) (proof.FinalResponse, error) {
	f.calls++
	if f.response != nil {
		return *f.response, f.err
	}
	return proof.FinalResponse{FinalRequest: r, TenantID: "tenant", ManifestDigest: "sha256:" + strings.Repeat("a", 64)}, f.err
}
func fixture(t *testing.T) (*Handler, *attemptStub, *finalStub) {
	t.Helper()
	a := &attemptStub{}
	f := &finalStub{}
	h, e := New(a, f, Options{ControlPrincipals: []string{controlID}, GatewayPrincipals: []string{gatewayID}})
	if e != nil {
		t.Fatal(e)
	}
	return h, a, f
}
func attemptBody() []byte {
	body, _ := proof.EncodeAttemptRequest(proof.AttemptRequest{WorkloadIdentity: workerID, ExecutionToken: "sensitive-attempt-token", ManifestID: "manifest", ManifestDigest: "sha256:" + strings.Repeat("a", 64)})
	return body
}
func finalBody() []byte {
	body, _ := proof.EncodeFinalRequest(proof.FinalRequest{IntentID: "intent", Digest: "sha256:" + strings.Repeat("b", 64), AdmissionID: "admission", RunID: "run", AttemptID: "attempt", CompletionID: "completion", ExecutionGeneration: 1, Sequence: 1})
	return body
}
func request(path string, body []byte, identities ...string) *http.Request {
	r := httptest.NewRequest("POST", path, bytes.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	if identities != nil {
		leaf := &x509.Certificate{}
		for _, raw := range identities {
			u, _ := url.Parse(raw)
			leaf.URIs = append(leaf.URIs, u)
		}
		r.TLS = &tls.ConnectionState{VerifiedChains: [][]*x509.Certificate{{leaf}}}
	}
	return r
}
func TestProofCallerIdentityIsEndpointSpecific(t *testing.T) {
	for _, test := range []struct {
		name, path string
		body       []byte
		ids        []string
		status     int
	}{{"missing TLS", proof.FinalVerifyPath, finalBody(), nil, 401}, {"header spoof", proof.FinalVerifyPath, finalBody(), nil, 401}, {"wrong workload", proof.FinalVerifyPath, finalBody(), []string{workerID}, 403}, {"multiple URI", proof.FinalVerifyPath, finalBody(), []string{gatewayID, controlID}, 403}, {"Gateway cannot query active lease", proof.AttemptVerifyPath, attemptBody(), []string{gatewayID}, 403}, {"Control committed Final", proof.FinalVerifyPath, finalBody(), []string{controlID}, 200}, {"Control active", proof.AttemptVerifyPath, attemptBody(), []string{controlID}, 200}, {"Gateway final", proof.FinalVerifyPath, finalBody(), []string{gatewayID}, 200}} {
		t.Run(test.name, func(t *testing.T) {
			h, a, f := fixture(t)
			r := request(test.path, test.body, test.ids...)
			r.Header.Set("Authorization", "Bearer admin")
			r.Header.Set("X-Workload-Identity", gatewayID)
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			if w.Code != test.status {
				t.Fatal(w.Code, w.Body.String())
			}
			if test.status != 200 && (a.calls.Load() != 0 || f.calls != 0) {
				t.Fatal("unauthorized callback")
			}
			if w.Header().Get("Cache-Control") != "no-store" {
				t.Fatal("proof cacheable")
			}
		})
	}
}
func TestUnverifiedPeerCertificateDoesNotAuthenticate(t *testing.T) {
	h, a, f := fixture(t)
	r := request(proof.FinalVerifyPath, finalBody(), gatewayID)
	leaf := r.TLS.VerifiedChains[0][0]
	r.TLS = &tls.ConnectionState{PeerCertificates: []*x509.Certificate{leaf}}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != 401 || a.calls.Load() != 0 || f.calls != 0 {
		t.Fatal(w.Code)
	}
}
func TestStrictProofBodiesNeverReachApplication(t *testing.T) {
	for _, test := range []struct {
		path   string
		body   []byte
		status int
	}{{proof.FinalVerifyPath, []byte(`{"intent_id":"i","intent_id":"j"}`), 400}, {proof.AttemptVerifyPath, []byte(`{"execution_token":"x","execution_token":"y"}`), 400}, {proof.FinalVerifyPath, []byte(strings.Repeat("x", proof.MaxFinalProofBytes+1)), 413}, {proof.AttemptVerifyPath, []byte(strings.Repeat("x", proof.MaxAttemptProofBytes+1)), 413}} {
		h, a, f := fixture(t)
		id := gatewayID
		if test.path == proof.AttemptVerifyPath {
			id = controlID
		}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, request(test.path, test.body, id))
		if w.Code != test.status || a.calls.Load() != 0 || f.calls != 0 {
			t.Fatal(w.Code, w.Body.String())
		}
	}
}
func TestProofErrorsHaveStableNonSensitiveClassification(t *testing.T) {
	for _, test := range []struct {
		err    error
		status int
		code   string
	}{{ErrAttemptDenied, 403, "ATTEMPT_DENIED"}, {ErrFinalMismatch, 409, "FINAL_MISMATCH"}, {ErrNotYet, 404, "PROOF_NOT_YET"}, {ErrUnavailable, 503, "PROOF_UNAVAILABLE"}, {errors.New("postgres://sensitive-admin:password@database"), 503, "PROOF_UNAVAILABLE"}} {
		h, _, f := fixture(t)
		f.err = test.err
		w := httptest.NewRecorder()
		h.ServeHTTP(w, request(proof.FinalVerifyPath, finalBody(), gatewayID))
		if w.Code != test.status || w.Body.String() != `{"code":"`+test.code+`"}` {
			t.Fatal(w.Code, w.Body.String())
		}
	}
}
func TestEndedAttemptDoesNotInvalidateCommittedFinal(t *testing.T) {
	h, a, f := fixture(t)
	a.err = ErrAttemptDenied
	first := httptest.NewRecorder()
	h.ServeHTTP(first, request(proof.AttemptVerifyPath, attemptBody(), controlID))
	if first.Code != 403 {
		t.Fatal(first.Code)
	}
	second := httptest.NewRecorder()
	h.ServeHTTP(second, request(proof.FinalVerifyPath, finalBody(), gatewayID))
	if second.Code != 200 || f.calls != 1 {
		t.Fatal(second.Code)
	}
	if _, err := proof.DecodeFinalResponse(second.Body.Bytes()); err != nil {
		t.Fatal(err)
	}
}
func TestInvalidApplicationEvidenceIsUnavailableNotAuthorized(t *testing.T) {
	h, a, f := fixture(t)
	f.response = &proof.FinalResponse{}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, request(proof.FinalVerifyPath, finalBody(), gatewayID))
	if w.Code != 503 {
		t.Fatal(w.Code)
	}
	a.response = &proof.AttemptResponse{}
	w = httptest.NewRecorder()
	h.ServeHTTP(w, request(proof.AttemptVerifyPath, attemptBody(), controlID))
	if w.Code != 503 {
		t.Fatal(w.Code)
	}
}
func TestProofConcurrencyIsBounded(t *testing.T) {
	a := &attemptStub{entered: make(chan struct{}), release: make(chan struct{})}
	f := &finalStub{}
	h, e := New(a, f, Options{ControlPrincipals: []string{controlID}, GatewayPrincipals: []string{gatewayID}, MaxConcurrent: 1})
	if e != nil {
		t.Fatal(e)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		h.ServeHTTP(httptest.NewRecorder(), request(proof.AttemptVerifyPath, attemptBody(), controlID))
	}()
	select {
	case <-a.entered:
	case <-time.After(time.Second):
		t.Fatal("first request did not enter")
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, request(proof.FinalVerifyPath, finalBody(), gatewayID))
	if w.Code != 503 || f.calls != 0 {
		t.Fatal("capacity admitted", w.Code)
	}
	close(a.release)
	<-done
}
func TestProofIdentityConfigIsExplicitAndDisjoint(t *testing.T) {
	for _, o := range []Options{{}, {ControlPrincipals: []string{controlID}, GatewayPrincipals: []string{controlID}}, {ControlPrincipals: []string{"control"}, GatewayPrincipals: []string{gatewayID}}, {ControlPrincipals: []string{controlID, controlID}, GatewayPrincipals: []string{gatewayID}}} {
		if _, e := New(&attemptStub{}, &finalStub{}, o); e == nil {
			t.Fatal("invalid config accepted", o)
		}
	}
}

func TestProofHandlerRealMutualTLS(t *testing.T) {
	h, a, f := fixture(t)
	now := time.Now()
	pub, key, e := ed25519.GenerateKey(rand.Reader)
	if e != nil {
		t.Fatal(e)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test proof root"}, NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	der, e := x509.CreateCertificate(rand.Reader, template, template, pub, key)
	if e != nil {
		t.Fatal(e)
	}
	ca, e := x509.ParseCertificate(der)
	if e != nil {
		t.Fatal(e)
	}
	roots := x509.NewCertPool()
	roots.AddCert(ca)
	issue := func(serial int64, usage x509.ExtKeyUsage, identity string) tls.Certificate {
		p, k, e := ed25519.GenerateKey(rand.Reader)
		if e != nil {
			t.Fatal(e)
		}
		leaf := &x509.Certificate{SerialNumber: big.NewInt(serial), Subject: pkix.Name{CommonName: "fixture"}, NotBefore: now.Add(-time.Minute), NotAfter: now.Add(time.Hour), ExtKeyUsage: []x509.ExtKeyUsage{usage}, KeyUsage: x509.KeyUsageDigitalSignature, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}}
		if identity != "" {
			u, _ := url.Parse(identity)
			leaf.URIs = []*url.URL{u}
		}
		der, e := x509.CreateCertificate(rand.Reader, leaf, ca, p, key)
		if e != nil {
			t.Fatal(e)
		}
		raw, e := x509.MarshalPKCS8PrivateKey(k)
		if e != nil {
			t.Fatal(e)
		}
		cert, e := tls.X509KeyPair(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: raw}))
		if e != nil {
			t.Fatal(e)
		}
		return cert
	}
	server := httptest.NewUnstartedServer(h)
	server.TLS = &tls.Config{Certificates: []tls.Certificate{issue(2, x509.ExtKeyUsageServerAuth, "")}, ClientCAs: roots, ClientAuth: tls.RequireAndVerifyClientCert, MinVersion: tls.VersionTLS12}
	server.StartTLS()
	defer server.Close()
	call := func(serial int64, identity, path string, body []byte) int {
		t.Helper()
		tr := &http.Transport{TLSClientConfig: &tls.Config{RootCAs: roots, Certificates: []tls.Certificate{issue(serial, x509.ExtKeyUsageClientAuth, identity)}, MinVersion: tls.VersionTLS12}}
		defer tr.CloseIdleConnections()
		client := &http.Client{Transport: tr, Timeout: time.Second}
		response, e := client.Post(server.URL+path, "application/json", bytes.NewReader(body))
		if e != nil {
			t.Fatal(e)
		}
		defer response.Body.Close()
		_, _ = io.Copy(io.Discard, response.Body)
		return response.StatusCode
	}
	if status := call(3, controlID, proof.AttemptVerifyPath, attemptBody()); status != 200 {
		t.Fatal(status)
	}
	if status := call(4, gatewayID, proof.FinalVerifyPath, finalBody()); status != 200 {
		t.Fatal(status)
	}
	if status := call(5, workerID, proof.FinalVerifyPath, finalBody()); status != 403 {
		t.Fatal(status)
	}
	if status := call(6, gatewayID, proof.AttemptVerifyPath, attemptBody()); status != 403 {
		t.Fatal(status)
	}
	if a.calls.Load() != 1 || f.calls != 1 {
		t.Fatal("incorrect authenticated callback counts", a.calls.Load(), f.calls)
	}
}
