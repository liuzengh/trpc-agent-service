package workerhttp

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	wire "github.com/liuzengh/trpc-agent-service/api/runtime/execution/v1"
	d "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/delivery/domain"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func mtls(t *testing.T, handler http.Handler) (*Client, *httptest.Server) {
	t.Helper()
	now := time.Now()
	caKey, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	ca := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "proof-ca"}, NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	caDER, e := x509.CreateCertificate(rand.Reader, ca, ca, &caKey.PublicKey, caKey)
	if e != nil {
		t.Fatal(e)
	}
	roots := x509.NewCertPool()
	roots.AddCert(ca)
	// Add the parsed certificate: its RawSubject and signature are needed by TLS.
	parsed, e := x509.ParseCertificate(caDER)
	if e != nil {
		t.Fatal(e)
	}
	roots = x509.NewCertPool()
	roots.AddCert(parsed)
	issue := func(n int64, usage x509.ExtKeyUsage) tls.Certificate {
		k, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		template := &x509.Certificate{SerialNumber: big.NewInt(n), Subject: pkix.Name{CommonName: "fixture"}, NotBefore: now.Add(-time.Minute), NotAfter: now.Add(time.Hour), ExtKeyUsage: []x509.ExtKeyUsage{usage}, KeyUsage: x509.KeyUsageDigitalSignature, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}}
		der, e := x509.CreateCertificate(rand.Reader, template, parsed, &k.PublicKey, caKey)
		if e != nil {
			t.Fatal(e)
		}
		key, e := x509.MarshalECPrivateKey(k)
		if e != nil {
			t.Fatal(e)
		}
		cert, e := tls.X509KeyPair(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: key}))
		if e != nil {
			t.Fatal(e)
		}
		return cert
	}
	s := httptest.NewUnstartedServer(handler)
	s.TLS = &tls.Config{Certificates: []tls.Certificate{issue(2, x509.ExtKeyUsageServerAuth)}, ClientCAs: roots, ClientAuth: tls.RequireAndVerifyClientCert, MinVersion: tls.VersionTLS12}
	s.StartTLS()
	t.Cleanup(s.Close)
	c, e := New(Options{BaseURL: s.URL, RootCAs: roots, Certificate: issue(3, x509.ExtKeyUsageClientAuth)})
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(c.Close)
	return c, s
}
func intent() d.Intent {
	return d.Intent{ID: "intent", AdmissionID: "admission", RunID: "run", AttemptID: "attempt", CompletionID: "completion", ExecutionGeneration: 2, Sequence: 1, Text: "Final", Deadline: time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)}
}
func TestCommittedFinalMTLSAndExactContract(t *testing.T) {
	calls := 0
	c, _ := mtls(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.Method != "POST" || r.URL.Path != wire.FinalVerifyPath || r.TLS == nil || len(r.TLS.VerifiedChains) == 0 {
			t.Error("missing authenticated proof protocol")
		}
		raw, _ := io.ReadAll(r.Body)
		q, e := wire.DecodeFinalRequest(raw)
		if e != nil {
			t.Error(e)
			w.WriteHeader(400)
			return
		}
		body, e := wire.EncodeFinalResponse(wire.FinalResponse{FinalRequest: q, TenantID: "tenant", ManifestDigest: "sha256:" + strings.Repeat("b", 64)})
		if e != nil {
			t.Error(e)
		}
		_, _ = w.Write(body)
	}))
	i := intent()
	digest, e := d.IntentDigest(i)
	if e != nil {
		t.Fatal(e)
	}
	proof, e := c.VerifyCommittedFinal(context.Background(), i, digest)
	if e != nil || proof.IntentID != i.ID || proof.Digest != digest || proof.TenantID != "tenant" || proof.ExecutionGeneration != 2 || calls != 1 {
		t.Fatal(proof, e, calls)
	}
}
func TestProofErrorClassification(t *testing.T) {
	for _, code := range []int{400, 401, 403, 404, 409, 429, 500, 503, 307} {
		t.Run(http.StatusText(code), func(t *testing.T) {
			c, _ := mtls(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Location", "https://untrusted.invalid")
				w.WriteHeader(code)
			}))
			i := intent()
			digest, _ := d.IntentDigest(i)
			_, e := c.VerifyCommittedFinal(context.Background(), i, digest)
			want := d.ErrUnavailable
			if code == 409 {
				want = d.ErrUnauthorized
			}
			if !errors.Is(e, want) {
				t.Fatal(code, e)
			}
		})
	}
}
func TestInvalidProofResponsesRemainRetryable(t *testing.T) {
	for _, body := range []string{`{}`, strings.Repeat("x", wire.MaxFinalProofBytes+1), `{"intent_id":"intent","intent_id":"other"}`} {
		t.Run("protocol", func(t *testing.T) {
			c, _ := mtls(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, body) }))
			i := intent()
			digest, _ := d.IntentDigest(i)
			if _, e := c.VerifyCommittedFinal(context.Background(), i, digest); !errors.Is(e, d.ErrUnavailable) {
				t.Fatal(e)
			}
		})
	}
}
func TestMismatchedCommittedProofPermanentlyRejects(t *testing.T) {
	c, _ := mtls(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		q, _ := wire.DecodeFinalRequest(raw)
		q.CompletionID = "other"
		body, _ := wire.EncodeFinalResponse(wire.FinalResponse{FinalRequest: q, TenantID: "tenant", ManifestDigest: "sha256:" + strings.Repeat("b", 64)})
		_, _ = w.Write(body)
	}))
	i := intent()
	digest, _ := d.IntentDigest(i)
	if _, e := c.VerifyCommittedFinal(context.Background(), i, digest); !errors.Is(e, d.ErrUnauthorized) {
		t.Fatal(e)
	}
}
func TestProofCanceledCallRemainsRetryable(t *testing.T) {
	c, _ := mtls(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	i := intent()
	digest, _ := d.IntentDigest(i)
	if _, e := c.VerifyCommittedFinal(ctx, i, digest); !errors.Is(e, d.ErrUnavailable) {
		t.Fatal(e)
	}
}
