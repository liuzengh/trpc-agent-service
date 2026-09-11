package httpadapter

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"errors"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	proof "github.com/liuzengh/trpc-agent-service/api/runtime/execution/v1"
)

type replyArtifactStub struct {
	calls  int
	result proof.ReplyArtifactResponse
	err    error
	wait   bool
}

func (s *replyArtifactStub) ReplyArtifact(ctx context.Context, r proof.ReplyArtifactRequest) (proof.ReplyArtifactResponse, error) {
	s.calls++
	if r.Version != 0 || r.Name != "report.txt" {
		return proof.ReplyArtifactResponse{}, ErrArtifactMissing
	}
	if s.wait {
		<-ctx.Done()
	}
	return s.result, s.err
}
func replyArtifactBody() []byte {
	b, _ := proof.EncodeReplyArtifactRequest(proof.ReplyArtifactRequest{IntentID: "intent", RunID: "run", CompletionID: "completion", Name: "report.txt", Version: 0})
	return b
}
func replyResult() proof.ReplyArtifactResponse {
	b := []byte("trusted\nartifact\t中\x00")
	sum := sha256.Sum256(b)
	return proof.ReplyArtifactResponse{Content: b, SizeBytes: len(b), MimeType: "application/octet-stream", SHA256: hex.EncodeToString(sum[:])}
}
func replyHandler(t *testing.T, s *replyArtifactStub) *Handler {
	t.Helper()
	h, e := New(&attemptStub{}, &finalStub{}, Options{ControlPrincipals: []string{controlID}, GatewayPrincipals: []string{gatewayID}, ReplyArtifacts: s, Timeout: time.Millisecond})
	if e != nil {
		t.Fatal(e)
	}
	return h
}
func TestReplyArtifactGatewayOnlyAndRawBytes(t *testing.T) {
	for _, tc := range []struct {
		name       string
		ids        []string
		unverified bool
		status     int
	}{{"missing", nil, false, 401}, {"control", []string{controlID}, false, 403}, {"worker", []string{workerID}, false, 403}, {"multiple", []string{gatewayID, controlID}, false, 403}, {"unverified", []string{gatewayID}, true, 401}, {"gateway", []string{gatewayID}, false, 200}} {
		t.Run(tc.name, func(t *testing.T) {
			s := &replyArtifactStub{result: replyResult()}
			want := bytes.Clone(s.result.Content)
			h := replyHandler(t, s)
			r := request(proof.ReplyArtifactPath, replyArtifactBody(), tc.ids...)
			r.Header.Set("X-Workload-Identity", gatewayID)
			if tc.unverified {
				r.TLS = &tls.ConnectionState{PeerCertificates: r.TLS.VerifiedChains[0]}
			}
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			if w.Code != tc.status {
				t.Fatal(w.Code, w.Body.String())
			}
			if tc.status != 200 {
				if s.calls != 0 || w.Header().Get("X-Content-SHA256") != "" {
					t.Fatal("unauthorized read")
				}
				return
			}
			if s.calls != 1 || !bytes.Equal(w.Body.Bytes(), want) || w.Header().Get("Content-Type") != "application/octet-stream" || w.Header().Get("X-Content-SHA256") != s.result.SHA256 || w.Header().Get("Content-Length") == "" || w.Header().Get("Cache-Control") != "no-store" {
				t.Fatal("raw response contract", w.Header())
			}
			if !bytes.Equal(s.result.Content, make([]byte, len(want))) {
				t.Fatal("response buffer not cleared")
			}
		})
	}
}
func TestReplyArtifactErrorsNeverCarrySuccessMetadata(t *testing.T) {
	for _, tc := range []struct {
		name   string
		err    error
		status int
		mutate func(*replyArtifactStub)
	}{
		{"missing", ErrArtifactMissing, 404, nil}, {"denied", ErrAttemptDenied, 403, nil}, {"conflict", ErrFinalMismatch, 403, nil}, {"dependency", errors.New("secret backend diagnostic"), 503, nil}, {"capacity", ErrArtifactCapacity, 413, nil},
		{"checksum", nil, 503, func(s *replyArtifactStub) { s.result.SHA256 = strings.Repeat("0", 64) }}, {"size", nil, 503, func(s *replyArtifactStub) { s.result.SizeBytes++ }}, {"mime", nil, 503, func(s *replyArtifactStub) { s.result.MimeType = "text/plain\r\nX: y" }}, {"cancelled", nil, 503, func(s *replyArtifactStub) { s.wait = true }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := &replyArtifactStub{result: replyResult(), err: tc.err}
			if tc.mutate != nil {
				tc.mutate(s)
			}
			h := replyHandler(t, s)
			w := httptest.NewRecorder()
			h.ServeHTTP(w, request(proof.ReplyArtifactPath, replyArtifactBody(), gatewayID))
			if w.Code != tc.status || w.Header().Get("Content-Type") != "application/json" || w.Header().Get("X-Content-SHA256") != "" || w.Header().Get("Content-Length") != "" || strings.Contains(w.Body.String(), "secret") {
				t.Fatal(w.Code, w.Header(), w.Body.String())
			}
		})
	}
}
func TestReplyArtifactMalformedWireDoesNotRead(t *testing.T) {
	text := string(replyArtifactBody())
	for _, body := range []string{strings.Replace(text, `,"version":0`, "", 1), strings.Replace(text, "{", `{"tenant_id":"injected",`, 1), strings.Replace(text, `"version":0`, `"version":0,"version":1`, 1), strings.Repeat("x", proof.MaxReplyArtifactRequestBytes+1)} {
		s := &replyArtifactStub{result: replyResult()}
		h := replyHandler(t, s)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, request(proof.ReplyArtifactPath, []byte(body), gatewayID))
		if w.Code != http.StatusBadRequest && w.Code != http.StatusRequestEntityTooLarge {
			t.Fatal(w.Code)
		}
		if s.calls != 0 {
			t.Fatal("malformed reached application")
		}
	}
}

func TestReplyArtifactRealMutualTLSRawBody(t *testing.T) {
	stub := &replyArtifactStub{result: replyResult()}
	h := replyHandler(t, stub)
	h.timeout = time.Second
	now := time.Now()
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	ca := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "reply-artifact-fixture-ca"}, IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign, NotBefore: now.Add(-time.Minute), NotAfter: now.Add(time.Hour)}
	raw, err := x509.CreateCertificate(rand.Reader, ca, ca, pub, key)
	if err != nil {
		t.Fatal(err)
	}
	ca, err = x509.ParseCertificate(raw)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(ca)
	issue := func(serial int64, identity string, usage x509.ExtKeyUsage) tls.Certificate {
		p, k, e := ed25519.GenerateKey(rand.Reader)
		if e != nil {
			t.Fatal(e)
		}
		leaf := &x509.Certificate{SerialNumber: big.NewInt(serial), NotBefore: now.Add(-time.Minute), NotAfter: now.Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{usage}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}}
		if identity != "" {
			u, _ := url.Parse(identity)
			leaf.URIs = []*url.URL{u}
		}
		der, e := x509.CreateCertificate(rand.Reader, leaf, ca, p, key)
		if e != nil {
			t.Fatal(e)
		}
		return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: k}
	}
	server := httptest.NewUnstartedServer(h)
	server.TLS = &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{issue(2, "", x509.ExtKeyUsageServerAuth)}, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: roots}
	server.StartTLS()
	defer server.Close()
	for i, identity := range []string{controlID, workerID, gatewayID} {
		tr := &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots, Certificates: []tls.Certificate{issue(int64(i+3), identity, x509.ExtKeyUsageClientAuth)}}}
		client := &http.Client{Transport: tr, Timeout: time.Second}
		response, e := client.Post(server.URL+proof.ReplyArtifactPath, "application/json", bytes.NewReader(replyArtifactBody()))
		if e != nil {
			tr.CloseIdleConnections()
			t.Fatal(e)
		}
		body, e := io.ReadAll(response.Body)
		response.Body.Close()
		tr.CloseIdleConnections()
		if e != nil {
			t.Fatal(e)
		}
		if identity == gatewayID {
			if response.StatusCode != 200 || !bytes.Equal(body, replyResult().Content) || response.Header.Get("X-Content-SHA256") != replyResult().SHA256 {
				t.Fatal("authenticated raw byte contract")
			}
		} else if response.StatusCode != 403 || response.Header.Get("X-Content-SHA256") != "" {
			t.Fatal("wrong principal read", response.StatusCode)
		}
	}
	if stub.calls != 1 {
		t.Fatal("caller count", stub.calls)
	}
}

func TestReplyArtifactEmptyFileIsNotMissing(t *testing.T) {
	sum := sha256.Sum256(nil)
	stub := &replyArtifactStub{result: proof.ReplyArtifactResponse{Content: []byte{}, SizeBytes: 0, MimeType: "application/octet-stream", SHA256: hex.EncodeToString(sum[:])}}
	w := httptest.NewRecorder()
	replyHandler(t, stub).ServeHTTP(w, request(proof.ReplyArtifactPath, replyArtifactBody(), gatewayID))
	if w.Code != 200 || w.Header().Get("Content-Length") != "0" || w.Body.Len() != 0 || w.Header().Get("X-Content-SHA256") != stub.result.SHA256 {
		t.Fatal(w.Code, w.Header())
	}
}
