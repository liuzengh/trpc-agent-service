package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
)

// TestCheckAdminToken pins the fail-closed gate: the Admin API must never
// start unauthenticated by accident, only with an explicit dev sentinel.
func TestCheckAdminToken(t *testing.T) {
	if err := checkAdminToken(config.Config{}); err == nil {
		t.Fatal("unset TRPC_ADMIN_TOKEN must refuse to serve the Admin API")
	}
	if err := checkAdminToken(config.Config{
		AdminToken: config.AdminTokenDevInsecure, AdminAddr: "127.0.0.1:8081",
	}); err != nil {
		t.Fatalf("explicit dev sentinel on loopback must be accepted: %v", err)
	}
	// The sentinel is a public constant: pairing it with an all-interfaces
	// bind would publish an open management plane.
	if err := checkAdminToken(config.Config{
		AdminToken: config.AdminTokenDevInsecure, AdminAddr: ":8081",
	}); err == nil {
		t.Fatal("dev sentinel on an all-interfaces bind must be refused")
	}
	if err := checkAdminToken(config.Config{AdminToken: "s3cret"}); err != nil {
		t.Fatalf("a real token must be accepted: %v", err)
	}
}

// TestInstanceID covers the replica identity that ends up in stream consumer
// names and lock owner tokens: a shared name across replicas is what lets one
// replica renew another's expired session lease.
func TestInstanceID(t *testing.T) {
	first := instanceID()
	if first == "" {
		t.Fatal("instance id must never be empty")
	}
	if second := instanceID(); second != first {
		t.Fatalf("instance id must be stable within one process: %q then %q", first, second)
	}
}

// TestSchemeIs pins the IM endpoint gate: the token endpoints carry the corp
// secret in the query string, so anything but an https/wss URL with a host is
// refused and the channel is disabled at startup.
func TestSchemeIs(t *testing.T) {
	if !schemeIs("https://qyapi.weixin.qq.com", "https") {
		t.Fatal("https base must be accepted")
	}
	if !schemeIs("wss://openws.work.weixin.qq.com", "wss") {
		t.Fatal("wss addr must be accepted")
	}
	for _, bad := range []string{
		"http://qyapi.weixin.qq.com", "ws://127.0.0.1:9000", "://bad", "", "https://",
	} {
		if schemeIs(bad, "https") {
			t.Errorf("https: %q must be refused", bad)
		}
		if schemeIs(bad, "wss") {
			t.Errorf("wss: %q must be refused", bad)
		}
	}
}

func TestAdminTLSConfigUnset(t *testing.T) {
	cfg, err := adminTLSConfig(config.Config{})
	if cfg != nil || err != nil {
		t.Fatalf("unset mTLS envs must yield nil config, got %v %v", cfg, err)
	}
}

func TestAdminTLSConfigMissingFiles(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Config{
		AdminTLSCert:     filepath.Join(dir, "nope.crt"),
		AdminTLSKey:      filepath.Join(dir, "nope.key"),
		AdminTLSClientCA: filepath.Join(dir, "nope-ca.crt"),
	}
	if _, err := adminTLSConfig(cfg); err == nil {
		t.Fatal("missing cert files must fail fast")
	}
}

// TestAdminMTLSHandshake generates a CA + server + client certs and verifies
// the admin listener accepts only clients with a CA-signed certificate.
func TestAdminMTLSHandshake(t *testing.T) {
	dir := t.TempDir()
	ca := makeCA(t)
	caPath := filepath.Join(dir, "ca.crt")
	writePEM(t, caPath, "CERTIFICATE", ca.der)
	serverCert, serverKey := signCert(t, ca, "server", []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth})
	clientCert, clientKey := signCert(t, ca, "admin-client", []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth})

	tlsCfg, err := adminTLSConfig(config.Config{
		AdminTLSCert: serverCert, AdminTLSKey: serverKey, AdminTLSClientCA: caPath,
	})
	if err != nil {
		t.Fatal(err)
	}
	if tlsCfg.ClientAuth != tls.RequireAndVerifyClientCert {
		t.Fatal("mTLS must require verified client certs")
	}

	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "ok")
	}))
	srv.TLS = tlsCfg
	srv.StartTLS()
	defer srv.Close()

	// No client cert → handshake fails.
	if _, err := srv.Client().Get(srv.URL); err == nil {
		t.Fatal("client without a certificate must be rejected")
	}

	// CA-signed client cert → 200.
	cert, err := tls.LoadX509KeyPair(clientCert, clientKey)
	if err != nil {
		t.Fatal(err)
	}
	client := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      caPool(t, ca),
	}}}
	resp, err := client.Get(srv.URL)
	if err != nil {
		t.Fatalf("client with a CA-signed cert must pass: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d", resp.StatusCode)
	}
}

// testCA is a self-signed CA used to mint server/client certificates.
type testCA struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
	der  []byte
}

func makeCA(t *testing.T) testCA {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tpl, tpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return testCA{cert: cert, key: key, der: der}
}

// signCert mints a CA-signed certificate and returns PEM file paths.
func signCert(t *testing.T, ca testCA, cn string, usages []x509.ExtKeyUsage) (certPath, keyPath string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: cn},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		ExtKeyUsage:  usages,
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, tpl, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	certPath = filepath.Join(dir, cn+".crt")
	keyPath = filepath.Join(dir, cn+".key")
	writePEM(t, certPath, "CERTIFICATE", der)
	ecKey, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	writePEM(t, keyPath, "EC PRIVATE KEY", ecKey)
	return certPath, keyPath
}

func writePEM(t *testing.T, path, blockType string, der []byte) {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	if err := pem.Encode(f, &pem.Block{Type: blockType, Bytes: der}); err != nil {
		t.Fatal(err)
	}
}

func caPool(t *testing.T, ca testCA) *x509.CertPool {
	t.Helper()
	pool := x509.NewCertPool()
	pool.AddCert(ca.cert)
	return pool
}
