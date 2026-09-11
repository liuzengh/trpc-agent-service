package natsadapter

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestExplicitCARejectsPlaintextAndMixedFailoverBeforeDial(t *testing.T) {
	topology, err := LoadTopology("../../../../../deploy/nats/streams.yaml")
	if err != nil {
		t.Fatal(err)
	}
	for _, server := range []string{"nats://127.0.0.1:1", "tls://broker:4222,nats://other:4222", "", "tls://", "tls://user:secret@broker:4222", "https://broker:4222", "tls://broker:4222?x=1"} {
		if _, err := Connect(server, topology, Auth{CAFile: "must-not-be-opened.pem"}); err == nil || err.Error() != "NATS CA requires tls:// server origins" {
			t.Fatal("invalid TLS endpoint reached dial", server, err)
		}
	}
	for _, server := range []string{"tls://nats:4222", "tls://one:4222,tls://two:4222"} {
		if err := (Auth{CAFile: "ca.pem"}).ValidateServerURL(server); err != nil {
			t.Fatal(err)
		}
	}
	if err := (Auth{}).ValidateServerURL("nats://fixture:4222"); err != nil {
		t.Fatal("plaintext fixture changed", err)
	}
}
func unrelatedCA(t *testing.T) string {
	t.Helper()
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "unrelated test CA"}, IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour)}
	der, err := x509.CreateCertificate(rand.Reader, template, template, pub, key)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "unrelated-ca.pem")
	if err = os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}
func TestNATSTrustedTLSIntegration(t *testing.T) {
	address, ca := os.Getenv("GATEWAY_TEST_TLS_NATS_URL"), os.Getenv("GATEWAY_TEST_TLS_NATS_CA_FILE")
	if address == "" || ca == "" {
		t.Skip("GATEWAY_TEST_TLS_NATS_URL and GATEWAY_TEST_TLS_NATS_CA_FILE required")
	}
	topology, err := LoadTopology("../../../../../deploy/nats/streams.yaml")
	if err != nil {
		t.Fatal(err)
	}
	auth := Auth{User: os.Getenv("GATEWAY_TEST_TLS_NATS_USER"), Password: os.Getenv("GATEWAY_TEST_TLS_NATS_PASSWORD"), CAFile: ca, InboxPrefix: "_INBOX.gateway"}
	transport, err := Connect(address, topology, auth)
	if err != nil {
		t.Fatal("trusted TLS broker rejected", err)
	}
	state, err := transport.Conn.TLSConnectionState()
	if err != nil || len(state.VerifiedChains) == 0 {
		transport.Close()
		t.Fatal("unverified TLS broker connection", err)
	}
	transport.Close()
	bad := auth
	bad.CAFile = unrelatedCA(t)
	if unexpected, e := Connect(address, topology, bad); e == nil {
		unexpected.Close()
		t.Fatal("wrong trust root accepted")
	} else if strings.Contains(e.Error(), "fixture") || strings.Contains(e.Error(), "password") {
		t.Fatal("credential detail in error")
	}
	bad = auth
	bad.CAFile = filepath.Join(t.TempDir(), "missing-ca.pem")
	if unexpected, e := Connect(address, topology, bad); e == nil {
		unexpected.Close()
		t.Fatal("missing CA accepted")
	}
	t.Log("GATEWAY_NATS_TLS_PASS: authenticated broker connection verifies certificate chain and hostname; unrelated/missing CA rejected; no verification bypass")
}
