package preprocess_test

import (
	"bufio"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/netip"
	"strings"
	"sync"
	"testing"
	"time"

	channel "github.com/liuzengh/trpc-agent-service/trpcservice/channels/contract"
	"github.com/liuzengh/trpc-agent-service/trpcservice/preprocess"
)

func TestSecureHTTPSFetcherPinsApprovedDNSAddress(t *testing.T) {
	certificate, roots := testTLSCertificate(t)
	var dialed string
	fetcher := preprocess.SecureHTTPSFetcher{AllowedHosts: map[string]struct{}{"example.com": {}},
		Resolver: resolverStub{values: map[string][]netip.Addr{"example.com": {netip.MustParseAddr("93.184.216.34")}}},
		RootCAs:  roots, DialContext: func(ctx context.Context, _, address string) (net.Conn, error) {
			dialed = address
			return tlsPipe(ctx, certificate, "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: 4\r\n\r\nsafe"), nil
		}}
	download, err := fetcher.Fetch(context.Background(), mediaFetchRequest("https://example.com/file"))
	if err != nil {
		t.Fatal(err)
	}
	content, err := io.ReadAll(download.Body)
	closeErr := download.Body.Close()
	if err != nil || closeErr != nil || string(content) != "safe" || download.ContentType != "text/plain" {
		t.Fatalf("content=%q download=%#v err=%v close=%v", content, download, err, closeErr)
	}
	if dialed != "93.184.216.34:443" {
		t.Fatalf("connection was not pinned: %q", dialed)
	}
}

func TestSecureHTTPSFetcherRejectsPrivateMixedDNSAndUnsafeURLs(t *testing.T) {
	cases := []struct {
		name    string
		url     string
		hosts   map[string]struct{}
		resolve map[string][]netip.Addr
	}{
		{name: "metadata", url: "https://169.254.169.254/latest", hosts: map[string]struct{}{"169.254.169.254": {}}},
		{name: "mixed dns", url: "https://example.com/file", hosts: map[string]struct{}{"example.com": {}},
			resolve: map[string][]netip.Addr{"example.com": {netip.MustParseAddr("93.184.216.34"), netip.MustParseAddr("127.0.0.1")}}},
		{name: "unapproved host", url: "https://other.example/file", hosts: map[string]struct{}{"example.com": {}}},
		{name: "plain http", url: "http://example.com/file", hosts: map[string]struct{}{"example.com": {}}},
		{name: "credentials", url: "https://user:secret@example.com/file", hosts: map[string]struct{}{"example.com": {}}},
		{name: "fragment", url: "https://example.com/file#secret", hosts: map[string]struct{}{"example.com": {}}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			called := false
			fetcher := preprocess.SecureHTTPSFetcher{AllowedHosts: test.hosts, Resolver: resolverStub{values: test.resolve},
				DialContext: func(context.Context, string, string) (net.Conn, error) { called = true; return nil, context.Canceled }}
			if _, err := fetcher.Fetch(context.Background(), mediaFetchRequest(test.url)); err == nil {
				t.Fatal("unsafe URL accepted")
			}
			if called {
				t.Fatal("unsafe destination reached dialer")
			}
		})
	}
}

func TestSecureHTTPSFetcherRevalidatesRedirectDestination(t *testing.T) {
	certificate, roots := testTLSCertificate(t)
	resolver := &recordingResolver{values: map[string][]netip.Addr{
		"example.com": {netip.MustParseAddr("93.184.216.34")}, "evil.example": {netip.MustParseAddr("10.0.0.8")},
	}}
	dials := 0
	fetcher := preprocess.SecureHTTPSFetcher{AllowedHosts: map[string]struct{}{"example.com": {}, "evil.example": {}},
		Resolver: resolver, RootCAs: roots, DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			dials++
			return tlsPipe(ctx, certificate, "HTTP/1.1 302 Found\r\nLocation: https://evil.example/private\r\nContent-Length: 0\r\n\r\n"), nil
		}}
	if _, err := fetcher.Fetch(context.Background(), mediaFetchRequest("https://example.com/redirect")); err == nil {
		t.Fatal("private redirect accepted")
	}
	if dials != 1 || !strings.Contains(strings.Join(resolver.hosts(), ","), "evil.example") {
		t.Fatalf("dials=%d lookups=%v", dials, resolver.hosts())
	}
}

func mediaFetchRequest(value string) preprocess.MediaFetchRequest {
	return preprocess.MediaFetchRequest{TenantID: "tenant", RequestID: "request", Channel: "feishu",
		Media: channel.MediaRef{ID: value, Kind: "file", ContentType: "text/plain"}}
}

type resolverStub struct{ values map[string][]netip.Addr }

func (r resolverStub) LookupNetIP(_ context.Context, _, host string) ([]netip.Addr, error) {
	return append([]netip.Addr(nil), r.values[host]...), nil
}

type recordingResolver struct {
	mu     sync.Mutex
	values map[string][]netip.Addr
	seen   []string
}

func (r *recordingResolver) LookupNetIP(_ context.Context, _, host string) ([]netip.Addr, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.seen = append(r.seen, host)
	return append([]netip.Addr(nil), r.values[host]...), nil
}

func (r *recordingResolver) hosts() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.seen...)
}

func testTLSCertificate(t *testing.T) (tls.Certificate, *x509.CertPool) {
	t.Helper()
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "example.com"},
		DNSNames: []string{"example.com"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	der, err := x509.CreateCertificate(rand.Reader, template, template, public, private)
	if err != nil {
		t.Fatal(err)
	}
	key, err := x509.MarshalPKCS8PrivateKey(private)
	if err != nil {
		t.Fatal(err)
	}
	certificate, err := tls.X509KeyPair(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: key}))
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	parsed, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	roots.AddCert(parsed)
	return certificate, roots
}

func tlsPipe(ctx context.Context, certificate tls.Certificate, response string) net.Conn {
	client, server := net.Pipe()
	go func() {
		defer server.Close()
		secured := tls.Server(server, &tls.Config{Certificates: []tls.Certificate{certificate}, MinVersion: tls.VersionTLS12})
		defer secured.Close()
		if err := secured.HandshakeContext(ctx); err != nil {
			return
		}
		request, err := http.ReadRequest(bufio.NewReader(secured))
		if err != nil {
			return
		}
		if request.Body != nil {
			request.Body.Close()
		}
		_, _ = io.WriteString(secured, response)
	}()
	return client
}
