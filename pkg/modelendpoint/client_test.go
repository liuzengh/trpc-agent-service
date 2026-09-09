package modelendpoint

import (
	"bytes"
	"context"
	"crypto/x509"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"sync/atomic"
	"testing"
)

type resolverFunc func(context.Context, string, string) ([]netip.Addr, error)

func (f resolverFunc) LookupNetIP(ctx context.Context, network, host string) ([]netip.Addr, error) {
	return f(ctx, network, host)
}

func TestEndpointConfigurationBoundary(t *testing.T) {
	for _, raw := range []string{"https://api.provider.example/v1", "https://api.provider.example/v1/", "https://provider.example:8443/api/v1", "https://8.8.8.8/v1", "https://[2606:4700:4700::1111]/v1"} {
		if _, err := New(raw); err != nil {
			t.Errorf("valid endpoint rejected: %s", raw)
		}
	}
	for _, raw := range []string{"", "http://provider.example/v1", "https://user:secret@provider.example/v1", "https://provider.example/v1?token=private", "https://provider.example/v1?", "https://provider.example/v1#private", "https://provider.example:0/v1", "https://provider.example:65536/v1", "https://provider.example:/v1", "https://127.0.0.1/v1", "https://[::1]/v1", "https://169.254.169.254/v1", "https://100.64.0.1/v1", "https://localhost/v1", "https://provider.example./v1", "https://provider.example/a/../v1", "https://provider.example/%2e%2e/v1", "https://provider.example//v1", " https://provider.example/v1"} {
		_, err := New(raw)
		if !errors.Is(err, ErrConfiguration) || strings.Contains(err.Error(), "secret") || strings.Contains(err.Error(), "private") {
			t.Errorf("invalid endpoint accepted or leaked: %s", raw)
		}
	}
}

func TestPublicAddressBoundary(t *testing.T) {
	for _, raw := range []string{"0.0.0.0", "10.0.0.1", "127.0.0.1", "169.254.1.1", "172.16.0.1", "192.168.0.1", "100.64.0.1", "198.18.0.1", "192.0.2.1", "198.51.100.1", "203.0.113.1", "224.0.0.1", "240.0.0.1", "::", "::1", "::ffff:127.0.0.1", "64:ff9b::7f00:1", "fc00::1", "fe80::1", "ff02::1", "2001:db8::1", "2002:7f00:1::1", "3fff::1"} {
		if publicAddress(netip.MustParseAddr(raw)) {
			t.Errorf("nonpublic address accepted: %s", raw)
		}
	}
}

func tlsFixture(t *testing.T, handler http.Handler, lookup resolver) (*Client, *atomic.Int32, *httptest.Server) {
	t.Helper()
	server := httptest.NewTLSServer(handler)
	t.Cleanup(server.Close)
	_, port, _ := net.SplitHostPort(server.Listener.Addr().String())
	var dials atomic.Int32
	c, err := newClient("https://example.com:"+port+"/v1", lookup, func(ctx context.Context, network, address string) (net.Conn, error) {
		dials.Add(1)
		if address != net.JoinHostPort("8.8.8.8", port) {
			t.Errorf("dial did not use verified IP: %s", address)
		}
		return (&net.Dialer{}).DialContext(ctx, network, server.Listener.Addr().String())
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.client.CloseIdleConnections)
	return c, &dials, server
}

func publicDNS(context.Context, string, string) ([]netip.Addr, error) {
	return []netip.Addr{netip.MustParseAddr("8.8.8.8")}, nil
}
func requestFor(t *testing.T, c *Client, path string) *http.Request {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, "https://"+c.base.Host+path, strings.NewReader(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer test-only-key")
	return req
}
func trustFixture(c *Client, server *httptest.Server) {
	pool := x509.NewCertPool()
	pool.AddCert(server.Certificate())
	c.client.Transport.(*http.Transport).TLSClientConfig.RootCAs = pool
}

func TestPinnedDestinationTLSAndDNSRebinding(t *testing.T) {
	var rebound atomic.Bool
	var calls atomic.Int32
	c, dials, server := tlsFixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.TLS.ServerName != "example.com" || r.Header.Get("Authorization") != "Bearer test-only-key" {
			t.Error("TLS identity or API credential contract changed")
		}
		_, _ = io.WriteString(w, `{"ok":true}`)
	}), resolverFunc(func(ctx context.Context, network, host string) ([]netip.Addr, error) {
		if host != "example.com" {
			t.Error("wrong resolver host")
		}
		if rebound.Load() {
			return []netip.Addr{netip.MustParseAddr("127.0.0.1")}, nil
		}
		return publicDNS(ctx, network, host)
	}))
	trustFixture(c, server)
	resp, err := c.Do(requestFor(t, c, "/v1/chat/completions"))
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	c.client.CloseIdleConnections()
	rebound.Store(true)
	if _, err := c.Do(requestFor(t, c, "/v1/chat/completions")); !errors.Is(err, ErrTransport) {
		t.Fatalf("DNS rebinding accepted: %v", err)
	}
	if dials.Load() != 1 || calls.Load() != 1 {
		t.Fatalf("private target reached: dials=%d requests=%d", dials.Load(), calls.Load())
	}
}

func TestMixedDNSAnswerNeverDials(t *testing.T) {
	c, dials, _ := tlsFixture(t, http.NotFoundHandler(), resolverFunc(func(context.Context, string, string) ([]netip.Addr, error) {
		return []netip.Addr{netip.MustParseAddr("8.8.8.8"), netip.MustParseAddr("10.0.0.1")}, nil
	}))
	if _, err := c.Do(requestFor(t, c, "/v1/chat/completions")); !errors.Is(err, ErrTransport) || dials.Load() != 0 {
		t.Fatalf("mixed DNS accepted: err=%v dials=%d", err, dials.Load())
	}
}

func TestTLSVerificationCannotBeSkipped(t *testing.T) {
	var calls atomic.Int32
	c, _, _ := tlsFixture(t, http.HandlerFunc(func(http.ResponseWriter, *http.Request) { calls.Add(1) }), resolverFunc(publicDNS))
	if _, err := c.Do(requestFor(t, c, "/v1/chat/completions")); !errors.Is(err, ErrTransport) || calls.Load() != 0 {
		t.Fatalf("untrusted TLS accepted: %v requests=%d", err, calls.Load())
	}
}

func TestRedirectAndCrossOriginRequestsBlocked(t *testing.T) {
	var calls atomic.Int32
	c, dials, server := tlsFixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		http.Redirect(w, r, "https://other.example/private", http.StatusTemporaryRedirect)
	}), resolverFunc(publicDNS))
	trustFixture(c, server)
	resp, err := c.Do(requestFor(t, c, "/v1/chat/completions"))
	if err != nil || resp.StatusCode != 307 || calls.Load() != 1 {
		t.Fatalf("redirect followed: %v", err)
	}
	resp.Body.Close()
	for _, path := range []string{"/private", "/v1/../private", "/v1/chat/completions?token=private"} {
		if _, err := c.Do(requestFor(t, c, path)); !errors.Is(err, ErrTransport) {
			t.Errorf("off-prefix request accepted: %s", path)
		}
	}
	req := requestFor(t, c, "/v1/chat/completions")
	req.URL.Host = "other.example"
	if _, err := c.Do(req); !errors.Is(err, ErrTransport) || dials.Load() != 1 {
		t.Fatalf("cross origin reached: %v", err)
	}
}

func TestResolverFailureIsSanitized(t *testing.T) {
	c, _, _ := tlsFixture(t, http.NotFoundHandler(), resolverFunc(func(context.Context, string, string) ([]netip.Addr, error) {
		return nil, errors.New("secret https://private.invalid")
	}))
	_, err := c.Do(requestFor(t, c, "/v1/chat/completions"))
	if err != ErrTransport {
		t.Fatalf("unsafe error: %v", err)
	}
}

func TestProviderErrorSanitizedBeforeSDKLogging(t *testing.T) {
	for _, code := range []int{200, 401, 429, 503} {
		response := &http.Response{StatusCode: code, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"error":{"type":"leaked-key","message":"https://private.invalid"}}`))}
		response.Header.Set("Content-Type", "application/json")
		clean, err := sanitizeResponse(response)
		if err != nil {
			t.Fatal(err)
		}
		raw, _ := io.ReadAll(clean.Body)
		clean.Body.Close()
		if bytes.Contains(raw, []byte("leaked-key")) || bytes.Contains(raw, []byte("private.invalid")) || !bytes.Contains(raw, []byte("model_provider_error")) {
			t.Fatalf("unsafe error envelope: %s", raw)
		}
	}
	for _, contentType := range []string{"application/json", "text/event-stream"} {
		original := `{"choices":[{"message":{"content":"successful answer"}}],"usage":{"total_tokens":19}}`
		response := &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(original))}
		response.Header.Set("Content-Type", contentType)
		body := response.Body
		clean, err := sanitizeResponse(response)
		if err != nil {
			t.Fatal(err)
		}
		if contentType == "text/event-stream" && clean.Body != body {
			t.Fatal("SSE was buffered")
		}
		raw, _ := io.ReadAll(clean.Body)
		clean.Body.Close()
		if string(raw) != original {
			t.Fatal("successful response changed")
		}
	}
}
