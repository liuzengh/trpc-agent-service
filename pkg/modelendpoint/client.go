// Package modelendpoint enforces the network boundary for an operator-owned
// OpenAI-compatible endpoint. Tenants never select the destination.
package modelendpoint

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strconv"
	"strings"
	"time"
)

var (
	ErrConfiguration = errors.New("operator model endpoint configuration invalid")
	ErrTransport     = errors.New("operator model endpoint transport unavailable")
)

type resolver interface {
	LookupNetIP(context.Context, string, string) ([]netip.Addr, error)
}
type dialFunc func(context.Context, string, string) (net.Conn, error)

// Client implements the SDK's HTTPClient contract while removing URL-bearing
// transport errors. TLS certificate verification remains enabled.
type Client struct {
	base   *url.URL
	client *http.Client
}

func New(raw string) (*Client, error) {
	return newClient(raw, net.DefaultResolver, (&net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}).DialContext)
}

func newClient(raw string, resolver resolver, dial dialFunc) (*Client, error) {
	u, err := parseEndpoint(raw)
	if err != nil {
		return nil, err
	}
	c := &Client{base: u}
	port := u.Port()
	if port == "" {
		port = "443"
	}
	transport := &http.Transport{
		Proxy:                 nil, // An ambient proxy must not bypass destination IP checks.
		TLSClientConfig:       &tls.Config{MinVersion: tls.VersionTLS12},
		TLSHandshakeTimeout:   10 * time.Second,
		ResponseHeaderTimeout: 90 * time.Second,
		IdleConnTimeout:       30 * time.Second,
		MaxIdleConns:          4, MaxIdleConnsPerHost: 4,
		DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
			host, actualPort, err := net.SplitHostPort(address)
			if err != nil || host != u.Hostname() || actualPort != port {
				return nil, ErrTransport
			}
			addresses, err := resolver.LookupNetIP(ctx, "ip", host)
			if err != nil || len(addresses) == 0 {
				return nil, ErrTransport
			}
			// Validate the complete answer before dialing any address. In
			// particular, mixed public/private DNS answers fail closed.
			for _, ip := range addresses {
				if !publicAddress(ip) {
					return nil, ErrTransport
				}
			}
			for _, ip := range addresses {
				if ctx.Err() != nil {
					return nil, ctx.Err()
				}
				conn, err := dial(ctx, network, net.JoinHostPort(ip.Unmap().String(), port))
				if err == nil {
					return conn, nil
				}
			}
			return nil, ErrTransport
		},
	}
	c.client = &http.Client{Transport: transport, Timeout: 2 * time.Minute,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	return c, nil
}

// Do accepts only requests under the configured HTTPS API prefix. Redirects
// are returned without following them, including redirects on the same host.
func (c *Client) Do(req *http.Request) (*http.Response, error) {
	if c == nil || req == nil || req.URL == nil || req.URL.Scheme != "https" || req.URL.Host != c.base.Host || req.URL.User != nil || req.URL.Fragment != "" || req.URL.RawPath != "" || req.URL.RawQuery != "" || req.URL.ForceQuery || (req.Host != "" && req.Host != c.base.Host) {
		return nil, ErrTransport
	}
	prefix := strings.TrimRight(c.base.Path, "/") + "/"
	if !strings.HasPrefix(req.URL.Path, prefix) || !validPath(req.URL.Path) {
		return nil, ErrTransport
	}
	resp, err := c.client.Do(req)
	if err != nil {
		if req.Context().Err() != nil {
			return nil, req.Context().Err()
		}
		return nil, ErrTransport
	}
	return sanitizeResponse(resp)
}

// The framework logs error.type when a compatible provider embeds an error
// in HTTP 200. Normalize error envelopes before the SDK observes them. SSE
// stays streaming; its errors are sanitized at the model response boundary.
func sanitizeResponse(resp *http.Response) (*http.Response, error) {
	if resp == nil || resp.Body == nil {
		return nil, ErrTransport
	}
	const safeBody = `{"error":{"type":"model_provider_error","message":"operator model request failed"}}`
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		resp.Body.Close()
		return replaceBody(resp, []byte(safeBody)), nil
	}
	mediaType, _, _ := mime.ParseMediaType(resp.Header.Get("Content-Type"))
	if mediaType == "text/event-stream" {
		return resp, nil
	}
	const maxJSONResponseBytes = 8 << 20
	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxJSONResponseBytes+1))
	resp.Body.Close()
	if err != nil || len(raw) > maxJSONResponseBytes {
		return nil, ErrTransport
	}
	var envelope struct {
		Error   json.RawMessage   `json:"error"`
		Choices []json.RawMessage `json:"choices"`
	}
	if json.Unmarshal(raw, &envelope) == nil && len(envelope.Choices) == 0 && len(envelope.Error) != 0 && string(envelope.Error) != "null" {
		raw = []byte(safeBody)
	}
	return replaceBody(resp, raw), nil
}

func replaceBody(resp *http.Response, body []byte) *http.Response {
	resp.Body = io.NopCloser(bytes.NewReader(body))
	resp.ContentLength = int64(len(body))
	resp.Header.Set("Content-Length", strconv.Itoa(len(body)))
	return resp
}

func parseEndpoint(raw string) (*url.URL, error) {
	u, err := url.Parse(raw)
	if err != nil || raw == "" || strings.TrimSpace(raw) != raw || u.Scheme != "https" || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.RawFragment != "" || u.RawPath != "" || strings.HasSuffix(u.Host, ":") || !validPath(u.Path) {
		return nil, ErrConfiguration
	}
	if port := u.Port(); port != "" {
		n, err := strconv.Atoi(port)
		if err != nil || n < 1 || n > 65535 {
			return nil, ErrConfiguration
		}
	}
	host := u.Hostname()
	if ip, err := netip.ParseAddr(host); err == nil {
		if !publicAddress(ip) {
			return nil, ErrConfiguration
		}
	} else {
		if len(host) > 253 || strings.HasSuffix(host, ".") || !strings.Contains(host, ".") {
			return nil, ErrConfiguration
		}
		for _, label := range strings.Split(host, ".") {
			if label == "" || len(label) > 63 || strings.HasPrefix(label, "-") || strings.HasSuffix(label, "-") {
				return nil, ErrConfiguration
			}
			for _, ch := range label {
				if !(ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z' || ch >= '0' && ch <= '9' || ch == '-') {
					return nil, ErrConfiguration
				}
			}
		}
	}
	return u, nil
}

func validPath(path string) bool {
	for _, part := range strings.Split(path, "/") {
		if part == "." || part == ".." {
			return false
		}
		for _, ch := range part {
			if !(ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z' || ch >= '0' && ch <= '9' || ch == '-' || ch == '_' || ch == '.' || ch == '~') {
				return false
			}
		}
	}
	return !strings.Contains(path, "//")
}

var excluded = []netip.Prefix{
	netip.MustParsePrefix("0.0.0.0/8"), netip.MustParsePrefix("100.64.0.0/10"),
	netip.MustParsePrefix("192.0.0.0/24"), netip.MustParsePrefix("192.0.2.0/24"),
	netip.MustParsePrefix("198.18.0.0/15"), netip.MustParsePrefix("198.51.100.0/24"),
	netip.MustParsePrefix("203.0.113.0/24"), netip.MustParsePrefix("240.0.0.0/4"),
	netip.MustParsePrefix("2001::/23"), netip.MustParsePrefix("2001:db8::/32"),
	netip.MustParsePrefix("2002::/16"), netip.MustParsePrefix("3fff::/20"),
}

func publicAddress(ip netip.Addr) bool {
	ip = ip.Unmap()
	if !ip.IsValid() || ip.Zone() != "" || !ip.IsGlobalUnicast() || ip.IsPrivate() || ip.IsLoopback() || ip.IsLinkLocalUnicast() {
		return false
	}
	if ip.Is6() && !netip.MustParsePrefix("2000::/3").Contains(ip) {
		return false
	}
	for _, block := range excluded {
		if block.Contains(ip) {
			return false
		}
	}
	return true
}
