package modelregistry

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	PolicyPublicHTTPS = "public_https"
	PolicyAllowlist   = "allowlist"
)

// WithEndpointPolicy is deployment-owned, not a tenant or model setting.
// Public HTTPS is provider-independent; explicit origins are trusted exceptions.
func WithEndpointPolicy(raw string) Option {
	return func(s *Store) error {
		value := strings.TrimSpace(raw)
		if value == "" {
			value = PolicyPublicHTTPS
		}
		if value != PolicyPublicHTTPS && value != PolicyAllowlist {
			return fmt.Errorf("%w: TRPC_AGENT_MODEL_ENDPOINT_POLICY 只能为 public_https 或 allowlist", ErrEndpoint)
		}
		s.endpointPolicy = value
		return nil
	}
}

func (s *Store) EndpointPolicy() string {
	if s == nil {
		return PolicyAllowlist
	}
	return s.endpointPolicy
}

func endpoint(raw string) (*url.URL, error) {
	u, err := url.Parse(raw)
	if err != nil || u == nil || len(raw) > 2048 || (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.Opaque != "" || strings.ContainsAny(raw, "\r\n\\") || strings.ContainsAny(u.Host, "%*") || strings.HasSuffix(u.Host, ":") {
		return nil, fmt.Errorf("%w: 请填写完整 HTTP(S) Base URL；地址中不能包含账号、密码、查询参数或片段，API Key 请填在独立字段", ErrEndpoint)
	}
	if u.Port() != "" {
		port, err := strconv.Atoi(u.Port())
		if err != nil || port < 1 || port > 65535 {
			return nil, fmt.Errorf("%w: 端口必须在 1–65535 之间", ErrEndpoint)
		}
	}
	return u, nil
}

func origin(u *url.URL) string {
	host := strings.ToLower(u.Hostname())
	port := u.Port()
	if (u.Scheme == "https" && port == "443") || (u.Scheme == "http" && port == "80") {
		port = ""
	}
	if port != "" {
		host = net.JoinHostPort(host, port)
	} else if strings.Contains(host, ":") {
		host = "[" + host + "]"
	}
	return u.Scheme + "://" + host
}

func (s *Store) AllowedOrigins() []string {
	result := []string{}
	if s != nil {
		for value := range s.origins {
			result = append(result, value)
		}
	}
	sort.Strings(result)
	return result
}

func (s *Store) validateEndpoint(raw string) error {
	if s == nil {
		return ErrUnavailable
	}
	u, err := endpoint(raw)
	if err != nil {
		return err
	}
	if s.origins[origin(u)] {
		return nil
	}
	if s.endpointPolicy != PolicyPublicHTTPS {
		return fmt.Errorf("%w: 此部署启用了严格白名单，请部署者在 TRPC_AGENT_MODEL_ALLOWED_ORIGINS 中允许目标地址", ErrEndpoint)
	}
	if u.Scheme != "https" {
		return fmt.Errorf("%w: 公网模型请使用 HTTPS；本地或内网 HTTP 服务需要部署者在 TRPC_AGENT_MODEL_ALLOWED_ORIGINS 中显式允许", ErrEndpoint)
	}
	host := strings.ToLower(strings.TrimSuffix(u.Hostname(), "."))
	if ip, err := netip.ParseAddr(host); err == nil {
		if !publicIP(ip) {
			return privateEndpointError()
		}
		return nil
	}
	if !strings.Contains(host, ".") {
		return privateEndpointError()
	}
	for _, suffix := range []string{".localhost", ".local", ".internal", ".home.arpa"} {
		if strings.HasSuffix(host, suffix) {
			return privateEndpointError()
		}
	}
	// DNS is checked at dial time, not here: saving does not contact the provider.
	return nil
}

func privateEndpointError() error {
	return fmt.Errorf("%w: 目标为本地、内网或保留地址；自建模型需要部署者在 TRPC_AGENT_MODEL_ALLOWED_ORIGINS 中显式允许", ErrEndpoint)
}

// IsGlobalUnicast alone also accepts private/documentation space. Be conservative
// with IANA special-purpose space, including IPv6 transition/NAT64 mechanisms.
// A deployment can explicitly allow an otherwise restricted legitimate origin.
var nonPublicPrefixes = []netip.Prefix{
	netip.MustParsePrefix("0.0.0.0/8"),
	netip.MustParsePrefix("100.64.0.0/10"),
	netip.MustParsePrefix("192.0.0.0/24"),
	netip.MustParsePrefix("192.0.2.0/24"),
	netip.MustParsePrefix("192.88.99.0/24"),
	netip.MustParsePrefix("198.18.0.0/15"),
	netip.MustParsePrefix("198.51.100.0/24"),
	netip.MustParsePrefix("203.0.113.0/24"),
	netip.MustParsePrefix("240.0.0.0/4"),
	netip.MustParsePrefix("2001::/23"),
	netip.MustParsePrefix("2001:db8::/32"),
	netip.MustParsePrefix("2002::/16"),
	netip.MustParsePrefix("3fff::/20"),
}

var publicIPv6 = netip.MustParsePrefix("2000::/3")

func publicIP(ip netip.Addr) bool {
	ip = ip.Unmap()
	if ip.Zone() != "" || !ip.IsGlobalUnicast() || ip.IsPrivate() || ip.IsLoopback() || ip.IsLinkLocalUnicast() || (ip.Is6() && !publicIPv6.Contains(ip)) {
		return false
	}
	for _, prefix := range nonPublicPrefixes {
		if prefix.Contains(ip) {
			return false
		}
	}
	return true
}

func newPublicTransport() *http.Transport {
	return NewPublicHTTPTransport()
}

// NewPublicHTTPTransport is also used by explicit public-ingress health probes.
// It has no trusted-origin exceptions or proxies, validates every DNS answer,
// and retains normal TLS verification. Callers enforce HTTPS and no redirects.
func NewPublicHTTPTransport() *http.Transport {
	// Unlisted destinations must not delegate DNS resolution to a generic proxy:
	// that would bypass the IP checks below. Trusted origins retain the deployment
	// transport/proxy/loopback-alias behavior in transport.go.
	return &http.Transport{
		DialContext:           dialPublic,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: time.Second,
	}
}

func dialPublic(ctx context.Context, network, address string) (net.Conn, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return nil, ErrEndpoint
	}
	addresses, err := net.DefaultResolver.LookupNetIP(ctx, "ip", host)
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if err != nil || len(addresses) == 0 {
		return nil, fmt.Errorf("%w: DNS 解析失败，请检查模型域名和服务器网络", ErrEndpoint)
	}
	// Validate ALL answers before connecting. Dial only the validated IP, never
	// resolve the hostname a second time (DNS rebinding / TOCTOU).
	for _, ip := range addresses {
		if !publicIP(ip) {
			return nil, privateEndpointError()
		}
	}
	for _, ip := range addresses {
		dialer := &net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}
		conn, err := dialer.DialContext(ctx, network, net.JoinHostPort(ip.String(), port))
		if err == nil {
			return conn, nil
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
	}
	return nil, fmt.Errorf("%w: 无法连接公网模型服务，请检查服务地址和服务器网络", ErrEndpoint)
}
