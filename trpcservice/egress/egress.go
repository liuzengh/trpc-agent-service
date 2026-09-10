// Package egress contains the service's outbound network guard.
package egress

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
)

// ErrBlockedAddress indicates that an outbound destination is not an allowed
// public address.
var ErrBlockedAddress = errors.New("network address is not allowed")

// ResolveAllowedIPs resolves host and rejects every result that can target a
// private, local, or otherwise non-public network. Callers that make a real
// connection must repeat this check at dial time.
func ResolveAllowedIPs(ctx context.Context, host string) ([]net.IP, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	host = strings.TrimSpace(host)
	if host == "" {
		return nil, ErrBlockedAddress
	}
	if ip := net.ParseIP(host); ip != nil {
		if IsBlockedIP(ip) {
			return nil, ErrBlockedAddress
		}
		return []net.IP{ip}, nil
	}

	addresses, err := net.DefaultResolver.LookupIP(ctx, "ip", host)
	if err != nil {
		return nil, fmt.Errorf("resolve network address: %w", err)
	}
	if len(addresses) == 0 {
		return nil, errors.New("network address has no usable address")
	}
	for _, address := range addresses {
		if IsBlockedIP(address) {
			return nil, ErrBlockedAddress
		}
	}
	return addresses, nil
}

// IsBlockedIP identifies destinations that must never receive service
// credentials or tenant-controlled request data.
func IsBlockedIP(ip net.IP) bool {
	if ip == nil {
		return true
	}
	if v4 := ip.To4(); v4 != nil {
		return v4.IsPrivate() ||
			v4.IsLoopback() ||
			v4.IsLinkLocalUnicast() ||
			v4.IsLinkLocalMulticast() ||
			v4.IsUnspecified() ||
			v4.IsMulticast() ||
			isBlockedIPv4Special(v4)
	}
	return ip.IsPrivate() ||
		ip.IsLoopback() ||
		ip.IsLinkLocalUnicast() ||
		ip.IsLinkLocalMulticast() ||
		ip.IsUnspecified() ||
		ip.IsMulticast()
}

func isBlockedIPv4Special(ip net.IP) bool {
	if len(ip) < net.IPv4len {
		return true
	}
	// Shared address space, IETF protocol assignments, benchmarking ranges,
	// and reserved class-E space are not valid public service destinations.
	return (ip[0] == 100 && ip[1] >= 64 && ip[1] <= 127) ||
		(ip[0] == 192 && ip[1] == 0) ||
		(ip[0] == 198 && ip[1] >= 18 && ip[1] <= 19) ||
		ip[0] >= 240
}

// NewHTTPTransport returns an HTTPS-only transport that resolves and checks
// the destination immediately before every new connection. Proxy use is
// disabled so an environment proxy cannot bypass the destination policy.
func NewHTTPTransport() http.RoundTripper {
	transport, ok := http.DefaultTransport.(*http.Transport)
	if ok {
		transport = transport.Clone()
	} else {
		transport = &http.Transport{}
	}
	transport.Proxy = nil
	transport.DialTLSContext = nil
	transport.DialTLS = nil
	transport.DialContext = safeDialContext
	return &httpsTransport{transport: transport}
}

type httpsTransport struct {
	transport *http.Transport
}

func (t *httpsTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req == nil || req.URL == nil || req.URL.Scheme != "https" {
		return nil, errors.New("outbound endpoint must use https")
	}
	if req.URL.Hostname() == "" {
		return nil, ErrBlockedAddress
	}
	return t.transport.RoundTrip(req)
}

func safeDialContext(ctx context.Context, network, address string) (net.Conn, error) {
	host, port, err := net.SplitHostPort(address)
	if err != nil || host == "" || port == "" {
		return nil, fmt.Errorf("dial outbound endpoint: %w", ErrBlockedAddress)
	}
	addresses, err := ResolveAllowedIPs(ctx, host)
	if err != nil {
		return nil, fmt.Errorf("dial outbound endpoint: %w", err)
	}

	dialer := &net.Dialer{}
	var lastErr error
	for _, ip := range addresses {
		if IsBlockedIP(ip) {
			return nil, ErrBlockedAddress
		}
		conn, dialErr := dialer.DialContext(ctx, network, net.JoinHostPort(ip.String(), port))
		if dialErr == nil {
			return conn, nil
		}
		lastErr = dialErr
	}
	if lastErr == nil {
		return nil, ErrBlockedAddress
	}
	return nil, fmt.Errorf("dial outbound endpoint: %w", lastErr)
}
