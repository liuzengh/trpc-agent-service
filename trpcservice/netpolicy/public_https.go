package netpolicy

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// ValidatePublicHTTPSURL validates the stable URL shape. Network-address
// validation is repeated at dial time to prevent DNS rebinding.
func ValidatePublicHTTPSURL(raw string) error {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || parsed.Scheme != "https" || parsed.Hostname() == "" || parsed.User != nil || parsed.Fragment != "" {
		return errors.New("remote endpoint must be an HTTPS URL without credentials or fragment")
	}
	return nil
}

// ValidatePublicHTTPS resolves the endpoint immediately and rejects non-public
// addresses before work is queued or a remote integration is activated.
func ValidatePublicHTTPS(ctx context.Context, raw string) error {
	if err := ValidatePublicHTTPSURL(raw); err != nil {
		return err
	}
	parsed, _ := url.Parse(strings.TrimSpace(raw))
	addresses, err := net.DefaultResolver.LookupIPAddr(ctx, parsed.Hostname())
	if err != nil {
		return fmt.Errorf("resolve host %q: %w", parsed.Hostname(), err)
	}
	return ValidatePublicAddresses(parsed.Hostname(), addresses)
}

// NewPublicHTTPSClient returns an HTTP client that never uses environment
// proxies and refuses loopback, private, link-local, multicast and unspecified
// destinations on every dial and redirect.
func NewPublicHTTPSClient(timeout time.Duration) *http.Client {
	lookup := net.DefaultResolver.LookupIPAddr
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	transport.DialContext = safeDialContext(lookup)
	client := &http.Client{Timeout: timeout, Transport: transport}
	client.CheckRedirect = func(request *http.Request, via []*http.Request) error {
		if len(via) >= 10 {
			return errors.New("too many remote endpoint redirects")
		}
		return ValidatePublicHTTPSURL(request.URL.String())
	}
	return client
}

func safeDialContext(lookup func(context.Context, string) ([]net.IPAddr, error)) func(context.Context, string, string) (net.Conn, error) {
	return func(ctx context.Context, network, address string) (net.Conn, error) {
		host, port, err := net.SplitHostPort(address)
		if err != nil {
			return nil, err
		}
		addresses, err := lookup(ctx, host)
		if err != nil {
			return nil, err
		}
		if err := ValidatePublicAddresses(host, addresses); err != nil {
			return nil, err
		}
		dialer := &net.Dialer{}
		return dialer.DialContext(ctx, network, net.JoinHostPort(addresses[0].IP.String(), port))
	}
}

// ValidatePublicAddresses rejects destinations that can reach the local host,
// cloud metadata networks, private networks or multicast ranges.
func ValidatePublicAddresses(host string, addresses []net.IPAddr) error {
	if len(addresses) == 0 {
		return fmt.Errorf("host %q resolved to no addresses", host)
	}
	for _, address := range addresses {
		ip := address.IP
		if ip == nil || ip.IsLoopback() || ip.IsPrivate() || ip.IsUnspecified() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.IsMulticast() {
			return fmt.Errorf("host %q resolves to a non-public address", host)
		}
	}
	return nil
}
