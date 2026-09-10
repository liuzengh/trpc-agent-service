package preprocess

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"sort"
	"strings"
	"time"

	channel "github.com/liuzengh/trpc-agent-service/trpcservice/channels/contract"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

type MediaFetchRequest struct {
	TenantID, RequestID, Channel, ChannelBindingID, ExternalAccountID string
	ConfigVersion                                                     int64
	Ordinal                                                           int
	Media                                                             channel.MediaRef
}

type MediaDownload struct {
	Body            io.ReadCloser
	ContentType     string
	ContentEncoding string
	DeclaredSize    int64
	ResolvedURL     string
}

type MediaFetcher interface {
	Fetch(context.Context, MediaFetchRequest) (MediaDownload, error)
}

type IPResolver interface {
	LookupNetIP(context.Context, string, string) ([]netip.Addr, error)
}

type SecureHTTPSFetcher struct {
	AllowedHosts map[string]struct{}
	Resolver     IPResolver
	DialContext  func(context.Context, string, string) (net.Conn, error)
	Timeout      time.Duration
	MaxRedirects int
	RootCAs      *x509.CertPool
}

func (f SecureHTTPSFetcher) Fetch(ctx context.Context, request MediaFetchRequest) (MediaDownload, error) {
	if request.TenantID == "" || request.RequestID == "" || request.Ordinal < 0 || request.Media.ID == "" {
		return MediaDownload{}, runtime.ErrInvalidEnvelope
	}
	resolver := f.Resolver
	if resolver == nil {
		resolver = net.DefaultResolver
	}
	dial := f.DialContext
	if dial == nil {
		dialer := &net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}
		dial = dialer.DialContext
	}
	transport := &http.Transport{Proxy: nil, DisableCompression: true, ForceAttemptHTTP2: true,
		TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: f.RootCAs}}
	transport.DialContext = func(dialCtx context.Context, network, address string) (net.Conn, error) {
		host, port, err := net.SplitHostPort(address)
		if err != nil {
			return nil, runtime.ErrInvalidEnvelope
		}
		pinned, err := f.resolve(dialCtx, resolver, host)
		if err != nil {
			return nil, err
		}
		return dial(dialCtx, network, net.JoinHostPort(pinned, port))
	}
	maximum := f.MaxRedirects
	if maximum <= 0 {
		maximum = 3
	}
	client := &http.Client{Transport: transport, CheckRedirect: func(next *http.Request, previous []*http.Request) error {
		if len(previous) >= maximum {
			return runtime.ErrInvalidEnvelope
		}
		return f.validateURL(next.URL)
	}}
	client.Timeout = f.Timeout
	if client.Timeout <= 0 {
		client.Timeout = 15 * time.Second
	}
	parsed, err := url.Parse(request.Media.ID)
	if err != nil || f.validateURL(parsed) != nil {
		return MediaDownload{}, runtime.ErrInvalidEnvelope
	}
	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodGet, parsed.String(), nil)
	if err != nil {
		return MediaDownload{}, runtime.ErrInvalidEnvelope
	}
	httpRequest.Header.Set("Accept", "application/octet-stream,image/*,application/pdf,text/plain")
	response, err := client.Do(httpRequest)
	if err != nil {
		if ctx.Err() != nil {
			return MediaDownload{}, ctx.Err()
		}
		return MediaDownload{}, runtime.ErrBackendUnavailable
	}
	if response == nil {
		return MediaDownload{}, runtime.ErrBackendUnavailable
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		response.Body.Close()
		transport.CloseIdleConnections()
		return MediaDownload{}, runtime.ErrBackendUnavailable
	}
	return MediaDownload{Body: cleanupBody{ReadCloser: response.Body, cleanup: transport.CloseIdleConnections},
		ContentType: response.Header.Get("Content-Type"), ContentEncoding: response.Header.Get("Content-Encoding"), DeclaredSize: response.ContentLength,
		ResolvedURL: response.Request.URL.String()}, nil
}

func (f SecureHTTPSFetcher) validateURL(value *url.URL) error {
	if value == nil || value.Scheme != "https" || value.User != nil || value.Fragment != "" || value.Hostname() == "" {
		return runtime.ErrInvalidEnvelope
	}
	port := value.Port()
	if port != "" && port != "443" {
		return runtime.ErrInvalidEnvelope
	}
	host := strings.ToLower(value.Hostname())
	for _, current := range host {
		if (current < 'a' || current > 'z') && (current < '0' || current > '9') && current != '.' && current != '-' {
			return runtime.ErrInvalidEnvelope
		}
	}
	if len(f.AllowedHosts) == 0 {
		return runtime.ErrCapabilityUnsupported
	}
	allowed := false
	for configured := range f.AllowedHosts {
		if strings.EqualFold(configured, host) {
			allowed = true
			break
		}
	}
	if !allowed {
		return runtime.ErrTenantScope
	}
	return nil
}

func (f SecureHTTPSFetcher) resolve(ctx context.Context, resolver IPResolver, host string) (string, error) {
	if parsed, err := netip.ParseAddr(host); err == nil {
		if !safePublicAddress(parsed.Unmap()) {
			return "", runtime.ErrTenantScope
		}
		return parsed.String(), nil
	}
	addresses, err := resolver.LookupNetIP(ctx, "ip", host)
	if err != nil || len(addresses) == 0 {
		return "", runtime.ErrBackendUnavailable
	}
	values := make([]string, 0, len(addresses))
	for _, value := range addresses {
		if !safePublicAddress(value.Unmap()) {
			return "", runtime.ErrTenantScope
		}
		values = append(values, value.Unmap().String())
	}
	sort.Strings(values)
	return values[0], nil
}

var deniedAddressPrefixes = []netip.Prefix{
	netip.MustParsePrefix("0.0.0.0/8"), netip.MustParsePrefix("10.0.0.0/8"),
	netip.MustParsePrefix("100.64.0.0/10"), netip.MustParsePrefix("127.0.0.0/8"),
	netip.MustParsePrefix("169.254.0.0/16"), netip.MustParsePrefix("172.16.0.0/12"),
	netip.MustParsePrefix("192.0.0.0/24"), netip.MustParsePrefix("192.0.2.0/24"),
	netip.MustParsePrefix("192.168.0.0/16"), netip.MustParsePrefix("198.18.0.0/15"),
	netip.MustParsePrefix("198.51.100.0/24"), netip.MustParsePrefix("203.0.113.0/24"),
	netip.MustParsePrefix("224.0.0.0/4"), netip.MustParsePrefix("240.0.0.0/4"),
	netip.MustParsePrefix("::/128"), netip.MustParsePrefix("::1/128"),
	netip.MustParsePrefix("fc00::/7"), netip.MustParsePrefix("fe80::/10"),
	netip.MustParsePrefix("ff00::/8"), netip.MustParsePrefix("2001:db8::/32"),
}

func safePublicAddress(value netip.Addr) bool {
	if !value.IsValid() || !value.IsGlobalUnicast() {
		return false
	}
	for _, prefix := range deniedAddressPrefixes {
		if prefix.Contains(value) {
			return false
		}
	}
	return true
}

type cleanupBody struct {
	io.ReadCloser
	cleanup func()
}

func (b cleanupBody) Close() error {
	err := b.ReadCloser.Close()
	if b.cleanup != nil {
		b.cleanup()
	}
	return err
}
