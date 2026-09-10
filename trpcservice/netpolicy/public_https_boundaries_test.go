package netpolicy

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/url"
	"testing"
	"time"
)

func TestValidatePublicHTTPSURLContract(t *testing.T) {
	t.Parallel()
	for _, raw := range []string{
		"",
		"http://example.com",
		"https://",
		"https://user:password@example.com",
		"https://example.com/path#fragment",
		"://invalid",
	} {
		raw := raw
		t.Run(raw, func(t *testing.T) {
			t.Parallel()
			if err := ValidatePublicHTTPSURL(raw); err == nil {
				t.Fatalf("ValidatePublicHTTPSURL(%q) succeeded", raw)
			}
		})
	}
	if err := ValidatePublicHTTPSURL("  https://example.com/v1  "); err != nil {
		t.Fatalf("valid HTTPS URL rejected: %v", err)
	}
}

func TestValidatePublicHTTPSRejectsInvalidShapeBeforeDNS(t *testing.T) {
	t.Parallel()
	if err := ValidatePublicHTTPS(context.Background(), "http://127.0.0.1"); err == nil {
		t.Fatal("ValidatePublicHTTPS accepted a non-HTTPS endpoint")
	}
}

func TestPublicHTTPSClientDisablesProxyAndChecksRedirects(t *testing.T) {
	t.Parallel()
	client := NewPublicHTTPSClient(3 * time.Second)
	if client.Timeout != 3*time.Second {
		t.Fatalf("client timeout = %v", client.Timeout)
	}
	transport, ok := client.Transport.(*http.Transport)
	if !ok || transport.Proxy != nil || transport.DialContext == nil {
		t.Fatal("public HTTPS transport did not disable proxies or install safe dialing")
	}
	request := &http.Request{URL: &url.URL{Scheme: "https", Host: "example.com"}}
	if err := client.CheckRedirect(request, make([]*http.Request, 10)); err == nil {
		t.Fatal("redirect limit was not enforced")
	}
	request.URL.User = url.User("credential")
	if err := client.CheckRedirect(request, nil); err == nil {
		t.Fatal("credential-bearing redirect was accepted")
	}
	request.URL.User = nil
	if err := client.CheckRedirect(request, nil); err != nil {
		t.Fatalf("valid redirect rejected: %v", err)
	}
}

func TestSafeDialRejectsMalformedLookupAndPrivateTargets(t *testing.T) {
	t.Parallel()
	lookupErr := errors.New("DNS unavailable")
	dial := safeDialContext(func(context.Context, string) ([]net.IPAddr, error) {
		return nil, lookupErr
	})
	if _, err := dial(context.Background(), "tcp", "missing-port"); err == nil {
		t.Fatal("safe dial accepted an address without a port")
	}
	if _, err := dial(context.Background(), "tcp", "example.com:443"); !errors.Is(err, lookupErr) {
		t.Fatalf("lookup error = %v, want %v", err, lookupErr)
	}

	dial = safeDialContext(func(context.Context, string) ([]net.IPAddr, error) {
		return []net.IPAddr{{IP: net.ParseIP("127.0.0.1")}}, nil
	})
	if _, err := dial(context.Background(), "tcp", "example.com:443"); err == nil {
		t.Fatal("safe dial accepted a loopback resolution")
	}
}

func TestValidatePublicAddressesRejectsEmptyAndInvalidResults(t *testing.T) {
	t.Parallel()
	if err := ValidatePublicAddresses("empty.example", nil); err == nil {
		t.Fatal("empty DNS result was accepted")
	}
	for _, address := range []net.IPAddr{
		{},
		{IP: net.IPv4zero},
		{IP: net.ParseIP("224.0.0.1")},
		{IP: net.ParseIP("169.254.1.1")},
	} {
		if err := ValidatePublicAddresses("blocked.example", []net.IPAddr{address}); err == nil {
			t.Fatalf("non-public address %v was accepted", address.IP)
		}
	}
}
